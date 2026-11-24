"""Wire boundary tests without sockets or ROS nodes."""

import json
import ssl
from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from rclpy.node import Node
from swarm_mqtt_transport.node import TransportNode
from swarm_protocol.contracts import MAX_MESSAGE_BYTES, canonical, envelope

from swarm_mqtt_transport import node as transport_module


class Publisher:
    def __init__(self):
        self.values = []

    def publish(self, value):
        self.values.append(json.loads(value.data))


def fixture():
    relay = SimpleNamespace(
        incoming=Queue(maxsize=2),
        cluster="test",
        robot="A",
        platform="dev/dispatch",
        inbox=Publisher(),
        platform_in=Publisher(),
    )
    relay.get_logger = lambda: SimpleNamespace(warning=lambda _: None)
    return relay


def test_retained_control_and_oversize_payload_are_dropped():
    relay = fixture()
    for raw, retained in ((b"{}", True), (b"x" * (MAX_MESSAGE_BYTES + 1), False)):
        TransportNode.message(
            relay,
            None,
            None,
            SimpleNamespace(
                payload=raw, retain=retained, topic="dev/dispatch/services"
            ),
        )
    assert relay.incoming.empty()


def test_sender_topic_fencing_and_platform_isolation():
    relay = fixture()
    msg = envelope("test", "B", "boot", "A", "hello", {}, 1)
    relay.incoming.put(("fh/swarm/test/to/A/C", canonical(msg)))
    relay.incoming.put(("fh/swarm/test/to/A/B", canonical(msg)))
    TransportNode.drain(relay)
    assert relay.inbox.values == [msg]
    assert not relay.platform_in.values
    relay.incoming.put(("dev/dispatch/services", canonical({"method": "task.execute"})))
    TransportNode.drain(relay)
    assert relay.platform_in.values == [{"method": "task.execute"}]


def test_inbound_overflow_does_not_block_network_loop():
    relay = fixture()
    for _ in range(5):
        TransportNode.message(
            relay, None, None, SimpleNamespace(payload=b"{}", retain=False, topic="t")
        )
    assert relay.incoming.qsize() == 2


def test_disconnected_outbound_is_not_queued_as_future_permit():
    sent = []
    relay = SimpleNamespace(
        connected=False,
        client=SimpleNamespace(publish=lambda *args, **kw: sent.append((args, kw))),
        audit=Mock(),
    )
    TransportNode.send(relay, "t", {"kind": "permit_grant"})
    assert not sent
    relay.connected = True
    TransportNode.send(relay, "t", {"kind": "permit_grant"})
    assert len(sent) == 1 and sent[0][1] == {"qos": 1, "retain": False}


def test_platform_and_heartbeat_publish_only_new_swarm_topics():
    sent = []
    relay = SimpleNamespace(
        robot="CYVET-A",
        platform="fh/swarm/dispatch",
        send=lambda topic, value: sent.append((topic, value)),
    )
    for channel in ("events", "services_reply", "state", "heartbeat"):
        value = {"channel": channel, "payload": {"marker": channel}}
        TransportNode.platform_outbound(relay, SimpleNamespace(data=canonical(value)))
    assert sent == [
        ("fh/swarm/dispatch/events", {"marker": "events"}),
        ("fh/swarm/dispatch/services_reply", {"marker": "services_reply"}),
        ("fh/swarm/dispatch/state", {"marker": "state"}),
        ("fh/swarm/device/CYVET-A/heartbeat", {"marker": "heartbeat"}),
    ]


@pytest.mark.parametrize("tls", [False, True])
def test_remote_broker_needs_no_extra_permission_and_tls_keeps_certificate_checks(
    monkeypatch,
    tmp_path,
    tls,
):
    params = {
        "robot_id": "CYVET-A",
        "mqtt_host": "remote-broker.invalid",
        "mqtt_tls": tls,
        "mqtt_ca": "/test/ca.pem",
        "mqtt_username": "test-user",
        "mqtt_password": "test-password",
    }
    monkeypatch.setattr(Node, "__init__", lambda *args, **kw: None)
    monkeypatch.setattr(Node, "get_logger", lambda *args: Mock())
    monkeypatch.setattr(
        Node, "declare_parameter", lambda self, k, v: params.setdefault(k, v)
    )
    monkeypatch.setattr(
        Node, "get_parameter", lambda self, k: SimpleNamespace(value=params[k])
    )
    for name in ("create_publisher", "create_subscription", "create_timer"):
        monkeypatch.setattr(Node, name, lambda *args, **kw: Mock())
    monkeypatch.setattr(transport_module, "runtime_path", lambda *args: tmp_path)
    monkeypatch.setattr(transport_module, "ProcessLock", lambda *args: Mock())
    monkeypatch.setattr(transport_module, "Audit", lambda *args: Mock())
    control, media = Mock(), Mock()
    monkeypatch.setattr(
        transport_module.mqtt, "Client", Mock(side_effect=[control, media])
    )
    relay = TransportNode()
    assert "allow_insecure_remote" not in params
    assert relay.prefix == "fh/swarm/peer/inspection-swarm"
    assert relay.platform == "fh/swarm/dispatch"
    for client in (control, media):
        client.connect_async.assert_called_once_with(
            "remote-broker.invalid", 1883, keepalive=15
        )
        client.username_pw_set.assert_called_once_with("test-user", "test-password")
        if tls:
            client.tls_set.assert_called_once_with(
                ca_certs="/test/ca.pem", cert_reqs=ssl.CERT_REQUIRED
            )
        else:
            client.tls_set.assert_not_called()
    TransportNode.connect(relay, control, None, None, 0)
    subscribed = {call.args[0] for call in control.subscribe.call_args_list}
    assert subscribed == {
        "fh/swarm/peer/inspection-swarm/broadcast/+",
        "fh/swarm/peer/inspection-swarm/to/CYVET-A/+",
        "fh/swarm/dispatch/services",
    }
