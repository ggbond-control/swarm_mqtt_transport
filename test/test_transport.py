"""Wire boundary tests without sockets or ROS nodes."""

import json
from queue import Queue
from types import SimpleNamespace

from swarm_mqtt_transport.node import TransportNode
from swarm_protocol.contracts import MAX_MESSAGE_BYTES, canonical, envelope


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
    )
    TransportNode.send(relay, "t", {"kind": "permit_grant"})
    assert not sent
    relay.connected = True
    TransportNode.send(relay, "t", {"kind": "permit_grant"})
    assert len(sent) == 1 and sent[0][1] == {"qos": 1, "retain": False}
