"""Bounded MQTT relay. Does not acknowledge application commits or start routes."""

import queue
import ssl
import uuid

import paho.mqtt.client as mqtt
from rclpy.node import Node
from std_msgs.msg import String
from swarm_protocol.audit import Audit
from swarm_protocol.contracts import (
    MAX_MESSAGE_BYTES,
    canonical,
    decode,
    envelope,
    identifier,
    validate_envelope,
)
from swarm_protocol.ros import main_node, publish, runtime_path
from swarm_protocol.storage import ProcessLock
from swarm_protocol.topics import (
    DEFAULT_CLUSTER_ID,
    DEFAULT_PEER_PREFIX,
    DEFAULT_PLATFORM_PREFIX,
    heartbeat_topic,
)


class TransportNode(Node):
    def __init__(self):
        super().__init__("swarm_mqtt_transport")
        for key, value in [
            ("robot_id", ""),
            ("cluster_id", DEFAULT_CLUSTER_ID),
            ("mqtt_host", "127.0.0.1"),
            ("mqtt_port", 1883),
            ("mqtt_username", ""),
            ("mqtt_password", ""),
            ("mqtt_tls", False),
            ("mqtt_ca", ""),
            ("platform_prefix", DEFAULT_PLATFORM_PREFIX),
            ("mqtt_prefix", DEFAULT_PEER_PREFIX),
        ]:
            self.declare_parameter(key, value)
        get = lambda name: self.get_parameter(name).value
        self.robot, self.cluster = (
            identifier(get("robot_id")),
            identifier(get("cluster_id")),
        )
        self.prefix = get("mqtt_prefix").rstrip("/") + "/" + self.cluster
        self.platform = get("platform_prefix").rstrip("/")
        host = get("mqtt_host")
        root = runtime_path(self, "transport")
        self.lock = ProcessLock(root / "process.lock")
        self.audit = Audit(root, self.robot, self.get_logger())
        self.boot = uuid.uuid4().hex
        self.connected = False
        self.media_connected = False
        self.media_incoming = queue.Queue(maxsize=16)
        self.incoming = queue.Queue(maxsize=1024)
        self.inbox = self.create_publisher(String, "/swarm/network/inbound", 256)
        self.platform_in = self.create_publisher(String, "/swarm/platform/inbound", 32)
        self.status = self.create_publisher(String, "/swarm/network/state", 10)
        self.client = mqtt.Client(
            client_id=f"swarm-{self.cluster}-{self.robot}-{self.boot[:10]}",
            clean_session=True,
        )
        self.client.max_queued_messages_set(256)
        self.client.max_inflight_messages_set(20)
        self.media_client = mqtt.Client(
            client_id=f"swarm-media-{self.cluster}-{self.robot}-{self.boot[:10]}",
            clean_session=True,
        )
        self.media_client.max_queued_messages_set(4)
        self.media_client.max_inflight_messages_set(1)
        for client in (self.client, self.media_client):
            client.reconnect_delay_set(1, 10)
            if get("mqtt_username"):
                client.username_pw_set(get("mqtt_username"), get("mqtt_password"))
            if get("mqtt_tls"):
                client.tls_set(ca_certs=get("mqtt_ca") or None, cert_reqs=ssl.CERT_REQUIRED)
        self.media_client.on_connect = self.media_connect
        self.media_client.on_disconnect = lambda *args: setattr(
            self, "media_connected", False
        )
        self.media_client.on_message = self.message
        self.client.on_connect = self.connect
        self.client.on_disconnect = self.disconnect
        self.client.on_message = self.message
        self.create_subscription(String, "/swarm/network/outbound", self.outbound, 256)
        self.create_subscription(
            String, "/swarm/platform/outbound", self.platform_outbound, 100
        )
        self.create_timer(0.02, self.drain)
        self.create_timer(0.1, self.drain_media)
        self.create_timer(
            1.0,
            lambda: publish(
                self.status, {"connected": self.connected, "robot_id": self.robot}
            ),
        )
        self.client.connect_async(host, get("mqtt_port"), keepalive=15)
        self.client.loop_start()
        self.media_client.connect_async(host, get("mqtt_port"), keepalive=15)
        self.media_client.loop_start()

    def connect(self, client, userdata, flags, rc):
        self.connected = rc == 0
        self.audit.record("mqtt_connect", {"result": rc})
        if self.connected:
            for topic in [
                self.prefix + "/broadcast/+",
                self.prefix + "/to/" + self.robot + "/+",
                self.platform + "/services",
            ]:
                client.subscribe(topic, qos=1)

    def media_connect(self, client, userdata, flags, rc):
        self.media_connected = rc == 0
        if self.media_connected:
            for topic in (
                self.prefix + "/media/broadcast/+",
                self.prefix + "/media/to/" + self.robot + "/+",
            ):
                client.subscribe(topic, qos=1)

    def disconnect(self, client, userdata, rc):
        self.connected = False
        self.audit.record("mqtt_disconnect", {"result": rc})

    def message(self, client, userdata, msg):
        if len(msg.payload) > MAX_MESSAGE_BYTES or msg.retain:
            return
        try:
            target = self.media_incoming if "/media/" in msg.topic else self.incoming
            target.put_nowait((msg.topic, bytes(msg.payload)))
        except queue.Full:
            # Consensus and application outboxes repair loss; never block MQTT IO.
            pass

    def outbound(self, message):
        try:
            value = decode(message.data)
            wire = envelope(
                self.cluster,
                self.robot,
                self.boot,
                value["to"],
                value["kind"],
                value["data"],
                value.get("round_epoch", 0),
            )
            media = value["kind"].startswith("media_")
            prefix = self.prefix + ("/media" if media else "")
            topic = (
                prefix + "/broadcast/" + self.robot
                if value["to"] == "*"
                else prefix + "/to/" + identifier(value["to"]) + "/" + self.robot
            )
            if media:
                if self.media_connected:
                    self.media_client.publish(
                        topic, canonical(wire), qos=1, retain=False
                    )
            else:
                self.send(topic, wire)
        except Exception as error:
            self.get_logger().warning(f"协同发送拒绝：{type(error).__name__}")

    def platform_outbound(self, message):
        try:
            value = decode(message.data)
            channel = value["channel"]
            if channel not in {"events", "services_reply", "state", "heartbeat"}:
                raise ValueError("unsupported platform channel")
            topic = (
                heartbeat_topic(self.robot)
                if channel == "heartbeat"
                else self.platform + "/" + channel
            )
            self.send(topic, value["payload"])
        except Exception as error:
            self.get_logger().warning(f"平台发送拒绝：{type(error).__name__}")

    def send(self, topic, value):
        if self.connected:
            raw = canonical(value)
            size = len(raw.encode())
            if size > MAX_MESSAGE_BYTES:
                raise ValueError("message too large")
            self.client.publish(topic, raw, qos=1, retain=False)
            if value.get("kind") not in {"hello", "raft"} and not topic.endswith("/heartbeat"):
                data = value.get("data", {})
                if not isinstance(data, dict):
                    data = {}
                self.audit.record("mqtt_out", {
                    "topic": topic, "bytes": size, "kind": value.get("kind"),
                    "method": value.get("method"), "round_epoch": value.get("round_epoch", data.get("round_epoch", 0)),
                    "revision": value.get("plan_revision", data.get("plan_revision", 0)),
                })

    def drain_media(self):
        try:
            topic, raw = self.media_incoming.get_nowait()
        except queue.Empty:
            return
        try:
            value = decode(raw)
            validate_envelope(value, self.cluster, self.robot)
            if topic.rsplit("/", 1)[-1] != value["robot_id"] or not value[
                "kind"
            ].startswith("media_"):
                raise ValueError("invalid media source/topic")
            if value["robot_id"] != self.robot:
                publish(self.inbox, value)
        except Exception as error:
            self.get_logger().warning("图片复制拒绝：" + type(error).__name__)

    def drain(self):
        for _ in range(128):
            try:
                topic, raw = self.incoming.get_nowait()
            except queue.Empty:
                return
            try:
                value = decode(raw)
                if topic == self.platform + "/services":
                    publish(self.platform_in, value)
                    continue
                validate_envelope(value, self.cluster, self.robot)
                if topic.rsplit("/", 1)[-1] != value["robot_id"]:
                    raise ValueError("sender does not match ACL topic")
                if value["kind"].startswith("media_"):
                    raise ValueError("media must use separate transport")
                if value["robot_id"] != self.robot:
                    publish(self.inbox, value)
            except Exception as error:
                self.get_logger().warning(f"协同接收拒绝：{type(error).__name__}")

    def destroy_node(self):
        self.media_client.disconnect()
        self.media_client.loop_stop()
        self.client.disconnect()
        self.client.loop_stop()
        self.audit.close()
        self.lock.close()
        return super().destroy_node()


def main(args=None):
    main_node(TransportNode, args)
