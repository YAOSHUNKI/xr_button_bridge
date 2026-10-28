#!/usr/bin/env python3
"""
xr_button_bridge_node.py

Receive ALL XR controller inputs (buttons, triggers, grips, thumbsticks) from
teleop_hand_and_arm_button.py over UDP and republish them as ROS 2 topics.
Topic names and parameters are configured in a YAML file
(default: xr_button_bridge.yaml next to this script, override with --config).

Payload (little-endian, 42 bytes total): '<10?8f'
    bools  (10): L_trigger, L_squeeze, L_X, L_Y, L_thumbstick,
                 R_trigger, R_squeeze, R_A, R_B, R_thumbstick
    floats  (8): L_trigger(0..1), L_squeeze(0..1), L_stick_x, L_stick_y,
                 R_trigger(0..1), R_squeeze(0..1), R_stick_x, R_stick_y

Trigger on/off is NOT taken from the packet's bool; it is derived from the
analog trigger value with trigger.threshold (default 0.5) and optional hysteresis.

Published topics (names from YAML):
    buttons.*  std_msgs/Bool     edge-triggered (True once on press, False once on release)
    values.*   std_msgs/Float32  continuous at publish_hz
    joy.topic  sensor_msgs/Joy   continuous at publish_hz
If no packet arrives for `timeout` seconds, all inputs are released / zeroed.
"""

import argparse
import os
import socket
import struct
import sys
import time

import yaml
import rclpy
from rclpy.node import Node
from std_msgs.msg import Bool, Float32
from sensor_msgs.msg import Joy


FMT = '<10?8f'
SIZE = struct.calcsize(FMT)

# order == bool block of the payload == Joy.buttons
BUTTON_KEYS = [
    'left_trigger', 'left_squeeze', 'left_x', 'left_y', 'left_thumbstick',
    'right_trigger', 'right_squeeze', 'right_a', 'right_b', 'right_thumbstick',
]
# order == float block of the payload == Joy.axes
VALUE_KEYS = [
    'left_trigger', 'left_squeeze', 'left_stick_x', 'left_stick_y',
    'right_trigger', 'right_squeeze', 'right_stick_x', 'right_stick_y',
]
# button index -> value index used for threshold-based on/off
TRIGGER_MAP = {BUTTON_KEYS.index('left_trigger'):  VALUE_KEYS.index('left_trigger'),
               BUTTON_KEYS.index('right_trigger'): VALUE_KEYS.index('right_trigger')}

PACKAGE_NAME = 'xr_button_bridge'
CONFIG_NAME = 'xr_button_bridge.yaml'


def config_candidates():
    """Search order when --config is not given."""
    cands = []
    try:
        from ament_index_python.packages import get_package_share_directory
        share = get_package_share_directory(PACKAGE_NAME)
        cands += [os.path.join(share, 'config', CONFIG_NAME), os.path.join(share, CONFIG_NAME)]
    except Exception:
        pass
    here = os.path.dirname(os.path.realpath(__file__))
    cands += [os.path.join(here, CONFIG_NAME), os.path.join(os.getcwd(), CONFIG_NAME)]
    return cands


def _topics(entry):
    """YAML entry -> list of topic names (str / list / null)."""
    if entry is None:
        return []
    if isinstance(entry, str):
        return [entry] if entry.strip() else []
    if isinstance(entry, (list, tuple)):
        return [t for t in entry if isinstance(t, str) and t.strip()]
    raise ValueError(f'invalid topic entry: {entry!r}')


def load_config(path):
    with open(path, 'r') as f:
        cfg = yaml.safe_load(f) or {}
    for sect, keys in (('buttons', BUTTON_KEYS), ('values', VALUE_KEYS)):
        unknown = set((cfg.get(sect) or {}).keys()) - set(keys)
        if unknown:
            raise ValueError(f'unknown key(s) in "{sect}": {sorted(unknown)} (valid: {keys})')
    return cfg


class XrButtonBridge(Node):
    def __init__(self, cfg, host=None, port=None):
        super().__init__('xr_button_bridge')

        udp = cfg.get('udp') or {}
        host = host if host is not None else udp.get('host', '0.0.0.0')
        port = port if port is not None else int(udp.get('port', 9870))
        qos = int(cfg.get('qos_depth', 10))
        publish_hz = float(cfg.get('publish_hz', 30.0))
        self.timeout = float(cfg.get('timeout', 0.5))
        trig = cfg.get('trigger') or {}
        self.trig_on = float(trig.get('threshold', 0.5))
        self.trig_off = self.trig_on - float(trig.get('hysteresis', 0.0))

        btn_cfg = cfg.get('buttons') or {}
        val_cfg = cfg.get('values') or {}
        joy_cfg = cfg.get('joy') or {}

        # button index -> list of publishers
        self.pub_buttons = [[self.create_publisher(Bool, t, qos) for t in _topics(btn_cfg.get(k))]
                            for k in BUTTON_KEYS]
        self.pub_values = [[self.create_publisher(Float32, t, qos) for t in _topics(val_cfg.get(k))]
                           for k in VALUE_KEYS]
        joy_topics = _topics(joy_cfg.get('topic', '/xr/joy'))
        self.pub_joy = [self.create_publisher(Joy, t, qos) for t in joy_topics]
        self.joy_frame = str(joy_cfg.get('frame_id', 'xr_controller'))

        for k, pubs in zip(BUTTON_KEYS, self.pub_buttons):
            self.get_logger().info(f'button {k:17s} -> {[p.topic_name for p in pubs] or "(disabled)"}')
        for k, pubs in zip(VALUE_KEYS, self.pub_values):
            self.get_logger().info(f'value  {k:17s} -> {[p.topic_name for p in pubs] or "(disabled)"}')
        self.get_logger().info(f'joy                      -> {joy_topics or "(disabled)"}')

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.setblocking(False)
        self.get_logger().info(
            f'listening on UDP {host}:{port} (fmt={FMT}, size={SIZE}), '
            f'trigger on>={self.trig_on:.2f} off<{self.trig_off:.2f}, '
            f'{publish_hz:.1f} Hz, timeout {self.timeout:.2f}s'
        )

        self._buttons = [False] * len(BUTTON_KEYS)
        self._values = [0.0] * len(VALUE_KEYS)
        self._last_rx = None
        self._stale = True

        self.create_timer(0.005, self._poll)                # 200 Hz UDP drain + edges
        self.create_timer(1.0 / publish_hz, self._publish)  # continuous stream

    def _poll(self):
        latest = None
        while True:
            try:
                data, _ = self.sock.recvfrom(128)
            except BlockingIOError:
                break
            except Exception as e:
                self.get_logger().warn(f'recvfrom failed: {e}')
                break
            if len(data) != SIZE:
                self.get_logger().warn(
                    f'ignoring packet of unexpected size {len(data)} (expected {SIZE})',
                    throttle_duration_sec=2.0)
                continue
            latest = data

        now = time.monotonic()
        if latest is not None:
            raw = struct.unpack(FMT, latest)
            buttons = [bool(v) for v in raw[:10]]
            values = [float(v) for v in raw[10:]]
            # trigger on/off from analog value with threshold (+ hysteresis)
            for bi, vi in TRIGGER_MAP.items():
                thr = self.trig_off if self._buttons[bi] else self.trig_on
                buttons[bi] = values[vi] >= thr
            self._update(buttons, values)
            self._last_rx = now
            if self._stale:
                self.get_logger().info('receiving controller data')
                self._stale = False
        elif not self._stale and self._last_rx is not None and now - self._last_rx > self.timeout:
            self.get_logger().warn(f'no packet for {self.timeout:.2f}s -> releasing all inputs')
            self._update([False] * len(BUTTON_KEYS), [0.0] * len(VALUE_KEYS))
            self._stale = True

    def _update(self, buttons, values):
        # edge detection: publish only when a button state changes
        for i, (new, old) in enumerate(zip(buttons, self._buttons)):
            if new != old:
                msg = Bool(data=new)
                for pub in self.pub_buttons[i]:
                    pub.publish(msg)
                self.get_logger().info(f'{BUTTON_KEYS[i]} {"pressed" if new else "released"}')
        self._buttons = buttons
        self._values = values

    def _publish(self):
        if self.pub_joy:
            joy = Joy()
            joy.header.stamp = self.get_clock().now().to_msg()
            joy.header.frame_id = self.joy_frame
            joy.axes = list(self._values)
            joy.buttons = [int(b) for b in self._buttons]
            for pub in self.pub_joy:
                pub.publish(joy)
        for v, pubs in zip(self._values, self.pub_values):
            if pubs:
                msg = Float32(data=v)
                for pub in pubs:
                    pub.publish(msg)

    def destroy_node(self):
        try:
            self.sock.close()
        except Exception:
            pass
        super().destroy_node()


def main(argv=None):
    parser = argparse.ArgumentParser(description='XR controller (all inputs) UDP -> ROS 2 bridge')
    parser.add_argument('--config', type=str, default=None,
                        help='YAML config file (default: share/xr_button_bridge/config/'
                             'xr_button_bridge.yaml, then next to this script, then cwd)')
    parser.add_argument('--host', type=str, default=None, help='override udp.host in YAML')
    parser.add_argument('--port', type=int, default=None, help='override udp.port in YAML')
    args, ros_argv = parser.parse_known_args(argv if argv is not None else sys.argv[1:])

    if args.config is None:
        tried = config_candidates()
        args.config = next((c for c in tried if os.path.isfile(c)), None)
        if args.config is None:
            sys.exit('xr_button_bridge: config file not found. Tried:\n  ' + '\n  '.join(tried)
                     + '\nInstall it to share/xr_button_bridge/config/ or pass --config <path>.')
    cfg = load_config(args.config)

    rclpy.init(args=ros_argv)
    node = XrButtonBridge(cfg, host=args.host, port=args.port)
    node.get_logger().info(f'config: {os.path.abspath(args.config)}')
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        try:
            rclpy.shutdown()
        except Exception:
            pass


if __name__ == '__main__':
    main()

