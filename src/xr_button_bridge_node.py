#!/usr/bin/env python3
"""
xr_button_bridge_node.py

Receive XR-controller state from teleop_hand_and_arm_*.py over UDP and
republish it as ROS 2 topics inside this container.

Payload formats (little-endian, both accepted on the same port):
    2  bytes '<??'     : (right_ctrl_bButton [B],
                          left_ctrl_bButton  [Y])
    14 bytes '<fff??'  : (vx, vy, vyaw,
                          right_ctrl_bButton [B],
                          left_ctrl_bButton  [Y])

Published topics:
    /teleop/button/right_b   std_msgs/Bool     edge-triggered (existing)
    /teleop/button/left_y    std_msgs/Bool     edge-triggered (existing)
    /teleop/cmd_vel          geometry_msgs/Twist  latest stick value (new)
    /teleop/damp             std_msgs/Bool     True while both sticks pressed (new)

`/teleop/cmd_vel` is republished at a fixed rate (default 50 Hz) using the
most recent UDP sample, so the walking controller sees a steady stream even
if UDP packets are dropped. When no packet has arrived for --cmd-vel-timeout
seconds, zero velocity is published instead.
"""

import argparse
import socket
import struct
import sys
import time

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from std_msgs.msg import Bool
from geometry_msgs.msg import Twist


FMT_BTN     = '<??'      # 2 bytes  : B, Y
SIZE_BTN    = struct.calcsize(FMT_BTN)
FMT_FULL    = '<fff??'   # 14 bytes : vx, vy, vyaw, B, Y
SIZE_FULL   = struct.calcsize(FMT_FULL)


class XrButtonBridge(Node):
    def __init__(self, host, port,
                 cmd_vel_topic, cmd_vel_rate, cmd_vel_timeout,
                 cmd_vel_deadzone,
                 damp_hold_topic, qos_depth):
        super().__init__('xr_button_bridge')

        # ---- publishers ----
        # buttons: keep the existing behavior (edge-triggered, reliable)
        self.pub_right_b = self.create_publisher(Bool, '/right_button', qos_depth)
        self.pub_left_y  = self.create_publisher(Bool, '/left_button',  qos_depth)

        # cmd_vel: best-effort, small depth — a walking controller wants the
        # latest sample, not a backlog.
        cmd_qos = QoSProfile(depth=10,
                             reliability=ReliabilityPolicy.RELIABLE,
                             history=HistoryPolicy.KEEP_LAST)
        self.pub_cmd_vel = self.create_publisher(Twist, cmd_vel_topic, cmd_qos)
        self.pub_damp    = self.create_publisher(Bool,  damp_hold_topic, qos_depth)

        # ---- UDP socket ----
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.setblocking(False)
        self.get_logger().info(
            f'UDP listening on {host}:{port} '
            f'(accepts {SIZE_BTN}B [buttons] or {SIZE_FULL}B [stick+buttons])'
        )
        self.get_logger().info(
            f'publishing: {cmd_vel_topic} @ {cmd_vel_rate:.0f} Hz, '
            f'{damp_hold_topic}, /right_button, /left_button'
        )

        # ---- state ----
        self._prev_right_b = False
        self._prev_left_y  = False
        self._prev_damp    = False
        self._last_vx = 0.0
        self._last_vy = 0.0
        self._last_vyaw = 0.0
        self._last_rx_time = 0.0     # monotonic seconds; 0 = never
        self._cmd_vel_timeout = float(cmd_vel_timeout)
        self._cmd_vel_deadzone = float(cmd_vel_deadzone)
        self._was_moving = False

        # ---- timers ----
        self.create_timer(0.005, self._poll_udp)                     # 200 Hz recv
        self.create_timer(1.0 / cmd_vel_rate, self._publish_cmd_vel)  # steady rate

    # ------------------------------------------------------------------ #
    #  UDP intake                                                        #
    # ------------------------------------------------------------------ #
    def _poll_udp(self):
        latest = None
        while True:
            try:
                data, _ = self.sock.recvfrom(64)
            except BlockingIOError:
                break
            except Exception as e:
                self.get_logger().warn(f'recvfrom failed: {e}')
                break
            if len(data) not in (SIZE_BTN, SIZE_FULL):
                self.get_logger().warn(
                    f'ignoring packet of unexpected size {len(data)} '
                    f'(expected {SIZE_BTN} or {SIZE_FULL})')
                continue
            latest = data

        if latest is None:
            return

        if len(latest) == SIZE_FULL:
            vx, vy, vyaw, right_b, left_y = struct.unpack(FMT_FULL, latest)
            self._last_vx, self._last_vy, self._last_vyaw = float(vx), float(vy), float(vyaw)
            self._last_rx_time = time.monotonic()
        else:  # SIZE_BTN
            right_b, left_y = struct.unpack(FMT_BTN, latest)

        right_b = bool(right_b)
        left_y  = bool(left_y)

        # ---- buttons: edge-triggered publish (unchanged behavior) ----
        if right_b != self._prev_right_b:
            self.pub_right_b.publish(Bool(data=right_b))
            self.get_logger().info(f'B {"pressed" if right_b else "released"}')
            self._prev_right_b = right_b

        if left_y != self._prev_left_y:
            self.pub_left_y.publish(Bool(data=left_y))
            self.get_logger().info(f'Y {"pressed" if left_y else "released"}')
            self._prev_left_y = left_y

    # ------------------------------------------------------------------ #
    #  Steady cmd_vel republish                                          #
    # ------------------------------------------------------------------ #
    def _publish_cmd_vel(self):
        # Publish only while the stick is actually deflected (beyond the deadzone).
        # On the transition to rest (or on UDP timeout), publish ONE zero Twist so
        # the walking controller stops, then stay silent until the stick moves again.
        now = time.monotonic()
        stale = (self._last_rx_time == 0.0) or \
                (now - self._last_rx_time > self._cmd_vel_timeout)

        dz = self._cmd_vel_deadzone
        moving = (not stale) and (
            abs(self._last_vx)   > dz or
            abs(self._last_vy)   > dz or
            abs(self._last_vyaw) > dz
        )

        if moving:
            msg = Twist()
            msg.linear.x  = self._last_vx
            msg.linear.y  = self._last_vy
            msg.angular.z = self._last_vyaw
            self.pub_cmd_vel.publish(msg)
            self._was_moving = True
        elif self._was_moving:
            # one-shot zero on release / timeout
            self.pub_cmd_vel.publish(Twist())
            self._was_moving = False
        # else: idle — do not publish

    def destroy_node(self):
        try:
            self.sock.close()
        except Exception:
            pass
        super().destroy_node()


def main(argv=None):
    parser = argparse.ArgumentParser(description='XR controller UDP -> ROS 2 bridge')
    parser.add_argument('--host', type=str, default='0.0.0.0',
                        help='UDP bind host (0.0.0.0 = any interface)')
    parser.add_argument('--port', type=int, default=9870, help='UDP bind port')
    parser.add_argument('--cmd-vel-topic', type=str, default='/cmd_vel',
                        help='Twist topic name for stick-derived velocity')
    parser.add_argument('--cmd-vel-rate', type=float, default=50.0,
                        help='Republish rate for cmd_vel [Hz]')
    parser.add_argument('--cmd-vel-timeout', type=float, default=0.5,
                        help='Zero-out cmd_vel after this many seconds without a stick packet')
    parser.add_argument('--cmd-vel-deadzone', type=float, default=1e-3,
                        help='Stick magnitude threshold below which cmd_vel is NOT published (m/s or rad/s)')
    parser.add_argument('--damp-topic', type=str, default='/teleop/damp',
                        help='Bool topic pulsed when both sticks are pressed')
    parser.add_argument('--qos-depth', type=int, default=10, help='publisher QoS depth for buttons')
    args, ros_argv = parser.parse_known_args(argv if argv is not None else sys.argv[1:])

    rclpy.init(args=ros_argv)
    node = XrButtonBridge(host=args.host, port=args.port,
                          cmd_vel_topic=args.cmd_vel_topic,
                          cmd_vel_rate=args.cmd_vel_rate,
                          cmd_vel_timeout=args.cmd_vel_timeout,
                          cmd_vel_deadzone=args.cmd_vel_deadzone,
                          damp_hold_topic=args.damp_topic,
                          qos_depth=args.qos_depth)
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
