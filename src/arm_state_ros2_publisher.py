#!/usr/bin/env python3
"""
G1 arm joint-state publisher for rosbag2 recording.

Subscribes:  rt/lowstate  (Unitree DDS, hg_LowState)
Publishes:   /g1/arm_joint_states   sensor_msgs/msg/JointState

Usage (typical):
    # Terminal 1 - start teleop as usual
    python teleop/teleop_hand_and_arm.py --xr-mode=controller --arm=G1_29 ...

    # Terminal 2 - launch this bridge node
    python arm_state_ros2_publisher.py --network=<iface>

    # Terminal 3 - record
    ros2 bag record /g1/arm_joint_states -o arm_log

Notes:
- Uses the same DDS domain / iface as teleop; if teleop already runs on the
  robot's iface, pass the same one here (e.g. --network=eth0).
- Default publish rate is 200 Hz (rt/lowstate arrives at 500 Hz on G1).
- Default records the 14 arm joints (indices 15..28). Use --include-waist to
  also log waist yaw/roll/pitch (indices 12..14, total 17 joints); use
  --all to log every motor (35).
"""
import argparse
import threading
import time
from enum import IntEnum

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import JointState

from unitree_sdk2py.core.channel import (
    ChannelSubscriber,
    ChannelFactoryInitialize,
)
from unitree_sdk2py.idl.unitree_hg.msg.dds_ import LowState_ as hg_LowState


# ---- Joint tables (matches teleop/robot_control/robot_arm.py) ---------------

class G1_29_JointIndex(IntEnum):
    kLeftHipPitch = 0;  kLeftHipRoll = 1;  kLeftHipYaw = 2
    kLeftKnee = 3;      kLeftAnklePitch = 4; kLeftAnkleRoll = 5
    kRightHipPitch = 6; kRightHipRoll = 7; kRightHipYaw = 8
    kRightKnee = 9;     kRightAnklePitch = 10; kRightAnkleRoll = 11
    kWaistYaw = 12;     kWaistRoll = 13;  kWaistPitch = 14
    kLeftShoulderPitch = 15; kLeftShoulderRoll = 16; kLeftShoulderYaw = 17
    kLeftElbow = 18;    kLeftWristRoll = 19; kLeftWristPitch = 20; kLeftWristYaw = 21
    kRightShoulderPitch = 22; kRightShoulderRoll = 23; kRightShoulderYaw = 24
    kRightElbow = 25;   kRightWristRoll = 26; kRightWristPitch = 27; kRightWristYaw = 28


ARM_IDS = list(range(15, 29))                       # 14 joints
WAIST_ARM_IDS = list(range(12, 29))                 # 17 joints (waist + arms)
ALL_IDS = list(range(0, 35))                        # every motor slot

NAMES = {j.value: j.name.lstrip("k") for j in G1_29_JointIndex}


# ---- DDS subscription -------------------------------------------------------

class LowStateBuffer:
    def __init__(self):
        self.lock = threading.Lock()
        self.q = None      # np-free: python list
        self.stamp_ns = 0

    def set(self, q, stamp_ns):
        with self.lock:
            self.q = q
            self.stamp_ns = stamp_ns

    def get(self):
        with self.lock:
            return self.q, self.stamp_ns


def start_dds_subscriber(buf: LowStateBuffer, ids):
    sub = ChannelSubscriber("rt/lowstate", hg_LowState)
    sub.Init()

    def _spin():
        while True:
            msg = sub.Read()
            if msg is not None:
                q  = [float(msg.motor_state[i].q)  for i in ids]
                buf.set(q, time.time_ns())
            time.sleep(0.001)

    t = threading.Thread(target=_spin, daemon=True)
    t.start()
    return t


# ---- ROS 2 node -------------------------------------------------------------

class ArmStatePublisher(Node):
    def __init__(self, buf: LowStateBuffer, ids, rate_hz: float, topic: str):
        super().__init__("g1_arm_state_publisher")
        self.buf = buf
        self.ids = ids
        self.names = [NAMES[i] for i in ids]
        self.pub = self.create_publisher(JointState, topic, 50)
        self.timer = self.create_timer(1.0 / rate_hz, self._on_timer)
        self.get_logger().info(
            f"Publishing {len(ids)} joints on '{topic}' at {rate_hz:g} Hz"
        )

    def _on_timer(self):
        q, stamp_ns = self.buf.get()
        if q is None:
            return
        msg = JointState()
        msg.header.stamp.sec = stamp_ns // 1_000_000_000
        msg.header.stamp.nanosec = stamp_ns % 1_000_000_000
        msg.name = self.names
        msg.position = q
        self.pub.publish(msg)


# ---- main -------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--network", default="",
                   help="Network interface for Unitree DDS "
                        "(e.g. eth0). Empty = default / already initialized.")
    p.add_argument("--topic", default="/g1/arm_joint_states")
    p.add_argument("--rate", type=float, default=200.0,
                   help="ROS publish rate in Hz")
    grp = p.add_mutually_exclusive_group()
    grp.add_argument("--include-waist", action="store_true",
                     help="Include waist (3) + arms (14) = 17 joints")
    grp.add_argument("--all", action="store_true",
                     help="Include all 35 motors")
    args = p.parse_args()

    if args.all:
        ids = ALL_IDS
    elif args.include_waist:
        ids = WAIST_ARM_IDS
    else:
        ids = ARM_IDS

    # Init DDS (safe to call once per process; teleop runs in its own process)
    if args.network:
        ChannelFactoryInitialize(0, args.network)
    else:
        ChannelFactoryInitialize(0)

    buf = LowStateBuffer()
    start_dds_subscriber(buf, ids)

    # Wait until lowstate arrives so first published msg is valid
    t0 = time.time()
    while buf.get()[0] is None:
        if time.time() - t0 > 5.0:
            print("[warn] no rt/lowstate received yet; continuing anyway.")
            break
        time.sleep(0.05)

    rclpy.init()
    node = ArmStatePublisher(buf, ids, args.rate, args.topic)
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()

