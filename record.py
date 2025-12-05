#!/usr/bin/env python3
import argparse
import datetime
import os
import subprocess
import sys
from pathlib import Path

import rclpy
from rclpy.node import Node
import yaml


class TopicChecker(Node):
    def __init__(self):
        super().__init__("topic_checker")

    def get_available_topics(self) -> set[str]:
        # Returns a set of topic names currently in the graph
        return {name for name, _ in self.get_topic_names_and_types()}


def load_topics_from_yaml(path: Path) -> list[str]:
    if not path.is_file():
        print(f"[ERROR] Topics file not found: {path}")
        sys.exit(1)

    with path.open("r") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict) or "topics" not in data:
        print("[ERROR] YAML must have top-level key 'topics'")
        sys.exit(1)

    topics = []

    def _collect(node):
        if isinstance(node, dict):
            for _, v in node.items():
                _collect(v)
        elif isinstance(node, list):
            for item in node:
                _collect(item)
        elif isinstance(node, str):
            topics.append(node)
        elif node is None:
            return
        else:
            print(f"[WARN] Ignoring unsupported entry in YAML: {node!r}")

    _collect(data["topics"])

    # Remove duplicates and sort
    return sorted(set(topics))


def check_topics(desired: list[str], available: set[str]):
    present = sorted(set(desired) & available)
    missing = sorted(set(desired) - available)
    return present, missing


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check required ROS 2 topics and record ros2 bag."
    )
    parser.add_argument(
        "-t",
        "--topics-file",
        type=str,
        default="topics.yaml",
        help="Path to YAML file listing topics (default: topics.yaml)",
    )
    parser.add_argument(
        "--bag-root",
        type=str,
        default=os.environ.get("BAG_DIR", "."),
        help="Directory in which to create the bag (can be on SSD). "
             "Default: $BAG_DIR or current directory.",
    )
    parser.add_argument(
        "--bag-name",
        type=str,
        default=None,
        help="Bag name (subdirectory). Default: bag_YYYYmmdd_HHMMSS",
    )
    parser.add_argument(
        "--no-confirm",
        action="store_true",
        help="Do not prompt even if topics are missing; record only present topics.",
    )

    args = parser.parse_args()

    topics_file = Path(args.topics_file)
    desired_topics = load_topics_from_yaml(topics_file)

    if not desired_topics:
        print("[ERROR] No topics found in YAML.")
        return 1

    print("=== Desired topics ===")
    for t in desired_topics:
        print(f"  {t}")
    print("======================")

    rclpy.init()
    node = TopicChecker()

    try:
        available_topics = node.get_available_topics()
    finally:
        node.destroy_node()
        rclpy.shutdown()

    present, missing = check_topics(desired_topics, available_topics)

    print("\n=== Topic check ===")
    print(f"Total desired: {len(desired_topics)}")
    print(f"Present      : {len(present)}")
    print(f"Missing      : {len(missing)}")

    if present:
        print("\nPresent topics:")
        for t in present:
            print(f"  [OK]  {t}")
    else:
        print("\n[ERROR] None of the desired topics are currently available.")

    if missing:
        print("\nMissing topics:")
        for t in missing:
            print(f"  [MISS] {t}")

    if not present:
        print("\nNothing to record, exiting.")
        return 1

    if missing and not args.no-confirm:
        ans = input(
            "\nSome topics are missing. Record bag with present topics only? [y/N]: "
        ).strip().lower()
        if ans not in ("y", "yes"):
            print("Aborting without recording.")
            return 1

    # Determine bag directory / name
    bag_root = Path(args.bag_root).expanduser()
    bag_root.mkdir(parents=True, exist_ok=True)

    if args.bag_name is None:
        now = datetime.datetime.now()
        bag_name = now.strftime("bag_%Y%m%d_%H%M%S")
    else:
        bag_name = args.bag_name

    bag_path = bag_root / bag_name

    cmd = ["ros2", "bag", "record", "-o", str(bag_path)]
    cmd.extend(present)

    print("\n=== ros2 bag command ===")
    print(" ".join(cmd))
    print("\nRecording will run until you press Ctrl+C.\n")

    try:
        # Propagate ROS environment into the child process automatically
        return subprocess.call(cmd)
    except KeyboardInterrupt:
        # ros2 bag should handle SIGINT itself, but just in case:
        print("\nStopping recording...")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

