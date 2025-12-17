#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

"""
Adds the capacity to add ASM migration commands:

- the tool will execute different ASM CLUSTER MIGRATION IMPORT commands to different shards to force the topology of the Cluster into a state.

- This will be a first milestone to have some data of the behavior and performance under some ASM migration events.

- The CONFIG now accepts a new entry `asm_cluster_state`.

- The  `asm_cluster_state` is represented as a list of list of slot ranges:

[[List of ranges that wants to be ensured in shard0], [List of ranged that wants to be ensured in shard1] , etc ...]]

The engine will not touch the shards or slot ranges not mentioned in this state. And will ignore the shards that do not exist in the real cluster (like if a list of 4 is given and the cluster is of 3, the 4th entry will be ignored).

"""

import logging
import time
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import redis


class ShardSlotInfo:
    """
    Helper that fetches and stores cluster topology information using
    CLUSTER SHARDS and CLUSTER SLOTS.

    - Exposes shards as an ordered list (same order as CLUSTER SHARDS).
    - Provides a method to resolve shard index -> "ip:port" of its master.
    - Also loads CLUSTER SLOTS so you can extend this to map slots to shards
      if you want more validation / routing logic.
    """

    def __init__(self, conn: redis.Redis):
        self.conn = conn
        self.shards: List[Dict[str, Any]] = []
        self._load()

    def _load(self) -> None:
        # Redis 7+ : CLUSTER SHARDS returns a list of dicts describing each shard.
        # Example (simplified):
        # [
        #   {
        #     "id": "...",
        #     "slots": [[0, 5460]],
        #     "nodes": [
        #       {"id": "...", "role": "master", "ip": "10.0.0.1", "port": 7000, ...},
        #       {"id": "...", "role": "replica", ...},
        #     ],
        #   },
        #   ...
        # ]
        logging.info("Loading cluster topology information")
        shards = self.conn.execute_command("CLUSTER", "SHARDS")
        self.shards = [self._normalize_shard(shard) for shard in shards]
        logging.info("Cluster topology information loaded")

    @staticmethod
    def _b2s(x):
        return x.decode("utf-8") if isinstance(x, bytes) else x

    def _normalize_shard(self, shard) -> Dict[str, Any]:
        # shard is usually a dict-like object
        # Normalize keys and string values
        if isinstance(shard, dict):
            norm = {}
            for k, v in shard.items():
                key = self._b2s(k)
                if key == "nodes":
                    norm[key] = [
                        {
                            self._b2s(nk): (
                                self._b2s(nv) if isinstance(nv, (bytes, str)) else nv
                            )
                            for nk, nv in node.items()
                        }
                        for node in v
                    ]
                elif key == "slots":
                    # slots is normally a list of [start, end] pairs; leave as-is
                    norm[key] = v
                else:
                    norm[key] = self._b2s(v) if isinstance(v, (bytes, str)) else v
            return norm
        elif isinstance(shard, list):
            # Handle list-based format from Redis:
            # [b'slots', [5461, 10922], b'nodes', [[b'id', b'...', b'port', 6380, ...]], ...]
            # Convert to dict format
            norm = {}
            i = 0
            while i < len(shard):
                key = self._b2s(shard[i])
                if i + 1 < len(shard):
                    value = shard[i + 1]

                    if key == "nodes":
                        # nodes is a list of lists: [[b'id', b'...', b'port', 6380, ...], ...]
                        norm[key] = []
                        for node_list in value:
                            # Each node is a flat list: [b'key1', value1, b'key2', value2, ...]
                            node_dict = {}
                            j = 0
                            while j < len(node_list):
                                node_key = self._b2s(node_list[j])
                                if j + 1 < len(node_list):
                                    node_value = node_list[j + 1]
                                    node_dict[node_key] = (
                                        self._b2s(node_value)
                                        if isinstance(node_value, (bytes, str))
                                        else node_value
                                    )
                                j += 2
                            norm[key].append(node_dict)
                    elif key == "slots":
                        # slots is a list like [start, end] or [[start1, end1], [start2, end2]]
                        # Normalize to list of [start, end] pairs
                        if value and isinstance(value[0], int):
                            # Single range: [start, end]
                            norm[key] = [value]
                        else:
                            # Multiple ranges: [[start1, end1], [start2, end2]]
                            norm[key] = value
                    else:
                        # Other keys: just decode if bytes
                        norm[key] = (
                            self._b2s(value)
                            if isinstance(value, (bytes, str))
                            else value
                        )
                i += 2
            return norm

    def master_address_by_shard_index(self, shard_index: int) -> str:
        """
        Returns "<ip>:<port>" for the master node of the shard at the given index.
        The index is the position in the CLUSTER SHARDS response.
        """
        if shard_index < 0 or shard_index >= len(self.shards):
            raise IndexError(
                f"Shard index {shard_index} out of range (0..{len(self.shards) - 1})"
            )

        shard = self.shards[shard_index]
        nodes = shard.get("nodes", [])

        master = None
        for node in nodes:
            if node.get("role") == "master":
                master = node
                break

        if master is None:
            raise RuntimeError(f"No master node found for shard index {shard_index}")

        ip = master.get("ip")
        port = master.get("port")
        return f"{ip}:{port}"


@dataclass
class SlotRange:
    start: int
    end: int


@dataclass
class ASMCommandExecute:
    ranges: List[SlotRange]
    import_addr: str  # "ip:port"
    task_id: Optional[int] = None
    shard_conn: Optional[redis.Redis] = None

    def execute(self) -> int:
        flat_slots = [
            item
            for slot_range in self.ranges
            for item in [slot_range.start, slot_range.end]
        ]
        cmd = ["CLUSTER", "MIGRATION", "IMPORT", *flat_slots]
        logging.info("Executing ASM Command: {}".format(cmd))
        self.shard_conn = redis.Redis(
            host=self.import_addr.split(":")[0],
            port=int(self.import_addr.split(":")[1]),
        )
        task_id_raw = self.shard_conn.execute_command(*cmd)
        self.task_id = (
            task_id_raw.decode("utf-8")
            if isinstance(task_id_raw, bytes)
            else str(task_id_raw)
        )
        logging.info(f"Task ID: {self.task_id}")
        return self.task_id

    def wait_for_completion(self) -> None:
        assert self.task_id is not None, "Task ID is not set"
        assert self.shard_conn is not None, "Shard connection is not set"
        logging.info(f"Waiting for task {self.task_id} to complete")
        it = 0
        while True:
            status_response = self.shard_conn.execute_command(
                "CLUSTER", "MIGRATION", "STATUS", "ID", self.task_id
            )
            status_response = status_response[0]
            for i in range(0, len(status_response)):
                if status_response[i].decode("utf-8") == "state":
                    if status_response[i + 1].decode("utf-8") in [
                        "completed",
                        "done",
                        "finished",
                    ]:
                        logging.info(f"Task {self.task_id} completed")
                        logging.info(f"Task {self.task_id} completed")
                        return
                    else:
                        break
            time.sleep(0.1)
            it += 1
            if it % 10 == 0:
                logging.info(f"Task {self.task_id} still running")


@dataclass
class ASMCommand:
    ranges: List[SlotRange]
    import_node: int
    task_id: Optional[int] = None  # index into ShardSlotInfo.shards
    asm_command_executes: Optional[ASMCommandExecute] = None

    def to_execute(self, shard_slot_info: ShardSlotInfo) -> List[ASMCommandExecute]:
        """
        Use shard_slot_info to map the integer shard indices (target_node and
        destination_node) into "ip:port" strings, and return a list of
        ASMCommandExcute objects.

        You could return multiple commands if you later decide to split ranges,
        but for now this returns a single ASMCommandExcute mirroring `self`.
        """
        import_addr = shard_slot_info.master_address_by_shard_index(self.import_node)

        return ASMCommandExecute(
            ranges=self.ranges,
            import_addr=import_addr,
        )

    def wait_for_completion(self) -> None:
        assert self.asm_command_execute is not None, "ASMCommandExecute is not set"
        self.asm_command_execute.wait_for_completion()

    def execute(
        self, shard_slot_info: ShardSlotInfo, wait_for_completion: bool = True
    ) -> None:
        self.asm_command_execute: ASMCommandExecute = self.to_execute(shard_slot_info)
        self.task_id = self.asm_command_execute.execute()
        if wait_for_completion:
            self.wait_for_completion()


@dataclass
class ClusterState:
    shards: List[List[SlotRange]]

    def to_asm_commands(
        self, current_shards_info: ShardSlotInfo
    ) -> Dict[int, Dict[int, ASMCommand]]:
        """
        Compare current cluster state with desired state and generate ASM commands
        to migrate slots to reach the target configuration.
        Returns: Dict[dest_shard, Dict[origin_shard, ASMCommand]]
        """
        asm_commands = {}
        num_shards = len(self.shards)
        current_num_shards = len(current_shards_info.shards)

        # Get current slot distribution for each shard
        current_shard_slots = [set() for _ in range(current_num_shards)]

        for shard_idx, shard in enumerate(current_shards_info.shards):
            slot_ranges = shard.get("slots", [])
            if slot_ranges:
                slots_lists = slot_ranges[0] if slot_ranges else []
                for i in range(0, len(slots_lists), 2):
                    start = slots_lists[i]
                    end = slots_lists[i + 1]
                    for slot in range(start, end + 1):
                        current_shard_slots[shard_idx].add(slot)

        # Convert desired state to slot sets (only for shards that exist)
        max_shards = min(num_shards, current_num_shards)
        desired_shard_slots = [set() for _ in range(max_shards)]
        for shard_idx, slot_ranges in enumerate(self.shards):
            # Skip desired shards that don't exist in current cluster
            if shard_idx >= current_num_shards:
                continue
            for slot_range in slot_ranges:
                slot_range = (
                    SlotRange(**slot_range)
                    if isinstance(slot_range, dict)
                    else slot_range
                )
                for slot in range(slot_range.start, slot_range.end + 1):
                    desired_shard_slots[shard_idx].add(slot)

        # Find slots that need to be migrated
        for dest_shard in range(max_shards):
            slots_needed = (
                desired_shard_slots[dest_shard] - current_shard_slots[dest_shard]
            )

            if slots_needed:
                # Group slots by their current owner
                slots_by_origin = {}
                for origin_shard in range(current_num_shards):
                    if origin_shard != dest_shard:
                        slots_from_origin = (
                            slots_needed & current_shard_slots[origin_shard]
                        )
                        if slots_from_origin:
                            slots_by_origin[origin_shard] = slots_from_origin

                # Create ASM commands for each origin shard
                if slots_by_origin:
                    asm_commands[dest_shard] = {}
                    for origin_shard, slots_to_migrate in slots_by_origin.items():
                        sorted_slots = sorted(slots_to_migrate)
                        ranges = []
                        start = sorted_slots[0]
                        end = start

                        # Consolidate consecutive slots into ranges
                        for slot in sorted_slots[1:]:
                            if slot == end + 1:
                                end = slot
                            else:
                                ranges.append(SlotRange(start, end))
                                start = end = slot
                        ranges.append(SlotRange(start, end))

                        asm_command = ASMCommand(
                            ranges=ranges,
                            import_node=dest_shard,
                        )
                        asm_commands[dest_shard][origin_shard] = asm_command

        return asm_commands


def execute_asm_commands(benchmark_config, r, dbconfig_keyname="dbconfig"):
    cluster_state = None
    res = 0
    if dbconfig_keyname in benchmark_config:
        dbconfig = benchmark_config[dbconfig_keyname]
        # Handle both dict and list formats
        if isinstance(dbconfig, dict):
            # New format: dbconfig is a dict
            if "asm_cluster_state" in dbconfig:
                cluster_state = dbconfig["asm_cluster_state"]
        elif isinstance(dbconfig, list):
            # Old format: dbconfig is a list of dicts
            for k in dbconfig:
                if isinstance(k, dict) and "asm_cluster_state" in k:
                    cluster_state = k["asm_cluster_state"]

    asm_commands = {}
    if cluster_state is not None:
        logging.info("ASM Cluster State detected. Preparing ASM commands")
        shards_info = ShardSlotInfo(r)
        if isinstance(cluster_state, str) and cluster_state == "SPARSE":
            c_state = ClusterState(
                shards=[
                    [
                        SlotRange(start=i, end=i) for i in range(0, 16384, 2)
                    ],  # Shard 0: all even slots
                    [
                        SlotRange(start=i, end=i) for i in range(1, 16384, 2) if i != 1
                    ],  # Shard 1: all odd slots except 1
                ]
            )
            logging.info("Cluster State: {}".format(c_state))
            asm_commands = c_state.to_asm_commands(shards_info)
        elif isinstance(cluster_state, dict):
            c_state = ClusterState(**cluster_state)
            logging.info("Cluster State: {}".format(c_state))
            asm_commands = c_state.to_asm_commands(shards_info)

    num_commands = sum(
        len(origin_command_map) for origin_command_map in asm_commands.values()
    )
    logging.info(f"Executing {num_commands} ASM commands")
    for dest_shard, origin_command_map in asm_commands.items():
        for origin_shard, asm_command in origin_command_map.items():
            logging.info(
                f"Executing ASM Command to migrate from shard {origin_shard} to shard {dest_shard}"
            )
            asm_command.execute(shards_info, wait_for_completion=True)
    logging.info("ASM commands completed. The Cluster should be in the desired state.")
    return res


if __name__ == "__main__":
    r = redis.Redis(host="localhost", port=6379, db=0)
    benchmark_config = {
        "dbconfig": {
            "asm_cluster_state": {
                "shards": [
                    [
                        {"start": 0, "end": 100},
                    ],
                    [{"start": 14000, "end": 15000}, {"start": 5461, "end": 10922}],
                ]
            }
        }
    }
    benchmark_config = {
        "dbconfig": {
            "asm_cluster_state": "SPARSE",
        }
    }
    execute_asm_commands(benchmark_config, r)
