import logging
import time
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

import redis


@dataclass
class SlotRange:
    start: int
    end: int


@dataclass
class ASMCommandExecute:
    ranges: List[SlotRange]
    import_addr: str # "ip:port"
    task_id: Optional[int] = None

    def execute(self, r: redis.Redis) -> int:
        cmd = ["CLUSTER", "IMPORT", self.import_addr, *self.ranges]
        logging.info(
            "Executing ASM Command: {}".format(cmd)
        )
        self.task_id = r.execute_command(" ".join(cmd))
        return self.task_id


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
        shards = self.conn.execute_command("CLUSTER", "SHARDS")

        # Redis might return bytes for keys/values; normalize to str where convenient
        self.shards = [self._normalize_shard(shard) for shard in shards]

    @staticmethod
    def _b2s(x):
        return x.decode("utf-8") if isinstance(x, bytes) else x

    def _normalize_shard(self, shard) -> Dict[str, Any]:
        # shard is usually a dict-like object
        # Normalize keys and string values
        norm = {}
        for k, v in shard.items():
            key = self._b2s(k)
            if key == "nodes":
                norm[key] = [
                    {
                        self._b2s(nk): self._b2s(nv) if isinstance(nv, (bytes, str)) else nv
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

    def master_address_by_shard_index(self, shard_index: int) -> str:
        """
        Returns "<ip>:<port>" for the master node of the shard at the given index.
        The index is the position in the CLUSTER SHARDS response.
        """
        if shard_index < 0 or shard_index >= len(self.shards):
            raise IndexError(f"Shard index {shard_index} out of range (0..{len(self.shards)-1})")

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

def get_num_shards(r: redis.Redis) -> int:
    """
    Returns the number of shards in the cluster.
    """
    return len(r.execute_command("CLUSTER", "SHARDS"))


@dataclass
class ASMCommand:
    ranges: List[SlotRange]
    import_node: int
    task_id: Optional[int] = None # index into ShardSlotInfo.shards

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

    def wait_for_completion(self, r: redis.Redis) -> None:
        assert self.task_id is not None, "Task ID is not set"
        while True:
            status = r.execute_command("CLUSTER", "IMPORT", "STATUS", self.task_id)
            if status == "DONE":
                break
            time.sleep(0.1)

    def execute(self, r: redis.Redis, shard_slot_info: ShardSlotInfo, wait_for_completion: bool = True) -> None:
        asm_command_execute: ASMCommandExecute = self.to_execute(shard_slot_info)
        self.task_id = asm_command_execute.execute(r)
        if wait_for_completion:
            self.wait_for_completion(r)


def execute_asm_sparse_command(r: redis.Redis, shards_info: ShardSlotInfo) -> int:
    # Get current slot distribution for each shard
    shard_0_slots = set()
    shard_1_slots = set()

    for shard_idx, shard in enumerate(shards_info.shards):
        slot_ranges = shard.get("slots", [])
        for slot_range in slot_ranges:
            start, end = slot_range
            for slot in range(start, end + 1):
                if shard_idx == 0:
                    shard_0_slots.add(slot)
                else:
                    shard_1_slots.add(slot)

    # Define target distribution: shard 0 gets even slots, shard 1 gets odd slots
    target_even_slots = set(range(0, 16384, 2))  # 0, 2, 4, 6, ...
    target_odd_slots = set(range(1, 16384, 2))   # 1, 3, 5, 7, ...

    # Find slots that need to be migrated
    # Shard 0 should give up odd slots to shard 1
    odd_slots_to_migrate_from_0 = shard_0_slots & target_odd_slots
    # Shard 1 should give up even slots to shard 0
    even_slots_to_migrate_from_1 = shard_1_slots & target_even_slots

    # Create ASM commands for migrations
    asm_commands = []

    # Migrate odd slots from shard 0 to shard 1
    if odd_slots_to_migrate_from_0:
        sorted_slots = sorted(odd_slots_to_migrate_from_0)
        ranges = []
        start = sorted_slots[0]
        end = start

        for slot in sorted_slots[1:]:
            if slot == end + 1:
                end = slot
            else:
                ranges.append(SlotRange(start, end))
                start = end = slot
        ranges.append(SlotRange(start, end))

        asm_command = ASMCommand(
            ranges=ranges,
            import_node=1,
        )
        logging.info("SPARSE ASM Command: {}".format(asm_command))

        asm_commands.append(asm_command)

    # Migrate even slots from shard 1 to shard 0
    if even_slots_to_migrate_from_1:
        sorted_slots = sorted(even_slots_to_migrate_from_1)
        ranges = []
        start = sorted_slots[0]
        end = start

        for slot in sorted_slots[1:]:
            if slot == end + 1:
                end = slot
            else:
                ranges.append(SlotRange(start, end))
                start = end = slot
        ranges.append(SlotRange(start, end))

        asm_command = ASMCommand(
            ranges=ranges,
            import_node=0,
        )
        logging.info("SPARSE ASM Command: {}".format(asm_command))

        asm_commands.append(asm_command)

    # Execute the migration commands
    for asm_command in asm_commands:
        asm_command.execute(r, shards_info, wait_for_completion=True)

def execute_asm_commands(benchmark_config, r, dbconfig_keyname="dbconfig"):
    cmds = None
    res = 0
    if dbconfig_keyname in benchmark_config:
        dbconfig = benchmark_config[dbconfig_keyname]
        # Handle both dict and list formats
        if isinstance(dbconfig, dict):
            # New format: dbconfig is a dict
            if "asm_commands" in dbconfig:
                cmds = dbconfig["asm_commands"]
        elif isinstance(dbconfig, list):
            # Old format: dbconfig is a list of dicts
            for k in dbconfig:
                if isinstance(k, dict) and "asm_commands" in k:
                    cmds = k["asm_commands"]

    asm_commands = []
    shards_info = ShardSlotInfo(r)
    if cmds is not None:
        for cmd in cmds:
            if isinstance(cmd, str) and cmd == "SPARSE":
                assert get_num_shards(r) == 2, "Only 2 shards are supported when using the SPARSE ASM command"
                execute_asm_sparse_command(r, shards_info)
            elif isinstance(cmd, dict):
                asm_command = ASMCommand(**cmd)
                logging.info(
                    "ASM Command: {}".format(asm_command)
                )
                asm_commands.append(asm_command)
                asm_command.execute(r, shards_info)

    logging.info("ASM commands executed")

    logging.info("Waiting for ASM commands to complete")
    for asm_command in asm_commands:
        asm_command.wait_for_completion(r)
    return res
