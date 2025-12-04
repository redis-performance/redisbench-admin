import logging
import time
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import random
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
    shard_conn: Optional[redis.Redis] = None

    def execute(self) -> int:
        flat_slots = [item for slot_range in self.ranges for item in [slot_range.start, slot_range.end]]
        cmd = ["CLUSTER", "MIGRATION", "IMPORT", *flat_slots]
        logging.info(
            "Executing ASM Command: {}".format(cmd)
        )
        self.shard_conn = redis.Redis(host=self.import_addr.split(":")[0], port=int(self.import_addr.split(":")[1]))
        task_id_raw = self.shard_conn.execute_command(*cmd)
        self.task_id = task_id_raw.decode('utf-8') if isinstance(task_id_raw, bytes) else str(task_id_raw)
        print(f"Task ID: {self.task_id}")
        return self.task_id

    def wait_for_completion(self) -> None:
        assert self.task_id is not None, "Task ID is not set"
        assert self.shard_conn is not None, "Shard connection is not set"
        while True:
            status_response = self.shard_conn.execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", self.task_id)
            status_response = status_response[0]
            for i in range(0, len(status_response)):
                if status_response[i].decode('utf-8') == 'state':
                    if status_response[i + 1].decode('utf-8') in ['completed', 'done', 'finished']:
                        logging.info(f"Task {self.task_id} completed")
                        print(f"Task {self.task_id} completed")
                        return
                    else:
                        break
            time.sleep(0.1)



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
        self.shards = [self._normalize_shard(shard) for shard in shards]

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
                                    node_dict[node_key] = self._b2s(node_value) if isinstance(node_value, (bytes, str)) else node_value
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
                        norm[key] = self._b2s(value) if isinstance(value, (bytes, str)) else value
                i += 2
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

    def execute(self, shard_slot_info: ShardSlotInfo, wait_for_completion: bool = True) -> None:
        self.asm_command_execute: ASMCommandExecute = self.to_execute(shard_slot_info)
        self.task_id = self.asm_command_execute.execute()
        if wait_for_completion:
            self.wait_for_completion()


def execute_asm_sparse_migration(shards_info: ShardSlotInfo) -> int:
    """
    Redistributes hashslots across shards so that:
    - Even hashslots (0, 2, 4, ...) go to the first shard (index 0)
    - Odd hashslots (1, 3, 5, ...) go to the second shard (index 1)
    - All other shards (if any) give up their slots to shards 0 and 1

    Works with any number of shards >= 2.
    """
    num_shards = len(shards_info.shards)
    if num_shards < 2:
        raise ValueError(f"SPARSE command requires at least 2 shards, but found {num_shards}")

    # Get current slot distribution for each shard
    shard_slots = [set() for _ in range(num_shards)]

    for shard_idx, shard in enumerate(shards_info.shards):
        slot_ranges = shard.get("slots", [])
        slots_lists = slot_ranges[0]
        for i in range(0, len(slots_lists), 2):
            start = slots_lists[i]
            end = slots_lists[i + 1]
            for slot in range(start, end + 1):
                shard_slots[shard_idx].add(slot)

    # Define target distribution: shard 0 gets even slots, shard 1 gets odd slots
    target_even_slots = set(range(0, 16384, 2))  # 0, 2, 4, 6, ...
    target_odd_slots = set(range(1, 16384, 2))   # 1, 3, 5, 7, ...

    # Group migrations by destination shard -> origin shard
    migrations_by_destination = {}  # dest_shard -> {origin_shard: set_of_slots}

    for origin_shard in range(num_shards):
        current_slots = shard_slots[origin_shard]
        # Keep 2 random slots to ensure shard remains alive (with at least 1 hashslot)
        slots_to_keep = set(random.sample(list(current_slots), 2))
        current_slots = current_slots - slots_to_keep

        if origin_shard == 0:
            # Shard 0 should give up odd slots to shard 1
            odd_slots_to_give = current_slots & target_odd_slots
            if odd_slots_to_give:
                if 1 not in migrations_by_destination:
                    migrations_by_destination[1] = {}
                migrations_by_destination[1][origin_shard] = odd_slots_to_give

        elif origin_shard == 1:
            # Shard 1 should give up even slots to shard 0
            even_slots_to_give = current_slots & target_even_slots
            if even_slots_to_give:
                if 0 not in migrations_by_destination:
                    migrations_by_destination[0] = {}
                migrations_by_destination[0][origin_shard] = even_slots_to_give

        else:
            # All other shards give up all their slots
            even_slots_to_give = current_slots & target_even_slots
            odd_slots_to_give = current_slots & target_odd_slots

            if even_slots_to_give:
                if 0 not in migrations_by_destination:
                    migrations_by_destination[0] = {}
                migrations_by_destination[0][origin_shard] = even_slots_to_give
            if odd_slots_to_give:
                if 1 not in migrations_by_destination:
                    migrations_by_destination[1] = {}
                migrations_by_destination[1][origin_shard] = odd_slots_to_give

    # Create ASM commands grouped by destination, then by origin
    asm_commands: Dict[int, Dict[int, ASMCommand]] = {}
    asm_commands[0] = {}
    asm_commands[1] = {}
    num_commands = 0
    for dest_shard, origin_slots_map in migrations_by_destination.items():
        for origin_shard, slots_to_migrate in origin_slots_map.items():
            if slots_to_migrate:
                sorted_slots = sorted(slots_to_migrate)
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
                    import_node=dest_shard,
                )
                slot_type = "even" if dest_shard == 0 else "odd"

                logging.info(f"SPARSE ASM Command to migrate {slot_type} slots from shard {origin_shard} to shard {dest_shard}: {asm_command}")
                asm_commands[dest_shard][origin_shard] = asm_command
                num_commands += 1

    # Execute the migration commands
    logging.info(f"Executing {num_commands} ASM commands to complete SPARSE migration")
    print(f"Executing {num_commands} ASM commands to complete SPARSE migration")
    for dest_shard, origin_command_map in asm_commands.items():
        for origin_shard, asm_command in origin_command_map.items():
            logging.info(f"Executing ASM Command to migrate from shard {origin_shard} to shard {dest_shard}")
            print(f"Executing ASM Command to migrate from shard {origin_shard} to shard {dest_shard} with min slot {asm_command.ranges[0].start} and max slot {asm_command.ranges[-1].end}")
            asm_command.execute(shards_info, wait_for_completion=True)

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
                execute_asm_sparse_migration(shards_info)
            elif isinstance(cmd, dict):
                asm_command = ASMCommand(**cmd)
                logging.info(
                    "ASM Command: {}".format(asm_command)
                )
                asm_commands.append(asm_command)
                asm_command.execute(shards_info)

    logging.info("ASM commands executed")

    logging.info("Waiting for ASM commands to complete")
    for asm_command in asm_commands:
        asm_command.wait_for_completion()
    return res


if __name__ == "__main__":
    r = redis.Redis(host="localhost", port=6379, db=0)
    # benchmark_config = {
    #     "dbconfig": {
    #         "asm_commands": [
    #             {
    #                 "ranges": [{"start": 0, "end": 5460}],
    #                 "import_node": 1,
    #             },
    #             {
    #                 "ranges": [{"start": 5461, "end": 10922}],
    #                 "import_node": 0,
    #             },
    #         ]
    #     }
    # }
    # execute_asm_commands({}, r)

    benchmark_config = {
        "dbconfig": {
            "asm_commands": [
                "SPARSE",
            ]
        }
    }
    execute_asm_commands(benchmark_config, r)
