#!/usr/bin/env python3

import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_merge_dbconfig_properties():
    """Test the dbconfig merging functionality"""

    # Import the function
    from redisbench_admin.utils.benchmark_config import merge_dbconfig_properties

    # Test case: defaults as list, local as dict (like the actual case)
    benchmark_config = {
        "name": "test",
        "dbconfig": {
            "init_commands": ['"FT.CREATE" "idx" "SCHEMA" "text_field" "TEXT"']
        },
    }

    default_dbconfig = [
        {"install_steps": ["sudo apt-get update", "sudo apt-get install liburing2 -y"]}
    ]

    print("🧪 Testing dbconfig merging...")
    print(f"Before merge:")
    print(f'  Local dbconfig: {benchmark_config["dbconfig"]}')
    print(f"  Default dbconfig: {default_dbconfig}")

    # Perform the merge
    merge_dbconfig_properties(
        benchmark_config, default_dbconfig, "dbconfig", "test.yml"
    )

    print(f"After merge:")
    print(f'  Merged dbconfig: {benchmark_config["dbconfig"]}')

    # Check if both install_steps and init_commands are present
    merged = benchmark_config["dbconfig"]
    has_install_steps = False
    has_init_commands = False

    if isinstance(merged, list):
        for item in merged:
            if "install_steps" in item:
                has_install_steps = True
                print(f'  ✅ Found install_steps: {item["install_steps"]}')
            if "init_commands" in item:
                has_init_commands = True
                print(f'  ✅ Found init_commands: {item["init_commands"]}')
    else:
        if "install_steps" in merged:
            has_install_steps = True
            print(f'  ✅ Found install_steps: {merged["install_steps"]}')
        if "init_commands" in merged:
            has_init_commands = True
            print(f'  ✅ Found init_commands: {merged["init_commands"]}')

    print(f"Results:")
    print(f"  Has install_steps: {has_install_steps}")
    print(f"  Has init_commands: {has_init_commands}")

    if has_install_steps and has_init_commands:
        print("🎉 dbconfig merging works correctly!")
        return True
    else:
        print("❌ dbconfig merging failed")
        return False


if __name__ == "__main__":
    try:
        success = test_merge_dbconfig_properties()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
