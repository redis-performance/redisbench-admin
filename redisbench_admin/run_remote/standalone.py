#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
#
import logging
import os

from redisbench_admin.utils.remote import (
    copy_file_to_remote_setup,
    execute_remote_commands,
)
from redisbench_admin.utils.ssh import SSHSession
from redisbench_admin.utils.utils import redis_server_config_module_part
import tempfile


def ensure_redis_server_available(server_public_ip, username, private_key, port=22):
    """Check if redis-server is available, install if not"""
    logging.info("Checking if redis-server is available on remote server...")

    # Check if redis-server exists
    check_result = execute_remote_commands(
        server_public_ip, username, private_key, ["which redis-server"], port
    )

    # Check the result
    if len(check_result) > 0:
        [recv_exit_status, stdout, stderr] = check_result[0]
        if recv_exit_status != 0:
            logging.info("redis-server not found, installing Redis...")

            # Install Redis using the provided commands
            install_commands = [
                "sudo apt-get install lsb-release curl gpg -y",
                "curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg",
                "sudo chmod 644 /usr/share/keyrings/redis-archive-keyring.gpg",
                'echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list',
                "sudo apt-get update",
                "sudo apt-get install redis -y",
                "sudo systemctl disable redis-server",
            ]

            install_result = execute_remote_commands(
                server_public_ip, username, private_key, install_commands, port
            )

            # Check if installation was successful
            for pos, res_pos in enumerate(install_result):
                [recv_exit_status, stdout, stderr] = res_pos
                if recv_exit_status != 0:
                    logging.warning(
                        "Redis installation command {} returned exit code {}. stdout: {}. stderr: {}".format(
                            pos, recv_exit_status, stdout, stderr
                        )
                    )

            logging.info("Redis installation completed and auto-start disabled")
        else:
            logging.info("redis-server is already available")
    else:
        logging.error("Failed to check redis-server availability")


def ensure_zip_available(server_public_ip, username, private_key, port=22):
    """Check if zip is available, install if not"""
    logging.info("Checking if zip is available on remote server...")

    # Check if zip exists
    check_result = execute_remote_commands(
        server_public_ip, username, private_key, ["which zip"], port
    )

    # Check the result
    if len(check_result) > 0:
        [recv_exit_status, stdout, stderr] = check_result[0]
        if recv_exit_status != 0:
            logging.info("zip not found, installing...")

            # Install zip
            install_commands = ["sudo apt-get install zip -y"]

            install_result = execute_remote_commands(
                server_public_ip, username, private_key, install_commands, port
            )

            # Check if installation was successful
            for pos, res_pos in enumerate(install_result):
                [recv_exit_status, stdout, stderr] = res_pos
                if recv_exit_status != 0:
                    logging.warning(
                        "Zip installation command {} returned exit code {}. stdout: {}. stderr: {}".format(
                            pos, recv_exit_status, stdout, stderr
                        )
                    )

            logging.info("Zip installation completed")
        else:
            logging.info("zip is already available")
    else:
        logging.error("Failed to check zip availability")


def ensure_memtier_benchmark_available(
    client_public_ip, username, private_key, port=22
):
    """Check if memtier_benchmark is available, install if not"""
    logging.info("Checking if memtier_benchmark is available on remote client...")

    # Check if memtier_benchmark exists
    check_result = execute_remote_commands(
        client_public_ip, username, private_key, ["which memtier_benchmark"], port
    )

    # Check the result
    if len(check_result) > 0:
        [recv_exit_status, stdout, stderr] = check_result[0]
        if recv_exit_status != 0:
            logging.info("memtier_benchmark not found, installing...")

            # Install memtier_benchmark using the provided commands
            install_commands = [
                "sudo apt install lsb-release curl gpg -y",
                "curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg",
                'echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list',
                "sudo apt-get update",
                "sudo apt-get install memtier-benchmark -y",
            ]

            install_result = execute_remote_commands(
                client_public_ip, username, private_key, install_commands, port
            )

            # Check if installation was successful
            for pos, res_pos in enumerate(install_result):
                [recv_exit_status, stdout, stderr] = res_pos
                if recv_exit_status != 0:
                    logging.warning(
                        "memtier_benchmark installation command {} returned exit code {}. stdout: {}. stderr: {}".format(
                            pos, recv_exit_status, stdout, stderr
                        )
                    )

            logging.info("memtier_benchmark installation completed")
        else:
            logging.info("memtier_benchmark is already available")
    else:
        logging.error("Failed to check memtier_benchmark availability")


def validate_remote_host_compatibility(
    server_public_ip, username, private_key, custom_redis_server_path=None, port=22
):
    """
    Validate remote host compatibility by checking OS version, stdlib version,
    and optionally testing a custom redis-server binary.

    Args:
        server_public_ip: IP address of the remote server
        username: SSH username
        private_key: Path to SSH private key
        custom_redis_server_path: Optional path to custom redis-server binary on remote host
        port: SSH port (default 22)

    Returns:
        tuple: (success: bool, validation_info: dict)
    """
    logging.info("Validating remote host compatibility...")

    validation_info = {}
    success = True

    try:
        # Check OS version
        os_commands = [
            "uname -a",  # System information
            "cat /etc/os-release",  # OS release info
            "ldd --version",  # glibc version
        ]

        os_results = execute_remote_commands(
            server_public_ip, username, private_key, os_commands, port
        )

        # Process OS information
        for i, (recv_exit_status, stdout, stderr) in enumerate(os_results):
            if recv_exit_status == 0:
                if i == 0:  # uname -a
                    validation_info["system_info"] = "".join(stdout).strip()
                    logging.info(f"System info: {validation_info['system_info']}")
                elif i == 1:  # /etc/os-release
                    os_release = "".join(stdout).strip()
                    validation_info["os_release"] = os_release
                    logging.info(f"OS release info: {os_release}")
                elif i == 2:  # ldd --version (glibc)
                    glibc_info = "".join(stdout).strip()
                    validation_info["glibc_version"] = glibc_info
                    logging.info(f"glibc version: {glibc_info}")
            else:
                logging.warning(
                    f"Command {os_commands[i]} failed with exit code {recv_exit_status}"
                )
                if i == 2:  # glibc check failed, try alternative
                    alt_result = execute_remote_commands(
                        server_public_ip,
                        username,
                        private_key,
                        ["getconf GNU_LIBC_VERSION"],
                        port,
                    )
                    if alt_result and alt_result[0][0] == 0:
                        glibc_alt = "".join(alt_result[0][1]).strip()
                        validation_info["glibc_version"] = glibc_alt
                        logging.info(f"glibc version (alternative): {glibc_alt}")

        # Test custom redis-server binary if provided
        if custom_redis_server_path:
            logging.info(
                f"Testing custom redis-server binary: {custom_redis_server_path}"
            )

            # Check if the binary exists and is executable
            check_commands = [
                f"test -f {custom_redis_server_path}",
                f"test -x {custom_redis_server_path}",
                f"{custom_redis_server_path} --version",
            ]

            check_results = execute_remote_commands(
                server_public_ip, username, private_key, check_commands, port
            )

            for i, (recv_exit_status, stdout, stderr) in enumerate(check_results):
                if recv_exit_status != 0:
                    if i == 0:
                        logging.error(
                            f"Custom redis-server binary not found: {custom_redis_server_path}"
                        )
                        success = False
                    elif i == 1:
                        logging.error(
                            f"Custom redis-server binary not executable: {custom_redis_server_path}"
                        )
                        success = False
                    elif i == 2:
                        logging.error(
                            f"Custom redis-server binary --version failed: {stderr}"
                        )
                        success = False
                    break
                elif i == 2:  # --version succeeded
                    version_output = "".join(stdout).strip()
                    validation_info["custom_redis_version"] = version_output
                    logging.info(f"Custom redis-server version: {version_output}")

    except Exception as e:
        logging.error(f"Remote host validation failed with exception: {e}")
        success = False

    if success:
        logging.info("✅ Remote host validation passed")
    else:
        logging.error("❌ Remote host validation failed")

    return success, validation_info


def spin_up_standalone_remote_redis(
    temporary_dir,
    server_public_ip,
    username,
    private_key,
    remote_module_files,
    logfile,
    redis_configuration_parameters=None,
    port=22,
    modules_configuration_parameters_map={},
    redis_7=True,
):
    # Ensure redis-server is available before trying to start it
    ensure_redis_server_available(server_public_ip, username, private_key, port)

    full_logfile, initial_redis_cmd = generate_remote_standalone_redis_cmd(
        logfile,
        redis_configuration_parameters,
        remote_module_files,
        temporary_dir,
        modules_configuration_parameters_map,
        redis_7,
    )

    # start redis-server
    commands = [initial_redis_cmd]
    res = execute_remote_commands(
        server_public_ip, username, private_key, commands, port
    )
    for pos, res_pos in enumerate(res):
        [recv_exit_status, stdout, stderr] = res_pos
        if recv_exit_status != 0:
            logging.error(
                "Remote primary shard {} command returned exit code {}. stdout {}. stderr {}".format(
                    pos, recv_exit_status, stdout, stderr
                )
            )
    return full_logfile


def cp_local_dbdir_to_remote(
    dbdir_folder, private_key, server_public_ip, temporary_dir, username
):
    if dbdir_folder is not None:
        logging.info(
            "Copying entire content of {} into temporary path: {}".format(
                dbdir_folder, temporary_dir
            )
        )
        ssh = SSHSession(server_public_ip, username, key_file=open(private_key, "r"))
        ssh.put_all(dbdir_folder, temporary_dir)


def remote_module_files_cp(
    local_module_files,
    port,
    private_key,
    remote_module_file_dir,
    server_public_ip,
    username,
    continue_on_module_check_error=False,
):
    remote_module_files = []

    if local_module_files is not None:
        logging.info(f"there is a total of  {len(local_module_files)} modules")
        for local_module_file in local_module_files:
            splitted_module_and_plugins = []
            if type(local_module_file) is str:
                splitted_module_and_plugins = local_module_file.split(" ")
            if type(local_module_file) is list:
                splitted_module_and_plugins = local_module_file
            if len(splitted_module_and_plugins) > 1:
                logging.info(
                    "Detected a module and plugin(s) pairs {}".format(
                        splitted_module_and_plugins
                    )
                )
            abs_splitted_module_and_plugins = [
                os.path.abspath(x) for x in splitted_module_and_plugins
            ]
            remote_module_files_in = ""
            for pos, local_module_file_and_plugin in enumerate(
                abs_splitted_module_and_plugins, start=1
            ):
                file_basename = os.path.basename(local_module_file_and_plugin)
                remote_module_file = "{}/{}".format(
                    remote_module_file_dir,
                    file_basename,
                )
                logging.info(
                    "remote_module_file: {}. basename: {}".format(
                        remote_module_file, file_basename
                    )
                )
                # copy the module to the DB machine
                cp_res = copy_file_to_remote_setup(
                    server_public_ip,
                    username,
                    private_key,
                    local_module_file_and_plugin,
                    remote_module_file,
                    None,
                    port,
                    continue_on_module_check_error,
                )
                if cp_res:
                    execute_remote_commands(
                        server_public_ip,
                        username,
                        private_key,
                        ["chmod 755 {}".format(remote_module_file)],
                        port,
                    )
                else:
                    # If the copy was unsuccessful restore path to original basename
                    remote_module_file = file_basename
                    logging.info(
                        "Given the copy was unsuccessful restore path to original basename: {}.".format(
                            remote_module_file
                        )
                    )
                if pos > 1:
                    remote_module_files_in = remote_module_files_in + " "
                remote_module_files_in = remote_module_files_in + remote_module_file
            remote_module_files.append(remote_module_files_in)
    logging.info(
        "There are a total of {} remote files {}".format(
            len(remote_module_files), remote_module_files
        )
    )
    return remote_module_files


def generate_remote_standalone_redis_cmd(
    logfile,
    redis_configuration_parameters,
    remote_module_files,
    temporary_dir,
    modules_configuration_parameters_map,
    enable_redis_7_config_directives=True,
    enable_debug_command="yes",
    custom_redis_server_path=None,
    custom_redis_conf_path=None,
):
    # Use custom redis-server binary if provided, otherwise use default
    redis_server_binary = (
        custom_redis_server_path if custom_redis_server_path else "redis-server"
    )

    # If custom config file is provided, use it; otherwise use command line parameters
    if custom_redis_conf_path:
        initial_redis_cmd = "{} {} --daemonize yes".format(
            redis_server_binary, custom_redis_conf_path
        )
        logging.info(f"Using custom redis.conf: {custom_redis_conf_path}")
    else:
        initial_redis_cmd = "{} --save '' --logfile {} --dir {} --daemonize yes --protected-mode no ".format(
            redis_server_binary, logfile, temporary_dir
        )
    if enable_redis_7_config_directives:
        extra_str = " --enable-debug-command {} ".format(enable_debug_command)
        initial_redis_cmd = initial_redis_cmd + extra_str
    full_logfile = "{}/{}".format(temporary_dir, logfile)
    if redis_configuration_parameters is not None:
        for (
            configuration_parameter,
            configuration_value,
        ) in redis_configuration_parameters.items():
            initial_redis_cmd += " --{} {}".format(
                configuration_parameter, configuration_value
            )
    command = []
    if remote_module_files is not None:
        if type(remote_module_files) == str:
            redis_server_config_module_part(
                command, remote_module_files, modules_configuration_parameters_map
            )
        if type(remote_module_files) == list:
            logging.info(
                "There are a total of {} modules".format(len(remote_module_files))
            )
            for mod in remote_module_files:
                redis_server_config_module_part(
                    command, mod, modules_configuration_parameters_map
                )
    if remote_module_files is not None:
        initial_redis_cmd += " " + " ".join(command)
    return full_logfile, initial_redis_cmd


def spin_test_standalone_redis(
    server_public_ip,
    username,
    private_key,
    db_ssh_port=22,
    redis_port=6379,
    local_module_files=None,
    redis_configuration_parameters=None,
    modules_configuration_parameters_map=None,
    custom_redis_conf_path=None,
    custom_redis_server_path=None,
):
    """
    Setup standalone Redis server, run INFO SERVER, print output as markdown and exit.

    Args:
        server_public_ip: IP address of the remote server
        username: SSH username
        private_key: Path to SSH private key
        db_ssh_port: SSH port (default 22)
        redis_port: Redis port (default 6379)
        local_module_files: List of local module files to copy
        redis_configuration_parameters: Dict of Redis configuration parameters
        modules_configuration_parameters_map: Dict of module configuration parameters
        custom_redis_conf_path: Path to custom redis.conf file
        custom_redis_server_path: Path to custom redis-server binary
    """
    logging.info("🚀 Starting spin-test mode...")

    try:
        # Create temporary directory on remote host
        temporary_dir = "/tmp/redisbench-spin-test"
        create_dir_commands = [f"mkdir -p {temporary_dir}"]
        execute_remote_commands(
            server_public_ip, username, private_key, create_dir_commands, db_ssh_port
        )

        # Ensure Redis server is available (only if not using custom binary)
        if custom_redis_server_path is None:
            ensure_redis_server_available(
                server_public_ip, username, private_key, db_ssh_port
            )
        else:
            logging.info(
                "🔧 Using custom Redis binary - skipping system Redis installation"
            )

        # Copy custom Redis files if provided
        remote_redis_conf_path = None
        remote_redis_server_path = None

        if custom_redis_conf_path:
            if not os.path.exists(custom_redis_conf_path):
                logging.error(
                    f"❌ Custom redis.conf file not found: {custom_redis_conf_path}"
                )
                return False

            remote_redis_conf_path = f"{temporary_dir}/redis.conf"
            logging.info(f"📁 Copying custom redis.conf to remote host...")

            copy_result = copy_file_to_remote_setup(
                server_public_ip,
                username,
                private_key,
                custom_redis_conf_path,
                remote_redis_conf_path,
                None,
                db_ssh_port,
                False,  # don't continue on error
            )

            if not copy_result:
                logging.error("❌ Failed to copy redis.conf to remote host")
                return False

        if custom_redis_server_path:
            if not os.path.exists(custom_redis_server_path):
                logging.error(
                    f"❌ Custom redis-server binary not found: {custom_redis_server_path}"
                )
                return False

            remote_redis_server_path = f"{temporary_dir}/redis-server"
            logging.info(f"📁 Copying custom redis-server binary to remote host...")

            copy_result = copy_file_to_remote_setup(
                server_public_ip,
                username,
                private_key,
                custom_redis_server_path,
                remote_redis_server_path,
                None,
                db_ssh_port,
                False,  # don't continue on error
            )

            if not copy_result:
                logging.error("❌ Failed to copy redis-server binary to remote host")
                return False

            # Make the binary executable
            chmod_commands = [f"chmod +x {remote_redis_server_path}"]
            chmod_results = execute_remote_commands(
                server_public_ip, username, private_key, chmod_commands, db_ssh_port
            )

            recv_exit_status, stdout, stderr = chmod_results[0]
            if recv_exit_status != 0:
                logging.warning(
                    f"⚠️ Failed to make redis-server binary executable: {stderr}"
                )
            else:
                logging.info("✅ Redis-server binary made executable")

        # Copy modules if provided
        remote_module_files = None
        if local_module_files:
            remote_module_file_dir = f"{temporary_dir}/modules"
            create_module_dir_commands = [f"mkdir -p {remote_module_file_dir}"]
            execute_remote_commands(
                server_public_ip,
                username,
                private_key,
                create_module_dir_commands,
                db_ssh_port,
            )

            remote_module_files = remote_module_files_cp(
                local_module_files,
                db_ssh_port,
                private_key,
                remote_module_file_dir,
                server_public_ip,
                username,
                continue_on_module_check_error=True,
            )

        # Generate Redis startup command
        logfile = "redis-spin-test.log"
        full_logfile, redis_cmd = generate_remote_standalone_redis_cmd(
            logfile,
            redis_configuration_parameters,
            remote_module_files,
            temporary_dir,
            modules_configuration_parameters_map or {},
            enable_redis_7_config_directives=True,
            enable_debug_command="yes",
            custom_redis_server_path=remote_redis_server_path,
            custom_redis_conf_path=remote_redis_conf_path,
        )

        # Override port if specified
        if redis_port != 6379:
            redis_cmd += f" --port {redis_port}"

        logging.info(f"🔧 Starting Redis with command: {redis_cmd}")

        # Start Redis server
        start_commands = [redis_cmd]
        start_result = execute_remote_commands(
            server_public_ip, username, private_key, start_commands, db_ssh_port
        )

        # Check if Redis started successfully
        recv_exit_status, stdout, stderr = start_result[0]
        if recv_exit_status != 0:
            logging.error(
                f"❌ Failed to start Redis server. Exit code: {recv_exit_status}"
            )
            logging.error(f"STDOUT: {stdout}")
            logging.error(f"STDERR: {stderr}")
            return False

        # Wait a moment for Redis to fully start
        import time

        time.sleep(2)

        # Collect system information
        logging.info("📊 Collecting system information...")
        system_commands = [
            "uname -a",  # System information
            "cat /etc/os-release",  # OS release info
            "ldd --version 2>/dev/null || getconf GNU_LIBC_VERSION 2>/dev/null || echo 'glibc version not available'",  # glibc version
            "cat /proc/version",  # Kernel version
            "cat /proc/cpuinfo | grep 'model name' | head -1",  # CPU info
            "free -h",  # Memory info
            "df -h /",  # Disk space
        ]

        system_results = execute_remote_commands(
            server_public_ip, username, private_key, system_commands, db_ssh_port
        )

        # Run INFO SERVER command
        info_commands = [f"redis-cli -p {redis_port} INFO SERVER"]
        info_result = execute_remote_commands(
            server_public_ip, username, private_key, info_commands, db_ssh_port
        )

        recv_exit_status, stdout, stderr = info_result[0]
        if recv_exit_status != 0:
            logging.error(
                f"❌ Failed to run INFO SERVER. Exit code: {recv_exit_status}"
            )
            logging.error(f"STDOUT: {stdout}")
            logging.error(f"STDERR: {stderr}")
            return False

        # Format output as markdown
        info_output = "".join(stdout).strip()

        print("\n" + "=" * 80)
        print("🎯 REDIS SPIN-TEST RESULTS")
        print("=" * 80)
        print()

        # Display system information
        print("## 🖥️  System Information")
        print()

        system_labels = [
            "System Info",
            "OS Release",
            "glibc Version",
            "Kernel Version",
            "CPU Model",
            "Memory",
            "Disk Space",
        ]

        for i, (label, (recv_exit_status, stdout, stderr)) in enumerate(
            zip(system_labels, system_results)
        ):
            if recv_exit_status == 0 and stdout:
                output = "".join(stdout).strip()
                if output:
                    print(f"**{label}:**")
                    print("```")
                    print(output)
                    print("```")
                    print()
            else:
                print(f"**{label}:** ⚠️ Not available")
                print()

        # Display Redis information
        print("## 🔴 Redis Server Information")
        print()
        print("```")
        print(info_output)
        print("```")
        print()
        print("✅ Spin-test completed successfully!")
        print("=" * 80)

        # Cleanup: Stop Redis server
        cleanup_commands = [
            f"redis-cli -p {redis_port} shutdown nosave",
            f"rm -rf {temporary_dir}",
        ]
        execute_remote_commands(
            server_public_ip, username, private_key, cleanup_commands, db_ssh_port
        )

        logging.info("🧹 Cleanup completed")
        return True

    except Exception as e:
        logging.error(f"❌ Spin-test failed with error: {e}")

        # Attempt cleanup on error
        try:
            cleanup_commands = [
                f"redis-cli -p {redis_port} shutdown nosave",
                f"rm -rf {temporary_dir}",
            ]
            execute_remote_commands(
                server_public_ip, username, private_key, cleanup_commands, db_ssh_port
            )
        except:
            pass  # Ignore cleanup errors

        return False
