import jinja2


def renderServiceFile(access_key, region, secret_key, args, argv):
    environment = jinja2.Environment()
    template = environment.from_string("""[Unit]
Description=Redisbench-admin run service
    
[Service]
Type=oneshot
Environment="PATH=/home/ubuntu/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" 
Environment="AWS_ACCESS_KEY_ID={{access_key}}" 
Environment="AWS_DEFAULT_REGION={{region}}" 
Environment="AWS_SECRET_ACCESS_KEY={{secret_key}}"
WorkingDirectory=/home/ubuntu/work_dir/tests/benchmarks
User=ubuntu
ExecStart=/home/ubuntu/work_dir/.venv/bin/python /home/ubuntu/work_dir/redisbench_admin/run.py run-remote {{args}}
    
[Install]
WantedBy=multi-user.target
    """)
    if "--private_key" not in argv:
        argv.append("--private_key")
        argv.append("/home/ubuntu/work_dir/tests/benchmarks/id_rsa")
    else:
        argv[argv.index(args.private_key)] = "/home/ubuntu/work_dir/tests/benchmarks/id_rsa"
    if len(args.module_path) != 0:
        argv[argv.index(args.module_path[0])] = argv[argv.index(args.module_path[0])].replace(
            '$ROOT', '/home/ubuntu/work_dir')
    argv_str = " ".join(argv)
    with open("redisbench-admin.service", mode="w", encoding="utf-8") as results:
        results.write(
            template.render(
                access_key=access_key,
                region=region,
                secret_key=secret_key,
                args=argv_str,
            )
        )


def renderRunFile():
    with open("run.py", mode="w", encoding="utf-8") as run_file:
        run_file.write("""#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import re
import sys
from redisbench_admin.cli import main
        
if __name__ == "__main__":
    sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])
    sys.exit(main())
""")


def savePemFile(pem_data):
    with open("id_rsa", mode="w", encoding="utf-8") as pem_file:
        pem_file.write(pem_data)
