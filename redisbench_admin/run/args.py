#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
def create_run_arguments(parser):
    parser.add_argument(
        "--tool",
        type=str,
        required=True,
        help="benchmark tool to use",
    )
    parser.add_argument(
        "--remote",
        default=False,
        action="store_true",
        help="run the benchmark in remote mode",
    )
    parser.add_argument(
        "--output-file-prefix",
        type=str,
        default="",
        help="prefix to quickly tag some files",
    )
    return parser
