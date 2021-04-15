def create_run_local_arguments(parser):
    parser.add_argument("--module_path", type=str, required=True)
    parser.add_argument(
        "--allowed-tools",
        type=str,
        default="redis-benchmark,redisgraph-benchmark-go,ycsb",
        help="comma separated list of allowed tools for this module. By default all the supported are allowed.",
    )
    parser.add_argument(
        "--test",
        type=str,
        default="",
        help="specify a test to run. By default will run all of them.",
    )
    parser.add_argument("--port", type=int, default=6379)
    return parser
