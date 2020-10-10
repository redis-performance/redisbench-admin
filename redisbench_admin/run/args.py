def create_run_arguments(parser):
    parser.add_argument('--benchmark-config-file', type=str, required=True,
                        help="benchmark config file to read instructions from. can be a local file or a remote link")
    parser.add_argument('--workers', type=str, default=0,
                        help='number of workers to use during the benchark. If set to 0 it will auto adjust based on the machine number of VCPUs')
    parser.add_argument('--repetitions', type=int, default=1,
                        help='number of repetitions to run')
    parser.add_argument('--benchmark-requests', type=int, default=0,
                        help='Number of total requests to issue (0 = all of the present in input file)')
    parser.add_argument('--upload-results-s3', default=False, action='store_true',
                        help="uploads the result files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required")
    parser.add_argument('--redis-url', type=str, default="redis://localhost:6379", help='The url for Redis connection')
    parser.add_argument('--deployment-type', type=str, default="docker-oss",
                        help='one of docker-oss,docker-oss-cluster,docker-enterprise,oss,oss-cluster,enterprise')
    parser.add_argument('--deployment-shards', type=int, default=1,
                        help='number of database shards used in the deployment')
    parser.add_argument('--pipeline', type=int, default=1,
                        help='pipeline requests to Redis')
    parser.add_argument('--cluster-mode', default=False, action='store_true', help="Run client in cluster mode")
    parser.add_argument('--max-rps', type=int, default=0,
                        help="enable limiting the rate of queries per second, 0 = no limit. " + "By default no limit is specified and the binaries will stress the DB up to the maximum.")
    parser.add_argument('--output-file-prefix', type=str, default="", help='prefix to quickly tag some files')
    parser.add_argument('--requests', type=int, default=0,
                        help='Number of total requests to issue (0 = all of the present in input file).')
    return parser
