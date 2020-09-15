def create_export_arguments(parser):
    parser.add_argument('--benchmark-result-files', type=str, required=True,
                        help="benchmark results files to read results from. can be a local file, a remote link, or an s3 bucket.")
    parser.add_argument('--steps', type=str, default="setup,benchmark",
                        help="comma separated list of steps to be analyzed given the benchmark result files")
    parser.add_argument('--exporter', type=str, default="redistimeseries",
                        help="exporter to be used ( either csv or redistimeseries )")
    parser.add_argument('--use-result', type=str, default="median-result",
                        help="for each key-metric, use either worst-result, best-result, or median-result")
    parser.add_argument('--extra-tags', type=str, default="", help='comma separated extra tags in the format of key1=value,key2=value,...')
    parser.add_argument('--host', type=str, default="localhost",
                        help="redistimeseries host")
    parser.add_argument('--port', type=int, default=6379,
                        help="redistimeseries port")
    parser.add_argument('--password', type=str, default=None,
                        help="redistimeseries password")
    return parser
