def create_compare_arguments(parser):
    parser.add_argument('--baseline-file', type=str, required=True,
                        help="baseline benchmark output file to read results from. can be a local file or a remote link.")
    parser.add_argument('--comparison-file', type=str, required=True,
                        help="comparison benchmark output file to read results from. can be a local file or a remote link.")
    parser.add_argument('--use-result', type=str, default="median-result",
                        help="for each key-metric, use either worst-result, best-result, or median-result")
    parser.add_argument('--steps', type=str, default="setup,benchmark",
                        help="comma separated list of steps to be analyzed given the benchmark result files")
    parser.add_argument('--enable-fail-above', default=False, action='store_true',
                        help="enables failing test if percentage of change is above threshold on any of the benchmark steps being analysed")
    parser.add_argument('--fail-above-pct-change', type=float, default=10.0,
                        help='Fail above if any of the key-metrics presents an regression in percentage of change (from 0.0-100.0)')
    return parser
