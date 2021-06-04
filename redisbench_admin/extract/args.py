#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#


def create_extract_arguments(parser):
    parser.add_argument(
        "--redis-url",
        type=str,
        default="redis://localhost:6379",
        help="The url for Redis connection",
    )
    parser.add_argument(
        "--output-tags-json",
        type=str,
        default="extracted_tags.json",
        help="output filename containing the extracted tags from redis.",
    )
    parser.add_argument(
        "--s3-bucket-name",
        type=str,
        default="benchmarks.redislabs",
        help="S3 bucket name.",
    )
    parser.add_argument(
        "--upload-results-s3",
        default=False,
        action="store_true",
        help="uploads the result files and configuration file to public "
        "'benchmarks.redislabs' bucket. Proper credentials are required",
    )
    parser.add_argument(
        "--cluster-mode",
        default=False,
        action="store_true",
        help="Run client in cluster mode",
    )
    return parser
