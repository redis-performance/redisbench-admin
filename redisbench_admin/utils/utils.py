import csv
import datetime as dt
import json
import operator
import os
import os.path
import subprocess
import sys
import tarfile
from functools import reduce
from zipfile import ZipFile

import boto3
import requests
from tqdm import tqdm

EPOCH = dt.datetime.utcfromtimestamp(0)


def upload_artifacts_to_s3(artifacts, s3_bucket_name, s3_bucket_path):
    print("-- uploading results to s3 -- ")
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket_name)
    progress = tqdm(unit="files", total=len(artifacts))
    for input in artifacts:
        object_key = '{bucket_path}{filename}'.format(bucket_path=s3_bucket_path, filename=input)
        bucket.upload_file(input, object_key)
        object_acl = s3.ObjectAcl(s3_bucket_name, object_key)
        response = object_acl.put(ACL='public-read')
        progress.update()
    progress.close()


def whereis(program):
    for path in os.environ.get('PATH', '').split(':'):
        if os.path.exists(os.path.join(path, program)) and \
                not os.path.isdir(os.path.join(path, program)):
            return os.path.join(path, program)
    return None


# Check if system has the required utilities: ftsb_redisearch, etc
def required_utilities(utility_list):
    result = 1
    for index in utility_list:
        if whereis(index) == None:
            print('Cannot locate ' + index + ' in path!')
            result = 0
    return result


def decompress_file(compressed_filename, uncompressed_filename):
    if compressed_filename.endswith(".zip"):
        with ZipFile(compressed_filename, 'r') as zipObj:
            zipObj.extractall()

    elif compressed_filename.endswith("tar.gz"):
        tar = tarfile.open(compressed_filename, "r:gz")
        tar.extractall()
        tar.close()

    elif compressed_filename.endswith("tar"):
        tar = tarfile.open(compressed_filename, "r:")
        tar.extractall()
        tar.close()


def findJsonPath(element, json):
    return reduce(operator.getitem, element.split('.'), json)


def ts_milli(at_dt):
    return int((at_dt - dt.datetime(1970, 1, 1)).total_seconds() * 1000)


def retrieve_local_or_remote_input_json(config_filename, local_path, option_name, format="json", csv_header=False):
    benchmark_config = {}

    if config_filename.startswith("http"):
        print("retrieving benchmark config file from remote url {}".format(config_filename))
        r = requests.get(config_filename)
        benchmark_config[config_filename] = r.json()
        remote_filename = config_filename[config_filename.rfind('/') + 1:]
        local_config_file = "{}/{}".format(local_path, remote_filename)
        open(local_config_file, 'wb').write(r.content)
        print("To avoid fetching again the config file use the option {option_name} {filename}".format(
            option_name=option_name,
            filename=local_config_file))

    elif config_filename.startswith("S3://") or config_filename.startswith("s3://"):
        print("s3")
        s3 = boto3.resource('s3')
        bucket_str = config_filename[5:].split("/")[0]

        bucket_prefix = ""
        if len(config_filename[5:].split("/")) > 0:
            bucket_prefix = "/".join(config_filename[5:].split("/")[1:])
        my_bucket = s3.Bucket(bucket_str)

        print("Retrieving data from s3 bucket: {bucket_str}. Prefix={bucket_prefix}".format(bucket_str=bucket_str,
                                                                                            bucket_prefix=bucket_prefix))
        benchmark_config = {}
        objects = list(my_bucket.objects.filter(Prefix=bucket_prefix))
        for object_summary in tqdm(objects, total=len(objects)):
            filename = object_summary.key.split("/")[-1]
            local_config_file = "{}/{}".format(local_path, filename)
            my_bucket.download_file(object_summary.key, local_config_file)
            with open(local_config_file, "r") as local_file:
                read_json_or_csv(benchmark_config, config_filename, format, local_file, csv_header)

    else:
        with open(config_filename, "r") as local_file:
            read_json_or_csv(benchmark_config, config_filename, format, local_file, csv_header)

    return benchmark_config


def read_json_or_csv(benchmark_config, config_filename, format, local_file, csv_has_header):
    if format == "json":
        benchmark_config[config_filename] = json.load(local_file)
    if format == "csv":
        reader = csv.reader(local_file)
        header_array = []
        dict = {}
        header_row = next(reader)
        body_rows = [x for x in reader]
        if csv_has_header:
            for col in header_row:
                dict[col] = []
                header_array.append(col)
        else:
            for pos, _ in enumerate(header_row):
                col_name = "col_{}".format(pos)
                dict[col_name] = []
                header_array.append(col_name)
            newbd = [header_row]
            for x in body_rows:
                newbd.append(x)
            body_rows = newbd

        for row in body_rows:
            for col_pos, col in enumerate(row):
                col_name = header_array[col_pos]
                dict[col_name].append(col)
        benchmark_config[config_filename] = dict
