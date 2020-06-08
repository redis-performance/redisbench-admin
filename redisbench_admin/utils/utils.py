import datetime as dt
import operator
import os
import os.path
import tarfile
from functools import reduce
from zipfile import ZipFile

EPOCH = dt.datetime.utcfromtimestamp(0)


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
