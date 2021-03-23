import os
import shutil
import tempfile

from redisbench_admin.utils.local import checkDatasetLocalRequirements


def test_check_dataset_local_requirements():
    url = "https://s3.amazonaws.com/benchmarks.redislabs/redistimeseries/tsbs/datasets/devops/functional/scale-100-redistimeseries_data.rdb"
    # no db config
    checkDatasetLocalRequirements({}, ".", ".")
    # dbconfig with no dataset key
    checkDatasetLocalRequirements({"dbconfig": {}}, ".", ".")

    # dbconfig with local filename
    directory_from = tempfile.mkdtemp()
    directory_to = tempfile.mkdtemp()
    temp_file1 = tempfile.NamedTemporaryFile(dir=directory_from)
    checkDatasetLocalRequirements({"dbconfig": [{"dataset": temp_file1.name}]}, directory_to.__str__(), None)
    assert os.path.exists("{}/{}".format(directory_from.__str__(), temp_file1.name.split("/")[-1]))
    assert os.path.exists("{}/{}".format(directory_to.__str__(), "dump.rdb"))
    shutil.rmtree(directory_from)
    shutil.rmtree(directory_to)

    # dbconfig with remote filename
    tests_remote_tmp_datasets = "./tests/temp-datasets"
    if os.path.isdir(tests_remote_tmp_datasets):
        shutil.rmtree(tests_remote_tmp_datasets)

    directory_to = tempfile.mkdtemp()
    checkDatasetLocalRequirements({"dbconfig": [{"dataset": url}]}, directory_to.__str__(), None,
                                  tests_remote_tmp_datasets)
    assert os.path.exists("{}/{}".format(directory_to.__str__(), "dump.rdb"))
    assert os.path.exists("{}/{}".format(tests_remote_tmp_datasets, "scale-100-redistimeseries_data.rdb"))
    checkDatasetLocalRequirements({"dbconfig": [{"dataset": url}]}, directory_to.__str__(), None,
                                  tests_remote_tmp_datasets)
    assert os.path.exists("{}/{}".format(directory_to.__str__(), "dump.rdb"))
    shutil.rmtree(directory_to)
    if os.path.isdir(tests_remote_tmp_datasets):
        shutil.rmtree(tests_remote_tmp_datasets)
