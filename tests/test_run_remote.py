from redisbench_admin.run_remote.run_remote import (
    extract_module_semver_from_info_modules_cmd,
    redistimeseries_results_logic,
)


def test_extract_module_semver_from_info_modules_cmd():
    stdout = b"# Modules\r\nmodule:name=search,ver=999999,api=1,filters=0,usedby=[],using=[],options=[]\r\n".decode()
    module_name, semver = extract_module_semver_from_info_modules_cmd(stdout)
    assert semver[0] == "999999"
    assert module_name[0] == "search"
    module_name, semver = extract_module_semver_from_info_modules_cmd(b"")
    assert semver == []
    assert module_name == []


def test_redistimeseries_results_logic():
    # redistimeseries_results_logic(
    #     artifact_version,
    #     benchmark_config,
    #     default_metrics,
    #     deployment_type,
    #     exporter_timemetric_path,
    #     results_dict,
    #     rts,
    #     test_name,
    #     tf_github_branch,
    #     tf_github_org,
    #     tf_github_repo,
    #     tf_triggering_env,
    # )
    pass
