from redisbench_admin.run_remote.run_remote import (
    extract_module_semver_from_info_modules_cmd,
)


def test_extract_module_semver_from_info_modules_cmd():
    stdout = b"# Modules\r\nmodule:name=search,ver=999999,api=1,filters=0,usedby=[],using=[],options=[]\r\n".decode()
    module_name, semver = extract_module_semver_from_info_modules_cmd(stdout)
    assert semver[0] == "999999"
    assert module_name[0] == "search"
    module_name, semver = extract_module_semver_from_info_modules_cmd(b"")
    assert semver == []
    assert module_name == []
