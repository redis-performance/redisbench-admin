from redisbench_admin.environments.oss_cluster import split_primaries_per_db_nodes


def test_split_primaries_per_db_nodes():
    (
        primaries_per_node,
        server_private_ips,
        server_public_ips,
    ) = split_primaries_per_db_nodes(
        ["10.3.0.169", "10.3.0.55", "10.3.0.92"],
        ["18.117.74.99", "3.144.3.160", "3.145.92.27"],
        4,
        1,
    )
    assert primaries_per_node == [4]
    assert server_private_ips == ["10.3.0.169"]
    assert server_public_ips == ["18.117.74.99"]

    (
        primaries_per_node,
        server_private_ips,
        server_public_ips,
    ) = split_primaries_per_db_nodes(
        ["10.3.0.169", "10.3.0.55", "10.3.0.92"],
        ["18.117.74.99", "3.144.3.160", "3.145.92.27"],
        4,
        2,
    )
    assert primaries_per_node == [2, 2]
    assert server_private_ips == ["10.3.0.169", "10.3.0.55"]
    assert server_public_ips == ["18.117.74.99", "3.144.3.160"]

    (
        primaries_per_node,
        server_private_ips,
        server_public_ips,
    ) = split_primaries_per_db_nodes(
        ["10.3.0.169", "10.3.0.55", "10.3.0.92"],
        ["18.117.74.99", "3.144.3.160", "3.145.92.27"],
        5,
        2,
    )
    assert primaries_per_node == [3, 2]
    assert server_private_ips == ["10.3.0.169", "10.3.0.55"]
    assert server_public_ips == ["18.117.74.99", "3.144.3.160"]

    (
        primaries_per_node,
        server_private_ips,
        server_public_ips,
    ) = split_primaries_per_db_nodes(
        ["10.3.0.169", "10.3.0.55", "10.3.0.92"],
        ["18.117.74.99", "3.144.3.160", "3.145.92.27"],
        6,
        3,
    )
    assert primaries_per_node == [2, 2, 2]
    assert server_private_ips == ["10.3.0.169", "10.3.0.55", "10.3.0.92"]
    assert server_public_ips == ["18.117.74.99", "3.144.3.160", "3.145.92.27"]

    (
        primaries_per_node,
        server_private_ips,
        server_public_ips,
    ) = split_primaries_per_db_nodes(
        ["10.3.0.169", "10.3.0.55", "10.3.0.92"],
        ["18.117.74.99", "3.144.3.160", "3.145.92.27"],
        6,
        None,
    )
    assert primaries_per_node == [2, 2, 2]
    assert server_private_ips == ["10.3.0.169", "10.3.0.55", "10.3.0.92"]
    assert server_public_ips == ["18.117.74.99", "3.144.3.160", "3.145.92.27"]
