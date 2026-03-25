import pytest

from crucible_lib.net import parse_host


def test_host_with_embedded_port():
    assert parse_host("doris-fe:9030") == ("doris-fe", 9030)


def test_fqdn_with_embedded_port():
    assert parse_host("doris-doris-fe.crucible-benchmarks.svc.cluster.local:9030") == (
        "doris-doris-fe.crucible-benchmarks.svc.cluster.local",
        9030,
    )


def test_explicit_port_takes_precedence():
    assert parse_host("doris-fe:9030", port=5432) == ("doris-fe:9030", 5432)


def test_explicit_port_with_bare_host():
    assert parse_host("doris-doris-fe.crucible-benchmarks.svc.cluster.local", port=9030) == (
        "doris-doris-fe.crucible-benchmarks.svc.cluster.local",
        9030,
    )


def test_explicit_port_as_string():
    assert parse_host("doris-fe", port="9030") == ("doris-fe", 9030)


def test_no_port_anywhere_raises():
    with pytest.raises(ValueError, match="host must include a port"):
        parse_host("doris-doris-fe.crucible-benchmarks.svc.cluster.local")


def test_explicit_port_none_falls_back_to_embedded():
    assert parse_host("localhost:5432", port=None) == ("localhost", 5432)
