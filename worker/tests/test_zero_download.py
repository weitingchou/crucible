from unittest.mock import patch, MagicMock

import pytest

from worker.fixture_loader.strategies.zero_download import ZeroDownloadStrategy


def _make_config(host, port=None):
    """Build a minimal config dict with the given host/port layout."""
    config = {"host": host, "username": "root", "password": "", "target_db": "testdb"}
    if port is not None:
        config["port"] = port
    return config


@patch("worker.fixture_loader.strategies.zero_download.pymysql.connect")
def test_init_with_embedded_port(mock_connect):
    """host: 'doris-fe:9030' — port parsed from host string."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    config = _make_config("doris-fe:9030")

    ZeroDownloadStrategy().init(config)

    mock_connect.assert_called_once_with(
        host="doris-fe", port=9030, user="root", password=""
    )


@patch("worker.fixture_loader.strategies.zero_download.pymysql.connect")
def test_init_with_separate_port(mock_connect):
    """host + port as separate fields."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    config = _make_config("doris-doris-fe.crucible-benchmarks.svc.cluster.local", port=9030)

    ZeroDownloadStrategy().init(config)

    mock_connect.assert_called_once_with(
        host="doris-doris-fe.crucible-benchmarks.svc.cluster.local",
        port=9030, user="root", password=""
    )


def test_init_no_port_raises():
    """Neither embedded nor separate port — should raise ValueError."""
    config = _make_config("doris-doris-fe.crucible-benchmarks.svc.cluster.local")
    with pytest.raises(ValueError, match="host must include a port"):
        ZeroDownloadStrategy().init(config)
