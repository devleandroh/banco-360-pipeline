import pytest

from banco360.config import Config


@pytest.fixture(scope="session")
def spark():
    from banco360.jobs.comum import spark_session
    return spark_session("testes")


@pytest.fixture
def cfg(tmp_path):
    return Config(lake_dir=str(tmp_path / "lake"), landing_dir=str(tmp_path / "landing"), pii_salt="teste")
