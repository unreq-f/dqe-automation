import pytest
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.data_quality.data_quality_validation_library import DataQualityLibrary
from src.connectors.file_system.parquet_reader import LocalParquetConnector
import os

def pytest_addoption(parser):
    parser.addoption("--db_host", action="store", default="localhost", help="Database host")
    parser.addoption("--db_user", action="store", help="Database user")
    parser.addoption("--db_password", action="store", help="Database password")
    parser.addoption("--db_name", action="store", default="test_db", help="Database name")
    parser.addoption("--db_port", action="store", default="5434")

def pytest_configure(config):
    """
    Validates that all required command-line options are provided.
    """
    required_options = [
        "--db_user", "--db_password"
    ]
    for option in required_options:
        if not config.getoption(option):
            pytest.fail(f"Missing required option: {option}")

@pytest.fixture(scope='session')
def db_connection(request):
    db_host = request.config.getoption("--db_host")
    db_user = request.config.getoption("--db_user")
    db_password = request.config.getoption("--db_password")
    db_name = request.config.getoption("--db_name")
    db_port = request.config.getoption("--db_port")

    try:
        with PostgresConnectorContextManager( db_host=db_host, db_user=db_user, db_password=db_password, db_name=db_name, db_port=db_port) as db_connector:
            yield db_connector
    except Exception as e:
        pytest.fail(f"Failed to initialize PostgresConnectorContextManager: {e}")

@pytest.fixture(scope='session')
def parquet_data_factory():
    base_path = os.environ.get('PARQUET_BASE_PATH', 'PyTest_DQ_Framework/src/parquet_data')
    connector = LocalParquetConnector(base_path)

    def _reader(table_name: str, filters=None):
        return connector.read_table(table_name, filters=filters)

    return _reader