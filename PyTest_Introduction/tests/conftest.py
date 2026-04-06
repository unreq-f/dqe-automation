import pytest
import pandas as pd

# Fixture to read the CSV file
@pytest.fixture(scope= "session",params=["src\\data\\data.csv"])
def load_csv(request):
    return pd.read_csv(request.param)

# Fixture to validate the schema of the file
@pytest.fixture(scope= "session",params=[['id', 'name', 'age', 'email', 'is_active']])
def load_schema(request,load_csv):
    param = request.param
    return load_csv.columns.tolist(), param

# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(config, items):
    for item in items:
        if len(item.own_markers) == 0:
            item.add_marker(pytest.mark.hookadd)
