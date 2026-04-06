import pytest
import re

@pytest.mark.validate_csv
def test_file_not_empty(load_csv):
    assert not load_csv.empty, "csv is empty or not loaded correctly"

@pytest.mark.xfail
def test_duplicates(load_csv):
    duplicates = load_csv.duplicated().sum()
    assert duplicates == 0, f"found {duplicates} duplicate(s) row"


@pytest.mark.validate_csv
def test_validate_schema(load_schema):
    actual_schema, expected_schema = load_schema
    assert actual_schema == expected_schema, "schema missmatch"

@pytest.mark.validate_csv
@pytest.mark.skip
def test_age_column_valid(load_csv):
    assert load_csv['age'].between(0,100).all()

def test_email_column_valid(load_csv):
    regex = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    is_valid = load_csv["email"].str.contains(regex, regex=True, na=False)
    assert is_valid.all(), f"invalid: {load_csv[~is_valid]}"

@pytest.mark.parametrize("id, is_active", [
    (1, False),
    (2, True),
])
def test_active_players(load_csv, id, is_active):
    df = load_csv[["id", "is_active"]].set_index("id")
    assert df["is_active"].loc[id] == is_active, f"Expected value on id {id} is {is_active}, actual: {df["is_active"].loc[id]}"


def test_active_player(load_csv):
    df = load_csv[["id", "is_active"]].set_index("id")
    assert df["is_active"].loc[2] == True
