"""
Description: Data Quality checks facility_name_min_time_spent_per_visit_date
Requirement(s): TICKET-1234
Author(s): Vl_Yevtushenko
"""

import pytest
from src.data_quality.data_quality_validation_library import DataQualityLibrary


@pytest.fixture(scope='module')
def source_data(db_connection):
    source_query = """
    select  f.facility_name, date(v.visit_timestamp) as visit_date, min(duration_minutes) as min_time_spent
    FROM facilities f left join visits v  on f.external_id = v.facility_id
    group by  f.facility_name, date(v.visit_timestamp) ;
    """
    source_data = db_connection.get_data_sql(source_query)
    return source_data


@pytest.fixture(scope='module')
def target_data(parquet_data_factory):
    target_path = 'facility_name_min_time_spent_per_visit_date'
    target_data = parquet_data_factory(target_path)
    return target_data


@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_target_is_not_empty(target_data):
    DataQualityLibrary.check_dataset_is_not_empty(target_data)

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_count(target_data, source_data):
    DataQualityLibrary.check_count(target_data, source_data )

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_null_in_column(target_data):
    DataQualityLibrary.check_not_null_values(target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_duplicates(target_data):
    DataQualityLibrary.check_duplicates(target_data)

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
@pytest.mark.parametrize("column, pattern", [
    ("visit_date", r'^\d{4}-\d{2}-\d{2}$' ),
])
def test_valid_data(target_data, pattern, column):
    DataQualityLibrary.check_pattern(target_data, pattern, column)

