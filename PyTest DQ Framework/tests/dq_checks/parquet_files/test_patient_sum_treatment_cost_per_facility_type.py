"""
Description: Data Quality checks patient_sum_treatment_cost_per_facility_type
Requirement(s): TICKET-1236
Author(s): Vl_Yevtushenko
"""

import pytest
from src.data_quality.data_quality_validation_library import DataQualityLibrary


@pytest.fixture(scope='module')
def source_data(db_connection):
    source_query = """
    select  f.facility_type, concat(p.first_name,' ', p.last_name) as full_name, SUM(v.treatment_cost) as sum_treatment_cost
    FROM facilities f left join visits v  on f.external_id = v.facility_id
          left join patients p  on v.patient_id  = p.id 
    group by  f.facility_type, full_name
    """
    source_data = db_connection.get_data_sql(source_query)
    return source_data


@pytest.fixture(scope='module')
def target_data(parquet_data_factory):
    target_path = 'patient_sum_treatment_cost_per_facility_type'
    target_data = parquet_data_factory(target_path)
    return target_data


@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_target_is_not_empty(target_data):
    DataQualityLibrary.check_dataset_is_not_empty(target_data)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_count(target_data, source_data):
    DataQualityLibrary.check_count(target_data, source_data )


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_null_in_column(target_data):
    DataQualityLibrary.check_not_null_values(target_data)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_duplicates(target_data):
    DataQualityLibrary.check_duplicates(target_data)