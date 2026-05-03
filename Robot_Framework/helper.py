import pandas as pd
import os
import pathlib
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

#functions from previous module
#read html table
def read_html_table_to_dataframe(table):
    data = {}
    columns = table.find_elements(By.CLASS_NAME, "y-column")
    for item in columns:
        by_xpath = item.find_element(By.XPATH, './/*[@id="header"]')
        rows = item.find_elements(By.CLASS_NAME, "column-cell")
        lst = [row.text for row in rows if row.text != by_xpath.text]
        data[by_xpath.text] = lst
    df = pd.DataFrame(data)
    return df


#read local data
def read_partitioned_parquet(base_path, table_name):
    table_path = pathlib.Path(base_path) / table_name
    if not table_path.exists():
        raise FileNotFoundError(f"path {table_path} not found")
    df = pd.read_parquet(table_path, engine='pyarrow')
    return df

def filter_dataframe(df, column, value):
    return df[df[column] == value]


# find diff
def check_missing_rows(source: pd.DataFrame, target: pd.DataFrame, key_columns=None):
    """
Checks that all rows from the source DataFrame are present in the target DataFrame.
If rows are missing, it outputs them.
:param source: source DataFrame
:param target: target DataFrame
:param key_columns: list of columns to compare (if None, compares all columns)
    """
    if key_columns is None:
        mask = ~source.apply(tuple, 1).isin(target.apply(tuple, 1))
        missing = source[mask]
    else:
        merged = pd.merge(source[key_columns], target[key_columns], how='left', indicator=True)
        missing = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    assert missing.empty, f"Missing rows in target:\n{missing}"



def read_single_partition(base_path, table_name, partition_value):
    partition_path = pathlib.Path(base_path) / table_name / f"partition_date={partition_value}"
    if not partition_path.exists():
        raise FileNotFoundError(f"path {partition_path} not found")
    df = pd.read_parquet(partition_path, engine='pyarrow')
    return df

def normalize_columns(df):
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df