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
def read_html_table_to_dataframe(html_path, driver_path):
    service = Service(executable_path=driver_path)
    options = Options()
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(html_path)
        driver.set_window_size(1920, 1080)
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, "table")))
        data = {}
        table = driver.find_element(By.CLASS_NAME, "table")
        columns = table.find_elements(By.CLASS_NAME, "y-column")
        for item in columns:
            by_xpath = item.find_element(By.XPATH, './/*[@id="header"]')
            row_element = item.find_element(By.ID, "cells1")
            rows = item.find_elements(By.CLASS_NAME, "column-cell")
            lst = [row.text for row in rows if row.text != by_xpath.text]
            data[by_xpath.text] = lst
        df = pd.DataFrame(data)
        return df
    finally:
        driver.quit()

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
def compare_dataframes_and_return_diff(df1, df2):
    try:
        pd.testing.assert_frame_equal(df1, df2)
        return None
    except AssertionError as e:
        diff = df1.compare(df2)
        return diff

def read_single_partition(base_path, table_name, partition_value):
    partition_path = pathlib.Path(base_path) / table_name / f"partition_date={partition_value}"
    if not partition_path.exists():
        raise FileNotFoundError(f"path {partition_path} not found")
    df = pd.read_parquet(partition_path, engine='pyarrow')
    return df