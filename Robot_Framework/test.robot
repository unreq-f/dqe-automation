*** Settings ***
Library    SeleniumLibrary
Library    helper.py
Library    BuiltIn
Library    Collections

*** Variables ***
${REPORT_FILE}      report.html
${DRIVER_PATH}      chromedriver-win64/chromedriver.exe
${PARQUET_FOLDER}   parquet_data
${FILTER_DATE}      2026-03-16
${DATE_COLUMN}      visit_date
${TABLE_NAME}       facility_type_avg_time_spent_per_visit_date

*** Test Cases ***
Diff_Report
    ${html_path}=    Evaluate    __import__('os').path.abspath(r'''${REPORT_FILE}''')
    Open Browser        ${html_path}        chrome
    ${table}=    Get WebElement    class=table
    ${html_df}=    Read Html Table To Dataframe    ${table}
    Log    ${html_df}
    ${parquet_path}=    Evaluate    __import__('os').path.abspath(r'''${PARQUET_FOLDER}''')
    ${parquet_data}=    Read Partitioned Parquet    ${PARQUET_FOLDER}    ${TABLE_NAME}
    ${filtered_parquet}=     Filter Dataframe    ${parquet_data}   ${DATE_COLUMN}      ${FILTER_DATE}
    ${filtered_HTML}=     Filter Dataframe    ${html_df}   Visit Date      ${FILTER_DATE}
    Log    ${filtered_parquet}
    Log    ${filtered_HTML}
    Check Missing Rows      ${filtered_parquet}     ${filtered_HTML}
    [Teardown]    Close Browser