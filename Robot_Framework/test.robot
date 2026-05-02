*** Settings ***
Library    SeleniumLibrary
Library    helper.py
Library    BuiltIn
Library    Collections

*** Variables ***
${REPORT_FILE}      report.html
${DRIVER_PATH}      chromedriver-win64/chromedriver.exe
${PARQUET_FOLDER}   parquet_data
@{FILTER_DATE}      2026-03-19    2026-03-18     2026-03-17      2026-03-16
${DATE_COLUMN}      visit_date
${TABLE_NAME}       facility_type_avg_time_spent_per_visit_date

*** Test Cases ***
Read_HTML_debug
    ${html_path}=    Evaluate    __import__('os').path.abspath(r'''${REPORT_FILE}''')
    Log     ${html_path}
    ${driver_path}=    Evaluate    __import__('os').path.abspath(r'''${DRIVER_PATH}''')
    Log     ${driver_path}
    ${df1}=     Read Html Table To Dataframe   ${html_path}    ${driver_path}
    Log         ${df1}

Read_parquet_debug
    FOR  ${filter_date}     IN      @{FILTER_DATE}
        ${abs_path1}=    Evaluate    __import__('os').path.abspath(r'''${PARQUET_FOLDER}''')
        ${df}=    Read Partitioned Parquet    ${PARQUET_FOLDER}    ${TABLE_NAME}
        Log     ${df}
        ${filtered_df}=     Filter Dataframe    ${df}   ${DATE_COLUMN}      ${filter_date}
        Log     ${filtered_df}
    END

Compare_df
    ${html_path}=    Evaluate    __import__('os').path.abspath(r'''${REPORT_FILE}''')
    Log     ${html_path}
    ${driver_path}=    Evaluate    __import__('os').path.abspath(r'''${DRIVER_PATH}''')
    Log     ${driver_path}
    ${df_html}=     Read Html Table To Dataframe   ${html_path}    ${driver_path}
    Log         ${df_html}
    ${parquet_path}=    Evaluate    __import__('os').path.abspath(r'''${PARQUET_FOLDER}''')
    ${parquet_data}=    Read Partitioned Parquet    ${PARQUET_FOLDER}    ${TABLE_NAME}
    Log     ${parquet_data}
    FOR  ${filter_date}     IN      @{FILTER_DATE}
            ${filtered_parquet}=     Filter Dataframe    ${parquet_data}   ${DATE_COLUMN}      ${filter_date}
            ${filtered_HTML}=     Filter Dataframe    ${parquet_data}   ${DATE_COLUMN}      ${filter_date}
            Log     ${filtered_parquet}
            Log     ${filtered_HTML}
            Compare Dataframes And Return Diff      ${filtered_parquet}         ${filtered_HTML}
    END


