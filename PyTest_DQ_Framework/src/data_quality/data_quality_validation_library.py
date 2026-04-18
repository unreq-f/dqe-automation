import pandas as pd

class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.
    """
    @staticmethod
    def check_duplicates(df: pd.DataFrame, column_names=None):
        if column_names:
            duplicates = df.duplicated(subset=column_names)
        else:
            duplicates = df.duplicated()
        assert not duplicates.any(), f"Found duplicates in columns {column_names or 'all columns'}, num_duplicates = {df.duplicated().sum()}"

    @staticmethod
    def check_count(df1: pd.DataFrame, df2: pd.DataFrame):
        assert len(df1) == len(df2), f"Row count mismatch: {len(df1)} != {len(df2)}"

    @staticmethod
    def check_data_full_data_set(df1: pd.DataFrame, df2: pd.DataFrame):
        pd.testing.assert_frame_equal(df1, df2)

    @staticmethod
    def check_dataset_is_not_empty(df: pd.DataFrame):
        assert not df.empty, "DataFrame is empty"

    @staticmethod
    def check_not_null_values(df: pd.DataFrame, column_names=None):
        columns = column_names or df.columns
        for col in columns:
            assert df[col].notnull().all(), f"Column '{col}' contains null values"

    @staticmethod
    def check_pattern(df: pd.DataFrame, pattern: str, column: str):
        assert column in df.columns, f"Column '{column}' not found in DataFrame"
        matches = df[column].astype(str).str.match(pattern)
        assert matches.all(), f"Found not valid data in column '{column}': {df[~matches][column].tolist()}"

    @staticmethod
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