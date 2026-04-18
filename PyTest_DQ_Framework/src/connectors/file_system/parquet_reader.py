import pandas as pd
import pathlib
from typing import Optional, List

class LocalParquetConnector:
    def __init__(self, base_path: str):
        """
        :param base_path: path to local data
        """
        self.base_path = pathlib.Path(base_path)

    def read_table(self, table_name: str, filters: Optional[list] = None) -> pd.DataFrame:
        """
        :param table_name: table name inside data path
        :param filters: optional filters for reading parquet
        :return: Pandas DataFrame
        """
        table_path = self.base_path / table_name

        if not table_path.exists():
            raise FileNotFoundError(f"Путь {table_path} не найден")

        df = pd.read_parquet(
            table_path,
            engine='pyarrow',
            filters=filters
        )
        return df

    def get_partition_values(self, table_name: str, partition_col: str) -> List[str]:
        """
        get list partition
        :param table_name:
        :param partition_col:
        :return:
        """
        table_path = self.base_path / table_name
        return [p.name.split('=')[1] for p in table_path.glob(f"{partition_col}=*")]