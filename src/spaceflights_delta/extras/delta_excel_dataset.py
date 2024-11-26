import os
from kedro.io import AbstractDataset
import pandas as pd
from deltalake.writer import write_deltalake
from deltalake.table import DeltaTable

class DeltaExcelDataset(AbstractDataset):
    def __init__(self, filepath: str, delta_table_path: str, load_args=None, save_args=None):
        self._filepath = filepath
        self._delta_table_path = delta_table_path
        self._load_args = load_args or {}
        self._save_args = save_args or {}

    def _load(self) -> pd.DataFrame:
            if not os.path.exists(self._delta_table_path):
                raise ValueError(f"The path {self._delta_table_path} does not exist.")
    
            try:
                table = DeltaTable(self._delta_table_path)
                return table.to_pandas()
            except Exception as e:
                raise ValueError(f"Failed to load Delta table from {self._delta_table_path}: {e}")

    def _save(self, data: pd.DataFrame) -> None:
        write_deltalake(self._delta_table_path, data, mode="overwrite", **self._save_args)

    def _describe(self) -> dict:
        return {
            "filepath": self._filepath,
            "delta_table_path": self._delta_table_path,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }
