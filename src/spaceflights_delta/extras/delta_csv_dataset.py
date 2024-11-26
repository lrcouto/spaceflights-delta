import pandas as pd
from deltalake import DeltaTable
from kedro.io import AbstractDataset
from typing import Any, Dict, Optional

class DeltaCSVDataset(AbstractDataset):
    def __init__(
        self,
        filepath: str,
        delta_table_path: str,
        load_args: Optional[Dict[str, Any]] = None,
        save_args: Optional[Dict[str, Any]] = None,
    ):
        self._filepath = filepath
        self._delta_table_path = delta_table_path
        self._load_args = load_args or {}
        self._save_args = save_args or {}

    def _load(self) -> pd.DataFrame:
        delta_table = DeltaTable(self._delta_table_path)
        return delta_table.to_pandas()

    def _save(self, data: pd.DataFrame) -> None:
        delta_table = DeltaTable(self._delta_table_path)
        delta_table.write(data, mode="overwrite")

    def _describe(self) -> Dict[str, Any]:
        """Describe the dataset."""
        return {
            "filepath": self._filepath,
            "delta_table_path": self._delta_table_path,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    def from_csv(self) -> None:
        """Load the CSV into the Delta table."""
        df = pd.read_csv(self._filepath)
        self._save(df)
