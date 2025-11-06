"""
Base classes for postprocessing simulation results.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Union
import pandas as pd


class BaseResultsReader(ABC):
    """Abstract base class for reading simulation results."""

    def __init__(self, results_path: Union[str, Path]):
        self.results_path = Path(results_path)
        if not self.results_path.exists():
            raise FileNotFoundError(
                f"Results path does not exist: {self.results_path}"
            )

    @abstractmethod
    def read_case_results(self, case_folder: Path) -> Any:
        """Read results from a specific case folder."""

    @abstractmethod
    def validate_case_folder(self, case_folder: Path) -> bool:
        """Validate that case folder contains expected output files."""


class PostprocessingStrategy(ABC):
    """Abstract base class for analysis-specific postprocessing strategies."""

    @abstractmethod
    def process_results(
        self, raw_results: Dict[str, Dict[str, pd.DataFrame]]
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Process raw results and extract meaningful information."""
