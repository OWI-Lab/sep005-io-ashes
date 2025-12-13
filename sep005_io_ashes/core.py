"""
Main processor for simulation results across different analysis types.
"""

from pathlib import Path
from typing import Dict, List, Optional, Union
import re
import numpy as np
import pandas as pd
from .readers import (
    DynamicResultsReader,
    EigenResultsReader,
)
from .strategies import (
    DynamicPostprocessingStrategy,
    EigenPostprocessingStrategy,
)


class EigenResultsProcessor:
    """
    Processor for eigenvalue analysis results.

    Args:
        results_path: Path to the directory containing simulation results
        batch_data: Optional DataFrame with batch information to help locate
        results
    """

    def __init__(
        self,
        results_path: Union[str, Path],
        batch_data: Optional[pd.DataFrame] = None,
        analysis_folder_name: str = "Eigen analysis",
    ):
        self.results_path = Path(results_path)
        self.batch_data = batch_data
        self.analysis_folder_name = analysis_folder_name

        # Initialize reader and strategy
        self.reader = EigenResultsReader(self.results_path)
        self.strategy = EigenPostprocessingStrategy()

        # Find the analysis folder
        self.analysis_folder = self._find_analysis_folder()
        self._cache = {}

    def _find_analysis_folder(self) -> Path:
        """Find the analysis folder within results path."""
        analysis_folder = self.results_path / self.analysis_folder_name

        if not analysis_folder.exists():
            raise FileNotFoundError(
                f"Analysis folder '{self.analysis_folder_name}'"
                f"not found in {self.results_path}"
            )

        return analysis_folder

    def discover_case_folders(self) -> List[Path]:
        """
        Discover case folders in the analysis directory.

        Uses batch_data if available, otherwise scans directory.

        Returns:
            List of paths to case folders
        """
        if (
            self.batch_data is not None
            and "Load_case" in self.batch_data.columns
        ):
            # Use batch data to find specific case folders
            case_names = self.batch_data["Load_case"].tolist()
            case_folders = []

            for case_name in case_names:
                case_path = self.analysis_folder / case_name
                if case_path.exists() and case_path.is_dir():
                    if self.reader.validate_case_folder(case_path):
                        case_folders.append(case_path)

            return case_folders
        # Scan directory for all subdirectories that contain valid results
        case_folders = []
        for candidate_path in self.analysis_folder.iterdir():
            if (
                candidate_path.is_dir()
                and not candidate_path.name.startswith(".")
                and self.reader.validate_case_folder(candidate_path)
            ):
                case_folders.append(candidate_path)

        return sorted(case_folders)

    def read_all_eigenfrequencies(self) -> Dict[str, pd.DataFrame]:
        """
        Read eigenfrequencies from all case folders.

        Returns:
            Dictionary mapping case names to eigenfrequencies DataFrames
        """
        case_folders = self.discover_case_folders()
        eigenfreq_results = {}
        cache_key_base = "eigenfrequencies"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="eigenfrequencies")
                    self._cache[cache_key] = case_results

                if (
                    "eigenfrequencies" in case_results
                    and not case_results["eigenfrequencies"].empty
                ):
                    eigenfreq_results[case_name] = case_results[
                        "eigenfrequencies"
                    ]

            except Exception:
                continue

        return eigenfreq_results

    def get_consolidated_eigenfrequencies(self) -> pd.DataFrame:
        """
        Get consolidated eigenfrequencies across all cases into a single
        DataFrame.

        Returns:
            DataFrame with eigenfrequency data from all cases, with case name
                as additional column
        """
        eigenfreq_results = self.read_all_eigenfrequencies()

        if not eigenfreq_results:
            return pd.DataFrame()

        consolidated_data = []
        for case_name, eigenfreq_df in eigenfreq_results.items():
            df_copy = eigenfreq_df.copy()
            df_copy["Load_Case"] = case_name
            consolidated_data.append(df_copy)

        if not consolidated_data:
            return pd.DataFrame()

        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Reorder columns to put Load_Case first
        cols = ["Load_Case"] + [
            col for col in consolidated_df.columns if col != "Load_Case"
        ]
        consolidated_df = consolidated_df[cols]

        return consolidated_df

    def process_all_cases(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Process results from all discovered case folders.

        Returns:
            Dictionary mapping case names to their processed results
                dictionaries
        """
        case_folders = self.discover_case_folders()
        raw_results = {}
        cache_key_base = "eigen_all"

        # Read raw results from all cases
        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder)
                    self._cache[cache_key] = case_results

                raw_results[case_name] = case_results

            except Exception:
                continue

        # Apply analysis-specific postprocessing
        processed_results = self.strategy.process_results(raw_results)
        return processed_results

    def read_all_modeshapes(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Read modeshapes from all case folders.

        Returns:
            Nested dictionary: {case_name: {mode_name: DataFrame}}
        """
        case_folders = self.discover_case_folders()
        modeshapes_results = {}
        cache_key_base = "modeshapes"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="modeshapes")
                    self._cache[cache_key] = case_results

                # Extract only modeshape data (keys that start with "Mode ")
                case_modeshapes = {
                    key: df
                    for key, df in case_results.items()
                    if key.startswith("Mode ")
                }

                if case_modeshapes:
                    modeshapes_results[case_name] = case_modeshapes

            except Exception:
                continue

        return modeshapes_results

    def get_consolidated_modeshapes(self, mode_name: str) -> pd.DataFrame:
        """
        Get specific mode consolidated across all cases into a single
        DataFrame.

        Args:
            mode_name: Mode name (e.g., "Mode 1", "Mode 2")

        Returns:
            DataFrame with modeshape data from all cases, with case name
                as additional column
        """
        modeshapes_results = self.read_all_modeshapes()

        if not modeshapes_results:
            return pd.DataFrame()

        consolidated_data = []
        for case_name, case_modes in modeshapes_results.items():
            if mode_name in case_modes:
                mode_df = case_modes[mode_name].copy()
                mode_df["Load_Case"] = case_name
                consolidated_data.append(mode_df)

        if not consolidated_data:
            return pd.DataFrame()

        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Reorder columns to put Load_Case first
        cols = ["Load_Case"] + [
            col for col in consolidated_df.columns if col != "Load_Case"
        ]
        consolidated_df = consolidated_df[cols]

        return consolidated_df

    def list_available_modes(self) -> Dict[str, List[str]]:
        """
        Get overview of available modes for each case.

        Returns:
            Dictionary mapping case names to list of available mode names
        """
        modeshapes_results = self.read_all_modeshapes()

        return {
            case_name: list(modes.keys())
            for case_name, modes in modeshapes_results.items()
        }

    def clear_cache(self):
        """Call this if underlying data changes."""
        self._cache.clear()


class DynamicResultsProcessor:
    """
    Processor for dynamic analysis results.

    Args:
        results_path: Path to the directory containing simulation results
        batch_data: Optional DataFrame with batch information to help locate
            results
    """

    def __init__(
        self,
        results_path: Union[str, Path],
        batch_data: Optional[pd.DataFrame] = None,
        analysis_folder_name: str = "Wave regular",
    ):
        self.results_path = Path(results_path)
        self.batch_data = batch_data
        self.analysis_folder_name = analysis_folder_name

        # Initialize reader and strategy
        self.reader = DynamicResultsReader(self.results_path)
        self.strategy = DynamicPostprocessingStrategy()

        # Find the analysis folder
        self.analysis_folder = self._find_analysis_folder()
        self._cache = {}

    def _find_analysis_folder(self) -> Path:
        """Find the analysis folder within results path."""
        analysis_folder = self.results_path / self.analysis_folder_name

        if not analysis_folder.exists():
            raise FileNotFoundError(
                f"Analysis folder '{self.analysis_folder_name}' "
                f"not found in {self.results_path}"
            )

        return analysis_folder

    def discover_case_folders(self) -> List[Path]:
        """
        Discover case folders in the analysis directory.

        Uses batch_data if available, otherwise scans directory.

        Returns:
            List of paths to case folders
        """
        if (
            self.batch_data is not None
            and "Load_case" in self.batch_data.columns
        ):
            # Use batch data to find specific case folders
            case_names = self.batch_data["Load_case"].tolist()
            case_folders = []

            for case_name in case_names:
                case_path = self.analysis_folder / case_name
                if case_path.exists() and case_path.is_dir():
                    if self.reader.validate_case_folder(case_path):
                        case_folders.append(case_path)

            return case_folders
        # Scan directory for all subdirectories that contain valid results
        case_folders = []
        for candidate_path in self.analysis_folder.iterdir():
            if (
                candidate_path.is_dir()
                and not candidate_path.name.startswith(".")
                and self.reader.validate_case_folder(candidate_path)
            ):
                case_folders.append(candidate_path)

        return sorted(case_folders)

    def read_all_node_data(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Read sensor node data from all case folders.

        Returns:
            Nested dictionary: {case_name: {node_key: DataFrame}}
        """
        case_folders = self.discover_case_folders()
        node_results = {}
        cache_key_base = "Nodes"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Nodes")
                    self._cache[cache_key] = case_results

                # Extract only node data
                case_nodes = case_results["Nodes"]

                if case_nodes:
                    node_results[case_name] = case_nodes

            except Exception:
                continue

        return node_results

    def read_all_node_loads(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Read node loads data from all case folders.

        Returns:
            Nested dictionary: {case_name: {loads_key: DataFrame}}
        """
        case_folders = self.discover_case_folders()
        loads_results = {}
        cache_key_base = "Loads"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Loads")
                    self._cache[cache_key] = case_results

                # Extract only node loads data
                case_loads = case_results["Loads"]

                if case_loads:
                    loads_results[case_name] = case_loads

            except Exception:
                continue

        return loads_results

    def read_all_fluid_kinematics(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Read fluid kinematics data from all case folders.

        Returns:
            Nested dictionary: {case_name: {fluid_key: DataFrame}}
        """
        case_folders = self.discover_case_folders()
        fluid_results = {}
        cache_key_base = "Fluid"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Fluid")
                    self._cache[cache_key] = case_results

                # Extract only fluid kinematics data
                case_fluid = case_results["Fluid"]

                if case_fluid:
                    fluid_results[case_name] = case_fluid

            except Exception:
                continue

        return fluid_results

    def read_all_beam_elements(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Read beam element data from all case folders.

        Returns:
            Nested dictionary: {case_name: {element_key: DataFrame}}
        """
        case_folders = self.discover_case_folders()
        beam_results = {}
        cache_key_base = "BeamElements"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="BeamElements")
                    self._cache[cache_key] = case_results

                # Extract only beam element data
                case_beams = case_results["BeamElements"]

                if case_beams:
                    beam_results[case_name] = case_beams

            except Exception:
                continue

        return beam_results
    
    def read_all_rotor_sensors(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Read rotor data from all case folders.

        Returns:
            Nested dictionary: {case_name: {element_key: DataFrame}}
        """
        case_folders = self.discover_case_folders()
        rotor_results = {}
        cache_key_base = "Rotor"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Rotor")
                    self._cache[cache_key] = case_results

                # Extract only rotor sensor data
                case_rotor = case_results["Rotor"]

                if case_rotor:
                    rotor_results[case_name] = case_rotor

            except Exception:
                continue

        return rotor_results
    
    def read_all_generator_sensors(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Read generator sensor data from all case folders.

        Returns:
            Nested dictionary: {case_name: {element_key: DataFrame}}
        """
        case_folders = self.discover_case_folders()
        generator_results = {}
        cache_key_base = "Generator"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Generator")
                    self._cache[cache_key] = case_results

                # Extract only rotor sensor data
                case_generator = case_results["Generator"]

                if case_generator:
                    generator_results[case_name] = case_generator

            except Exception:
                continue

        return generator_results
    
    def read_all_sea_sensors(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Read sea sensor data from all case folders.

        Returns:
            Nested dictionary: {case_name: {element_key: DataFrame}}
        """
        case_folders = self.discover_case_folders()
        sea_results = {}
        cache_key_base = "Sea"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Sea")
                    self._cache[cache_key] = case_results

                # Extract only rotor sensor data
                case_sea = case_results["Sea"]

                if case_sea:
                    sea_results[case_name] = case_sea

            except Exception:
                continue

        return sea_results
    
    def read_all_fatigue_sensors(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Read Fatigue sensor data from all case folders.

        Returns:
            Nested dictionary: {case_name: {element_key: DataFrame}}
        """
        case_folders = self.discover_case_folders()
        fatigue_results = {}
        cache_key_base = "Fatigue"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Fatigue")
                    self._cache[cache_key] = case_results

                # Extract only rotor sensor data
                case_fatigue = case_results["Fatigue"]

                if case_fatigue:
                    fatigue_results[case_name] = case_fatigue

            except Exception:
                continue

        return fatigue_results

    def get_consolidated_node_data(self, node_name: str) -> pd.DataFrame:
        """
        Get specific node data consolidated across all cases.

        Args:
            node_name: Node identifier (e.g., "Node_MP_20_Foundation")

        Returns:
            DataFrame with node data from all cases, with case name as
                additional column
        """
        node_results = self.read_all_node_data()

        if not node_results:
            return pd.DataFrame()

        consolidated_data = []
        for case_name, case_nodes in node_results.items():
            # Use node_name directly, not prefixed
            if node_name in case_nodes:
                node_df = case_nodes[node_name].copy()
                node_df["Load_Case"] = case_name
                consolidated_data.append(node_df)

        if not consolidated_data:
            return pd.DataFrame()

        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Reorder columns to put Load_Case first
        cols = ["Load_Case"] + [
            col for col in consolidated_df.columns if col != "Load_Case"
        ]
        return consolidated_df[cols]

    def get_consolidated_node_loads(self, node_name: str) -> pd.DataFrame:
        """
        Get specific node loads consolidated across all cases.

        Args:
            node_name: Node identifier (e.g., "Node_MP_20_Foundation")

        Returns:
            DataFrame with node loads from all cases, with case name as
                additional column
        """
        loads_results = self.read_all_node_loads()

        if not loads_results:
            return pd.DataFrame()

        consolidated_data = []
        for case_name, case_loads in loads_results.items():
            # Use node_name directly, not prefixed
            if node_name in case_loads:
                loads_df = case_loads[node_name].copy()
                loads_df["Load_Case"] = case_name
                consolidated_data.append(loads_df)

        if not consolidated_data:
            return pd.DataFrame()

        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Reorder columns to put Load_Case first
        cols = ["Load_Case"] + [
            col for col in consolidated_df.columns if col != "Load_Case"
        ]
        return consolidated_df[cols]

    def get_consolidated_fluid_kinematics(
        self, node_name: str
    ) -> pd.DataFrame:
        """
        Get specific fluid kinematics consolidated across all cases.

        Args:
            node_name: Node identifier (e.g., "Node_MP_20_Foundation")

        Returns:
            DataFrame with fluid kinematics from all cases, with case name as
                additional column
        """
        fluid_results = self.read_all_fluid_kinematics()

        if not fluid_results:
            return pd.DataFrame()

        consolidated_data = []
        for case_name, case_fluid in fluid_results.items():
            # Use node_name directly, not prefixed
            if node_name in case_fluid:
                fluid_df = case_fluid[node_name].copy()
                fluid_df["Load_Case"] = case_name
                consolidated_data.append(fluid_df)

        if not consolidated_data:
            return pd.DataFrame()

        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Reorder columns to put Load_Case first
        cols = ["Load_Case"] + [
            col for col in consolidated_df.columns if col != "Load_Case"
        ]
        return consolidated_df[cols]

    def get_consolidated_beam_elements(
        self, element_name: str
    ) -> pd.DataFrame:
        """
        Get specific beam element data consolidated across all cases.

        Args:
            element_name: Element identifier (e.g., "Element_MP1_Foundation")

        Returns:
            DataFrame with beam element data from all cases, with case name as
                additional column
        """
        beam_results = self.read_all_beam_elements()

        if not beam_results:
            return pd.DataFrame()

        consolidated_data = []
        for case_name, case_beams in beam_results.items():
            # Use element_name directly, not prefixed
            if element_name in case_beams:
                beam_df = case_beams[element_name].copy()
                beam_df["Load_Case"] = case_name
                consolidated_data.append(beam_df)

        if not consolidated_data:
            return pd.DataFrame()

        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Reorder columns to put Load_Case first
        cols = ["Load_Case"] + [
            col for col in consolidated_df.columns if col != "Load_Case"
        ]
        return consolidated_df[cols]
    
    def get_consolidated_rotor_sensors(
        self, rotor_name: str
    ) -> pd.DataFrame:
        """
        Get specific rotor sensor data consolidated across all cases.

        Args:
            rotor_name: rotor identifier (e.g., "Rotor")

        Returns:
            DataFrame with rotor data from all cases, with case name as
                additional column
        """
        rotor_results = self.read_all_rotor_sensors()

        if not rotor_results:
            return pd.DataFrame()

        consolidated_data = []
        for case_name, case_rotor in rotor_results.items():
            # Use element_name directly, not prefixed
            if rotor_name in case_rotor:
                rotor_df = case_rotor[rotor_name].copy()
                rotor_df["Load_Case"] = case_name
                consolidated_data.append(rotor_df)

        if not consolidated_data:
            return pd.DataFrame()

        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Reorder columns to put Load_Case first
        cols = ["Load_Case"] + [
            col for col in consolidated_df.columns if col != "Load_Case"
        ]
        return consolidated_df[cols]
    
    def get_consolidated_generator_sensors(
        self, generator_name: str
    ) -> pd.DataFrame:
        """
        Get specific generator sensor data consolidated across all cases.

        Args:
            generator_name: rotor identifier (e.g., "Generator")

        Returns:
            DataFrame with rotor data from all cases, with case name as
                additional column
        """
        generator_results = self.read_all_generator_sensors()

        if not generator_results:
            return pd.DataFrame()

        consolidated_data = []
        for case_name, case_generator in generator_results.items():
            # Use element_name directly, not prefixed
            if generator_name in case_generator:
                generator_df = case_generator[generator_name].copy()
                generator_df["Load_Case"] = case_name
                consolidated_data.append(generator_df)

        if not consolidated_data:
            return pd.DataFrame()

        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Reorder columns to put Load_Case first
        cols = ["Load_Case"] + [
            col for col in consolidated_df.columns if col != "Load_Case"
        ]
        return consolidated_df[cols]

    def get_consolidated_sea_sensors(
        self, sea_name: str
    ) -> pd.DataFrame:
        """
        Get specific sea sensor data consolidated across all cases.

        Args:
            sea_name: sea identifier (e.g., "Sea")

        Returns:
            DataFrame with sea data from all cases, with case name as
                additional column
        """
        sea_results = self.read_all_sea_sensors()

        if not sea_results:
            return pd.DataFrame()

        consolidated_data = []
        for case_name, case_sea in sea_results.items():
            # Use element_name directly, not prefixed
            if sea_name in case_sea:
                sea_df = case_sea[sea_name].copy()
                sea_df["Load_Case"] = case_name
                consolidated_data.append(sea_df)

        if not consolidated_data:
            return pd.DataFrame()

        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Reorder columns to put Load_Case first
        cols = ["Load_Case"] + [
            col for col in consolidated_df.columns if col != "Load_Case"
        ]
        return consolidated_df[cols]
    
    def get_consolidated_fatigue_sensors(
        self, fatigue_name: str
    ) -> pd.DataFrame:
        """
        Get specific fatigue sensor data consolidated across all cases.

        Args:
            fatigue_name: fatigue identifier (e.g., "Fatigue")

        Returns:
            DataFrame with fatigue data from all cases, with case name as
                additional column
        """
        fatigue_results = self.read_all_fatigue_sensors()

        if not fatigue_results:
            return pd.DataFrame()

        consolidated_data = []
        for case_name, case_fatigue in fatigue_results.items():
            # Use element_name directly, not prefixed
            if fatigue_name in case_fatigue:
                fatigue_df = case_fatigue[fatigue_name].copy()
                fatigue_df["Load_Case"] = case_name
                consolidated_data.append(fatigue_df)

        if not consolidated_data:
            return pd.DataFrame()

        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Reorder columns to put Load_Case first
        cols = ["Load_Case"] + [
            col for col in consolidated_df.columns if col != "Load_Case"
        ]
        return consolidated_df[cols]

    def process_all_cases(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Process results from all discovered case folders.

        Returns:
            Dictionary mapping case names to their processed results d
                ictionaries
        """
        case_folders = self.discover_case_folders()
        raw_results = {}
        cache_key_base = "dynamic_all"

        # Read raw results from all cases
        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder)
                    self._cache[cache_key] = case_results
                # Flatten the nested dict
                flat_results = {}
                for key in ["Nodes", "Loads", "Fluid", "BeamElements", "Rotor", "Generator", "Sea", "Fatigue"]:
                    flat_results.update(case_results.get(key, {}))
                raw_results[case_name] = flat_results

            except Exception:
                continue

        # Now raw_results matches the expected type
        processed_results = self.strategy.process_results(raw_results)
        return processed_results

    def list_available_nodes(self) -> Dict[str, List[str]]:
        """
        Get overview of available nodes for each case.

        Returns:
            Dictionary mapping case names to list of available node names
        """
        all_results = {}
        case_folders = self.discover_case_folders()
        cache_key_base = "Nodes"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Nodes")
                    self._cache[cache_key] = case_results

                # Get node names from the 'Nodes' key
                node_names = list(case_results.get("Nodes", {}).keys())
                all_results[case_name] = sorted(node_names)

            except Exception:
                continue

        return all_results

    def list_available_beam_elements(self) -> Dict[str, List[str]]:
        """
        Get overview of available beam elements for each case.

        Returns:
            Dictionary mapping case names to list of available beam element
            names
        """
        all_results = {}
        case_folders = self.discover_case_folders()
        cache_key_base = "BeamElements"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="BeamElements")
                    self._cache[cache_key] = case_results

                # Get beam element names from the 'BeamElements' key
                beam_names = list(case_results.get("BeamElements", {}).keys())
                all_results[case_name] = sorted(beam_names)

            except Exception:
                continue

        return all_results

    def list_available_rotor_sensors(self) -> Dict[str, List[str]]:
        """
        Get overview of available rotor for each case.
 
        Returns:
            Dictionary mapping case names to list of available rotor sensor
            names
        """
        all_results = {}
        case_folders = self.discover_case_folders()
        cache_key_base = "Rotor"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Rotor")
                    self._cache[cache_key] = case_results

                # Get Rotor names from the 'Rotor' key
                rotor_names = list(case_results.get("Rotor", {}).keys())
                all_results[case_name] = sorted(rotor_names)

            except Exception:
                continue

        return all_results
    
    def list_available_generator_sensors(self) -> Dict[str, List[str]]:
        """
        Get overview of available generator sensors for each case.
 
        Returns:
            Dictionary mapping case names to list of available generator sensor
            names
        """
        all_results = {}
        case_folders = self.discover_case_folders()
        cache_key_base = "Generator"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Generator")
                    self._cache[cache_key] = case_results

                # Get Generator names from the 'Generator' key
                generator_names = list(case_results.get("Generator", {}).keys())
                all_results[case_name] = sorted(generator_names)

            except Exception:
                continue

        return all_results
    
    def list_available_sea_sensors(self) -> Dict[str, List[str]]:
        """
        Get overview of available sea for each case.
 
        Returns:
            Dictionary mapping case names to list of available sea sensor
            names
        """
        all_results = {}
        case_folders = self.discover_case_folders()
        cache_key_base = "Sea"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Sea")
                    self._cache[cache_key] = case_results

                # Get Sea names from the 'Sea' key
                sea_names = list(case_results.get("Sea", {}).keys())
                all_results[case_name] = sorted(sea_names)

            except Exception:
                continue

        return all_results
    
    def list_available_fatigue_sensors(self) -> Dict[str, List[str]]:
        """
        Get overview of available fatigue sensors for each case.
 
        Returns:
            Dictionary mapping case names to list of available fatigue sensor
            names
        """
        all_results = {}
        case_folders = self.discover_case_folders()
        cache_key_base = "Fatigue"

        for case_folder in case_folders:
            try:
                case_name = case_folder.name
                cache_key = cache_key_base + '_' + case_name
                if cache_key in self._cache:
                    case_results = self._cache[cache_key]
                else:
                    case_results = self.reader.read_case_results(case_folder, read_target="Fatigue")
                    self._cache[cache_key] = case_results

                # Get fatigue names from the 'Sea' key
                fatigue_names = list(case_results.get("Fatigue", {}).keys())
                all_results[case_name] = sorted(fatigue_names)

            except Exception:
                continue

        return all_results
    

    @staticmethod
    def df_to_sep005(
        df: pd.DataFrame,
    ) -> List[Dict[str, Union[str, float, np.ndarray]]]:
        """
        Convert a DataFrame to a list of SEP005 signal dictionaries.

        Args:
            df: DataFrame with simulation results. Each column is a signal, and
                the time column must be present.

        Returns:
            List of dictionaries, each representing a signal in SEP005 format.
        """

        # Find the time column (case-insensitive, matches 'Time [unit]')
        time_col = None
        for col in df.columns:
            if re.match(r"^Time\s*\[.*\]$", col, re.IGNORECASE):
                time_col = col
                break
        if time_col is None:
            raise ValueError("No time column found in DataFrame.")

        # Extract start timestamp (first value in time column)
        time_values = df[time_col].values
        start_timestamp = time_values[0]

        # Compute sampling frequency (fs) from time column
        if len(time_values) < 2:
            raise ValueError(
                "Time column must have at least two entries "
                "to compute sampling frequency."
            )
        dt = time_values[1] - time_values[0]
        fs = 1.0 / dt if dt != 0 else 0.0

        signals = {}
        for col in df.columns:
            if col == time_col:
                continue

            # Extract units from column name (pattern: [unit] at end)
            unit_match = re.search(r"\[([^\[\]]+)\]\s*$", col)
            unit_str = unit_match.group(1) if unit_match else ""

            # Clean channel name (remove units and extra spaces)
            channel_name = re.sub(r"\s*\[[^\[\]]+\]\s*$", "", col).strip()

            # Get signal data
            data = pd.to_numeric(df[col], errors="coerce").to_numpy()

            signal = {
                "name": channel_name,
                "data": data,
                "start_timestamp": str(start_timestamp),
                "fs": fs,
                "unit_str": unit_str,
            }
            signals[channel_name] = signal

        return signals

    def clear_cache(self):
        """Call this if underlying data changes."""
        self._cache.clear()
