"""
Analysis-specific readers for different output file formats.
"""

import re
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

from .base import BaseResultsReader

class AshesResults:
    """
    Ashes-specific results contracts using centralized column definitions.
    """
    # Sensor files
    SENSOR_FILENAME: dict[str, str] = {
        "Node": "Node [Node",
        "Loads": "Node loads [",
        "Fluid": "Node [Fluid kinematics]",
        "BeamElement": "Beam element [",
        "Rotor": "Rotor",
        "Generator": "Generator",
        "Sea": "Sea",
    }


class EigenResultsReader(BaseResultsReader):
    """Reader for eigenvalue analysis results."""

    def validate_case_folder(self, case_folder: Path) -> bool:
        """Check if folder contains expected eigen analysis outputs."""
        # Look for Eigenmodes subfolder
        eigenmodes_folder = case_folder / "Eigenmodes"
        if not eigenmodes_folder.exists():
            return False

        # Check for expected files
        eigenfreq_file = eigenmodes_folder / "Eigenfrequencies.txt"
        modeshapes_file = eigenmodes_folder / "Modeshapes.txt"

        return eigenfreq_file.exists() and modeshapes_file.exists()

    def read_case_results(
        self,
        case_folder: Path,
        read_target: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """Read eigenvalue analysis results from case folder."""
        results = {}
        eigenmodes_folder = case_folder / "Eigenmodes"

        if not eigenmodes_folder.exists():
            raise FileNotFoundError(
                f"Eigenmodes folder not found in {case_folder}"
            )

        # Read eigenfrequencies
        if read_target in (None, "eigenfrequencies"):
            eigenfreq_file = eigenmodes_folder / "Eigenfrequencies.txt"
            if eigenfreq_file.exists():
                results["eigenfrequencies"] = self._read_eigenfrequencies_file(
                    eigenfreq_file
                )

        # Read modeshapes - returns dictionary of DataFrames by mode
        if read_target in (None, "modeshapes"):
            modeshapes_file = eigenmodes_folder / "Modeshapes.txt"
            if modeshapes_file.exists():
                modeshapes_dict = self._read_modeshapes_file(modeshapes_file)
                # Add each mode directly to results
                results.update(modeshapes_dict)

        return results

    def _read_eigenfrequencies_file(self, file_path: Path) -> pd.DataFrame:
        """Read eigenfrequencies.txt file."""
        try:
            # Read the file, skipping comment lines
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # Find the header line (starts with # Mode)
            header_line_idx = None
            for i, line in enumerate(lines):
                if line.strip().startswith("# Mode"):
                    header_line_idx = i
                    break

            if header_line_idx is None:
                raise ValueError(
                    "Could not find header line in eigenfrequencies file"
                )

            # Extract column names from header (remove # and split)
            header_line = lines[header_line_idx].strip()
            columns = [
                col.strip()
                for col in header_line[1:].split("\t")
                if col.strip()
            ]

            # Read data lines (after header)
            data_lines = []
            for line in lines[header_line_idx + 1:]:
                line = line.strip()
                if line and not line.startswith("#"):
                    # Split by tab or multiple spaces
                    row_data = [
                        item.strip()
                        for item in re.split(r"\t+|\s{2,}", line)
                        if item.strip()
                    ]
                    if len(row_data) == len(columns):
                        data_lines.append(row_data)

            if not data_lines:
                raise ValueError("No data found in eigenfrequencies file")

            # Create DataFrame
            df = pd.DataFrame(data_lines, columns=columns)

            # Convert numeric columns
            numeric_columns = [
                "Mode",
                "Frequency [Hz]",
                "Period [s]",
                "Frequency [rad/s]",
            ]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            return df

        except Exception:
            return pd.DataFrame()

    def _read_modeshapes_file(
        self, file_path: Path
    ) -> Dict[str, pd.DataFrame]:
        """
        Read modeshapes.txt file and return dictionary of DataFrames for each
        mode.

        Returns:
            Dictionary mapping mode names (e.g., "Mode 1") to DataFrames with
                nodal data
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Split content by "Modeshape" sections
            sections = content.split("Modeshape\n")[
                1:
            ]  # Skip content before first Modeshape

            modeshapes_dict = {}

            for section in sections:
                if not section.strip():
                    continue

                mode_df = self._parse_modeshape_section(section)
                if mode_df is not None:
                    mode_num = self._extract_mode_number(section)
                    if mode_num is not None:
                        modeshapes_dict[f"Mode {mode_num}"] = mode_df

            return modeshapes_dict

        except Exception:
            return {}

    def _extract_mode_number(self, section: str) -> Optional[int]:
        """Extract mode number from section."""
        lines = section.strip().split("\n")
        for line in lines[:3]:  # Check first few lines
            if line.strip().startswith("Mode "):
                try:
                    return int(line.strip().split()[1])
                except (IndexError, ValueError):
                    continue
        return None

    def _parse_modeshape_section(self, section: str) -> Optional[pd.DataFrame]:
        """
        Parse a single modeshape section and return DataFrame with nodal data.

        Args:
            section: String content of a single mode section

        Returns:
            DataFrame with columns: Node_ID, Initial_pos_x, Initial_pos_y,
                Initial_pos_z, Displacement_x, Displacement_y, Displacement_z,
                Rotation_x, Rotation_y, Rotation_z, Node_name
        """
        lines = section.strip().split("\n")

        # Find the header line starting with "# Node (global id)"
        header_idx = None
        for i, line in enumerate(lines):
            if line.strip().startswith("# Node (global id)"):
                header_idx = i
                break

        if header_idx is None:
            return None

        # Define expected column names based on the file structure
        columns = [
            "Node_ID",
            "Initial_pos_x",
            "Initial_pos_y",
            "Initial_pos_z",
            "Displacement_x",
            "Displacement_y",
            "Displacement_z",
            "Rotation_x",
            "Rotation_y",
            "Rotation_z",
            "Node_name",
        ]

        # Parse data rows (after header)
        data_rows = []
        for line in lines[header_idx + 1:]:
            line = line.strip()
            if line and not line.startswith("#"):
                # Split by tabs or spaces
                row_parts = line.split("\t")

                # Clean and process row data
                if len(row_parts) >= 11:  # Ensure we have enough columns
                    # The last column (Node_name) might contain spaces,
                    # so handle specially
                    numeric_cols = row_parts[:10]  # First 10 are numeric
                    node_name = "\t".join(
                        row_parts[10:]
                    ).strip()  # Join remaining parts

                    row_data = numeric_cols + [node_name]
                    data_rows.append(row_data)

        if not data_rows:
            return None

        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=columns)

        # Convert numeric columns
        numeric_columns = [
            "Node_ID",
            "Initial_pos_x",
            "Initial_pos_y",
            "Initial_pos_z",
            "Displacement_x",
            "Displacement_y",
            "Displacement_z",
            "Rotation_x",
            "Rotation_y",
            "Rotation_z",
        ]

        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df


class DynamicResultsReader(BaseResultsReader):
    """Reader for dynamic analysis results."""

    @staticmethod
    def find_sensor_files(folder: Path, keyword: str) -> List[Path]:
        """Find sensor files in the given folder matching the keyword."""
        return [
            f
            for f in folder.iterdir()
            if f.name.startswith(keyword) and f.name.endswith(".txt")
        ]

    def validate_case_folder(self, case_folder: Path) -> bool:
        """Check if folder contains expected dynamic analysis outputs."""
        # Look for Time simulation subfolder
        time_sim_folder = case_folder / "Time simulation"
        if not time_sim_folder.exists():
            return False

        # Check for Node files
        keyword = AshesResults.SENSOR_FILENAME["Node"]
        node_files = self.find_sensor_files(time_sim_folder, keyword)

        # Check for Node loads files
        keyword = AshesResults.SENSOR_FILENAME["Loads"]
        node_loads_files = self.find_sensor_files(time_sim_folder, keyword)

        # Check for fluid kinematics files
        keyword = AshesResults.SENSOR_FILENAME["Fluid"]
        fluid_files = self.find_sensor_files(time_sim_folder, keyword)

        # Check for beam element files
        keyword = AshesResults.SENSOR_FILENAME["BeamElement"]
        beam_files = self.find_sensor_files(time_sim_folder, keyword)

        # Check for rotor sensor file
        keyword = AshesResults.SENSOR_FILENAME["Rotor"]
        rotor_files = self.find_sensor_files(time_sim_folder, keyword)

        # Check for generator sensor file
        keyword = AshesResults.SENSOR_FILENAME["Generator"]
        generator_files = self.find_sensor_files(time_sim_folder, keyword)

        # Check for sea sensor file
        keyword = AshesResults.SENSOR_FILENAME["Sea"]
        sea_files = self.find_sensor_files(time_sim_folder, keyword)

        return (
            len(node_files) > 0
            or len(node_loads_files) > 0
            or len(fluid_files) > 0
            or len(beam_files) > 0
            or len(rotor_files) > 0
            or len(generator_files) > 0
            or len(sea_files) > 0
        )

    def read_case_results(
        self, case_folder: Path, read_target: Optional[str] = None
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Read dynamic analysis results from a given load case folder.
        """
        results: Dict[str, Dict[str, pd.DataFrame]] = {
            "Nodes": {},
            "Loads": {},
            "Fluid": {},
            "BeamElements": {},
            "Rotor": {},
            "Generator": {},
            "Sea": {},       # Add this line
        }
        
        time_sim_folder = case_folder / "Time simulation"

        if not time_sim_folder.exists():
            raise FileNotFoundError(
                f"Time simulation folder not found in {case_folder}"
            )

        # Read Node files
        if read_target in (None, "Nodes"):
            keyword = AshesResults.SENSOR_FILENAME["Node"]
            node_files = self.find_sensor_files(time_sim_folder, keyword)
            for node_file in node_files:
                df = self._read_node_file(node_file)
                if not df.empty:
                    results["Nodes"][f"{self._extract_node_name(node_file)}"] = df

        # Read Node loads files
        if read_target in (None, "Loads"):
            keyword = AshesResults.SENSOR_FILENAME["Loads"]
            node_loads_files = self.find_sensor_files(time_sim_folder, keyword)
            for loads_file in node_loads_files:
                df = self._read_node_loads_file(loads_file)
                if not df.empty:
                    results["Loads"][f"{self._extract_node_name(loads_file)}"] = df

        # Read Node fluid kinematics files
        if read_target in (None, "Fluid"):
            keyword = AshesResults.SENSOR_FILENAME["Fluid"]
            fluid_files = self.find_sensor_files(time_sim_folder, keyword)
            for fluid_file in fluid_files:
                df = self._read_fluid_kinematics_file(fluid_file)
                if not df.empty:
                    results["Fluid"][f"{self._extract_node_name(fluid_file)}"] = df

        # Read Beam element files
        if read_target in (None, "BeamElements"):
            keyword = AshesResults.SENSOR_FILENAME["BeamElement"]
            beam_files = self.find_sensor_files(time_sim_folder, keyword)
            for beam_file in beam_files:
                df = self._read_beam_element_file(beam_file)
                if not df.empty:
                    element_name = self._extract_element_name(beam_file)
                    results["BeamElements"][element_name] = df

        # Read Rotor files
        if read_target in (None, "Rotor"):
            keyword = AshesResults.SENSOR_FILENAME["Rotor"]
            rotor_files = self.find_sensor_files(time_sim_folder, keyword)
            for rotor_file in rotor_files:
                df = self._read_rotor_sensor_file(rotor_file)
                if not df.empty:
                    rotor_name = self._extract_sensor_name(rotor_file)
                    results["Rotor"][rotor_name] = df

        # Read Generator files
        if read_target in (None, "Generator"):
            keyword = AshesResults.SENSOR_FILENAME["Generator"]
            generator_files = self.find_sensor_files(time_sim_folder, keyword)
            for generator_file in generator_files:
                df = self._read_generator_sensor_file(generator_file)
                if not df.empty:
                    generator_name = self._extract_sensor_name(generator_file)
                    results["Generator"][generator_name] = df

        # Read Sea files
        if read_target in (None, "Sea"):
            keyword = AshesResults.SENSOR_FILENAME["Sea"]
            sea_files = self.find_sensor_files(time_sim_folder, keyword)
            for sea_file in sea_files:
                df = self._read_sea_sensor_file(sea_file)
                if not df.empty:
                    sea_name = self._extract_sensor_name(sea_file)
                    results["Sea"][sea_name] = df

        return results

    def _read_node_file(self, file_path: Path) -> pd.DataFrame:
        """Read a Node file."""
        try:
            return self._read_ashes_sensor_file(file_path)
        except Exception:
            return pd.DataFrame()

    def _read_node_loads_file(self, file_path: Path) -> pd.DataFrame:
        """Read a Node loads file."""
        try:
            return self._read_ashes_sensor_file(file_path)
        except Exception:
            return pd.DataFrame()

    def _read_fluid_kinematics_file(self, file_path: Path) -> pd.DataFrame:
        """Read a Node fluid kinematics file."""
        try:
            return self._read_ashes_sensor_file(file_path)
        except Exception:
            return pd.DataFrame()

    def _read_beam_element_file(self, file_path: Path) -> pd.DataFrame:
        """Read a Beam element file."""
        try:
            return self._read_ashes_sensor_file(file_path)
        except Exception:
            return pd.DataFrame()
        
    def _read_rotor_sensor_file(self, file_path: Path) -> pd.DataFrame:
        """Read a Rotor sensor file."""
        try:
            return self._read_ashes_sensor_file(file_path)
        except Exception:
            return pd.DataFrame()
    
    def _read_generator_sensor_file(self, file_path: Path) -> pd.DataFrame:
        """Read a Generator sensor file."""
        try:
            return self._read_ashes_sensor_file(file_path)
        except Exception:
            return pd.DataFrame()
        
    def _read_sea_sensor_file(self, file_path: Path) -> pd.DataFrame:
        """Read a Sea sensor file."""
        try:
            return self._read_ashes_sensor_file(file_path)
        except Exception:
            return pd.DataFrame()

    def _read_ashes_sensor_file(self, file_path: Path) -> pd.DataFrame:
        """Generic method to read Ashes sensor files with similar format."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                lines = [line.rstrip() for line in f.readlines()]

            # Step 1: Find the "# Column" line
            column_section_start = None
            for i, line in enumerate(lines):
                if line.startswith("# Column"):
                    column_section_start = i
                    break

            if column_section_start is None:
                raise ValueError("Could not find '# Column' line")

            # Step 2: Extract column information
            # Line i+1: column numbers (1, 2, 3, ...)
            # Line i+2: column names (Time [s], Particle acc. ...)

            if column_section_start + 2 >= len(lines):
                raise ValueError("File too short to contain column headers")

            numbers_line = lines[column_section_start + 1].strip()
            headers_line = lines[column_section_start + 2].strip()

            # Parse column numbers to know how many columns to expect
            column_numbers = [
                num.strip() for num in numbers_line.split("\t") if num.strip()
            ]
            expected_col_count = len(column_numbers)

            # Parse column headers
            column_headers = [
                col.strip() for col in headers_line.split("\t") if col.strip()
            ]

            # Validate column count consistency
            if len(column_headers) != expected_col_count:
                # Sometimes there might be trailing tabs, try to fix
                all_headers = headers_line.split("\t")
                if len(all_headers) >= expected_col_count:
                    column_headers = [
                        col.strip() for col in all_headers[:expected_col_count]
                    ]
                else:
                    raise ValueError(
                        f"Column count mismatch: expected {expected_col_count}"
                        f",got {len(column_headers)}"
                    )

            # Step 3: Find data start (after second separator)
            data_start_idx = None
            separator_count = 0

            # Start looking from after the headers
            for i in range(column_section_start + 3, len(lines)):
                line = lines[i].strip()
                if line.startswith("---"):
                    separator_count += 1
                    if separator_count == 2:  # Found second separator
                        data_start_idx = i + 1
                        break

            if data_start_idx is None:
                raise ValueError("Could not find data start position")

            # Step 4: Read data
            data_rows = []
            for i in range(data_start_idx, len(lines)):
                line = lines[i].strip()
                if line:  # Skip empty lines
                    row_data = line.split("\t")

                    # Clean and validate row
                    row_data = [item.strip() for item in row_data]

                    # Handle cases where row has fewer columns
                    # (incomplete data)
                    if len(row_data) == expected_col_count:
                        data_rows.append(row_data)
                    elif len(row_data) < expected_col_count:
                        # Pad with empty strings and add
                        padded_row = row_data + [""] * (
                            expected_col_count - len(row_data)
                        )
                        data_rows.append(padded_row)
                    # Skip rows with too many columns (formatting issues)

            if not data_rows:
                return pd.DataFrame()

            # Step 5: Create DataFrame and convert to numeric
            df = pd.DataFrame(data_rows, columns=column_headers)

            # Convert all columns to numeric
            # for col in df.columns:
            #     df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.apply(pd.to_numeric, errors="coerce")
            # Remove rows where all values are NaN (failed conversions)
            df = df.dropna(how="all")

            return df

        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            return pd.DataFrame()

    def _extract_node_name(self, file_path: Path) -> str:
        """Extract node name from filename."""
        filename = file_path.name
        matches = re.findall(r"\[([^\]]+)\]", filename)
        if matches:
            # "Node [Node MP_20 Foundation].txt"
            # "Node loads [Node MP_20 Foundation].txt"
            # "Node [Fluid kinematics] [Node MP_20 Foundation].txt"
            node_name = matches[
                -1
            ]  # Take the last match which should be the node name
            return node_name.replace(" ", "_").replace("|", "_")
        return "unknown"

    def _extract_element_name(self, file_path: Path) -> str:
        """Extract element name from beam element filename."""
        filename = file_path.name
        matches = re.findall(r"\[([^\]]+)\]", filename)
        if matches:
            # "Beam element [Element MP1 Foundation].txt"
            element_name = matches[-1]  # Take the last match
            return element_name.replace(" ", "_").replace("|", "_")
        return "unknown"
    
    def _extract_sensor_name(self, file_path: Path) -> str:
        """Extract sensor name from sensor filename."""
        filename = file_path.name
        matches = filename
        if matches:
            sensor_name = matches[:-4]  # Take the last match
            return sensor_name.replace(" ", "_").replace("|", "_")
        return "unknown"
