"""
Analysis-specific postprocessing strategies.
"""
from typing import Dict
import pandas as pd
import numpy as np
from .base import PostprocessingStrategy


class EigenPostprocessingStrategy(PostprocessingStrategy):
    """Postprocessing strategy for eigenvalue analysis results."""

    def process_results(
        self, raw_results: Dict[str, Dict[str, pd.DataFrame]]
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Process eigenvalue analysis results."""
        processed = {}

        for case_name, case_data in raw_results.items():
            processed_case = {}

            # Process eigenfrequencies
            if (
                "eigenfrequencies" in case_data
                and not case_data["eigenfrequencies"].empty
            ):
                eigenfreq_df = case_data["eigenfrequencies"].copy()

                # Ensure Mode column is sorted
                if "Mode" in eigenfreq_df.columns:
                    eigenfreq_df = eigenfreq_df.sort_values(
                        "Mode"
                    ).reset_index(drop=True)

                # Add additional calculations
                if "Frequency [Hz]" in eigenfreq_df.columns:
                    freq_hz = eigenfreq_df["Frequency [Hz]"]

                    # Calculate additional metrics
                    eigenfreq_df["Period_s_calculated"] = (
                        1.0 / freq_hz.replace(0, np.nan)
                    )
                    eigenfreq_df["Frequency_rad_s_calculated"] = (
                        2 * np.pi * freq_hz
                    )

                processed_case["eigenfrequencies"] = eigenfreq_df

            # Process individual modeshapes
            modeshape_keys = [
                key for key in case_data.keys() if key.startswith("Mode ")
            ]
            for mode_key in modeshape_keys:
                mode_df = case_data[mode_key].copy()

                if not mode_df.empty:
                    # Sort by node ID if available
                    if "Node_ID" in mode_df.columns:
                        mode_df = mode_df.sort_values("Node_ID").reset_index(
                            drop=True
                        )

                    processed_case[mode_key] = mode_df

            processed[case_name] = processed_case

        return processed


class DynamicPostprocessingStrategy(PostprocessingStrategy):
    """Postprocessing strategy for dynamic analysis results."""

    def process_results(
        self, raw_results: Dict[str, Dict[str, pd.DataFrame]]
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Process dynamic analysis results."""
        processed = {}

        for case_name, case_data in raw_results.items():
            processed_case = {}

            # Process node data
            node_keys = [
                key
                for key in case_data.keys()
                if key.startswith("node_")
                and not key.startswith("node_loads_")
            ]
            for node_key in node_keys:
                node_df = case_data[node_key].copy()
                if not node_df.empty:
                    # Basic time-series formatting
                    if "Time [s]" in node_df.columns:
                        # Ensure time is sorted
                        node_df = node_df.sort_values("Time [s]").reset_index(
                            drop=True
                        )

                    processed_case[node_key] = node_df

            # Process node loads data
            loads_keys = [
                key
                for key in case_data.keys()
                if key.startswith("node_loads_")
            ]
            for loads_key in loads_keys:
                loads_df = case_data[loads_key].copy()
                if not loads_df.empty:
                    # Basic time-series formatting
                    if "Time [s]" in loads_df.columns:
                        # Ensure time is sorted
                        loads_df = loads_df.sort_values(
                            "Time [s]"
                        ).reset_index(drop=True)

                    processed_case[loads_key] = loads_df

            # Process fluid kinematics data
            fluid_keys = [
                key
                for key in case_data.keys()
                if key.startswith("fluid_kinematics_")
            ]
            for fluid_key in fluid_keys:
                fluid_df = case_data[fluid_key].copy()
                if not fluid_df.empty:
                    # Basic time-series formatting
                    if "Time [s]" in fluid_df.columns:
                        # Ensure time is sorted
                        fluid_df = fluid_df.sort_values(
                            "Time [s]"
                        ).reset_index(drop=True)

                    processed_case[fluid_key] = fluid_df

            # Process beam element data
            beam_keys = [
                key
                for key in case_data.keys()
                if key.startswith("beam_element_")
            ]
            for beam_key in beam_keys:
                beam_df = case_data[beam_key].copy()
                if not beam_df.empty:
                    # Basic time-series formatting
                    if "Time [s]" in beam_df.columns:
                        # Ensure time is sorted
                        beam_df = beam_df.sort_values(
                            "Time [s]"
                        ).reset_index(drop=True)

                    processed_case[beam_key] = beam_df

            processed[case_name] = processed_case

        return processed
