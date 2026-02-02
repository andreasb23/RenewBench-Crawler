"""ICON-DREAM Global NWP data downloader.

Download ICON-DREAM Global reanalysis data from DWD open data portal.
"""

import pickle
import re
from pathlib import Path
from typing import Optional

import numpy as np
import requests
from loguru import logger


class IconDreamGlobalDownloader:
    """ICON-DREAM Global NWP data downloader.

    Downloads hourly ICON-DREAM Global weather data from DWD open data portal.
    Supports checkpoint-based resume and dry-run modes.

    Attributes:
        years (list[int]): List of years to download data for.
        months (list[str]): List of months to download data for (01-12).
        variables (list[str]): List of variables to download.
        output_path (Path): Path to the output directory.
        checkpoint_path (Path): Path to the checkpoint file for resuming.
        checkpoint (np.ndarray): Array tracking download status (0=not done, 1=done).
        dry_run (bool): If True, print requests without downloading.
        resume (bool): If True, resume from previous checkpoint.
        available_variables (set[str]): Set of available variables from DWD.
        available_dates (dict): Dict of variables to available year-months.
    """

    # Base URL for ICON-DREAM Global data
    BASE_URL = (
        "https://opendata.dwd.de/climate_environment/REA/ICON-DREAM-Global/hourly"
    )

    # Type annotations
    years: list[int]
    months: list[str]
    variables: list[str]
    output_path: Path
    checkpoint_path: Path
    checkpoint: np.ndarray
    dry_run: bool
    resume: bool
    available_variables: set[str]
    available_dates: dict

    def __init__(
        self,
        output_path: Path,
        years: list[int],
        months: Optional[list[str]] = None,
        variables: Optional[list[str]] = None,
        dry_run: bool = False,
        resume: bool = False,
    ) -> None:
        """Initialize the IconDreamGlobalDownloader.

        Args:
            output_path (Path): Path to the output directory.
            years (list[int]): List of years to download.
            months (list[str], optional): List of months (01-12). Defaults to all months.
            variables (list[str], optional): List of variables. Defaults to common variables.
            dry_run (bool, optional): If True, print requests without downloading. Defaults to False.
            resume (bool, optional): If True, resume from checkpoint. Defaults to False.

        Raises:
            ValueError: If invalid parameters are provided.
        """
        self.years = years
        self.months = (
            months if months is not None else [f"{i:02d}" for i in range(1, 13)]
        )
        self.variables = (
            variables if variables is not None else self._get_default_variables()
        )
        self.dry_run = dry_run
        self.resume = resume

        self.output_path = Path(output_path)
        self.checkpoint_path = self.output_path / "status.pickle"

        # Create output directory if it doesn't exist
        self.output_path.mkdir(parents=True, exist_ok=True)

        dry_run_str = " [DRY RUN - NO DATA WILL BE DOWNLOADED]" if self.dry_run else ""
        logger.info(
            f"ICON-DREAM Downloader initialised for:{dry_run_str}"
            f"\n- years:\t\t{years}"
            f"\n- months:\t\t{self.months}"
            f"\n- variables:\t\t{self.variables}"
        )

        # Discover available data from DWD
        logger.info("Discovering available data from DWD...")
        self.available_variables = self._discover_available_variables()
        logger.info(f"Found {len(self.available_variables)} available variables")

        # Validate variables
        self._validate_variables()

        # Initialize or load checkpoint
        if resume and self.checkpoint_path.is_file():
            with open(self.checkpoint_path, "rb") as f:
                self.checkpoint = pickle.load(f)
            logger.info(f"Resuming from checkpoint: {self.checkpoint_path}")
        else:
            # Create checkpoint: (years, months, variables)
            self.checkpoint = np.zeros(
                (len(self.years), len(self.months), len(self.variables)), dtype=int
            )
            logger.info("Starting fresh download (no checkpoint found).")

    def _get_default_variables(self) -> list[str]:
        """Get default variables to download.

        Returns:
            list[str]: List of default variables.
        """
        # Default: common variables like temperature
        return ["T"]  # Temperature is the most common variable

    def _discover_available_variables(self) -> set[str]:
        """Discover available variables from DWD open data portal.

        Returns:
            set[str]: Set of available variable codes (e.g., {'T', 'U', 'V', 'T_2M', ...}).
        """
        try:
            # List directory to find available variables
            response = requests.get(self.BASE_URL, timeout=30)
            response.raise_for_status()

            # Extract variable folder names from HTML directory listing
            # Looking for links like: <a href="T/">T/</a>
            pattern = r'href="([A-Z0-9_]+)/"'
            matches = re.findall(pattern, response.text)
            # Filter out parent directory (..)
            variables = set(m for m in matches if m != "..")

            if not variables:
                logger.warning("No variables found in DWD directory, using defaults")
                return self._get_default_variable_set()

            logger.info(f"Discovered {len(variables)} available variables from DWD")
            return variables

        except Exception as e:
            logger.warning(f"Error discovering variables: {e}, using defaults")
            return self._get_default_variable_set()

    @staticmethod
    def _get_default_variable_set() -> set[str]:
        """Get the default set of variables as fallback.

        Returns:
            set[str]: Set of commonly available ICON-DREAM variables.
        """
        return {
            # 3D Pressure-Level Variables
            "T",
            "U",
            "V",
            "W",
            "P",
            "QV",
            "TKE",
            "WS",
            "DEN",
            # 2D Surface Variables
            "T_2M",
            "U_10M",
            "V_10M",
            "TD_2M",
            "TOT_PREC",
            "PS",
            "PMSL",
            "CLCT",
            "ASWDIR_S",
            "ASWDIFD_S",
            "QV_S",
            "TMAX_2M",
            "TMIN_2M",
            "VMAX_10M",
            "WS_10M",
            "Z0",
        }

    def _validate_variables(self) -> None:
        """Validate that all requested variables are available.

        Raises:
            ValueError: If any requested variable is not available.
        """
        invalid_vars = [v for v in self.variables if v not in self.available_variables]

        if invalid_vars:
            raise ValueError(
                f"Invalid variables: {', '.join(invalid_vars)}. "
                f"Available variables: {', '.join(sorted(self.available_variables))}"
            )

        logger.info(f"All {len(self.variables)} requested variables are available.")

    def download_data(self) -> None:
        """Download ICON-DREAM data for all specified years, months, and variables."""
        for year_idx, year in enumerate(self.years):
            logger.info(f"Processing year {year}...")

            for month_idx, month in enumerate(self.months):
                for var_idx, variable in enumerate(self.variables):
                    # Check checkpoint
                    if self.checkpoint[year_idx, month_idx, var_idx] != 0:
                        logger.info(
                            f"{year}-{month} ({variable}): Already downloaded, skipping"
                        )
                        continue

                    # Download the file
                    status = self._download_file(year, month, variable)

                    # Update checkpoint
                    if status == 1:
                        self.checkpoint[year_idx, month_idx, var_idx] = 1
                        self._save_checkpoint()

        logger.info("All downloads completed!")

    def _download_file(self, year: int, month: str, variable: str) -> int:
        """Download a single data file.

        Args:
            year (int): Year to download.
            month (str): Month to download (format: '01' to '12').
            variable (str): Variable code (e.g., 'T').

        Returns:
            int: 1 if successful, 0 if failed.
        """
        # Build filename and URL
        filename = f"ICON-DREAM-Global_{year}{month}_{variable}_hourly.grb"
        url = f"{self.BASE_URL}/{variable}/{filename}"
        output_file = self.output_path / filename

        # Check if file already exists
        if output_file.exists():
            logger.info(
                f"{year}-{month} ({variable}): File already exists locally, skipping"
            )
            return 1

        if self.dry_run:
            logger.info(
                f"{year}-{month} ({variable}): DRY RUN - Would download from {url}"
            )
            return 1

        try:
            logger.info(f"{year}-{month} ({variable}): Downloading {filename}...")

            # Download with streaming
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()

            # Get total file size
            total_size = int(response.headers.get("content-length", 0))
            size_gb = total_size / (1024**3)

            logger.info(f"{year}-{month} ({variable}): File size: {size_gb:.2f} GB")

            # Download with progress tracking
            downloaded = 0
            chunk_size = 8192  # 8KB chunks

            with open(output_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Log progress every 100MB
                        if downloaded % (100 * 1024 * 1024) == 0:
                            progress = (
                                (downloaded / total_size * 100) if total_size > 0 else 0
                            )
                            logger.info(
                                f"{year}-{month} ({variable}): "
                                f"Downloaded {downloaded / (1024**3):.2f}GB / "
                                f"{size_gb:.2f}GB ({progress:.1f}%)"
                            )

            logger.info(
                f"{year}-{month} ({variable}): Successfully downloaded to {output_file}"
            )
            return 1

        except requests.exceptions.RequestException as e:
            logger.error(f"{year}-{month} ({variable}): Download failed: {e}")
            # Clean up partial file
            if output_file.exists():
                output_file.unlink()
            return 0
        except Exception as e:
            logger.error(f"{year}-{month} ({variable}): Error: {e}")
            if output_file.exists():
                output_file.unlink()
            return 0

    def _save_checkpoint(self) -> None:
        """Save checkpoint to disk."""
        with open(self.checkpoint_path, "wb") as f:
            pickle.dump(self.checkpoint, f)

    @staticmethod
    def print_available_variables() -> None:
        """Print all available ICON-DREAM Global variables."""
        print("\n" + "=" * 80)
        print("AVAILABLE ICON-DREAM GLOBAL VARIABLES")
        print("=" * 80)
        print("Dataset: ICON-DREAM-Global (DWD Open Data)")
        print("Resolution: ~13km (icosahedral grid)")
        print("Temporal: Hourly data")
        print("Time period: 2010-01 to present\n")

        # Try to discover available variables
        try:
            downloader = IconDreamGlobalDownloader(
                output_path=Path("/tmp"),
                years=[2020],
            )
            variables = sorted(downloader.available_variables)
            print(f"Available variables ({len(variables)}):\n")

            # Group variables by type
            pressure_3d = [
                v
                for v in variables
                if v in ["T", "U", "V", "P", "QV", "W", "TKE", "WS", "DEN"]
            ]
            surface_2d = [v for v in variables if v != ".." and v not in pressure_3d]

            if pressure_3d:
                print("  3D Pressure-Level Variables:")
                for var in sorted(pressure_3d):
                    print(f"    - {var}")
                print()

            if surface_2d:
                print("  2D Surface Variables:")
                for var in sorted(surface_2d):
                    print(f"    - {var}")

        except Exception as e:
            logger.error(f"Could not discover variables: {e}")
            print("Common ICON-DREAM variables:")
            default_vars = IconDreamGlobalDownloader._get_default_variable_set()
            for var in sorted(default_vars):
                print(f"  - {var}")

        print("\n" + "=" * 80)
