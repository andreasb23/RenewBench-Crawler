"""ERA5 NWP DATA DOWNLOADER.

Remote API access of ERA5 reanalysis data using the cdsapi package.
"""

import pickle
from pathlib import Path

import cdsapi  # type: ignore[import-untyped]
import numpy as np
from loguru import logger


class Era5Downloader:
    """ERA5 NWP data downloader.

    Attributes:
        years (list[int]): List of years to get data for.
        months (list[str]): List of months to get data for.
        variables (list[str]): List of ERA5 variables to download.
        area (list[float]): Bounding box [North, West, South, East] in degrees.
        pressure_levels (list[str]): List of pressure levels to download (for 3D variables).
        model_levels (list[str]): List of model levels to download (for 3D variables).
        output_path (Path): Path to the output directory.
        checkpoint_path (Path): Path to the checkpoint file for resuming.
        checkpoint (np.array): Array of 0 and 1 values for resuming.
    """

    # Type annotations for instance attributes
    years: list[int]
    months: list[str]
    variables: list[str]
    area: list[float] | None
    pressure_levels: list[str] | None
    model_levels: list[str] | None
    output_path: Path
    checkpoint_path: Path
    checkpoint: np.ndarray
    client: cdsapi.Client
    dry_run: bool
    file_format: str
    file_extension: str
    resolution: str

    # Common ERA5 variables for renewable energy applications
    DEFAULT_VARIABLES = [
        # 2D surface variables
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "100m_u_component_of_wind",
        "100m_v_component_of_wind",
        "2m_temperature",
        "surface_solar_radiation_downwards",
        "surface_pressure",
        "total_precipitation",
        # 3D variables at pressure levels
        "temperature",
        "u_component_of_wind",
        "v_component_of_wind",
        "relative_humidity",
        "geopotential",
    ]

    # 2D single-level variables (no vertical levels needed)
    # All available single-level ERA5 variables
    ALL_SINGLE_LEVEL_VARIABLES = {
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "100m_u_component_of_wind",
        "100m_v_component_of_wind",
        "2m_dewpoint_temperature",
        "2m_temperature",
        "boundary_layer_height",
        "convective_available_potential_energy",
        "convective_precipitation",
        "eastward_turbulent_surface_stress",
        "evaporation",
        "friction_velocity",
        "geopotential_at_surface",
        "high_vegetation_cover",
        "instantaneous_10m_wind_direction",
        "instantaneous_10m_wind_speed",
        "instantaneous_eastward_turbulent_surface_stress",
        "instantaneous_northward_turbulent_surface_stress",
        "land_sea_mask",
        "large_scale_precipitation",
        "leaf_area_index_high_vegetation",
        "leaf_area_index_low_vegetation",
        "low_vegetation_cover",
        "mean_sea_level_pressure",
        "mean_surface_downward_long_wave_radiation_flux",
        "mean_surface_downward_short_wave_radiation_flux",
        "mean_surface_latent_heat_flux",
        "mean_surface_sensible_heat_flux",
        "mean_top_downward_long_wave_radiation_flux",
        "mean_top_downward_short_wave_radiation_flux",
        "mean_top_net_long_wave_radiation_flux",
        "mean_top_net_short_wave_radiation_flux",
        "northward_turbulent_surface_stress",
        "potential_evaporation",
        "runoff",
        "sea_ice_cover",
        "sea_surface_temperature",
        "skin_reservoir_content",
        "skin_temperature",
        "snow_cover",
        "snow_depth",
        "snowfall",
        "soil_temperature_level_1",
        "soil_temperature_level_2",
        "soil_temperature_level_3",
        "soil_temperature_level_4",
        "soil_type",
        "surface_latent_heat_flux",
        "surface_net_solar_radiation",
        "surface_net_thermal_radiation",
        "surface_pressure",
        "surface_sensible_heat_flux",
        "surface_solar_radiation_downwards",
        "surface_thermal_radiation_downwards",
        "top_net_solar_radiation",
        "top_net_thermal_radiation",
        "total_cloud_cover",
        "total_column_cloud_liquid_water",
        "total_column_cloud_ice_water",
        "total_column_ozone",
        "total_column_rain_water",
        "total_column_supercooled_liquid_water",
        "total_column_water",
        "total_column_water_vapour",
        "total_precipitation",
        "type_of_high_vegetation",
        "type_of_low_vegetation",
        "uv_visible_albedo_for_direct_radiation",
        "uv_visible_albedo_for_diffuse_radiation",
        "vertical_integral_of_divergence_of_moisture_flux",
        "vertical_integral_of_eastward_water_vapour_flux",
        "vertical_integral_of_northward_water_vapour_flux",
        "volumetric_soil_water_layer_1",
        "volumetric_soil_water_layer_2",
        "volumetric_soil_water_layer_3",
        "volumetric_soil_water_layer_4",
    }

    # All available 3D pressure-level ERA5 variables
    ALL_PRESSURE_LEVEL_VARIABLES = {
        "divergence",
        "fraction_of_cloud_cover",
        "geopotential",
        "ozone_mass_mixing_ratio",
        "potential_vorticity",
        "quality_indicators_cloud_icing_level",
        "quality_indicators_cloud_type",
        "relative_humidity",
        "specific_cloud_ice_water_content",
        "specific_cloud_liquid_water_content",
        "specific_humidity",
        "temperature",
        "u_component_of_wind",
        "v_component_of_wind",
        "vorticity",
    }

    # All available 3D model-level ERA5 variables
    ALL_MODEL_LEVEL_VARIABLES = {
        "divergence",
        "fraction_of_cloud_cover",
        "geopotential",
        "ozone_mass_mixing_ratio",
        "potential_vorticity",
        "quality_indicators_cloud_icing_level",
        "quality_indicators_cloud_type",
        "relative_humidity",
        "specific_cloud_ice_water_content",
        "specific_cloud_liquid_water_content",
        "specific_humidity",
        "temperature",
        "u_component_of_wind",
        "v_component_of_wind",
        "vorticity",
    }

    # Standard pressure levels (hPa)
    # Lowest 3: 1000 hPa (~110m), 975 hPa (~300m), 950 hPa (~560m)
    STANDARD_PRESSURE_LEVELS = ["1000", "975", "950"]

    # All available pressure levels in ERA5
    ALL_PRESSURE_LEVELS = [
        "1",
        "2",
        "3",
        "5",
        "7",
        "10",
        "20",
        "30",
        "50",
        "70",
        "100",
        "125",
        "150",
        "175",
        "200",
        "225",
        "250",
        "300",
        "350",
        "400",
        "450",
        "500",
        "550",
        "600",
        "650",
        "700",
        "750",
        "775",
        "800",
        "825",
        "850",
        "875",
        "900",
        "925",
        "950",
        "975",
        "1000",
    ]

    # Model levels (1-137, where 137 is the surface)
    # Lowest 5 model levels (approximately: 133~150m, 134~100-120m, 135~50-70m, 136~10-15m, 137~surface)
    STANDARD_MODEL_LEVELS = ["133", "134", "135", "136", "137"]

    # All available model levels in ERA5
    ALL_MODEL_LEVELS = [str(i) for i in range(1, 138)]

    # Mapping of variable names to MARS parameter short codes
    VARIABLE_TO_MARS_PARAM = {
        # Single-level variables
        "10m_u_component_of_wind": "10u",
        "10m_v_component_of_wind": "10v",
        "100m_u_component_of_wind": "100u",
        "100m_v_component_of_wind": "100v",
        "2m_temperature": "2t",
        "2m_dewpoint_temperature": "2d",
        "surface_solar_radiation_downwards": "ssrd",
        "surface_pressure": "sp",
        "total_precipitation": "tp",
        "mean_sea_level_pressure": "msl",
        "boundary_layer_height": "blh",
        "convective_precipitation": "cp",
        "evaporation": "e",
        "snowfall": "sf",
        "snow_depth": "sd",
        "sea_ice_cover": "ci",
        "sea_surface_temperature": "sst",
        "skin_temperature": "skt",
        "skin_reservoir_content": "src",
        "soil_type": "slt",
        "surface_latent_heat_flux": "slhf",
        "surface_sensible_heat_flux": "sshf",
        "surface_thermal_radiation_downwards": "strd",
        "surface_net_solar_radiation": "ssr",
        "surface_net_thermal_radiation": "str",
        "top_net_solar_radiation": "tsr",
        "top_net_thermal_radiation": "ttr",
        "total_cloud_cover": "tcc",
        # 3D variables (pressure/model levels)
        "temperature": "t",
        "u_component_of_wind": "u",
        "v_component_of_wind": "v",
        "geopotential": "z",
        "relative_humidity": "r",
        "specific_humidity": "q",
        "vertical_velocity": "w",
        "divergence": "d",
        "vorticity": "vo",
        "potential_vorticity": "pv",
        "fraction_of_cloud_cover": "cc",
        "specific_cloud_ice_water_content": "ciwc",
        "specific_cloud_liquid_water_content": "clwc",
        "ozone_mass_mixing_ratio": "o3",
    }

    def __init__(
        self,
        api_url: str,
        api_key: str,
        output_path: Path,
        years: list[int],
        months: list[str] | None = None,
        variables: list[str] | None = None,
        area: list[float] | None = None,
        resolution: str = "0.25/0.25",
        pressure_levels: list[str] | None = None,
        model_levels: list[str] | None = None,
        file_format: str = "grib",
        resume: bool = False,
        dry_run: bool = False,
    ) -> None:
        """Initializes the instance.

        Args:
            api_url (str): The CDS API URL.
            api_key (str): The CDS API key.
            output_path (Path): Path to the output directory.
            years (list[int]): List of years to get data for.
            months (list[str], optional): List of months (01-12). Defaults to all months.
            variables (list[str], optional): List of ERA5 variables. Defaults to common variables.
            area (list[float], optional): Bounding box [N, W, S, E]. Defaults to world (None).
            resolution (str, optional): Grid resolution. Defaults to "0.25/0.25".
            pressure_levels (list[str], optional): Pressure levels (hPa). Defaults to standard levels if None.
            model_levels (list[str], optional): Model levels (1-137). Defaults to standard levels if None.
            file_format (str, optional): Output file format ("grib" or "netcdf"). Defaults to "grib".
            resume (bool, optional): Whether to resume from a previous download. Defaults to False.
            dry_run (bool, optional): If True, print requests without submitting them. Defaults to False.

        Raises:
            ValueError: If API credentials are invalid or invalid file_format.
        """
        if file_format.lower() not in ["grib", "netcdf"]:
            raise ValueError(
                f"file_format must be 'grib' or 'netcdf', got '{file_format}'"
            )

        self.years = years
        self.months = (
            months if months is not None else [f"{i:02d}" for i in range(1, 13)]
        )
        self.variables = variables if variables is not None else self.DEFAULT_VARIABLES
        self.area = area  # None means world (all)
        self.resolution = resolution
        self.file_format = file_format.lower()
        self.file_extension = "nc" if self.file_format == "netcdf" else "grib"
        self.dry_run = dry_run

        # Determine which level types to download
        # If both are None, default to pressure levels
        if pressure_levels is None and model_levels is None:
            self.pressure_levels = self.STANDARD_PRESSURE_LEVELS
            self.model_levels = None
        else:
            # Use default levels if specified as empty but not None
            self.pressure_levels = (
                pressure_levels if pressure_levels is not None else None
            )
            if self.pressure_levels is not None and len(self.pressure_levels) == 0:
                self.pressure_levels = self.STANDARD_PRESSURE_LEVELS

            self.model_levels = model_levels if model_levels is not None else None
            if self.model_levels is not None and len(self.model_levels) == 0:
                self.model_levels = self.STANDARD_MODEL_LEVELS

        self.output_path = Path(output_path)
        self.checkpoint_path = self.output_path / "status.pickle"

        # Create output directory if it doesn't exist
        self.output_path.mkdir(parents=True, exist_ok=True)

        area_str = f"{self.area}" if self.area is not None else "World (all)"
        level_info = []
        if self.pressure_levels is not None:
            level_info.append(f"Pressure levels: {len(self.pressure_levels)} levels")
        if self.model_levels is not None:
            level_info.append(f"Model levels: {len(self.model_levels)} levels")

        dry_run_str = " [DRY RUN - NO DATA WILL BE DOWNLOADED]" if self.dry_run else ""
        logger.info(
            f"ERA5 Downloader initialised for:{dry_run_str}"
            f"\n- years:\t\t{years}"
            f"\n- months:\t\t{self.months}"
            f"\n- variables:\t\t{self.variables}"
            f"\n- area (N,W,S,E):\t{area_str}"
            f"\n- resolution:\t\t{self.resolution}"
            f"\n- file_format:\t\t{self.file_format}"
            f"\n- {'; '.join(level_info) if level_info else 'No levels specified'}"
        )

        # Initialize CDS API client
        try:
            self.client = cdsapi.Client(url=api_url, key=api_key)
            logger.info("CDS API client initialized successfully.")
        except Exception as e:
            raise ValueError(f"Failed to initialize CDS API client: {e}")

        # Validate requested variables
        self._validate_variables()

        # Initialize or load checkpoint
        # Determine checkpoint dimensions based on which level types are requested
        has_single = True  # Always true
        has_pressure = self.pressure_levels is not None
        has_model = self.model_levels is not None

        num_level_types = sum([has_single, has_pressure, has_model])

        if resume and self.checkpoint_path.is_file():
            with open(self.checkpoint_path, "rb") as f:
                self.checkpoint = pickle.load(f)
            logger.info(f"Resuming from checkpoint: {self.checkpoint_path}")
        else:
            if num_level_types > 1:
                # Shape: (years, months, level_types)
                self.checkpoint = np.zeros(
                    (len(years), len(self.months), num_level_types)
                )
            else:
                self.checkpoint = np.zeros((len(years), len(self.months)))
            logger.info("Starting fresh download (no checkpoint found).")

    def _validate_variables(self) -> None:
        """Validate that all requested variables are available in ERA5.

        Raises:
            ValueError: If any requested variable is not available.
        """
        invalid_single_level: list[str] = []
        invalid_pressure_level: list[str] = []
        invalid_model_level: list[str] = []

        for variable in self.variables:
            is_single_level = variable in self.ALL_SINGLE_LEVEL_VARIABLES

            if is_single_level:
                # Single-level variable is valid (already checked membership above)
                pass
            else:
                # Check if 3D variable is available
                if (
                    variable not in self.ALL_PRESSURE_LEVEL_VARIABLES
                    and variable not in self.ALL_MODEL_LEVEL_VARIABLES
                ):
                    invalid_pressure_level.append(variable)
                else:
                    # Check against specific level types if requested
                    if (
                        self.pressure_levels is not None
                        and variable not in self.ALL_PRESSURE_LEVEL_VARIABLES
                    ):
                        invalid_pressure_level.append(
                            f"{variable} (not available at pressure levels)"
                        )
                    if (
                        self.model_levels is not None
                        and variable not in self.ALL_MODEL_LEVEL_VARIABLES
                    ):
                        invalid_model_level.append(
                            f"{variable} (not available at model levels)"
                        )

        # Compile error messages
        error_messages = []

        if invalid_single_level:
            error_messages.append(
                f"Invalid single-level variables: {', '.join(invalid_single_level)}\n"
                f"Run 'python scripts/era5_download.py --list-variables' to see available variables."
            )

        if invalid_pressure_level:
            error_messages.append(
                f"Invalid pressure-level variables: {', '.join(invalid_pressure_level)}\n"
                f"Run 'python scripts/era5_download.py --list-variables' to see available variables."
            )

        if invalid_model_level:
            error_messages.append(
                f"Invalid model-level variables: {', '.join(invalid_model_level)}\n"
                f"Run 'python scripts/era5_download.py --list-variables' to see available variables."
            )

        if error_messages:
            raise ValueError("\n".join(error_messages))

        logger.info(f"All {len(self.variables)} requested variables are available.")

    def download_data(self) -> None:
        """Download ERA5 data for all given years and months."""
        # Determine checkpoint indices for each level type
        checkpoint_idx_map = {}
        idx = 0
        if True:  # always have single-level
            checkpoint_idx_map["single"] = idx
            idx += 1
        if self.pressure_levels is not None:
            checkpoint_idx_map["pressure"] = idx
            idx += 1
        if self.model_levels is not None:
            checkpoint_idx_map["model"] = idx

        for year in self.years:
            logger.info(f"Processing year {year}...")

            for month in self.months:
                # Download single-level (2D) variables
                self._download_variables(
                    year=year,
                    month=month,
                    level_type="single",
                    checkpoint_idx=checkpoint_idx_map.get("single", 0),
                )

                # Download pressure levels if specified
                if self.pressure_levels is not None:
                    self._download_variables(
                        year=year,
                        month=month,
                        level_type="pressure",
                        checkpoint_idx=checkpoint_idx_map.get("pressure", 0),
                    )

                # Download model levels if specified
                if self.model_levels is not None:
                    self._download_variables(
                        year=year,
                        month=month,
                        level_type="model",
                        checkpoint_idx=checkpoint_idx_map.get("model", 0),
                    )

        logger.info("All downloads completed!")

    def _download_variables(
        self, year: int, month: str, level_type: str = "single", checkpoint_idx: int = 0
    ) -> int:
        """Download ERA5 variables for a specific year, month, and level type.

        Unified method for downloading single-level (2D), pressure-level (3D), and model-level (3D) variables.
        Each variable is downloaded in a separate file using the MARS format via reanalysis-era5-complete dataset.

        Args:
            year (int): Year to download data for.
            month (str): Month to download data for (format: '01' to '12').
            level_type (str): Type of levels to download ("single", "pressure", or "model").
            checkpoint_idx (int): Index in checkpoint array for tracking download status.

        Returns:
            int: Status of the download (1 if successful, 0 if any failed).
        """
        # Determine which variables to download based on level type
        if level_type == "single":
            variables_to_download = [
                v for v in self.variables if v in self.ALL_SINGLE_LEVEL_VARIABLES
            ]
            level_prefix = "sl"
        elif level_type == "pressure":
            variables_to_download = [
                v for v in self.variables if v in self.ALL_PRESSURE_LEVEL_VARIABLES
            ]
            level_prefix = "pl"
        else:  # model
            variables_to_download = [
                v for v in self.variables if v in self.ALL_MODEL_LEVEL_VARIABLES
            ]
            level_prefix = "ml"

        # Skip if no variables to download for this level type
        if not variables_to_download:
            return 1

        # Check checkpoint
        year_idx = self.years.index(year)
        month_idx = self.months.index(month)

        if self.checkpoint.ndim == 3:
            if self.checkpoint[year_idx, month_idx, checkpoint_idx] != 0:
                logger.info(
                    f"{year}-{month} ({level_type}): Data previously downloaded."
                )
                return 1
        else:
            if self.checkpoint[year_idx, month_idx] != 0:
                logger.info(
                    f"{year}-{month} ({level_type}): Data previously downloaded."
                )
                return 1

        # Build filename suffix
        level_suffix = f"_{level_prefix}"
        if (
            level_type == "pressure"
            and self.pressure_levels is not None
            and self.pressure_levels != self.ALL_PRESSURE_LEVELS
        ):
            levels_str = "-".join(self.pressure_levels)
            level_suffix += f"_{levels_str}"
        elif (
            level_type == "model"
            and self.model_levels is not None
            and self.model_levels != self.ALL_MODEL_LEVELS
        ):
            levels_str = "-".join(self.model_levels)
            level_suffix += f"_{levels_str}"

        # Build combined filename with short names separated by "-"
        short_names = [self._get_mars_param(var) for var in variables_to_download]
        variables_str = "-".join(short_names)
        output_file = (
            self.output_path
            / f"era5_{year}_{month}{level_suffix}_{variables_str}.{self.file_extension}"
        )

        try:
            # Build a single request with all variables combined
            request_params = self._build_mars_request_batch(
                variables=variables_to_download,
                year=year,
                month=month,
                level_type=level_type,
            )

            if self.dry_run:
                # Print request without submitting
                print("\n" + "=" * 80)
                print(
                    f"DRY RUN: {year}-{month} ({level_type}, {len(variables_to_download)} variables)"
                )
                print("=" * 80)
                print("Dataset: reanalysis-era5-complete")
                print(f"Variables: {', '.join(variables_to_download)}")
                print("Request parameters:")
                for key, value in request_params.items():
                    print(f"  {key}: {value}")
                print(f"Output file (would be): {output_file}")
                print("=" * 80 + "\n")
                logger.info(
                    f"{year}-{month} ({level_type}): DRY RUN - Request printed (not submitted)"
                )
            else:
                logger.info(
                    f"{year}-{month} ({level_type}, {len(variables_to_download)} variables): Starting download..."
                )
                self.client.retrieve(
                    "reanalysis-era5-complete", request_params, str(output_file)
                )
                logger.info(
                    f"{year}-{month} ({level_type}): Downloaded and saved to {output_file}"
                )

            all_success = True
        except Exception as e:
            logger.error(
                f"{year}-{month} ({level_type}): Download failed with error: {e}"
            )
            all_success = False

        # Update checkpoint
        status = 1 if all_success else 0
        if self.checkpoint.ndim == 3:
            self.checkpoint[year_idx, month_idx, checkpoint_idx] = status
        else:
            self.checkpoint[year_idx, month_idx] = status

        with open(self.checkpoint_path, "wb") as f:
            pickle.dump(self.checkpoint, f)

        return status

    def _get_mars_param(self, variable: str) -> str:
        """Convert variable name to MARS parameter code.

        Args:
            variable (str): ERA5 variable name

        Returns:
            str: MARS parameter code
        """
        if variable in self.VARIABLE_TO_MARS_PARAM:
            return self.VARIABLE_TO_MARS_PARAM[variable]
        # Fallback: try to use variable name directly
        return variable.replace("_", "")[:10]

    def _build_mars_request_batch(
        self, variables: list[str], year: int, month: str, level_type: str = "single"
    ) -> dict:
        """Build a MARS format request for multiple variables combined.

        Combines all variables into a single request with param codes joined by "/".
        This is more efficient for MARS backend processing.

        Args:
            variables (list[str]): List of ERA5 variable names
            year (int): Year
            month (str): Month (format: '01' to '12')
            level_type (str): Type of levels ("single", "pressure", or "model")

        Returns:
            dict: MARS format request parameters with combined param codes
        """
        days_in_month = self._get_days_in_month(year, month)
        date_range = f"{year}-{month}-01/to/{year}-{month}-{days_in_month:02d}"

        # Format times as HH:MM:SS separated by slashes
        times = "/".join([f"{i:02d}:00:00" for i in range(24)])

        # Format area as N/W/S/E string
        area_str = (
            f"{self.area[0]}/{self.area[1]}/{self.area[2]}/{self.area[3]}"
            if self.area
            else None
        )

        # Combine all parameter codes with "/"
        param_codes = [self._get_mars_param(var) for var in variables]
        combined_params = "/".join(param_codes)

        # Build base request
        request = {
            "class": "ea",
            "date": date_range,
            "expver": "1",
            "grid": self.resolution,
            "param": combined_params,
            "stream": "oper",
            "time": times,
            "type": "an",
        }

        # Add area if specified
        if area_str:
            request["area"] = area_str

        # Add level-specific parameters
        if level_type == "single":
            # Single-level (surface) variables
            request["levtype"] = "sfc"
        elif level_type == "pressure":
            request["levtype"] = "pl"
            if self.pressure_levels is not None:
                request["levelist"] = "/".join(self.pressure_levels)
        elif level_type == "model":
            request["levtype"] = "ml"
            if self.model_levels is not None:
                request["levelist"] = "/".join(self.model_levels)

        return request

    @staticmethod
    def _get_days_in_month(year: int, month: str) -> int:
        """Get the number of days in a specific month.

        Args:
            year (int): Year.
            month (str): Month (format: '01' to '12').

        Returns:
            int: Number of days in the month.
        """
        month_int = int(month)
        if month_int in [1, 3, 5, 7, 8, 10, 12]:
            return 31
        elif month_int in [4, 6, 9, 11]:
            return 30
        else:  # February
            # Check for leap year
            if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
                return 29
            return 28

    @staticmethod
    def print_available_variables() -> None:
        """Print all available ERA5 variables organized by dataset type."""
        print("\n" + "=" * 80)
        print("AVAILABLE ERA5 VARIABLES")
        print("=" * 80)

        print("\n--- SINGLE-LEVEL (2D) VARIABLES ---")
        print("Dataset: reanalysis-era5-single-levels")
        print(f"Total: {len(Era5Downloader.ALL_SINGLE_LEVEL_VARIABLES)} variables\n")
        for var in sorted(Era5Downloader.ALL_SINGLE_LEVEL_VARIABLES):
            marker = " [DEFAULT]" if var in Era5Downloader.DEFAULT_VARIABLES else ""
            print(f"  • {var}{marker}")

        print("\n--- PRESSURE-LEVEL (3D) VARIABLES ---")
        print("Dataset: reanalysis-era5-pressure-levels")
        print(
            f"Available levels (hPa): {', '.join(Era5Downloader.ALL_PRESSURE_LEVELS)}"
        )
        print(f"Default levels: {', '.join(Era5Downloader.STANDARD_PRESSURE_LEVELS)}")
        print(f"Total: {len(Era5Downloader.ALL_PRESSURE_LEVEL_VARIABLES)} variables\n")
        for var in sorted(Era5Downloader.ALL_PRESSURE_LEVEL_VARIABLES):
            marker = (
                " [DEFAULT]"
                if var
                in [
                    "temperature",
                    "u_component_of_wind",
                    "v_component_of_wind",
                    "relative_humidity",
                    "geopotential",
                ]
                else ""
            )
            print(f"  • {var}{marker}")

        print("\n--- MODEL-LEVEL (3D) VARIABLES ---")
        print("Dataset: reanalysis-era5-complete")
        print("Available levels: 1-137 (137 levels)")
        print(f"Default levels: {', '.join(Era5Downloader.STANDARD_MODEL_LEVELS)}")
        print(f"Total: {len(Era5Downloader.ALL_MODEL_LEVEL_VARIABLES)} variables\n")
        for var in sorted(Era5Downloader.ALL_MODEL_LEVEL_VARIABLES):
            marker = (
                " [DEFAULT]"
                if var
                in [
                    "temperature",
                    "u_component_of_wind",
                    "v_component_of_wind",
                    "relative_humidity",
                    "geopotential",
                ]
                else ""
            )
            print(f"  • {var}{marker}")

        print("\n" + "=" * 80)
        print("USAGE EXAMPLES:")
        print("=" * 80)
        print("\n1. Single-level variables only:")
        print(
            "   python scripts/era5_download.py -y 2020 -m 01 -v 2m_temperature surface_pressure\n"
        )
        print("2. Pressure-level variables with default levels:")
        print(
            "   python scripts/era5_download.py -y 2020 -m 01 -v temperature u_component_of_wind -pl\n"
        )
        print("3. Model-level variables with custom levels:")
        print(
            "   python scripts/era5_download.py -y 2020 -m 01 -v temperature -ml 135 136 137\n"
        )
        print("=" * 80 + "\n")
