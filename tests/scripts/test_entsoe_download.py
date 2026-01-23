# tests/scripts/test_entsoe_download.py
from unittest.mock import patch

import scripts.entsoe_download as entsoe_script
from tests.conftest import dict_to_namespace


def test_main_execution_logic(source_configs):
    """
    Verifies that the main function correctly orchestrates the
    config loading and downloader execution.
    """
    years = ["2020"]
    zones = ["10YES-REE------0"]

    cfg_dict = source_configs["entsoe"]
    mock_cfg = dict_to_namespace(cfg_dict)

    # 1. SETUP MOCKS (of config loading and Downloader class)
    with (
        patch(
            "scripts.entsoe_download.load_config", return_value=mock_cfg
        ) as mock_load,
        patch("scripts.entsoe_download.EntsoeDownloader") as MockDownloader,
    ):
        # 2. RUN
        entsoe_script.main(years=years, bidding_zones=zones)

        # 3. ASSERT
        mock_load.assert_called_once_with("entsoe")
        MockDownloader.assert_called_once_with(
            token=mock_cfg.access.api_key,
            output_path=mock_cfg.paths.dst_dir_raw,
            bidding_zones=zones,
            years=years,
        )
        MockDownloader.return_value.dump_all_to_csv.assert_called_once()
