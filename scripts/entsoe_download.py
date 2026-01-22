#!/usr/bin/env python
from entsoe.utils import mappings
from loguru import logger

from rbc.config.loader import load_config
from rbc.downloaders.entsoe import EntsoeDownloader

SOURCE = "entsoe"


if __name__ == "__main__":
    cfg = load_config(SOURCE)
    logger.info(f"Config loaded for {SOURCE}:\n{cfg}")

    # Spain as an example. We want all countries of course
    specific_country_code = "ES"
    specific_zones = [
        bz for bz, val in mappings.items() if specific_country_code in val.keys()
    ]

    EntsoeDownloader(
        token=cfg.access.api_key,
        output_path=cfg.paths.dst_dir_raw,
        bidding_zones=specific_zones,
        years=["2020"],
    )
