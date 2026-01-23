#!/usr/bin/env python
from entsoe.utils import mappings
from loguru import logger

from rbc.config.loader import load_config
from rbc.downloaders.entsoe import EntsoeDownloader

SOURCE = "entsoe"


def main(years: list[str], bidding_zones: list[str] = mappings.keys()):
    cfg = load_config(SOURCE)
    logger.info(f"Config loaded for {SOURCE}:\n{cfg}")

    downloader = EntsoeDownloader(
        token=cfg.access.api_key,
        output_path=cfg.paths.dst_dir_raw,
        bidding_zones=bidding_zones,
        years=years,
    )
    downloader.dump_all_to_csv()


if __name__ == "__main__":
    # Spain as an example. We want all countries at some point in time
    specific_country_code = "ES"
    specific_zones = [
        bz for bz, val in mappings.items() if specific_country_code in val.keys()
    ]

    main(years=["2020"], bidding_zones=specific_zones)
