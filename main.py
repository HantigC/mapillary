import aiohttp
import asyncio
import argparse

from mapillary_client.api import CoverageAPI, EntitiesAPI
from mapillary_client.download import Downloader

import logging
from dataclasses import dataclass


logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


@dataclass
class Config:
    download_directory: str
    south: float
    west: float
    east: float
    north: float
    zoom: int = 10
    thumb: str = "thumb_original_url"


def _get_config_file():
    parser = argparse.ArgumentParser(description="Config File")
    parser.add_argument(
        "--config-file", type=str, help="Path to the config file", default="config.toml"
    )
    config_file = parser.parse_args().config_file
    return config_file


def main() -> None:
    config_file = _get_config_file()

    import toml

    with open(config_file, "r") as toml_file:
        config_dict = toml.load(toml_file)
    config = Config(**config_dict)
    asyncio.run(amain(config))


async def amain(config: Config):
    async with aiohttp.ClientSession() as session:
        coverage_api = CoverageAPI(session=session)
        entities_api = EntitiesAPI(session=session)
        downloader = Downloader(
            config.download_directory,
            session=session,
            entities_api=entities_api,
            coverage_api=coverage_api,
            thumb=config.thumb,
            zoom=config.zoom,
        )
        await downloader.download_region(
            config.west, config.south, config.east, config.north
        )


if __name__ == "__main__":
    main()
