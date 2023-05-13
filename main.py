import aiohttp
import asyncio

from mapillary_client.api import CoverageAPI, EntitiesAPI, IMAGE_FIELDS
from mapillary_client.download import Downloader

import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


async def main():
    west, south, east, north = 23.103337, 46.558860, 24.068298, 46.970882
    async with aiohttp.ClientSession() as session:
        coverage_api = CoverageAPI(session=session)
        entities_api = EntitiesAPI(session=session)
        downloader = Downloader(
            "/home/hsc/Projects/mapillary/data",
            session=session,
            entities_api=entities_api,
            coverage_api=coverage_api,
            thumb=IMAGE_FIELDS.thumb_original_url,
            zoom=10,
        )
        await downloader.download_region(west, south, east, north)


if __name__ == "__main__":
    asyncio.run(main())
