import asyncio
import more_itertools as mit
import json
import os
from typing import List, Union
import mercantile
import aiohttp
import aiofiles
from pathlib import Path
from .api import EntitiesAPI, CoverageAPI, TileType, NamedPair, ImageMetadata
from .utils import init_if_none

import logging

LOGGER = logging.getLogger()


class Downloader:
    def __init__(
        self,
        directory: Union[str, Path],
        *,
        session: aiohttp.ClientSession = None,
        entities_api: EntitiesAPI = None,
        coverage_api: CoverageAPI = None,
        verbose: bool = True,
        thumb: List[str] = None,
        fields: str = "image",
        zoom: int = 14,
        chunks: int = 5,
    ):
        if coverage_api is None:
            coverage_api = CoverageAPI(session, verbose=verbose)
        if entities_api is None:
            entities_api = EntitiesAPI(session, verbose=verbose)
        self.session = session
        self.entities_api = entities_api
        self.coverage_api = coverage_api
        self.directory = directory
        self._registry_lock = asyncio.Lock()
        self._sequence_registry = set()
        self._fields = fields
        self._thumb = thumb
        self._zoom = zoom
        self._chunks = chunks

    async def download_region(
        self,
        west: float,
        south: float,
        east: float,
        north: float,
        *,
        zoom: int = None,
        directory: str = None,
        session: aiohttp.ClientSession = None,
        **kwargs,
    ):
        os.makedirs(self.directory, exist_ok=True)
        LOGGER.info("Directory created")
        session = init_if_none(session, self.session)
        directory = init_if_none(directory, self.directory)
        zoom = init_if_none(zoom, self._zoom)
        LOGGER.info(
            f"Start downloading region: west={west}, south={south}, east={east}, north={north}"
        )
        for tile in mercantile.tiles(west, south, east, north, zoom):
            await self._get_entities(tile, session=session)

    async def _save_sequence(
        self,
        sequence_id: str,
        image_datas: List[Union[ImageMetadata, NamedPair]],
    ) -> None:
        LOGGER.info("Save sequence %s", sequence_id)
        sequence_paht = Path(self.directory) / sequence_id
        os.makedirs(sequence_paht, exist_ok=True)
        for image_data in image_datas:
            image_path = sequence_paht / image_data.metadata["id"]
            async with aiofiles.open(image_path, "wb") as asyncfile:
                await asyncfile.write(image_data.data)

            async with aiofiles.open(f"{image_path}.json", "w") as json_file:
                await json_file.write(json.dumps(image_data.metadata))

    async def _get_entities(
        self, tile: mercantile.Tile, session: aiohttp.ClientSession
    ):
        session = init_if_none(session, self.session)
        sequences_vtile = await self.coverage_api.aget_tile(
            tile, session, layer=TileType.SEQUENCE_LAYER, astuple=False
        )
        async with self._registry_lock:
            new_sequence_ids = self._update_registry(sequences_vtile)
        LOGGER.info(f"Download tile {tile} with {len(new_sequence_ids)} sequences")
        for sequences_chunk in mit.chunked(new_sequence_ids, self._chunks):
            tasks = [
                self.entities_api.aget_sequence_data(
                    sequence_id,
                    session=session,
                    fields=self._fields,
                    thumbs=self._thumb,
                )
                for sequence_id in sequences_chunk
            ]

            for task in asyncio.as_completed(tasks):
                sequence_id, images_data = await task
                await self._save_sequence(sequence_id, images_data)

    def _update_registry(self, sequences_vtile) -> List[str]:
        new_sequence_ids = []
        for sequence in sequences_vtile["features"]:
            sequence_id = sequence["properties"]["id"]
            if sequence_id in self._sequence_registry:
                continue
            self._sequence_registry.add(sequence_id)
            new_sequence_ids.append(sequence_id)
        return new_sequence_ids
