from typing import NamedTuple, List, Dict, Any, Union, TypeVar
from string import Template

from tqdm.auto import tqdm
import aiohttp
import asyncio
import mercantile

from .keeper import secret_keeper
from .utils import init_if_none
from vt2geojson.tools import vt_bytes_to_geojson
import logging


LOGGER = logging.getLogger()


class ImageFields:
    def __init__(self):
        self.id = "id"
        self.altitude = "altitude"
        self.atomic_scale = "atomic_scale"
        self.camera_parameters = "camera_parameters"
        self.camera_type = "camera_type"
        self.captured_at = "captured_at"
        self.compass_angle = "compass_angle"
        self.computed_altitude = "computed_altitude"
        self.computed_compass_angle = "computed_compass_angle"
        self.computed_geometry = "computed_geometry"
        self.computed_rotation = "computed_rotation"
        self.exif_orientation = "exif_orientation"
        self.geometry = "geometry"
        self.height = "height"
        self.thumb_256_url = "thumb_256_url"
        self.thumb_1024_url = "thumb_1024_url"
        self.thumb_2048_url = "thumb_2048_url"
        self.thumb_original_url = "thumb_original_url"
        self.merge_cc = "merge_cc"
        self.mesh = "mesh"
        self.sequence = "sequence"
        self.sfm_cluster = "sfm_cluster"
        self.width = "width"
        self.detections = "detections"


IMAGE_FIELDS = ImageFields()


MAPILLARY_CLIENT_SECRET = "MAPILLARY_CLIENT_SECRET"


class TilePair(NamedTuple):
    tile: mercantile.Tile
    content: str


T = TypeVar("T")
K = TypeVar("K")


class NamedPair(NamedTuple):
    metadata: Any
    data: Any


class TileType:
    IMAGE_LAYER = "image"
    SEQUENCE_LAYER = "sequence"
    OVERVIEW_LAYER = "overview"


Id = Union[str, int]
ImageMetadata = Dict[str, Any]


class EntitiesAPI:
    """docstring for ImageAPI"""

    IMAGE_URL = Template("https://graph.mapillary.com/$image_id")
    SEQUENCE_URL = Template(
        "https://graph.mapillary.com/image_ids?sequence_id=$sequence_id"
    )
    ENTITIES_URL = Template("https://graph.mapillary.com?ids=$ids")

    def __init__(
        self,
        session: aiohttp.ClientSession = None,
        verbose: bool = True,
        access_token: str = None,
    ) -> None:
        self.session = session
        self.verbose = verbose
        self.headers = None
        if access_token is not None:
            self.headers = self._add_token_to_header(access_token)

    async def aget_by_id(
        self,
        ids: Union[Id, List[Id]],
        *,
        session: aiohttp.ClientSession = None,
        fields: List[str] = None,
        access_token: str = None,
    ):
        session = init_if_none(session, self.session)
        if not isinstance(ids, list):
            ids = [ids]
        url = self.ENTITIES_URL.substitute(ids=",".join(ids))
        async with session.get(
            url, headers=self._make_header(access_token)
        ) as response:
            response = await response.json()
        return response

    async def aget_image(
        self,
        image_id,
        *,
        fields: List[str] = None,
        session: aiohttp.ClientSession = None,
        access_token=None,
        thumbs: List[str] = None,
    ) -> Union[ImageMetadata, NamedPair]:
        session = init_if_none(session, self.session)
        url = self.IMAGE_URL.substitute(image_id=image_id)
        url = self._collect_fields(url, fields)

        async with session.get(
            url, headers=self._make_header(access_token)
        ) as response:
            image = await response.json()

        if thumbs is not None:
            if isinstance(thumbs, list):
                thumbs_map = {}
                for thumb in thumbs:
                    async with session.get(image[thumb]) as response:
                        thumbs_map[f"{thumb}_image"] = await response.read()
                image = NamedPair(image, thumbs_map)
            elif isinstance(thumbs, str):
                async with session.get(image[thumbs]) as response:
                    image_data = await response.read()
                image = NamedPair(image, image_data)

        return image

    async def aget_sequence_data(
        self,
        sequence_id,
        session: aiohttp.ClientSession = None,
        access_token: str = None,
        verbose: bool = True,
        **kwargs,
    ) -> NamedPair:
        sequence = await self.aget_sequence(sequence_id, session, access_token)
        ts = [
            self.aget_image(
                image["id"], session=session, access_token=access_token, **kwargs
            )
            for image in sequence["data"]
        ]

        tasks = asyncio.as_completed(ts)
        total = len(ts)
        images = []
        for num, task in enumerate(tasks, 1):
            images.append(await task)
            if verbose:
                LOGGER.info(
                    "Download [%03d/%03d] images for sequence %s",
                    num,
                    total,
                    sequence_id,
                )

        return sequence_id, images

    async def aget_sequence(
        self,
        sequence_id,
        session: aiohttp.ClientSession = None,
        access_token: str = None,
    ) -> Dict[str, List[Dict[str, str]]]:
        url = self.SEQUENCE_URL.substitute(sequence_id=sequence_id)
        session = init_if_none(session, self.session)
        async with session.get(
            url, headers=self._make_header(access_token)
        ) as response:
            response = await response.json()
        return response

    def _make_header(self, access_token):
        if access_token is None:
            if self.headers is None:
                return self._add_token_to_header(secret_keeper[MAPILLARY_CLIENT_SECRET])
            else:
                return self.headers
        else:
            return self._add_token_to_header(access_token)

    @staticmethod
    def _add_token_to_header(access_token):
        return {"Authorization": f"OAuth {access_token}"}

    @staticmethod
    def _collect_fields(url, fields):
        if fields is not None:
            if fields == "image":
                fields = list(vars(IMAGE_FIELDS).values())
            elif not isinstance(fields, list) or not isinstance(fields[0], str):
                raise ValueError(
                    "`fields` should be a list of strings, `None`, or `image`"
                )
            fields = ",".join(fields)
            url = f"{url}?fields={fields}"
        return url


class CoverageAPI:
    """docstring for Coverage"""

    VTILES_URL = Template(
        "https://tiles.mapillary.com/maps/vtp/$vtile_type/2/{z}/{x}/{y}?access_token={access_token}"
    )
    COVERAGE_VTILES_URL = VTILES_URL.safe_substitute(vtile_type="mly1_public")

    def __init__(
        self, session: aiohttp.ClientSession = None, verbose: bool = True
    ) -> None:
        self.session = session
        self.verbose = verbose

    async def aget_tile(
        self,
        tile: mercantile.Tile,
        session: aiohttp.ClientSession = None,
        access_token=None,
        astuple: bool = True,
        layer=None,
    ):
        access_token = init_if_none(
            access_token, secret_keeper[MAPILLARY_CLIENT_SECRET]
        )
        session = init_if_none(session, self.session)
        tile_url = self.COVERAGE_VTILES_URL.format(
            x=tile.x, y=tile.y, z=tile.z, access_token=access_token
        )
        async with session.get(tile_url) as response:
            content = await response.read()
        content = vt_bytes_to_geojson(
            content,
            tile.x,
            tile.y,
            tile.z,
            layer,
        )
        if astuple:
            return TilePair(tile, content)
        return content

    async def agenerate_tiles(self, tiles: List[mercantile.Tile], **kwargs):
        ts = [self.aget_tile(tile, **kwargs) for tile in tiles]
        tasks = asyncio.as_completed(ts)
        if self.verbose:
            tasks = tqdm(tasks, total=len(ts))
        for task in tasks:
            yield task

    async def aget_tiles(self, tiles: List[mercantile.Tile], **kwargs):
        return [await task async for task in self.agenerate_tiles(tiles, **kwargs)]

    async def aget_region(
        self,
        west: float,
        south: float,
        east: float,
        north: float,
        **kwargs,
    ):
        tiles = list(mercantile.tiles(west, south, east, north, 14))
        return await self.aget_tiles(tiles, **kwargs)
