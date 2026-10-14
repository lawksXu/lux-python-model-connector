"""Java-owned Asset lookup and bounded image transport for provider references."""

import asyncio
import base64
from datetime import datetime

import httpx
from pydantic import Field, StrictInt, StrictStr

from lux_model_connector.generation_plan import LocatedAsset, PixelDimensions
from lux_model_connector.media_inspection import inspect_media, verify_content
from lux_model_connector.protocol import CanonicalUuid, FrozenModel, GenerationIdentity

MAX_REFERENCE_BYTES = 10 * 1024 * 1024


class JavaAssetMetadata(FrozenModel):
    id: CanonicalUuid
    user_id: CanonicalUuid
    task_id: CanonicalUuid | None
    filename: StrictStr
    media_type: StrictStr
    size_bytes: StrictInt = Field(ge=1, le=MAX_REFERENCE_BYTES)
    sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime


class JavaAssetLocator:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def locate(self, identity: GenerationIdentity, asset_id: str) -> LocatedAsset:
        # Decode IDs before interpolation even when called outside the worker.
        from pydantic import TypeAdapter

        asset_id = TypeAdapter(CanonicalUuid).validate_python(asset_id)
        path = f"/internal/v1/users/{identity.user_id}/assets/{asset_id}"
        async with self._client.stream("GET", path, follow_redirects=False) as response:
            response.raise_for_status()
            metadata_bytes = bytearray()
            async for chunk in response.aiter_bytes():
                if len(metadata_bytes) + len(chunk) > 65536:
                    raise ValueError("Asset metadata exceeds read budget")
                metadata_bytes.extend(chunk)
        metadata = JavaAssetMetadata.model_validate_json(bytes(metadata_bytes))
        if metadata.id != asset_id or metadata.user_id != identity.user_id:
            raise ValueError("Asset metadata belongs to another owner or Asset")
        if metadata.media_type not in ("image/png", "image/jpeg"):
            raise ValueError("Only PNG/JPEG reference images are supported")
        async with self._client.stream(
            "GET", path + "/content", follow_redirects=False
        ) as response:
            response.raise_for_status()
            if (
                response.headers.get("content-type", "").split(";")[0]
                != metadata.media_type
            ):
                raise ValueError("Asset content media type differs")
            content = bytearray()
            async for chunk in response.aiter_bytes():
                if len(content) + len(chunk) > metadata.size_bytes:
                    raise ValueError("Asset content exceeds declared read budget")
                content.extend(chunk)
        body = bytes(content)
        verify_content(body, metadata.size_bytes, metadata.sha256)
        inspected = await asyncio.to_thread(inspect_media, body, metadata.media_type)
        return LocatedAsset(
            asset_id=asset_id,
            media_type=metadata.media_type,
            size_bytes=metadata.size_bytes,
            dimensions=PixelDimensions(width=inspected.width, height=inspected.height),
            url=f"data:{metadata.media_type};base64,"
            + base64.b64encode(body).decode("ascii"),
        )
