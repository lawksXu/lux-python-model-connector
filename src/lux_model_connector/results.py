"""Bounded original-media preservation behind an immutable S3 object boundary."""

import asyncio
import base64
import binascii
import hashlib
import re
from dataclasses import dataclass
from tempfile import SpooledTemporaryFile
from typing import BinaryIO, Literal, Protocol, Self, TypedDict
from urllib.parse import urljoin, urlsplit

import httpx
from pydantic import Field, StrictInt, StrictStr, field_validator, model_validator

from lux_model_connector.protocol import (
    CanonicalUuid,
    FrozenModel,
    GenerationKind,
    canonical_json,
)

DEFAULT_MAX_RESULT_BYTES = 100 * 1024 * 1024
_TRANSFER_CHUNK_BYTES = 64 * 1024


def _http_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or re.search(r"[\x00-\x20\x7f]", value)
    ):
        raise ValueError("Result URLs must use HTTP(S) without embedded credentials")
    parsed.port
    return value


class S3Settings(FrozenModel):
    endpoint_url: StrictStr | None = None
    region_name: StrictStr
    bucket: StrictStr
    access_key_id: StrictStr = Field(repr=False)
    secret_access_key: StrictStr = Field(repr=False)
    session_token: StrictStr | None = Field(default=None, repr=False)
    prefix: StrictStr = "connector-results"

    @field_validator("endpoint_url")
    @classmethod
    def validate_endpoint(cls, value: str | None) -> str | None:
        if value is None:
            return None
        _http_url(value)
        parsed = urlsplit(value)
        if parsed.query or parsed.fragment:
            raise ValueError("S3 endpoints cannot contain a query or fragment")
        return value.rstrip("/")

    @field_validator("region_name", "bucket", "access_key_id", "secret_access_key")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Object storage settings must be nonblank")
        return value

    @field_validator("prefix")
    @classmethod
    def validate_prefix(cls, value: str) -> str:
        if not re.fullmatch(r"[a-zA-Z0-9_-]+(?:/[a-zA-Z0-9_-]+)*", value):
            raise ValueError("Object prefix must contain safe nonempty path segments")
        return value

    @property
    def store_ref(self) -> str:
        identity = "\0".join(
            (self.endpoint_url or "", self.region_name, self.bucket, self.prefix)
        )
        return hashlib.sha256(identity.encode()).hexdigest()


class PreparedOutput(FrozenModel):
    source_index: StrictInt = Field(ge=0, le=63)
    media_kind: GenerationKind
    url: StrictStr | None = None
    b64_json: StrictStr | None = Field(default=None, repr=False)
    expected_format: StrictStr | None = None

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str | None) -> str | None:
        return _http_url(value) if value is not None else None

    @model_validator(mode="after")
    def one_source(self) -> Self:
        if (self.url is None) == (self.b64_json is None):
            raise ValueError("An output needs exactly one URL or base64 source")
        return self


class StoredResult(FrozenModel):
    source_index: StrictInt = Field(ge=0, le=63)
    store_ref: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    bucket: StrictStr
    object_key: StrictStr
    filename: StrictStr
    media_type: StrictStr
    sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    byte_length: StrictInt = Field(ge=1, le=DEFAULT_MAX_RESULT_BYTES)


class RetainedObjectMetadata(FrozenModel):
    sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    source_identity_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    media_kind: GenerationKind


class BinaryReadable(Protocol):
    def read(self, size: int = -1) -> bytes: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class S3Object:
    body: BinaryReadable
    media_type: str
    byte_length: int


class S3MetadataResponse(TypedDict, total=False):
    ContentLength: int
    ContentType: str
    Metadata: dict[str, str]


class S3GetResponse(S3MetadataResponse):
    Body: BinaryReadable


class S3PutResponse(TypedDict, total=False):
    ETag: str


class S3Client(Protocol):
    """The SDK's named external records are decoded at this adapter boundary."""

    def head_object(self, *, Bucket: str, Key: str) -> S3MetadataResponse: ...

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: BinaryIO,
        ContentLength: int,
        ContentType: str,
        Metadata: dict[str, str],
        IfNoneMatch: str,
    ) -> S3PutResponse: ...

    def get_object(self, *, Bucket: str, Key: str) -> S3GetResponse: ...


class ResultPreservationError(Exception):
    """Original result retention is unconfirmed and can be recovered without POST."""


class ResultSourceUnavailable(ResultPreservationError):
    """An original output is definitively unavailable before it could be retained."""


@dataclass(frozen=True)
class MediaFormat:
    media_type: str
    extension: str
    kind: Literal["IMAGE", "VIDEO"]


def _media_format(header: bytes) -> MediaFormat:
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return MediaFormat("image/png", "png", "IMAGE")
    if header.startswith(b"\xff\xd8\xff"):
        return MediaFormat("image/jpeg", "jpg", "IMAGE")
    if header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return MediaFormat("image/webp", "webp", "IMAGE")
    if header.startswith((b"GIF87a", b"GIF89a")):
        return MediaFormat("image/gif", "gif", "IMAGE")
    if header.startswith((b"II*\x00", b"MM\x00*")):
        return MediaFormat("image/tiff", "tif", "IMAGE")
    if header.startswith(b"BM"):
        return MediaFormat("image/bmp", "bmp", "IMAGE")
    if len(header) >= 16 and header[4:8] == b"ftyp":
        brands = header[8:12] + header[16:64]
        if b"avif" in brands or b"avis" in brands:
            return MediaFormat("image/avif", "avif", "IMAGE")
        if b"heic" in brands or b"heix" in brands:
            return MediaFormat("image/heic", "heic", "IMAGE")
        if header[8:12] == b"qt  ":
            return MediaFormat("video/quicktime", "mov", "VIDEO")
        if any(brand in brands for brand in (b"isom", b"mp41", b"mp42", b"avc1")):
            return MediaFormat("video/mp4", "mp4", "VIDEO")
    if header.startswith(b"\x1a\x45\xdf\xa3") and b"webm" in header:
        return MediaFormat("video/webm", "webm", "VIDEO")
    raise ResultPreservationError("Output does not have a supported media signature")


_FORMATS_BY_TYPE = {
    media.media_type: media
    for media in (
        MediaFormat("image/png", "png", "IMAGE"),
        MediaFormat("image/jpeg", "jpg", "IMAGE"),
        MediaFormat("image/webp", "webp", "IMAGE"),
        MediaFormat("image/gif", "gif", "IMAGE"),
        MediaFormat("image/tiff", "tif", "IMAGE"),
        MediaFormat("image/bmp", "bmp", "IMAGE"),
        MediaFormat("image/avif", "avif", "IMAGE"),
        MediaFormat("image/heic", "heic", "IMAGE"),
        MediaFormat("video/quicktime", "mov", "VIDEO"),
        MediaFormat("video/mp4", "mp4", "VIDEO"),
        MediaFormat("video/webm", "webm", "VIDEO"),
    )
}


class S3ResultStore:
    def __init__(
        self,
        settings: S3Settings,
        http_client: httpx.AsyncClient,
        s3_client: S3Client | None = None,
        *,
        max_bytes: int = DEFAULT_MAX_RESULT_BYTES,
    ) -> None:
        if not 1 <= max_bytes <= DEFAULT_MAX_RESULT_BYTES:
            raise ValueError("Result byte budget must be between 1 and 100 MiB")
        self.settings = settings
        self.http_client = http_client
        self.max_bytes = max_bytes
        if s3_client is None:
            import boto3
            from botocore.config import Config

            s3_client = boto3.client(
                "s3",
                endpoint_url=settings.endpoint_url,
                region_name=settings.region_name,
                aws_access_key_id=settings.access_key_id,
                aws_secret_access_key=settings.secret_access_key,
                aws_session_token=settings.session_token,
                config=Config(
                    connect_timeout=10,
                    read_timeout=60,
                    retries={"total_max_attempts": 1},
                    s3={"addressing_style": "path"},
                ),
            )
        self.s3_client = s3_client

    async def preserve(
        self, user_id: str, task_id: str, output: PreparedOutput
    ) -> StoredResult:
        from pydantic import TypeAdapter

        TypeAdapter(CanonicalUuid).validate_python(user_id)
        TypeAdapter(CanonicalUuid).validate_python(task_id)
        object_key = f"{self.settings.prefix}/{user_id}/{task_id}/{output.source_index}"
        source_identity = hashlib.sha256(
            canonical_json(output.model_dump(mode="json")).encode()
        ).hexdigest()
        existing = await asyncio.to_thread(self._head, self.settings.bucket, object_key)
        if existing is not None:
            return self._recover(output, object_key, source_identity, existing)
        with SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b") as temporary:
            try:
                if output.url is not None:
                    async with asyncio.timeout(120):
                        await self._download(output.url, temporary)
                else:
                    self._decode_base64(output.b64_json, temporary)
                byte_length = temporary.tell()
                temporary.seek(0)
                media = _media_format(temporary.read(_TRANSFER_CHUNK_BYTES))
                if media.kind != output.media_kind:
                    raise ResultPreservationError(
                        "Output media does not match its kind"
                    )
                temporary.seek(0)
                checksum = hashlib.file_digest(temporary, "sha256").hexdigest()
                filename = f"{output.source_index}.{media.extension}"
                saved = StoredResult(
                    source_index=output.source_index,
                    store_ref=self.settings.store_ref,
                    bucket=self.settings.bucket,
                    object_key=object_key,
                    filename=filename,
                    media_type=media.media_type,
                    sha256=checksum,
                    byte_length=byte_length,
                )
                temporary.seek(0)
                upload = asyncio.create_task(
                    asyncio.to_thread(
                        self._upload,
                        saved,
                        temporary,
                        source_identity,
                        output.media_kind,
                    )
                )
                try:
                    await asyncio.shield(upload)
                except asyncio.CancelledError:
                    # Cancelling an await cannot stop the SDK thread. Keep its
                    # input open until the bounded SDK operation has returned.
                    try:
                        await upload
                    except Exception:
                        pass
                    raise
                return saved
            except ResultPreservationError:
                raise
            except (httpx.HTTPError, OSError, ValueError, binascii.Error) as error:
                raise ResultPreservationError(
                    "Original result transfer is unconfirmed"
                ) from error

    async def _download(self, url: str, target: BinaryIO) -> None:
        for redirect in range(6):
            _http_url(url)
            # A constructed request bypasses client default headers; auth=None also
            # disables client authentication. Signed output URLs carry no API key.
            request = httpx.Request("GET", url, headers={"Accept-Encoding": "identity"})
            response = await self.http_client.send(
                request, auth=None, stream=True, follow_redirects=False
            )
            try:
                if response.is_redirect:
                    location = response.headers.get("location")
                    if location is None or redirect == 5:
                        raise ResultPreservationError("Result redirect is unconfirmed")
                    url = urljoin(url, location)
                    continue
                if response.status_code in {404, 410}:
                    raise ResultSourceUnavailable("Original output is unavailable")
                if response.status_code != 200:
                    raise ResultPreservationError("Original output download failed")
                declared = response.headers.get("content-length")
                declared_bytes = int(declared) if declared is not None else None
                if declared_bytes is not None:
                    if not 0 <= declared_bytes <= self.max_bytes:
                        raise ResultPreservationError(
                            "Original output exceeds byte budget"
                        )
                async for chunk in response.aiter_bytes(_TRANSFER_CHUNK_BYTES):
                    self._write(target, chunk)
                if target.tell() == 0:
                    raise ResultPreservationError("Original output is empty")
                if (
                    declared_bytes is not None
                    and response.headers.get("content-encoding", "identity")
                    == "identity"
                    and target.tell() != declared_bytes
                ):
                    raise ResultPreservationError(
                        "Original output transfer is incomplete"
                    )
                return
            finally:
                await response.aclose()
        raise ResultPreservationError("Result redirect budget exhausted")

    def _decode_base64(self, encoded: str | None, target: BinaryIO) -> None:
        if (
            not encoded
            or len(encoded) % 4
            or len(encoded) > 4 * ((self.max_bytes + 2) // 3)
            or not re.fullmatch(r"[A-Za-z0-9+/]*={0,2}", encoded)
        ):
            raise ResultPreservationError("Base64 output is invalid or too large")
        for offset in range(0, len(encoded), _TRANSFER_CHUNK_BYTES):
            self._write(
                target,
                base64.b64decode(
                    encoded[offset : offset + _TRANSFER_CHUNK_BYTES], validate=True
                ),
            )

    def _write(self, target: BinaryIO, chunk: bytes) -> None:
        if target.tell() + len(chunk) > self.max_bytes:
            raise ResultPreservationError("Original output exceeds byte budget")
        target.write(chunk)

    def _metadata(self, response: S3MetadataResponse) -> RetainedObjectMetadata:
        try:
            return RetainedObjectMetadata.model_validate(response.get("Metadata", {}))
        except ValueError as error:
            raise ResultPreservationError(
                "Retained object metadata is invalid"
            ) from error

    def _recover(
        self,
        output: PreparedOutput,
        object_key: str,
        source_identity: str,
        response: S3MetadataResponse,
    ) -> StoredResult:
        metadata = self._metadata(response)
        media = _FORMATS_BY_TYPE.get(response.get("ContentType", ""))
        byte_length = response.get("ContentLength")
        if (
            metadata.source_identity_sha256 != source_identity
            or metadata.media_kind != output.media_kind
            or media is None
            or media.kind != output.media_kind
            or type(byte_length) is not int
            or not 1 <= byte_length <= self.max_bytes
        ):
            raise ResultPreservationError("Retained object does not match its receipt")
        return StoredResult(
            source_index=output.source_index,
            store_ref=self.settings.store_ref,
            bucket=self.settings.bucket,
            object_key=object_key,
            filename=f"{output.source_index}.{media.extension}",
            media_type=media.media_type,
            sha256=metadata.sha256,
            byte_length=byte_length,
        )

    def _verify(
        self,
        expected: StoredResult,
        response: S3MetadataResponse,
        source_identity: str | None = None,
    ) -> None:
        metadata = self._metadata(response)
        media = _FORMATS_BY_TYPE.get(expected.media_type)
        if (
            type(response.get("ContentLength")) is not int
            or response.get("ContentLength") != expected.byte_length
            or response.get("ContentType") != expected.media_type
            or metadata.sha256 != expected.sha256
            or media is None
            or media.kind != metadata.media_kind
            or (
                source_identity is not None
                and source_identity != metadata.source_identity_sha256
            )
        ):
            raise ResultPreservationError("Retained object metadata does not match")

    def _head(self, bucket: str, object_key: str) -> S3MetadataResponse | None:
        from botocore.exceptions import ClientError

        try:
            return self.s3_client.head_object(Bucket=bucket, Key=object_key)
        except ClientError as error:
            if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return None
            raise ResultPreservationError("Object metadata is unconfirmed") from error
        except Exception as error:
            raise ResultPreservationError("Object metadata is unconfirmed") from error

    def _upload(
        self,
        saved: StoredResult,
        body: BinaryIO,
        source_identity: str,
        media_kind: GenerationKind,
    ) -> None:
        existing = self._head(saved.bucket, saved.object_key)
        if existing is not None:
            self._verify(saved, existing, source_identity)
            return
        try:
            self.s3_client.put_object(
                Bucket=saved.bucket,
                Key=saved.object_key,
                Body=body,
                ContentLength=saved.byte_length,
                ContentType=saved.media_type,
                Metadata=RetainedObjectMetadata(
                    sha256=saved.sha256,
                    source_identity_sha256=source_identity,
                    media_kind=media_kind,
                ).model_dump(),
                IfNoneMatch="*",
            )
        except Exception as error:
            # An ambiguous PUT and a concurrent conditional PUT can both have
            # committed the exact object. Verify it before reporting uncertainty.
            reconciled = self._head(saved.bucket, saved.object_key)
            if reconciled is not None:
                self._verify(saved, reconciled, source_identity)
                return
            raise ResultPreservationError(
                "Object publication is unconfirmed"
            ) from error
        confirmed = self._head(saved.bucket, saved.object_key)
        if confirmed is None:
            raise ResultPreservationError("Object publication is unconfirmed")
        self._verify(saved, confirmed, source_identity)

    def read(self, result: StoredResult) -> S3Object:
        if (
            result.store_ref != self.settings.store_ref
            or result.bucket != self.settings.bucket
        ):
            raise ResultPreservationError(
                "Stored result belongs to another object store"
            )
        existing = self._head(result.bucket, result.object_key)
        if existing is None:
            raise ResultPreservationError("Retained object is unavailable")
        self._verify(result, existing)
        try:
            response = self.s3_client.get_object(
                Bucket=result.bucket, Key=result.object_key
            )
        except Exception as error:
            raise ResultPreservationError(
                "Retained object read is unconfirmed"
            ) from error
        body = response["Body"]
        try:
            self._verify(result, response)
        except Exception:
            body.close()
            raise
        return S3Object(body, result.media_type, result.byte_length)
