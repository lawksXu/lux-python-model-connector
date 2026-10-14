import asyncio
import base64
import hashlib
import io
import threading
from dataclasses import dataclass
from typing import BinaryIO, Literal

import httpx
import pytest

from lux_model_connector.results import (
    PreparedOutput,
    ResultPreservationError,
    ResultSourceUnavailable,
    S3ResultStore,
    S3Settings,
)

USER = "11111111-1111-4111-8111-111111111111"
TASK = "22222222-2222-4222-8222-222222222222"
PNG = b"\x89PNG\r\n\x1a\n" + b"sample-original-png-bytes"
JPEG = b"\xff\xd8\xff\xe0" + b"sample-original-jpeg-bytes"
MP4 = b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom" + b"video"


@dataclass
class FakeObject:
    content: bytes
    media_type: str
    metadata: dict[str, str]


def s3_error(code: str, status: int):
    from botocore.exceptions import ClientError

    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "test-boundary",
    )


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], FakeObject] = {}
        self.puts: list[tuple[str, str]] = []
        self.mode: Literal["lost_ack", "race", "omit", "corrupt"] | None = None
        self.get_body: io.BytesIO | None = None
        self.get_media_type: str | None = None

    def head_object(self, *, Bucket: str, Key: str):
        item = self.objects.get((Bucket, Key))
        if item is None:
            raise s3_error("NoSuchKey", 404)
        return {
            "ContentLength": len(item.content),
            "ContentType": item.media_type,
            "Metadata": item.metadata,
        }

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
    ):
        assert IfNoneMatch == "*"
        assert (Bucket, Key) not in self.objects
        data = Body.read()
        assert len(data) == ContentLength
        self.puts.append((Bucket, Key))
        if self.mode != "omit":
            metadata = dict(Metadata)
            if self.mode == "corrupt":
                metadata["sha256"] = "0" * 64
            self.objects[(Bucket, Key)] = FakeObject(data, ContentType, metadata)
        if self.mode == "lost_ack":
            raise TimeoutError("secret-in-transport-exception")
        if self.mode == "race":
            raise s3_error("PreconditionFailed", 412)
        return {}

    def get_object(self, *, Bucket: str, Key: str):
        response = self.head_object(Bucket=Bucket, Key=Key)
        self.get_body = io.BytesIO(self.objects[(Bucket, Key)].content)
        if self.get_media_type is not None:
            response["ContentType"] = self.get_media_type
        return {**response, "Body": self.get_body}


def settings(**updates) -> S3Settings:
    return S3Settings(
        endpoint_url="https://objects.test",
        region_name="test-region",
        bucket="connector-results",
        access_key_id="storage-access-secret",
        secret_access_key="storage-private-secret",
        **updates,
    )


def output(**updates) -> PreparedOutput:
    return PreparedOutput(
        source_index=0,
        media_kind="IMAGE",
        url="https://provider-output.test/original.png",
        **updates,
    )


def run(coroutine):
    return asyncio.run(coroutine)


def test_preserves_original_bytes_and_sniffs_type_without_download_credentials():
    requests: list[httpx.Request] = []
    s3 = FakeS3()

    def reply(request: httpx.Request):
        requests.append(request)
        return httpx.Response(200, content=PNG, headers={"Content-Type": "text/html"})

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(reply),
            headers={"Authorization": "Bearer private-provider-key"},
            auth=("private", "password"),
        ) as client:
            return await S3ResultStore(settings(), client, s3).preserve(
                USER, TASK, output(expected_format="jpeg")
            )

    stored = run(scenario())
    assert requests[0].headers.get("Authorization") is None
    assert stored.media_type == "image/png"
    assert stored.filename == "0.png"
    assert stored.sha256 == hashlib.sha256(PNG).hexdigest()
    assert stored.byte_length == len(PNG)
    assert stored.object_key == f"connector-results/{USER}/{TASK}/0"
    assert s3.objects[(stored.bucket, stored.object_key)].content == PNG
    assert "private-secret" not in repr(settings())


def test_base64_output_and_mp4_have_explicit_original_media_types():
    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=MP4))
        ) as client:
            store = S3ResultStore(settings(), client, FakeS3())
            image = await store.preserve(
                USER,
                TASK,
                PreparedOutput(
                    source_index=1,
                    media_kind="IMAGE",
                    b64_json=base64.b64encode(JPEG).decode(),
                ),
            )
            video = await store.preserve(
                USER,
                TASK,
                PreparedOutput(
                    source_index=2,
                    media_kind="VIDEO",
                    url="https://provider-output.test/video",
                ),
            )
            return image, video

    image, video = run(scenario())
    assert (image.media_type, image.filename) == ("image/jpeg", "1.jpg")
    assert (video.media_type, video.filename) == ("video/mp4", "2.mp4")


def test_repeating_receipt_reuses_first_original_without_downloading_again():
    s3 = FakeS3()
    downloads = iter([PNG, PNG, PNG + b"different"])
    requests: list[httpx.Request] = []

    def reply(request: httpx.Request):
        requests.append(request)
        return httpx.Response(200, content=next(downloads))

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
            store = S3ResultStore(settings(), client, s3)
            first = await store.preserve(USER, TASK, output())
            same = await store.preserve(USER, TASK, output())
            changed = await store.preserve(USER, TASK, output())
            return first, same, changed

    first, same, changed = run(scenario())
    assert first == same
    assert changed == first
    assert len(s3.puts) == len(requests) == 1
    assert s3.objects[(first.bucket, first.object_key)].content == PNG


@pytest.mark.parametrize("mode", ["lost_ack", "race"])
def test_uncertain_or_concurrent_put_reconciles_same_verified_object(mode):
    s3 = FakeS3()
    s3.mode = mode

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=PNG))
        ) as client:
            return await S3ResultStore(settings(), client, s3).preserve(
                USER, TASK, output()
            )

    assert run(scenario()).sha256 == hashlib.sha256(PNG).hexdigest()
    assert len(s3.puts) == 1


@pytest.mark.parametrize("mode", ["omit", "corrupt"])
def test_put_response_alone_cannot_confirm_preservation(mode):
    s3 = FakeS3()
    s3.mode = mode

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=PNG))
        ) as client:
            await S3ResultStore(settings(), client, s3).preserve(USER, TASK, output())

    with pytest.raises(ResultPreservationError):
        run(scenario())


class BrokenDownload(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield PNG[:8]
        raise httpx.ReadError("private-signed-url-must-not-be-logged")


def test_interrupted_download_has_no_partial_object_and_retries_original():
    s3 = FakeS3()
    replies = iter(
        [httpx.Response(200, stream=BrokenDownload()), httpx.Response(200, content=PNG)]
    )

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: next(replies))
        ) as client:
            store = S3ResultStore(settings(), client, s3)
            with pytest.raises(ResultPreservationError) as failure:
                await store.preserve(USER, TASK, output())
            assert "private-signed" not in str(failure.value)
            assert not s3.objects
            return await store.preserve(USER, TASK, output())

    assert run(scenario()).byte_length == len(PNG)


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, content=PNG, headers={"Content-Length": "1000"}),
        httpx.Response(200, content=PNG),
    ],
)
def test_download_budget_applies_to_advertised_and_actual_bytes(response):
    s3 = FakeS3()

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: response)
        ) as client:
            await S3ResultStore(settings(), client, s3, max_bytes=16).preserve(
                USER, TASK, output()
            )

    with pytest.raises(ResultPreservationError):
        run(scenario())
    assert not s3.puts


@pytest.mark.parametrize("status", [404, 410])
def test_expired_original_is_a_definite_result_source_failure(status):
    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(status))
        ) as client:
            await S3ResultStore(settings(), client, FakeS3()).preserve(
                USER, TASK, output()
            )

    with pytest.raises(ResultSourceUnavailable):
        run(scenario())


@pytest.mark.parametrize("status", [403, 429, 500, 503])
def test_other_source_http_failures_remain_retryable(status):
    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(status))
        ) as client:
            await S3ResultStore(settings(), client, FakeS3()).preserve(
                USER, TASK, output()
            )

    with pytest.raises(ResultPreservationError) as failure:
        run(scenario())
    assert not isinstance(failure.value, ResultSourceUnavailable)


@pytest.mark.parametrize("encoded", ["!!!!", "eA==eA==", "", "AAAAA"])
def test_invalid_base64_cannot_upload(encoded):
    s3 = FakeS3()

    async def scenario():
        async with httpx.AsyncClient() as client:
            await S3ResultStore(settings(), client, s3).preserve(
                USER,
                TASK,
                PreparedOutput(source_index=0, media_kind="IMAGE", b64_json=encoded),
            )

    with pytest.raises((ValueError, ResultPreservationError)):
        run(scenario())
    assert not s3.objects


@pytest.mark.parametrize("content", [b"<html>expired</html>", MP4])
def test_unrecognized_or_wrong_kind_output_cannot_be_success(content):
    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, content=content)
            )
        ) as client:
            await S3ResultStore(settings(), client, FakeS3()).preserve(
                USER, TASK, output()
            )

    with pytest.raises(ResultPreservationError):
        run(scenario())


def test_redirect_downloads_never_receive_provider_or_object_store_credentials():
    requests: list[httpx.Request] = []

    def reply(request: httpx.Request):
        requests.append(request)
        if request.url.host == "provider-output.test":
            return httpx.Response(302, headers={"Location": "https://cdn.test/image"})
        return httpx.Response(200, content=PNG)

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(reply),
            headers={"Authorization": "Bearer private-provider-key"},
        ) as client:
            return await S3ResultStore(settings(), client, FakeS3()).preserve(
                USER, TASK, output()
            )

    run(scenario())
    assert len(requests) == 2
    assert all(request.headers.get("Authorization") is None for request in requests)


def test_read_checks_storage_identity_and_metadata_and_closes_mismatched_get():
    s3 = FakeS3()

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=PNG))
        ) as client:
            store = S3ResultStore(settings(), client, s3)
            saved = await store.preserve(USER, TASK, output())
            stream = store.read(saved)
            assert stream.body.read() == PNG
            stream.body.close()
            other = S3ResultStore(settings(prefix="other-prefix"), client, s3)
            with pytest.raises(ResultPreservationError):
                other.read(saved)
            s3.get_media_type = "image/jpeg"
            with pytest.raises(ResultPreservationError):
                store.read(saved)
            assert s3.get_body is not None and s3.get_body.closed
            s3.get_media_type = None
            item = s3.objects[(saved.bucket, saved.object_key)]
            item.metadata["sha256"] = "0" * 64
            with pytest.raises(ResultPreservationError):
                store.read(saved)

    run(scenario())


@pytest.mark.parametrize(
    "url", ["file:///tmp/picture.png", "https://user:secret@output.test/a", "http:///a"]
)
def test_output_source_rejects_non_http_or_embedded_credentials(url):
    with pytest.raises(ValueError):
        PreparedOutput(source_index=0, media_kind="IMAGE", url=url)


def test_result_key_identity_is_not_changed_by_rotating_credentials():
    first = settings()
    rotated = first.model_copy(update={"access_key_id": "rotated-key"})
    assert first.store_ref == rotated.store_ref
    assert first.store_ref != settings(prefix="different").store_ref


def test_cancelled_preservation_keeps_upload_file_open_until_sdk_returns():
    started = threading.Event()
    release = threading.Event()
    s3 = FakeS3()
    original_put = s3.put_object

    def blocked_put(**parameters):
        started.set()
        assert release.wait(5)
        return original_put(**parameters)

    s3.put_object = blocked_put

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=PNG))
        ) as client:
            preservation = asyncio.create_task(
                S3ResultStore(settings(), client, s3).preserve(USER, TASK, output())
            )
            assert await asyncio.to_thread(started.wait, 5)
            preservation.cancel()
            await asyncio.sleep(0)
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await preservation
            assert len(s3.objects) == 1

    run(scenario())


def test_truncated_declared_original_is_not_published():
    s3 = FakeS3()

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200, content=PNG, headers={"Content-Length": str(len(PNG) + 10)}
                )
            )
        ) as client:
            await S3ResultStore(settings(), client, s3).preserve(USER, TASK, output())

    with pytest.raises(ResultPreservationError):
        run(scenario())
    assert not s3.puts


def test_object_survives_missing_sql_checkpoint_and_expired_original_url():
    s3 = FakeS3()
    calls: list[httpx.Request] = []

    def reply(request: httpx.Request):
        calls.append(request)
        return (
            httpx.Response(200, content=PNG) if len(calls) == 1 else httpx.Response(410)
        )

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
            saved = await S3ResultStore(settings(), client, s3).preserve(
                USER, TASK, output()
            )
            # A new worker has the immutable receipt, but no saved SQL manifest.
            recovered = await S3ResultStore(settings(), client, s3).preserve(
                USER, TASK, output()
            )
            assert recovered == saved

    run(scenario())
    assert len(calls) == 1
    assert len(s3.puts) == 1


def test_existing_source_slot_rejects_changed_receipt_without_overwrite():
    s3 = FakeS3()

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=PNG))
        ) as client:
            store = S3ResultStore(settings(), client, s3)
            await store.preserve(USER, TASK, output())
            changed = output().model_copy(
                update={"url": "https://provider-output.test/a-different-output.png"}
            )
            with pytest.raises(ResultPreservationError):
                await store.preserve(USER, TASK, changed)

    run(scenario())
    assert len(s3.puts) == 1
