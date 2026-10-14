"""Provider protocol and single-attempt submission behavior, without live calls."""

import asyncio
import json

import httpx
import pytest
from pydantic import ValidationError

from lux_model_connector.byteplus import (
    BytePlusClient,
    BytePlusQueryError,
    BytePlusRejected,
    BytePlusSettings,
    BytePlusUncertainSubmission,
    ProviderSubmissionPayload,
    SeedanceReceipt,
    SeedanceRequest,
    SeedreamReceipt,
    SeedreamRequest,
)


def test_upstream_model_id_is_required_and_passes_through_unchanged():
    for model_id in ("endpoint-any-new-model", "future-image-model-2040", "新模型"):
        request = SeedreamRequest(model_id=model_id, prompt="a cat")
        assert request.provider_payload() == {"model": model_id, "prompt": "a cat"}
    for value in ({"prompt": "cat"}, {"model_id": " ", "prompt": "cat"}):
        with pytest.raises(ValidationError):
            SeedreamRequest.model_validate(value)


def test_all_documented_image_options_are_preserved_and_defaults_are_omitted():
    body = {
        "model_id": "any-image-endpoint",
        "prompt": "cat",
        "image": ["https://example.org/a.png", "data:image/png;base64,YQ=="],
        "layer_decomposition": False,
        "size": "4K",
        "optimize_prompt_options": {"mode": "fast"},
        "output_format": "png",
        "background": "opaque",
        "response_format": "b64_json",
        "sequential_image_generation": "auto",
        "sequential_image_generation_options": {"max_images": 3},
        "stream": False,
        "watermark": False,
    }
    request = SeedreamRequest.model_validate(body)
    expected = {key: value for key, value in body.items() if key != "model_id"}
    expected["model"] = body["model_id"]
    assert request.provider_payload() == expected
    assert SeedreamRequest(model_id="any", prompt="cat").provider_payload() == {
        "model": "any",
        "prompt": "cat",
    }
    decomposition = SeedreamRequest(
        model_id="any", image="https://example.org/a.png", layer_decomposition=True
    )
    assert "prompt" not in decomposition.provider_payload()


def test_video_options_and_all_content_variants_have_defined_shapes():
    body = {
        "model_id": "new-video-endpoint",
        "content": [
            {"type": "text", "text": "move"},
            {
                "type": "image_url",
                "image_url": {"url": "asset://provider-digital-character"},
                "role": "reference_image",
            },
            {
                "type": "video_url",
                "video_url": {"url": "https://example.org/a.mov"},
                "role": "reference_video",
            },
            {
                "type": "audio_url",
                "audio_url": {"url": "data:audio/wav;base64,YQ=="},
                "role": "reference_audio",
            },
            {"type": "draft_task", "draft_task": {"id": "cgt-original-draft"}},
        ],
        "omni_reference_task_type": "auto",
        "resolution": "4k",
        "ratio": "adaptive",
        "duration": -1,
        "frames": 57,
        "generate_audio": False,
        "watermark": False,
        "output_format": "mov",
        "seed": -1,
        "camera_fixed": False,
        "return_last_frame": True,
        "draft": False,
        "service_tier": "default",
        "callback_url": "https://example.org/callback",
        "execution_expires_after": 3600,
        "priority": 9,
        "safety_identifier": "hashed-user",
    }
    request = SeedanceRequest.model_validate(body)
    expected = {key: value for key, value in body.items() if key != "model_id"}
    expected["model"] = body["model_id"]
    assert request.provider_payload() == expected
    assert request.content[1].image_url.url == "asset://provider-digital-character"
    assert SeedanceRequest(
        model_id="unlisted",
        content=(
            {"type": "audio_url", "audio_url": {"url": "https://example.org/a.wav"}},
        ),
    ).provider_payload() == {
        "model": "unlisted",
        "content": [
            {"type": "audio_url", "audio_url": {"url": "https://example.org/a.wav"}}
        ],
    }


@pytest.mark.parametrize(
    "body",
    [
        {"model_id": "x", "prompt": "cat", "stream": True},
        {"model_id": "x", "prompt": "cat", "watermark": None},
        {"model_id": "x", "prompt": "cat", "optimize_prompt_options": {"mode": None}},
        {"model_id": "x", "prompt": "cat", "watermark": 0},
        {"model_id": "x", "prompt": "cat", "unknown_parameter": 1},
        {"model_id": "x", "layer_decomposition": True},
        {"model_id": "x"},
        {
            "model_id": "x",
            "prompt": "cat",
            "sequential_image_generation_options": {"max_images": 0},
        },
    ],
)
def test_image_request_rejects_unhandled_transport_and_invalid_shapes(body):
    with pytest.raises(ValidationError):
        SeedreamRequest.model_validate(body)


@pytest.mark.parametrize(
    "patch",
    [
        {"content": []},
        {"content": [{"type": "asset_id", "asset_id": "java-asset"}]},
        {"content": [{"type": "text", "text": None}]},
        {
            "content": [
                {
                    "type": "image_url",
                    "image_url": {"url": "https://example.org/a.png"},
                    "role": None,
                }
            ]
        },
        {"seed": True},
        {"seed": 2147483648},
        {"priority": 10},
        {"execution_expires_after": 3599},
        {"generate_audio": None},
        {"safety_identifier": "a" * 65},
    ],
)
def test_video_request_rejects_invalid_known_shapes_and_unresolved_java_assets(patch):
    with pytest.raises(ValidationError):
        SeedanceRequest.model_validate(
            {"model_id": "x", "content": [{"type": "text", "text": "move"}], **patch}
        )


def test_submission_archive_is_typed_immutable_and_has_no_credentials():
    payload = ProviderSubmissionPayload(
        account_ref="byteplus-primary",
        kind="IMAGE",
        request=SeedreamRequest(model_id="endpoint-new", prompt="cat"),
    )
    assert payload.provider_id == "byteplus"
    assert "api_key" not in payload.model_dump_json()
    assert (
        ProviderSubmissionPayload.model_validate_json(payload.model_dump_json())
        == payload
    )
    with pytest.raises(ValidationError):
        payload.kind = "VIDEO"
    with pytest.raises(ValidationError):
        ProviderSubmissionPayload(kind="VIDEO", request=payload.request)


def test_synchronous_image_receipt_retains_partial_errors_usage_and_layer_metadata():
    body = {
        "created": 123,
        "model": "provider-resolved-model",
        "data": [
            {
                "url": "https://example.org/base.png",
                "size": "1024x1024",
                "output_format": "png",
                "z_index": 0,
                "name": "base",
                "description": "background",
            },
            {
                "b64_json": "YQ==",
                "z_index": 1,
                "bounding_box": {
                    "absolute": [1, 2, 3, 4],
                    "normalized": [10, 20, 30, 40],
                },
            },
            {"error": {"code": "Moderation", "message": "blocked"}},
        ],
        "usage": {
            "generated_images": 2,
            "input_images": 1,
            "output_tokens": 10,
            "total_tokens": 10,
        },
    }
    receipt = SeedreamReceipt.model_validate(body)
    assert receipt.model_dump(mode="json", exclude_unset=True) == body
    assert receipt.data[2].error.code == "Moderation"
    assert receipt.data[1].bounding_box.absolute == (1, 2, 3, 4)
    assert (
        SeedreamReceipt.model_validate(
            {"error": {"code": "Failed", "message": "none"}}
        ).error.code
        == "Failed"
    )


def test_query_receipt_has_typed_output_and_rejects_unknown_status():
    receipt = SeedanceReceipt.model_validate(
        {
            "id": "cgt-original",
            "status": "succeeded",
            "content": {
                "video_url": "https://example.org/a.mp4",
                "last_frame_url": "https://example.org/a.jpg",
            },
            "usage": {"completion_tokens": 12, "total_tokens": 12},
            "error": None,
            "output_format": "mp4",
        }
    )
    assert receipt.content.video_url == "https://example.org/a.mp4"
    with pytest.raises(ValidationError):
        SeedanceReceipt.model_validate(
            {"id": "cgt-original", "status": "future-unknown"}
        )


def test_query_receipt_preserves_documented_frame_count_and_nullable_metadata():
    body = {
        "id": "cgt-original",
        "status": "succeeded",
        "frames": 57,
        "framespersecond": 24,
        "error": None,
        "content": {"video_url": "https://example.org/a.mp4", "last_frame_url": None},
    }
    receipt = SeedanceReceipt.model_validate(body)
    assert receipt.model_dump(mode="json", exclude_unset=True) == body


@pytest.mark.parametrize(
    "raw",
    [
        b'{"id":"original","id":"different"}',
        b'{"id":"original","ignored":NaN}',
        b"\xff",
    ],
)
def test_malformed_provider_json_cannot_confirm_a_submission(raw):
    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=raw)
            )
        ) as http_client:
            client = BytePlusClient(BytePlusSettings(api_key="secret"), http_client)
            with pytest.raises(BytePlusUncertainSubmission):
                await client.create_video(
                    SeedanceRequest(
                        model_id="anything", content=({"type": "text", "text": "move"},)
                    )
                )

    asyncio.run(scenario())


@pytest.mark.parametrize("status", [404, 429, 500])
def test_failed_http_observation_never_becomes_submission_rejection(status):
    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(status, content=b"error")
            )
        ) as http_client:
            client = BytePlusClient(BytePlusSettings(api_key="secret"), http_client)
            with pytest.raises(BytePlusQueryError) as caught:
                await client.get_video("original")
            assert caught.value.status_code == status

    asyncio.run(scenario())


def test_provider_redirect_is_not_followed_with_authorization():
    observed = []

    def handler(request):
        observed.append(request)
        return httpx.Response(
            307, headers={"Location": "https://another.example/generate"}
        )

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        ) as http_client:
            client = BytePlusClient(BytePlusSettings(api_key="secret"), http_client)
            with pytest.raises(BytePlusUncertainSubmission):
                await client.generate_image(
                    SeedreamRequest(model_id="anything", prompt="cat")
                )

    asyncio.run(scenario())
    assert len(observed) == 1


def test_http_adapter_uses_fixed_official_origin_key_and_selected_model():
    observed = []

    def handler(request):
        observed.append(request)
        if request.url.path.endswith("/images/generations"):
            return httpx.Response(
                200, json={"data": [{"url": "https://example.org/a.png"}]}
            )
        if request.method == "POST":
            return httpx.Response(200, json={"id": "cgt-returned"})
        return httpx.Response(200, json={"id": "cgt-returned", "status": "queued"})

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            client = BytePlusClient(BytePlusSettings(api_key="test-key"), http_client)
            assert (
                (
                    await client.generate_image(
                        SeedreamRequest(model_id="arbitrary", prompt="cat")
                    )
                )
                .data[0]
                .url
            )
            assert (
                await client.create_video(
                    SeedanceRequest(
                        model_id="new-endpoint",
                        content=({"type": "text", "text": "move"},),
                    )
                )
            ).id == "cgt-returned"
            assert (await client.get_video("cgt-returned")).status == "queued"

    asyncio.run(scenario())
    assert [(request.method, str(request.url)) for request in observed] == [
        ("POST", "https://ark.ap-southeast.bytepluses.com/api/v3/images/generations"),
        (
            "POST",
            "https://ark.ap-southeast.bytepluses.com/api/v3/contents/generations/tasks",
        ),
        (
            "GET",
            "https://ark.ap-southeast.bytepluses.com/api/v3/contents/generations/tasks/cgt-returned",
        ),
    ]
    assert all(
        request.headers["Authorization"] == "Bearer test-key" for request in observed
    )
    assert json.loads(observed[0].content) == {"model": "arbitrary", "prompt": "cat"}
    assert json.loads(observed[1].content)["model"] == "new-endpoint"
    assert "test-key" not in repr(BytePlusSettings(api_key="test-key"))


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422, 429])
def test_definitive_rejection_has_typed_evidence_and_does_not_retry(status):
    observed = []

    def handler(request):
        observed.append(request)
        return httpx.Response(
            status,
            json={"error": {"code": "Rejected", "message": "private prompt echoed"}},
        )

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            client = BytePlusClient(BytePlusSettings(api_key="secret"), http_client)
            with pytest.raises(BytePlusRejected) as caught:
                await client.generate_image(
                    SeedreamRequest(model_id="new", prompt="cat")
                )
            assert caught.value.error.code == "Rejected"
            assert caught.value.status_code == status
            assert "private prompt" not in str(caught.value)
            assert "secret" not in str(caught.value)

    asyncio.run(scenario())
    assert len(observed) == 1


@pytest.mark.parametrize(
    "result",
    [
        httpx.Response(408, json={"error": {"code": "Timeout", "message": "timeout"}}),
        httpx.Response(500, json={"error": {"code": "Internal", "message": "error"}}),
        httpx.Response(502, content=b"bad gateway"),
        httpx.Response(200, content=b"not JSON"),
        httpx.Response(200, json={}),
        httpx.Response(200, json={"id": "../escape"}),
        httpx.ReadTimeout("echoed private prompt"),
        httpx.ConnectError("secret"),
    ],
)
def test_ambiguous_submission_never_retries_or_claims_definitive_failure(result):
    attempts = []

    def handler(request):
        attempts.append(request)
        if isinstance(result, Exception):
            raise result
        return result

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            client = BytePlusClient(BytePlusSettings(api_key="secret"), http_client)
            with pytest.raises(BytePlusUncertainSubmission) as caught:
                await client.create_video(
                    SeedanceRequest(
                        model_id="anything", content=({"type": "text", "text": "move"},)
                    )
                )
            assert "secret" not in str(caught.value)
            assert "private prompt" not in str(caught.value)

    asyncio.run(scenario())
    assert len(attempts) == 1


@pytest.mark.parametrize(
    "external_id", ["", " ", "../escape", "a/b", "a?b", "a#b", "a%2fb", "a\nb"]
)
def test_query_path_rejects_unsafe_external_ids_before_http(external_id):
    def handler(request):
        pytest.fail("Unsafe ID must never reach HTTP")

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            client = BytePlusClient(BytePlusSettings(api_key="secret"), http_client)
            with pytest.raises(ValueError):
                await client.get_video(external_id)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "body",
    [{"id": "different", "status": "queued"}, {"id": "original", "status": "unknown"}],
)
def test_query_errors_are_distinct_from_terminal_generation_failure(body):
    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=body)
            )
        ) as http_client:
            client = BytePlusClient(BytePlusSettings(api_key="secret"), http_client)
            with pytest.raises(BytePlusQueryError):
                await client.get_video("original")

    asyncio.run(scenario())


def test_provider_response_has_a_bounded_read_without_resubmission():
    attempts = []

    def handler(request):
        attempts.append(request)
        return httpx.Response(200, content=b" " * 129)

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            client = BytePlusClient(
                BytePlusSettings(api_key="secret", max_response_bytes=128), http_client
            )
            with pytest.raises(BytePlusUncertainSubmission):
                await client.generate_image(
                    SeedreamRequest(model_id="anything", prompt="cat")
                )

    asyncio.run(scenario())
    assert len(attempts) == 1


def test_status_queries_use_a_shorter_timeout_than_generation_submission():
    observed = []

    def handler(request):
        observed.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"id": "original", "status": "running"})
        return httpx.Response(200, json={"id": "original"})

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            client = BytePlusClient(BytePlusSettings(api_key="secret"), http_client)
            await client.create_video(
                SeedanceRequest(
                    model_id="anything", content=({"type": "text", "text": "move"},)
                )
            )
            await client.get_video("original")

    asyncio.run(scenario())
    assert observed[0].extensions["timeout"]["read"] == 600
    assert observed[1].extensions["timeout"]["read"] == 30


@pytest.mark.parametrize("value", [0, -1, 121, True, float("nan"), float("inf")])
def test_query_timeout_rejects_unbounded_or_invalid_configuration(value):
    with pytest.raises(ValueError):
        BytePlusSettings(api_key="secret", query_timeout_seconds=value)
