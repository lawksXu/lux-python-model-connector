"""Inspect bounded retained bytes with ffprobe, never provider metadata alone."""

import hashlib
import json
import subprocess
from dataclasses import dataclass
from tempfile import NamedTemporaryFile

from pydantic import BaseModel, ConfigDict, Field


class ProbeStream(BaseModel):
    model_config = ConfigDict(extra="ignore")
    codec_type: str
    width: int | None = None
    height: int | None = None
    duration: str | None = None
    nb_read_frames: str | None = None


class ProbeFormat(BaseModel):
    model_config = ConfigDict(extra="ignore")
    duration: str | None = None


class ProbeResult(BaseModel):
    model_config = ConfigDict(extra="ignore")
    streams: tuple[ProbeStream, ...]
    format: ProbeFormat = Field(default_factory=ProbeFormat)


@dataclass(frozen=True)
class InspectedMedia:
    output_format: str
    width: int
    height: int
    duration_seconds: float | None
    has_audio: bool


def inspect_media(body: bytes, media_type: str) -> InspectedMedia:
    if media_type == "image/png" and body.startswith(b"\x89PNG\r\n\x1a\n"):
        output_format = "PNG"
    elif media_type == "image/jpeg" and body.startswith(b"\xff\xd8\xff"):
        output_format = "JPEG"
    elif media_type in ("video/mp4", "video/quicktime") and body[4:8] == b"ftyp":
        output_format = "MOV" if body[8:12] == b"qt  " else "MP4"
        if media_type != ("video/quicktime" if output_format == "MOV" else "video/mp4"):
            raise ValueError("Retained container differs from MIME type")
    else:
        raise ValueError("Unsupported media signature or format")
    with NamedTemporaryFile() as source:
        source.write(body)
        source.flush()
        completed = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-protocol_whitelist",
                "file",
                "-count_frames",
                "-show_entries",
                "stream=codec_type,width,height,duration,nb_read_frames:format=duration",
                "-show_format",
                "-of",
                "json",
                source.name,
            ],
            capture_output=True,
            timeout=20,
            check=False,
        )
    if (
        completed.returncode != 0
        or completed.stderr.strip()
        or len(completed.stdout) > 1024 * 1024
    ):
        raise ValueError("Media probe failed")
    probe = ProbeResult.model_validate(json.loads(completed.stdout))
    visual = tuple(stream for stream in probe.streams if stream.codec_type == "video")
    if len(visual) != 1 or not visual[0].width or not visual[0].height:
        raise ValueError("Expected exactly one visual stream")
    # Counting frames forces decoding; headers alone can describe missing samples.
    for stream in probe.streams:
        if stream.codec_type in ("video", "audio") and (
            stream.nb_read_frames is None
            or not stream.nb_read_frames.isdecimal()
            or int(stream.nb_read_frames) < 1
        ):
            raise ValueError("Media stream contains no decodable frames")
    duration = probe.format.duration or visual[0].duration
    return InspectedMedia(
        output_format,
        visual[0].width,
        visual[0].height,
        float(duration) if duration is not None else None,
        any(stream.codec_type == "audio" for stream in probe.streams),
    )


def verify_content(body: bytes, expected_length: int, expected_sha256: str) -> None:
    if (
        len(body) != expected_length
        or hashlib.sha256(body).hexdigest() != expected_sha256
    ):
        raise ValueError("Media length or checksum differs from immutable metadata")
