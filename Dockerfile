FROM python:3.14-slim-trixie AS build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN python -m venv /opt/venv \
    && /opt/venv/bin/python -m pip install --no-cache-dir . \
    && /opt/venv/bin/python -m pip check

FROM python:3.14-slim-trixie AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PATH="/opt/venv/bin:$PATH"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && ffmpeg -protocols > /tmp/ffmpeg-protocols.txt 2>/dev/null \
    && grep -Eq '^[[:space:]]*fd[[:space:]]*$' /tmp/ffmpeg-protocols.txt \
    && ffprobe -protocols > /tmp/ffprobe-protocols.txt 2>/dev/null \
    && grep -Eq '^[[:space:]]*fd[[:space:]]*$' /tmp/ffprobe-protocols.txt \
    && rm -rf /var/lib/apt/lists/* /tmp/ffmpeg-protocols.txt /tmp/ffprobe-protocols.txt \
    && groupadd --gid 10001 lux \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin lux

RUN install -d -o lux -g lux -m 0700 /var/lib/lux-model-connector

COPY --from=build /opt/venv /opt/venv
WORKDIR /app
USER 10001:10001
EXPOSE 8001
CMD ["python", "-m", "uvicorn", "lux_model_connector.main:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1"]
