FROM python:3.11-slim AS base

RUN groupadd --gid 1001 extractor \
 && useradd  --uid 1001 --gid extractor --shell /bin/bash --create-home extractor

WORKDIR /app

FROM base AS builder

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM base AS runtime

COPY --from=builder /install /usr/local
COPY yahoo_extractor.py .

# Remove build tools not needed at runtime
# This also removes the vulnerable vendored jaraco.context and wheel
RUN pip uninstall -y setuptools wheel pip 2>/dev/null || true

RUN chown -R extractor:extractor /app
USER extractor

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import requests, boto3; print('ok')" || exit 1

ENTRYPOINT ["python", "yahoo_extractor.py"]