# --- Stage 1: Build stage with all dependencies ---
FROM python:3.13-alpine3.21 AS builder

# Install build-time dependencies, including libstdc++
RUN apk add --no-cache \
        postgresql-client \
        libstdc++ \
        libgcc \
        freetype \
        fontconfig \
        ttf-dejavu \
        jpeg && \
    apk add --no-cache --virtual .tmp-build-deps \
        build-base \
        postgresql-dev \
        musl-dev \
        zlib \
        zlib-dev \
        linux-headers

WORKDIR /app
COPY ./requirements.txt /tmp/requirements.txt
# Install Python dependencies into a virtual environment
RUN python -m venv /py && \
    /py/bin/pip install --no-cache-dir --upgrade pip && \
    /py/bin/pip install --no-cache-dir -r /tmp/requirements.txt && \
    # Clean up temporary build dependencies
    apk del .tmp-build-deps

# --- Stage 2: Final lightweight runtime stage ---
FROM python:3.13-alpine3.21

# Headless + unbuffered Python output for logs
ENV PYTHONUNBUFFERED=1 \
    MPLBACKEND=Agg \
    PATH="/py/bin:$PATH"

# Copy the virtual environment and installed packages from the builder stage
COPY --from=builder /py /py
# Copy other runtime assets and code
COPY ./app /app
COPY ./scripts /scripts

WORKDIR /app
EXPOSE 8000

# Install minimal runtime dependencies needed for the application
RUN apk add --no-cache \
        postgresql-client \
        libstdc++ \
        libgcc \
        freetype \
        fontconfig \
        ttf-dejavu \
        jpeg

# Set up user and permissions
RUN adduser --disabled-password --no-create-home app-user && \
    mkdir -p /vol/web/media /vol/web/static && \
    chown -R app-user:app-user /vol && \
    chmod -R 755 /vol && \
    chmod -R +x /scripts

USER app-user

CMD ["run.sh"]
