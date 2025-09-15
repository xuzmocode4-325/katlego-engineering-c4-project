FROM python:3.13-alpine3.21
LABEL maintainer="xuzmonomi.com"

# Headless + unbuffered Python output for logs
ENV PYTHONUNBUFFERED=1 \
    MPLBACKEND=Agg

# Copy app & requirements
COPY ./requirements.txt /tmp/requirements.txt
COPY ./requirements.dev.txt /tmp/requirements.dev.txt
COPY ./app /app
COPY ./scripts /scripts
WORKDIR /app
EXPOSE 8000

ARG DEV=false

# System deps:
# - Runtime: libstdc++ (matplotlib), fontconfig + fonts, libpq (postgresql-client), jpeg (Pillow)
# - Build deps (removed later): compilers, headers, zlib, postgres-dev
RUN python -m venv /py && \
    /py/bin/pip install --upgrade pip && \
    apk add --no-cache \
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
        linux-headers && \
    /py/bin/pip install -r /tmp/requirements.txt && \
    if [ "$DEV" = "true" ]; then /py/bin/pip install -r /tmp/requirements.dev.txt ; fi && \
    rm -rf /tmp && \
    apk del .tmp-build-deps && \
    adduser --disabled-password --no-create-home app-user && \
    mkdir -p /vol/web/media /vol/web/static && \
    chown -R app-user:app-user /vol && \
    chmod -R 755 /vol && \
    chmod -R +x /scripts

ENV PATH="/scripts:/py/bin:$PATH"

USER app-user

CMD ["run.sh"]
