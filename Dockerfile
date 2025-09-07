FROM python:3.13-alpine3.21
LABEL maintainer="xuzmonomi.com"

ENV PYTHONBUFFERED=1

COPY ./requirements.txt /tmp/requirments.txt
COPY ./requirements.dev.txt /tmp/requirements.dev.txt
COPY ./app /app
COPY ./scripts /scripts
WORKDIR /app
EXPOSE 8000

ARG DEV=false
RUN python -m venv /py && \
    /py/bin/pip install --upgrade pip && \
    apk add --update --no-cache postgresql-client jpeg-dev && \
    apk add --update --no-cache --virtual .tmp-build-deps \
        build-base postgresql-dev musl-dev zlib zlib-dev linux-headers && \
    /py/bin/pip install -r /tmp/requirments.txt && \
    if [ $DEV = "true" ]; \
        then /py/bin/pip install -r /tmp/requirements.dev.txt ; \
    fi && \
    rm -rf /tmp && \
    apk del .tmp-build-deps && \
    adduser \
        --disabled-password \
        --no-create-home \ 
        app-user && \
    mkdir -p /vol/web/media && \ 
    mkdir -p /vol/web/static && \
    chown -R app-user:app-user /vol && \
    chmod -R 755 /vol && \
    chmod -R +x /scripts

ENV PATH="/scripts:/py/bin:$PATH"

USER app-user

CMD ["run.sh"]