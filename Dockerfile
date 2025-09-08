FROM python:3.13-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.8.15 /uv /uvx /bin/

ENV APP_HOME="/home/app"
ENV APP_USER="app"

ENV LOG_PATH=${APP_HOME}/logs
ENV EXPORT_PATH=${APP_HOME}/exports

RUN useradd --create-home ${APP_USER}
USER ${APP_USER}
RUN mkdir -p ${LOG_PATH}
RUN mkdir -p ${EXPORT_PATH}

ADD . ${APP_HOME}
WORKDIR ${APP_HOME}
RUN uv sync --locked

VOLUME ${LOG_PATH}
VOLUME ${EXPORT_PATH}
ENTRYPOINT ["uv", "run", "src/main.py"]
