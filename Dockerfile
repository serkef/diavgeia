FROM python:3.8.1-slim-buster

# Setup paths and users
ENV APP_HOME="/home/app"
ENV APP_USER="app"
ENV LOG_DIR=${APP_HOME}/logs
RUN useradd --create-home ${APP_USER}
WORKDIR ${APP_HOME}
RUN mkdir ${LOG_DIR}

# Install system dependencies
RUN apt-get update \
    && apt-get install -y build-essential vim \
    && pip -qq --no-cache-dir install 'poetry==1.0.5' \
    && poetry config virtualenvs.create false

# Install project & dependencies (check .dockerignore for exceptions)
COPY . .
RUN poetry install --no-dev --no-interaction

# Set permissions and user
RUN chown -R ${APP_USER}:${APP_USER} .
USER ${APP_USER}

# Run
VOLUME ${LOG_DIR}
ENTRYPOINT ["diavgeia-daily"]
CMD []
