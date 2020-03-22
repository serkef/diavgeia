FROM python:3.8.2-slim-buster

# Setup paths and users
ENV APP_HOME="/home/app"
ENV APP_USER="app"
RUN useradd --create-home ${APP_USER}
WORKDIR ${APP_HOME}

# Install system dependencies
RUN pip -qq --no-cache-dir install 'poetry==1.0.5' \
    && poetry config virtualenvs.create false

# Install project & dependencies (check .dockerignore for exceptions)
COPY . .
RUN poetry install --no-dev --no-interaction

# Set permissions and user
RUN chown -R ${APP_USER}:${APP_USER} .
USER ${APP_USER}

# Run
ENTRYPOINT ["diavgeia-daily"]
