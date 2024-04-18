FROM devex-docker.artifactory.zgtools.net/devex/docker-images/python3.8-build-debian11 AS base
# These images can be found here: http://zg/docker-images

WORKDIR /app

ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV VIRTUALENV_ACTIVATE_FILE=/app/.venv/bin/activate

ARG POETRY_VERSION_SPECIFIER=">=1.0.0"
RUN pip install "poetry$POETRY_VERSION_SPECIFIER"

# cache layer for dependency changes
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-interaction --no-ansi --no-dev --no-root

COPY snowflake_export ./snowflake_export
RUN poetry install --no-interaction --no-ansi --no-dev

FROM base AS test

COPY tests ./tests

# install dev dependencies(pytest)
RUN poetry install --no-interaction --no-ansi && \
    poetry run pytest --cov=snowflake_export && \
    poetry run black snowflake_export tests --check

FROM base AS final

COPY scripts/entrypoint.sh /usr/local/bin

ENTRYPOINT ["entrypoint.sh"]
