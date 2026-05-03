FROM python:3.12-slim
WORKDIR /app
# Version is derived from git tags by hatch-vcs. The build context doesn't
# include `.git`, so CI passes the tag here as VERSION=0.1.1 and
# setuptools-scm picks it up via the pretend-version env var below.
ARG VERSION=0.0.0+unknown
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION}
RUN pip install uv
COPY pyproject.toml README.md ./
COPY src/ src/
RUN uv pip install --system .

ENTRYPOINT ["iceberg-ivm"]
CMD ["-c", "/app/config.yaml"]
