FROM python:3.12-slim
WORKDIR /app
# hatch-vcs derives the version from git tags, which aren't in the build
# context; CI passes the tag as VERSION.
ARG VERSION=0.0.0+unknown
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION}
RUN pip install --no-cache-dir uv
COPY pyproject.toml README.md ./
COPY src/ src/
RUN uv pip install --system --no-cache .

# Mount a directory holding config.yaml and views.yaml at /data;
# state.db is written next to views.yaml.
EXPOSE 8000
ENTRYPOINT ["iceberg-ivm"]
CMD ["-c", "/data/config.yaml", "--views", "/data/views.yaml"]
