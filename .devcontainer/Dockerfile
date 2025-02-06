FROM  mcr.microsoft.com/devcontainers/python:3.13

# The installer requires curl (and certificates) to download the release archive
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

ENV UV_SYSTEM_PYTHON=1
ENV UV_LINK_MODE=copy
