FROM python:3.12-slim

# Install UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy project files
COPY . /app

# Disable development dependencies
ENV UV_NO_DEV=1

# Sync dependencies
WORKDIR /app
RUN uv sync --locked

# Start Jupyter Lab
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]