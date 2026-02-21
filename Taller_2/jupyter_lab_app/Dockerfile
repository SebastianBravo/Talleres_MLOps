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

# Expose Jupyter Lab port
EXPOSE 8888

# Start Jupyter Lab
CMD ["uv", "run", "jupyter", "lab", "--ip=0.0.0.0", "--allow-root"]