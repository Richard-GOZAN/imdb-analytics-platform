# Dockerfile for IMDB Chat Application
# 
# This container runs the Streamlit chat interface for querying IMDB data.

FROM python:3.11-slim

# Metadata
LABEL maintainer="your.email@example.com"
LABEL description="IMDB Analytics - Chat Application"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# Install system dependencies and uv
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/0.5.11/install.sh | sh \
    && ln -s /root/.local/bin/uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock ./
COPY app/ ./app/
COPY .env.template ./.env.template

# Install Python dependencies
RUN uv sync --frozen

# Expose Streamlit port
EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Run Streamlit from parent directory so imports work
CMD ["uv", "run", "python", "-m", "streamlit", "run", "app/chat.py", "--server.port=8501", "--server.address=0.0.0.0"]
