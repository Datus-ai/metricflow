FROM m.daocloud.io/docker.io/library/python:3.12-slim

# Set environment variables for non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive

# Use Chinese mirror for faster package installation
RUN sed -i 's|http://deb.debian.org/debian|https://mirrors.tuna.tsinghua.edu.cn/debian|g' /etc/apt/sources.list.d/debian.sources \
    && sed -i 's|http://security.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g' /etc/apt/sources.list.d/debian.sources

# Install system dependencies including Node.js and npm
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    libpq-dev \
    postgresql-client \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml poetry.lock* README.md ./
COPY metricflow/ ./metricflow/
COPY datus_mf/ ./datus_mf/
COPY mcp_metricflow/ ./mcp_metricflow/
COPY docker-init.sh ./
RUN chmod +x docker-init.sh

# Configure pip to use Chinese mirror
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple/

# Install poetry
RUN pip install poetry

# Configure poetry to not create virtual environment and use Chinese mirror
RUN poetry config virtualenvs.create false \
    && poetry source add --priority=primary tsinghua https://pypi.tuna.tsinghua.edu.cn/simple/

# Remove old lock file and generate new one, then install dependencies
RUN rm -f poetry.lock \
    && poetry lock \
    && poetry install --only=main

# Create directories and set environment variables
RUN mkdir -p /root/.metricflow/semantic_models /root/.datus/conf
ENV MF_PROJECT_DIR=/root/.metricflow
ENV MF_VERBOSE=true
ENV MF_MODEL_PATH=/root/.metricflow/semantic_models
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8080

# Expose MCP server ports
EXPOSE 8080 8081

# Use docker-init.sh as entrypoint
ENTRYPOINT ["./docker-init.sh"]
CMD ["serve"]
