FROM python:3.11-slim

# Install Java (required by H2O) and system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Create non-root user (HF Spaces requirement)
RUN useradd -m -u 1000 user
WORKDIR /home/user/app

# Install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=user:user . .

# Switch to non-root user
USER user

# Expose Dash port
EXPOSE 7860

# Health check
HEALTHCHECK CMD curl --fail http://localhost:7860/ || exit 1

# Run Dash app
CMD ["python", "app.py"]
