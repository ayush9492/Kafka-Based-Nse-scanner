FROM python:3.11-slim

# 1. Install build tools + compile TA-Lib C library from source
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential wget && \
    wget -q http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz && \
    ldconfig

WORKDIR /app

# 2. Install Python deps (build-essential still present for TA-Lib wheel compilation)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Clean up build tools to shrink image
RUN apt-get purge -y build-essential wget && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# 4. Copy app files
COPY app.py .
COPY index.html .

# Railway injects PORT env variable
ENV PORT=8000
EXPOSE 8000

CMD uvicorn app:app --host 0.0.0.0 --port $PORT