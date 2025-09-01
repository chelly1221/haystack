FROM python:3.10
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    curl \
    libglib2.0-0 \
    ffmpeg \
    libsm6 \
    libxext6 \
    libxrender-dev \
    build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install all required Python packages
RUN pip install --upgrade pip

# Install PyTorch with CUDA support (required for vLLM)
RUN pip install torch==2.1.2 torchvision==0.16.2 torchaudio==2.1.2 --index-url https://download.pytorch.org/whl/cu121

# Install vLLM and dependencies in a more controlled way
# First install numpy with version constraint
RUN pip install "numpy<2.0.0"

# Install vLLM without optional dependencies that cause issues
RUN pip install vllm>=0.6.4.post1 --no-deps

# Install core vLLM dependencies manually
RUN pip install \
    "typing-extensions>=4.8.0" \
    "pydantic>=2.0" \
    pillow \
    "prometheus-client>=0.19.0" \
    py-cpuinfo \
    "ray>=2.9" \
    sentencepiece \
    fastapi \
    "uvicorn[standard]" \
    openai \
    tiktoken \
    xformers==0.0.23.post1 \
    triton==2.1.0 \
    "msgspec>=0.18.5" \
    "aiohttp>=3.9.0"

# Install other required packages
RUN pip install \
    scipy \
    pdfplumber \
    python-docx \
    python-pptx \
    haystack-ai==2.11.0 \
    qdrant-haystack \
    "sentence-transformers>=4.46.0" \
    --upgrade safetensors \
    "huggingface_hub>=0.16.4" \
    git-lfs \
    python-dotenv \
    requests \
    poppler-utils \
    python-multipart \
    transformers \
    PyMuPDF \
    pdf2image \
    "psutil>=5.9.0"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1"]