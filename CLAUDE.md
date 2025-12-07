# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a document processing and RAG (Retrieval-Augmented Generation) system built with FastAPI and vLLM. The system processes various document formats (PDF, DOCX, PPTX, HWPX) and provides semantic search and AI-powered question-answering capabilities using Korean language models.

## Core Architecture

### Main Components

- **main.py**: FastAPI application entry point with CORS, static file serving, and router registration
- **api/**: FastAPI routers for different endpoints
  - `query.py`: Streaming query endpoint with document filtering and permission checks
  - `upload.py`: Document upload and processing 
  - `documents.py`: Document management operations
  - `statistics.py`: Usage statistics and analytics
- **util/**: Core utilities and data structures
  - `simple_vector_store.py`: Direct Qdrant client wrapper (Haystack-free implementation)
  - `simple_embedder.py`: Korean text embedding using KURE-v1 model
  - `simple_document.py`: Document data structure
  - `embedding.py`: Embedding utilities and similarity functions
  - `pdf/`: PDF processing utilities (text extraction, table processing, image extraction)
- **llama_server_generator.py**: vLLM server client for streaming text generation

### Technology Stack

- **FastAPI**: Web framework with async support
- **Qdrant**: Vector database for document embeddings
- **vLLM**: LLM inference server with OpenAI-compatible API
- **Docker**: Containerization with GPU support
- **Korean NLP**: KURE-v1 embeddings, A.X-3.1-Light language model

### Document Processing Pipeline

1. Document upload → Format detection (PDF/DOCX/PPTX/HWPX)
2. Text extraction → Section splitting → Table/image processing
3. Embedding generation → Vector storage in Qdrant
4. Query processing → Semantic search → Context retrieval → LLM generation

### Permission System

Documents have metadata-based access control with `sosok` (organization) and `site` (location) fields. The system supports:
- Admin access (sosok="관리자", site="관리자")
- Organization-level filtering
- Department-wide access (site ending with "_전체")
- Exact site matching

## Development Commands

### Running the Application

```bash
# Start all services (development)
docker-compose up

# Start specific services
docker-compose up qdrant vllm-server
python main.py  # For local development

# Production deployment
docker-compose up -d
```

### Service Endpoints

- **Main API**: http://localhost:8001
- **Qdrant**: http://localhost:6333  
- **vLLM Server**: http://localhost:8080

### Environment Configuration

Key environment variables (see `.env`):
- `HAYSTACK_ENV=production`
- `VLLM_API_BASE=http://192.168.10.101:8080`
- `NVIDIA_VISIBLE_DEVICES=1`

## Important Implementation Notes

### Vector Store Architecture

The codebase uses a custom `SimpleVectorStore` that directly interfaces with Qdrant, bypassing Haystack dependencies. This provides:
- Direct control over vector operations
- Simplified document metadata handling
- Better performance for Korean text processing

### Korean Language Support

- Embeddings: KURE-v1 model (./models/KURE-v1)
- LLM: A.X-3.1-Light model (./models/A.X-3.1-Light) 
- Optimized for Korean document processing and QA

### Document Metadata Structure

Documents include metadata fields:
- `original_filename`: Source file name
- `page_number`: Page location in source document
- `section_title`, `section_id`: Content organization
- `sosok`, `site`: Access control fields
- `tags`: Document categorization
- `total_pdf_pages`: Document length

### GPU Requirements

The system requires NVIDIA GPUs for:
- vLLM inference server (typically GPU 0)
- Main application with embedding generation (typically GPU 1)
- Configure via `NVIDIA_VISIBLE_DEVICES` environment variable

### Architecture Guidelines

Always consider maintaining clean microservice architecture with clear separation of concerns between document processing, vector operations, and LLM inference components.

## File Processing Capabilities

- **PDF**: Text extraction, table detection, image extraction, page-by-page processing
- **DOCX**: Full document structure preservation
- **PPTX**: Slide content and embedded media
- **HWPX**: Korean document format support

## API Usage Patterns

### Query Streaming
```python
# Streaming query with filters
GET /query-stream/?user_query=<query>&sosok=<org>&site=<location>&tags=<tags>&top_n=4
```

### Document Management
```python
# Upload documents
POST /upload/
# Get document list
GET /documents/
# Delete documents
DELETE /documents/{doc_id}
```

The system is designed for production Korean document processing workloads with enterprise-grade access controls and GPU-accelerated inference.