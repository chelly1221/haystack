from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

from util.pdf import clean_text_by_fixed_margins, split_pdf_by_pages, split_pdf_by_section_headings
from util.embedding import embed_document_sections, embed_query, cosine_similarity

from api.upload import get_upload_router
from api.query import get_query_router
from api.documents import get_documents_router
from api.statistics import get_statistics_router
from api.websocket_handler import router as websocket_router

import os
import logging
import time
logging.basicConfig(level=logging.INFO)

# Initialize FastAPI
app = FastAPI()
@app.get("/")
async def root():
    return {"status": "ok"}

# Ensure image directory exists
os.makedirs("./image_store", exist_ok=True)
app.mount("/images", StaticFiles(directory="./image_store"), name="images")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://kac.chelly.kr"],  # You can restrict this to your frontend IP/domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Qdrant client
qdrant_client = QdrantClient(url="http://qdrant:6333")

# Initialize embedder
embedder = SentenceTransformer("./models/KURE-v1")

# Create upload directory if it doesn't exist
UPLOAD_DIR = "./uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Register routers
app.include_router(get_upload_router(qdrant_client, embedder))
app.include_router(get_query_router(qdrant_client, embedder))
app.include_router(get_documents_router(qdrant_client))
app.include_router(get_statistics_router(qdrant_client))
app.include_router(websocket_router)