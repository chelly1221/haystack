from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from util.simple_vector_store import SimpleVectorStore
from util.simple_embedder import SimpleEmbedder

from util.pdf import clean_text_by_fixed_margins, split_pdf_by_pages, split_pdf_by_section_headings
from util.embedding import embed_document_sections, embed_query, cosine_similarity

from api.upload import get_upload_router
from api.query import get_query_router
from api.documents import get_documents_router
from api.statistics import get_statistics_router

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

# Initialize vector store and embedder
vector_store = SimpleVectorStore(
    url="http://qdrant:6333",
    collection_name="documents",
    embedding_dim=1024,
    recreate_collection=False
)

embedder = SimpleEmbedder(model_name="./models/KURE-v1")
embedder.warm_up()

# Create upload directory if it doesn't exist
UPLOAD_DIR = "./uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Register routers
app.include_router(get_upload_router(vector_store, embedder))
app.include_router(get_query_router(vector_store, embedder))
app.include_router(get_documents_router(vector_store))
app.include_router(get_statistics_router(vector_store))