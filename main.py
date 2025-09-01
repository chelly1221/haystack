from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore
from haystack.components.embedders import SentenceTransformersDocumentEmbedder

from util.pdf import clean_text_by_fixed_margins, split_pdf_by_pages, split_pdf_by_section_headings
from util.embedding import embed_document_sections, embed_query, cosine_similarity

from api.upload import get_upload_router
from api.query import get_query_router
from api.documents import get_documents_router
from api.statistics import get_statistics_router
from api.background_upload import get_background_upload_router
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

recreate = not os.path.exists("./qdrant_data/index_created.flag")

document_store = QdrantDocumentStore(
    url="http://qdrant:6333",
    index="haystack-index",
    recreate_index=False,
    embedding_dim=1024
)

if recreate:
    os.makedirs("./qdrant_data", exist_ok=True)
    with open("./qdrant_data/index_created.flag", "w") as f:
        f.write("done")
    time.sleep(1.5)  # ✅ Qdrant가 생성 완료될 시간 확보

# Initialize embedder
embedder = SentenceTransformersDocumentEmbedder(model="./models/KURE-v1")
embedder.warm_up()

# Create upload directory if it doesn't exist
UPLOAD_DIR = "./uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Register routers
app.include_router(get_upload_router(document_store, embedder))
app.include_router(get_query_router(document_store, embedder))
app.include_router(get_documents_router(document_store))
app.include_router(get_statistics_router(document_store))
app.include_router(get_background_upload_router(document_store, embedder))
app.include_router(websocket_router)