from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore

# Import embedder with error handling for diagnostics
try:
    from haystack.components.embedders import SentenceTransformersDocumentEmbedder
except ImportError as e:
    import sys
    print(f"❌ Failed to import SentenceTransformersDocumentEmbedder: {e}", file=sys.stderr)
    print(f"🔍 Python version: {sys.version}", file=sys.stderr)
    try:
        import haystack
        print(f"📦 Haystack version: {haystack.__version__}", file=sys.stderr)
    except:
        print("❌ Haystack not properly installed", file=sys.stderr)
    try:
        import sentence_transformers
        print(f"📦 Sentence-transformers version: {sentence_transformers.__version__}", file=sys.stderr)
    except:
        print("❌ Sentence-transformers not properly installed", file=sys.stderr)
    raise

from util.pdf import clean_text_by_fixed_margins, split_pdf_by_pages, split_pdf_by_section_headings
from util.embedding import embed_document_sections, embed_query, cosine_similarity

from api.upload import get_upload_router
from api.query import get_query_router
from api.documents import get_documents_router
from api.statistics import get_statistics_router
from api.background_upload import get_background_upload_router
from api.processor_tasks import get_processor_task_router
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
app.include_router(get_processor_task_router())
app.include_router(websocket_router)

# Initialize background file processor
from background_processor import start_background_processor

# Import file processor components
from file_processor_service import StandaloneFileProcessor
import asyncio
import threading

# Global file processor instance
file_processor = None

@app.on_event("startup")
async def startup_event():
    """애플리케이션 시작 시 백그라운드 프로세서 시작"""
    global file_processor
    
    # Start original background processor
    await start_background_processor(document_store, embedder)
    
    # Start integrated file processor in background thread
    def start_file_processor():
        file_processor = StandaloneFileProcessor()
        file_processor.start_watching()
    
    file_processor_thread = threading.Thread(target=start_file_processor, daemon=True)
    file_processor_thread.start()
    
    logging.info("🚀 Application startup complete - background processor and file processor running")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean shutdown of file processor"""
    global file_processor
    if file_processor and hasattr(file_processor, 'observer'):
        file_processor.observer.stop()
        file_processor.observer.join()
    logging.info("🛑 File processor shutdown complete")