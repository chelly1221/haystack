# Legacy synchronous upload endpoint removed - use /upload-async/ instead
# This module now only provides utility functions for background processing

import logging

def get_upload_router(document_store, embedder):
    from fastapi import APIRouter
    router = APIRouter()
    
    # All upload functionality has been moved to background_upload.py
    # This router is kept for backward compatibility but contains no endpoints
    logging.info("📝 Upload router loaded - all functionality moved to async background processing")
    
    return router
