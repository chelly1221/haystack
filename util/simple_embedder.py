"""
Simple Embedder - Direct sentence-transformers implementation
Replaces Haystack SentenceTransformersDocumentEmbedder with direct calls
"""

import logging
from typing import List, Union
import numpy as np
from sentence_transformers import SentenceTransformer

class SimpleEmbedder:
    """Direct sentence-transformers embedder without Haystack wrapper"""
    
    def __init__(self, model_name: str = "./models/KURE-v1"):
        """
        Initialize the embedder with a sentence-transformers model
        
        Args:
            model_name: Path to model or HuggingFace model name
        """
        self.model_name = model_name
        self.model = None
        logging.info(f"📦 Initializing SimpleEmbedder with model: {model_name}")
    
    def warm_up(self):
        """Load the model into memory"""
        try:
            self.model = SentenceTransformer(self.model_name)
            logging.info(f"✅ Model {self.model_name} loaded successfully")
        except Exception as e:
            logging.error(f"❌ Failed to load model {self.model_name}: {e}")
            raise
    
    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """
        Embed a list of texts
        
        Args:
            texts: List of strings to embed
            
        Returns:
            List of embeddings (each embedding is a list of floats)
        """
        if self.model is None:
            raise ValueError("Model not loaded. Call warm_up() first.")
        
        if not texts:
            return []
        
        try:
            embeddings = self.model.encode(texts, convert_to_tensor=False)
            # Convert numpy arrays to lists for JSON serialization
            return [emb.tolist() if isinstance(emb, np.ndarray) else emb for emb in embeddings]
        except Exception as e:
            logging.error(f"❌ Embedding failed: {e}")
            raise
    
    def embed_single(self, text: str) -> List[float]:
        """
        Embed a single text
        
        Args:
            text: String to embed
            
        Returns:
            Single embedding as list of floats
        """
        embeddings = self.embed_texts([text])
        return embeddings[0] if embeddings else []
    
    def get_embedding_dim(self) -> int:
        """Get the dimension of embeddings produced by this model"""
        if self.model is None:
            raise ValueError("Model not loaded. Call warm_up() first.")
        return self.model.get_sentence_embedding_dimension()