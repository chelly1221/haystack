"""
Simple Vector Store - Direct Qdrant client implementation
Replaces Haystack QdrantDocumentStore with direct qdrant-client calls
"""

import logging
from typing import List, Dict, Any, Optional, Union
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, Range, MatchValue
from qdrant_client.http import models
import uuid

from .simple_document import SimpleDocument

class SimpleVectorStore:
    """Direct Qdrant client wrapper without Haystack dependencies"""
    
    def __init__(
        self, 
        url: str = "http://qdrant:6333",
        collection_name: str = "documents",
        embedding_dim: int = 1024,
        recreate_collection: bool = False
    ):
        """
        Initialize the vector store
        
        Args:
            url: Qdrant server URL
            collection_name: Name of the collection
            embedding_dim: Dimension of embeddings
            recreate_collection: Whether to recreate the collection
        """
        self.url = url
        self.collection_name = collection_name
        self.embedding_dim = embedding_dim
        self.client = None
        
        logging.info(f"📦 Initializing SimpleVectorStore: {url}/{collection_name}")
        
        # Initialize client
        self._connect()
        
        # Setup collection
        if recreate_collection:
            self._recreate_collection()
        else:
            self._ensure_collection_exists()
    
    def _connect(self):
        """Connect to Qdrant server"""
        try:
            self.client = QdrantClient(url=self.url)
            logging.info(f"✅ Connected to Qdrant at {self.url}")
        except Exception as e:
            logging.error(f"❌ Failed to connect to Qdrant: {e}")
            raise
    
    def _ensure_collection_exists(self):
        """Create collection if it doesn't exist"""
        try:
            collections = self.client.get_collections().collections
            collection_names = [col.name for col in collections]
            
            if self.collection_name not in collection_names:
                logging.info(f"🔧 Creating collection: {self.collection_name}")
                self._create_collection()
            else:
                logging.info(f"✅ Collection {self.collection_name} already exists")
        except Exception as e:
            logging.error(f"❌ Error checking collection: {e}")
            raise
    
    def _create_collection(self):
        """Create a new collection"""
        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(
                size=self.embedding_dim,
                distance=Distance.COSINE
            )
        )
        logging.info(f"✅ Created collection: {self.collection_name}")
    
    def _recreate_collection(self):
        """Delete and recreate collection"""
        try:
            self.client.delete_collection(self.collection_name)
            logging.info(f"🗑️ Deleted existing collection: {self.collection_name}")
        except:
            pass  # Collection might not exist
        
        self._create_collection()
    
    def write_documents(self, documents: List[SimpleDocument]):
        """
        Write documents to the vector store
        
        Args:
            documents: List of SimpleDocument instances with embeddings
        """
        if not documents:
            logging.warning("No documents to write")
            return
        
        points = []
        for doc in documents:
            if doc.embedding is None:
                logging.warning(f"Skipping document {doc.id} - no embedding")
                continue
            
            point = PointStruct(
                id=str(uuid.uuid4()),  # Use random UUID as point ID
                vector=doc.embedding,
                payload={
                    "doc_id": doc.id,  # Store original document ID in payload
                    "content": doc.content,
                    **doc.meta
                }
            )
            points.append(point)
        
        if points:
            self.client.upsert(
                collection_name=self.collection_name,
                points=points
            )
            logging.info(f"✅ Wrote {len(points)} documents to vector store")
    
    def search_similar(
        self, 
        query_embedding: List[float], 
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[SimpleDocument]:
        """
        Search for similar documents
        
        Args:
            query_embedding: Query vector
            limit: Number of results to return
            filters: Optional filters to apply
            
        Returns:
            List of similar documents with similarity scores in metadata
        """
        # Build filter conditions
        filter_conditions = None
        if filters:
            filter_conditions = self._build_filter(filters)
        
        try:
            results = self.client.search(
                collection_name=self.collection_name,
                query_vector=query_embedding,
                limit=limit,
                query_filter=filter_conditions
            )
            
            documents = []
            for result in results:
                # Extract document data from payload
                payload = result.payload
                doc_id = payload.pop("doc_id", str(uuid.uuid4()))
                content = payload.pop("content", "")
                
                # Create document with similarity score in metadata
                doc = SimpleDocument(
                    id=doc_id,
                    content=content,
                    meta={**payload, "similarity_score": result.score}
                )
                documents.append(doc)
            
            logging.info(f"🔍 Found {len(documents)} similar documents")
            return documents
            
        except Exception as e:
            logging.error(f"❌ Search failed: {e}")
            raise
    
    def filter_documents(self, filters: Optional[Dict[str, Any]] = None) -> List[SimpleDocument]:
        """
        Filter documents by metadata
        
        Args:
            filters: Dictionary of filters to apply
            
        Returns:
            List of matching documents
        """
        filter_conditions = None
        if filters:
            filter_conditions = self._build_filter(filters)
        
        try:
            # Use scroll to get all matching documents
            results, _ = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=filter_conditions,
                with_payload=True,
                with_vectors=False,
                limit=10000  # Large limit to get all results
            )
            
            documents = []
            for result in results:
                payload = result.payload
                doc_id = payload.pop("doc_id", str(uuid.uuid4()))
                content = payload.pop("content", "")
                
                doc = SimpleDocument(
                    id=doc_id,
                    content=content,
                    meta=payload
                )
                documents.append(doc)
            
            logging.info(f"📋 Filtered {len(documents)} documents")
            return documents
            
        except Exception as e:
            logging.error(f"❌ Filter failed: {e}")
            raise
    
    def delete_documents(self, document_ids: List[str]):
        """
        Delete documents by their IDs
        
        Args:
            document_ids: List of document IDs to delete
        """
        if not document_ids:
            return
        
        try:
            # Delete by filtering on doc_id field
            filter_condition = Filter(
                should=[
                    FieldCondition(
                        key="doc_id",
                        match=MatchValue(value=doc_id)
                    ) for doc_id in document_ids
                ]
            )
            
            self.client.delete(
                collection_name=self.collection_name,
                points_selector=models.FilterSelector(filter=filter_condition)
            )
            
            logging.info(f"🗑️ Deleted {len(document_ids)} documents")
            
        except Exception as e:
            logging.error(f"❌ Delete failed: {e}")
            raise
    
    def _build_filter(self, filters: Dict[str, Any]) -> Filter:
        """Build Qdrant filter from dictionary"""
        conditions = []
        
        for key, value in filters.items():
            if isinstance(value, str):
                conditions.append(
                    FieldCondition(key=key, match=MatchValue(value=value))
                )
            elif isinstance(value, (int, float)):
                conditions.append(
                    FieldCondition(key=key, match=MatchValue(value=value))
                )
            elif isinstance(value, list):
                # Multiple possible values
                conditions.append(
                    FieldCondition(key=key, match=MatchValue(value=value[0]))  # Simplified
                )
        
        return Filter(must=conditions) if conditions else None
    
    def get_collection_info(self) -> Dict[str, Any]:
        """Get information about the collection"""
        try:
            info = self.client.get_collection(self.collection_name)
            return {
                "name": info.config.params.vectors.size,
                "vectors_count": info.vectors_count,
                "points_count": info.points_count,
                "status": info.status
            }
        except Exception as e:
            logging.error(f"❌ Failed to get collection info: {e}")
            return {}

# Alias for compatibility
index = "haystack-index"  # For backward compatibility