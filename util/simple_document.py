"""
Simple Document - Replacement for Haystack Document class
Lightweight dataclass for document storage and retrieval
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
import uuid
import logging

@dataclass
class SimpleDocument:
    """Simple document class to replace Haystack Document"""
    
    content: str
    meta: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def __post_init__(self):
        """Ensure consistent metadata structure"""
        if not isinstance(self.meta, dict):
            self.meta = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert document to dictionary for serialization"""
        return {
            "id": self.id,
            "content": self.content,
            "meta": self.meta,
            "embedding": self.embedding
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SimpleDocument':
        """Create document from dictionary"""
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            content=data.get("content", ""),
            meta=data.get("meta", {}),
            embedding=data.get("embedding")
        )
    
    def get_meta(self, key: str, default: Any = None) -> Any:
        """Get metadata value with default"""
        return self.meta.get(key, default)
    
    def set_meta(self, key: str, value: Any):
        """Set metadata value"""
        self.meta[key] = value
    
    def __repr__(self) -> str:
        content_preview = self.content[:50] + "..." if len(self.content) > 50 else self.content
        return f"SimpleDocument(id='{self.id[:8]}...', content='{content_preview}', meta_keys={list(self.meta.keys())})"

def create_document_batch(contents: List[str], metadatas: List[Dict[str, Any]] = None) -> List[SimpleDocument]:
    """
    Create a batch of documents efficiently
    
    Args:
        contents: List of document contents
        metadatas: Optional list of metadata dicts (same length as contents)
        
    Returns:
        List of SimpleDocument instances
    """
    if metadatas is None:
        metadatas = [{}] * len(contents)
    
    if len(contents) != len(metadatas):
        raise ValueError("Contents and metadatas must have the same length")
    
    return [
        SimpleDocument(content=content, meta=meta)
        for content, meta in zip(contents, metadatas)
    ]