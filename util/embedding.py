from .simple_document import SimpleDocument
from .simple_embedder import SimpleEmbedder
import numpy as np
import logging

def embed_document_sections(sections, metadata_base, total_pages, embedder):
    embedded_docs = []
    # 문서 제목 추출
    document_title = metadata_base.get("original_filename", "Unknown Document")
    
    # Prepare texts for batch embedding
    texts = []
    docs_metadata = []
    
    for idx, section in enumerate(sections):
        if len(section["content"].strip()) < 1:
            logging.info(f"⏭ Skipping low-content section: {section['title']}")
            continue

        meta = {
            **metadata_base,
            "section_title": section["title"],
            "section_id": section.get("section_id", f"{idx + 1}"),
            "section_number": idx + 1,
            "page_number": section.get("page_number", section.get("start_page")),
            "total_pdf_pages": total_pages
        }

        # 문서 제목과 섹션 제목 모두 포함하여 임베딩
        content_with_header = f"문서: {document_title}\n<h2>{section['title']}</h2>\n{section['content']}"
        texts.append(content_with_header)
        docs_metadata.append(meta)
    
    # Batch embedding for efficiency
    if texts:
        embeddings = embedder.embed_texts(texts)
        
        for text, meta, embedding in zip(texts, docs_metadata, embeddings):
            doc = SimpleDocument(content=text, meta=meta, embedding=embedding)
            embedded_docs.append(doc)
            
            # 로깅 추가 (선택사항)
            section_title = meta.get("section_title", "Unknown Section")
            logging.info(f"✅ Embedded section from '{document_title}' - Section: {section_title}")
    
    return embedded_docs

def embed_query(query_text, embedder):
    embedding = embedder.embed_single(query_text)
    if not embedding or len(embedding) == 0:
        raise ValueError("❌ Embedding failed: query embedding is empty")
    return embedding

def cosine_similarity(a, b):
    a = np.array(a)
    b = np.array(b)
    if a.size == 0 or b.size == 0:
        return 0.0
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))