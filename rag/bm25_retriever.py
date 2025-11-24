"""
BM25 keyword retriever for IntelSent.
Uses rank_bm25 library (pure Python).
Complements dense vector search with exact keyword matching.
"""

from typing import List, Dict, Any, Optional, Tuple
import psycopg
from rank_bm25 import BM25Okapi
import re

class BM25Retriever:
    """
    BM25 keyword-based retriever using PostgreSQL chunks table.
    Includes in-memory caching to speed up sequential queries.
    """
    
    def __init__(
        self,
        dsn: str,
        table: str = "chunks",
        text_col: str = "text",
    ):
        self.dsn = dsn
        self.table = table
        self.text_col = text_col
        
        # Cache: Key=(company, year), Value=(bm25_object, chunks_list)
        self._cache: Dict[Tuple[Optional[str], Optional[int]], Tuple[BM25Okapi, List[Dict]]] = {}
        
    @staticmethod
    def tokenize(text: str) -> List[str]:
        """Simple tokenization: lowercase + split on non-alphanumeric."""
        text = text.lower()
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        return [t for t in text.split() if len(t) > 2]
    
    def _get_index_from_cache_or_build(
        self,
        company: Optional[str] = None,
        year: Optional[int] = None,
    ) -> Tuple[Optional[BM25Okapi], List[Dict[str, Any]]]:
        """
        Retrieve BM25 index and chunks from cache, or build if missing.
        """
        cache_key = (company, year)
        
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        # --- Build Index ---
        where_parts: List[str] = []
        params: List[Any] = []
        
        if company:
            where_parts.append("company = %s")
            params.append(company)
        if year:
            where_parts.append("year = %s")
            params.append(int(year))
            
        where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        
        sql = f"""
        SELECT id, company, year, {self.text_col} AS chunk
        FROM {self.table}
        {where_sql}
        """
        
        chunks = []
        with psycopg.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                _id, _company, _year, _chunk = row
                chunks.append({
                    "id": _id,
                    "company": _company,
                    "year": _year,
                    "chunk": _chunk or "",
                })
        
        if not chunks:
            return None, []
            
        corpus = [self.tokenize(c["chunk"]) for c in chunks]
        bm25 = BM25Okapi(corpus)
        
        # Store in cache
        self._cache[cache_key] = (bm25, chunks)
        return bm25, chunks
    
    def search(
        self,
        query: str,
        company: Optional[str] = None,
        year: Optional[int] = None,
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Search for query using BM25.
        """
        # Get index from cache or build it
        bm25, chunks = self._get_index_from_cache_or_build(company, year)
        
        if bm25 is None or not chunks:
            return []
        
        # Tokenize query
        query_tokens = self.tokenize(query)
        if not query_tokens:
            return []
        
        # Get BM25 scores
        scores = bm25.get_scores(query_tokens)
        
        # Sort by score descending
        top_indices = sorted(
            range(len(scores)),
            key=lambda i: scores[i],
            reverse=True
        )[:top_k]
        
        scored_chunks = []
        for rank, i in enumerate(top_indices):
            if scores[i] > 0: # Only return non-zero matches
                chunk_data = dict(chunks[i])
                chunk_data.update({
                    "bm25_score": float(scores[i]),
                    "rank": rank,
                })
                scored_chunks.append(chunk_data)
        
        return scored_chunks

if __name__ == "__main__":
    # Quick test
    import os
    dsn = os.getenv("PGVECTOR_DSN", "postgresql://intel:intel@localhost:5432/intelrag")
    retriever = BM25Retriever(dsn=dsn)
    results = retriever.search(query="revenue growth", company="AAPL", year=2023, top_k=3)
    print(f"Found {len(results)} results")
    for r in results:
        print(f"Score: {r['bm25_score']:.4f} | Text: {r['chunk'][:50]}...")