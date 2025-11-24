"""
Hybrid retriever: Dense (pgvector) + BM25 with Reciprocal Rank Fusion.
"""

from typing import List, Dict, Any, Optional
from collections import defaultdict

class HybridRetriever:
    """
    Combines dense vector search and BM25 keyword search using RRF.
    """
    
    def __init__(
        self,
        dense_retriever: Any,  # PGVectorRetriever
        bm25_retriever: Any,   # BM25Retriever
        k: int = 60,           # RRF constant
        dense_weight: float = 0.5,
    ):
        self.dense = dense_retriever
        self.bm25 = bm25_retriever
        self.k = k
        self.dense_weight = dense_weight
        self.bm25_weight = 1.0 - dense_weight
    
    def _rrf_score(self, rank: int) -> float:
        """Reciprocal Rank Fusion score for a given rank (1-indexed)."""
        return 1.0 / (self.k + rank)
    
    def search(
        self,
        query: str,
        company: Optional[str] = None,
        year: Optional[int] = None,
        top_k: int = 5,
        retrieve_k: int = 20,  # Retrieve more from each, then fuse
    ) -> List[Dict[str, Any]]:
        """
        Hybrid search: dense + BM25 with RRF fusion.
        """
        # 1. Get results from both retrievers
        # Note: We fetch 'retrieve_k' (e.g. 20) candidates from each side
        dense_results = self.dense.get_relevant(
            question=query,
            company=company,
            year=year,
            top_k=retrieve_k,
        )
        
        bm25_results = self.bm25.search(
            query=query,
            company=company,
            year=year,
            top_k=retrieve_k,
        )
        
        # 2. Build RRF scores
        # Key: chunk_id, Value: {chunk_data, rrf_score}
        chunk_scores: Dict[int, Dict[str, Any]] = defaultdict(
            lambda: {"rrf_score": 0.0, "dense_rank": None, "bm25_rank": None, "chunk_data": {}}
        )
        
        # Score Dense Results
        for rank, result in enumerate(dense_results, start=1):
            chunk_id = result["id"]
            chunk_scores[chunk_id]["chunk_data"] = result
            chunk_scores[chunk_id]["dense_rank"] = rank
            chunk_scores[chunk_id]["rrf_score"] += (
                self.dense_weight * self._rrf_score(rank)
            )
        
        # Score BM25 Results
        for rank, result in enumerate(bm25_results, start=1):
            chunk_id = result["id"]
            # If not already present from dense, add data
            if not chunk_scores[chunk_id]["chunk_data"]:
                chunk_scores[chunk_id]["chunk_data"] = result
            
            chunk_scores[chunk_id]["bm25_rank"] = rank
            chunk_scores[chunk_id]["rrf_score"] += (
                self.bm25_weight * self._rrf_score(rank)
            )
        
        # 3. Sort and Format
        sorted_results = sorted(
            chunk_scores.items(),
            key=lambda x: x[1]["rrf_score"],
            reverse=True
        )[:top_k]
        
        final_results = []
        for chunk_id, data in sorted_results:
            chunk = dict(data["chunk_data"])
            chunk.update({
                "hybrid_score": data["rrf_score"],
                "dense_rank": data["dense_rank"],
                "bm25_rank": data["bm25_rank"],
                "retrieval_method": "hybrid",
            })
            final_results.append(chunk)
        
        return final_results