"""
FlashRank reranker for IntelSent.
Optimized for CPU.
"""

from typing import List, Dict, Any

# Robust import check
try:
    from flashrank import Ranker, RerankRequest
    FLASHRANK_AVAILABLE = True
except ImportError:
    FLASHRANK_AVAILABLE = False


class FlashRankReranker:
    """
    Rerank retrieved chunks using FlashRank (ONNX-based Cross-Encoder).
    """
    
    def __init__(
        self,
        model_name: str = "ms-marco-MiniLM-L-12-v2",
        cache_dir: str = "./models/flashrank",
    ):
        if not FLASHRANK_AVAILABLE:
            raise ImportError("flashrank not installed. Run: pip install flashrank")
        
        self.ranker = Ranker(
            model_name=model_name,
            cache_dir=cache_dir,
        )
    
    def rerank(
        self,
        query: str,
        chunks: List[Dict[str, Any]],
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Rerank chunks by relevance to query.
        """
        if not chunks:
            return []
        
        # FlashRank expects: [{"text": "...", "meta": {...}}, ...]
        passages = [
            {
                "text": chunk.get("chunk", "") or chunk.get("text", ""),
                "meta": {
                    "id": chunk.get("id"),
                    "company": chunk.get("company"),
                    "year": chunk.get("year"),
                    # Pass through original data to recover it later
                    "_original": chunk 
                }
            }
            for chunk in chunks
        ]
        
        rerank_request = RerankRequest(query=query, passages=passages)
        reranked = self.ranker.rerank(rerank_request)
        
        # Format results
        results = []
        for rank, result in enumerate(reranked[:top_k], start=1):
            # Recover original chunk data
            original = result["meta"].pop("_original")
            original.update({
                "rerank_score": result["score"],
                "rerank_rank": rank,
                "retrieval_method": original.get("retrieval_method", "dense") + "+rerank",
            })
            results.append(original)
        
        return results


class NoOpReranker:
    """Passthrough reranker (does nothing) if FlashRank is missing/disabled."""
    def rerank(self, query: str, chunks: List[Dict[str, Any]], top_k: int = 5) -> List[Dict[str, Any]]:
        return chunks[:top_k]


def get_reranker(use_flashrank: bool = True, **kwargs):
    """Factory to return correct reranker instance."""
    if use_flashrank and FLASHRANK_AVAILABLE:
        return FlashRankReranker(**kwargs)
    return NoOpReranker()