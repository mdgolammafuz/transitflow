"""
Offline evaluator for IntelSent API with proper chunk ID matching.
Computes:
- exact_match, contains_match, token_f1 (answer quality)
- hit@1/3/5, MRR (retrieval quality using chunk IDs)
- latency metrics
"""
import argparse, json, pathlib, random, re, time
from typing import Any, Dict, List, Optional, Tuple
import requests

API_DEFAULT = "http://127.0.0.1:8000"

_WS = re.compile(r"\s+")
_PUNCT = re.compile(r"[^\w\s]")
_STOPWORDS = {
    "the","a","an","and","or","but","for","nor","beyond","on","in","at","to","from","by",
    "of","with","as","is","are","was","were","be","been","being","that","this",
    "it","its","their","his","her","our","your","my"
}

def normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = _PUNCT.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s

def tokenize(s: str) -> List[str]:
    return [t for t in normalize_text(s).split(" ") if t and t not in _STOPWORDS and len(t) > 1]

def exact_match(pred: str, gold: str) -> int:
    return int(normalize_text(pred) == normalize_text(gold))

def contains_match(pred: str, gold: str) -> int:
    p = normalize_text(pred); g = normalize_text(gold)
    if not p or not g: return 0
    return int(p in g or g in p)

def token_f1(pred: str, gold: str) -> float:
    p = set(tokenize(pred)); g = set(tokenize(gold))
    if not p and not g: return 1.0
    if not p or not g: return 0.0
    tp = len(p & g)
    prec = tp / len(p); rec = tp / len(g)
    return 0.0 if (prec + rec) == 0 else (2 * prec * rec / (prec + rec))

def compute_retrieval_metrics(retrieved_ids: List[int], gold_ids: List[int]) -> Dict[str, float]:
    """
    Compute retrieval metrics based on chunk IDs.
    
    Returns:
        hit@1, hit@3, hit@5, MRR
    """
    if not gold_ids or not retrieved_ids:
        return {"hit@1": 0.0, "hit@3": 0.0, "hit@5": 0.0, "mrr": 0.0}
    
    gold_set = set(gold_ids)
    
    # Find first hit rank (1-indexed)
    first_hit_rank = None
    for rank, ret_id in enumerate(retrieved_ids, start=1):
        if ret_id in gold_set:
            first_hit_rank = rank
            break
    
    # Compute metrics
    hit_1 = 1.0 if first_hit_rank and first_hit_rank <= 1 else 0.0
    hit_3 = 1.0 if first_hit_rank and first_hit_rank <= 3 else 0.0
    hit_5 = 1.0 if first_hit_rank and first_hit_rank <= 5 else 0.0
    mrr = (1.0 / first_hit_rank) if first_hit_rank else 0.0
    
    return {
        "hit@1": hit_1,
        "hit@3": hit_3,
        "hit@5": hit_5,
        "mrr": mrr,
    }

def retrieval_hit(chunks: List[str], gold: str) -> int:
    """Legacy text-based retrieval check."""
    g = normalize_text(gold)
    return 0 if not g else int(any(g in normalize_text(c) for c in chunks))

def context_hit_rate(chunks: List[str], gold: str) -> float:
    """Legacy text-based context hit rate."""
    g = normalize_text(gold)
    if not chunks or not g: return 0.0
    hits = sum(1 for c in chunks if g in normalize_text(c))
    return hits / max(1, len(chunks))

def mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0

def _percentile(values: List[float], q: float) -> float:
    if not values: return 0.0
    xs = sorted(values)
    if len(xs) == 1: return xs[0]
    pos = (q/100.0) * (len(xs) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    frac = pos - lo
    return xs[lo] + (xs[hi] - xs[lo]) * frac

def load_qa(path: str) -> List[Dict[str, Any]]:
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

def query_api(api: str, api_key: Optional[str], q: str, company: Optional[str],
              year: Optional[int], top_k: int, use_openai: bool, timeout: int
             ) -> Tuple[str, List[str], List[int], int]:
    """
    Returns: (answer, contexts, chunk_ids, status_code)
    """
    payload = {"text": q, "company": company, "year": year, "top_k": top_k, "no_openai": not use_openai}
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    
    try:
        resp = requests.post(f"{api}/query", json=payload, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            data = resp.json()
            answer = data.get("answer", "")
            contexts = data.get("contexts", [])
            chunk_ids = data.get("meta", {}).get("chunk_ids", [])
            return answer, contexts, chunk_ids, 200
        else:
            return "", [], [], resp.status_code
    except Exception as e:
        print(f"Error querying API: {e}")
        return "", [], [], 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--qa", required=True, help="QA JSONL file")
    ap.add_argument("--out", required=True, help="Output JSON report")
    ap.add_argument("--api", default=API_DEFAULT)
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--use-openai", action="store_true")
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--shuffle", action="store_true")
    args = ap.parse_args()
    
    qa_items = load_qa(args.qa)
    if args.shuffle:
        random.shuffle(qa_items)
    if args.limit:
        qa_items = qa_items[:args.limit]
    
    print(f"Evaluating {len(qa_items)} questions...")
    
    latencies_all = []
    latencies_success = []
    
    exact_matches = []
    contains_matches = []
    token_f1s = []
    retrieval_hits_legacy = []
    context_hit_rates_legacy = []
    
    # New ID-based metrics
    hit_at_1_scores = []
    hit_at_3_scores = []
    hit_at_5_scores = []
    mrr_scores = []
    
    answer_lens = []
    context_lens = []
    
    failed = 0
    
    for i, item in enumerate(qa_items, start=1):
        question = item.get("question", "")
        gold_answer = item.get("gold_answer") or item.get("ground_truth") or item.get("answer", "")
        company = item.get("company")
        year = item.get("year")
        gold_chunk_ids = item.get("gold_chunk_ids")
        
        t0 = time.perf_counter()
        answer, contexts, chunk_ids, status = query_api(
            args.api, args.api_key, question, company, year, args.top_k, args.use_openai, args.timeout
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        latencies_all.append(elapsed_ms)
        
        if status != 200:
            failed += 1
            print(f"[{i}/{len(qa_items)}] FAILED (status={status})")
            continue
        
        latencies_success.append(elapsed_ms)
        
        # Answer quality metrics
        exact_matches.append(exact_match(answer, gold_answer))
        contains_matches.append(contains_match(answer, gold_answer))
        token_f1s.append(token_f1(answer, gold_answer))
        
        # Legacy text-based retrieval
        retrieval_hits_legacy.append(retrieval_hit(contexts, gold_answer))
        context_hit_rates_legacy.append(context_hit_rate(contexts, gold_answer))
        
        # New ID-based retrieval metrics
        if gold_chunk_ids and chunk_ids:
            metrics = compute_retrieval_metrics(chunk_ids, gold_chunk_ids)
            hit_at_1_scores.append(metrics["hit@1"])
            hit_at_3_scores.append(metrics["hit@3"])
            hit_at_5_scores.append(metrics["hit@5"])
            mrr_scores.append(metrics["mrr"])
        else:
            hit_at_1_scores.append(0.0)
            hit_at_3_scores.append(0.0)
            hit_at_5_scores.append(0.0)
            mrr_scores.append(0.0)
        
        # Length stats
        answer_lens.append(len(tokenize(answer)))
        context_lens.append(mean([len(tokenize(c)) for c in contexts]) if contexts else 0)
        
        if i % 10 == 0:
            print(f"[{i}/{len(qa_items)}] Processed...")
    
    n_success = len(qa_items) - failed
    
    report = {
        "n_items": len(qa_items),
        "failed_requests": failed,
        "request_success_rate": n_success / len(qa_items) if qa_items else 0.0,
        "latency_ms_overall": {
            "mean_ms": mean(latencies_all),
            "p50_ms": _percentile(latencies_all, 50),
            "p95_ms": _percentile(latencies_all, 95),
            "max_ms": max(latencies_all) if latencies_all else 0.0,
        },
        "latency_ms_success_only": {
            "mean_ms": mean(latencies_success),
            "p50_ms": _percentile(latencies_success, 50),
            "p95_ms": _percentile(latencies_success, 95),
            "max_ms": max(latencies_success) if latencies_success else 0.0,
        },
        "quality_success_only": {
            "n_success": n_success,
            "exact_match": mean(exact_matches),
            "contains_match": mean(contains_matches),
            "token_f1": mean(token_f1s),
            "retrieval_hit": mean(retrieval_hits_legacy),
            "context_hit_rate": mean(context_hit_rates_legacy),
            "mrr": mean(mrr_scores),
            "hit@1": mean(hit_at_1_scores),
            "hit@3": mean(hit_at_3_scores),
            "hit@5": mean(hit_at_5_scores),
            "answer_len_tokens_mean": mean(answer_lens),
            "avg_context_len_tokens_mean": mean(context_lens),
        }
    }
    
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2)
    
    print(f"\n✓ Evaluation complete!")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()