# scripts/generate_gold_qa.py
import os
import json
import argparse
import random
import time
import psycopg
from pathlib import Path
from typing import List, Dict, Any
import yaml
import google.generativeai as genai

# --- Config Loader (Reused from your rag/chain.py logic) ---
def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

def _get(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def load_db_dsn(cfg_path: str) -> str:
    if not os.path.exists(cfg_path):
        return "postgresql://intel:intel@localhost:5432/intelrag"
    cfg = _load_yaml(cfg_path)
    return (
        _get(cfg, "pgvector.conn")
        or _get(cfg, "db.conn_str")
        or _get(cfg, "pgvector.dsn")
        or "postgresql://intel:intel@localhost:5432/intelrag"
    )

# --- Gemini Generator ---
def generate_qa_with_gemini(model, chunk_text: str) -> Dict[str, str]:
    """
    Uses Gemini 1.5 Flash to generate a QA pair from text.
    """
    prompt = f"""
    You are an expert financial analyst creating a test dataset.
    Read the following text from an SEC 10-K filing.
    
    Task:
    1. Generate ONE specific, difficult question that can be answered ONLY using this text.
    2. Provide the exact answer based strictly on the text.
    3. If the text is boilerplate (table of contents, signatures, empty), return NULL.

    TEXT:
    {chunk_text[:4000]}

    OUTPUT FORMAT (Strict JSON):
    {{
        "question": "...",
        "answer": "..."
    }}
    """
    try:
        response = model.generate_content(prompt)
        # Clean up potential markdown fencing
        text = response.text.replace("```json", "").replace("```", "").strip()
        if "NULL" in text:
            return None
        return json.loads(text)
    except Exception as e:
        print(f" [!] Error generating QA: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Generate Synthetic Gold QA Dataset using Gemini")
    parser.add_argument("--config", default="config/app.local.yaml", help="Path to app config")
    parser.add_argument("--out", default="eval/qa_gold_v2.jsonl", help="Output file path")
    parser.add_argument("--limit", type=int, default=100, help="Total QAs to generate")
    parser.add_argument("--min-length", type=int, default=300, help="Min chunk length chars")
    args = parser.parse_args()

    # 1. Setup Gemini
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("Error: GOOGLE_API_KEY env var not set.")
        return
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-flash') # Fast & Free

    # 2. Connect to DB & Sample Chunks
    dsn = load_db_dsn(args.config)
    print(f"Connecting to DB...")
    
    rows = []
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                # Get distribution across companies
                companies = ['AAPL', 'MSFT', 'TSLA'] # Ensure TSLA is ingested first!
                per_company = args.limit // len(companies) + 5
                
                for comp in companies:
                    query = """
                        SELECT id, chunk_id, company, year, text 
                        FROM chunks 
                        WHERE length(text) > %s AND company = %s
                        ORDER BY RANDOM() 
                        LIMIT %s
                    """
                    cur.execute(query, (args.min_length, comp, per_company))
                    fetched = cur.fetchall()
                    rows.extend(fetched)
                    print(f" -> Fetched {len(fetched)} chunks for {comp}")
    except Exception as e:
        print(f"DB Error: {e}")
        return

    random.shuffle(rows)
    rows = rows[:args.limit] # Trim to exact limit
    print(f"Starting generation for {len(rows)} chunks using Gemini...")

    # 3. Generate
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with open(out_path, "w") as f:
        for row in rows:
            db_id, chunk_id, company, year, text = row
            
            # Rate limit safety (Gemini free tier is generous but good practice)
            time.sleep(1.0) 
            
            qa = generate_qa_with_gemini(model, text)
            if qa:
                record = {
                    "question": qa["question"],
                    "gold_answer": qa["answer"],
                    "gold_chunk_id": chunk_id, # CRITICAL for retrieval metrics
                    "company": company,
                    "year": year,
                    "source_text": text[:200] + "..."
                }
                f.write(json.dumps(record) + "\n")
                count += 1
                print(f" [{count}/{args.limit}] Generated for {company} {year}")

    print(f"Done. {count} QAs written to {args.out}")

if __name__ == "__main__":
    main()