
"""
Find gold chunk IDs for QA dataset.
Simple batch mode - auto-finds chunks containing ground_truth answers.
"""

import argparse
import json
import os
import re
import psycopg


def normalize(text):
    """Simple normalization for matching."""
    return re.sub(r'[^\w\s]', ' ', text.lower()).strip()


def find_chunks(dsn, table, ground_truth, company, year):
    """Find chunks containing the ground truth answer."""
    gt_norm = normalize(ground_truth)
    
    where = []
    params = []
    
    if company:
        where.append("company = %s")
        params.append(company)
    if year:
        where.append("year = %s")
        params.append(int(year))
    
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    
    sql = f"SELECT id, text FROM {table} {where_sql}"
    
    matches = []
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        for row in cur.fetchall():
            chunk_id, text = row
            if not text:
                continue
            
            # Exact match (case-insensitive)
            if ground_truth.lower() in text.lower():
                matches.append((chunk_id, 100))
            # Normalized match
            elif gt_norm in normalize(text):
                matches.append((chunk_id, 50))
    
    # Sort by score, return top 3
    matches.sort(key=lambda x: x[1], reverse=True)
    return [m[0] for m in matches[:3]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--qa", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--conn", default=os.getenv("PGVECTOR_DSN", "postgresql://intel:intel@localhost:5432/intelrag"))
    ap.add_argument("--table", default="chunks")
    ap.add_argument("--batch", action="store_true")
    args = ap.parse_args()
    
    # Load QA
    qa_items = []
    with open(args.qa) as f:
        for line in f:
            if line.strip():
                qa_items.append(json.loads(line))
    
    print(f"Processing {len(qa_items)} QAs...")
    
    found = 0
    for qa in qa_items:
        ground_truth = qa.get("gold_answer") or qa.get("ground_truth", "")
        company = qa.get("company")
        year = qa.get("year")
        
        if not ground_truth:
            qa["gold_chunk_ids"] = None
            continue
        
        chunk_ids = find_chunks(args.conn, args.table, ground_truth, company, year)
        
        if chunk_ids:
            qa["gold_chunk_ids"] = chunk_ids
            found += 1
        else:
            qa["gold_chunk_ids"] = None
    
    # Save
    with open(args.out, "w") as f:
        for qa in qa_items:
            f.write(json.dumps(qa) + "\n")
    
    print(f"\n✓ Found chunks for {found}/{len(qa_items)} QAs")
    print(f"✓ Saved to {args.out}")


if __name__ == "__main__":
    main()