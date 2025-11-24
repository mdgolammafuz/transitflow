# IntelSent

**API (Render):** https://intelsent.onrender.com/  
**UI (Vercel):** https://intel-sent-bnhbx1kjb-mdgolammafuzs-projects.vercel.app

SEC 10-K retrieval-augmented generation (RAG) service with explicit ingest, chunk, embed, and query stages, exposed through FastAPI and a minimal React/Vite UI. Includes auth (API key), rate limiting, Redis caching, LangSmith tracing hooks, and a Prefect-based "A2A" flow to run the whole pipeline inside Docker.

---

## 1. What this repository contains

- A FastAPI app (`serving/`) that:
  - serves `/query` and `/query_min` for RAG answers,
  - protects endpoints with API keys (`API_KEYS`),
  - rate-limits with SlowAPI + Redis,
  - exposes ingestion helpers for SEC 10-Ks (`/ingest/...`),
  - exports debug endpoints to verify LangSmith env.
- A RAG chain (`rag/`) that loads config from YAML and talks to pgvector.
- A data pipeline (`data/`) to fetch 10-Ks from SEC, chunk them, and embed them into Postgres/pgvector.
- A Prefect flow (`flows/prefect_a2a.py`) to do "ingest → chunk → embed → ask" in one go.
- A minimal React UI (`ui/`) that hits the API with GET and query params (no preflight).
- Docker and docker-compose files for a local stack with API + Redis + pgvector.

---

## 2. Repository layout (current)

Top-level:

```text
.
├── Dockerfile
├── Dockerfile.psycopg
├── Makefile
├── Makefile.a2a
├── README.md
├── artifacts/
├── config/
├── data/
├── db/
├── docker-compose.api.yml
├── docker-compose.redis.yml
├── docker-compose.yml
├── docs/
├── eval/
├── flows/
├── k8s/
├── rag/
├── reports/
├── requirements-api.txt
├── requirements-dev.txt
├── requirements.txt
├── scripts/
├── sec-edgar-filings/
├── serving/
├── tests/
├── ui/
└── utils/
```

UI subfolder (Vite React):

```text
ui/
├── index.html
├── eslint.config.js
├── package.json
├── package-lock.json
├── src/
│   ├── main.tsx
│   ├── App.tsx
│   └── api.ts
├── tsconfig.json
├── tsconfig.app.json
├── tsconfig.node.json
└── vite.config.ts
```

This is enough for a reviewer to navigate quickly.

---

## 3. Runtime endpoints (API)

Base URL (Render): **https://intelsent.onrender.com/**

Key endpoints:

- `/` → service info + config (redacted) + langsmith flags
- `/healthz` → liveness probe
- `/query` (POST) → full query, body JSON
- `/query_min` (GET) → browser-friendly query (no preflight), accepts `api_key` query param
- `/ingest/sec` (POST) → fetch specific ticker(s)/year(s) from SEC and store under `/app/data/...`
- `/ingest/status` (GET) → check if a ticker/year is already ingested
- `/actions/status` and `/actions/ingest` → same idea, but routed under `/actions`
- `/debug/langsmith`, `/debug/ls_imports`, `/debug/ls_emit` → non-secret diagnostics (guarded by API key if set)

All protected endpoints accept:
- `X-API-Key: <key>` header **or**
- `?api_key=<key>` as query

API keys are read from the environment:

```bash
API_KEYS=demo-key-123
```

Multiple keys can be comma-separated.

---

## 4. Configuration files (important)

### 4.1 `serving/settings.py`

- Reads config path from env: `APP_CFG_PATH` or `APP_CONFIG`, falls back to `config/app.yaml`.
- Exposes:
  - `db_dsn`
  - `langsmith_tracing` (from `LANGSMITH_TRACING`)
  - `langsmith_project`
  - `openai_api_key`
- Returns a **redacted** config on `/` so reviewers can see which flags are on.

### 4.2 `config/app.docker.yml`

Used by the API container. Key parts:

```yaml
embedding:
  model_name: sentence-transformers/all-MiniLM-L6-v2
  device: cpu

pgvector:
  enabled: true
  conn: postgresql://intel:intel@pgvector:5432/intelrag
  table: chunks
  text_col: content
```

This tells the chain to talk to the pgvector service defined in docker compose.

### 4.3 `config/app.local.yaml`

Same as docker, but points pgvector at `127.0.0.1:5432` for local runs.

### 4.4 `config/app.render.example.yml`

Template for remote DBs (Neon, etc).

---

## 5. Running locally (Docker)

1. **Export API key** (used by the container app):

```bash
export API_KEYS=demo-key-123
```

2. **Start API + Redis + pgvector**:

```bash
docker compose -f docker-compose.api.yml up -d --build
```

3. **Check health**:

```bash
curl -s http://localhost:8000/healthz
```

4. **Ingest a filing** (AAPL 2023):

```bash
curl -s -X POST http://localhost:8000/ingest/sec   -H 'Content-Type: application/json'   -d '{"companies":["AAPL"],"years":[2023]}'
```

This pulls from SEC, stores under `/app/data/sec-edgar-filings/AAPL/2023/` and appends to `/app/data/sec_catalog/sec_docs.csv`.

5. **Chunk**:

```bash
docker exec -it intelsent-api bash -lc   "python -u data/chunker.py     --catalog /app/data/sec_catalog/sec_docs.csv     --input-root /app/data/sec-edgar-filings     --out /app/data/chunks/sec_chunks.csv"
```

6. **Embed**:

```bash
docker exec -it intelsent-api bash -lc   "python -u data/embedder.py     --chunks /app/data/chunks/sec_chunks.csv     --db-dsn postgresql://intel:intel@intel_pgvector:5432/intelrag     --table chunks --batch-size 128"
```

7. **Query**:

```bash
curl -s "http://localhost:8000/query_min?text=Which%20products%20drove%20revenue%20growth%3F&company=AAPL&year=2023&top_k=3&no_openai=true&api_key=demo-key-123"
```

---

## 6. Prefect "A2A" flow

To run the end-to-end flow (ingest → chunk → embed → ask) from the host:

```bash
export INTELSENT_API=http://localhost:8000
export INTELSENT_KEY=demo-key-123
export INTELSENT_CONTAINER=intelsent-api

make -f Makefile.a2a a2a
```

This script:
- ensures the API is healthy,
- ensures SEC data for `AAPL 2023` (or the ticker/year passed in) is present,
- runs chunker and embedder **inside the API container**,
- calls `/query_min` and prints both answer and the first two contexts.

Optional reset to clear duplicate chunks:

```bash
make -f Makefile.a2a a2a A2A_RESET=1
```

---

## 7. Ingestion for other tickers (TSLA, AMZN, …)

Current design is **explicit**: the API does **not** auto-fetch from SEC on a query miss.

To ingest a new ticker/year:

1. Check status:

```bash
curl -s "http://localhost:8000/ingest/status?company=TSLA&year=2023" | jq .
```

2. If `ingested: false`, ingest:

```bash
curl -s -X POST http://localhost:8000/ingest/sec   -H 'Content-Type: application/json'   -d '{"companies":["TSLA"],"years":[2023]}'
```

3. Run chunker + embedder (two docker execs as above).

4. Query again and the new content should be retrieved.

This is the trade-off: explicit ETL keeps the HTTP path fast and predictable; the cost is two short maintenance commands per new company/year.

---

## 8. Evaluation assets

Located in `eval/`:

- `eval/qa_small.jsonl` – 3 small QAs, all "Which product or service was revenue growth driven by?" for MSFT/AAPL 2022–2023.
- `eval/reports/offline.json` – sample run output.

The sample report shows:
- `n_items: 3`
- `retrieval_hit: 0.33`
- `context_hit_rate: 0.083`
- generation failed because the OpenAI key was not supplied and Docker profile forced `NO_OPENAI=1`. So the answer text is the fallback message.

This is deliberate: the evaluation is in the repo to show how to measure the retrieval path, even when LLM calls are blocked. A reviewer can:
1. set `OPENAI_API_KEY` in the container,
2. re-run the eval script in `scripts/` or `eval/`,
3. and see improved metrics.

---

## 9. Benchmark: Dense vs Hybrid Retrieval

### Methodology
- **Dataset**: 47 questions from Apple, Microsoft, Tesla, Amazon 10-K filings (2021-2024)
- **Evaluation**: Chunk-level retrieval accuracy using gold chunk ID annotations
- **Metrics**: 
  - **hit@k**: Percentage of queries where gold chunk appears in top-k results
  - **MRR**: Mean Reciprocal Rank of first correct chunk (higher = better ranking)

### Results

| Metric | Dense Only | Hybrid (Dense+BM25) | Improvement |
|--------|-----------|---------------------|-------------|
| hit@1 | 0.0% | 26.1% | +26.1pp |
| hit@5 | 0.0% | 32.6% | +32.6pp |
| MRR | 0.000 | 0.290 | +0.290 |
| Latency (p50) | 2.7s | 2.3s | -17% |

### Key Findings

1. **Dense embeddings failed completely** (0% hit rate) - semantic search couldn't match exact gold chunks despite retrieving contextually similar text
2. **BM25 keyword matching essential** - hybrid retrieval found correct chunks in 1 out of 3 queries  
3. **Speed improvement** - hybrid 17% faster due to in-memory BM25 caching vs pure pgvector lookups
4. **Domain-specific advantage** - financial queries contain precise terminology (ticker symbols, fiscal years, specific metrics) that lexical matching captures better than embeddings alone

### Architecture

Hybrid retrieval combines:
- **Dense**: `sentence-transformers/all-MiniLM-L6-v2` embeddings in pgvector
- **BM25**: In-memory keyword index with reciprocal rank fusion (RRF)
- **Fusion**: Weighted combination (50/50) of dense and keyword scores

### Reproduce

```bash
# Dense baseline (default)
python scripts/eval_offline.py \
  --qa eval/qa_gold_v2_with_chunks.jsonl \
  --out eval/reports/dense_baseline.json \
  --api http://localhost:8000

# Hybrid (set USE_HYBRID=1 in docker-compose.api.yml, rebuild)
docker-compose -f docker-compose.api.yml down
# Edit docker-compose.api.yml: uncomment USE_HYBRID: "1"
docker-compose -f docker-compose.api.yml build api
docker-compose -f docker-compose.api.yml up -d

python scripts/eval_offline.py \
  --qa eval/qa_gold_v2_with_chunks.jsonl \
  --out eval/reports/hybrid_baseline.json \
  --api http://localhost:8000
```

**Reports**: `eval/reports/{dense_clean.json, hybrid_final_fixed.json}`

---

## 10. LangSmith tracing

The API is wired to LangSmith and exposes what it sees from the environment.

Environment variables used:

```bash
LANGCHAIN_TRACING_V2=true
LANGCHAIN_API_KEY=<your-langsmith-key>
LANGCHAIN_PROJECT=IntelSent
# optional
LANGCHAIN_ENDPOINT=https://api.smith.langchain.com
```

To confirm from the outside (Render):

```bash
curl -s "https://intelsent.onrender.com/debug/langsmith?api_key=demo-key-123" | jq .
curl -s "https://intelsent.onrender.com/debug/ls_emit?api_key=demo-key-123" | jq .
```

If both show `import_ok: true` and the emit endpoint says `emitted: true`, traces are being sent.

---

## 11. Security and operational notes

- **API keys:** enforced in `serving/api.py` for all meaningful routes. Keys are **not** hardcoded; local demo uses `demo-key-123`, but the flow (`flows/prefect_a2a.py`) now refuses to run if `INTELSENT_KEY` is missing.
- **Rate limiting:** configured with SlowAPI and backed by Redis (`REDIS_URL`), so the API cannot be hammered by the UI.
- **CORS:** restricted to localhost dev origin and the deployed Vercel origin.
- **Data location:** everything the SEC fetcher writes goes to `/app/data/...` (host bind `./data`), so filesystem inspection and cleanup are possible.
- **GDPR/DSVGO:** data processed is US public company filings; no personal data is ingested by design. If any extra data is added later, it should go through the same explicit ingest path.

---

## 12. Deploy targets

**Render API**  
- URL: https://intelsent.onrender.com/  
- Must set:
  - `API_KEYS=demo-key-123`
  - `LANGCHAIN_TRACING_V2=true`
  - `LANGCHAIN_API_KEY=<real-key>`
  - `LANGCHAIN_PROJECT=IntelSent`
  - mount/ship `config/app.render.example.yml` as actual `APP_CONFIG` or keep default

**Vercel UI**  
- URL: https://intel-sent-bnhbx1kjb-mdgolammafuzs-projects.vercel.app  
- Must set:
  - `VITE_API_BASE=https://intelsent.onrender.com`
  - `VITE_API_KEY=demo-key-123`

---

## 13. Future automation (not enabled by default)

The current manual 4-step for a new ticker/year:

1. `/ingest/sec`
2. `data/chunker.py`
3. `data/embedder.py`
4. Query

…can be automated by:
- adding a background worker that listens for "no context" responses and enqueues ingest,
- or extending the API to fire chunk/embed after a successful ingest.
The code is already separated, so this is a small next step.

---

## 14. GDPR / Data Protection Notes

This project processes only public SEC 10-K filings fetched from data.sec.gov. These filings do not contain end-user personal data. The system does not persist user questions beyond normal API/service logs.

What is collected:
- HTTP access logs from FastAPI/uvicorn (timestamp, path, status, request ID).

- Rate-limit source (IP / remote address) via slowapi for abuse protection.

- Optional: LangSmith trace metadata (prompt, inputs, outputs) when `LANGCHAIN_TRACING_V2=true`.

How to run it in a stricter environment:

1. Disable LangSmith by not setting LANGCHAIN_TRACING_V2 or LANGCHAIN_API_KEY.

2. Put the API behind your own gateway and terminate logs there.

3. Rotate API keys and keep API_KEYS= out of the repo.

4. If you ingest internal reports instead of SEC data, you become the data controller — document your own retention and access rules.

This setup is intended for demo/tech-screen use and does not constitute legal advice or a complete GDPR DPIA. For production, add a proper privacy notice, retention policy, and DSR handling.

---

## License

This project is licensed under the **MIT License**.

```text
Copyright (c) 2025 <Your Name>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```
