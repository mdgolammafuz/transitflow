# rag/chain.py
from __future__ import annotations

import os
import json
from typing import Any, Dict, List, Optional, Tuple
from urllib import request as urlrequest, error as urlerror

import yaml
import numpy as np
import psycopg
from sentence_transformers import SentenceTransformer

# ---------- optional flan-T5 tiny-LLM ----------
try:
    from rag.generator import generate_answer as t5_generate_answer  # type: ignore

    _HAS_T5 = True
    _T5_NAME = "flan-t5-small"
except Exception:
    _HAS_T5 = False
    _T5_NAME = "none"


# ---------- tiny config helpers ----------
def _get(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def _env_has_openai() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY"))


def _env_force_no_openai() -> bool:
    return os.environ.get("NO_OPENAI", "").lower() in {"1", "true", "yes"}


def _env_use_ollama() -> bool:
    return os.environ.get("USE_OLLAMA", "").lower() in {"1", "true", "yes"}


def load_chain(cfg_path: str) -> "_SimpleChain":
    """
    Entry point used by serving/api.py.

    - Builds a PGVectorRetriever wired to the `chunks` table.
    - Creates a _SimpleChain that:
        * retrieves top_k chunks from pgvector
        * returns those chunks as `contexts`
        * can optionally call a local LLM (Ollama) to summarize
        * else falls back to a tiny flan-T5 generator (HF pipeline) if available
    """
    cfg = _load_yaml(cfg_path)

    # Embedding config
    embed_model = _get(
        cfg,
        "embedding.model_name",
        "sentence-transformers/all-MiniLM-L6-v2",
    )
    embed_device = _get(cfg, "embedding.device", "cpu")

    # pgvector DSN resolution
    pg_dsn = (
        _get(cfg, "pgvector.conn")
        or _get(cfg, "db.conn_str")
        or _get(cfg, "pgvector.dsn")
        or "postgresql://intel:intel@pgvector:5432/intelrag"
    )
    table = _get(cfg, "pgvector.table", "chunks")
    text_col_pref = _get(
        cfg,
        "pgvector.text_col",
        _get(cfg, "pgvector.text_column", None),
    )

    # generation
    max_context_chars = int(_get(cfg, "generation.max_context_chars", 6000))

    # retrieval extras
    rerank_keep = int(_get(cfg, "retrieval.rerank_keep", 5))

    # LLM toggles
    use_openai = _env_has_openai() and not _env_force_no_openai()
    use_ollama = _env_use_ollama()

    ollama_base_url = os.environ.get("OLLAMA_BASE_URL", "http://ollama:11434")
    ollama_model = os.environ.get("OLLAMA_MODEL", "llama3.2:1b")

    retriever = PGVectorRetriever(
        dsn=pg_dsn,
        table=table,
        text_col_pref=text_col_pref,
        embed_model=embed_model,
        embed_device=embed_device,
    )

    chain = _SimpleChain(
        retriever=retriever,
        max_context_chars=max_context_chars,
        rerank_keep=rerank_keep,
        use_openai=use_openai,
        use_ollama=use_ollama,
        ollama_base_url=ollama_base_url,
        ollama_model=ollama_model,
        use_flan_t5=_HAS_T5,
    )
    return chain


# ---------- pgvector retriever ----------
class PGVectorRetriever:
    """
    Minimal pgvector-backed retriever.

    Expects a table with columns:
      - id (PK)
      - company
      - year
      - embedding (vector)
      - chunk/text/content/... (one of the candidate text cols)
    """

    _CANDIDATE_TEXT_COLS = ["chunk", "content", "text", "passage", "body"]

    def __init__(
        self,
        dsn: str,
        table: str,
        text_col_pref: Optional[str],
        embed_model: str,
        embed_device: str = "cpu",
    ) -> None:
        self.dsn = dsn
        self.table = table
        self._embedder = SentenceTransformer(embed_model, device=embed_device)
        self.text_col = self._resolve_text_column(text_col_pref)

    def _resolve_text_column(self, preferred: Optional[str]) -> str:
        with psycopg.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position;
                """,
                (self.table,),
            )
            cols = [r[0] for r in cur.fetchall()]

        if preferred and preferred in cols:
            return preferred

        for c in self._CANDIDATE_TEXT_COLS:
            if c in cols:
                return c

        raise RuntimeError(
            f"Could not determine text column for table '{self.table}'. "
            f"Tried {self._CANDIDATE_TEXT_COLS} plus config "
            f"'pgvector.text_column'/'pgvector.text_col'. "
            f"Existing columns: {cols}. "
            f"Fix by setting pgvector.text_col in config/app.yaml."
        )

    def _embed(self, text: str) -> List[float]:
        vec = self._embedder.encode(
            [text],
            convert_to_numpy=True,
            normalize_embeddings=True,
        )[0]
        return vec.astype(np.float32).tolist()

    @staticmethod
    def _to_pgvector_literal(vec: List[float]) -> str:
        # Format as a pgvector literal, e.g. "[0.1,0.2,...]"
        return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"

    def get_relevant(
        self,
        question: str,
        company: Optional[str],
        year: Optional[int],
        top_k: int,
    ) -> List[Dict[str, Any]]:
        if top_k <= 0:
            top_k = 5

        q_lit = self._to_pgvector_literal(self._embed(question))

        where_parts: List[str] = []
        params: List[Any] = [q_lit]

        if company is not None:
            where_parts.append("company = %s")
            params.append(company)

        if year is not None:
            where_parts.append("year = %s")
            params.append(int(year))

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        sql = f"""
        SELECT id, company, year, {self.text_col} AS chunk, dist
        FROM (
            SELECT
                id,
                company,
                year,
                {self.text_col},
                embedding <-> %s::vector AS dist
            FROM {self.table}
            {where_sql}
            ORDER BY embedding <-> %s::vector
            LIMIT %s
        ) sub
        ORDER BY dist ASC;
        """

        exec_params = params + [q_lit, top_k]

        out: List[Dict[str, Any]] = []
        with psycopg.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, exec_params)
            for row in cur.fetchall():
                _id, _company, _year, _chunk, _dist = row
                out.append(
                    {
                        "id": _id,
                        "company": _company,
                        "year": _year,
                        "chunk": _chunk,
                        "distance": float(_dist),
                    }
                )

        return out


# ---------- simple chain ----------
class _SimpleChain:
    """
    The object that serving/api.py treats as CHAIN.

    - .run(...) / .invoke(...) returns:
        { "answer": str, "contexts": [str], "meta": {...} }

    Retrieval:
      - always pgvector (chunks table).

    Answering:
      - if use_ollama and we have context: summarize from context with local LLM
      - elif use_openai and we have context: summarize via OpenAI (off in Docker)
      - elif flan-T5 is available: summarize via local HF pipeline
      - else: answer = first chunk (or fallback string)
    """

    def __init__(
        self,
        retriever: PGVectorRetriever,
        max_context_chars: int = 6000,
        rerank_keep: int = 5,
        use_openai: bool = False,
        use_ollama: bool = False,
        ollama_base_url: Optional[str] = None,
        ollama_model: Optional[str] = None,
        use_flan_t5: bool = False,
    ) -> None:
        self.retriever = retriever
        self.max_context_chars = max_context_chars
        self.rerank_keep = max(rerank_keep, 1)

        self.use_openai = use_openai
        self.use_ollama = use_ollama
        self.use_flan_t5 = use_flan_t5 and _HAS_T5

        self.ollama_base_url = (ollama_base_url or "http://ollama:11434").rstrip("/")
        self.ollama_model = ollama_model or "llama3.2:1b"

        self._openai_chat = None  # kept for serving.api compatibility

        # For meta.llm
        if self.use_ollama:
            self.llm_name = f"ollama:{self.ollama_model}"
        elif self.use_openai:
            self.llm_name = "openai"
        elif self.use_flan_t5:
            self.llm_name = _T5_NAME
        else:
            self.llm_name = "none"

    def run(
        self,
        question: str,
        company: Optional[str],
        year: Optional[int],
        top_k: int,
    ) -> Dict[str, Any]:
        # serving/api.py calls this
        return self.invoke(question, company, year, top_k)

    def invoke(
        self,
        question: str,
        company: Optional[str],
        year: Optional[int],
        top_k: int,
    ) -> Dict[str, Any]:
        # 1) retrieve
        hits = self.retriever.get_relevant(question, company, year, top_k)
        hits = hits[: self.rerank_keep]

        # 2) pack context
        context_text, kept = self._pack_context(hits, self.max_context_chars)

        # 3) answer
        if self.use_ollama and kept:
            answer = self._answer_with_ollama(question, context_text)
        elif self.use_openai and kept:
            answer = self._answer_with_openai(question, context_text)
        else:
            # local path (flan-T5 if available, otherwise raw chunk)
            answer = self._answer_locally(question, kept)

        # 4) return
        return {
            "answer": answer,
            "contexts": [h["chunk"] for h in kept],
            "meta": {
                "company": company,
                "year": year,
                "n_ctx": len(kept),
                "retriever": "pgvector",
                "llm": self.llm_name,
            },
        }

    def _pack_context(
        self,
        hits: List[Dict[str, Any]],
        max_chars: int,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        buf: List[str] = []
        kept: List[Dict[str, Any]] = []
        cur = 0

        for h in hits:
            t = h.get("chunk", "") or ""
            if cur + len(t) + 2 > max_chars:
                break
            buf.append(t)
            kept.append(h)
            cur += len(t) + 2

        return "\n\n".join(buf), kept

    def _answer_locally(self, question: str, hits: List[Dict[str, Any]]) -> str:
        """
        Local answer path:
        - if flan-T5 HF pipeline is available, use it
        - else, just return the first chunk
        """
        if not hits:
            return "I couldn't find relevant context locally."

        chunks = [h["chunk"] for h in hits]

        if self.use_flan_t5 and _HAS_T5 and t5_generate_answer is not None:
            try:
                return t5_generate_answer(chunks, question)
            except Exception:
                # Silent fallback to deterministic chunk
                return chunks[0]

        return chunks[0]

    def _answer_with_ollama(self, question: str, context_text: str) -> str:
        """
        Call local Ollama chat API:
          POST {OLLAMA_BASE_URL}/api/chat
        """
        if not context_text.strip():
            return self._answer_locally(question, [])

        url = self.ollama_base_url.rstrip("/") + "/api/chat"

        payload = {
            "model": self.ollama_model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You answer questions strictly from the provided SEC filing "
                        "context. Be concise, factual, and if the answer is not in "
                        "the context, say you don't know."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Question: {question}\n\n"
                        f"Context:\n{context_text}\n\n"
                        "Answer in 1–3 sentences."
                    ),
                },
            ],
            "stream": False,
        }

        data = json.dumps(payload).encode("utf-8")
        req = urlrequest.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        # 1B on CPU can be slow; be generous with timeout
        try:
            with urlrequest.urlopen(req, timeout=180) as resp:
                body = resp.read().decode("utf-8")
            obj = json.loads(body)
            msg = (obj.get("message") or {}).get("content") or ""
            msg = msg.strip()
            if msg:
                return msg
        except (urlerror.URLError, TimeoutError, json.JSONDecodeError, KeyError):
            # Fall back to local path (which may be flan-T5)
            return self._answer_locally(question, [])

        return self._answer_locally(question, [])

    def _answer_with_openai(self, question: str, context_text: str) -> str:
        """
        OpenAI path – effectively disabled in your Docker env
        (NO_OPENAI=1 and OPENAI_API_KEY empty).
        """
        try:
            from openai import OpenAI
        except Exception:
            return self._answer_locally(question, [])

        if self._openai_chat is None:
            self._openai_chat = OpenAI()

        sys_prompt = (
            "You are a helpful assistant answering questions strictly from the provided context. "
            "If the answer isn't present, say you don't know."
        )
        user_prompt = f"Question: {question}\n\nContext:\n{context_text}\n\nAnswer:"

        try:
            resp = self._openai_chat.chat.completions.create(
                model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.2,
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            return self._answer_locally(question, [])
