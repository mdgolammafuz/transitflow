# serving/api.py
import os, sys, time, uuid
from typing import Any, Dict, List, Optional

# make "rag/..." imports work when running inside container
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi import (
    FastAPI, HTTPException, Request, Response, Header, Depends, Body, Query, APIRouter
)
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import yaml

from serving.logging_setup import logger as log
from serving.settings import Settings
from serving.cache import get_cache, make_key

from rag.chain import load_chain
from rag.extract import extract_driver
from rag.generator import generate_answer

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

# Optional ingest helpers
try:
    from serving.ingest_runner import ensure_ingested, is_ingested
except Exception:
    ensure_ingested = None
    is_ingested = None

# --------------------------------------------------------------------------------------
# Config / settings
# --------------------------------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DRIVERS_CFG_PATH = os.path.join(BASE_DIR, "config", "drivers.yml")
settings = Settings()

def _load_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path): return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _langsmith_env() -> Dict[str, Any]:
    """Non-secret snapshot of LangSmith env flags (for debugging on Render)."""
    return {
        "LANGCHAIN_TRACING_V2": bool(os.getenv("LANGCHAIN_TRACING_V2")),
        "LANGCHAIN_API_KEY_set": bool(os.getenv("LANGCHAIN_API_KEY")),
        "LANGCHAIN_PROJECT": os.getenv("LANGCHAIN_PROJECT"),
        "LANGCHAIN_ENDPOINT_set": bool(os.getenv("LANGCHAIN_ENDPOINT")),
    }

# --------------------------------------------------------------------------------------
# Schemas
# --------------------------------------------------------------------------------------
class QueryRequest(BaseModel):
    text: str
    company: Optional[str] = None
    year: Optional[int] = None
    top_k: int = 5
    no_openai: bool = False

class QueryResponse(BaseModel):
    answer: str
    contexts: List[str]
    meta: Dict[str, Any]

class DriverRequest(BaseModel):
    company: str
    year: Optional[int] = None
    top_k: int = 8
    no_openai: bool = False

# --------------------------------------------------------------------------------------
# Chain / cache / auth / limiter
# --------------------------------------------------------------------------------------
SKIP_CHAIN_INIT = os.getenv("SKIP_CHAIN_INIT", "").lower() in {"1","true","yes"}
USE_HYBRID = os.getenv("USE_HYBRID", "false").lower() in {"1", "true", "yes"}

# Initialize CHAIN with hybrid support
if SKIP_CHAIN_INIT:
    CHAIN = None
elif USE_HYBRID:
    try:
        from rag.chain import load_chain_hybrid
        CHAIN = load_chain_hybrid(settings.app_cfg_path)
        log.info("chain.loaded", mode="hybrid")
    except Exception as e:
        log.warning("chain.hybrid_failed", error=str(e), fallback="dense")
        CHAIN = load_chain(settings.app_cfg_path)
else:
    CHAIN = load_chain(settings.app_cfg_path)
    log.info("chain.loaded", mode="dense")

CACHE = get_cache(ttl_s=int(os.getenv("CACHE_TTL_SECONDS", "600")))
API_KEYS = {k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()}

def _check_key(x_api_key: Optional[str], request: Request) -> None:
    """Allow key via header X-API-Key or query param api_key."""
    if not API_KEYS:
        return
    qp_key = request.query_params.get("api_key")
    key = x_api_key or qp_key
    if key not in API_KEYS:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

def require_api_key(request: Request, x_api_key: Optional[str] = Header(None)):
    _check_key(x_api_key, request)

limiter = Limiter(key_func=get_remote_address, storage_uri=os.getenv("REDIS_URL","memory://"))

# LangSmith (safe no-op if unavailable)
_ls_import_ok = False
_ls_version = None
try:
    import langsmith  # type: ignore
    from langsmith import traceable  # type: ignore
    _ls_import_ok = True
    try:
        _ls_version = getattr(langsmith, "__version__", None)
    except Exception:
        _ls_version = None
except Exception:
    def traceable(*args, **kwargs):
        def _wrap(fn): return fn
        return _wrap

@traceable(name="query", run_type="chain")
def traced_chain_run(question: str, company: Optional[str], year: Optional[int], top_k: int, use_openai: bool) -> Dict[str, Any]:
    CHAIN.use_openai = use_openai
    return CHAIN.run(question=question, company=company, year=year, top_k=top_k)

@traceable(name="ls-smoke", run_type="chain")
def _ls_smoke(payload: Dict[str, Any]) -> Dict[str, Any]:
    # tiny function only to emit a LangSmith run
    return {"ok": True, "echo": payload}

# --------------------------------------------------------------------------------------
# App (CORS FIRST)
# --------------------------------------------------------------------------------------
app = FastAPI(title="IntelSent API", version="0.6")

# CORS first so preflights never hit other middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "https://intel-ui.vercel.app",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_methods=["GET","POST","OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=86400,
)

# Rate limit AFTER CORS
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

# wildcard OPTIONS to avoid 405 from proxies
@app.options("/{rest_of_path:path}")
def _opt_any(rest_of_path: str) -> Response:
    return Response(status_code=204)

# --------------------------------------------------------------------------------------
# Access log
# --------------------------------------------------------------------------------------
@app.middleware("http")
async def access_log_mw(request: Request, call_next):
    rid = str(uuid.uuid4()); start = time.perf_counter()
    request.state.request_id = rid
    try:
        resp: Response = await call_next(request)
        dur_ms = (time.perf_counter()-start)*1000.0
        log.info("http.request", path=request.url.path, status=resp.status_code, duration_ms=round(dur_ms,2), request_id=rid)
        resp.headers["X-Request-ID"] = rid
        return resp
    except Exception as e:
        log.error("http.error", path=request.url.path, error=str(e), request_id=rid)
        raise

@app.exception_handler(RateLimitExceeded)
def _rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse({"detail":"rate limit exceeded"}, status_code=429)

# Ingest router (best-effort)
try:
    from serving.ingest_runner import router as ingest_router
    app.include_router(ingest_router)
except Exception:
    pass

# --------------------------------------------------------------------------------------
# A2A: actions router
# --------------------------------------------------------------------------------------
actions = APIRouter(prefix="/actions", tags=["actions"])

@actions.get("/suggest")
def suggest_next(
    company: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    q: Optional[str] = Query(None),
    _=Depends(require_api_key),
):
    ideas: List[str] = []
    if company and year:
        ideas = [
            f"Fetch {company} {year} 10-K if missing",
            f"Which products drove revenue growth for {company} in {year}?",
            f"What were the main risks {company} listed in {year}?",
        ]
    elif company:
        ideas = [
            f"Top revenue drivers over last 3 years for {company}?",
            f"Fastest-growing segment for {company} recently?",
        ]
    else:
        ideas = [
            "Which products drove revenue growth?",
            "Which segments changed YoY?",
        ]
    return {"suggestions": ideas[:3], "echo": {"company": company, "year": year, "q": q}}

@actions.post("/ingest")
def ingest_if_missing(
    company: str = Body(..., embed=True),
    year: int = Body(..., embed=True),
    rebuild: bool = Body(False, embed=True),
    sec_user_agent: Optional[str] = Body(None, embed=True),
    _=Depends(require_api_key),
):
    if ensure_ingested is None or is_ingested is None:
        raise HTTPException(status_code=503, detail="ingest helpers unavailable")
    if is_ingested(company, year) and not rebuild:
        return {"ok": True, "ingested": True, "status": "already_present"}
    ok = ensure_ingested(company, year, rebuild=rebuild, sec_user_agent=sec_user_agent)
    if not ok:
        raise HTTPException(status_code=502, detail="ingest failed (EDGAR fetch error)")
    return {"ok": True, "ingested": True, "status": "fetched_and_stamped"}

@actions.get("/status")
def ingest_status(
    company: str = Query(...),
    year: int = Query(...),
    _=Depends(require_api_key),
):
    if is_ingested is None:
        raise HTTPException(status_code=503, detail="ingest helpers unavailable")
    return {"ok": True, "ingested": is_ingested(company, year), "company": company, "year": year}

@actions.post("/compare")
def compare_companies(
    companies: List[str] = Body(...),
    metric: str = Body(...),
    years: List[int] = Body(...),
    _=Depends(require_api_key),
):
    """
    Compare metric across multiple companies.
    
    Example:
        POST /actions/compare
        {
          "companies": ["AAPL", "MSFT"],
          "metric": "revenue",
          "years": [2023]
        }
    """
    if CHAIN is None:
        raise HTTPException(status_code=503, detail="CHAIN not initialized")
    
    results = {}
    all_sources = []
    
    for company in companies:
        company_data = {}
        for year in years:
            question = f"What was {company}'s {metric} in {year}?"
            
            try:
                response = traced_chain_run(question, company, year, 3, False)
                company_data[str(year)] = response["answer"]
                all_sources.extend(response.get("contexts", [])[:1])
            except Exception as e:
                log.error("compare.error", company=company, year=year, error=str(e))
                company_data[str(year)] = f"Error: {str(e)}"
        
        results[company] = company_data
    
    return {
        "metric": metric,
        "companies": companies,
        "years": years,
        "comparison": results,
        "sources": all_sources[:5],
    }

app.include_router(actions)

# --------------------------------------------------------------------------------------
# MCP endpoints
# --------------------------------------------------------------------------------------
@app.get("/mcp/tools")
def mcp_get_tools():
    """Get MCP tools manifest."""
    return {
        "tools": [
            {
                "name": "retrieve_filing",
                "description": "Retrieve relevant chunks from SEC filing",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "company": {"type": "string", "description": "Company ticker (AAPL, MSFT, TSLA)"},
                        "year": {"type": "integer", "description": "Fiscal year"},
                        "question": {"type": "string", "description": "Query question"},
                        "top_k": {"type": "integer", "default": 5},
                    },
                    "required": ["company", "year", "question"],
                },
            },
            {
                "name": "compare_companies",
                "description": "Compare financial metrics across companies",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "companies": {"type": "array", "items": {"type": "string"}},
                        "metric": {"type": "string", "description": "revenue, margin, R&D, etc."},
                        "years": {"type": "array", "items": {"type": "integer"}},
                    },
                    "required": ["companies", "metric", "years"],
                },
            },
            {
                "name": "analyze_trend",
                "description": "Analyze metric trend over multiple years for a company",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "company": {"type": "string", "description": "Company ticker"},
                        "metric": {"type": "string", "description": "revenue, profit, R&D, etc."},
                        "start_year": {"type": "integer", "description": "Start year"},
                        "end_year": {"type": "integer", "description": "End year"},
                    },
                    "required": ["company", "metric", "start_year", "end_year"],
                },
            },
        ]
    }

@app.post("/mcp/execute")
def mcp_execute(
    tool: str = Body(...),
    arguments: Dict[str, Any] = Body(...),
):
    """Execute MCP tool."""
    if CHAIN is None:
        raise HTTPException(status_code=503, detail="CHAIN not initialized")
    
    if tool == "retrieve_filing":
        company = arguments.get("company")
        year = arguments.get("year")
        question = arguments.get("question")
        top_k = arguments.get("top_k", 5)
        
        response = traced_chain_run(question, company, year, top_k, False)
        
        return {
            "company": company,
            "year": year,
            "question": question,
            "answer": response["answer"],
            "chunks": [
                {"text": ctx, "index": i}
                for i, ctx in enumerate(response.get("contexts", []))
            ],
        }
    
    elif tool == "compare_companies":
        return compare_companies(
            companies=arguments["companies"],
            metric=arguments["metric"],
            years=arguments["years"],
        )
    
    elif tool == "analyze_trend":
        company = arguments["company"]
        metric = arguments["metric"]
        start_year = arguments["start_year"]
        end_year = arguments["end_year"]
        
        years = list(range(start_year, end_year + 1))
        results = {}
        
        for year in years:
            question = f"What was {company}'s {metric} in {year}?"
            try:
                response = traced_chain_run(question, company, year, 3, False)
                results[str(year)] = {
                    "answer": response["answer"],
                    "context": response.get("contexts", [])[0] if response.get("contexts") else ""
                }
            except Exception as e:
                results[str(year)] = {"error": str(e)}
        
        return {
            "company": company,
            "metric": metric,
            "years": years,
            "trend": results,
        }
    
    else:
        raise HTTPException(status_code=400, detail=f"Unknown tool: {tool}")

# --------------------------------------------------------------------------------------
# Startup
# --------------------------------------------------------------------------------------
DRIVERS_CFG: Dict[str, Any] = {}

@app.on_event("startup")
def startup() -> None:
    global DRIVERS_CFG
    DRIVERS_CFG = _load_yaml(DRIVERS_CFG_PATH)
    log.info(
        "app.startup",
        app_config=settings.app_cfg_path,
        drivers_cfg_loaded=bool(DRIVERS_CFG),
        chain_initialized=CHAIN is not None,
        chain_mode="hybrid" if USE_HYBRID else "dense",
        config=settings.redacted(),
        langsmith_env=_langsmith_env(),
        langsmith_import_ok=_ls_import_ok,
        langsmith_version=_ls_version,
    )

# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------
@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "IntelSent",
        "version": "0.6",
        "config": settings.redacted(),
        "drivers_cfg_loaded": bool(DRIVERS_CFG),
        "chain_initialized": CHAIN is not None,
        "chain_mode": "hybrid" if USE_HYBRID else "dense",
        "langsmith_env": _langsmith_env(),
        "langsmith_import_ok": _ls_import_ok,
        "langsmith_version": _ls_version,
    }

@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"status": "ok"}

@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> str:
    return "# intelsent demo metrics\nok 1\n"

@app.get("/debug/langsmith")
def debug_langsmith(request: Request) -> Dict[str, Any]:
    _check_key(None, request)
    return _langsmith_env() | {"import_ok": _ls_import_ok, "version": _ls_version}

@app.get("/debug/ls_imports")
def debug_ls_imports(request: Request) -> Dict[str, Any]:
    _check_key(None, request)
    return {"import_ok": _ls_import_ok, "version": _ls_version}

@app.get("/debug/ls_emit")
def debug_ls_emit(request: Request) -> Dict[str, Any]:
    _check_key(None, request)
    ts = int(time.time())
    out = _ls_smoke({"ts": ts, "note": "manual-debug"})
    return {"emitted": True, "ts": ts, "result": out}

# Keep POST /query for API clients
@app.post("/query", response_model=QueryResponse)
def query(
    request: Request,
    req: QueryRequest = Body(...),
    _=Depends(require_api_key),
) -> QueryResponse:
    if CHAIN is None:
        raise HTTPException(status_code=503, detail="CHAIN not initialized")
    cache_key = make_key("q:v1", {
        "text": req.text, "company": req.company, "year": req.year,
        "top_k": req.top_k, "use_openai": not req.no_openai,
    })
    cached = CACHE.get(cache_key)
    if cached: return QueryResponse(**cached)
    out = traced_chain_run(req.text, req.company, req.year, req.top_k, not req.no_openai)
    # Add chunk_ids to meta for eval
    if "chunk_ids" in out and "meta" in out:
        out["meta"]["chunk_ids"] = out["chunk_ids"]
    CACHE.set(cache_key, out)
    return QueryResponse(**out)

# GET /query_min (no preflight) for browsers
@app.get("/query_min", response_model=QueryResponse)
@limiter.limit("10/minute;2/second")
def query_min(
    request: Request,
    text: str = Query(...),
    company: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    top_k: int = Query(3),
    no_openai: bool = Query(True),
    api_key: Optional[str] = Query(None),
):
    _check_key(None, request)
    if CHAIN is None:
        raise HTTPException(status_code=503, detail="CHAIN not initialized")
    cache_key = make_key("qmin:v1", {
        "text": text, "company": company, "year": year,
        "top_k": top_k, "use_openai": not no_openai,
    })
    cached = CACHE.get(cache_key)
    if cached: return QueryResponse(**cached)
    out = traced_chain_run(text, company, year, top_k, not no_openai)
    # Add chunk_ids to meta for eval
    if "chunk_ids" in out and "meta" in out:
        out["meta"]["chunk_ids"] = out["chunk_ids"]
    CACHE.set(cache_key, out)
    return QueryResponse(**out)