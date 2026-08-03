from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import json, re, httpx

app = FastAPI(title="AI Agent Sidecar")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class LLMClient:
    def __init__(self, mode="local", model="qwen2.5:7b"):
        self.mode = mode
        self.model = model
    async def generate(self, prompt, system=""):
        url = "http://localhost:11434/api/generate"
        payload = {"model": self.model, "prompt": prompt, "system": system, "stream": False, "temperature": 0.1}
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                r = await client.post(url, json=payload)
                return r.json().get("response", "")
        except Exception as e:
            return f"LLM error: {e}"

llm = LLMClient()

class NLP2DAGReq(BaseModel):
    text: str
    available_controls: list[dict]

class NLP2DAGResp(BaseModel):
    dag_json: dict
    error: Optional[str] = None

class OpsReq(BaseModel):
    job_id: str
    metrics: Optional[dict] = None
    logs: Optional[list[str]] = None
    question: Optional[str] = None

class OpsResp(BaseModel):
    analysis: str
    suggestions: list[str] = []

@app.get("/health")
async def health():
    return {"ok": True, "mode": llm.mode}

@app.post("/nlp-to-dag")
async def nlp_to_dag(req: NLP2DAGReq):
    prompt = f"可用控件：{json.dumps(req.available_controls, ensure_ascii=False)}\n用户需求：{req.text}\n输出DAG JSON，只输出JSON。"
    resp = await llm.generate(prompt, "你是一个数据流编排专家。")
    m = re.search(r"\{.*\}", resp, re.DOTALL)
    if m:
        return NLP2DAGResp(dag_json=json.loads(m.group()))
    return NLP2DAGResp(dag_json={}, error="解析失败")

@app.post("/opsbot/analyze")
async def ops_analyze(req: OpsReq):
    prompt = f"作业{req.job_id}，指标{req.metrics}，日志{req.logs}，问题{req.question}"
    resp = await llm.generate(prompt, "你是一个运维专家。")
    return OpsResp(analysis=resp, suggestions=["检查并行度", "检查数据源"])

@app.post("/nl2data/query")
async def nl2data_query(query: str, db_schema: dict):
    prompt = f"表结构{json.dumps(db_schema, ensure_ascii=False)}\n查询{query}\n只输出SQL。"
    sql = await llm.generate(prompt, "你是一个SQL专家。")
    return {"sql": sql.strip(), "summary": "已生成SQL"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
