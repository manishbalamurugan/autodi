from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from typing import List, Dict, Optional
import os
import json
from agent.agent import Agent

app = FastAPI()
agent = Agent()

# Setup directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

@app.post("/api/v1/process")
async def process_data(
    prompt: str = Form(...),
    files: List[UploadFile] = File(None),
    approve_plan: bool = Form(False),
    plan: Optional[str] = Form(None)
):
    try:
        # Save uploaded files
        file_paths = {}
        if files:
            for file in files:
                file_path = os.path.join(UPLOAD_DIR, file.filename)
                content = await file.read()
                with open(file_path, "wb") as f:
                    f.write(content)
                file_paths[file.filename] = file_path

        # Handle request based on type
        if approve_plan and plan:
            plan_dict = json.loads(plan)
            result = agent.execute_tool_calls(plan_dict["function_calls"], file_paths)  # Changed from tool_calls to function_calls
            return {
                "response": "Plan executed successfully",
                "filename": result.get("filename"),
                "file": result.get("file")
            }

        # Check if this is a processing request
        if any(keyword in prompt.lower() for keyword in ["process", "convert", "transform"]):
            plan = agent.plan_and_execute(prompt, file_paths)
            return {
                "requires_approval": True,  # Always require approval for processing
                "explanation": plan.get("explanation", ""),
                "plan": {
                    "function_calls": plan.get("tool_calls", [])  # Changed structure to match client expectation
                }
            }

        # Handle as a regular question
        response = agent.ask(prompt, file_paths)
        return {"response": response.get('response', '')}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/available_functions")
async def get_available_functions():
    return {"functions": [tool["function"]["name"] for tool in agent.tools]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)