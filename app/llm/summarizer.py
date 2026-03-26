import json
from typing import List, Dict, Any
from app.config import settings

from langchain_core.messages import SystemMessage, HumanMessage
from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from langchain_community.chat_models import ChatOllama

async def generate_summary(task: str, current_preview: List[Dict[str, Any]]) -> str:
    preview_json = json.dumps(current_preview[:20], default=str)
    prompt = f"""The user asked: "{task}".
    
Here are the first up to 20 rows of the executed query result:
{preview_json}

Write a natural language summary (2-3 sentences max) explaining what this data shows in response to the user's request.
"""
    provider = settings.LLM_PROVIDER.lower()
    
    # Auto-select
    if getattr(settings, "XAI_API_KEY", None) and provider != "ollama":
        provider = "grok"
    elif getattr(settings, "GROQ_API_KEY", None) and provider != "ollama":
        provider = "groq"
        
    messages = [
        SystemMessage(content="You are an AI data analyst. Summarise the findings concisely."),
        HumanMessage(content=prompt)
    ]
    
    try:
        if provider == "groq":
            llm = ChatGroq(api_key=settings.GROQ_API_KEY, model_name=settings.GROQ_MODEL, temperature=0.2)
        elif provider == "grok" or getattr(settings, "XAI_API_KEY", None):
            llm = ChatOpenAI(api_key=settings.XAI_API_KEY, base_url="https://api.x.ai/v1", model="grok-2-latest", temperature=0.2)
        elif provider == "ollama":
            llm = ChatOllama(base_url=settings.OLLAMA_BASE_URL, model=settings.OLLAMA_MODEL, temperature=0.2)
        elif provider == "mcp":
            llm = ChatOpenAI(api_key=settings.OPENAI_API_KEY, model="gpt-4", temperature=0.2)
        else:
            return "Summary unavailable."
            
        response = await llm.ainvoke(messages)
        return response.content.strip()
    except Exception as e:
        return "Summary unavailable."
