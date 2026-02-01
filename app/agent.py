"""
LLM Agent for IMDB Chat

This module implements an agent that:
1. Receives natural language questions
2. Generates SQL queries using OpenAI
3. Executes queries on BigQuery
4. Returns formatted results
"""

import json
import os
from typing import List, Dict, Any, Optional

from openai import OpenAI
from dotenv import load_dotenv

from app.bigquery_tool import execute_sql, format_schema_for_llm

# Load environment variables
load_dotenv()

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)


# Tool definition for OpenAI
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "execute_bigquery_sql",
            "description": "Execute a SQL query on the BigQuery IMDB database and return results",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql_query": {
                        "type": "string",
                        "description": "The SQL query to execute on BigQuery. Must be fully qualified (include project.dataset.table).",
                    }
                },
                "required": ["sql_query"],
                "additionalProperties": False,
            },
            "strict": True,
        },
    }
]


def create_system_prompt() -> str:
    """
    Create the system prompt with schema information.
    
    Returns:
        System prompt string
    """
    schema_info = format_schema_for_llm()
    
    system_prompt = f"""You are an expert SQL assistant for an IMDB movie database.

Your role is to:
1. Understand natural language questions about movies, actors, and directors
2. Generate correct BigQuery SQL queries
3. Execute queries and interpret results
4. Provide clear, concise answers

{schema_info}

**Guidelines:**
- Always use fully qualified table names: `imdb-analytics.silver.table_name`
- Use backticks for table names with hyphens
- For nested arrays (directors, actors), use UNNEST() and EXISTS()
- Keep queries efficient (use LIMIT when appropriate)
- If a question is ambiguous, make reasonable assumptions
- Return results in a user-friendly format

**Response Style:**
- Be conversational and helpful
- Explain what you found
- Suggest related queries if relevant
- If no results, explain why and suggest alternatives
"""
    
    return system_prompt


def run_agent(
    user_question: str,
    conversation_history: List[Dict[str, str]] = None,
    model: str = None
) -> Dict[str, Any]:
    """
    Run the agent to answer a user question.
    
    Args:
        user_question: Natural language question from user
        conversation_history: Previous messages (for context)
        model: OpenAI model to use (defaults to OPENAI_MODEL env var)
    
    Returns:
        Dictionary with:
        - answer: str (final answer)
        - sql_query: str or None (SQL that was executed)
        - data: DataFrame or None (query results)
        - error: str or None (error message if any)
        - usage: dict or None (token usage statistics)
    """
    # Use provided model or default
    if model is None:
        model = OPENAI_MODEL
    
    # Initialize conversation
    if conversation_history is None:
        conversation_history = []
    
    # Add system prompt
    messages = [{"role": "system", "content": create_system_prompt()}]
    
    # Add conversation history
    messages.extend(conversation_history)
    
    # Add user question
    messages.append({"role": "user", "content": user_question})
    
    # Variables to track
    sql_executed = None
    query_result = None
    total_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    max_iterations = 3  # Prevent infinite loops
    
    # Agent loop
    for iteration in range(max_iterations):
        # Call OpenAI
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
        )
        
        # Track usage
        if response.usage:
            total_usage["prompt_tokens"] += response.usage.prompt_tokens
            total_usage["completion_tokens"] += response.usage.completion_tokens
            total_usage["total_tokens"] += response.usage.total_tokens
        
        assistant_message = response.choices[0].message
        
        # Check if tool was called
        if assistant_message.tool_calls:
            # Add assistant message to history
            messages.append({
                "role": "assistant",
                "content": assistant_message.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": tc.type,
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        }
                    }
                    for tc in assistant_message.tool_calls
                ]
            })
            
            # Execute each tool call
            for tool_call in assistant_message.tool_calls:
                if tool_call.function.name == "execute_bigquery_sql":
                    # Parse arguments
                    args = json.loads(tool_call.function.arguments)
                    sql_query = args["sql_query"]
                    sql_executed = sql_query
                    
                    # Execute query
                    result = execute_sql(sql_query)
                    query_result = result
                    
                    # Format result for LLM
                    if result["success"]:
                        tool_response = {
                            "success": True,
                            "rows_returned": result["rows_returned"],
                            "data": result["data"].to_dict(orient="records") if result["data"] is not None else [],
                        }
                    else:
                        tool_response = {
                            "success": False,
                            "error": result["error"],
                        }
                    
                    # Add tool response to messages
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(tool_response),
                    })
        else:
            # No tool call, model has final answer
            final_answer = assistant_message.content
            
            return {
                "answer": final_answer,
                "sql_query": sql_executed,
                "data": query_result["data"] if query_result and query_result["success"] else None,
                "error": None,
                "usage": total_usage,
            }
    
    # Max iterations reached
    return {
        "answer": "I'm sorry, I couldn't complete the query. Please try rephrasing your question.",
        "sql_query": sql_executed,
        "data": None,
        "error": "Max iterations reached",
        "usage": total_usage,
    }

# Test function
if __name__ == "__main__":
    print("=== Testing Agent ===\n")
    
    # Test question
    question = "What are the top 5 highest-rated movies?"
    
    print(f"Question: {question}\n")
    
    result = run_agent(question)
    
    print(f"Answer: {result['answer']}\n")
    
    if result["sql_query"]:
        print(f"SQL Executed:\n{result['sql_query']}\n")
    
    if result["data"] is not None:
        print(f"Results:\n{result['data']}")