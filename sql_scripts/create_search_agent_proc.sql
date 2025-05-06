USE DATABASE SALESFORCE_DEMO_DB;
USE SCHEMA public; 
use warehouse compute_wh;

// Title: Create a Cortex Search Service and Procedure for Lost Item Description
CREATE OR REPLACE CORTEX SEARCH SERVICE resort_conversation_search
  ON LOST_ITEM_DESCRIPTION 
  ATTRIBUTES guest_name, room_number, lost_item_status
  WAREHOUSE = compute_wh
  TARGET_LAG = '1 hour'
  AS (
    SELECT
        object_type,
        object_label,
        guest_name,
        room_number,
        lost_item_status,
        location_name,
        booking_source,
        lost_report_id,
        LOST_ITEM_DESCRIPTION
    FROM salesforce_demo_db.public.RESORT_EVENT_UNIFIED_TABLE);

// Title: Create a Cortex Agent Procedure to wrap the agent API call
CREATE OR REPLACE PROCEDURE call_cortex_agent_proc(query STRING, limit INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'call_cortex_agent_proc'
AS $$
import json
import _snowflake
import re
from snowflake.snowpark.context import get_active_session

def call_cortex_agent_proc(query: str, limit: int = 10):
    session = get_active_session()
    
    API_ENDPOINT = "/api/v2/cortex/agent:run"
    API_TIMEOUT = 50000  

    CORTEX_SEARCH_SERVICES = "salesforce_demo_db.public.resort_conversation_search"
    SEMANTIC_MODELS = "@SALESFORCE_LAI_AGENT.CORE.LIB_STG/salesforce_lai_agent.yaml"

    payload = {
        "model": "llama3.1-70b",
        "messages": [{"role": "user", "content": [{"type": "text", "text": query}]}],
        "tools": [
            {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "analyst1"}},
            {"tool_spec": {"type": "cortex_search", "name": "search1"}}
        ],
        "tool_resources": {
            "analyst1": {"semantic_model_file": SEMANTIC_MODELS},
            "search1": {"name": CORTEX_SEARCH_SERVICES, "max_results": limit}
        }
    }

    try:
        resp = _snowflake.send_snow_api_request(
            "POST", API_ENDPOINT, {}, {}, payload, None, API_TIMEOUT
        )

        if resp["status"] != 200:
            return {"error": "API call failed"}

        response_content = json.loads(resp["content"])
        return process_cortex_response(response_content, session)

    except Exception as e:
        return {"error": str(e)}

def clean_text(text):
    """ Cleans up unwanted characters and symbols from search results. """
    text = re.sub(r'[\u3010\u3011\u2020\u2021]', '', text)  # Remove unwanted symbols
    text = re.sub(r'^\s*ns\s+\d+\.*', '', text)  # Remove prefixes like "ns 1."
    text = text.strip()  # Trim whitespace
    return text

def process_cortex_response(response, session):
    """ Parses Cortex response and executes SQL if provided. """
    result = {"type": "unknown", "text": None, "sql": None, "query_results": None}

    full_text_response = []  # Stores formatted search responses
    
    for event in response:
        if event.get("event") == "message.delta":
            data = event.get("data", {})
            delta = data.get("delta", {})

            for content_item in delta.get("content", []):
                content_type = content_item.get("type")

                if content_type == "tool_results":
                    tool_results = content_item.get("tool_results", {})

                    for result_item in tool_results.get("content", []):
                        if result_item.get("type") == "json":
                            json_data = result_item.get("json", {})

                            if "sql" in json_data:
                                result["type"] = "cortex_analyst"
                                result["sql"] = json_data["sql"]
                                result["text"] = json_data.get("text", "")

                                # Execute the generated SQL query in Snowflake
                                try:
                                    query_results = session.sql(result["sql"]).collect()
                                    result["query_results"] = [row.as_dict() for row in query_results]
                                except Exception as e:
                                    result["query_results"] = {"error": str(e)}

                            elif "searchResults" in json_data:
                                result["type"] = "cortex_search"
                                formatted_results = []

                                for sr in json_data.get("searchResults", []):
                                    search_text = clean_text(sr.get("text", "").strip())
                                    citation = sr.get("citation", "").strip()

                                    if search_text:
                                        if citation:
                                            formatted_results.append(f"- {search_text} (Source: {citation})")
                                        else:
                                            formatted_results.append(f"- {search_text}")
                                
                                if formatted_results:
                                    full_text_response.extend(formatted_results)
                
                elif content_type == "text":
                    text_piece = clean_text(content_item.get("text", "").strip())
                    if text_piece:
                        full_text_response.append(text_piece)

    result["text"] = "\n".join(full_text_response) if full_text_response else "No relevant search results found."
    return query_results
$$;
