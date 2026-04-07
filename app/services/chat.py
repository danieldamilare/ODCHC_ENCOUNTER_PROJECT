from .base import BaseServices
from google import genai
from google.genai import types
from groq import Groq, RateLimitError, APIStatusError
from app.config import Config
from app.exceptions import ValidationError, MissingError, QueryParameterError
from typing import Optional, List, Dict
import time
import json
import sqlite3

class ChatServices(BaseServices):
    with open(Config.LLM_SCHEMA_PATH, "r") as f:
        _schema_content = f.read()

    MODEL_CHAIN =[
    'llama-3.3-70b-versatile',
    'meta-llama/llama-4-scout-17b-16e-instruct',
    'moonshotai/kimi-k2-instruct-0905',
    'openai/gpt-oss-120b'
    ]

    system_prompt = f"""
YOUR ROLE:
You are Son of Aton, an expert healthcare data analyst for Ondo State Contributory Health Commission (ODCHC), a commission focused on health insurance for Ondo State, Nigeria. Your mission is to provide high-integrity, data-driven insights into health insurance encounters, facility performance, disease trends, and mortality statistics across Ondo State based on ODCHC Database records.

YOUR CAPABILITIES:
1. SQL Queries: Use execute_sql_query to fetch data. use SQLite dialect only.
2. Data Analysis: Generate reports, identify trends, and provide actionable insights.
3. Pattern Recognition: Detect anomalies, trends, and patterns in healthcare data.

BEHAVIORAL GUIDELINES:
- Always query the database first - never guess or estimate.
- Use master_encounter_view for most analytical queries (it's pre-joined with costs already in Naira)
- If a query fails, analyze the error message, fix the SQL, and retry before responding.
- If a query returns no results, inform the user clearly (don't hallucinate data)
- PRIVACY: Never display full client_name or phone_number unless explicitly requested for patient verification
- For ambiguous queries (e.g., "Check mortality"), ask for clarification on time period or location
- Politely decline questions unrelated to ODCHC data analysis


{_schema_content}

RESPONSE FORMAT:
- Give numbers with context
- Trends: Explain patterns with key metrics: what the pattern likely means, not just what number are
  Example: "Malaria cases decreased 23% in Q2, likely due to the rainy season intervention program"
-  Use Markdown tables and lists when presenting structured data.

- If a tool call doesn't have a response in the format {{"role": "tool",
                                            "tool_call_id": func.id,
                                            "name": func.function.name,
                                            "content": str(result)}}. Then an error has occured when calling the tool, 
                                            inform the user. Do not hallucinate response

## EXAMPLE WORKFLOWS:
Question: "What is the total service utilization last month?"
1. Use execute_sql_query:
   SELECT COUNT(*) as total_items
   FROM view_utilization_items v
   JOIN encounters e ON v.encounter_id = e.id
   WHERE e.date >= date('now', 'start of month', '-1 month')
     AND e.date < date('now', 'start of month')
2. Return: "Last month had 3,456 total utilizations (diseases + services combined)"
"""

    def __init__(self) -> None:
        self.db = sqlite3.connect(f"file:{Config.DATABASE}?mode=ro", uri=True, check_same_thread=False, timeout=7)
        self.db.row_factory = sqlite3.Row
        self.MAX_ITER_LOOP = 10
        self.execute_sql_query_schema = {
        "type": "function",
        "function": {
            "name": "execute_sql_query",
            "description": """Executes a read-only SQLite SELECT query against the ODCHC database.
    Returns a JSON object with:
    - success (bool): whether the query succeeded
    - data (list of dicts): the query results, max 100 rows
    - row_count (int): number of rows returned
    - truncated (bool): true if results were cut off at 100 rows
    On failure returns: success (false) and error (str) describing what went wrong.""",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "A valid SQLite SELECT query string"
                    }
                },
                "required": ["query"]
            }
        }
    }


    def execute_sql_query(self, query: str):

        """
    Executes a read-only SQLite SELECT query against the ODCHC healthcare database.
    Use this tool to fetch raw data for analysis, the master_encounter_view flat table is a good starting point for most queries as it contains pre-joined data with costs in Naira.
    But for complex query that include joins not included in the view, you can query the underlying tables directly.

    Args:
        query: valid SQL SELECT query as a string
    Returns:
    Dict with 'success' (bool), 'data' (list of dicts with query results), and 'row_count' (int) if successful, and truncated if query row is greater than 100 and has been reduced;
    or 'success' (False) and 'error' (str) if an error occurs
    """

        query = query.strip()
        print("=======================Accessing Database with Query================")
        print("Query: ", query)
        res = {}
        try:
            cursor = self.db.execute(query)
            rows = cursor.fetchmany(100)

            res =  {
                "success": True,
                "data": [dict(row) for row in rows],
                "row_count": len(rows),
                "truncated": cursor.fetchone() is not None
            }
        except sqlite3.Error as e:
            res = {
                "error": str(e),
                "success": False
            }
        print("Result: ", res)
        return res


class GroqChatServices(ChatServices):
    def __init__(self):
        print(Config.GROQ_API_KEY)
        self.client = Groq(api_key=Config.GROQ_API_KEY)

        super().__init__()

    def generate_response(self,
                               user_input: str,
                               conversation_history: Optional[List[Dict]] = None):
        history = conversation_history or []
        print("Generating response with conversation history:", history)
        history = ([{"role": "system", "content" : self.system_prompt}] +
                    history + [{"role": "user", "content": user_input}])

        max_retries = 7

        for attempt in range(1, max_retries):
            print("History: ")
            print(history)
            try:
                for i in range(self.MAX_ITER_LOOP):
                    response = self.client.chat.completions.create(
                        model='moonshotai/kimi-k2-instruct',
                        messages=history,
                        tools=[self.execute_sql_query_schema],
                        temperature=0.1,
                        top_p = 0.85,
                        stream= True,
                    )
                    last_function_call = None
                    collected_content = ""
                    finish_reason = ""

                    for chunk in response:
                        delta = chunk.choices[0].delta if chunk.choices else None
                        if delta is None:
                            continue
                        if delta.content:
                            collected_content += delta.content
                            yield f"[[MESG]]{delta.content}"

                        if delta.tool_calls:
                            last_function_call = delta.tool_calls

                        if chunk.choices[0].finish_reason:
                            finish_reason = chunk.choices[0].finish_reason

                    history.append({
                        "role": "assistant",
                        "content": collected_content,
                        "tool_calls": last_function_call if  last_function_call else []
                        })

                    if last_function_call and finish_reason == 'tool_calls':
                        func = last_function_call[0]
                        print(func)
                        yield f"[[QUERY]]Analyzing Database..."
                        print("Tool call for execute_sql_query received.")
                        function_args = json.loads(
                                func.function.arguments)
                        query = function_args.get("query")
                        if query:
                            print("Executing SQL query from tool call:", query)
                            result = self.execute_sql_query(query=query)
                            history.append({"role": "tool",
                                            "tool_call_id": func.id,
                                            "name": func.function.name,
                                            "content": str(result)})
                            print(f"\n\n\nHistory: ")
                            print(history)
                        continue
                    elif finish_reason == 'stop':
                        return
            except RateLimitError:
                wait = 5 * attempt
                print(f"Rate limited. Retrying in {wait}s")
                time.sleep(wait)
            except APIStatusError as e:
                print(f"API Error (attempt {attempt}/{max_retries}): {e}. Retrying in 3s...")
                time.sleep(3)
            except Exception as e:
                print(f"Network/parsing error (attempt {attempt}/{max_retries}): {e}. " f"Retrying in 3s...")
                time.sleep(3)

        yield "[[ERROR]]Son of Aton failed after multiple retries. Please try again."
    

class GeminiChatServices(ChatServices):
    def __init__(self):
        self.client = genai.Client(api_key=Config.GOOGLE_GENAI_API_KEY)
        super().__init__()

    def generate_response(self,
                          user_input: str,
                          conversation_history: Optional[List[Dict]] = None):


        history = conversation_history or []
        print("Generating response with conversation history:", history)

        tools = [self.execute_sql_query]
        print("Tools available to the model:", tools)
        config = types.GenerateContentConfig(
            system_instruction=self.system_prompt,
            max_output_tokens=4096,
            temperature= 0.1,
            candidate_count=1,
            tools= tools,
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True)
        )

        chat = self.client.chats.create(config = config,
                                 model = Config.GOOGLE_GENAI_MODEL,
                                 history = history)
        current_input = user_input
        counter = 0
        while counter < self.MAX_ITER_LOOP:
            counter +=1
            print("===================== START OF MODEL RESPONSE CYCLE =====================")
            try:
                response_stream = chat.send_message_stream(current_input)
                last_function_call = None

                for res in response_stream:
                    if res.text:
                        print("Getting Response text")
                        yield f"[[MESG]]{res.text}"

                    elif res.candidates and res.candidates[0].content.parts:
                        for part in res.candidates[0].content.parts:
                            if part.function_call:
                                print("Getting Function Call")
                                last_function_call = part.function_call
                                yield f"[[QUERY]]Analyzing Database..."

                if last_function_call:
                    print("Executing function call:", last_function_call)
                    result = self.execute_sql_query(**last_function_call.args)
                    print("Done calling function")

                    current_input = types.Part.from_function_response(
                        name=last_function_call.name,
                        response={"result": result}
                    )
                    print("Sending function response back to model for analysis:", current_input)
                    continue
                break
                print("===================== END OF MODEL RESPONSE CYCLE =====================\n\n")

            except Exception as e:
                print(f"Error: {str(e)}")
                yield f"[[ERROR]] Son of Aton encountered a logic failure."
                break
