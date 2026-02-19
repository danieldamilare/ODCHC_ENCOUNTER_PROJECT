from .base import BaseServices
from google import genai
from google.genai import types
from app.config import Config
from app.exceptions import ValidationError, MissingError, QueryParameterError
from typing import Optional, List, Dict
import sqlite3

class ChatServices(BaseServices):
    schema_content = open(Config.LLM_SCHEMA_PATH, "r").read()
    system_prompt = f"""
YOUR ROLE:
You are Son of Aton, an expert healthcare data analyst for Ondo State Contributory Health Commission (ODCHC), a commission focused on health insurance for Ondo State, Nigeria. Your mission is to provide high-integrity, data-driven insights into health insurance encounters, facility performance, disease trends, and mortality statistics across Ondo State based on ODCHC Database records.

YOUR CAPABILITIES:
1. SQL Queries: Use execute_sql_query to fetch data from the database
2. Data Analysis: Generate reports, identify trends, and provide actionable insights
3. Pattern Recognition: Detect anomalies, trends, and patterns in healthcare data

BEHAVIORAL GUIDELINES:
- Always query the database first - never guess or estimate
- Use master_encounter_view for most analytical queries (it's pre-joined with costs already in Naira)
- If a query fails, analyze the error message, fix the SQL, and retry
- If a query returns no results, inform the user clearly (don't hallucinate data)
- Never display full client_name or phone_number unless explicitly requested for patient verification
- For ambiguous queries (e.g., "Check mortality"), ask for clarification on time period or location
- Politely decline questions unrelated to ODCHC data analysis

{schema_content}

RESPONSE FORMAT:
- Simple stats: Give numbers with context
  Example: "There were 1,247 encounters in January 2024, a 12% increase from December"
- Trends: Explain patterns with key metrics
  Example: "Malaria cases decreased 23% in Q2, likely due to the rainy season intervention program"
- Tables: Use Markdown formatting for clarity
- Context: Don't just give a number; explain what it means
- Use Markdown to beautify tables and lists when presenting data

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
        self.client = genai.Client(api_key=Config.GOOGLE_GENAI_API_KEY)
        self.db = sqlite3.connect(f"file:{Config.DATABASE}?mode=ro", uri=True, check_same_thread=False)
        self.db.row_factory = sqlite3.Row

        print("==============================INITIALIZING CHAT SERVICES==============================")
        print("Client initialized with model:", Config.GOOGLE_GENAI_MODEL)
        print("Client: ", self.client)
        print("Database connection established in read-only mode.")
        print("=============================CHAT SERVICES INITIALIZATION COMPLETE==============================\n\n")

    def execute_sql_query(self, query: str):

        """
    Executes a read-only SQLite SELECT query against the ODCHC healthcare database.
    Use this tool to fetch raw data for analysis, the master_encounter_view flat table is a good starting point for most queries as it contains pre-joined data with costs in Naira.
    But for complex query that include joins not included in the view, you can query the underlying tables directly.

    Args:
        query: valid SQL SELECT query as a string
    Returns:
    Dict with 'success' (bool), 'data' (list of dicts with query results), and 'row_count' (int) if successful,
    or 'success' (False) and 'error' (str) if an error occurs
    """

        query = query.strip()

        try:
            cursor = self.db.execute(query)
            rows = cursor.fetchall()
            return {
                "success": True,
                "data": [dict(row) for row in rows],
                "row_count": len(rows)
            }
        except sqlite3.Error as e:
            return {
                "error": str(e),
                "success": False
            }

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
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=False)
        )

        chat = self.client.chats.create(config = config,
                                 model = Config.GOOGLE_GENAI_MODEL,
                                 history = history)
        print("Initialized Chat object: ", chat)
        try:
            response = chat.send_message_stream(user_input)
            for res in response:
                print("response chunk received: ", res)
                if res.text:
                    yield res.text
                elif res.candidates and res.candidates[0].content.parts:
                    for part in res.candidates[0].content.parts:
                        if hasattr(part, 'text') and part.text:
                            yield part.text

        except Exception as e:
            print("Error during response generation: ", str(e))
            yield f"[[ERROR]] An error occurred while generating the response"
