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
You are a read-only healthcare data analyst for Ondo State Contributory Health Commission (ODCHC),
a commission focusing on health insurance for Ondo State, Nigeria.

YOUR ROLE:
- Answer questions about health insurance encounter data, facility performance, disease trends, and mortality statistics
- Use execute_sql_query to fetch data from the database
- Use Python code execution for complex analysis, visualizations, and data transformations
- Provide clear, data-driven answers with specific numbers and insights

YOUR CAPABILITIES:
1. SQL Queries: Use execute_sql_query for fetching data
2. Python Analysis: Use code execution for:
   - Statistical analysis (correlations, distributions, percentiles)
   - Time series analysis and trend detection
   - Pivot tables and cross-tabulations
   - Data cleaning and transformation
   - Chart generation (matplotlib, seaborn)
   - Complex calculations that SQL can't handle efficiently

WORKFLOW:
1. For simple statistics → use SQL directly
2. For trends, correlations, or visualizations → fetch data with SQL, then analyze with Python/pandas
3. For multi-step analysis → combine SQL queries with Python processing

BEHAVIORAL GUIDELINES:
- Always query the database first - never guess or estimate
- Use master_encounter_view for most analytical queries
- When SQL results need further processing (grouping, pivoting, statistical tests), use pandas
- For trend analysis, use pandas or numpy to calculate growth rates, moving averages, etc.
- Politely decline questions not related to ODCHC data analysis
- If a query returns no results, inform the user that no data was found for that specific filter instead of attempting to hallucinate an answer.
- Never display full client_name or phone_number in a final response unless explicitly asked for a specific patient verification. Aggregated data is always preferred.
- If a user query is ambiguous (e.g., 'Check mortality'), ask for clarification on the time period or location before running massive queries.

{schema_content}

RESPONSE FORMAT:
- For simple stats: Give numbers with context
- For trends: Describe the pattern with key metrics (% change, direction, outliers)
- For complex analysis: Summarize findings, then offer details
- When generating charts: Describe what the chart shows before presenting it


EXAMPLE WORKFLOWS:
Question: "What's the trend of AMCHIS deliveries over the last 6 months?"
1. execute_sql_query to get delivery data by month
2. Use pandas to calculate month-over-month growth
3. Optionally create a line chart showing the trend

Question: "Is there a correlation between facility type and mortality rate?"
1. execute_sql_query to get encounters by facility type and outcome
2. Use pandas to calculate mortality rates per facility type
3. Use statistical test (chi-square, correlation) if appropriate
4. Present findings with specific numbers

Question: "Show me top diseases by LGA in a heatmap"
1. execute_sql_query to get disease counts by LGA
2. Use pandas pivot_table to reshape data
3. Create seaborn heatmap
4. Return the visualization

Question: "What is the total service utilized last month"
1. execute_sql_query to get total services and diseases last month
2. Return stat with context

"""
    execute_sql_query_declaration = {
            "type": "function",
            "name": "execute_sql_query",
            "description": "Execute sqlite3 query and return result",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type":  "string",
                        "description": "Read only SQL query to execute"
                    }
                },
                "required": ["query"]
            }

        }

    def __init__(self) -> None:
        self.client = genai.Client(api_key=Config.GOOGLE_GENAI_API_KEY)
        self.db = sqlite3.connect(f"file:{Config.DATABASE}?mode=ro", uri=True, check_same_thread=False)
        self.db.row_factory = sqlite3.Row

    def execute_sql_query(self, query: str):

        """
    Executes a read-only SQLite SELECT query against the ODCHC healthcare database.

    Use this tool to fetch raw data for analysis. Always prefer using the
    'master_encounter_view' if you can, for analytical queries as it contains pre-calculated
    Naira costs and flattened patient data.

    Args:
        query: A valid SQLite SELECT statement. The query must be read-only
               and start with the 'SELECT' keyword.

    Returns:
        A list of dictionaries where each dictionary represents a row from the
        database results. Returns an empty list if no results are found.
    """


        query = query.strip()
        if not query.lower().startswith("select"):
            raise ValidationError("Invalid SQL query")
        try:
            cursor = self.db.execute(query)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error:
            raise  QueryParameterError("Error while running sql");

    def generate_response(self,
                          user_input: str,
                          conversation_history: Optional[List[Dict]] = None):


        if conversation_history is None:
            conversation_history = []

        conversation_history.append({
            "role": "user",
            "parts": [
                {
                    "text": user_input
                }
            ]
        })

        tools = [self.execute_sql_query, {"code_execution": {}}]
        config = types.GenerateContentConfig(
            system_instruction=self.system_prompt,
            max_output_tokens=4096,
            temperature= 0.0,
            candidate_count=1,
            tools=[self.execute_sql_query, {"code_execution": {}}], # The SDK handles the types.Tool wrapping
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=False)
        )
        chat = self.client.chats.create(config = config,
                                 model = Config.GOOGLE_GENAI_MODEL,
                                 history = conversation_history)
        response = chat.send_message_stream(user_input)

        for res in response:
            yield res.text
