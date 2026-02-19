import os
import secrets

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DBNAME = 'odchc_encounter.db'
DBPATH = os.path.join(os.path.dirname(BASE_DIR), DBNAME)
# print(DBPATH)


class Config:
    SECRET_KEY = os.getenv(
        'SECRET_KEY') or "This is supposed to be a secret key"
    ADMIN_PAGE_PAGINATION = 15
    DATABASE = os.getenv('ODCHC_DATABASE') or DBPATH
    LLM_SCHEMA_PATH = os.getenv('LLM_SCHEMA_PATH')
    GOOGLE_GENAI_API_KEY = os.getenv('GOOGLE_GENAI_API_KEY')
    GOOGLE_GENAI_MODEL = 'gemini-2.5-flash-lite'
