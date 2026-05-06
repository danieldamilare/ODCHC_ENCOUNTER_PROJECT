from .base import BaseServices
from enum import Enum, auto
from app.db import get_db
from app.models import BotSession

class BotState(Enum):
    START  = auto(),
    REQUESTING_USERNAME = auto(),
    REQUESTING_PASSWORD = auto(),
    LOGGED_IN = auto(),
    REQUESTING_REFERRAL = auto(),
    REFERRAL_POLICY = auto(),
    REFERRAL_fACILITY = auto(),
    REFERRAL_REASON = auto(),
    REFERRAL_CONFIRMATION = auto(),
    REFERRAL_SELECT_LGA = auto(),
    REFERRAL_SELECT_FACILITY = auto(),
    IDLE = auto()

class BotServices(BaseServices):
    table_name = "bot_sessions"
    columns = {'id', 'phone_number', 'state', 'context_data'}
    model = BotSession
    message = {
        BotState.START: "Welcome to the ODCHC Referral Portal! Please Click the Login button to get started.",
        BotState.REQUESTING_USERNAME: "Please enter your username to log in.",
        BotState.REQUESTING_PASSWORD: "Please enter your password.",
        BotState.REFERRAL_SELECT_LGA: "Please Enter Local Government of the facility you are referring to",
        BotState.REFERRAL_SELECT_FACILITY: "Please Enter the facility you are referring to",
    }

    @classmethod
    def get_session(cls, phone_number: str):
        db = get_db()
        query = """SELECT * FROM bot_sessions WHERE phone_number = ?"""
        cur = db.execute(query, (phone_number,))
        row = cur.fetchone()
        if not row:
            query = """
            INSERT INTO bot_sessions (phone_number, state) VALUES
            (?, ?)
            """
            cursor = db.execute(query, (phone_number, BotState.START))
            db.commit()
            new_id = cursor.lastrowid
            return cls.get_by_id(new_id)
        else:
            return cls._row_to_model(row)

    @classmethod
    def update_session (cls, session: BotSession):
