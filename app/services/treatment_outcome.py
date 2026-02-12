import sqlite3
from app.db import get_db
from app.models import TreatmentOutcome
from app.exceptions import ValidationError

# Relative Import
from .base import BaseServices

class TreatmentOutcomeServices(BaseServices):
    table_name = 'treatment_outcome'
    columns = {'id', 'name', 'type'}
    model = TreatmentOutcome

    @classmethod
    def create_treatment_outcome(cls, name: str, treatment_type: str, commit: bool = True) -> TreatmentOutcome:
        db = get_db()
        try:
            cur = db.execute(
                f'INSERT INTO {cls.table_name} (name, type) VALUES (?, ?)', (name, treatment_type))
            if commit:
                db.commit()
            new_id = cur.lastrowid
            return cls.get_by_id(new_id)

        except sqlite3.IntegrityError:
            db.rollback()
            raise ValidationError(
                "Treatment Outcome already exist in the database")
