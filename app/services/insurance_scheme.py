import sqlite3
from app.db import get_db
from app.models import InsuranceScheme
from app.constants import SchemeEnum
from app.exceptions import DuplicateError, MissingError

from .base import BaseServices

class InsuranceSchemeServices(BaseServices):
    table_name = 'insurance_scheme'
    model = InsuranceScheme
    columns_to_update = {'scheme_name'}
    columns = {'id', 'scheme_name'}

    @classmethod
    def create_scheme(cls, name: str, color_scheme: str, commit=True):
        db = get_db()
        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (scheme_name, color_scheme) VALUES (?, ?)',
                                (name, color_scheme))
            if commit:
                db.commit()
            new_id = cursor.lastrowid
            return cls.get_by_id(new_id)

        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError(
                "Insurance scheme already exists in the database")

    @classmethod
    def update_scheme(cls, scheme: InsuranceScheme):
        try:
            cls.update_data(scheme)
        except sqlite3.IntegrityError:
            get_db().rollback()
            raise DuplicateError(
                f"{scheme.scheme_name} already exists in database")

    @classmethod
    def get_scheme_by_enum(cls, scheme: SchemeEnum) -> InsuranceScheme:
        query = f'''SELECT * from {cls.table_name} where scheme_name = ?'''
        db = get_db()
        result = db.execute(query, (scheme.value, )).fetchone()
        if not result:
            raise MissingError(f"{scheme.value} not in insurance scheme")
        return cls._row_to_model(result, InsuranceScheme)
