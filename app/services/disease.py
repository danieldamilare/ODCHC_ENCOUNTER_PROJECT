import sqlite3
from typing import Optional, Iterator

from app.db import get_db
from app.models import Disease, DiseaseCategory, DiseaseView
from app.exceptions import MissingError, InvalidReferenceError, DuplicateError
from app.filter_parser import Params, FilterParser

from .base import BaseServices, _legacy_to_params

class DiseaseCategoryServices(BaseServices):
    model = DiseaseCategory
    table_name = 'diseases_category'
    columns = {'id', 'category_name'}
    columns_to_update = {'category_name'}

    @classmethod
    def create_category(cls, category_name, commit=True) -> DiseaseCategory:
        db = get_db()

        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (category_name) VALUES(?)',
                                (category_name, ))
            if commit:
                db.commit()
            new_id = cursor.lastrowid
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError(
                f"Category {category_name} already exist in database")

class DiseaseServices(BaseServices):
    table_name = 'diseases'
    model = Disease
    columns_to_update = {'name', 'category_id'}
    MODEL_ALIAS_MAP = {
        DiseaseCategory: 'dc',
        Disease:  'dis'
    }

    @classmethod
    def create_disease(cls, name: str, category_id: int, commit=True) -> Disease:
        db = get_db()
        try:
            DiseaseCategoryServices.get_by_id(category_id)
        except MissingError:
            raise InvalidReferenceError('Disease Category does not exist')
        try:
            cursor = db.execute(
                f'INSERT INTO {cls.table_name} (name, category_id) VALUES (?, ?)', [name, category_id])
            if commit:
                db.commit()
            new_id = cursor.lastrowid
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError(f'Disease {name} already exists')

    @classmethod
    def get_all(cls,
                params: Optional[Params] = None,
                **kwargs
                ) -> Iterator:

        query = '''
        SELECT
            dis.id as disease_id,
            dis.name as disease_name,
            dc.id as category_id,
            dc.category_name
        FROM diseases as dis
        JOIN diseases_category as dc
        ON dis.category_id = dc.id'''

        if params:
            res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        else:
            res = _legacy_to_params(**kwargs)

        query, args = cls._apply_filter(
            base_query=query,
            base_arg=[],
            **res
        )
        db = get_db()
        rows = db.execute(query, args)
        for row in rows:
            category = DiseaseCategory(row['category_id'],
                                       row['category_name'])
            disease = DiseaseView(id=row['disease_id'],
                                  name=row['disease_name'],
                                  category=category)
            yield disease

    @classmethod
    def delete_disease(cls, disease: Disease):
        db = get_db()
        db.execute(f'DELETE FROM {cls.table_name} WHERE id = ?', (disease.id,))
        db.commit()

    @classmethod
    def get_disease_by_name(cls, name: str) -> Disease:
        db = get_db()
        row = db.execute(
            f'SELECT * FROM {cls.table_name} WHERE name = ?', (name,)).fetchone()
        if row is None:
            raise MissingError('Disease does not Exist')
        return DiseaseServices._row_to_model(row, Disease)

    @staticmethod
    def update_disease(disease: Disease):
        db = get_db()
        try:
            DiseaseServices.update_data(disease)
        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError("Disease name already exist")
        return disease
