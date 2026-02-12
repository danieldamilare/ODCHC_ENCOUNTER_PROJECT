import sqlite3
from typing import Optional, Iterator

from app.db import get_db
from app.models import Service, ServiceCategory, ServiceView
from app.exceptions import MissingError, InvalidReferenceError, DuplicateError
from app.filter_parser import Params, FilterParser

from .base import BaseServices, _legacy_to_params

class ServiceServices(BaseServices):
    MODEL_ALIAS_MAP = {
        ServiceCategory: 'scg',
        Service:  'srv'
    }

    model = Service
    table_name = 'services'

    @classmethod
    def create_service(cls, name: str, category_id: int, commit=True) -> Service:
        db = get_db()
        try:
            ServiceCategoryServices.get_by_id(category_id)
        except MissingError:
            raise InvalidReferenceError('Service Category does not exist')
        try:
            cursor = db.execute(
                f'INSERT INTO {cls.table_name} (name, category_id) VALUES (?, ?)', [name, category_id])
            if commit:
                db.commit()
            new_id = cursor.lastrowid
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError(f'Service {name} already exists')

    @classmethod
    def get_all(cls,
                params: Optional[Params] = None,
                **kwargs
                ) -> Iterator:

        query = '''
        SELECT
            srv.id as service_id,
            srv.name as service_name,
            scg.id as category_id,
            scg.name as category_name
        FROM services as srv
        JOIN service_category as scg
        ON srv.category_id = scg.id'''

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
            category = ServiceCategory(row['category_id'],
                                       row['category_name'])
            service = ServiceView(id=row['service_id'],
                                  name=row['service_name'],
                                  category=category)
            yield service

    @staticmethod
    def update_service(service: Service):
        db = get_db()
        try:
            ServiceServices.update_data(service)
        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError("Service name already exist")
        return service


    @classmethod
    def delete_service(cls, service: Service):
        db = get_db()
        db.execute(f'DELETE FROM {cls.table_name} WHERE id = ?', (service.id,))
        db.commit()

class ServiceCategoryServices(BaseServices):
    model = ServiceCategory
    table_name = 'service_category'
    columns = {'id', 'name'}
    columns_to_update = {'name'}

    @classmethod
    def create_category(cls, category_name, commit=True) -> ServiceCategory:
        db = get_db()

        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (name) VALUES(?)',
                                (category_name, ))
            if commit:
                db.commit()
            new_id = cursor.lastrowid
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError(
                f"Category {category_name} already exist in Service database")
