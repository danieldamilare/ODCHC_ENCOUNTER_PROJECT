from datetime import timedelta
from datetime import datetime, date
from collections import defaultdict
import sqlite3
import pandas as pd
from werkzeug.security import generate_password_hash, check_password_hash
from typing import Optional, Type, TypeVar, Iterator, List, Tuple, Dict, Literal
from app.db import get_db
from app.models import (User, Facility, Disease, Encounter, TreatmentOutcome, DiseaseCategory, Role,
            InsuranceScheme, FacilityView, UserView, EncounterView, DiseaseView, FacilityScheme,
            EncounterDiseases, ServiceCategory, ServiceView, ANCEncounterView, DeliveryEncounterView,
            DeliveryBaby, ANCRegistry, DeliveryEncounter, ChildHealth, ChildHealthEncounterView, Service
            )
from app.exceptions import MissingError, InvalidReferenceError, DuplicateError, QueryParameterError
from app.exceptions import ValidationError, AuthenticationError, ServiceError
from app import app
from app.constants import ONDO_LGAS_LOWER, DeliveryMode, EncType, BabyOutcome, SchemeEnum, AgeGroup
from copy import copy
from app.filter_parser import FilterParser, Params
from dateutil.relativedelta import relativedelta

def _legacy_to_params(**kwargs) -> Dict:
    res = {}
    if 'and_filter' in kwargs:
        res['and_filter'] = kwargs['and_filter']
    if 'or_filter' in kwargs:
        res['or_filter'] = kwargs['or_filter']
    if 'group_by' in kwargs:
        res['group_by'] = kwargs['group_by']
    if 'order_by' in kwargs:
        res['order_by'] = kwargs['order_by']
    if 'limit' in kwargs:
        res['limit'] = kwargs['limit']
    if 'offset' in kwargs:
        res['offset'] = kwargs['offset']
    return res

T = TypeVar('T')
class BaseServices:
    model: Type[T] = None
    table_name = ''
    columns_to_update: set = set()
    MODEL_ALIAS_MAP = {}

    @staticmethod
    def _row_to_model(row, model_cls: Type[T]) -> T:
        if row is None:
            raise MissingError("Invalid Row Data")
        return model_cls(**row)

    @classmethod
    def get_by_id(cls, id: int) -> object:
        db = get_db()
        row = db.execute(
            f'SELECT * FROM {cls.table_name} WHERE id = ?', (id,)).fetchone()
        if row is None:
            raise MissingError(
                f"{cls.model.get_name()} not found in the database")
        return cls._row_to_model(row, cls.model)

    @classmethod
    def list_row_by_page(cls,
                         page: int,
                         params: Optional[Params] = None,
                         **kwargs,
                         ) -> Iterator:
        if page < 1:
            raise ValidationError("Page number must be >= 1")
        limit = params.limit if params and params.limit > 0 else app.config['ADMIN_PAGE_PAGINATION']
        if limit < 0:
            raise ValidationError("Invalid Page limit")

        offset = (page - 1) * limit
        params = Params() if params is None else params
        params = params.set_limit(limit).set_offset(offset)
        return cls.get_all(params=params)

    @classmethod
    def update_data(cls, model: Type[T]) -> T:
        db = get_db()
        field = [f"{key}=?" for key in vars(
            model).keys() if key in cls.columns_to_update]
        values = [v for k, v in vars(
            model).items() if k in cls.columns_to_update]

        db.execute(
            f'UPDATE {cls.table_name} SET {",".join(field)} WHERE id = ?', values + [model.id])
        db.commit()
        return model

    @classmethod
    def get_total(cls,
                  params: Optional[Params] = None,
                  **kwargs) -> int:

        query = f'SELECT COUNT(*) from {cls.table_name}'
        res ={}
        if params is not None:
            if params.group_by or params.order_by:
                raise QueryParameterError("You can't groupby or order by to get_total")

            mapper = {cls.model: cls.table_name}
            res = FilterParser.parse_params(params, model_map=mapper)
        else:
            res = _legacy_to_params(**kwargs)

        query, args = cls._apply_filter(
            base_query=query,
            base_arg=[],
            **res
        )
        db = get_db()
        res =db.execute(query, args).fetchone()
        if res:
            return res[0]
        else:
            return 0

    @classmethod
    def _apply_filter(cls,
                      base_query: str,
                      base_arg: Optional[List] = None,
                      limit: int = 0,
                      offset: int = 0,
                      and_filter: Optional[List[Tuple]] = None,
                      or_filter: Optional[List[Tuple]] = None,
                      order_by: Optional[List[Tuple[str, str]]] = None,
                      group_by: Optional[List[str]] = None
                      ):
        ALLOWED_OPERATORS = {'=', '>', '<', '>=', '<=', '!=', 'LIKE', 'IN'}
        query = ''
        args = base_arg if base_arg is not None else []
        conditions = []
        query = base_query

        if and_filter:
            for column_name, value, opt in and_filter:
                if opt.upper() not in ALLOWED_OPERATORS:
                    raise ValidationError(f"Invalid operator: {opt}")
                conditions.append(f"{column_name} {opt} ?")
                args.append(value)

        if or_filter:
            or_conditions = []
            for column_name, value, opt in or_filter:
                if opt.upper() not in ALLOWED_OPERATORS:
                    raise ValidationError(f"Invalid operator: {opt}")
                or_conditions.append(f"{column_name} {opt} ?")
                args.append(value)
            if or_conditions:
                conditions.append("(" + " OR ".join(or_conditions) + ")")

        where = ' WHERE ' if not 'WHERE' in base_query.upper() else ' AND '

        if conditions:
            query += where + " AND ".join(conditions)

        # --- GROUP BY Clause ---
        if group_by:
            query += f" GROUP BY {', '.join(group_by)}"
        # --- ORDER BY Clause ---
        if order_by:
            clause = []
            for col, direction in order_by:
                if direction.upper() not in ['ASC', 'DESC']:
                    raise ValidationError(
                        f"Invalid sort direction: {direction}")
                clause.append(f"{col} {direction.upper()}")
            query += f" ORDER BY {','.join(clause)}"

        if limit > 0:
            query += ' LIMIT ?'
            args.append(limit)

        if offset > 0:
            query += ' OFFSET ?'
            args.append(offset)

        return query, args

    @classmethod
    def get_all(cls,
                params: Optional[Params] = None,
                **kwargs
                ) -> Iterator:

        res = {}
        if params:
            model_map = {cls.model: cls.table_name}
            res = FilterParser.parse_params(params, model_map=model_map)
        else:
            res = _legacy_to_params(**kwargs)
        query = f"SELECT * from {cls.table_name}"
        query, args = cls._apply_filter(query,
                                        **res)
        db = get_db()
        rows = db.execute(query, args)
        for row in rows:
            yield cls._row_to_model(row, cls.model)

    @classmethod
    def has_next_page(cls, page: int,
                      page_count = app.config['ADMIN_PAGE_PAGINATION'],
                      params: Optional[Params] =  None,
                      **kwargs
                      ) -> bool:
        res = {}
        if params:
            total = cls.get_total(params)
        else:
            total = cls.get_total(**kwargs)

        current = page * page_count
        if current < total:
            return True
        return False

class UserServices(BaseServices):
    model = User
    table_name = 'users'
    columns_to_update = {'username', 'facility_id', 'role'}
    MODEL_ALIAS_MAP = {
        User: 'u',
        Facility: 'fc'
    }

    @classmethod
    def create_user(cls, username: str, facility_id: Optional[int], password: str, role=None, commit=True) -> User:
        password_hash = generate_password_hash(password)
        role = ('admin' if role == Role.admin else 'user')
        if role != 'admin':
            if not facility_id:
                raise ValidationError("User must have a facility")
            try:
                FacilityServices.get_by_id(facility_id)
            except MissingError:
                raise InvalidReferenceError(
                    "You can't attach user to a facility that does not exists")

        db = get_db()
        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name}(username, password_hash, facility_id, role)'
                                ' VALUES (?, ?, ?, ?)', (username, password_hash,
                                                         facility_id, role))
            if commit:
                db.commit()
            new_id: int = cursor.lastrowid
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError(
                'Username exists! Please use another username')

    @classmethod
    def get_user_by_username(cls, username: str) -> User:
        db = get_db()
        row = db.execute(
            f'SELECT * FROM {cls.table_name} where username = ?', [username]).fetchone()

        if row is None:
            raise MissingError("Username does not exist")
        row = dict(row)
        try:
            row['role'] = Role[row['role']]
        except KeyError:
            row['role'] = Role.user
        return UserServices._row_to_model(row, User)

    @classmethod
    def get_by_id(cls, id: int) -> User:
        db = get_db()
        try:
            row = db.execute(
                f'SELECT * FROM {cls.table_name} where id = ?', (id,)).fetchone()
        except sqlite3.IntegrityError:
            db.rollback()
            raise MissingError("User is not in the database")
        if row is None:
            raise MissingError("Username does not exist")
        row = dict(row)
        try:
            row['role'] = Role[row['role']]
        except KeyError:
            row['role'] = Role.user
        return UserServices._row_to_model(row, User)

    @classmethod
    def get_all(cls,
                params: Optional[Params] = None,
                **kwargs
                ) -> Iterator:

        db = get_db()

        query = '''
            SELECT
                u.id AS user_id,
                u.facility_id,
                fc.name,
                fc.local_government,
                fc.facility_type,
                u.username,
                u.password_hash,
                u.role AS role
            FROM users AS u
            LEFT JOIN facility AS fc ON u.facility_id = fc.id
        '''
        res = {}
        if params:
            res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        else:
            res = _legacy_to_params(**kwargs)

        query, args = cls._apply_filter(
            base_query=query,
            base_arg=[],
            **res
        )

        rows = db.execute(query, args).fetchall()
        facility_ids = [row['facility_id'] for row in rows]
        scheme_map = {}
        if facility_ids:
            scheme_map = FacilityServices.get_insurance_list(facility_ids)

        for row in rows:
            facility_view = None
            if row['facility_id'] is not None:
                facility_view = FacilityView(
                    id=row['facility_id'],
                    name=row['name'],
                    lga=row['local_government'],
                    facility_type=row['facility_type'],
                    scheme=scheme_map[row['facility_id']]
                )
            yield UserView(
                id=row['user_id'],
                facility=facility_view,
                username=row['username'],
                role=Role[row['role']],
            )

    @classmethod
    def get_view_by_id(cls, id: int):
        and_filter = [('u.id', id, '=')]
        try:
            return next(cls.get_all(and_filter=and_filter))
        except StopIteration as e:
            raise MissingError("User does not exist")

    @staticmethod
    def get_verified_user(username: str, password: str):
        try:
            user = UserServices.get_user_by_username(username)
        except MissingError:
            raise AuthenticationError("Invalid Username or Password")

        if not check_password_hash(user.password_hash, password):
            raise AuthenticationError("Invalid Username or Password")
        return UserServices.get_view_by_id(user.id)

    @classmethod
    def update_data(cls, model: User) -> User:
        db = get_db()
        field = []
        values = []
        for key, value in vars(model).items():
            if key in cls.columns_to_update:
                field.append(f'{key}=?')
                if key == 'role':
                    values.append('admin' if value == Role.admin else 'user')
                else:
                    values.append(value)
        try:
            db.execute(
                f'UPDATE {cls.table_name} SET {",".join(field)} WHERE id = ?', values + [model.id])
            db.commit()
        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError(
                "You cannot a new user with the same username as another user")
        return model

    @classmethod
    def update_user(cls, user: User) -> User:
        return cls.update_data(user)

    @classmethod
    def update_user_password(cls, user: User, password: str) -> User:
        db = get_db()
        password_hash = generate_password_hash(password)
        user.password_hash = password_hash
        db.execute(
            f'UPDATE {cls.table_name} SET password_hash = ? WHERE id = ?', (password_hash, user.id))
        db.commit()
        return user

    @classmethod
    def delete_user(cls, user: User):
        db = get_db()
        db.execute(f"DELETE FROM {cls.table_name} where id = ?", [user.id])
        db.commit()


class FacilityServices(BaseServices):
    table_name = 'facility'
    model = Facility
    LOCAL_GOVERNMENT = ONDO_LGAS_LOWER
    columns_to_update = {'name', 'local_government', 'facility_type'}
    MODEL_ALIAS_MAP = {
        Facility: 'fc',
        FacilityScheme: 'fsc'
    }

    @classmethod
    def create_facility(cls, name: str, local_government: str,
                        facility_type: str, scheme: List[int],
                        commit=True) -> Facility:
        db = get_db()
        if local_government.lower() not in FacilityServices.LOCAL_GOVERNMENT:
            raise ValidationError("Local Government does not exist in Akure")
        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (name, local_government, facility_type) VALUES (?, ?, ?)', (
                name, local_government, facility_type))
            new_id = cursor.lastrowid
            if scheme:
                scheme_list = list(set((new_id, x) for x in scheme))
                cls.add_scheme(scheme_list)
            if commit:
                db.commit()
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError(f"Facility {name} already exist in database")

    @classmethod
    def add_scheme(cls, scheme_list: List[Tuple[int, int]]):
        db = get_db()
        db.executemany('INSERT INTO facility_scheme (facility_id, scheme_id) VALUES (?, ?)',
                       scheme_list)

    @classmethod
    def get_current_scheme(cls, facility_id: int):
        db = get_db()
        cur = db.execute(
            'SELECT scheme_id from facility_scheme WHERE facility_id = ?', [facility_id])
        return [row['scheme_id'] for row in cur]

    @classmethod
    def get_facility_by_name(cls, name: str) -> Facility:
        db = get_db()
        row = db.execute(
            f'SELECT * FROM {cls.table_name} WHERE name = ?', (name,)).fetchone()
        if row is None:
            raise MissingError(f"No Facility with name {name}")
        return FacilityServices._row_to_model(row, Facility)

    @classmethod
    def delete_facility(cls, facility: Facility):
        db = get_db()
        db.execute(
            f"DELETE FROM {cls.table_name} WHERE id = ?",  [facility.id])
        db.commit()

    @classmethod
    def update_facility(cls, facility: Facility, scheme: List[int]):
        if facility.local_government.lower() not in FacilityServices.LOCAL_GOVERNMENT:
            raise ValidationError("Local Government does not exist in Akure")
        db = get_db()
        try:
            db.execute(
                'DELETE FROM facility_scheme WHERE facility_id = ?', (facility.id, ))
            if scheme:
                scheme_list = [(facility.id, sc) for sc in scheme]
                cls.add_scheme(scheme_list)

            # update data does commit
            FacilityServices.update_data(facility)
        except DuplicateError:
            db.rollback()
            raise DuplicateError(
                f'Facility with name {facility.name} already exists')
        return facility

    @classmethod  # facility service
    def get_insurance_list(cls, row_ids: List[int]):
        db = get_db()
        placeholders = ','.join(('?' * len(row_ids)))
        query = f'''
        SELECT
            fsc.facility_id as id,
            isc.id as scheme_id,
            isc.scheme_name,
            isc.color_scheme
        FROM facility_scheme as fsc
        JOIN insurance_scheme as isc
        ON isc.id = fsc.scheme_id
        WHERE fsc.facility_id IN ({placeholders})
        '''

        scheme_rows = db.execute(query, row_ids).fetchall()
        scheme_map = defaultdict(list)
        for row in scheme_rows:
            scheme_map[row['id']].append(InsuranceScheme(id=row['scheme_id'],
                                                         scheme_name=row['scheme_name'],
                                                         color_scheme=row['color_scheme']))
        return scheme_map

    @classmethod
    def get_all(cls,
                params: Optional[Params] = None,
                **kwargs
                ) -> Iterator:

        query = f'''
        SELECT
            fc.id,
            fc.name as facility_name,
            fc.local_government,
            fc.facility_type
        FROM {cls.table_name} as fc
        JOIN facility_scheme as fsc on fsc.facility_id = fc.id
        '''
        res = {}
        if params:
            res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)

        query, args = cls._apply_filter(
            base_query=query,
            **res
        )

        db = get_db()
        facility_rows = list(db.execute(query, args).fetchall())
        row_ids = list(set(row['id'] for row in facility_rows))
        scheme_map = cls.get_insurance_list(row_ids)
        done = set()
        for row in facility_rows:
            if row['id'] in done:
                continue
            done.add(row['id'])
            facility = FacilityView(
                id=row['id'],
                lga=row['local_government'],
                scheme=scheme_map[row['id']],
                name=row['facility_name'],
                facility_type=row['facility_type']
            )
            yield facility

    @classmethod
    def get_view_by_id(cls, facility_id: int) -> FacilityView:
        try:
            return next(cls.get_all(and_filter=[('fc.id', facility_id, '=')]))
        except StopIteration as e:
            raise MissingError("Facility is invalid and does not exist in database")


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
            db.rollback()
            raise DuplicateError(
                f"{scheme.scheme_name} already exists in database")

    @classmethod
    def get_scheme_by_enum(cls, scheme: SchemeEnum) -> InsuranceScheme:
        query = f'''SELECT * from {cls.table_name} where scheme_name = ?'''
        print(query)
        db = get_db()
        # print(scheme, scheme.value)
        result = db.execute(query, (scheme.value, )).fetchone()
        if not result:
            raise MissingError(f"{scheme.value} not in insurance scheme")
        return cls._row_to_model(result, InsuranceScheme)


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

        try:
            DiseaseServices.update_data(disease)
        except DuplicateError:
            db.rollback()
            raise DuplicateError("Disease name already exist")
        return disease

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

        try:
            ServiceServices.update_data(service)
        except DuplicateError:
            db.rollback()
            raise DuplicateError("Disease name already exist")
        return service


    @classmethod
    def delete_service(cls, service: Service):
        db = get_db()
        db.execute(f'DELETE FROM {cls.table_name} WHERE id = ?', (service.id,))
        db.commit()

class ServiceCategoryServices(BaseServices):
    model = ServiceCategory
    table_name = 'service_category'
    columns = {'id', 'category_name'}
    columns_to_update = {'name'}

    @classmethod
    def create_category(cls, category_name, commit=True) -> DiseaseCategory:
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

class EncounterServices(BaseServices):
    table_name = 'encounters'
    model = Encounter
    columns = {'id', 'facility_id', 'date', 'policy_number', 'client_name',
               'gender', 'age', 'age_group', 'treatment', 'referral', 'doctor_name',
               'created_by', 'created_at', "scheme", "outcome"
               }

    MODEL_ALIAS_MAP = {Encounter: 'ec',
         Facility: 'fc',
         TreatmentOutcome: 'tc',
         InsuranceScheme: 'isc',
         EncounterDiseases: 'eds'}

    @classmethod
    def get_total(cls,
                  params: Optional[Params] = None,
                  **kwargs) -> int:

        query = f'''
            SELECT COUNT(*) from {cls.table_name} as ec
            JOIN insurance_scheme as isc on isc.id = ec.scheme
            JOIN facility as fc on ec.facility_id = fc.id
            JOIN treatment_outcome as tc on ec.outcome = tc.id
            LEFT JOIN users AS u ON ec.created_by = u.id
        '''

        res ={}
        if params is not None:
            if params.group_by or params.order_by:
                raise QueryParameterError("You can't groupby or order by to get_total")

            mapper = cls.MODEL_ALIAS_MAP
            res = FilterParser.parse_params(params, model_map=mapper)
        else:
            res = _legacy_to_params(**kwargs)

        query, args = cls._apply_filter(
            base_query=query,
            base_arg=[],
            **res
        )
        # print(query)
        db = get_db()

        res =db.execute(query, args).fetchone()
        if res:
            return res[0]
        else:
            return 0

    @classmethod
    def create_encounter(cls, facility_id: int,
                         date: date,
                         policy_number: str,
                         client_name: str,
                         gender: str,
                         age: int,
                         treatment: Optional[str],
                         doctor_name: Optional[str],
                         scheme: int,
                         nin: str,
                         phone_number: str,
                         outcome: int,
                         created_by: int,
                         diseases_id: Optional[List[int]] = None,
                         enc_type: EncType = EncType.GENERAL,
                         services_id: Optional[List[int]] = None,
                         commit: bool=True) -> Encounter:
        db = get_db()
        gender = gender.upper().strip()
        policy_number = policy_number.strip()
        client_name = client_name.strip()
        treatment = treatment.strip() if treatment else treatment
        doctor_name = doctor_name .strip() if doctor_name else doctor_name
        nin = nin.strip()
        phone_number = phone_number.strip()
        created_at = datetime.now().date()

        try:

            cur = db.execute(f'''INSERT INTO {cls.table_name} (facility_id, date, policy_number
                   , client_name, gender, age, treatment, doctor_name, nin, phone_number, enc_type,
                   scheme, outcome, created_by, created_at) VALUES( ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?)''', (facility_id, date, policy_number, client_name,
                                          gender, age, treatment, doctor_name, nin, phone_number,
                                          enc_type.value, scheme, outcome, created_by, created_at))

            new_id = cur.lastrowid
            if diseases_id:
                diseases_list = list(set((new_id, x) for x in diseases_id))
                db.executemany('''INSERT into encounters_diseases(encounter_id, disease_id)
                            VALUES(?, ?)''', diseases_list)
            if services_id:
                services_list = list(set((new_id, x) for x in services_id))
                db.executemany('''INSERT into encounters_services(encounter_id, service_id)
                               VALUES(?, ?)''', services_list)
            if commit:
                db.commit()
            return cls.get_by_id(new_id)

        except sqlite3.IntegrityError as e:
            db.rollback()
            error_msg = str(e).lower()
            if 'foreign key' in error_msg:
                raise InvalidReferenceError(
                    "Invalid reference to facility, disease, or user"
                )
            elif 'check constraint' in error_msg:
                raise ValidationError(
                    "Data validation failed: check age and gender values"
                )
            else:
                raise InvalidReferenceError(f"Database error: {str(e)}")

    @classmethod
    def create_delivery_encounter(cls,
                                facility_id: int,
                                date: date,
                                policy_number: str,
                                client_name: str,
                                gender: str,
                                age: int,
                                treatment: Optional[str],
                                doctor_name: Optional[str],
                                scheme: int,
                                nin: str,
                                phone_number: str,
                                created_by: int,
                                anc_id: int,
                                anc_count: int,
                                mode_of_delivery: DeliveryMode,
                                mother_outcome: int,
                                baby_details: List[Dict],
                                commit: bool = True
                                  ):

        db = get_db()
        try:
            new_enc = cls.create_encounter(
                facility_id = facility_id,
                date = date,
                policy_number = policy_number,
                client_name = client_name,
                gender = gender,
                age =  age,
                treatment= treatment,
                doctor_name = doctor_name,
                scheme = scheme,
                nin = nin,
                phone_number = phone_number,
                enc_type  = EncType.DELIVERY,
                outcome = mother_outcome,
                created_by = created_by,
                commit = False
            )
            cur = db.execute('''
            INSERT INTO delivery_encounters(anc_id, encounter_id, anc_count,
                              mode_of_delivery)
            VALUES(?, ?, ?, ?)''',

            (anc_id, new_enc.id, anc_count, mode_of_delivery.value))
            identifier = 'cesarean' if (mode_of_delivery == DeliveryMode.CS) else "delivery"
            service_id = db.execute('''SELECT id from services WHERE LOWER(name) LIKE ?''', (f'%{identifier}%',)).fetchone()
            if not service_id:
                raise MissingError(f"Service {identifier} not found")

            db.execute("INSERT INTO encounters_services(encounter_id, service_id) VALUES(?, ?)", (new_enc.id, service_id['id']))

            db.execute('''
            UPDATE anc_registry SET status = "inactive" WHERE id = ?
            ''', (anc_id,))

            baby_list = [(new_enc.id, baby['gender'], baby['outcome']) for baby in baby_details]
            # print(baby_list)

            db.executemany("""INSERT INTO delivery_babies(encounter_id, gender, outcome)
                           VALUES (?, ?, ?)""", baby_list)
            if commit:
                db.commit()
            return new_enc
        except Exception as e:
            db.rollback()
            raise ServiceError(f"Failed to create delivery encounter: {str(e)}")

    @classmethod
    def create_anc_encounter(cls,
                             lmp: date,
                             policy_number: str,
                             kia_date: date,
                             client_name: str,
                             booking_date: date,
                             parity: int,
                             place_of_issue: str,
                             address: str,
                             date: date,
                             hospital_number: str,
                             expected_delivery_date: date,
                             anc_count: int,
                             facility_id: int,
                             gender: Literal["M", "F"],
                             age: int,
                             scheme: int,
                             nin: str,
                             phone_number: str,
                             doctor_name: str,
                             outcome: int,
                             created_by: int,
                             treatment: Optional[str],
                             commit: bool = True
                             ):
        db = get_db()
        try:
            new_enc = cls.create_encounter(
                facility_id = facility_id,
                date = date,
                policy_number = policy_number,
                client_name = client_name,
                gender = gender,
                age= age,
                treatment= treatment,
                doctor_name = doctor_name,
                scheme = scheme,
                nin = nin,
                phone_number= phone_number,
                enc_type = EncType.ANC,
                outcome = outcome,
                created_by= created_by,
                commit = False
            )
            anc_service = db.execute('''SELECT id from services where LOWER(name) LIKE ?''', ('%antenatal%',)).fetchone()
            if not anc_service:
                raise MissingError("ANC service not found in services table")
            db.execute('''
            INSERT INTO encounters_services(encounter_id, service_id) VALUES(?, ?)''', (new_enc.id, anc_service['id']))

            existing = db.execute(
                "SELECT id FROM anc_registry where orin = ? AND status = 'active'",
                (policy_number, )).fetchone()
            anc_id = None;
            if existing:
                db.execute("UPDATE anc_registry SET anc_count = anc_count+1 WHERE id = ?",
                           (existing['id'], ))
                anc_id = existing['id']
            else:
                cur = db.execute(
                '''INSERT INTO anc_registry(orin, kia_date, client_name,
                booking_date, parity, place_of_issue, hospital_number, address, lmp,
                expected_delivery_date, anc_count, status, nin, phone_number) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (policy_number, kia_date, client_name, booking_date, parity,
                place_of_issue, hospital_number, address, lmp, expected_delivery_date,
                anc_count, "active", nin, phone_number))
                anc_id = cur.lastrowid

            new_id = new_enc.id

            db.execute('''INSERT INTO anc_encounters(encounter_id, anc_id, anc_count) VALUES (
                       ?, ?, ?)''', (new_enc.id, anc_id, anc_count))
            if commit:
                db.commit()
            return new_enc
        except Exception as e:
            db.rollback()
            raise ServiceError(f"Failed to create ANC encounter: {str(e)}")

    @classmethod
    def create_child_health_encounter(cls,
                         facility_id: int,
                         date: date,
                         policy_number: str,
                         client_name: str,
                         gender: str,
                         age: int,
                         treatment: Optional[str],
                         doctor_name: Optional[str],
                         scheme: int,
                         nin: str,
                         phone_number: str,
                         outcome: int,
                         created_by: int,
                         address: str,
                         guardian_name: str,
                         dob: date,
                         diseases_id: Optional[List[int]] = None,
                         services_id: Optional[List[int]] = None,
                         commit: bool= True):

        db = get_db()
        try:
            new_enc = cls.create_encounter(
                facility_id= facility_id,
                date = date,
                policy_number= policy_number,
                client_name = client_name,
                gender = gender,
                age = age,
                treatment = treatment,
                doctor_name = doctor_name,
                scheme = scheme,
                nin = nin,
                phone_number = phone_number,
                enc_type = EncType.CHILDHEALTH,
                services_id= services_id,
                diseases_id = diseases_id,
                created_by = created_by,
                outcome=outcome,
                commit=False)
            query = '''
            INSERT INTO child_health_encounters(encounter_id, orin, dob, address, guardian_name)
            VALUES(?, ?, ?, ?, ?)'''
            db.execute(query, (new_enc.id, policy_number, dob, address, guardian_name))
            return new_enc
        except Exception as e:
            db.rollback()
            raise ServiceError(f"Failed to create child health encounter: {str(e)}")

    @classmethod
    def get_anc_record_by_registry(cls, orin: str) -> ANCRegistry:
        orin = orin.strip()
        query = '''
        SELECT * FROM anc_registry
        WHERE orin = ? AND status = 'active'
        '''
        db = get_db()
        row = db.execute(query, (orin,)).fetchone()
        if not row:
            raise MissingError("Can't find record from registry")
        return ANCRegistry(
            id = row['id'],
            orin = row['orin'],
            kia_date = row['kia_date'],
            booking_date= row['booking_date'],
            client_name = row['client_name'],
            parity = row['parity'],
            place_of_issue= row['place_of_issue'],
            hospital_number= row['hospital_number'],
            address = row['address'],
            lmp = row['lmp'],
            nin = row['nin'],
            phone_number= row['phone_number'],
            expected_delivery_date= row['expected_delivery_date'],
            anc_count= row['anc_count'],
            status= row['status']
        )

    @classmethod
    def _get_base_encounter(cls,
                params: Optional[Params] = None,
                **kwargs
                ):
        query = '''
            SELECT
                ec.id,
                ec.client_name,
                ec.gender,
                ec.age,
                ec.enc_type,
                ec.nin,
                ec.phone_number,
                ec.policy_number,
                ec.date,
                ec.facility_id ,
                isc.id as scheme_id,
                isc.scheme_name,
                isc.color_scheme,
                ec.doctor_name,
                tc.name as treatment_outcome,
                tc.type as treatment_type,
                tc.id as treatment_id,
                ec.created_at,
                ec.treatment,
                fc.name as facility_name,
                fc.facility_type,
                fc.local_government as lga,
                u.username AS created_by
            FROM encounters AS ec
            JOIN insurance_scheme as isc on isc.id = ec.scheme
            JOIN facility as fc on ec.facility_id = fc.id
            JOIN treatment_outcome as tc on ec.outcome = tc.id
            LEFT JOIN users AS u ON ec.created_by = u.id
        '''   #polymorphism for all encounters

        if params:
            res =FilterParser.parse_params(params,cls.MODEL_ALIAS_MAP)
        else:
            res = _legacy_to_params(**kwargs)

        query, args = cls._apply_filter(
            base_query=query,
            base_arg=[],
            **res
        )
        # print(query)

        db = get_db()
        return db.execute(query, args).fetchall()

    @classmethod
    def _get_diseases_mapping(cls, encounter_ids: List) -> Dict:
        if not encounter_ids:
            return {}

        db = get_db()

        placeholders = ','.join('?' * len(encounter_ids))
        diseases_query = f'''
            SELECT
                ecd.encounter_id,
                dis.id AS disease_id,
                dis.name AS disease_name,
                cg.id AS category_id,
                cg.category_name
            FROM encounters_diseases AS ecd
            JOIN diseases AS dis ON ecd.disease_id = dis.id
            JOIN diseases_category AS cg ON dis.category_id = cg.id
            WHERE ecd.encounter_id IN ({placeholders})
            ORDER BY ecd.encounter_id
        '''
        diseases_rows = db.execute(diseases_query, encounter_ids).fetchall()
        diseases_by_encounter = defaultdict(list)

        for row in diseases_rows:
            encounter_id = row['encounter_id']
            category = DiseaseCategory(
                id=row['category_id'],
                category_name=row['category_name']
            )
            disease = DiseaseView(
                id=row['disease_id'],
                name=row['disease_name'],
                category=category
            )
            diseases_by_encounter[encounter_id].append(disease)
        return diseases_by_encounter

    @classmethod
    def _get_services_mapping(cls, encounter_ids: List)-> Dict:
        if not encounter_ids:
            return {}

        db = get_db()
        placeholders = ', '.join('?' * len(encounter_ids))
        services_query = f'''
            SELECT
                ecs.encounter_id,
                srv.id as service_id,
                srv.name as service_name,
                scg.id as category_id,
                scg.name as category_name
            FROM encounters_services as ecs
            JOIN services as srv on srv.id = ecs.service_id
            JOIN service_category as scg on scg.id = srv.category_id
            WHERE ecs.encounter_id in ({placeholders})
        '''

        service_rows = db.execute(services_query, encounter_ids).fetchall()
        services_by_encounter = defaultdict(list)
        for row in service_rows:
            encounter_id = row['encounter_id']
            category = ServiceCategory(
                id = row['category_id'],
                name = row['category_name']
            )
            service = ServiceView(
                id = row['service_id'],
                name = row['service_name'],
                category = category
            )
            services_by_encounter[encounter_id].append(service)
        return services_by_encounter

    @classmethod
    def _get_babies_mapping(cls, delivery_ids: List) -> Dict:
        if not delivery_ids:
            return {}

        placeholders = ', '.join('?' * len(delivery_ids))
        db = get_db()
        babies_query = f'''
            SELECT
                db.id as baby_id,
                db.encounter_id as encounter_id,
                db.gender,
                db.outcome
            FROM delivery_babies as db WHERE db.encounter_id in ({placeholders})
        '''
        babies_row = db.execute(babies_query, delivery_ids).fetchall()
        babies_list = defaultdict(list)

        for row in babies_row:
            encounter_id = row['encounter_id']
            babies_list[encounter_id].append(DeliveryBaby(
                id = row['baby_id'],
                gender = row['gender'],
                outcome = BabyOutcome(row['outcome'])
            ))
        return babies_list

    @classmethod
    def _get_anc_mapping(cls, anc_ids: List) -> Dict[int, ANCRegistry]:
        if not anc_ids:
            return {}

        placeholders = ','.join('?' * len(anc_ids))
        query = f'''
        SELECT
            ae.encounter_id,
            ar.id as anc_id,
            ar.kia_date,
            ar.orin,
            ar.booking_date,
            ar.parity,
            ar.place_of_issue,
            ar.hospital_number,
            ar.client_name,
            ar.address,
            ar.lmp,
            ar.nin,
            ar.phone_number,
            ar.expected_delivery_date as edd,
            ae.anc_count,
            ar.status as anc_status
        FROM anc_encounters as ae
        JOIN anc_registry as ar on ar.id = ae.anc_id
        WHERE ae.encounter_id IN ({placeholders})
        '''
        db = get_db()
        rows = db.execute(query, anc_ids).fetchall()
        mapping = {}
        for row in rows:
            encounter_id = row['encounter_id']

            anc_encounter = ANCRegistry(
                id = row['anc_id'],
                orin = row['orin'],
                kia_date = row['kia_date'],
                client_name= row['client_name'],
                booking_date = row['booking_date'],
                parity= row['parity'],
                place_of_issue= row['place_of_issue'],
                hospital_number= row['hospital_number'],
                address= row['address'],
                lmp = row['lmp'],
                nin = row['nin'],
                phone_number = row['phone_number'],
                expected_delivery_date= row['edd'],
                anc_count= row['anc_count'],
                status = row['anc_status']
            )
            mapping[encounter_id] = anc_encounter
        return mapping

    @classmethod
    def _get_delivery_mapping(cls, delivery_ids: List) -> Dict[int, DeliveryEncounter]:
        if not delivery_ids:
            return {}

        placeholders = ','.join('?' * len(delivery_ids))
        query = f'''
        SELECT
            de.encounter_id,
            de.id,
            de.anc_count,
            de.mode_of_delivery
        FROM delivery_encounters as de
        WHERE de.encounter_id in ({placeholders})
        '''

        db = get_db()
        rows = db.execute(query, delivery_ids).fetchall()
        delivery_babies = cls._get_babies_mapping(delivery_ids=delivery_ids)
        mapping = {}
        for row in rows:
            encounter_id = row['encounter_id']
            mapping[encounter_id]  = DeliveryEncounter(
                id =row['id'],
                mode_of_delivery= DeliveryMode(row['mode_of_delivery']),
                babies = delivery_babies.get(encounter_id, []),
                anc_count = row['anc_count']
            )
        return mapping

    @classmethod
    def _get_child_health_mapping(cls, child_health_ids: List) -> Dict:
        if not child_health_ids:
            return {}

        placeholders = ','.join('?' * len(child_health_ids))
        query = f'''
        SELECT
            che.encounter_id,
            che.id as id,
            che.orin,
            che.dob,
            che.address as address,
            che.guardian_name
        FROM child_health_encounters as che
        WHERE che.encounter_id in ({placeholders})
        '''

        db = get_db()
        rows = db.execute(query, child_health_ids).fetchall()
        mapping = {}
        for row in rows:
            encounter_id = row['encounter_id']
            mapping[encounter_id] = ChildHealth(
                id = row['id'],
                dob = row['dob'],
                address = row['address'],
                guardian_name = row['guardian_name'],
                orin = row['orin']
            )
        return mapping

    @classmethod
    def _build_general_encounter(cls,
                                  facility: FacilityView,
                                  isc: InsuranceScheme,
                                  services_list: List,
                                  diseases_list: List,
                                  row: Dict):
        encounter = EncounterView(
                id=row['id'],
                facility= facility,
                insurance_scheme= isc,
                diseases=diseases_list,
                services = services_list,
                policy_number=row['policy_number'],
                client_name=row['client_name'],
                gender=row['gender'],
                date=row['date'],
                treatment_outcome= TreatmentOutcome(id = row['treatment_id'],
                                            name = row['treatment_outcome'],
                                            type = row['treatment_type']),
                age=row['age'],
                nin = row['nin'],
                enc_type = EncType(row['enc_type']),
                phone_number= row['phone_number'],
                treatment=row['treatment'],
                doctor_name=row['doctor_name'],
                created_by=row['created_by'],
                created_at=row['created_at']
            )
        return encounter

    @classmethod
    def _build_anc_encounter(cls,
                              facility: FacilityView,
                              isc: InsuranceScheme,
                              services_list: List,
                              diseases_list: List,
                              anc_registry: ANCRegistry,
                              row: Dict) -> ANCEncounterView:
        encounter = cls._build_general_encounter(facility = facility,
                                                 isc = isc,
                                                 services_list = services_list,
                                                 diseases_list= diseases_list,
                                                 row = row)
        return ANCEncounterView(**encounter.__dict__, anc = anc_registry)


    @classmethod
    def _build_delivery_encounter(cls,
                              facility: FacilityView,
                              isc: InsuranceScheme,
                              services_list: List,
                              diseases_list: List,
                              delivery_encounter: DeliveryEncounter,
                              row: Dict) -> DeliveryEncounterView:
        encounter = cls._build_general_encounter(facility = facility,
                                                 isc = isc,
                                                 services_list = services_list,
                                                 diseases_list= diseases_list,
                                                 row = row)
        return DeliveryEncounterView(**encounter.__dict__, delivery= delivery_encounter)

    @classmethod
    def _build_child_health_encounter(cls,
                                       facility: FacilityView,
                                       isc: InsuranceScheme,
                                       services_list: List,
                                       diseases_list: List,
                                       child_encounter: ChildHealth,
                                       row: Dict):
        encounter = cls._build_general_encounter(facility = facility,
                                                  isc = isc,
                                                  services_list= services_list,
                                                  diseases_list= diseases_list,
                                                  row = row)
        return ChildHealthEncounterView(
            **encounter.__dict__, health_details=child_encounter
        )

    @classmethod
    def get_all(cls,
                params: Optional[Params] = None,
                **kwargs
                ) -> Iterator:

        encounters_rows = cls._get_base_encounter(params, **kwargs)

        encounter_ids = []
        delivery_ids = []
        anc_ids = []
        child_health_id = []

        for row in encounters_rows:
            encounter_ids.append(row['id'])
            if row['enc_type'] == EncType.DELIVERY.value:
                delivery_ids.append(row['id'])
            elif row['enc_type'] == EncType.ANC.value:
                anc_ids.append(row['id'])
            elif row['enc_type'] == EncType.CHILDHEALTH.value:
                child_health_id.append(row['id'])

        if not encounter_ids:
            return []

        diseases_by_encounter = cls._get_diseases_mapping(encounter_ids)
        services_by_encounter = cls._get_services_mapping(encounter_ids=encounter_ids)
        anc_by_encounters = cls._get_anc_mapping(anc_ids)
        delivery_by_encounters = cls._get_delivery_mapping(delivery_ids)
        child_health_by_encounters = cls._get_child_health_mapping(child_health_ids=child_health_id)

        facility_ids = [row['facility_id'] for row in encounters_rows]
        scheme_map = FacilityServices.get_insurance_list(facility_ids)

        for row in encounters_rows:
            encounter = None

            facility=FacilityView(
                id=row['facility_id'],
                name=row['facility_name'],
                lga=row['lga'],
                scheme=scheme_map[row['facility_id']],
                facility_type=row['facility_type']
            )

            insurance_scheme=InsuranceScheme(id=row['scheme_id'],
                            scheme_name=row['scheme_name'],
                            color_scheme=row['color_scheme'])

            if  row['enc_type'] == EncType.ANC.value:
                encounter = cls._build_anc_encounter(
                    facility = facility,
                    isc = insurance_scheme,
                    services_list = services_by_encounter.get(row['id'], []),
                    diseases_list = diseases_by_encounter.get(row['id'], []),
                    anc_registry= anc_by_encounters[row['id']],
                    row = row
                )
            elif row['enc_type'] == EncType.DELIVERY.value:
                encounter = cls._build_delivery_encounter(
                    facility = facility,
                    isc = insurance_scheme,
                    services_list = services_by_encounter.get(row['id'], []),
                    diseases_list = diseases_by_encounter.get(row['id'], []),
                    delivery_encounter = delivery_by_encounters[row['id']],
                    row = row
                )

            elif row['enc_type'] == EncType.GENERAL.value:
                encounter = cls._build_general_encounter(
                    facility = facility,
                    isc = insurance_scheme,
                    services_list = services_by_encounter.get(row['id'], []),
                    diseases_list = diseases_by_encounter.get(row['id'], []),
                    row = row
                )
            elif row['enc_type'] == EncType.CHILDHEALTH.value:
                encounter  = cls._build_child_health_encounter(

                    facility = facility,
                    isc = insurance_scheme,
                    services_list = services_by_encounter.get(row['id'], []),
                    diseases_list = diseases_by_encounter.get(row['id'], []),
                    child_encounter = child_health_by_encounters[row['id']],
                    row = row
                )

            yield encounter

    @classmethod
    def get_encounter_by_facility(cls, facility_id: int) -> Iterator:
        return cls.get_all(and_filter=[('ec.facility_id', facility_id, '=')])

    @classmethod
    def get_view_by_id(cls, id: int) -> EncounterView:
        filters = Params().where(Encounter, 'id', '=', id)
        try:
            return next(cls.get_all(params=filters))
        except StopIteration:
            raise MissingError("Encounter does not exist in database")

    @classmethod
    def update_data(cls, model):
        # do not allow update of encounter
        raise NotImplementedError(
            "Encounter are immutable and cannot be updated")


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


class DashboardServices(BaseServices):
    model = None
    table_name = None
    MODEL_ALIAS_MAP = {**EncounterServices.MODEL_ALIAS_MAP,
                       User : 'u',
                       Disease: 'dis',
                       Service: 'srv',
                       DiseaseCategory: 'dc',
                       EncounterDiseases: 'ecd',
                       EncounterServices: 'ecs'}

    @classmethod
    def get_top_encounter_facilities(cls, params: Params):
        query = f'''
        SELECT
            fc.name AS facility_name,
            COUNT(ec.id) as encounter_count
           FROM encounters as ec
           JOIN facility as fc on ec.facility_id = fc.id
          '''

        params = params.group(Facility, 'id')
        params = params.sort(None, 'encounter_count', 'DESC')
        if not params.limit:
            params = params.set_limit(5)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query=query, **res)

        return cls._run_query(query, args,
                              lambda row: {'facility_name': row['facility_name'],
                                           'encounter_count': row['encounter_count'],
                                           })

    @classmethod
    def get_top_utilization_facilities(cls, params: Params):
        query = f'''
        SELECT
            fc.name AS facility_name,
            COUNT(ec.id) as encounter_count
           FROM encounters as ec
           LEFT JOIN encounters_diseases as ecd ON ec.id = ecd.encounter_id
           LEFT JOIN encounters_services as ecs on ec.id = ecs.encounter_id
           JOIN facility as fc on ec.facility_id = fc.id
          '''

        params = params.group(Facility, 'id')
        params = params.sort(None, 'encounter_count', 'DESC')
        if not params.limit:
            params = params.set_limit(5)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query=query, **res)

        return cls._run_query(query, args,
                              lambda row: {'facility_name': row['facility_name'],
                                           'count': row['encounter_count'],
                                           })

    @classmethod
    def _run_query(cls, query: str, params: list, row_mapper):
        db = get_db()
        rows = db.execute(query, params)
        return [row_mapper(row) for row in rows]

    @classmethod
    def get_age_group(cls, query, args):
        db = get_db()
        rows = db.execute(query, args).fetchall()
        age_group = [g.value for g in AgeGroup]
        used = set()
        result = []

        for row in rows:
            print(row)
            result.append({'age_group': row['age_group'], 'count': row['age_group_count']})
            used.add(row['age_group'])

        for age in age_group:
            if age not in used:
                result.append({'age_group': age, 'count': 0})
        return result

    @classmethod
    def top_diseases(cls,
                     params:Params):

        query = '''
        SELECT
         cause_name || " (" || cause_type || ")" as disease_name,
         COUNT(*) as count
        FROM
        ( SELECT
                ecs.encounter_id as encounter_id,
                ecs.service_id as cause_id,
                'Service' as cause_type,
                srv.name as cause_name
            FROM encounters_services as ecs
            JOIN services as srv on ecs.service_id = srv.id
            UNION ALL

            SELECT
                ecd.encounter_id as encounter_id,
                ecd.disease_id as cause_id,
                'Disease' as cause_type,
                dis.name as cause_name
            FROM encounters_diseases as ecd
            JOIN diseases as dis on ecd.disease_id = dis.id
        ) as temp

        JOIN encounters as ec on temp.encounter_id = ec.id
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.group(None, 'cause_id')
        params = params.sort(None, 'Count', 'DESC')
        if not params.limit:
            params = params.set_limit(10)

        res:Dict = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        return cls._run_query(query,
                              args,
                              lambda row: {'disease_name': row['disease_name'], 'count': row['count']})

    @classmethod
    def top_services(cls,
                     params:Params):

        query = '''
             SELECT srv.name AS service_name, COUNT(srv.id) AS service_count
             FROM encounters AS ec
             JOIN encounters_services as ecs ON ecs.encounter_id = ec.id
             JOIN services AS srv ON ecs.service_id = srv.id
             JOIN facility AS fc ON fc.id = ec.facility_id
        '''
        args = ()
        params = params.group(Service, 'id')
        params = params.sort(None, 'service_count', 'DESC')
        if not params.limit:
            params = params.set_limit(10)

        res:Dict = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        return cls._run_query(query,
                              args,
                              lambda row: {'service_name': row['service_name'], 'count': row['service_count']})

    @classmethod
    def encounter_gender_distribution(cls,
                            params: Params):
        query = '''
            SELECT
              CASE
                WHEN ec.gender = 'M' THEN 'Male'
                WHEN ec.gender = 'F' THEN 'Female'
              END as gender,
            COUNT(ec.gender) as gender_count
            FROM encounters AS ec
            JOIN facility as fc ON fc.id = ec.facility_id
        '''
        params = params.group(Encounter, 'gender')
        if not params.limit:
            params = params.set_limit(5)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        return cls._run_query(query,
                             args,
                             lambda row: {'gender': row['gender'], 'count': row['gender_count']})


    @classmethod
    def encounter_age_group_distribution(cls,
                               params:Params):

        query = '''
            SELECT age_group, COUNT(*) as age_group_count
            FROM encounters as ec
            JOIN facility as fc ON fc.id = ec.facility_id
        '''

        params = params.group(Encounter, 'age_group')
        res  = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        return cls.get_age_group(query, args)

    @classmethod
    def utilization_age_group_distribution(cls,
                               params:Params):

        query = '''
            SELECT age_group, COUNT(*) as age_group_count
            FROM encounters as ec
            LEFT JOIN encounters_diseases as ecd on ecd.encounter_id = ec.id
            LEFT JOIN encounters_services as ecs on ecs.encounter_id = ec.id
            JOIN facility as fc ON fc.id = ec.facility_id
        '''

        params = params.group(Encounter, 'age_group')
        res  = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        return cls.get_age_group(query, args)

    @classmethod
    def get_utilization_trend(cls, params: Params, start_date, end_date):
        # ensure at least 6 months range
        start_date = start_date.replace(day=1)
        supposed_start = (end_date.replace(day=1) - relativedelta(month=6))
        if start_date > supposed_start:
            start_date = supposed_start

        query = '''
            SELECT ec.date, COUNT(*) AS date_count
            FROM encounters AS ec
            LEFT JOIN encounters_diseases as ecd on ecd.encounter_id = ec.id
            LEFT JOIN encounters_services as ecs on ecs.encounter_id = ec.id
            JOIN facility AS fc ON fc.id = ec.facility_id
        '''

        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        params = params.group(Encounter, 'date')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])

        if df.empty:
            return pd.DataFrame(columns=['date', 'date_count']).to_json(orient="records")

        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('M')

        trend = df.groupby('date')['date_count'].sum().reset_index()

        # Fill missing months
        all_months = pd.period_range(start=start_date, end=end_date, freq='M')
        trend = trend.set_index('date').reindex(all_months, fill_value=0)
        trend.index.name = 'date'
        trend = trend.reset_index()

        trend['date'] = trend['date'].astype(str)
        trend.rename(columns={'date_count': 'count'})
        return trend.to_dict(orient="records")

    @classmethod
    def get_encounter_trend(cls, params: Params, start_date, end_date):
        # ensure at least 6 months range
        start_date = start_date.replace(day=1)
        supposed_start = (end_date.replace(day=1) - relativedelta(month=6))
        if start_date > supposed_start:
            start_date = supposed_start

        query = '''
            SELECT ec.date, COUNT(*) AS date_count
            FROM encounters AS ec
            JOIN facility AS fc ON fc.id = ec.facility_id
        '''

        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        params = params.group(Encounter, 'date')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])

        if df.empty:
            return pd.DataFrame(columns=['date', 'date_count']).to_json(orient="records")

        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('M')

        trend = df.groupby('date')['date_count'].sum().reset_index()

        # Fill missing months
        all_months = pd.period_range(start=start_date, end=end_date, freq='M')
        trend = trend.set_index('date').reindex(all_months, fill_value=0)
        trend.index.name = 'date'
        trend = trend.reset_index()

        trend['date'] = trend['date'].astype(str)
        return trend.to_dict(orient='records')

    @classmethod
    def get_encounter_per_scheme(cls, params: Params):
        query = '''
        SELECT
            COUNT(*) as encounter_count,
            isc.scheme_name as encounter_scheme,
            isc.color_scheme as color_scheme
        FROM encounters as ec
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on ec.facility_id = fc.id
        '''

        params = params.group(Encounter, 'scheme')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query= query, **res)
        return cls._run_query(query=query,
                        params=args,
                        row_mapper = lambda row: {'scheme_name': row['encounter_scheme'],
                                                  'color':  row['color_scheme'],
                                                  'count': row['encounter_count']}
                       )


    @classmethod
    def get_mortality_per_scheme(cls, params: Params):
        query = '''
        SELECT
            COUNT(*) as encounter_count,
            isc.scheme_name as encounter_scheme,
            isc.color_scheme as color_scheme
        FROM encounters as ec
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on ec.facility_id = fc.id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(Encounter, 'scheme')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query= query, **res)
        return cls._run_query(query=query,
                        params=args,
                        row_mapper = lambda row: {'scheme_name': row['encounter_scheme'],
                                                  'color':  row['color_scheme'],
                                                  'count': row['encounter_count']}
                       )
    @classmethod
    def case_fatality(cls, params: Params):
        query = '''
        SELECT
            CASE WHEN COUNT(ec.id) = 0 THEN 0
            ELSE
                (SUM(CASE WHEN LOWER(tc.type) = 'death' THEN 1 ELSE 0 END) * 1.0/
                COUNT(ec.id)) * 100.0
            END as fatality_count
        FROM encounters as ec
        JOIN facility as fc on ec.facility_id = fc.id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()
        return row['fatality_count'] if row else 0.0

    @classmethod
    def get_utilization_per_scheme(cls, params: Params):
        query = '''
        SELECT
            COUNT(*) as encounter_count,
            isc.scheme_name as encounter_scheme,
            isc.color_scheme as color_scheme
        FROM encounters as ec
        LEFT JOIN encounters_diseases as ecd on ecd.encounter_id = ec.id
        LEFT JOIN encounters_services as ecs on ecs.encounter_id = ec.id
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on ec.facility_id = fc.id
        '''

        params = params.group(Encounter, 'scheme')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query= query, **res)
        return cls._run_query(query=query,
                        params=args,
                        row_mapper = lambda row: {'scheme_name': row['encounter_scheme'],
                                                  'color':  row['color_scheme'],
                                                  'count': row['encounter_count']}
                       )
    @classmethod
    def get_treatment_outcome_distribution(cls, params: Params):
        inner_query = '''
        SELECT
            CASE WHEN tc.type = 'Death' THEN 'Death' ELSE tc.name END AS outcome
        FROM encounters AS ec
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome AS tc ON tc.id = ec.outcome
        '''
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query = inner_query, **res)
        new_query = f'SELECT outcome, COUNT(*) as outcome_count FROM ({query}) GROUP BY outcome'
        # print(new_query, args)
        return cls._run_query(query =new_query,
                      params = args,
                      row_mapper = lambda row: {'outcome': row['outcome'],
                                                'count': row['outcome_count']})

    @classmethod
    def get_referral_count(cls, params: Params):
        query = '''
        SELECT
            COUNT(*) as referral_count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'name', '=', 'Referral')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args =  cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()
        return row['referral_count'] if row else 0


    @classmethod
    def get_total_utilization(cls, params: Params, start_date, end_date):
        diff = end_date - start_date
        prev_start_date = start_date - diff
        prev_end_date = start_date - timedelta(days=1)

        query = '''
        SELECT
            COALESCE(SUM(CASE WHEN ec.date BETWEEN ? AND ? THEN 1 ELSE 0 END), 0) AS current_count,
            COALESCE(SUM(CASE WHEN ec.date BETWEEN ? AND ? THEN 1 ELSE 0 END), 0) AS prev_count
        FROM encounters AS ec
        LEFT JOIN encounters_diseases as ecd on ecd.encounter_id = ec.id
        LEFT JOIN encounters_services as ecs on ecs.encounter_id = ec.id
        JOIN facility AS fc ON fc.id = ec.facility_id
        '''

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, filter_args = cls._apply_filter(query, **res)

        args = [start_date, end_date, prev_start_date, prev_end_date] + filter_args

        db = get_db()
        row = db.execute(query, args).fetchone()
        current = row['current_count'] if row else 0
        prev = row['prev_count'] if row else 0

        pct_change = 0.0
        if prev:
            pct_change = ((current - prev) / prev) * 100
        else:
            pct_change = 100.0 if current else 0.0

        return current, pct_change


    @classmethod
    def get_total_encounters(cls, params: Params, start_date, end_date):
        diff = end_date - start_date
        prev_start_date = start_date - diff
        prev_end_date = start_date - timedelta(days=1)

        query = '''
        SELECT
            COALESCE(SUM(CASE WHEN ec.date BETWEEN ? AND ? THEN 1 ELSE 0 END), 0) AS current_count,
            COALESCE(SUM(CASE WHEN ec.date BETWEEN ? AND ? THEN 1 ELSE 0 END), 0) AS prev_count
        FROM encounters AS ec
        JOIN facility AS fc ON fc.id = ec.facility_id
        '''

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, filter_args = cls._apply_filter(query, **res)

        args = [start_date, end_date, prev_start_date, prev_end_date] + filter_args
        # print(query, args)

        db = get_db()
        row = db.execute(query, args).fetchone()
        current = row['current_count'] if row else 0
        prev = row['prev_count'] if row else 0
        # print(current, prev)

        pct_change = 0.0
        if prev:
            pct_change = ((current - prev) / prev) * 100
        else:
            pct_change = 100.0 if current else 0.0

        return current, pct_change


    @classmethod
    def encounter_distribution_across_lga(cls, params: Params):
        query = '''
        SELECT
            fc.local_government as lga,
            COUNT(*) as count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        params = params.group(Facility, 'local_government')
        params = params.sort(None, 'count', 'DESC')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        rows = db.execute(query, args).fetchall()
        result = dict.fromkeys([x.upper() for x in ONDO_LGAS_LOWER], 0)
        for row in rows:
            result[row['lga'].upper()] = row['count']
        return sorted([{'lga': key, 'count': value} for key, value in result.items()], key = lambda row: row['count'])


    @classmethod
    def utilization_distribution_across_lga(cls, params: Params):
        query = '''
        SELECT
            fc.local_government as lga,
            COUNT(*) as count
        FROM encounters as ec
        LEFT JOIN encounters_diseases as ecd on ecd.encounter_id = ec.id
        LEFT JOIN encounters_services as ecs on ecs.encounter_id = ec.id
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        params = params.group(Facility, 'local_government')
        params = params.sort(Facility, 'local_government', 'DESC')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        rows = db.execute(query, args).fetchall()
        result = dict.fromkeys([x.upper() for x in ONDO_LGAS_LOWER], 0)
        for row in rows:
            result[row['lga'].upper()] = row['count']
        return sorted([{'lga': key, 'count': value} for key, value in result.items()], key = lambda row: row['count'])


    @classmethod
    def total_utilization_by_scheme_grouped(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            ec.date,
            isc.scheme_name,
            isc.color_scheme
        FROM encounters as ec
        LEFT JOIN encounters_diseases as ecd on ecd.encounter_id = ec.id
        LEFT JOIN encounters_services as ecs on ecs.encounter_id = ec.id
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        if (end_date - start_date).days < (365 * 5): #minumum of 5 display years
            start_date = end_date.replace(day=1, month=1, year = end_date.year - 5)

        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        # params = params.group(Encounter, 'date').group(InsuranceScheme, 'scheme_name')\
                # .group(InsuranceScheme, 'color_scheme')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            return {}

        df['date'] =  pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('Y')
        df.sort_values('date')
        df = df.groupby(['date', 'scheme_name', 'color_scheme']).size().reset_index(name='count')
        all_schemes = df[['scheme_name', 'color_scheme']].drop_duplicates()
        all_years = pd.period_range(start_date, end_date, freq='Y')
        full_index = []
        for year in all_years:
            for scheme in all_schemes.itertuples(name=None, index=False):
                full_index.append((year, *scheme))
        df = (
            df.set_index(['date', 'scheme_name', 'color_scheme'])
            .reindex(full_index, fill_value=0)
            .reset_index()
        )
        df['date'] = df['date'].astype('str')
        return df.to_dict(orient="records")

    @classmethod
    def mortality_distribution_by_type(cls, params: Params):
        query = '''
        SELECT
            tc.name,
            COUNT(*) as count
        FROM encounters as ec
        JOIN facility as fc ON ec.facility_id = fc.id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(TreatmentOutcome, 'name')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        return cls._run_query(query, args,
                            lambda row: {'death_type': row['name'], 'count': row['count']})

    @classmethod
    def mortality_distribution_by_age_group(cls, params: Params):
        query = '''
        SELECT
            ec.age_group,
            COUNT(*) as age_group_count
        FROM encounters as ec
        JOIN facility as fc ON ec.facility_id = fc.id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.group(Encounter, 'age_group')
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        return cls.get_age_group(query, args)

    @classmethod
    def get_top_cause_of_mortality(cls, params: Params):
        query = '''
        SELECT
         cause_name || " (" || cause_type || ")" as cause_name,
         COUNT(*) as count
        FROM
        ( SELECT
                ecs.encounter_id as encounter_id,
                ecs.service_id as cause_id,
                'Service' as cause_type,
                srv.name as cause_name
            FROM encounters_services as ecs
            JOIN services as srv on ecs.service_id = srv.id
            UNION ALL

            SELECT
                ecd.encounter_id as encounter_id,
                ecd.disease_id as cause_id,
                'Disease' as cause_type,
                dis.name as cause_name
            FROM encounters_diseases as ecd
            JOIN diseases as dis on ecd.disease_id = dis.id
        ) as temp

        JOIN encounters as ec on temp.encounter_id = ec.id
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(None, 'cause_id')
        params = params.group(None, 'cause_name')
        params = params.sort(None, 'count', 'DESC')

        if not params.limit:
            params = params.set_limit(10)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        # print(query, args)
        return cls._run_query(query, args,
                              lambda row: {'name': row['cause_name'], 'count': row['count']})

    @classmethod
    def get_mortality_distribution_by_gender(cls, params: Params):
        query = '''
        SELECT
            CASE
                WHEN ec.gender = 'M' THEN 'Male'
                WHEN ec.gender = 'F' THEN 'Female'
            END as gender,
            COUNT(ec.id) as count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(Encounter, 'gender')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        return cls._run_query(query, args,
                lambda row: {'gender': row['gender'], 'count': row['count']})


    @classmethod
    def total_mortality_by_scheme_grouped(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            ec.date ,
            isc.scheme_name,
            isc.color_scheme
        FROM encounters as ec
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        if (end_date - start_date).days < 365 * 5:
            start_date = end_date.replace(year = end_date.year - 5, day = 1, month = 1)
        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            return {}
        df['date'] =  pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('Y')
        df.sort_values('date')
        df = df.groupby(['date', 'scheme_name', 'color_scheme']).size().reset_index(name='count')
        all_schemes = df[['scheme_name', 'color_scheme']].drop_duplicates()
        all_years = pd.period_range(start_date, end_date, freq='Y')
        full_index = []
        for year in all_years:
            for scheme in all_schemes.itertuples(name=None, index=False):
                full_index.append((year, *scheme))
        df = (
            df.set_index(['date', 'scheme_name', 'color_scheme'])
            .reindex(full_index, fill_value=0)
            .reset_index()
        )
        df['date'] = df['date'].astype('str')
        return df.to_dict(orient='records')



    @classmethod
    def total_encounter_by_scheme_grouped(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            ec.date,
            isc.scheme_name,
            isc.color_scheme
        FROM encounters as ec
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        if (end_date - start_date).days < 365 * 5:
            start_date = end_date.replace(year = end_date.year - 5, day = 1, month = 1)
        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        # params = params.group(Encounter, 'date').group(InsuranceScheme, 'scheme_name')\
                # .group(InsuranceScheme, 'color_scheme')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            return  {}

        df['date'] =  pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('Y')
        df.sort_values('date')
        df = df.groupby(['date', 'scheme_name', 'color_scheme']).size().reset_index(name='count')
        all_schemes = df[['scheme_name', 'color_scheme']].drop_duplicates()
        all_years = pd.period_range(start_date, end_date, freq='Y')
        full_index = []
        for year in all_years:
            for scheme in all_schemes.itertuples(name=None, index=False):
                full_index.append((year, *scheme))
        df = (
            df.set_index(['date', 'scheme_name', 'color_scheme'])
            .reindex(full_index, fill_value=0)
            .reset_index()
        )
        df['date'] = df['date'].astype('str')
        return df.to_dict(orient='records')

    @classmethod
    def get_mortality_count_per_facility(cls, params: Params):
        query = '''
            SELECT
                fc.name as facility_name,
                COUNT(*) as count
            FROM encounters as ec
            JOIN facility as fc on ec.facility_id = fc.id
            JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(Facility, 'id')
        params = params.sort(None, 'count', 'desc')
        if not params.limit:
            params = params.set_limit(10)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        return cls._run_query(query,
                              args,
                              lambda row: {'facility_name': row['facility_name'],
                                            'count': row['count']})

    @classmethod
    def get_mortality_trend(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            ec.date,
            tc.name as death_type,
            COUNT(ec.id) as death_count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        if (end_date - start_date).days < 30 * 6:
            start_date = end_date.replace(day = 1) - relativedelta(month=6)
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        params = params.group(Encounter, 'date')
        params = params.group(TreatmentOutcome, 'name')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        # print(query, args)
        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            return {}

        # print(df)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period(freq='M')
        rows =  db.execute('SELECT tc.name  as death_type from \
                                  treatment_outcome as tc where tc.type = "Death"').fetchall()
        death_type = [ row['death_type'] for row in rows]
        all_month = list(set(pd.period_range(start=start_date, end=end_date, freq='M')))
        df = df.groupby(['date', 'death_type'], as_index=False)['death_count'].sum()
        df = df.set_index(['date', 'death_type'])
        df = df.reindex(pd.MultiIndex.from_product([all_month, death_type], names = ['date', 'death_type']),
                        fill_value=0).reset_index()
        df.date = df.date.astype(str)
        # print(df)
        pivot = df.pivot_table(
            index = 'date',
            columns = 'death_type',
            values= 'death_count',\
            aggfunc= 'sum',
        ).reset_index()
        pivot['total'] = pivot.drop('date', axis=1).sum(axis=1)
        return pivot.to_dict(orient='records')


    @classmethod
    def get_mortality_by_lga(cls, params: Params):
        query = '''
        SELECT
            fc.local_government as lga,
            COUNT(*) as count
        FROM encounters as ec
        JOIN facility as fc on ec.facility_id = fc.id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(Facility, 'local_government')
        params = params.sort(None, 'count', 'DESC')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        rows = db.execute(query, args)
        result = dict.fromkeys([lga.upper() for lga in ONDO_LGAS_LOWER], 0)
        for row in rows:
            result[row['lga'].upper()] = row['count']
        return sorted([{'lga': key, 'count': value} for key, value in result.items()], key= lambda row: row['count'])

    @classmethod
    def get_average_mortality_per_day(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            CASE WHEN COUNT(DISTINCT ec.date) = 0 THEN 0
            ELSE COUNT(*) * 1.0/ COUNT(DISTINCT ec.date)
            END as count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(Encounter, 'date', '>=', start_date)\
                        .where(Encounter, 'date', "<=", end_date)
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()
        res = row['count'] if row else 0
        return res or 0

    @classmethod
    def get_average_encounter_per_day(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            CASE WHEN COUNT(DISTINCT ec.date) = 0 THEN 0
            ELSE COUNT(*) * 1.0/ COUNT(DISTINCT ec.date)
            END as count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        params = params.where(Encounter, 'date', '>=', start_date)\
                        .where(Encounter, 'date', "<=", end_date)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()
        res = row['count'] if row else 0
        return res or 0


    @classmethod
    def get_average_utilization_per_day(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            CASE WHEN COUNT(DISTINCT ec.date) = 0 THEN 0
            ELSE COUNT(*) * 1.0/ COUNT(DISTINCT ec.date)
            END as count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        LEFT JOIN encounters_diseases as ecd on ec.id =  ecd.encounter_id
        LEFT JOIN encounters_services as ecs on ec.id =  ecs.encounter_id
        '''
        params = params.where(Encounter, 'date', '>=', start_date)\
                        .where(Encounter, 'date', "<=", end_date)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        # print(query, args)
        db = get_db()
        row = db.execute(query, args).fetchone()
        # print( dict(row))
        res = row['count'] if row else 0
        return res or 0

    @classmethod
    def get_total_death_outcome(cls, params: Params, start_date, end_date):
        ''''
        Return total death outcome and the percentage difference based on the prevous month
        Params should not filter based on date. but instead parse the start_date and end_date
        '''

        diff = end_date - start_date
        prev_start_date = start_date - diff
        prev_end_date = start_date - timedelta(days=1)
        query = '''
        SELECT
            COALESCE(SUM(CASE WHEN ec.date >= ? and ec.date <= ? THEN 1 ELSE 0 END), 0)  as prev_count,
            COALESCE(SUM(CASE WHEN ec.date >= ? and ec.date <= ? THEN 1 ELSE 0 END), 0)  as current_count
        FROM encounters as ec
        JOIN facility as fc on ec.facility_id = fc.id
        JOIN treatment_outcome as tc on ec.outcome = tc.id
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        db = get_db()
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)

        # print(res)

        query, args = cls._apply_filter(base_query= query, **res)
        # print(query, args)
        args = [prev_start_date, prev_end_date, start_date, end_date] + args
        row = db.execute(query, args).fetchone()

        prev = row['prev_count'] if row else 0
        current = row['current_count'] if row else 0
        pct_change = 0

        if prev:
            pct_change =  ((current - prev)/prev) * 100
        elif current:
            pct_change = 100
        return (current, pct_change)


    @classmethod
    def get_active_encounter_facility(cls, params: Params):
        query = '''
        SELECT
            COUNT (DISTINCT fc.id) as facility_count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()
        return row['facility_count'] if row else 0

    @classmethod
    def get_top_facilities_summaries(cls, params: Params, start_date, end_date,):
        query = '''
        SELECT
            fc.name AS facility_name,
            COUNT(ec.id) as encounter_count,
            (
                SELECT dis.name
                FROM encounters AS ec2
                JOIN encounters_diseases as ecd ON ec2.id = ecd.encounter_id
                JOIN diseases AS dis ON ecd.disease_id = dis.id
                WHERE ec2.facility_id = ec.facility_id
                 GROUP BY ecd.disease_id
                 ORDER BY COUNT(ecd.disease_id) DESC
                 LIMIT 1
            ) AS top_disease,
           MAX(ec.created_at) as last_submission
           FROM encounters as ec
            JOIN facility as fc on ec.facility_id = fc.id
          '''
        params = (params.where(Encounter, 'date', '>=', start_date)
                        .where(Encounter, 'date', '<=', end_date)
                        .group(Encounter, 'facility_id')
                        .sort(None, 'encounter_count', 'DESC'))
        if not params.limit:
            params = params.set_limit(10)
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query,  **res)

        return cls._run_query(query, args,
                              lambda row: {'facility_name': row['facility_name'],
                                           'encounter_count': row['encounter_count'],
                                           'top_disease': row['top_disease'],
                                           'last_submission': row['last_submission']})

class ReportServices(BaseServices):

    @classmethod
    def get_start_end_date(cls, month: Optional[int], year: Optional[int]):
        filter_date = datetime.now().date()
        if month is not None:
            filter_date = filter_date.replace(month=month)
        if year is not None:
            filter_date = filter_date.replace(year=year)

        start_date = filter_date.replace(day=1)
        if filter_date.month == 12:
            end_date = filter_date.replace(
                year=filter_date.year+1, month=1, day=1) - timedelta(days=1)
        else:
            end_date = filter_date.replace(
                month=filter_date.month+1, day=1) - timedelta(days=1)
        return start_date, end_date

    @classmethod
    def generate_service_utilization_report(cls, facility: int, month: Optional[int] = None,
                                            year: Optional[int] = None,
                                            ) -> Tuple:
        try:
            facility_name = FacilityServices.get_by_id(facility)
        except MissingError:
            raise MissingError("Facility does not exist. No report generated")

        if month and month > 12:
            raise ValidationError("Invalid month selection")
        start_date, end_date = cls.get_start_end_date(month, year)

        query = '''
            SELECT
                ec.policy_number,
                dis.name AS disease_name,
                ec.gender,
                ec.age_group
            FROM encounters AS ec
            LEFT JOIN encounters_diseases as ed on ed.encounter_id = ec.id
            JOIN diseases as dis on dis.id = ed.disease_id
            WHERE ec.date >= ? and ec.date <= ?
            AND ec.facility_id = ?
        '''
        args = (start_date, end_date, facility)
        db = get_db()

        rows = db.execute(query, args)
        df = pd.DataFrame([dict(row) for row in rows])

        if df.empty:
            raise MissingError("No report available for this timeframe!")

        age_groups = ['<1', '1-5', '6-14', '15-19', '20-44', '45-64', '65&AB']
        gender = ['M', 'F']

        table = df.pivot_table(
            index='disease_name',
            values='policy_number',
            columns=['age_group', 'gender'],
            aggfunc='count',
            fill_value=0
        ).reindex(
            pd.MultiIndex.from_product([age_groups, gender]),
            axis=1,
            fill_value=0
        )
        # table

        table[('TOTAL', 'M')] = table.filter(like='M').sum(axis=1)
        table[('TOTAL', 'F')] = table.filter(like='F').sum(axis=1)
        table['GRAND TOTAL'] = table[('TOTAL', 'M')] + table[('TOTAL', 'F')]
        table.loc['TOTAL'] = table.sum()
        table = table.reset_index()
        table.index.name = ''
        table.columns.name = ''
        table.rename(columns={'disease_name': 'Diseases'}, inplace=True)

        return facility_name, start_date, table

    @classmethod
    def generate_encounter_report(cls, month: Optional[int] = None,
                                  year: Optional[int] = None,
                                  ) -> Tuple:

        if month and (month > 12 or month < 1):
            raise ValidationError("Invalid month selection")

        start_date, end_date = cls.get_start_end_date(month, year)

        query = '''
            SELECT
                f.name as facility_name,
                ec.policy_number,
                ec.gender,
                ec.age_group
            FROM encounters as ec
            JOIN facility as f ON ec.facility_id = f.id
            WHERE  ec.date >= ? AND ec.date <= ?
        '''

        db = get_db()
        rows = db.execute(query, (start_date, end_date))
        df = pd.DataFrame([dict(row) for row in rows])

        if df.empty:
            raise MissingError("No report available for this timeframe!")

        age_groups = ['<1', '1-5', '6-14', '15-19', '20-44', '45-64', '65&AB']
        gender = ['M', 'F']

        table = df.pivot_table(
            index='facility_name',
            values='policy_number',
            columns=['age_group', 'gender'],
            aggfunc='count',
            fill_value=0,

        ).reindex(
            pd.MultiIndex.from_product([age_groups, gender]),
            fill_value=0,
            axis=1
        )

        table[('TOTAL', 'M')] = table.filter(like='M').sum(axis=1)
        table[('TOTAL', 'F')] = table.filter(like='F').sum(axis=1)
        table['GRAND TOTAL'] = table[('TOTAL', 'M')] + table[('TOTAL', 'F')]
        table.loc['TOTAL'] = table.sum()
        table = table.reset_index()
        table.index.name = ''
        table.columns.name = ''
        table.rename(columns={'facility_name': 'Facilities'}, inplace=True)
        return start_date, table

    @classmethod
    def generate_categorization_report(cls, month: Optional[None], year: Optional[None]):
        if month and (month < 1 or month > 12):
            raise ValidationError("Invalid Month selection")
        start_date, end_date = cls.get_start_end_date(month, year)
        query = '''
            SELECT
              ec.policy_number,
              cg.category_name,
              f.name as facility_name
            FROM encounters as ec
            JOIN facility as f on ec.facility_id = f.id
            LEFT JOIN encounters_diseases as ed on ed.encounter_id = ec.id
            JOIN diseases as dis on dis.id = ed.disease_id
            JOIN diseases_category as cg on cg.id = dis.category_id
            WHERE ec.date >= ? AND ec.date <= ?
        '''
        db = get_db()
        rows = db.execute(query, (start_date, end_date))
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            raise MissingError("No report available for the time frame")

        table = df.pivot_table(
            index='facility_name',
            values='policy_number',
            columns=['category_name'],
            aggfunc='count',
            fill_value=0
        )
        rows = db.execute('SELECT category_name from diseases_category')
        categories = [row['category_name'] for row in rows]
        table.reindex(categories, axis=1, fill_value=0)
        table['TOTAL'] = table.sum(axis=1)
        table.loc['TOTAL'] = table.sum()
        table.reset_index(inplace=True)
        table.index.name = ''
        table.columns.name = ''
        table.rename(columns={'facility_name': 'Facilities'}, inplace=True)

        return start_date, table

class UploadServices(BaseServices):

    @classmethod
    # todo
    def upload_sheet(cls):
        return
