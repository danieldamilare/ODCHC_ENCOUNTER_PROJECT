from werkzeug.security import generate_password_hash, check_password_hash
from typing import Optional, Type, TypeVar, Any, Iterator, List, Tuple
from app.db import get_db, close_db
from app.models import User, Facility, Disease, Encounter, DiseaseCategory, Role
from app.exceptions import MissingError, InvalidReferenceError, DuplicateError
from app.exceptions import ValidationError, AuthenticationError
from datetime import datetime, date
from flask_wtf import FlaskForm
from app import app
from app.config import LOCAL_GOVERNMENT
import sqlite3
from datetime import timedelta
import pandas as pd
import json


T = TypeVar('T')
class BaseServices:
    model: Type[T] = None
    table_name = ''
    columns: set = set()
    columns_to_update: set = set()
    @staticmethod
    def _row_to_model(row, model_cls: Type[T]) -> T:
        if row is None:
            raise MissingError("Invalid Row Data")
        return model_cls(**row)
    
    @classmethod
    def get_by_id(cls, id: int) -> object:
        db = get_db()
        row = db.execute(f'SELECT * FROM {cls.table_name} WHERE id = ?', (id,)).fetchone()
        if row is None:
            raise MissingError(f"Object not found in the database")
        return cls._row_to_model(row, cls.model)

    @classmethod
    def list_row_by_page(cls,
                         page: int,
                         page_size = app.config['ADMIN_PAGE_PAGINATION'],
                         and_filter: Optional[List[Tuple]] = None,
                         or_filter: Optional[List[Tuple]] = None,
                         order_by: Optional[List[Tuple[str, str]]] = None,
                         group_by: Optional[List[str]] = None
                         ) -> Iterator:

        try:
            offset = (int(page) - 1) * page_size
            if offset < 0: raise ValueError
        except:
            raise ValidationError("Invalid listing page")

        return cls.get_all(limit=page_size, offset = offset, and_filter =and_filter,
                           or_filter = or_filter, group_by = group_by,
                           order_by = order_by)

    @classmethod
    def update_data(cls, model: Type[T]) -> T:
        db = get_db()
        field = [f"{key}=?" for key in vars(model).keys() if key in cls.columns_to_update]
        values = [v for k, v in vars(model).items() if k in cls.columns_to_update]

        try:
            db.execute(f'UPDATE {cls.table_name} SET {",".join(field)} WHERE id = ?', values + [model.id])
            db.commit()
        except sqlite3.IntegrityError:
            raise DuplicateError
        return model

    @classmethod
    def get_total(cls, 
                  and_filter: Optional[List[Tuple]] = None,
                  or_filter: Optional[List[Tuple]] = None):

        query = f'SELECT COUNT(*) from {cls.table_name}'
        query, args = cls._apply_filter(
            base_query = query,
            base_arg = [],
            and_filter = and_filter,
            or_filter = or_filter,
        )
        # print(query)

        db = get_db()
        return int(db.execute(query, args).fetchone()[0])

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
        ALLOWED_OPERATORS = {'=', '>', '<', '>=', '<=', '!=', 'LIKE'}
        query = ''
        args = base_arg  if base_arg is not None else []
        conditions = []
        query = base_query
        
        if and_filter:
            for column_name, value, opt in and_filter:
                if opt not in ALLOWED_OPERATORS:
                    raise ValidationError(f"Invalid operator: {opt}")
                conditions.append(f"{column_name} {opt} ?")
                args.append(value)
        
        if or_filter:
            or_conditions = []
            for column_name, value, opt in or_filter:
                if column_name not in cls.columns:
                    raise ValidationError("Invalid Column access")
                if opt not in ALLOWED_OPERATORS:
                    raise ValidationError(f"Invalid operator: {opt}")
                or_conditions.append(f"{column_name} {opt} ?")
                args.append(value)
            if or_conditions:
                conditions.append("(" + " OR ".join(or_conditions) + ")")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # --- GROUP BY Clause ---
        if group_by:
            for col in group_by:
                if col not in cls.columns:
                    raise ValidationError(f"Invalid column for group_by: {col}")
            query += f" GROUP BY {', '.join(group_by)}"

        # --- ORDER BY Clause ---
        if order_by:
            clause = []
            for col, direction in order_by:
                if col not in cls.columns:
                    raise ValidationError(f"Invalid column for order_by: {col}")
                if direction.upper() not in ['ASC', 'DESC']:
                    raise ValidationError(f"Invalid sort direction: {direction}")
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
            limit: int = 0,
            offset: int = 0, 
            and_filter: Optional[List[Tuple]] = None,
            or_filter: Optional[List[Tuple]] = None,
            order_by: Optional[List[Tuple[str, str]]] = None,
            group_by: Optional[List[str]] = None
            ) -> Iterator:
        query = f"SELECT * from {cls.table_name}"
        query, args = cls._apply_filter(query, 
                        limit = limit, offset = offset,
                        and_filter= and_filter,
                        or_filter = or_filter,
                        order_by = order_by,
                        group_by = group_by)
        db = get_db()
        rows = db.execute(query, args)
        for  row in rows:
            yield cls._row_to_model(row, cls.model)
    

    @classmethod
    def has_next_page(cls, page: int, 
                      and_filter: Optional[list[Tuple]] = None,
                      or_filter: Optional[List[Tuple]]  = None) -> bool:
        total = cls.get_total(and_filter=and_filter, or_filter=or_filter)
        current = page * app.config['ADMIN_PAGE_PAGINATION']
        if (current < total): return True
        return False

class UserServices(BaseServices):
    model = User
    table_name = 'users';
    columns_to_update = {'username', 'facility_id', 'role'}
    columns = {'id', 'username', 'facility_id', 'role', 'password_hash'}

    @classmethod
    def create_user(cls, username: str, facility_id: Optional[int], password: str, role=None, commit = True) -> User:
        password_hash = generate_password_hash(password)
        role = ('admin' if role == Role.admin else 'user')
        if (role != 'admin'):
            if not facility_id:
                raise ValidationError("User must have a facility")
            try:
                FacilityServices.get_by_id(facility_id)
            except MissingError:
                raise InvalidReferenceError("You can't attach user to a facility that does not exists")

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
            raise DuplicateError('Username exists! Please use another username')

    @classmethod
    def get_user_by_username(cls, username: str) -> User:
        db = get_db()
        row = db.execute(f'SELECT * FROM {cls.table_name} where username = ?', [username]).fetchone()

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
            row = db.execute(f'SELECT * FROM {cls.table_name} where id = ?', (id,)).fetchone()
        except sqlite3.IntegrityError:
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
            limit: int = 0,
            offset: int = 0, 
            and_filter: Optional[List[Tuple]] = None,
            or_filter: Optional[List[Tuple]] = None,
            order_by: Optional[List[Tuple[str, str]]] = None,
            group_by: Optional[List[str]] = None
            ) -> Iterator:

        from app.models import UserView, FacilityView
        db = get_db()
        query = '''
            SELECT 
                u.id AS user_id,
                u.username,
                u.password_hash,
                u.role AS role,
                f.name AS facility_name,
                f.local_government AS lga
            FROM users AS u
            LEFT JOIN facility AS f ON u.facility_id = f.id
        '''
        query, args = cls._apply_filter(
            base_query = query,
            base_arg = [],
            limit= limit,
            offset= offset, 
            and_filter= and_filter,
            or_filter= or_filter,
            order_by=  order_by,
            group_by=  group_by
        )
        rows = db.execute(query, args)

        for row in rows:
            facility = FacilityView(row['facility_name'], row['lga']) if row['facility_name'] else None
            yield UserView(
                id=row['user_id'],
                username=row['username'],
                facility=facility,
                role=Role[row['role']],
            )
                
    @staticmethod
    def get_verified_user(username: str, password: str):
        db = get_db()
        try:
            user = UserServices.get_user_by_username(username)
        except MissingError:
            raise AuthenticationError("Invalid Username or Password")

        if not check_password_hash(user.password_hash, password):
            raise AuthenticationError("Invalid Username or Password")
        return user

    @classmethod
    def update_data(cls, model: User)->User:
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
            db.execute(f'UPDATE {cls.table_name} SET {",".join(field)} WHERE id = ?', values + [model.id])
            db.commit()
        except sqlite3.IntegrityError:
            raise DuplicateError("You cannot a new user with the same username as another user")
        return model

    @classmethod
    def update_user(cls, user: User) -> User:
        return cls.update_data(user)

    @classmethod
    def update_user_password(cls, user: User, password: str) ->User:
        db = get_db()
        password_hash = generate_password_hash(password)
        user.password_hash = password_hash
        db.execute(f'UPDATE {cls.table_name} SET password_hash = ? WHERE id = ?', (password_hash, user.id))
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
    LOCAL_GOVERNMENT = LOCAL_GOVERNMENT
    columns = {'id', 'name', 'local_government', 'facility_type'}
    columns_to_update  = {'name', 'local_government', 'facility_type'}

    @classmethod
    def create_facility(cls, name: str, local_government: str, facility_type: str, commit=True) -> Facility:
        db = get_db()
        if local_government.lower() not in FacilityServices.LOCAL_GOVERNMENT:
            raise ValidationError("Local Government does not exist in Akure")
        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (name, local_government, facility_type) VALUES (?, ?, ?)', (name, local_government, facility_type))
            if commit: db.commit()
            new_id = cursor.lastrowid
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            raise DuplicateError(f"Facility {name} already exist in database")

    @classmethod
    def get_facility_by_name(cls, name: str) -> Facility:
        db = get_db()
        row = db.execute(f'SELECT * FROM {cls.table_name} WHERE name = ?', (name,)).fetchone()
        if row is None:
            raise MissingError(f"No Facility with name {name}")
        return FacilityServices._row_to_model(row, Facility)

    @classmethod
    def delete_facility(cls, facility: Facility):
        db = get_db()
        db.execute(f"DELETE FROM {cls.table_name} WHERE id = ?",  [facility.id])
        db.commit()

    @staticmethod
    def update_facility(facility: Facility):
        if facility.local_government.lower() not in FacilityServices.LOCAL_GOVERNMENT:
            raise ValidationError("Local Government does not exist in Akure")
        try:
            FacilityServices.update_data(facility)
        except DuplicateError:
            raise DuplicateError(f'Facility with name {facility.name} already exists')
        return facility


class DiseaseServices(BaseServices):
    table_name = 'diseases'
    model = Disease
    columns = {'id', 'name', 'category_id'}
    columns_to_update = {'name', 'category_id'}

    @classmethod
    def create_disease(cls, name: str, category_id: int, commit=True) -> Disease:
        db = get_db()
        try:
            DiseaseCategoryServices.get_by_id(category_id)
        except MissingError:
            raise InvalidReferenceError('Disease Category does not exist')
        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (name, category_id) VALUES (?, ?)', [name,category_id])
            if commit: db.commit()
            new_id = cursor.lastrowid
            return  cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            raise DuplicateError(f'Disease {name} already exists')

    
    @classmethod
    def get_all(cls, 
            limit: int = 0,
            offset: int = 0, 
            and_filter: Optional[List[Tuple]] = None,
            or_filter: Optional[List[Tuple]] = None,
            order_by: Optional[List[Tuple[str, str]]] = None,
            group_by: Optional[List[str]] = None
            ) -> Iterator:

        from app.models import DiseaseView
        query = '''
        SELECT 
            d.id as disease_id,
            d.name as disease_name,
            dc.id as category_id,
            dc.category_name 
            FROM diseases as d
            JOIN diseases_category as dc 
            ON d.category_id = dc.id'''

        query, args = cls._apply_filter(
            base_query= query,
            base_arg = [],
            limit = limit,
            offset = offset,
            and_filter = and_filter,
            or_filter= or_filter,
            order_by= order_by,
            group_by= group_by
        )
        db = get_db()
        rows = db.execute(query, args)
        for row in rows:
            category= DiseaseCategory(row['category_id'], 
                                      row['category_name'])
            disease = DiseaseView(id =row['disease_id'],
                                  name = row['disease_name'],
                                  category =category)
            yield disease


    @classmethod
    def delete_disease(cls, disease: Disease):
        db = get_db()
        db.execute(f'DELETE FROM {cls.table_name} WHERE id = ?', (disease.id,))
        db.commit()

    @classmethod
    def get_disease_by_name(cls, name: str) -> Disease:
        db = get_db()
        row = db.execute(f'SELECT * FROM {cls.table_name} WHERE name = ?', (name,)).fetchone()
        if row is None:
            raise  MissingError('Disease does not Exist')
        return DiseaseServices._row_to_model(row, Disease)


    @staticmethod
    def update_disease(disease: Disease):

        try:
            DiseaseServices.update_data(disease)
        except DuplicateError:
            raise DuplicateError("Disease name already exist")
        return disease


class DiseaseCategoryServices(BaseServices):
    model = DiseaseCategory
    table_name = 'diseases_category'
    columns= {'id', 'category_name'}
    columns_to_update = {'category_name'}

    @classmethod
    def create_category(cls, category_name, commit=True) -> DiseaseCategory:
        db = get_db()

        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (category_name) VALUES(?)',
                       (category_name, ))
            if commit: db.commit()
            new_id = cursor.lastrowid
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            raise DuplicateError(f"Category {category_name} already exist in database")
    

class EncounterServices(BaseServices):
    table_name = 'encounters'
    model = Encounter
    columns = {'id', 'facility_id', 'date', 'policy_number', 'client_name',
               'gender', 'age', 'age_group', 'treatment', 'referral', 'doctor_name', 
               'professional_service', 'created_by', 'created_at', "ec.facility_id", 
               }

    columns_to_update = {'facility_id', 'disease_id', 'date', 'policy_number', 'client_name',
               'gender', 'age', 'age_group', 'treatment', 'referral', 'doctor_name', 
               'professional_service'}

    @classmethod
    def create_encounter(cls, facility_id: int,
                         diseases_id: List[int],
                         date: date,
                         policy_number: str,
                         client_name: str,
                         gender: str,
                         age: int,
                         treatment: Optional[str], 
                         referral: bool,
                         doctor_name: Optional[str], 
                         professional_service: Optional[str],
                         created_by: User,
                         commit=True) -> Encounter:
        db = get_db()
        gender = gender.upper()
        if gender not in ('M', 'F'):
            raise ValidationError("Gender can only be male or female")
        if age < 0 or age > 120:
            raise ValidationError("Age can only be between 0 and 120")
        import re
        if not re.match(r'^[A-Za-z]{3}/\d+/\d+/[A-Za-z]/[0-5]$', policy_number):
            raise ValidationError('Invalid Policy number')
        
        try:
            FacilityServices.get_by_id(facility_id)
        except MissingError:
            raise InvalidReferenceError('You cannot add Encounter to a facility that does not exists')
        

        created_at = datetime.now().date()
        try:
        
            cur = db.execute(f'''INSERT INTO {cls.table_name} (facility_id, date, policy_number
                   , client_name, gender, age, treatment, referral, doctor_name,
                   professional_service, created_by, created_at) VALUES( ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?)''', (facility_id, date, policy_number, client_name,
                                   gender, age, treatment, int(referral), doctor_name,
                                   professional_service, created_by.id, created_at))
            new_id = cur.lastrowid

            diseases_list = list(set((new_id, x) for x in diseases_id))
            db.executemany('''INSERT into encounter_diseases(encounter_id, disease_id)
                           VALUES(?, ?)''', diseases_list)
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
    def get_all(cls, 
        limit: int = 0,
        offset: int = 0, 
        and_filter: Optional[List[Tuple]] = None,
        or_filter: Optional[List[Tuple]] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        group_by: Optional[List[str]] = None
    ) -> Iterator:
    
        query = '''
            SELECT 
                ec.id,
                ec.client_name,
                ec.gender,
                ec.age,
                ec.policy_number,
                ec.referral ,
                ec.date,
                ec.doctor_name,
                ec.professional_service,
                ec.created_at,
                ec.treatment,
                fc.name AS facility_name,
                fc.local_government AS lga,
                u.username AS created_by
            FROM encounters AS ec
            JOIN facility AS fc ON ec.facility_id = fc.id
            LEFT JOIN users AS u ON ec.created_by = u.id
        '''
        
        query, args = cls._apply_filter(
            base_query=query,
            base_arg=[],
            limit=limit,
            offset=offset,
            and_filter=and_filter,
            or_filter=or_filter,
            group_by= group_by,
            order_by=order_by
        )
        
        db = get_db()
        encounters_rows = db.execute(query, args).fetchall()
        
        # Get all encounter IDs
        encounter_ids = [row['id'] for row in encounters_rows]
        
        if not encounter_ids:
            return
        
        # Fetch all diseases for these encounters in ONE query
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
        
        from collections import defaultdict
        diseases_by_encounter = defaultdict(list)
        from app.models import EncounterView, DiseaseView, FacilityView
        
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
        
        for row in encounters_rows:
            facility = FacilityView(
                name=row['facility_name'],
                lga=row['lga']
            )
            
            encounter = EncounterView(
                id=row['id'],
                facility=facility,
                diseases=diseases_by_encounter[row['id']],  
                policy_number=row['policy_number'],
                client_name=row['client_name'],
                referral=bool(row['referral']),
                gender=row['gender'],
                date=row['date'],
                age=row['age'],
                treatment=row['treatment'],
                doctor_name=row['doctor_name'],
                professional_service=row['professional_service'],
                created_by=row['created_by'],
                created_at=row['created_at']
            )
            
            yield encounter

    @classmethod
    def get_encounter_by_facility(cls, facility_id: int) -> Iterator:
        return cls.get_all(and_filter=[('ec.facility_id', facility_id, '=')])


    @classmethod
    def update_data(cls, model):
        # do not allow update of encounter
        raise NotImplementedError("Encounter are immutable and cannot be updated")


class DashboardServices(BaseServices):
    model = None
    table_name = None

    @classmethod
    def get_top_facilities(cls, start_date: Optional[date] = None, end_date: Optional[date] = None, limit: int = 5):
        start_date, end_date, limit = cls._validate_date(start_date, end_date, limit)
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
                    AND ec2.date >= ? AND ec2.date <= ?
                 GROUP BY ecd.disease_id
                 ORDER BY COUNT(ecd.disease_id) DESC 
                 LIMIT 1
            ) AS top_disease, 
           MAX(ec.created_at) as last_submission
           FROM encounters as ec
            JOIN facility as fc on ec.facility_id = fc.id
            WHERE ec.date >= ? AND ec.date <= ?
            GROUP BY ec.facility_id
            ORDER BY encounter_count DESC
            LIMIT ?
          '''

        return cls._run_query(query, (start_date, end_date, start_date, end_date, limit),
                              lambda row: {'facility_name': row['facility_name'], 
                                           'encounter_count': row['encounter_count'],
                                           'top_disease': row['top_disease'],
                                           'last_submission': row['last_submission']})

    @classmethod
    def get_top_facility(cls):

        query = '''
            SELECT fc.name AS facility_name, COUNT(ec.id) AS encounter_count
            FROM facility AS fc
            JOIN encounters AS ec ON fc.id = ec.facility_id 
            GROUP BY ec.facility_id
            ORDER BY encounter_count DESC
            LIMIT 1
        '''
        return cls._run_query(query,  [],
                               lambda row: {'facility_name': row['facility_name'], 'encounter_count': row['encounter_count']})

    @classmethod
    def _validate_date(cls, start_date, end_date, limit):
        today = datetime.today().date()
        start_date = start_date or  today.replace(day=1)
        end_date = end_date or today

        if end_date < start_date:
            raise ValidationError("Invalid Date Range")
        if limit <= 0:
            raise ValidationError("Invalid Display Row")

        return start_date, end_date, limit

    @classmethod
    def _run_query(cls, query: str, params: tuple, row_mapper ):
        db = get_db()
        rows = db.execute(query, params)
        return [ row_mapper(row) for row in rows]


    @classmethod
    def  top_diseases(cls, start_date: Optional[date] = None, end_date: Optional[date]= None, facility_id: Optional[int] = 0, limit: int = 5):
        start_date, end_date, limit = cls._validate_date(start_date, end_date, limit)
        query = '''
             SELECT dis.name AS disease_name, COUNT(dis.id) AS disease_count
             FROM encounters AS ec
             JOIN encounters_diseases as ecd ON ecd.encounter_id = ec.id
             JOIN diseases as dis ON ecd.disease_id = dis.id
             WHERE ec.date >= ? AND ec.date <= ?
             GROUP BY dis.id
             ORDER BY disease_count DESC
             LIMIT ?
        '''

        return cls._run_query(query, 
                            (start_date, end_date, limit),
                            lambda row: {'disease_name': row['disease_name'], 'disease_count': row['disease_count']})

    @classmethod
    def gender_distribution(cls, start_date: Optional[date] = None, end_date: Optional[date] = None, facility_id: Optional[int] =0,  limit: int = 5):
        start_date, end_date, limit = cls._validate_date(start_date, end_date, limit)
        query = '''
            SELECT 
              CASE
                WHEN ec.gender = 'M' THEN 'Male'
                WHEN ec.gender = 'F' THEN 'Female'
              END as gender,
            COUNT(ec.gender) as gender_count
            FROM encounters AS ec
            WHERE ec.date >= ? AND ec.date <= ?
            GROUP BY ec.gender
            LIMIT ?
        '''
        return cls._run_query(query,
                              (start_date, end_date, limit,),
                              lambda row: {'gender': row['gender'], 'gender_count': row['gender_count']})

    @classmethod
    def age_group_distribution(cls, start_date: Optional[date] = None, end_date: Optional[date] = None, facility_id: Optional[int] = 0, limit: int = 5):
        start_date, end_date, limit = cls._validate_date(start_date= start_date, end_date = end_date, limit = limit)
        query = '''
            SELECT age_group, COUNT(*) as age_group_count 
            FROM encounters as ec
            WHERE ec.date >= ? AND ec.date <= ?
            GROUP BY ec.age_group
            LIMIT ?
        '''
        return cls._run_query(query,
                              (start_date, end_date, limit),
                              lambda row: {'age_group': row['age_group'], 'age_group_count': row['age_group_count']})

    @classmethod
    def trend_last_n_weeks(cls, n: int = 8, facility_id: Optional[int] = None):
        import pandas as pd
        if n < 0:
            raise ValidationError("Invalid month range")

        today = datetime.today().date()
        start_date = today -timedelta(weeks=n)

        query = '''
            SELECT date, COUNT(date) as date_count
            FROM encounters 
            WHERE date >= ?
            GROUP BY date
        '''
        db = get_db()
        rows = db.execute(query, (start_date, )).fetchall()
        df = pd.DataFrame(rows, columns=['date', 'date_count'])

        if df.empty:
            return pd.DataFrame(columns=['date', 'date_count']).to_json(orient="records")
        # print(df)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('W')
        trend: pd.DataFrame = df.groupby(df['date'])["date_count"].sum().reset_index()
        all_week = pd.period_range(start = start_date, end = today, freq='W')
        trend =trend.set_index('date').reindex(all_week, fill_value=0).reset_index()
        trend.rename(columns={'index': 'date'}, inplace=True)
        trend['date'] = trend['date'].astype(str)
        return trend.to_json(orient="records")

    @classmethod
    def trend_last_n_days(cls, n: int = 7, facility_id: Optional[int] = None):
        import pandas as pd
        if n <= 0:
            raise ValidationError("Invalid day range")

        today = datetime.today().date()
        start_date = today - timedelta(days=n)

        query = '''
            SELECT date, COUNT(date) AS date_count
            FROM encounters
            WHERE date >= ?
            GROUP BY date
        '''
        db = get_db()
        rows = db.execute(query, (start_date,)).fetchall()

        df = pd.DataFrame(rows, columns = ['date', 'date_count'])
        if df.empty:
            return pd.DataFrame(columns=['day', 'date_count']).to_json(orient="records")

        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('D')
        trend = df.groupby('date')['date_count'].sum().reset_index()

        all_days = pd.period_range(start=start_date, end=today, freq='D')
        trend = trend.set_index('date').reindex(all_days, fill_value=0).reset_index()
        trend.rename(columns={'index': 'day'}, inplace=True)
        trend['day'] = trend['day'].astype(str)

        return trend.to_json(orient="records")

    @classmethod
    def encounter_last_this_month(cls):
        from datetime import timedelta
        import json
        this_month_end = datetime.now().date()
        this_month_start = this_month_end.replace(day=1)

        last_month_end = this_month_start - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        query = '''
        SELECT month_range, COUNT(*) as encounter_count
        FROM (
            SELECT
                CASE
                    WHEN date >= ? AND date <= ? THEN 'LM'
                    WHEN date >= ? AND date <= ? THEN 'TM'
                END AS month_range
            FROM encounters
            WHERE date >= ? AND date <= ?
       ) sub
       GROUP BY month_range;
       '''
        db = get_db()
        row = db.execute(query, (last_month_start, last_month_end,
            this_month_start, this_month_end,
            last_month_start, this_month_end)).fetchall()
        json_result = json.dumps({r["month_range"]: r["encounter_count"] for r in row})
        return json_result

