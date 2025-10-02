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
    def get_total(cls)-> int:
        query = f'SELECT COUNT(*) from {cls.table_name}'
        db = get_db()
        return int(db.execute(query).fetchone()[0])

    @classmethod
    def get_all(cls, 
            limit: int = 0,
            offset: int = 0, 
            and_filter: Optional[List[Tuple]] = None,
            or_filter: Optional[List[Tuple]] = None,
            order_by: Optional[List[Tuple[str, str]]] = None,
            group_by: Optional[List[str]] = None
            ) -> Iterator:
    
        ALLOWED_OPERATORS = {'=', '>', '<', '>=', '<=', '!=', 'LIKE'}
        query = ''
        args = []
        conditions = []

        query = f"SELECT * FROM {cls.table_name}"
        
        if and_filter:
            for column_name, value, opt in and_filter:
                if column_name not in cls.columns:
                    raise ValidationError("Invalid Column access")
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
            col, direction = order_by
            if col not in cls.columns:
                raise ValidationError(f"Invalid column for order_by: {col}")
            if direction.upper() not in ['ASC', 'DESC']:
                raise ValidationError(f"Invalid sort direction: {direction}")
            query += f" ORDER BY {col} {direction.upper()}"

        
        if limit > 0:
            query += ' LIMIT ?'
            args.append(limit)
        
        if offset > 0:
            query += ' OFFSET ?'
            args.append(offset)
        print(query)
        db = get_db()
        try:
            rows = db.execute(query, args)
            for row in rows:
                    yield cls._row_to_model(row, cls.model)
        except :
            raise ValidationError('Invalid limit selection')


    @classmethod
    def has_next_page(cls, page: int) -> bool:
        total = cls.get_total()
        current = page * app.config['ADMIN_PAGE_PAGINATION']
        if (current < total): return True
        return False


class UserServices(BaseServices):
    model = User
    table_name = 'users';
    columns_to_update = {'username', 'facility_id', 'role'}
    columns = {'id', 'username', 'facility_id', 'role', 'password_hash'}

    @classmethod
    def create_user(cls, username: str, facility_id: int, password: str, role=None) -> User:
        password_hash = generate_password_hash(password)
        role = ('admin' if role == Role.admin else 'user')
        try:
            FacilityServices.get_by_id(facility_id)
        except MissingError:
            raise InvalidReferenceError("You can't attach user to a facility that does not exists")

        db = get_db()
        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name}(username, password_hash, facility_id, role)'
                   ' VALUES (?, ?, ?, ?)', (username, password_hash,
                   facility_id, role))
            db.commit()
            new_id: int = cursor.lastrowid
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            raise DuplicateError('Username exists! Please use another username')

    @classmethod
    def get_user_by_username(cls, username: str) -> User:
        db = get_db()
        try:
            row = db.execute(f'SELECT * FROM {cls.table_name} where username = ?', [username]).fetchone()
        except sqlite3.IntegrityError:
            raise MissingError(f"User with {username} cannot be found")

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
    def create_facility(cls, name: str, local_government: str, facility_type: str) -> Facility:
        db = get_db()
        if local_government.lower() not in FacilityServices.LOCAL_GOVERNMENT:
            raise ValidationError("Local Government does not exist in Akure")
        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (name, local_government, facility_type) VALUES (?, ?, ?)', (name, local_government, facility_type))
            db.commit()
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
    def create_disease(cls, name: str, category_id: int) -> Disease:
        db = get_db()
        try:
            DiseaseCategoryServices.get_by_id(category_id)
        except MissingError:
            raise InvalidReferenceError('Disease Category does not exist')
        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (name, category_id) VALUES (?, ?)', [disease_name,category_id])
            db.commit()
            new_id = cursor.lastrowid
            return  cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            raise DuplicateError(f'Disease {disease_name} already exists')

    @classmethod
    def delete_disease(cls, disease: Disease):
        db = get_db()
        db.execute(f'DELETE FROM {cls.table_name} WHERE id = ?', (disease.id,))
        db.commit()

    @classmethod
    def get_disease_by_name(cls, disease_name: str) -> Disease:
        db = get_db()
        row = db.execute(f'SELECT * FROM {cls.table_name} WHERE name = ?', (disease_name,)).fetchone()
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
    def create_category(cls, category_name) -> DiseaseCategory:
        db = get_db()

        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (category_name) VALUES(?)',
                       (category_name, ))
            db.commit()
            new_id = cursor.lastrowid
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            raise DuplicateError(f"Category {category_name} already exist in database")
    

class EncounterServices(BaseServices):
    table_name = 'encounters'
    model = Encounter
    columns = {'id', 'facility_id', 'disease_id', 'date', 'policy_number', 'client_name',
               'gender', 'age', 'age_group', 'treatment', 'referral', 'doctor_name', 
               'professional_service', 'created_by', 'created_at'}

    columns_to_update = {'facility_id', 'disease_id', 'date', 'policy_number', 'client_name',
               'gender', 'age', 'age_group', 'treatment', 'referral', 'doctor_name', 
               'professional_service'}

    @classmethod
    def create_encounter(cls, facility_id: int,
                         disease_id: int,
                         date: date,
                         policy_number: str,
                         client_name: str,
                         gender: str,
                         age: int,
                         treatment: Optional[str], 
                         referral: bool,
                         doctor_name: Optional[str], 
                         professional_service: Optional[str],
                         created_by: User) -> Encounter:
        db = get_db()
        if gender.lower() not in ('m', 'f'):
            raise ValidationError("Gender can only be male or female")
        if age < 0 or age > 120:
            raise ValidationError("Age can only be between 0 and 120")
        import re
        if not re.match(r'\w{3}/\d+/\d+/\w/[012345]', policy_number):
            raise ValidationError('Invalid Policy number')
        
        created_by:int = created_by.id

        try:
            FacilityServices.get_by_id(facility_id)
        except MissingError:
            raise InvalidReferenceError('You cannot add Encounter to a facility that does not exists')
        
        try:
            DiseaseServices.get_by_id(disease_id)
        except MissingError:
            raise InvalidReferenceError("Disease is not valid")

        try:
            UserServices.get_by_id(created_by)
        except MissingError:
            raise InvalidReferenceError("Unregistered User can't input data")
            
        created_at = datetime.now().date()
        try:
        
            cur = db.execute(f'''INSERT INTO {cls.table_name} (facility_id, disease_id, date, policy_number
                   , client_name, gender, age, treatment, referral, doctor_name,
                   professional_service, created_by, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?)''', (facility_id, disease_id, date, policy_number, client_name,
                                   gender, age, treatment, referral, doctor_name,
                                   professional_service, created_by, created_at))
            db.commit()
            new_id = cur.lastrowid
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            raise InvalidReferenceError("Invalid facility or disease or user")

    @classmethod
    def get_encounter_by_facility(cls, facility_id: int) -> Iterator:
        return cls.get_all(and_filter=[('facility_id', facility_id, '=')])


    @classmethod
    def update_data(cls, model):
        # do not allow update of encounter
        raise NotImplementedError("Encounter are immutable and cannot be updated")