from werkzeug.security import generate_password_hash, check_password_hash
from typing import Optional, Type, TypeVar, Any, Iterator, List, Tuple
from app.db import get_db, close_db
from app.models import User, Facility, Disease, Encounter, DiseaseCategory
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
    @staticmethod
    def _row_to_model(row, model_cls: Type[T]) -> T:
        if row is None:
            raise MissingError("Invalid Row Data")
        return model_cls(**row)
    
    @staticmethod
    def _get_by_id(table: str, model: Type[T], id: int) -> T:
        db = get_db()
        row = db.execute(f'SELECT * FROM {table} WHERE id = ?', (id,)).fetchone()
        if row is None:
            raise MissingError(f"{table.capitalize()} with id {id} not found")
        return model(**row)

    @classmethod
    def list_row_by_page(cls,
                         page: int,
                         page_size = app.config['ADMIN_PAGE_PAGINATION'],
                         and_filter: Optional[List[Tuple[str, Any, str]]] = None,
                         or_filter: Optional[List[Tuple[str, Any, str]]] = None) -> Iterator:

        try:
            offset = (int(page) - 1) * page_size
            if offset < 0: raise ValueError
        except:
            raise ValidationError("Invalid listing page")

        return cls.get_all(limit=page_size, offset = offset, and_filter =and_filter,
                           or_filter = or_filter)

    @classmethod
    def get_total(cls)-> int:
        query = f'SELECT COUNT(*) from {cls.table_name}'
        db = get_db()
        return int(db.execute(query).fetchone()[0])

    @classmethod
    @classmethod
    def get_all(cls, 
            limit: int = 0,
            offset: int = 0, 
            and_filter: Optional[List[Tuple]] = None,
            or_filter: Optional[List[Tuple]] = None) -> Iterator:
    
        ALLOWED_OPERATORS = {'=', '>', '<', '>=', '<=', '!=', 'LIKE'}
        query = f'SELECT * FROM {cls.table_name}'
        args = []
        conditions = []
        
        if and_filter:
            for column_name, value, opt in and_filter:
                if opt not in ALLOWED_OPERATORS:
                    raise ValidationError(f"Invalid operator: {opt}")
                conditions.append(f"{column_name} {opt} ?")
                args.append(value)
        
        if or_filter:
            or_conditions = []
            for column_name, value, opt in or_filter:
                if opt not in ALLOWED_OPERATORS:
                    raise ValidationError(f"Invalid operator: {opt}")
                or_conditions.append(f"{column_name} {opt} ?")
                args.append(value)
            if or_conditions:
                conditions.append("(" + " OR ".join(or_conditions) + ")")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        if limit > 0:
            query += ' LIMIT ?'
            args.append(limit)
        
        if offset > 0:
            query += ' OFFSET ?'
            args.append(offset)
        
        db = get_db()
        try:
            rows = db.execute(query, args)
            for row in rows:
                yield BaseServices._row_to_model(row, cls.model)
        except sqlite3.IntegrityError:
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
    @staticmethod
    def create_user(username: str, facility_id: int, password: str) -> User:
        password_hash = generate_password_hash(password)
        db = get_db()
        try:
            user = UserServices.get_user_by_username(username)
            raise DuplicateError('Username exists! Please use another username')
        except MissingError:
            pass

        try:
            facility = FacilityServices.get_facility_by_id(facility_id)
        except MissingError:
            raise InvalidReferenceError('Facility Does not exists! You can\'t attach it to User')

        db.execute('INSERT INTO users(username, password_hash, facility_id)'
                   ' VALUES (?, ?, ?)', (username, password_hash,
                   facility_id))
        db.commit()
        return  UserServices.get_user_by_username(username)

    @staticmethod
    def get_user_by_username(username: str) -> User:
        db = get_db()
        row = db.execute('SELECT * FROM users where username = ?', [username]).fetchone()
        if row is None:
            raise MissingError("Username does not exist")
        return UserServices._row_to_model(row, User)

    @staticmethod
    def get_user_by_id(id: int) -> User:
        # will probably be used by flask login
        return UserServices._get_by_id("users", User, id)

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

    @staticmethod
    def update_user_details(user: User)->User:
        db = get_db()
        field = [f"{key}=?" for key in vars(user).keys() if key != 'id']
        values = [v for k, v in vars(user).items() if k != 'id']
        try:
            facility = FacilityServices.get_facility_by_id(user.facility_id)
        except MissingError:
            raise InvalidReferenceError('Facility Does not exists! You can\'t attach it to User')

        try:
            db.execute(f'UPDATE users SET {",".join(field)} WHERE id = ?', values + [user.id])
            db.commit()
        except sqlite3.IntegrityError:
            raise DuplicateError(f"You can't change username to {user.username}")
        return user

    @staticmethod
    def update_user_password(user: User, password: str) ->User:
        db = get_db()
        password_hash = generate_password_hash(password)
        user.password_hash = password_hash
        return UserServices.update_user_details(user)

    @staticmethod
    def delete_user(user: User):
        db = get_db()
        db.execute("DELETE FROM users where id = ?", [user.id])
        db.commit()


class FacilityServices(BaseServices):
    table_name = 'facility'
    model = Facility
    LOCAL_GOVERNMENT = LOCAL_GOVERNMENT
    @staticmethod
    def create_facility(name: str, local_government: str, facility_type: str) -> Facility:
        db = get_db()
        if local_government.lower() not in FacilityServices.LOCAL_GOVERNMENT:
            raise ValidationError("Local Government does not exist in Akure")

        try:
            FacilityServices.get_facility_by_name(name)
            raise DuplicateError("Facility with the same name exists")
        except MissingError:
            pass
        db.execute('INSERT INTO facility (name, local_government, facility_type) VALUES (?, ?, ?)', (name, local_government, facility_type))
        db.commit()
        return FacilityServices.get_facility_by_name(name)

    @staticmethod
    def get_facility_by_name(name: str) -> Facility:
        db = get_db()
        row = db.execute('SELECT * FROM facility WHERE name = ?', (name,)).fetchone()
        if row is None:
            raise MissingError(f"No Facility with name {name}")
        return FacilityServices._row_to_model(row, Facility)

    @staticmethod
    def get_facility_by_id(id: int) -> Facility:
        return FacilityServices._get_by_id('facility', Facility, id)

    @staticmethod
    def delete_facility(facility: Facility):
        db = get_db()
        db.execute("DELETE FROM facility WHERE name =  ?",  [facility.name])
        db.commit()

    @staticmethod
    def update_facility(facility: Facility):
        db = get_db()

        if facility.local_government.lower() not in FacilityServices.LOCAL_GOVERNMENT:
            raise ValidationError("Local Government does not exist in Akure")

        fields = [f'{key} = ?' for key in vars(facility).keys() if key != 'id']
        values = [v for k, v in vars(facility).items() if k != 'id']
        try:
            db.execute(f'UPDATE facility SET {",".join(fields)} WHERE id = ?', values + [facility.id])
        except sqlite3.IntegrityError:
            raise DuplicateError(f'Facility with name {facility.name} already exists')
        db.commit()
        return facility
    

class DiseaseServices(BaseServices):
    table_name = 'diseases'
    model = Disease
    @staticmethod
    def create_disease(disease_name: str, category_id: int):
        db = get_db()
        try:
            row = DiseaseCategoryServices.get_category_by_id(category_id)
        except MissingError:
            raise InvalidReferenceError("Disease Category does not exists")

        try:
            db.execute('INSERT INTO diseases (name, category_id) VALUES (?, ?)', [disease_name, row.id])
            db.commit()
        except sqlite3.IntegrityError:
            raise DuplicateError(f'Disease {disease_name} already exists')
        return DiseaseServices.get_disease_by_name(disease_name)

    @staticmethod
    def delete_disease(disease: Disease):
        db = get_db()
        db.execute('DELETE FROM diseases WHERE name = ?', (disease.name,))
        db.commit()

    @staticmethod
    def get_disease_by_name(disease_name: str) -> Disease:
        db = get_db()
        row = db.execute('SELECT * FROM diseases WHERE name = ?', (disease_name,)).fetchone()
        if row is None:
            raise  MissingError('Disease does not Exist')
        return DiseaseServices._row_to_model(row, Disease)

    @staticmethod
    def get_disease_by_id(id: int):
        return DiseaseServices._get_by_id('diseases', Disease, id)

    @staticmethod
    def update_disease(disease: Disease):
        db = get_db()
        try:
            row = DiseaseCategoryServices.get_category_by_id(disease.category_id)
        except MissingError:
            raise InvalidReferenceError('Category does not exist! You can\'t create a disease with invalid category')

        fields = [f'{key}=?' for key in vars(disease).keys() if key != 'id']
        values = [v for k, v in vars(disease).items() if k != 'id']
        db.execute(f'UPDATE diseases SET {",".join(fields)} WHERE id = ?', values + [disease.id])
        db.commit()
        return disease
    

class DiseaseCategoryServices(BaseServices):
    model = DiseaseCategory
    table_name = 'diseases_category'
    @staticmethod
    def create_category(category_name):
        db = get_db()
        try:
            db.execute('INSERT INTO diseases_category (category_name) VALUES(?)',
                       (category_name, ))
            db.commit()
        except sqlite3.IntegrityError:
            raise DuplicateError(f"Category {category_name} already exist in database")
        row = db.execute('SELECT * FROM diseases_category WHERE category_name = ?', (category_name, )).fetchone()
        return DiseaseCategoryServices._row_to_model(row, DiseaseCategory)
    
    @staticmethod
    def get_category_by_id(id: int):
        return DiseaseCategoryServices._get_by_id('diseases_category', 
                                                 DiseaseCategory, id)
    @staticmethod
    def get_category_by_name(name: str):
        db = get_db()
        row = db.execute('SELECT * FROM diseases_category WHERE category_name = ?', (name,)).fetchone()
        if row is None:
            raise MissingError("Disease Category does not exist in database")
        return DiseaseCategoryServices._row_to_model(row, DiseaseCategory)

class EncounterServices(BaseServices):
    table_name = 'encounters'
    model = Encounter
    @staticmethod
    def create_encounter(facility_id: int,
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
                         created_by: int) -> Encounter:
        db = get_db()
        try:
            FacilityServices.get_facility_by_id(facility_id)
        except MissingError:
            raise InvalidReferenceError("Facility does not exist in the database")
        
        try:
           DiseaseServices.get_disease_by_id(disease_id) 
        except MissingError:
            raise InvalidReferenceError("Disease is not valid")

        try: 
            UserServices.get_user_by_id(created_by)
        except MissingError:
            raise InvalidReferenceError("User Cannot input Data")

        if gender.lower() not in ('m', 'f'):
            raise ValidationError("Gender can only be male or female")
        if age < 0 or age > 120:
            raise ValidationError("Age can only be between 0 and 120")
        import re
        if not re.match(r'\w{3}/\d+/\d+/\w/[012345]', policy_number):
            raise ValidationError('Invalid Policy number')
        
        cur = db.execute('''INSERT INTO encounters (facility_id, disease_id, date, policy_number
                   , client_name, gender, age, treatment, referral, doctor_name,
                   professional_service, created_by) VALUES(?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?)''', (facility_id, disease_id, date, policy_number, client_name,
                                   gender, age, treatment, referral, doctor_name,
                                   professional_service, created_by))
        db.commit()
        rowid =  cur.lastrowid
        row = db.execute('SELECT * from encounters WHERE id = ?', (rowid,)).fetchone()
        return EncounterServices._row_to_model(row, Encounter)
    
    @staticmethod
    def get_encounter_by_id(id: int) -> Encounter:
        return EncounterServices._get_by_id('encounters', Encounter, id)
