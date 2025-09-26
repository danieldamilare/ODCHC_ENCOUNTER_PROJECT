from werkzeug.security import generate_password_hash, check_password_hash
from typing import Optional, Type, TypeVar, Any
from app.db import get_db, close_db
from app.models import User, Facility, Disease, Encounter, DiseaseCategory
from app.exceptions import ServiceError, MissingError, InvalidReferenceError, DuplicateError
from app.exceptions import ValidationError, AuthenticationError
from datetime import datetime, date
from app import app
from app.config import LOCAL_GOVERNMENT
import sqlite3


T = TypeVar('T')
class BaseServices:
    @staticmethod
    def _row_to_model(row, model_cls: Type[T]) -> T:
        if row is None:
            raise MissingError("Invalid Row Data")
        return model_cls(**row)
    
    @staticmethod
    def _get_by_id(table: str, model, id: int):
        db = get_db()
        row = db.execute(f'SELECT * FROM {table} WHERE id = ?', (id,)).fetchone()
        if row is None:
            raise MissingError(f"{table.capitalize()} with id {id} not found")
        return model(**row)

    @staticmethod
    def list_row_by_page(table: str,
                         offset: Any,
                           page: int = app.config['ADMIN_PAGE_PAGINATION'],
                           column: Optional[str] = None,
                           like: Optional[str] = None):
        db = get_db()
        allowed_tables = {'users', 'facility', 'diseases', 'diseases_category', 'encounters'}
        if table not in allowed_tables:
            raise ValidationError("Invalid table name")
        try:
            offset = (int(offset) - 1) * page
        except:
            raise ValidationError("Invalid listing page")
        query = f"SELECT * FROM {table}"
        args = []
        if column and like:
            # Whitelist columns per table if needed
            query += f" WHERE {column} = ?"
            args.append(like)
        query += " LIMIT ? OFFSET ?"
        args.extend([page, offset])
        rows = db.execute(query, args).fetchall()
        return rows
    

class UserServices(BaseServices):
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
    def get_encounter_by_facility(facility_name: str, offset: Any):
        db = get_db()
        facility = db.execute('SELECT * FROM facility WHERE name = ?', (facility_name,)).fetchone()
        if facility is None:
            raise InvalidReferenceError("Facility does not exist in the database")
        facility_id = facility['id']
        rows = EncounterServices.list_row_by_page('encounters', offset, column='facility_id', like=str(facility_id))
        for row in rows:
            yield EncounterServices._row_to_model(row, Encounter)

    @staticmethod
    def get_encounter_by_id(id: int) -> Encounter:
        return EncounterServices._get_by_id('encounters', Encounter, id)