import sqlite3
from typing import Optional, Iterator
from werkzeug.security import generate_password_hash, check_password_hash

from app.db import get_db
from app.models import User, Facility, Role, UserView, FacilityView
from app.exceptions import (
    ValidationError, MissingError, InvalidReferenceError,
    DuplicateError, AuthenticationError
)
from app.filter_parser import Params, FilterParser

from .base import BaseServices, _legacy_to_params
from .facility import FacilityServices

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
                fc.ownership,
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
                    scheme=scheme_map[row['facility_id']],
                    ownership = row['ownership']
                )
            yield UserView(
                id=row['user_id'],
                facility=facility_view,
                username=row['username'],
                role=Role[row['role']],
            )

    @classmethod
    def get_view_by_id(cls, id: int):
        params = Params().where(User, 'id', '=', id)
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        try:
            return next(cls.get_all(**res))
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
