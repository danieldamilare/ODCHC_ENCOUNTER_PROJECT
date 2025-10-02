from dataclasses import dataclass
from datetime import datetime, date, time
from flask_login import UserMixin, current_user
from enum import Enum, auto
from app.exceptions import MissingError
from app import login
from typing import Optional

class Role(Enum):
    admin = auto()
    user = auto()

@dataclass
class User:
    id: int
    username: str
    facility_id: int
    role: Role
    password_hash: str

@dataclass
class Facility:
    id: int
    name: str
    local_government: str
    facility_type: str #(health center -> primary, hospital -> second, specialist hospital -> tertiary)

@dataclass
class Disease:
    id: int
    name:  str
    category_id:  int

@dataclass
class DiseaseCategory:
    id: int
    category_name: str


@dataclass
class Encounter:
    id: int
    facility_id: int
    disease_id: int
    date: date
    policy_number: str
    client_name: str
    gender: str
    age: int
    age_group: str
    treatment: Optional[str]
    referral: bool
    doctor_name: Optional[str]
    professional_service: Optional[str]
    created_by: int
    created_at: datetime


@dataclass
class UserView:
    id: int
    username: str
    facility: Facility
    role: Role
    password_hash: str


@dataclass
class DiseaseView:
    id: int
    name: str
    category: DiseaseCategory


class AuthUser(UserMixin):
    def __init__(self, user: User):
        self.user = user
    def  get_id(self) -> str:
        return str(self.user.id)

@login.user_loader
def load_user(id:str) -> Optional[AuthUser]:

    from app.services import User, UserServices
    try:
        user:User = UserServices.get_by_id(int(id))
    except MissingError:
        return None
    return AuthUser(user)

def is_logged_in() -> bool:
    return current_user.is_authenticated

def get_current_user() -> Optional[User]:
    if is_logged_in():
        return current_user.user
    return None