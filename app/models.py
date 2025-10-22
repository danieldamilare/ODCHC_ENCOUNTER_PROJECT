from dataclasses import dataclass
from datetime import datetime, date, time
from flask_login import UserMixin, current_user
from enum import Enum, auto
from app.exceptions import MissingError
from app import login
from typing import Optional, List

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

    @property
    def get_name(self):
        return f"User"


@dataclass
class Facility:
    id: int
    name: str
    local_government: str
    facility_type: str #(health center -> primary, hospital -> secondary, private)

    @property
    def get_name(self) -> str:
        return f"Facility"

@dataclass
class Disease:
    id: int
    name:  str
    category_id:  int

    @property
    def get_name(self) -> str:
        return f"Disease"


@dataclass
class DiseaseCategory:
    id: int
    category_name: str

    @property
    def get_name(self) -> str:
        return f"Disease Category"


@dataclass
class Encounter:
    id: int
    facility_id: int
    date: date
    policy_number: str
    client_name: str
    gender: str
    age: int
    age_group: str
    scheme: int
    treatment: Optional[str]
    outcome: str
    doctor_name: Optional[str]
    created_by: int
    created_at: datetime

    @property
    def get_name(self) -> str:
        return "Encounter"

@dataclass
class InsuranceScheme:
    id: int
    scheme_name: str
    color_scheme: str

    @property
    def get_name(self) -> str:
        return "Insurance Scheme"

@dataclass
class TreatmentOutcome:
    id: int
    name: str
    type: str

@dataclass


@dataclass
class FacilityView:
    id: int
    name: str
    lga: str
    scheme: List[InsuranceScheme]
    facility_type: str

@dataclass
class DiseaseView:
    id: int
    name: str
    category: DiseaseCategory


@dataclass
class EncounterView:
    id: int
    policy_number: str
    client_name: str
    gender: str
    date: date
    age: int
    treatment: Optional[str]
    doctor_name: Optional[str]
    treatment_outcome: TreatmentOutcome
    created_by: str
    created_at: datetime
    insurance_scheme: InsuranceScheme
    facility: FacilityView
    diseases: List[DiseaseView]

    @property
    def diseases_name(self):
        return ', '.join([disease.name for disease in self.diseases])


@dataclass
class UserView:
    id: int
    username: str
    role: Role
    facility: Optional[FacilityView]


class AuthUser(UserMixin, UserView):
    def __init__(self, user: UserView):
        super().__init__(**user.__dict__)
    def  get_id(self) -> str:
        return str(self.id)

@login.user_loader
def load_user(id:str) -> Optional[AuthUser]:

    from app.services import User, UserServices
    print("In here")
    try:
        user:UserView = UserServices.get_view_by_id(int(id))
        print(user)
    except MissingError:
        return None
    print("In load user")
    return AuthUser(user)

def is_logged_in() -> bool:
    return current_user.is_authenticated

def get_current_user() -> Optional[AuthUser]:
    return current_user if current_user.is_authenticated else None
