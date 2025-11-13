from dataclasses import dataclass, fields
from datetime import datetime, date
from flask_login import UserMixin, current_user
from enum import Enum, auto
from app.exceptions import MissingError
from app import login
from typing import Optional, List
from app.constants import EncType, BabyOutcome, DeliveryMode

class Role(Enum):
    admin = auto()
    user = auto()
@dataclass
class Model:
    @classmethod
    def validate_col(cls, col: str):
        for field in fields(cls):
            if (field.name == col):
                return True
        return False
@dataclass
class User(Model):
    id: int
    username: str
    facility_id: int
    role: Role
    password_hash: str

    @classmethod
    def get_name(cls):
        return "User"

@dataclass
class Facility(Model):
    id: int
    name: str
    local_government: str
    # (health center -> primary, hospital -> secondary, private)
    facility_type: str

    @classmethod
    def get_name(cls) -> str:
        return "Facility"
@dataclass
class Disease(Model):
    id: int
    name:  str
    category_id:  int

    @classmethod
    def get_name(cls) -> str:
        return "Disease"

@dataclass
class DiseaseCategory(Model):
    id: int
    category_name: str

    @classmethod
    def get_name(cls) -> str:
        return "Disease Category"
@dataclass
class Encounter(Model):
    id: int
    facility_id: int
    date: date
    policy_number: str
    client_name: str
    gender: str
    age: int
    age_group: str
    scheme: int
    nin: str
    phone_number: str
    enc_type: EncType
    treatment: Optional[str]
    outcome: str
    doctor_name: Optional[str]
    created_by: int
    created_at: datetime

    @classmethod
    def get_name(cls) -> str:
        return "Encounter"

@dataclass
class InsuranceScheme(Model):
    id: int
    scheme_name: str
    color_scheme: str

    @classmethod
    def get_name(cls) -> str:
        return "Insurance Scheme"
@dataclass
class ServiceCategory(Model):
    id: int
    name: str

    @classmethod
    def get_name(cls) -> str:
        return "Service Category"

@dataclass
class Service(Model):
    id: int
    name: str
    category_id: int

    @classmethod
    def get_name(cls) -> str:
        return "Service"
@dataclass
class TreatmentOutcome(Model):
    id: int
    name: str
    type: str

    @classmethod
    def get_name(cls) -> str:
        return "Treatment Outcome"

@dataclass
class FacilityScheme(Model):
    facility_id: int
    scheme_id: int

@dataclass
class EncounterDiseases(Model):
    encounter_id: int
    disease_id: int

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
class ServiceView:
    id: int
    name: str
    category: ServiceCategory
@dataclass
class EncounterView:
    id: int
    policy_number: str
    client_name: str
    gender: str
    date: date
    age: int
    nin: str
    phone_number: str
    enc_type: EncType
    treatment: Optional[str]
    doctor_name: Optional[str]
    treatment_outcome: TreatmentOutcome
    created_by: str
    created_at: datetime
    insurance_scheme: InsuranceScheme
    facility: FacilityView
    diseases: List[DiseaseView]
    services: List[ServiceView]

    @property
    def diseases_name(self):
        return ', '.join([disease.name for disease in self.diseases])

    @property
    def service_name(self):
        return ', '.join([service.name for service in self.services])
@dataclass
class ANCRegistry:
    id: int
    orin: str
    kia_date: date
    booking_date: date
    client_name: Optional[str]
    parity: int
    place_of_issue: str
    hospital_number: str
    address: str
    lmp: date
    expected_delivery_date: date
    anc_count: int
    status: str
@dataclass
class DeliveryBaby:
    id: int
    gender: str
    outcome: BabyOutcome

@dataclass
class ChildHealth:
    id: int
    orin: str
    dob: date
    address: str
    guardian_name: str

@dataclass
class DeliveryEncounter:
    id: int
    mode_of_delivery: DeliveryMode
    anc_count: int
    babies: List[DeliveryBaby]
@dataclass
class ANCEncounterView(EncounterView):
    anc: ANCRegistry
@dataclass
class DeliveryEncounterView(EncounterView):
    delivery: DeliveryEncounter
@dataclass
class ChildHealthEncounterView(EncounterView):
    health_details: ChildHealth
@dataclass
class UserView:
    id: int
    username: str
    role: Role
    facility: Optional[FacilityView]
class AuthUser(UserMixin, UserView):
    def __init__(self, user: UserView):
        super().__init__(**user.__dict__)

    def get_id(self) -> str:
        return str(self.id)

@login.user_loader
def load_user(id: str) -> Optional[AuthUser]:

    from app.services import UserServices
    # print("In here")
    try:
        user: UserView = UserServices.get_view_by_id(int(id))
        # print(user)
    except MissingError:
        return None
    # print("In load user")
    return AuthUser(user)


def is_logged_in() -> bool:
    return current_user.is_authenticated


def get_current_user() -> Optional[AuthUser]:
    return current_user if current_user.is_authenticated else None
