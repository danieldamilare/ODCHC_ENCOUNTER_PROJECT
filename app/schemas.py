from typing import Optional, List, Literal
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from datetime import datetime, date
from .constants import AgeGroup, ModeOfEntry, BabyOutcome, DeliveryMode
import re
from decimal import Decimal


class BaseEncounterSchema(BaseModel):
    model_config = ConfigDict(use_enum_values=True)
    policy_number: str
    client_name: str
    date: date
    gender: Literal['M', 'F']
    age: int = Field(ge=0, le=120)
    age_group: Optional[AgeGroup] = None
    phone_number: str
    hospital_number: str
    address: str
    nin: str
    facility_id: int
    outcome: int
    referral_reason: Optional[str] = None
    investigation: Optional[str] = None
    investigation_cost: Optional[Decimal] = None
    treatment: Optional[str] = None
    doctor_name: str = Field(min_length=2)
    medication: Optional[str] = None
    medication_cost: Optional[Decimal] = None
    treatment_cost: Optional[Decimal] = None
    mode_of_entry: ModeOfEntry

    @field_validator('phone_number')
    @classmethod
    def validate_phone_number(cls, field):
        pattern = re.compile(r'^(0\d{10}|\+234\d{10})$')
        if not pattern.match(field):
            raise ValueError("Invalid Nigerian phone number format")
        return field

    @field_validator('nin')
    @classmethod
    def validate_nin(cls, field):
        pattern = re.compile(r'^\d{11}$')
        if not pattern.match(field):
            raise ValueError("NIN must be exactly 11 digits")
        return field


class ANCEncounterSchema(BaseEncounterSchema):
    gender: Literal['F'] = 'F'
    kia_date: date
    place_of_issue: str
    booking_date: date
    lmp: date
    parity: int = Field(ge=0)

    @field_validator('policy_number')
    @classmethod
    def validate_orin(cls, field):
        pattern = re.compile(r'^\d{10}$')
        if not pattern.match(field):
            raise ValueError("ORIN must be exactly 10 digits")
        return field


class BabyForm(BaseModel):
    model_config = ConfigDict(use_enum_values=True)
    gender: Literal['M', 'F']
    outcome: BabyOutcome


class DeliveryEncounterSchema(ANCEncounterSchema):
    no_of_babies: int = Field(ge=1, default=1)
    mode_of_delivery: DeliveryMode
    babies: List[BabyForm]

    @model_validator(mode='after')
    def validate_baby_count(self) -> 'DeliveryEncounter':
        if self.no_of_babies != len(self.babies):
            raise ValueError(
                f"no_of_babies is {self.no_of_babies} but {len(self.babies)} baby record(s) were provided"
            )
        return self


class EncounterSchema(BaseEncounterSchema):
    diseases: List[int] = Field(default_factory=list)
    services: List[int] = Field(default_factory=list)


class ChildHealthEncounterSchema(EncounterSchema):
    dob: date
    guardian_name: str

    @field_validator('policy_number')
    @classmethod
    def validate_orin(cls, field):
        pattern = re.compile(r'^\d{10}$')
        if not pattern.match(field):
            raise ValueError("ORIN must be exactly 10 digits")
        return field
