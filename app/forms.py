from wtforms import StringField, PasswordField, SubmitField, BooleanField, TextAreaField, HiddenField
from wtforms import IntegerField, SelectField, DateField, FieldList, SelectMultipleField, FileField, FormField, DecimalField
from wtforms.validators import DataRequired, Length, NumberRange, EqualTo, Optional, ValidationError, AnyOf
from app.services import InsuranceSchemeServices, FacilityServices, TreatmentOutcomeServices
from wtforms import widgets
from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed, FileRequired
from app.models import get_current_user
from app.constants import (LGA_CHOICES, DeliveryMode, BabyOutcome, FacilityType,
                           FacilityOwnerShip, ModeOfEntry, OutcomeEnum, AgeGroup)
from app.config import Config
import re

def nigerian_phone_number(form, field):
    pattern = re.compile(r'^(0\d{10}|\+234\d{10})$')
    if not pattern.match(field.data):
        raise ValidationError("Invalid Nigerian phone number format")

def validate_facility(form, field):
    user = get_current_user()
    if user.role.name == 'admin' and not field.data:
        raise ValidationError(
            "Admin User have to select a facility for encounter")

def validate_diseases(form, field):
    for disease in field:
        if not disease.data or int(disease.data) == 0:
            raise ValidationError(
                "Please select a valid disease from the list")

def validate_nin(form, field):
    pattern = re.compile(r'^\d{11}$')
    if not pattern.match(field.data):
        raise ValidationError("Invalid Nin: Nin must all be digit and 11 in length")

def validate_orin(form, field):
    print("In validate Orin")
    pattern = re.compile(r'^\d{10}$')
    if not pattern.match(field.data):
        raise ValidationError("Invalid ORIN: ORIN must all be digit and 10 in length")

class LoginForm(FlaskForm):
    username = StringField('Enter Username: ', validators=[DataRequired()])
    password = PasswordField('Enter Password: ', validators=[DataRequired()])
    remember_me = BooleanField('Remember Me')
    submit = SubmitField('Sign in')

class OutcomeMixin:
    def populate_outcome_choices(self):
        outcomes = list(TreatmentOutcomeServices.get_all())

        self.outcome.choices = (
            [('0', 'Select Treatment Outcome')] +
            [(s.id, s.name) for s in outcomes if s.type.lower() != 'death'] +
            [('-1', 'Death')]
        )

        self.death_type.choices = (
            [('0', 'Select Death Type')] +
            [(s.id, s.name) for s in outcomes if s.type.lower() == 'death']
        )
    def populate_maternal_outcome_choices(self):
        outcomes = list(TreatmentOutcomeServices.get_all())
        self.outcome.choices = (
            [('0', 'Select Treatment Outcome')] +
            [(s.id, s.name) for s in outcomes if s.type.lower() != 'death']
        )
        self.outcome.choices += [(s.id, s.name) for s in outcomes if s.type.lower() == 'death' and s.name ==  OutcomeEnum.MATERNAL_DEATH.value]
        self.death_type.choices = [('0', '')]

class FacilityMixin:
    def populate_facility_choices(self):
        """Populate facility choices from database"""
        self.facility.choices = (
            [('0', 'Select Facility')] +
            [(f.id, f.name) for f in sorted(FacilityServices.get_all(), key  =  lambda x: x.name)]
        )

    def populate_facility_type(self):
        self.facility_type.choices = ([("", "--SELECT--")] +
                                      [(x.name, x.name) for x in FacilityType])
    def populate_facility_ownership(self):
        self.ownership.choices = ([("", "--SELECT--")] +
                                  [(x.name, x.name) for x in FacilityOwnerShip])

class SchemeMixin:
    def populate_scheme_choices(self):
        """Populate scheme choices from database"""
        self.scheme.choices = (
            [('0', 'Select Scheme')] +
            [(f.id, f.scheme_name) for f in InsuranceSchemeServices.get_all()]
        )
class EncTypeForm(FlaskForm):
    encounter_list = [('ANC', 'ANC'), ('Delivery', 'Delivery'),
                      ('Child Health', 'Child Health')]
    orin = StringField("Enter ORIN Number: ", validators=[DataRequired(), validate_orin])

    enc_type = SelectField("Select Encounter Type", validators=[DataRequired()],
                           choices= encounter_list)
    submit = SubmitField("Proceed")

# 1. THE BASE FORM (Shared Fields Only)
class BaseEncounterForm(FlaskForm, FacilityMixin, OutcomeMixin):
    # --- Common Fields ---
    policy_number = StringField('Policy Number / ORIN', validators=[DataRequired()])
    client_name = StringField('Client Name', validators=[DataRequired()])
    date = DateField('Date Of Visit', format="%Y-%m-%d", validators=[DataRequired()])

    gender = SelectField('Gender', choices=[('M', 'Male'), ('F', 'Female')],
                         validators=[DataRequired(), AnyOf(('M', 'F'))])
    age = IntegerField('Age', validators=[DataRequired(), NumberRange(0, 120)])
    age_group = SelectField("Age Group", validators=[Optional()],
                            choices=[('', "Select Age Group")] + [(x.value, x.value) for x in list(AgeGroup)])

    # Contact Info
    phone_number = StringField("Phone Number", validators=[DataRequired(), nigerian_phone_number])
    hospital_number = StringField("Hospital Number", validators=[DataRequired()])
    address = StringField("Residential Address", validators=[DataRequired()])
    nin = StringField('NIN', validators=[DataRequired(), validate_nin])

    # Facility & Outcome
    facility = SelectField("Select Facility", coerce=int, validators=[validate_facility])
    outcome = SelectField('Treatment Outcome', coerce=int, validators=[DataRequired()], render_kw={'id': 'outcome-select'})
    death_type = SelectField("Death Type", coerce=int, validators=[Optional()], render_kw={'id': 'death-type-select'})
    referral_reason = StringField("Referral Reason", validators=[Optional()])

    # Medical Details
    treatment = TextAreaField('Treatment')
    doctor_name = StringField('Doctor Name', validators=[DataRequired(), Length(min=2)])

    # Costs
    investigation = TextAreaField("Investigations", validators=[Optional()])
    investigation_cost = DecimalField("Cost of Investigations", validators=[Optional()])
    medication = TextAreaField("Medications", validators=[Optional()])
    medication_cost = DecimalField("Cost of medications", validators=[Optional()])
    treatment_cost = DecimalField("Cost of Treatment", validators=[Optional()])
    total_cost = DecimalField("Total Cost", validators=[Optional()], default=0,
                              render_kw={'readonly': True, 'class': 'bg-slate-100'})

    mode_of_entry = SelectField("Mode of Entry", choices=[('', "Select Mode")] + [(m.value, m.value) for m in ModeOfEntry], validators=[DataRequired()])
    submit = SubmitField('Submit')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.populate_facility_choices()

    def validate(self, extra_validators=None):
        """Common validation logic for ALL encounters"""
        if not super().validate(extra_validators):
            return False
        # Require death type if outcome is death
        if self.outcome.data and self.outcome.data == -1 and not self.death_type.data:
            self.errors.setdefault('death_type', []).append("Please select a death type for 'Death' outcome.")
            return False

        if self.age.data == 0 and not self.age_group.data:
            self.errors.setdefault('age_group', []).append("Age Group is required for age 0")
            return False

        outcomes = list(TreatmentOutcomeServices.get_all())
        referred_outcome = next((t for t in outcomes if t.name == OutcomeEnum.REFERRED.value), None)

        if referred_outcome:
            if self.outcome.data == referred_outcome.id and not self.referral_reason.data:
                self.errors.setdefault('referral_reason', []).append("Please provide a reason for referral.")
                return False
        return True

class AddEncounterForm(BaseEncounterForm):
    diseases = FieldList(SelectField('Disease/Diagnosis', validators=[DataRequired(), validate_diseases], coerce=int))
    services = FieldList(SelectField("Services", validators=[DataRequired()], coerce=int))

    def validate(self, extra_validators=None):
        valid = super().validate(extra_validators)
        if not valid:
            return False
        # Require either disease or service
        if not any(d.data for d in self.diseases) and not any(s.data for s in self.services):
            self.errors.setdefault('services', []).append("Please select at least one disease or service.")
            return False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.populate_outcome_choices()

class ANCEncounterForm(BaseEncounterForm):
    #only for pregnant women scheme, other scheme with delivery will use the encounter form scheme
    # --- Specific Fields ---
    kia_date = DateField("Date of Issue (Kaadi Igbeayo)", validators=[DataRequired()])
    place_of_issue = StringField("Place of Issue", validators=[DataRequired()])
    booking_date = DateField("Date of Booking", validators=[Optional()])

    # Pregnancy Vitals
    lmp = DateField("LMP", validators=[DataRequired()])
    expected_delivery_date = DateField("EDD", validators=[DataRequired()])
    gestational_age = StringField("Gestational Age", validators=[DataRequired()])
    gender = SelectField('Gender', choices=[('M', 'Male'), ('F', 'Female')],
                         default= 'F', validators=[DataRequired(), AnyOf(('M', 'F'))])
    parity = IntegerField("Parity", validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.populate_maternal_outcome_choices()

class BabyForm(FlaskForm):
    gender = SelectField('Baby Gender', choices=[
                         ('M', 'Male'), ('F', 'Female')], validators=[DataRequired()])

    outcome = SelectField('Delivery Outcome',  validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        baby_outcome = [(x.value,  x.value) for x in BabyOutcome]
        self.outcome.choices = baby_outcome


class DeliveryEncounterForm(ANCEncounterForm):
    date = DateField("Date of Delivery", validators= [DataRequired()])
    anc_count = IntegerField("Number of ANC Visit", validators=[DataRequired()])
    outcome = SelectField('Pregnancy Outcome',  validators=[
                          Optional()], coerce=int, render_kw={'id': 'outcome-select'})
    no_of_babies = IntegerField("Number of babies", validators=[DataRequired()], default=1)
    mode_of_delivery = SelectField(
            "Mode of Delivery",
            validators=[DataRequired()],
            choices=[(mode.name, mode.value) for mode in DeliveryMode],
            coerce=lambda x: DeliveryMode[x] if x else None
        )

    babies = FieldList(FormField(BabyForm), min_entries=1)

    @property
    def babies_data(self):
        return [
            {
                'gender': baby_form.gender.data,
                'outcome': baby_form.outcome.data
            }
             for baby_form in self.babies
        ]

    def validate(self, extra_validators=None):
        valid = super().validate(extra_validators=extra_validators)
        if self.no_of_babies.data != len(self.babies.entries):
            self.errors.setdefault('no_of_babies', []).append("Babies Number does not match baby input data")
            return False
        return valid


class ChildHealthEncounterForm(AddEncounterForm):
    client_name = StringField("Cient Name", validators=[DataRequired()])
    dob = DateField("Date of Birth", validators = [DataRequired()])
    policy_number = StringField("ORIN", validators= [DataRequired(), validate_orin])
    nin = StringField("Parent/Guardian's NIN",  validators=[DataRequired(),validate_nin])
    guardian_name = StringField("Parent/Guardian's Name", validators=[DataRequired()])
    outcome = SelectField('Treatment Outcome',  validators=[
                          Optional()], coerce=int, render_kw={'id': 'outcome-select'})
    death_type = SelectField("Death Type", validators=[
                             Optional()], coerce=int, render_kw={'id': 'death-type-select'})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.address.label.text = "Parent's Address"
        self.phone_number.label.text = "Parent/Guardian's Phone Number"
        death = list(TreatmentOutcomeServices.get_all())
        self.death_type.choices = [(d.id, d.name) for d in death
                           if d.name != OutcomeEnum.MATERNAL_DEATH]

class AddFacilityForm(FlaskForm, SchemeMixin):
    name = StringField('Facility Name', validators=[DataRequired()])
    lga = SelectField('Local Government',
                                   choices=LGA_CHOICES,
                                   validators=[DataRequired('Please select a local government from list')])
    facility_type = SelectField('Facility Type',
                                choices = [('', 'Select Type')] + [(fc.value, fc.value) for fc in FacilityType], validators=[DataRequired()])
    ownership = SelectField("Ownership",
                            choices = [('', 'Select Ownership')] + [(fo.value, fo.value) for fo in FacilityOwnerShip],
                            validators = [DataRequired()])

    scheme = SelectMultipleField('Insurance Scheme', coerce=int,
                                 validators=[DataRequired(
                                     "Please select the insurance scheme allowed for this facility")],
                                 widget=widgets.ListWidget(),
                                 option_widget=widgets.CheckboxInput())
    submit = SubmitField('submit')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scheme.choices  = [(scheme.id, scheme.scheme_name) for scheme in InsuranceSchemeServices.get_all()]

class AddCategoryForm(FlaskForm):
    category_name = StringField('Category Name', validators=[DataRequired()])
    submit = SubmitField('Add Category')


class AddDiseaseForm(FlaskForm):
    name = StringField('Disease Name', validators=[DataRequired()])
    category_id = SelectField('Category', validators=[
                              DataRequired()], coerce=int)
    submit = SubmitField('Add Disease')

class AddServiceForm(FlaskForm):
    name = StringField('Service Name', validators=[DataRequired()])
    category_id = SelectField('Category', validators=[
                              DataRequired()], coerce=int)
    submit = SubmitField('Add Service')
class AddUserForm(FlaskForm):
    username = StringField('Username', validators=[
                           DataRequired("Please enter a username")])
    facility_id = SelectField('Facility',
                              validators=[DataRequired(
                                  "Please select facility from the list")],
                              coerce=int)
    role = SelectField("Role", validators=[DataRequired()],
                       choices=[('admin', 'Admin'), ('user', "User")],
                       default='user')
    password = PasswordField('Password', validators=[
                             DataRequired("You must enter a password")])
    password2 = PasswordField('Confirm Password', validators=[
                              DataRequired("You must enter a password"), EqualTo('password', )])
    submit = SubmitField('Add New User')

class AddInsuranceSchemeForm(FlaskForm):
    name = StringField("Insurance SCheme Name",
                       validators=[DataRequired("Enter the name of scheme to add")])
    submit = SubmitField("Add Insurance Scheme")

class DeleteUserForm(FlaskForm):
    submit = SubmitField("Delete User")


class EditDiseaseForm(FlaskForm):
    name = StringField('Name', validators=[
                       DataRequired('Please Enter name')])
    category_id = SelectField('Category', validators=[
                              DataRequired('Please select a category')])
    submit = SubmitField("Update")

class EditUserForm(FlaskForm):
    username = StringField('Username', validators=[
                           DataRequired("Please enter a username")])
    password = PasswordField('Password', validators=[
                             Optional(),  EqualTo('password2', "Password must match")])
    password2 = PasswordField('Confirm Password', validators=[Optional()])
    submit = SubmitField('Change Password')


class EditFacilityForm(AddFacilityForm):
    pass

class EncounterFilterForm(FlaskForm, FacilityMixin, SchemeMixin):
    start_date = DateField(
        'Start Date', format='%Y-%m-%d', validators=[Optional()])
    end_date = DateField('End Date', format='%Y-%m-%d',
                         validators=[Optional()])
    lga = SelectField('Local Government',
                                   choices=LGA_CHOICES,
                                   validators=[Optional()])
    facility_id = SelectField('Facility', coerce=int, validators=[Optional()])
    scheme_id = SelectField('Scheme', coerce=int, validators = [Optional()])
    outcome =SelectField("Treatment Outcome", coerce=int, validators = [Optional()])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.facility_id.choices = [('0', 'Select Facility')] + [(f.id, f.name) for f in sorted(FacilityServices.get_all(), key = lambda x: x.name)]
        self.scheme_id.choices = [('0', "Select Scheme")] + [(s.id, s.scheme_name) for s in InsuranceSchemeServices.get_all()]
        self.outcome.choices = [('0', 'Select Treatment Outcome')] +[(t.id, t.name) for t in TreatmentOutcomeServices.get_all()]

class FacilityFilterForm(FlaskForm, SchemeMixin, FacilityMixin):
    scheme = SelectField("Scheme", validators= [Optional()], coerce=int)
    lga = SelectField("LGA", validators= [Optional()], choices = LGA_CHOICES)
    name = StringField("", validators= [Optional()])
    facility_type = SelectField("Facility Type", validators = [Optional()], choices = [(x.name, x.name) for x in FacilityType])
    ownership = SelectField("Ownership", validators = [Optional()], choices = [(x.name, x.name) for x in FacilityOwnerShip])
    limit = SelectField("Number", validators=[Optional()], coerce = int, default=Config.ADMIN_PAGE_PAGINATION)
    class Meta:
        csrf = False
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.populate_facility_ownership()
        self.populate_facility_type()
        self.populate_scheme_choices()

class DiseaseFilterForm(FlaskForm):
    name = StringField("name", validators= [Optional()])
    class Meta:
        csrf = False
class ExcelUploadForm(FlaskForm):
    facility_id = SelectField('Facility', coerce=int,
                              validators=[DataRequired()])
    month = SelectField("Month", coerce=int, validators=[DataRequired()])
    excel_file = FileField("Upload Excel File", validators=[
        FileRequired(),
        FileAllowed(['xls', 'xlsx'], "Excel files only!")
    ])
    submit = SubmitField("Upload")

class DashboardFilterForm(FlaskForm):
    """Base filter form - shared across all dashboards"""
    class Meta:
        csrf = False

    scheme_id = SelectField(
        'Insurance Scheme',
        choices=[('', 'Select Schemes')],  # Populated dynamically
        coerce=lambda x: int(x) if x else None,
        validators=[Optional()]
    )

    gender = SelectField(
        'Gender',
        choices=[
            ('', 'Select Gender'),
            ('M', 'Male'),
            ('F', 'Female')
        ],
        validators=[Optional()]
    )
    submit = SubmitField("Apply")
    clear = SubmitField("Clear")

    def __init__(self, *args, **kwargs):
        """Populate dynamic choices on form init"""
        super().__init__(*args, **kwargs)

        # Populate schemes from database
        schemes = InsuranceSchemeServices.get_all()
        self.scheme_id.choices = [('', 'Select Schemes')] + [
            (str(s.id), s.scheme_name.upper()) for s in schemes
        ]

class AdminDashboardFilterForm(DashboardFilterForm):
    facility_id = SelectField('Facility',
                              choices = [('', 'Select Facilities')],
                              validators = [Optional()])
    lga =  SelectField('Local Government',
                    choices = [('', 'Select LGAs')] + LGA_CHOICES[1:],
                    validators = [Optional()])
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        facilities = [(str( f.id ), f.name.upper()) for f in sorted(FacilityServices.get_all(), key = lambda x: x.name)]
        self.facility_id.choices = [('', 'Select Facilities')]  + facilities
