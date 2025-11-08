from wtforms import StringField, PasswordField, SubmitField, BooleanField, TextAreaField, HiddenField
from wtforms import IntegerField, SelectField, DateField, FieldList, SelectMultipleField, FileField
from wtforms.validators import DataRequired, Length, NumberRange, EqualTo, Optional, ValidationError
from app.services import InsuranceSchemeServices, FacilityServices, TreatmentOutcomeServices
from wtforms import widgets
from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed, FileRequired
from app.models import get_current_user
from app.constants import LGA_CHOICES
import re

class LoginForm(FlaskForm):
    username = StringField('Enter Username: ', validators=[DataRequired()])
    password = PasswordField('Enter Password: ', validators=[DataRequired()])
    remember_me = BooleanField('Remember Me')
    submit = SubmitField('Sign in')

class AddEncounterForm(FlaskForm):
    policy_number = StringField('Policy Number', validators=[DataRequired()])
    client_name = StringField('Client Name', validators=[DataRequired()])
    treatment = TextAreaField('Treatment')
    doctor_name = StringField('Doctor Name', validators=[
                              DataRequired(), Length(min=2)])
    age = IntegerField('Age', validators=[DataRequired(), NumberRange(
        0, 120, 'Age must be between 0 - 120 ')])
    date = DateField('Date Of Visit', format="%Y-%m-%d", validators=[DataRequired()])
    gender = SelectField('Gender', choices=[
                         ('M', 'Male'), ('F', 'Female')], validators=[DataRequired()])
    nin = StringField('Nin', validators=[DataRequired(), Length(min = 11, max= 11, message="Invalid NiN number")])
    diseases = FieldList(SelectField('Disease/Diagnosis',
                         validators=[DataRequired()], coerce=int))
    services = FieldList(SelectField("Services",
                         validators=[DataRequired()], coerce=int))
    facility = SelectField("Select Facility", coerce=int)
    outcome = SelectField('Treatment Outcome',  validators=[
                          Optional()], coerce=int, render_kw={'id': 'outcome-select'})
    phone_number = StringField("Phone Number", validators = [DataRequired(), Length(min=11, "Invalid Phone Number")])
    death_type = SelectField("Death Type", validators=[
                             Optional()], coerce=int, render_kw={'id': 'death-type-select'})
    submit = SubmitField('submit')

    def validate_diseases(self, diseases):
        for disease in diseases:
            if not disease.data or int(disease.data) == 0:
                raise ValidationError(
                    "Please select a valid disease from the list")

    def validate_phone_number(self, phone_number):
        pattern = re.compile(r'^(0\d{10}|\+234\d{10})$')
        if not pattern.match(phone_number.data):
            raise ValidationError("Invalid Nigerian phone number format")

    def validate_gender(self, gender):
        if gender.upper() not in ('M', 'F'):
            raise ValidationError("Gender can only be male or female")

    def validate_facility(self, facility):
        user = get_current_user()
        if user.role.name == 'admin' and not facility.data:
            raise ValidationError(
                "Admin User have to select a facility for encounter")

    def validate(self, extra_validators=None):
        valid = super().validate(extra_validators)
        if not valid:
            return False
        # Require either disease or service
        if not any(d.data for d in self.diseases) and not any(s.data for s in self.services):
            self.errors.setdefault('services', []).append("Please select at least one disease or service.")
            return False
        # Require death type if outcome is death
        if self.outcome.data and self.outcome.data == self.death_outcome_id and not self.death_type.data:
            self.errors.setdefault('death_type', []).append("Please select a death type for 'Death' outcome.")
            return False
        return True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        outcome = TreatmentOutcomeServices.get_all()
        self.outcome.choices = [(s.id, s.name) for s in outcome if outcome.type.lower() != 'death']
        self.death_type.choices = [(s.id, s.name) for s in  outcome if outcome.type.lower() == 'death']

class ANCEncounterForm(FlaskForm): #only for pregnant women scheme, other scheme with delivery will use the encounter form scheme
    policy_number = StringField('ORIN', validators=[DataRequired(), Length(min=10, max=10)])
    client_name = StringField('Client Name', validators=[DataRequired()])
    treatment = TextAreaField('Treatment')
    doctor_name = StringField('Doctor Name', validators=[
                              DataRequired(), Length(min=2)])
    age = IntegerField('Age', validators=[DataRequired(), NumberRange(
        0, 60, 'Age must be between 0 - 120 ')])

    date = DateField('Date Of Visit', format="%Y-%m-%d", validators=[DataRequired()])
    gender = SelectField('Gender', choices=[
                         ('M', 'Male'), ('F', 'Female')], validators=[DataRequired()])

    date_of_issue = DateField("Date of Issue Of Kaadi Igbeayo", validators=[DataRequired()])
    place_of_issue = StringField("Place of Issue of Kaadi Igbeayo", validators = [DataRequired()])

    nin = StringField('Nin', validators=[DataRequired(), Length(min = 11, max= 11, message="Invalid NiN number")])
    date_of_booking = DateField("Date of Booking", )
    facility = SelectField("Select Facility", coerce=int)
    outcome = SelectField('Treatment Outcome',  validators=[
                          Optional()], coerce=int, render_kw={'id': 'outcome-select'})
    death_type = SelectField("Death Type", validators=[
                             Optional()], coerce=int, render_kw={'id': 'death-type-select'})
    submit = SubmitField('submit')
    phone_number = StringField("Phone Number", validators = [DataRequired(), Length(min=11)])
    last_menstrual_period = DateField("Last Menstrual Period (in weeks)",
                                   validators=[DataRequired()] )
    expected_delivery_date = DateField("Expected Delivery Date",
                                       validators=[DataRequired()])
    expected_gestational_age = DateField("Expected Gestational Age",
                                         validators = [DataRequired()])
    parity = IntegerField("Number of Previous Baby",
                          validators=[DataRequired()])
    def validate_phone_number(self, phone_number):
        pattern = re.compile(r'^(0\d{10}|\+234\d{10})$')
        if not pattern.match(phone_number.data):
            raise ValidationError("Invalid Nigerian phone number format")

    def validate_facility(self, facility):
        user = get_current_user()
        if user.role.name == 'admin' and not facility.data:
            raise ValidationError(
                "Admin User have to select a facility for encounter")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        outcome = TreatmentOutcomeServices.get_all()
        self.outcome.choices = [(s.id, s.name) for s in outcome if outcome.type.lower() != 'death']
        self.death_type.choices = [(s.id, s.name) for s in  outcome if outcome.type.lower() == 'death']

class DeliveryEncounterForm(ANCEncounterForm):
    date_of_delivery = DateField("Date of Delivery", validators= [DataRequired()])
    number_of_anc_visit = IntegerField("Number of ANC Visit", validators=[DataRequired()])
    outcome = SelectField('Mother Outcome',  validators=[
                          Optional()], coerce=int, render_kw={'id': 'outcome-select'})
    death_type = SelectField("Death Type", validators=[
                             Optional()], coerce=int, render_kw={'id': 'death-type-select'})
    no_of_babies = IntegerField("Number of babies", coerce=int, validators=[DataRequired()], default=1)
    mother_status = StringField("Mother Status", validators=[DataRequired()])
    mode_of_delivery = SelectField("Mode of Delivery", validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        outcome = TreatmentOutcomeServices.get_all()
        self.outcome.choices = [(s.id, s.name) for s in outcome if outcome.type.lower() != 'death']
        self.death_type.choices = [(s.id, s.name) for s in  outcome if outcome.type.lower() == 'death']

class ChildHealthEncounterForm(FlaskForm):
    client_name = StringField("Cient Name", validators=[DataRequired()])
    dob = DateField("Date of Birth", validators = [DataRequired()])
    policy_number = StringField("Orin", validators= [DataRequired()])
    nin = StringField("Mother NIN",  validaors=[DataRequired(), Length(min=11, max=11, message="Invalid NIN Number")])
    gender = SelectField("Gender", validators=[DataRequired()], choices=[('M', 'Male'), ('F', 'Female')])
    address = StringField("Parent's Address", validators=[DataRequired()])
    guardian_name = StringField("Parent/Guardian's Name", validators=[DataRequired()])
    phone_number = StringField("Parent/Guardian's Phone Number", validators=[DataRequired()])
    diseases = FieldList(SelectField('Disease/Diagnosis',
                         validators=[DataRequired()], coerce=int))
    services = FieldList(SelectField("Services",
                         validators=[DataRequired()], coerce=int))
    outcome = SelectField('Treatment Outcome',  validators=[
                          Optional()], coerce=int, render_kw={'id': 'outcome-select'})
    death_type = SelectField("Death Type", validators=[
                             Optional()], coerce=int, render_kw={'id': 'death-type-select'})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        outcome = TreatmentOutcomeServices.get_all()
        self.outcome.choices = [(s.id, s.name) for s in outcome if outcome.type.lower() != 'death']
        self.death_type.choices = [(s.id, s.name) for s in  outcome if outcome.type.lower() == 'death']

class AddFacilityForm(FlaskForm):
    name = StringField('Facility Name', validators=[DataRequired()])
    local_government = SelectField('Local Government',
                                   choices=LGA_CHOICES,
                                   validators=[DataRequired('Please select a local government from list')])
    facility_type = SelectField('Facility Type',
                                choices=['Primary', 'Secondary', 'Private'], validators=[DataRequired()])

    scheme = SelectMultipleField('Insurance Scheme', coerce=int,
                                 validators=[DataRequired(
                                     "Please select the insurance scheme allowed for this facility")],
                                 widget=widgets.ListWidget(),
                                 option_widget=widgets.CheckboxInput())
    submit = SubmitField('submit')


class AddCategoryForm(FlaskForm):
    category_name = StringField('Category Name', validators=[DataRequired()])
    submit = SubmitField('Add Category')


class AddDiseaseForm(FlaskForm):
    name = StringField('Disease Name', validators=[DataRequired()])
    category_id = SelectField('Category', validators=[
                              DataRequired()], coerce=int)
    submit = SubmitField('Add Disease')


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
                       DataRequired('Please Enter disease name')])
    category_id = SelectField('Category', validators=[
                              DataRequired('Please select a category')])
    submit = SubmitField("Update Disease")

class EditUserForm(FlaskForm):
    username = StringField('Username', validators=[
                           DataRequired("Please enter a username")])
    password = PasswordField('Password', validators=[
                             Optional(),  EqualTo('password2', "Password must match")])
    password2 = PasswordField('Confirm Password', validators=[Optional()])
    submit = SubmitField('Change Password')


class EditFacilityForm(AddFacilityForm):
    pass

class EncounterFilterForm(FlaskForm):
    start_date = DateField(
        'Start Date', format='%Y-%m-%d', validators=[Optional()])
    end_date = DateField('End Date', format='%Y-%m-%d',
                         validators=[Optional()])
    local_government = SelectField('Local Government',
                                   choices=LGA_CHOICES,
                                   validators=[Optional()])
    facility_id = SelectField('Facility', coerce=int, validators=[Optional()])
    submit = SubmitField('Filter')


class ExcelUploadForm(FlaskForm):
    facility_id = SelectField('Facility', coerce=int,
                              validators=[DataRequired()])
    month = SelectField("Month", coerce=int, validators=[DataRequired()])
    excel_file = FileField("Upload Excel File", validators=[
        FileRequired(),
        FileAllowed(['xls', 'xlsx'], "Excel files only!")
    ])
    submit = SubmitField("Upload")


class OrinForm(FlaskForm):
    orin = StringField("ORIN Number", validators=[DataRequired],
                       Length(min=10, max=10))
    service = HiddenField()
    class Meta:
        csrf = False


class DashboardFilterForm(FlaskForm):
    """Base filter form - shared across all dashboards"""
    class Meta:
        csrf = False

    period = SelectField(
        'Date Range',
        choices=[
            ('this_month', 'This Month'),
            ('last_3_months', 'Last 3 Months'),
            ('last_year', 'Last Year'),
        ],
        default='this_month',
        validators=[Optional()]
    )

    scheme_id = SelectField(
        'Insurance Scheme',
        choices=[('', 'All Schemes')],  # Populated dynamically
        coerce=lambda x: int(x) if x else None,
        validators=[Optional()]
    )

    gender = SelectField(
        'Gender',
        choices=[
            ('', 'All'),
            ('M', 'Male'),
            ('F', 'Female')
        ],
        validators=[Optional()]
    )

    def __init__(self, *args, **kwargs):
        """Populate dynamic choices on form init"""
        super().__init__(*args, **kwargs)

        # Populate schemes from database
        schemes = InsuranceSchemeServices.get_all()
        self.scheme_id.choices = [('', 'All Schemes')] + [
            (str(s.id), s.scheme_name.upper()) for s in schemes
        ]

class AdminDashboardFilterForm(DashboardFilterForm):
    facility_id = SelectField('Facility',
                              choices = [('', 'All Facilities')],
                              validators = [Optional()])
    lga =  SelectField('Local Government',
                    choices = [('', 'All LGAs')] + LGA_CHOICES[1:],
                    validators = [Optional()])
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        facilities = [(str( f.id ), f.name.upper()) for f in FacilityServices.get_all()]
        self.facility_id.choices = [('', 'All Facilities')]  + facilities
