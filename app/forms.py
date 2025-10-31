from wtforms import StringField, PasswordField, SubmitField, BooleanField, TextAreaField
from wtforms import IntegerField, SelectField, DateField, FieldList, SelectMultipleField, FileField
from wtforms.validators import DataRequired, Length, NumberRange, EqualTo, Optional, ValidationError
from app.services import InsuranceSchemeServices, FacilityServices
from wtforms import widgets
from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed, FileRequired
from app.models import get_current_user
from app.constants import LGA_CHOICES


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
    date = DateField('Date', format="%Y-%m-%d", validators=[DataRequired()])
    gender = SelectField('Gender', choices=[
                         ('M', 'Male'), ('F', 'Female')], validators=[DataRequired()])
    diseases = FieldList(SelectField('Disease/Diagnosis/Services',
                         validators=[DataRequired()], coerce=int), min_entries=1)
    facility = SelectField("Select Facility", coerce=int)
    outcome = SelectField('Treatment Outcome',  validators=[
                          Optional()], coerce=int, render_kw={'id': 'outcome-select'})
    death_type = SelectField("Death Type", validators=[
                             Optional()], coerce=int, render_kw={'id': 'death-type-select'})
    submit = SubmitField('submit')

    def validate_diseases(self, diseases):
        for disease in diseases:
            if not disease.data or int(disease.data) == 0:
                raise ValidationError(
                    "Please select a valid disease from the list")

    def validate_facility(self, facility):
        user = get_current_user()
        if user.role.name == 'admin' and not facility.data:
            raise ValidationError(
                "Admin User have to select a facility for encounter")

    def validate(self, extra_validators=None):
        # First, run all the standard validators
        if not super().validate(extra_validators):
            return False

        # Now, add your custom cross-field logic
        # Assuming '5' is the ID for the 'Death' outcome
        if self.outcome.data == -1 and not self.death_type.data:
            self.death_type.errors.append(
                "Please select a death type when the outcome is 'Death'.")
            return False

        # If all is good
        return True


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


class DashboardFilterForm(FlaskForm):
    """Base filter form - shared across all dashboards"""

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
    lga =  SelectField('Facility',
                    choices = [('', 'All LGAs')] + LGA_CHOICES[1:],
                    validators = [Optional()])
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        facilities = [(f.id, f.name.upper()) for f in FacilityServices.get_all()]
        self.facility_id.choices = [('', 'All Facilities')]  + facilities
