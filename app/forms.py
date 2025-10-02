from app.models import User, Facility, Disease 
from wtforms import StringField, PasswordField, SubmitField, BooleanField, TextAreaField
from wtforms import IntegerField, SelectField, HiddenField, FieldList, DateField
from wtforms.validators import DataRequired, Length, NumberRange, EqualTo, Optional
from app.config import LOCAL_GOVERNMENT
from app.services import FacilityServices, DiseaseCategoryServices, DiseaseServices
from flask_wtf import FlaskForm

class LoginForm(FlaskForm):
    username = StringField('Enter Username: ', validators=[DataRequired() ])
    password = PasswordField('Enter Password: ', validators = [DataRequired()])
    remember_me = BooleanField('Remember Me')
    submit = SubmitField('Sign in')

class AddEncounterForm(FlaskForm):
    policy_number = StringField('Policy Number', validators = [DataRequired()])
    client_name = StringField('Client Name', validators=[DataRequired()])
    treatment = TextAreaField('Treatment')
    doctor_name = StringField('Doctor Name', validators = [DataRequired(), Length(min = 2)])
    age = IntegerField('Age', validators=[DataRequired(), NumberRange(0, 120, 'Age must be between 0 - 120 ')])
    date = DateField('Date', format="%Y-%m-%d", validators = [DataRequired()])
    professional_service = TextAreaField('Professional Service')
    gender = SelectField('Gender', choices= [('M', 'Male'), ('F','Female')], validators=[DataRequired()])
    referral = BooleanField('Referral')
    diseases  = FieldList(SelectField('Disease', validators=[DataRequired()]), min_entries=1)
    submit = SubmitField('submit')

class AddFacilityForm(FlaskForm):
    name = StringField('Facility Name', validators =[DataRequired()])
    local_government =  SelectField('Local Government', 
                                     choices=[(lga, lga) for lga in LOCAL_GOVERNMENT],
                                     validators=[DataRequired('Please select a local government from list')])
    facility_type = SelectField('Facility Type', 
                                choices= ['Primary', 'Secondary', 'Tertiary'], validators=[DataRequired()])
    submit = SubmitField('submit')

class AddCategoryForm(FlaskForm):
    category_name = StringField('Category Name', validators=[DataRequired()])
    form_name = HiddenField(default='add_category')
    submit = SubmitField('Add Category')

class AddDiseaseForm(FlaskForm):
    name = StringField('Disease Name', validators=[DataRequired()])
    category_id = SelectField('Category', validators=[DataRequired()], coerce=int)
    form_name= HiddenField(default='add_disease')
    submit = SubmitField('Add Disease')


class AddUserForm(FlaskForm):
    username = StringField('Username', validators = [DataRequired("Please enter a username")])
    facility_id = SelectField('Facility',
                                 validators = [DataRequired("Please select facility from the list")], 
                                 coerce= int)
    password = PasswordField('Password', validators = [DataRequired("You must enter a password")] )
    password2 = PasswordField('Confirm Password', validators = [DataRequired("You must enter a password"), EqualTo('password', )])
    submit = SubmitField('Add New User')

class DeleteUserForm(FlaskForm):
    form_name = HiddenField(default="delete_form")
    submit = SubmitField("Delete User")


class EditDiseaseForm(FlaskForm):
    name = StringField('Name', validators=[DataRequired('Please Enter disease name')])
    category_id = SelectField('Category', validators=[DataRequired('Please select a category')])


class EditUserForm(FlaskForm):
    form_name = HiddenField(default="edit_form")
    username = StringField('Username', validators = [DataRequired("Please enter a username")])
    password = PasswordField('Password', validators = [Optional(),  EqualTo('password2', "Password must match")] )
    password2 = PasswordField('Password', validators = [Optional()])
    submit = SubmitField('Change Password')


class EditFacilityForm(AddFacilityForm):
    pass