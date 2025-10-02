from app import app
from flask import redirect, flash, url_for, request, render_template, abort
from flask_login import login_required, login_user, logout_user
from app.models import Role, is_logged_in, get_current_user, AuthUser, Facility, Encounter
from app.models import DiseaseCategory, Disease, User
from app.models import get_current_user
from app.services import UserServices, EncounterServices, FacilityServices, DiseaseServices
from app.services import DiseaseCategoryServices
from app.exceptions import AuthenticationError, MissingError, ValidationError
from app.exceptions import InvalidReferenceError, DuplicateError
from urllib.parse import urlparse
from app.utils import form_to_dict, admin_required
from app.forms import LoginForm, AddEncounterForm, AddFacilityForm, EditFacilityForm, AddDiseaseForm
from app.forms import AddUserForm, AddCategoryForm, DeleteUserForm, EditUserForm
from flask_wtf import FlaskForm
from typing import Optional
from flask_wtf.csrf import validate_csrf
from datetime import datetime, date
from typing import Any

@app.route('/')
@app.route('/index')
@login_required
def index():
    user = get_current_user()
    if user.role == Role.admin:
         return redirect(url_for('admin'))
    return redirect(url_for('add_encounter'))

@app.route('/auth/login', methods = ['GET', 'POST'])
def login() -> Any:
    if is_logged_in():
        return redirect(url_for('index'))

    form = LoginForm()
    if form.validate_on_submit():
        username = form.username.data
        password = form.password.data
        remember_me = form.remember_me.data
        try:
            user = UserServices.get_verified_user(username, password)
            authuser = AuthUser(user)
            login_user(authuser, remember = remember_me)
            next_page = request.args.get('next')

            if not next_page or urlparse(next_page).netloc != '':
                next_page = 'index'
            return redirect(url_for(next_page))

        except AuthenticationError as e:
            flash(str(e), 'error')
        except:
            abort(500)
    return render_template('login.html', title='Sign in', form = form)

@app.route('/auth/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/add_encounter', methods = ['GET', 'POST'])
@login_required
def add_encounter() -> Any:
    form:AddEncounterForm  = AddEncounterForm()
    form.date.data = date.today()
    for subfield in form.diseases:
        subfield.choices = [(dis.id, dis.name) for dis in DiseaseServices.get_all()]
    if form.validate_on_submit():
        res = form_to_dict(form, Encounter)
        #for the purpose of the demo deadline use the first disease and ignore others
        diseases = [disease.data for disease in form.diseases]
        res['diseases_id']  = diseases[0]
        user = get_current_user()
        res['created_by'] =user.id
        res['facility_id'] = user.facility_id
        try:
            EncounterServices.create_encounter(**res)
            flash('Encounter has successfully been added', 'success')
            return redirect(url_for('add_encounter'))
        except (InvalidReferenceError, ValidationError ) as e:
            flash(str(e), 'error')
        except:
            abort(500)

    return render_template('add_encounter.html', 
                           form = form, 
                           title = 'Add Encounter')

        
@app.route('/admin/facilities', methods = ['GET', 'POST'])
@admin_required
def facilities() -> Any:
    facility_form = AddFacilityForm()
    if facility_form.validate_on_submit():
        res = form_to_dict(facility_form, Facility)
        try:
            FacilityServices.create_facility(**res)
            flash("You have successfuly created a new facility", 'success')
            return redirect(url_for('facilities'))
        except (ValidationError, DuplicateError) as e:
            flash(str(e), 'error')

    facility_total = FacilityServices.get_total()
    page:int = int(request.args.get('page', 1))
    facility_list = FacilityServices.list_row_by_page(page)
    next_url: Optional[str] = (url_for('facilities', page=page+1)
                if FacilityServices.has_next_page(page) else None)
    prev_url = (url_for('facilities', page=page-1) if page > 1 else None)

    return render_template('facilities.html', 
                           title = 'Facilities',
                           prev_url = prev_url,
                           next_url = next_url, 
                           facility_total = facility_total,
                           facility_form = facility_form,
                           facility_list = facility_list)


@app.route('/admin/facilities/edit/<int:pid>', methods = ['GET', 'POST'])
@admin_required
def edit_facilities(pid: int) ->Any:
    try:
        facility = FacilityServices.get_by_id(pid)
    except MissingError as e:
        flash(str(e), 'error')
        return redirect(url_for('facilities'))
    except:
        abort(500)

    form: FlaskForm = EditFacilityForm(obj=facility)
    if form.validate_on_submit():
        try:
            form.populate_obj(facility)
            FacilityServices.update_facility(facility)
            flash("You have successfully added a new facility", 'success')
            return redirect(url_for('facilities'))
        except (DuplicateError, ValidationError) as e:
            flash(str(e), 'error')
        except:
            abort(500)

    return render_template('edit_facilities.html', 
                           form = form, 
                           title = 'Edit Facility')
@app.route('/admin/diseases', methods=['GET'])
@admin_required
def diseases():
    page = int(request.args.get('page', 1))
    disease_list = DiseaseServices.list_row_by_page(page)

    category_list = DiseaseCategoryServices.get_all()
    
    next_url = url_for('diseases', page=page + 1) if DiseaseServices.has_next_page(page) else None
    prev_url = url_for('diseases', page=page - 1) if page > 1 else None

    return render_template('diseases.html', 
                           title='Manage Diseases', 
                           disease_list=disease_list,
                           category_list=category_list,
                           next_url=next_url,
                           prev_url=prev_url)

@app.route('/admin/diseases/category/add', methods=['GET', 'POST'])
@admin_required
def add_category():
    form = AddCategoryForm()
    if form.validate_on_submit():
        try:
            DiseaseCategoryServices.create_category(category_name=form.category_name.data)
            flash('Disease category added successfully', 'success')
            return redirect(url_for('diseases'))
        except DuplicateError as e:
            flash(str(e), 'error')
        except Exception:
            abort(500)
    return render_template('add_category.html', title='Add Disease Category', form=form)


@app.route('/admin/diseases/add', methods=['GET', 'POST'])
@admin_required
def add_disease():
    form = AddDiseaseForm()
    form.category_id.choices = [(cat.id, cat.category_name) for cat in DiseaseCategoryServices.get_all()]
    
    if form.validate_on_submit():
        try:
            DiseaseServices.create_disease(name=form.name.data, category_id=form.category_id.data)
            flash('New disease added successfully', 'success')
            return redirect(url_for('diseases'))
        except (DuplicateError, InvalidReferenceError) as e:
            flash(str(e), 'error')
        except Exception:
            abort(500)
    return render_template('add_disease.html', title='Add Disease', form=form)

@app.route('/admin/diseases/edit/<int:disease_id>', methods=['GET', 'POST'])
@admin_required
def edit_disease(disease_id: int):
    try:
        disease = DiseaseServices.get_by_id(disease_id)
    except MissingError:
        abort(404)

    form = EditDiseaseForm(obj=disease) # Pre-populate form with existing data
    form.category_id.choices = [(cat.id, cat.category_name) for cat in DiseaseCategoryServices.get_all()]

    if form.validate_on_submit():
        try:
            form.populate_obj(disease)
            DiseaseServices.update_disease(disease)
            flash(f"Disease '{disease.name}' has been updated.", 'success')
            return redirect(url_for('diseases'))
        except (DuplicateError, InvalidReferenceError) as e:
            flash(str(e), 'error')
        except Exception:
            abort(500)
            
    return render_template('edit_disease.html', title=f"Edit Disease: {disease.name}", form=form, disease=disease)

   
@app.route('/admin/users', methods = ['GET', 'POST'])
@admin_required
def users():
    page = int(request.args.get('page', 1))
    user_form: AddUserForm = AddUserForm()

    if not user_form.facility_id.choices:
        user_form.facility_id.choices = [(fac.id, fac.name) for fac in FacilityServices.get_all()]

    if user_form.validate_on_submit():
        res = form_to_dict(user_form, User)
        try: 
            user =UserServices.create_user(**res)
            facility:Facility = FacilityServices.get_by_id(user.facility_id)
            flash(f'{user.username} added to {facility.name} facility',
                  'success')
            return redirect(url_for('users'))
        except (DuplicateError, InvalidReferenceError) as e:
            flash(str(e), 'error')

    user_list = UserServices.list_row_by_page(page)
    next_url = (url_for('users', page=page + 1) 
                if UserServices.has_next_page(page) else None)
    prev_url = None if page == 1 else url_for('users', page =page - 1)

    return render_template('users.html', 
                    user_form = user_form,
                    user_list = user_list,
                    next_url = next_url,
                    prev_url = prev_url)


@app.route('/admin/user/edit/<int:user_id>', methods=['GET', 'POST'])
@admin_required
def edit_user(user_id: int):
    try:
        user = UserServices.get_by_id(user_id) 
    except MissingError:
        flash('User not found in database')
        return redirect(url_for('users'))
    form = EditUserForm()
    form.username.data = user.username

    if form.validate_on_submit():
        try:
            if user.username != form.username.data:
                user.username = form.username.data
                UserServices.update_user(user)
            if form.password.data:
                UserServices.update_user_password(user, form.password.data)
            flash('User updated successfully', 'success')
            return redirect(url_for('users'))
        except DuplicateError as e:
            flash(str(e), 'error')
        
    delete_form = DeleteUserForm()
    return render_template('edit_user.html', user=user, edit_form=form, delete_form=delete_form)

@app.route('/admin/user/delete/<int:user_id>', methods=['POST'])
@admin_required
def delete_user(user_id: int):
    user = UserServices.get_by_id(user_id)
    delete_form = DeleteUserForm()

    if delete_form.validate_on_submit(): 
        UserServices.delete_user(user)
        flash(f"User '{user.username}' has been deleted.", 'success')
    else:
        flash("CSRF token invalid. Could not delete user.", 'error')
        
    return redirect(url_for('users'))

           
@app.route('/encounters')
@login_required
def encounters():
    user =  get_current_user()
    page = int(request.args.get('page', 1))
    start_date = request.args.get('start_date')
    if start_date:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = request.args.get('end_date')
    if end_date:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    and_filter = []
    if start_date:
        and_filter.append(('date', start_date, '>='))
    if end_date:
        and_filter.append(('date', end_date, '<='))
    if user.role == Role.user:
        and_filter.append(('facility_id', user.facility_id, '=' ))
    encounter_list = EncounterServices.list_row_by_page(page,
                                                        and_filter=and_filter)

    next_url = (url_for('users', page=page + 1) 
                if UserServices.has_next_page(page) else None)
    prev_url = None if page == 1 else url_for('users', page =page - 1)
    return render_template('encounters.html', 
                           encounter_list = encounter_list, 
                           )

@app.route('/admin')
@admin_required
def admin():
    # basic dashboard interfacee for prototype still simplified for now
    facilities_count = FacilityServices.get_total()
    user_count = UserServices.get_total()
    encounter_count = EncounterServices.get_total()
    #should actually be encounter view since I don't want to show facility Id, I should show
    # facility name, user name, and others. but currently I just want it working
    recent_encounter = EncounterServices.get_all(10,
                                                 order_by=('created_at', 'DESC'))
    return render_template('admin.html', 
                           facilities_count = facilities_count,
                           encounter_count= encounter_count,
                           recent_encounter = recent_encounter)