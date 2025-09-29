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
from app.utils import form_to_dict, populate_form, admin_required
from app.forms import LoginForm
from flask_wtf import FlaskForm
from typing import Optional
from flask_wtf.csrf import validate_csrf
from datetime import datetime
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
def login():
    if is_logged_in():
        return redirect(url_for('index'))

    form = LoginForm()
    if form.validate_on_submit():
        username = form.username.data
        password = form.password.data
        remember_me = form.remember_me.data
        try:
            user = UserServices.get_verified_user(username, password)
        except AuthenticationError as e:
            flash(str(e), 'error')
            return redirect(url_for('login'))
        except:
            abort(500)

        authuser = AuthUser(user)
        login_user(authuser, remember = remember_me)
        next_page = request.args.get('next')

        if not next_page or urlparse(next_page).netloc != '':
            next_page = 'index'

        return redirect(url_for(next_page))
    return render_template('login.html', title='Sign in', form = form)

@app.route('/auth/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/add_encounter', methods = ['GET', 'POST'])
@login_required
def add_encounter() -> Any:
    form = AddEncounterForm()

    if form.validate_on_submit():
        res = form_to_dict(form, Encounter)
    
        try:
            EncounterServices.create_encounter(**res)
        except (InvalidReferenceError, ValidationError ) as e:
            flash(str(e), 'error')
            return redirect(url_for('add_encounter'))
        except:
            abort(500)
        flash('Encounter has successfully been added', 'success')
        return redirect(url_for('add_encounter'))

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
                           facility_list = facility_list)


@app.route('/admin/facilities/edit/<int:pid>', methods = ['GET', 'POST'])
@admin_required
def edit_facilities(pid: int) ->Any:
    try:
        facility = FacilityServices.get_facility_by_id(pid)
    except MissingError as e:
        flash(str(e), 'error')
        return redirect(url_for('facilities'))
    except:
        abort(500)

    form = EditFacilityForm()
    if form.validate_on_submit():
        res = form_to_dict(form, Facility)
        facility = Facility(**res)
        try:
            FacilityServices.update_facility(facility)
        except (DuplicateError, ValidationError) as e:
            flash(str(e), 'error')
            return redirect(request.referrer or url_for('edit_facilities'))
        except:
            abort(500)
        flash("You have successfully added a new facility", 'success')
        return redirect(url_for('facilities'))
    elif request.method == 'GET':
        populate_form(form, facility)

    return render_template('edit_facilities.html', 
                           form = form, 
                           title = 'Edit Facility')


@app.route('/admin/diseases', methods = ['GET', 'POST'])
@admin_required
def diseases():
    category_form: FlaskForm = AddCategoryForm()
    disease_form: FlaskForm = AddDiseaseForm()
    disease_list  = DiseaseServices.list_row_by_page()
    category_list = DiseaseCategoryServices.get_all()
    if request.method == 'POST':
        form_type = request.form.get('form_name')
        if form_type == 'add_category' and category_form.validate_on_submit():
            res = form_to_dict(category_form, DiseaseCategory)
            try:
                disease = DiseaseCategoryServices.create_category(**res)
                flash('Disease category added successfully', 'success')
                return redirect(url_for('diseases'))
            except DuplicateError as e:
                flash(str(e), 'error')
                return redirect(url_for('diseases'))
            except:
                abort(500)

        if form_type == 'add_disease' and disease_form.validate_on_submit():
            res = form_to_dict(disease_form, Disease)
            try:
                DiseaseServices.create_disease(**res)
                flash('New disease added successfully', 'success')
                return redirect(url_for('diseases'))
            except (DuplicateError, InvalidReferenceError ) as e:
                flash(str(e), 'error')
                return redirect(url_for('diseases'))
            except:
                abort(500)

        if form_type == 'update_disease':
            try:
                validate_csrf(request.form.get('csrf_token'))
            except Exception:
                flash("You can't update disease", 'error')
                return redirect(request.referrer or url_for('diseases'))
            try:
                disease_id = int(request.form.get('id'))
            name = request.form.get('name')
            category_id = request.form.get('category_id')
            if disease_id and name and category_id:
                updated_disease = Disease(id = disease_id,
                                    name = name, 
                                    category_id = category_id)
                try:
                    DiseaseServices.update_disease(updated_disease)

                except InvalidReferenceError as e:
                    flash(str(e), 'error')
                    return redirect(url_for('disease'))

    return render_template('disease.html', 
                           category_form = category_form, 
                           disease_form = disease_form,
                           title = 'Diseases', 
                           disease_list = disease_list,
                           category_list = category_list)

   
@app.route('/admin/users', methods = ['GET', 'POST'])
@admin_required
def users():
    page = int(request.args.get('page', 1))
    user_form: FlaskForm = AddUserForm()

    if user_form.validate_on_submit():
        res = form_to_dict(user_form, User)
        try: 
            UserServices.create_user(**res)
            flash(f'New user added added to facility {facilities[res['facility_id']]}',
                  'success')
            return redirect(url_for('users'))
        except (DuplicateError, InvalidReferenceError) as e:
            flash(str(e), 'error')
            return redirect(url_for('users'))
 
    user_list = UserServices.list_row_by_page(page)
    next_url = (url_for('users', page=page + 1) 
                if UserServices.has_next_page(page) else None)
    prev_url = None if page == 1 else url_for('users', page =page - 1)

    return render_template('users.html', 
                    user_form = user_form,
                    user_list = user_list,
                    next_url = next_url,
                    prev_url = prev_url)


@app.route('/admin/user/<int:user_id>', methods = ['GET', 'POST'])
@admin_required
def edit_delete_user(user_id: int) -> Any:
    try:
        user = UserServices.get_user_by_id(user_id)
    except MissingError as e:
        flash(str(e), 'error')
        return redirect(url_for(user))
    except:
        abort(500)

    delete_form: FlaskForm = DeleteUserForm()
    edit_form: FlaskForm = EditUserForm() #edit
    if request.method == 'POST':
        form_type = request.form.get('forn_name')
        if form_type == 'delete_form' and delete_form.validate_on_submit():
            try:
                UserServices.delete_user(user)
                flash(f"You have successfuly deleted user {user.name}", 'success')
                return redirect(url_for('users'))
            except Exception as e:
                abort(500)

        elif form_type == 'edit_form' and edit_form.validate_on_submit():

            try:
                UserServices.update_user_password(user, edit_form.password.data)
                flash(f'User password changed successfully', 'success')
                return redirect(url_for('users'))
            except DuplicateError as e:
                flash(str(e), 'error')
                return redirect(url_for('users'))
    return render_template('edit_user.html',
                           user = user,
                           edit_form = edit_form,
                           delete_form = delete_form)
            
@app.route('/encounters')
@login_required
def encounters():
    user =  get_current_user()
    if user is None: return redirec(url_for('login'))
    page = int(request.args.get('page', 1))
    start_date = request.args.get('start_date')
    if start_date:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = request.args.get('end_date')
    if end_date:
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

    and_filter = []
    if start_date:
        and_filter.append(('date', start_date, '>='))
    if end_date:
        and_filter.append(('date', start_date, '<='))
    if user.role == Role.admin:
        local_government = request.args.get('local_government')
        and_filter.append(('local_government', local_government, '='))
    else:
        and_filter.append(('facility_id', user.facility_id, '=' ))
    encounter_list = EncounterServices.list_row_by_page(page,
                                                        and_filter=and_filter)
    filter_form = EncounterFilterForm()

    return render_template('encounters.html', 
                           encounter_list = encounter_list, 
                           filter_form = filter_form)

@app.route('/admin')
@admin_required
def admin():
    # dashboard interfacee