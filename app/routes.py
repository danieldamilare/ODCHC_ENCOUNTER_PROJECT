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
from app.forms import AddUserForm, AddCategoryForm, DeleteUserForm, EditUserForm, EditDiseaseForm, EncounterFilterForm
from flask_wtf import FlaskForm
from app.services import DashboardServices, ReportServices
from typing import Optional
from flask_wtf.csrf import validate_csrf
from datetime import datetime, date, timedelta
from typing import Any
import json
import io

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
    # print("In auth.login")
    form = LoginForm()

    if form.validate_on_submit():
        # print("Form validated")
        username = form.username.data
        password = form.password.data
        remember_me = form.remember_me.data
        # print(username, password)
        try:
            user = UserServices.get_verified_user(username, password)
            # print(user)
            authuser = AuthUser(user)
            # print("created auth user")
            # print(authuser)
            login_user(authuser, remember = remember_me)
            next_page = request.args.get('next')

            if not next_page or urlparse(next_page).netloc != '':
                next_page = url_for('index')
            # print(next_page)
            return redirect(next_page)

        except AuthenticationError as e:
            # print('An error occured')
            flash(str(e), 'error')
        # except Exception as e:
            # abort(500)
    # print("In login")
    # return "<h1>Hello, world</h1>"
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
    disease_choices = [('0', 'Select a disease')] + sorted([(dis.id, str(dis.name).title()) for dis in DiseaseServices.get_all()], key=lambda x: x[1])
    for subfield in form.diseases:
        subfield.choices =  disease_choices
    # print("In add Encounter")
    if form.validate_on_submit():
        # print('validated on submit')
        res = form_to_dict(form, Encounter)
        #for the purpose of the demo deadline use the first disease and ignore others
        diseases = [disease.data for disease in form.diseases]
        # print("disease id", diseases)
        res['diseases_id']  = diseases
        user = get_current_user()
        res['created_by'] =user
        res['facility_id'] = user.facility_id
        try:
            EncounterServices.create_encounter(**res)
            flash('Encounter has successfully been added', 'success')
            return redirect(url_for('add_encounter'))
        except (InvalidReferenceError, ValidationError ) as e:
            flash(str(e), 'error')
        # except:
            # abort(500)

    if request.method == 'GET':
        form.date.data = date.today()

    return render_template('add_encounter.html', 
                           disease_choices = disease_choices[1:],
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
                           facility = facility,
                           form = form, 
                           title = 'Edit Facility')


@app.route('/admin/facilities/view/<int:pid>', methods=['GET', 'POST'])
@admin_required
def view_facilities(pid: int) -> Any:
    # print("In view_facilities")
    try:
        facility = FacilityServices.get_by_id(pid)
    except MissingError as e:
        flash(str(e), 'error')
        return redirect(url_for('facilities'))
    except:
        abort(500)

    user_form: AddUserForm = AddUserForm()
    user_form.facility_id.choices = [('0', 'Select a facility')] + sorted([(fac.id, fac.name.title()) for fac in FacilityServices.get_all()], key=lambda x: x[1])
    user_form.facility_id.data = pid
    # print("About to validate user_form")

    if user_form.validate_on_submit():
        # print("User form validate on submit in view_facilities")
        try:
            created_user = UserServices.create_user(username =user_form.username.data,
                                     facility_id = pid, 
                                     password=user_form.password.data)
            # print("Successfully created user", created_user)
            redirect(url_for('view_facilities', pid=pid))
        except (DuplicateError, InvalidReferenceError, ValidationError) as e:
            # print(e)
            flash(str(e), "error")

    # print("In view funtion after validation")
    today = datetime.now().date()
    first_month_day = today.replace(day=1)
    # print(today, first_month_day)

    user_count = UserServices.get_total(
        and_filter = [('facility_id', pid, '=')]
    )
    and_filter =[('facility_id', pid, '='),
                 ('date', first_month_day, '>='),
                 ('date', today, '<=')]
    and_filter2 = [('ec.facility_id', pid, '='), and_filter[1], and_filter[2]]

    month_encounter_count = EncounterServices.get_total(
                        and_filter=and_filter)

    recent_encounters = list(EncounterServices.get_all(limit = 10,
                            and_filter = and_filter2,
                            order_by=[('date', 'DESC')]))

    page = int(request.args.get('user_page', 1))
    user_list = list(UserServices.list_row_by_page(page=page,
                                and_filter=[('facility_id', pid, '=')]))

    next_url = url_for('view_facilities', pid=pid, user_page=page + 1) if UserServices.has_next_page(page, and_filter = [and_filter[0]]) else None
    prev_url = url_for('view_facilities', pid= pid, user_page=page - 1) if page > 1 else None

    return render_template('view_facilities.html',
                           month_encounter_count=month_encounter_count,
                           facility = facility,
                           recent_encounters=recent_encounters,
                           user_list = user_list,
                           user_count = user_count,
                           user_form = user_form,
                           next_url = next_url,
                           prev_url = prev_url
                           )
    

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
    form.category_id.choices = [('0', 'Select a Category' )] + sorted([(cat.id, cat.category_name.title()) for cat in DiseaseCategoryServices.get_all()], key=lambda x: x[1])
    
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
    form.category_id.choices = [('0', 'Select a Category')] + sorted(
                    [(cat.id, cat.category_name.title()) for cat in DiseaseCategoryServices.get_all()],
                    key= lambda x: x[1])

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
    return render_template('add_disease.html', title=f"Edit Disease: {disease.name}", form=form, disease=disease)

   
@app.route('/admin/users', methods = ['GET', 'POST'])
@admin_required
def users():
    page = int(request.args.get('page', 1))
    user_form: AddUserForm = AddUserForm()

    if not user_form.facility_id.choices:
        user_form.facility_id.choices = [('0', 'Select a facility')] + [(fac.id, fac.name.title()) for fac in FacilityServices.get_all()]

    if user_form.validate_on_submit():
        res = form_to_dict(user_form, User)
        res['password'] = user_form.password.data
        try: 
            user =UserServices.create_user(**res)
            facility:Facility = FacilityServices.get_by_id(user.facility_id)
            flash(f'{user.username} added to {facility.name} facility',
                  'success')
            return redirect(url_for('users'))
        except (DuplicateError, InvalidReferenceError) as e:
            flash(str(e), 'error')

    user_list = list(UserServices.list_row_by_page(page))
    next_url = (url_for('users', page=page + 1) 
                if UserServices.has_next_page(page) else None)
    prev_url = None if page == 1 else url_for('users', page =page - 1)

    return render_template('users.html', 
                    title = "Manage User",
                    user_form = user_form,
                    user_list = user_list,
                    next_url = next_url,
                    prev_url = prev_url)


@app.route('/admin/users/edit/<int:user_id>', methods=['GET', 'POST'])
@admin_required
def edit_user(user_id: int):
    try:
        user = UserServices.get_by_id(user_id) 
    except MissingError:
        flash('User not found in database')
        return redirect(url_for('users'))
    form = EditUserForm()

    if form.validate_on_submit():
        # print(f"on validate form data: {form.username.data}")
        try:
            if user.username != form.username.data:
                # print(user.username, form.username.data)
                user.username = form.username.data
                UserServices.update_user(user)
            if form.password.data:
                UserServices.update_user_password(user, form.password.data)
            flash('User updated successfully', 'success')
            return redirect(url_for('users'))
        except DuplicateError as e:
            flash(str(e), 'error')
        
    if request.method == 'GET':
        form.username.data = user.username
    delete_form = DeleteUserForm()
    return render_template('edit_user.html', 
                            title=f"Edit user {user.username}",
                            user=user,
                            edit_form=form, 
                            delete_form=delete_form)

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
    filter_form = EncounterFilterForm(request.args)
    filter_form.facility_id.choices = [('0', 'All Facilities')] + sorted(
        [(fac.id, fac.name.title()) for fac in FacilityServices.get_all()],
        key=lambda x: x[1])

    and_filter = []

    if filter_form.start_date.data:
        start_date = datetime.strptime(filter_form.start_date.data, '%Y-%m-%d').date()
        and_filter.append(('date', start_date, '>='))

    if filter_form.end_date.data:
        end_date = datetime.strptime(filter_form.end_date.data, '%Y-%m-%d').date()
        and_filter.append(('date', end_date, '<='))

    if user.role == Role.user:
        and_filter.append(('ec.facility_id', user.facility_id, '=' ))
    else:
        lga = filter_form.local_government.data
        facility_id = filter_form.facility_id.data
        if lga:
            and_filter.append(('fc.local_government', lga, '='))
        if facility_id:
            and_filter.append(('ec.facility_id', facility_id, '='))
        

    encounter_list = EncounterServices.list_row_by_page(page,
                                                        and_filter=and_filter)

    pagination_args = {**request.args, "page": page +1}
    next_url = (url_for('encounters', **pagination_args) 
                if EncounterServices.has_next_page(page) else None)

    pagination_args['page'] = page - 1
    prev_url = None if page == 1 else url_for('encounters', **pagination_args)
    return render_template('encounters.html', 
                           title = "Encounter List",
                           encounter_list = encounter_list, 
                           filter_form = filter_form,
                           next_url = next_url,
                           prev_url = prev_url
                           )

@app.route('/encounters/view/<int:pid>')
@admin_required
def view_encounter(pid: int):
    try:
        EncounterServices.get_by_id(pid)
    except MissingError as e:
        flash(str(e), "error")
    encounter_view = list(EncounterServices.get_all(and_filter=[
        ("ec.id", pid, '=')]))[0]
    return render_template('view_encounter.html', 
                           title = f"Encounter Details: {encounter_view.client_name}", 
                           encounter=encounter_view)


@app.route('/admin')
@admin_required
def admin():
    # --- 1. GET FILTERS FROM URL ---
    period = request.args.get('period', 'this_month')
    facility_id = request.args.get('facility_id', 'all')

    # --- 2. CALCULATE DATE RANGE ---
    today = date.today()
    start_date = None
    end_date = today

    if period == 'this_month':
        start_date = today.replace(day=1)
    elif period == 'last_3_months':
        start_date = today - timedelta(days=90)
    elif period == 'last_year':
        start_date = today.replace(year=today.year - 1)
    
    # Custom range would be handled by a different form, but this is good for demo
    
    # --- 3. BUILD FILTERS FOR SERVICES ---
    encounter_and_filters = []
    facility_and_filters = [] # For services that filter on the facility table directly
    other_encounter_filter = []
    if start_date:
        encounter_and_filters.append(('date', start_date, '>='))
        other_encounter_filter += encounter_and_filters
    
    if facility_id != 'all':
        try:
            # Filter for encounter-related queries
            encounter_and_filters.append(('ec.facility_id', int(facility_id), '='))
            other_encounter_filter.append(('facility_id', int(facility_id), '='))
            # Filter for facility-related queries
            facility_and_filters.append(('id', int(facility_id), '='))
        except ValueError:
            # Handle case where facility_id is not a valid integer
            pass

    # --- 4. FETCH DATA FROM SERVICES ---
    print('other_encounter_filter', other_encounter_filter)
    print('encounter_and_filter', encounter_and_filters)
    total_encounters = EncounterServices.get_total(and_filter=other_encounter_filter)
    # print(encounter_and_filters)
    active_facilities = len(list(EncounterServices.get_all(and_filter=encounter_and_filters, group_by=['ec.facility_id'])))
    
    top_diseases_data = DashboardServices.top_diseases(start_date=start_date, end_date=end_date, limit=5)
    
    gender_distribution_data = DashboardServices.gender_distribution(start_date=start_date, end_date=end_date)
    male_count = 0
    female_count = 0
    for item in gender_distribution_data:
        if item['gender'] == 'Male':
            male_count = item['gender_count']
        elif item['gender'] == 'Female':
            female_count = item['gender_count']
    total_gender = male_count + female_count
    male_perc = (male_count / total_gender * 100) if total_gender > 0 else 0
    female_perc = (female_count / total_gender * 100) if total_gender > 0 else 0

    # For charts
    monthly_trend_raw = json.loads(DashboardServices.trend_last_n_weeks()) # 6 months
    daily_trend_raw = json.loads(DashboardServices.trend_last_n_days())
    age_distribution = DashboardServices.age_group_distribution(start_date = start_date, end_date = end_date)
    # print(monthly_trend_raw)
    top_facilities_raw = DashboardServices.get_top_facilities(start_date=start_date, end_date=end_date, limit=5)

    # For recent encounters table
    recent_encounters = list(EncounterServices.get_all(limit=5, order_by=[('date', 'DESC')], and_filter=encounter_and_filters))

    # For dropdown
    all_facilities = list(FacilityServices.get_all())

    return render_template('admin.html',
                           # Filters for UI
                           all_facilities=all_facilities,
                           current_period=period,
                           current_facility_id=facility_id,

                           # KPI Cards
                           total_encounters=total_encounters,
                           active_facilities=active_facilities,
                           top_disease=top_diseases_data[0] if top_diseases_data else None,
                           male_perc=round(male_perc, 1),
                           female_perc=round(female_perc, 1),

                           # Chart Data
                           monthly_trend=monthly_trend_raw,
                           gender_distribution=gender_distribution_data,
                           top_diseases=top_diseases_data,
                           top_facilities=top_facilities_raw,
                           age_distribution = age_distribution,
                           daily_trend_raw = daily_trend_raw,

                           # Table Data
                           top_facilities_raw = top_facilities_raw,
                           recent_encounters=recent_encounters  )


@app.route('/admin/reports')
@admin_required
def reports():
    facilities = list(FacilityServices.get_all())
    
    current_year = datetime.now().year
    year_choices = [(year, year) for year in range(current_year - 5 , current_year + 1)]
    month = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 
             'September', 'October', 'Nobember', 'December']
    month_choices = [(num, name) for num, name in enumerate(month, start=1)]
    
    return render_template('reports.html',
                           title="Generate Reports",
                           facilities=facilities,
                           year_choices=year_choices,
                           month_choices = month_choices)


def get_report_data():
    report_type = request.args.get('report_type')
    facility_id_str = request.args.get('facility_id')
    month_str = request.args.get('month')
    year_str = request.args.get('year')
    print('report_type', report_type, 'facility_id', facility_id_str, 'month_str', month_str, 'year_str', year_str)

    month = int(month_str) if month_str else None
    year = int(year_str) if year_str else None
    facility_id = int(facility_id_str) if facility_id_str else None

    report_data = None
    report_title = ""
    start_date = None

    if report_type == 'utilization':
        if not facility_id:
            raise ValidationError("Please select a facility for the Utilization Report.")
        facility, start_date, report_data = ReportServices.generate_service_utilization_report(
            facility=facility_id, month=month, year=year
        )
        report_title = f"Service Utilization Report for {facility.name} "

    elif report_type == 'encounter':
        start_date, report_data = ReportServices.generate_encounter_report(month=month, year=year)
        report_title = "Encounter Report "

    elif report_type == 'categorization':
        start_date, report_data = ReportServices.generate_categorization_report(month=month, year=year)
        report_title = "Disease Categorization Report "
    else:
        raise ValidationError("Invalid report type selected.")
    
    return report_title, start_date, report_data

 
@app.route('/admin/view_report')
@admin_required
def view_report():
    try:
        report_title, start_date, report_data = get_report_data()
    except (MissingError, ValidationError) as e:
        flash(str(e), 'error')
        return redirect(url_for('reports'))
    except ValueError:
        flash("Invalid value provided for month, year, or facility.", 'error')
        return redirect(url_for('reports'))
    # except Exception as e:
        # abort(500)

    # print(report_html)
    header_info = []
    if report_data is not None and not report_data.empty:
        if report_data.columns.nlevels > 1:
            # Handle MultiIndex
            outer_headers = []
            for col in report_data.columns:
                if col[0] not in outer_headers:
                    outer_headers.append(col[0])
            
            for header in outer_headers:
                loc = report_data.columns.get_loc(header)
                colspan = 1
                if isinstance(loc, slice):
                    colspan = loc.stop - loc.start
                elif hasattr(loc, 'sum'): # It's a boolean numpy array
                    colspan = loc.sum()
                
                header_info.append({'name': header, 'colspan': colspan})
    
    return render_template('view_report.html',
                           title=report_title,
                           report_title=report_title,
                           start_date=start_date,
                           report_data=report_data,
                           header_info=header_info) # Pass the new header info
                           
