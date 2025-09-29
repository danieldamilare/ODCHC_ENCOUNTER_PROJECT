from flask_wtf import FlaskForm
import functools
from flask import request, url_for, redirect, flash
from typing import Any, TypeVar, Type
from app.models import get_current_user, Role

def form_to_dict(form: FlaskForm, model: Any) -> dict:
    """Extract object attribute relating to a model from a form and create the model"""
    data = {}
    for field_name, field_obj in form._fields.items():
        if hasattr(model, field_name):
            data[field_name] = field_obj.data
    return data

def populate_form(form: FlaskForm, obj: Any) -> None:
    """Populate form fields from model object"""
    for attr_name, value in vars(obj).items():
        if (hasattr(form, attr_name)):
            field = getattr(form, attr_name)
            field.data = value

def admin_required(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        user = get_current_user()
        if user is None or user.role != Role.admin:
            flash("You cannot access this page")
            return  redirect(request.referrer or url_for('index'))
        return func(*args, **kwargs)
    return wrapper

# def require_facility_access(func, facility_id: int) -> Any:
#     @functools.wraps(func)
#     def wrapper(*args, **kwargs):
#         user = get_current_user()
#         if user is None or (user.facility_id != facility_id and user.role != Role.admin):
#             flash("You cannot access this page")
#             return redirect(request.referrer or url_for('index'))
#         return func(*args, **kwargs)
#     return wrapper