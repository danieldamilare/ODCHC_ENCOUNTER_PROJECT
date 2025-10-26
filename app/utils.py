from flask_wtf import FlaskForm
import functools
from flask import url_for, redirect, flash
from app import app
import arrow
from typing import Any
from dataclasses import fields as datafield
from app.models import get_current_user, Role


def form_to_dict(form: FlaskForm, model: Any) -> dict:
    """Extract object attribute relating to a model from a form and create the model"""
    # print("In form to dict")
    data = {}
    # print(vars(model), form._fields.keys())
    model_fields = {f.name for f in datafield(model)}
    for field_name, field_obj in form._fields.items():
        # print(field_name)
        if field_name in model_fields:
            # print(field_name)
            data[field_name] = field_obj.data
    # print(data)
    return data


def admin_required(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        user = get_current_user()
        if user is None or user.role != Role.admin:
            flash("You  don't have access to that page", "error")
            return redirect(url_for('index'))
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


def humanize_datetime_filter(dt):
    """Converts a datetime object or ISO string to a human-readable string."""
    if not dt:
        return "N/A"
    try:
        # Arrow can parse various formats, including ISO strings from databases
        arrow_dt = arrow.get(dt)
        return arrow_dt.humanize()
    except Exception:
        # Fallback if parsing fails
        return str(dt)


# Register the filter with Jinja
app.jinja_env.filters['humanize_datetime'] = humanize_datetime_filter
