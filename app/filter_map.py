from app.models import Facility, Encounter, FacilityScheme

filter_config = {
    'lga': (Facility, 'local_government', '='),
    'scheme_id': (Encounter, 'scheme', '='),
    'gender': (Encounter, 'gender', '='),
    'period': (Encounter, 'date', 'BETWEEN'),
    'facility_id': (Encounter, 'facility_id', '='),
    'start_date': (Encounter, 'date', '>='),
    'policy_number': (Encounter, 'policy_number', '='),
    'outcome': (Encounter, 'outcome', '='),
    'end_date': (Encounter, 'date', '<='),
}

encounter_filter_config = {
    **filter_config,
    'age_group': (Encounter, 'age', 'BETWEEN')
}

facility_filter_config = {
    'lga': (Facility, 'local_government', '='),
    'facility_type': (Facility, 'facility_type', '='),
    'ownership': (Facility, 'ownership', '='),
    'scheme':  (FacilityScheme, 'scheme_id', '='),
}

download_encounter_filter_config = {
    "lga": ("", '"Local Government"', "="),
    "scheme_id": ("", 'Scheme', "="),
    "gender": ("", "Gender", "="),
    "period": ("", '"Date of Encounter"', "BETWEEN"),
    "facility_id": ("", 'Facility ID', "="),
    "policy_number": ("", 'Policy Number', "="),
    "outcome": ("", "Outcome", "="),
    "age_group": ("", "Age", "BETWEEN")
}
