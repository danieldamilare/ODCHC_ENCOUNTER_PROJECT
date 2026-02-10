from app.models import Facility, Encounter, FacilityScheme

filter_config = {
    'lga': (Facility, 'local_government', '='),
    'scheme_id': (Encounter, 'scheme', '='),
    'gender': (Encounter, 'gender', '='),
    'period': (Encounter, '', ''),
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
    'name':  (FacilityScheme, 'name', 'LIKE')
}
