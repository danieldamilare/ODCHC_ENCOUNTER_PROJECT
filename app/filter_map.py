from app.models import Facility, Encounter

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
