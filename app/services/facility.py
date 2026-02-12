import sqlite3
from typing import List, Tuple, Optional, Iterator
from collections import defaultdict

from app.db import get_db
from app.models import Facility, FacilityScheme, InsuranceScheme, FacilityView
from app.constants import ONDO_LGAS_LOWER
from app.filter_parser import Params, FilterParser
from app.exceptions import ValidationError, MissingError, DuplicateError, QueryParameterError

from .base import BaseServices, _legacy_to_params

class FacilityServices(BaseServices):
    table_name = 'facility'
    model = Facility
    LOCAL_GOVERNMENT = ONDO_LGAS_LOWER
    columns_to_update = {'name', 'local_government', 'facility_type', "ownership"}
    MODEL_ALIAS_MAP = {
        Facility: 'fc',
        FacilityScheme: 'fsc'
    }

    @classmethod
    def create_facility(cls, name: str, lga: str,
                        facility_type: str, scheme: List[int],
                        ownership: str,
                        commit=True) -> Facility:
        db = get_db()
        try:
            cursor = db.execute(f'INSERT INTO {cls.table_name} (name, local_government, facility_type, ownership) VALUES (?, ?, ?, ?)', (
                name, lga, facility_type, ownership))
            new_id = cursor.lastrowid
            if scheme:
                scheme_list = list(set((new_id, x) for x in scheme))
                cls.add_scheme(scheme_list)
            if commit:
                db.commit()
            return cls.get_by_id(new_id)
        except sqlite3.IntegrityError:
            db.rollback()
            raise DuplicateError(f"Facility {name} already exist in database")

    @classmethod
    def add_scheme(cls, scheme_list: List[Tuple[int, int]]):
        db = get_db()
        db.executemany('INSERT INTO facility_scheme (facility_id, scheme_id) VALUES (?, ?)',
                       scheme_list)

    @classmethod
    def get_current_scheme(cls, facility_id: int):
        db = get_db()
        cur = db.execute(
            'SELECT scheme_id from facility_scheme WHERE facility_id = ?', [facility_id])
        return [row['scheme_id'] for row in cur]

    @classmethod
    def get_facility_by_name(cls, name: str) -> Facility:
        db = get_db()
        row = db.execute(
            f'SELECT * FROM {cls.table_name} WHERE name = ?', (name,)).fetchone()
        if row is None:
            raise MissingError(f"No Facility with name {name}")
        return FacilityServices._row_to_model(row, Facility)

    @classmethod
    def delete_facility(cls, facility: Facility):
        db = get_db()
        db.execute(
            f"DELETE FROM {cls.table_name} WHERE id = ?",  [facility.id])
        db.commit()

    @classmethod
    def update_facility(cls, facility: Facility, scheme: List[int]):
        if facility.local_government.lower() not in FacilityServices.LOCAL_GOVERNMENT:
            raise ValidationError("Local Government does not exist in Akure")
        db = get_db()
        try:
            db.execute(
                'DELETE FROM facility_scheme WHERE facility_id = ?', (facility.id, ))
            if scheme:
                scheme_list = [(facility.id, sc) for sc in scheme]
                cls.add_scheme(scheme_list)

            FacilityServices.update_data(facility)
        except DuplicateError:
            db.rollback()
            raise DuplicateError(
                f'Facility with name {facility.name} already exists')
        return facility

    @classmethod
    def get_insurance_list(cls, row_ids: List[int]):
        db = get_db()
        placeholders = ','.join(('?' * len(row_ids)))
        query = f'''
        SELECT
            fsc.facility_id as id,
            isc.id as scheme_id,
            isc.scheme_name,
            isc.color_scheme
        FROM facility_scheme as fsc
        JOIN insurance_scheme as isc
        ON isc.id = fsc.scheme_id
        WHERE fsc.facility_id IN ({placeholders})
        '''

        scheme_rows = db.execute(query, row_ids).fetchall()
        scheme_map = defaultdict(list)
        for row in scheme_rows:
            scheme_map[row['id']].append(InsuranceScheme(id=row['scheme_id'],
                                                         scheme_name=row['scheme_name'],
                                                         color_scheme=row['color_scheme']))
        return scheme_map

    @classmethod
    def get_all(cls,
                params: Optional[Params] = None,
                **kwargs
                ) -> Iterator:

        query = f'''
        SELECT
            fc.id,
            fc.name,
            fc.local_government,
            fc.ownership,
            fc.facility_type
        FROM {cls.table_name} as fc
        LEFT JOIN facility_scheme as fsc on fsc.facility_id = fc.id
        '''
        params = params if params else Params()
        params = params.group(Facility, 'id')
        res = {}
        if params:
            res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        else:
            res = _legacy_to_params(**kwargs)

        query, args = cls._apply_filter(
            base_query=query,
            **res
        )

        db = get_db()
        facility_rows = list(db.execute(query, args).fetchall())
        row_ids = list(set(row['id'] for row in facility_rows))
        scheme_map = cls.get_insurance_list(row_ids)
        done = set()
        for row in facility_rows:
            if row['id'] in done:
                continue
            done.add(row['id'])
            facility = FacilityView(
                id=row['id'],
                lga=row['local_government'],
                scheme=scheme_map[row['id']],
                name=row['name'],
                facility_type=row['facility_type'],
                ownership = row['ownership']
            )
            yield facility

    @classmethod
    def get_total(cls, params: Optional[Params] = None, **kwargs) -> int:
        query = f'SELECT COUNT(DISTINCT fc.id) from {cls.table_name} as fc LEFT JOIN facility_scheme as fsc on fsc.facility_id = fc.id'
        res = {}
        if params is not None:
             if params.group_by or params.order_by:
                raise QueryParameterError("You can't groupby or order by to get_total")

             mapper = {cls.model: cls.table_name} if not cls.MODEL_ALIAS_MAP else cls.MODEL_ALIAS_MAP
             res = FilterParser.parse_params(params, model_map=mapper)
        else:
            res = _legacy_to_params(**kwargs)

        query, args = cls._apply_filter(
            base_query=query,
            base_arg=[],
            **res
        )
        db = get_db()
        res =db.execute(query, args).fetchone()
        if res:
            return res[0]
        else:
            return 0

    @classmethod
    def get_view_by_id(cls, facility_id: int) -> FacilityView:
        try:
            return next(cls.get_all(Params().where(Facility, 'id', '=', facility_id)))
        except StopIteration as e:
            raise MissingError("Facility is invalid and does not exist in database")
