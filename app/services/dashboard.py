from app.db import get_db
from app.models import (
    Encounter, Facility, TreatmentOutcome, EncounterDiseases,
    ServiceCategory, Service, DiseaseCategory, User, Disease
)
from app.constants import ONDO_LGAS_LOWER, AgeGroup
from app.filter_parser import FilterParser, Params
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

from .base import BaseServices
from .encounter import EncounterServices

class DashboardServices(BaseServices):
    model = None
    table_name = None
    MODEL_ALIAS_MAP = {**EncounterServices.MODEL_ALIAS_MAP,
                       User : 'u',
                       Disease: 'dis',
                       Service: 'srv',
                       DiseaseCategory: 'dc',
                       EncounterDiseases: 'ecd',
                       EncounterServices: 'ecs'}

    @classmethod
    def get_top_encounter_facilities(cls, params: Params):
        query = f'''
        SELECT
            fc.name AS facility_name,
            COUNT(ec.id) as encounter_count
           FROM encounters as ec
           JOIN facility as fc on ec.facility_id = fc.id
          '''

        params = params.group(Facility, 'id')
        params = params.sort(None, 'encounter_count', 'DESC')
        if not params.limit:
            params = params.set_limit(5)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query=query, **res)

        return cls._run_query(query, args,
                              lambda row: {'facility_name': row['facility_name'],
                                           'encounter_count': row['encounter_count'],
                                           })

    @classmethod
    def get_top_utilization_facilities(cls, params: Params):
        query = f'''
        SELECT
            fc.name AS facility_name,
            COUNT(DISTINCT ec.id) as encounter_count
        FROM encounters as ec
        LEFT JOIN view_utilization_items as vui ON vui.encounter_id = ec.id
        JOIN facility as fc on ec.facility_id = fc.id
          '''

        params = params.group(Facility, 'id')
        params = params.sort(None, 'encounter_count', 'DESC')
        if not params.limit:
            params = params.set_limit(5)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query=query, **res)

        return cls._run_query(query, args,
                              lambda row: {'facility_name': row['facility_name'],
                                           'count': row['encounter_count'],
                                           })

    @classmethod
    def get_age_group(cls, query, args):
        db = get_db()
        rows = db.execute(query, args).fetchall()
        age_group = [g.value for g in AgeGroup]
        used = set()
        result = []

        def parse_key(item: str):
            for idx, group in enumerate(AgeGroup):
                if group.value == item:
                    return idx
            return 999

        for row in rows:
            result.append({'age_group': row['age_group'], 'count': row['age_group_count']})
            used.add(row['age_group'])

        for age in age_group:
            if age not in used:
                result.append({'age_group': age, 'count': 0})

        result = sorted(result, key= lambda x: parse_key(x['age_group']))

        return result

    @classmethod
    def top_utilized_items(cls,
                     params:Params):

        query = '''
        SELECT
         vui.item_name || " (" || vui.item_type || ")" as disease_name,
         COUNT(*) as count
        FROM view_utilization_items as vui

        JOIN encounters as ec on vui.encounter_id = ec.id
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.group(None, 'disease_name')
        params = params.sort(None, 'Count', 'DESC')
        if not params.limit:
            params = params.set_limit(10)

        res:Dict = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        return cls._run_query(query,
                              args,
                              lambda row: {'disease_name': row['disease_name'], 'count': row['count']})
    @classmethod
    def encounter_gender_distribution(cls,
                            params: Params):
        query = '''
            SELECT
              CASE
                WHEN ec.gender = 'M' THEN 'Male'
                WHEN ec.gender = 'F' THEN 'Female'
              END as gender,
            COUNT(ec.gender) as gender_count
            FROM encounters AS ec
            JOIN facility as fc ON fc.id = ec.facility_id
        '''
        params = params.group(Encounter, 'gender')
        if not params.limit:
            params = params.set_limit(5)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        result =  cls._run_query(query,
                             args,
                             lambda row: {'gender': row['gender'], 'count': row['gender_count']})

        gender = ['Male', 'Female']
        used_gender = set(row['gender'] for row in result)
        for row in gender:
            if row not in used_gender:
                result.append({'gender': row, 'count': 0})

        return result


    @classmethod
    def encounter_age_group_distribution(cls,
                               params:Params):

        query = '''
            SELECT age_group, COUNT(*) as age_group_count
            FROM encounters as ec
            JOIN facility as fc ON fc.id = ec.facility_id
        '''

        params = params.group(Encounter, 'age_group')
        res  = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        return cls.get_age_group(query, args)

    @classmethod
    def utilization_age_group_distribution(cls,
                               params:Params):

        query = '''
            SELECT age_group, COUNT(*) as age_group_count
            FROM encounters as ec
            LEFT JOIN  view_utilization_items as vui on vui.encounter_id = ec.id
            JOIN facility as fc ON fc.id = ec.facility_id
        '''

        params = params.group(Encounter, 'age_group')
        res  = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        return cls.get_age_group(query, args)

    @classmethod
    def get_utilization_trend(cls, params: Params, start_date, end_date):
        # ensure at least 6 months range
        start_date = start_date.replace(day=1)
        supposed_start = (end_date.replace(day=1) - relativedelta(month=6))
        if start_date > supposed_start:
            start_date = supposed_start

        query = '''
            SELECT ec.date, COUNT(*) AS date_count
            FROM encounters AS ec
            LEFT JOIN  view_utilization_items as vui on vui.encounter_id = ec.id
            JOIN facility AS fc ON fc.id = ec.facility_id
        '''

        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        params = params.group(Encounter, 'date')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])

        if df.empty:
            return pd.DataFrame(columns=['date', 'date_count']).to_json(orient="records")

        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('M')

        trend = df.groupby('date')['date_count'].sum().reset_index()

        # Fill missing months
        all_months = pd.period_range(start=start_date, end=end_date, freq='M')
        trend = trend.set_index('date').reindex(all_months, fill_value=0)
        trend.index.name = 'date'
        trend = trend.reset_index()

        trend['date'] = trend['date'].astype(str)
        trend.rename(columns={'date_count': 'count'})
        return trend.to_dict(orient="records")

    @classmethod
    def get_encounter_trend(cls, params: Params, start_date, end_date):
        # ensure at least 6 months range
        start_date = start_date.replace(day=1)
        supposed_start = (end_date.replace(day=1) - relativedelta(month=6))
        if start_date > supposed_start:
            start_date = supposed_start

        query = '''
            SELECT ec.date, COUNT(*) AS date_count
            FROM encounters AS ec
            JOIN facility AS fc ON fc.id = ec.facility_id
        '''

        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        params = params.group(Encounter, 'date')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])

        if df.empty:
            return pd.DataFrame(columns=['date', 'date_count']).to_json(orient="records")

        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('M')

        trend = df.groupby('date')['date_count'].sum().reset_index()

        # Fill missing months
        all_months = pd.period_range(start=start_date, end=end_date, freq='M')
        trend = trend.set_index('date').reindex(all_months, fill_value=0)
        trend.index.name = 'date'
        trend = trend.reset_index()

        trend['date'] = trend['date'].astype(str)
        return trend.to_dict(orient='records')

    @classmethod
    def get_encounter_per_scheme(cls, params: Params):
        query = '''
        SELECT
            COUNT(*) as encounter_count,
            isc.scheme_name as encounter_scheme,
            isc.color_scheme as color_scheme
        FROM encounters as ec
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on ec.facility_id = fc.id
        '''

        params = params.group(Encounter, 'scheme')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query= query, **res)
        return cls._run_query(query=query,
                        params=args,
                        row_mapper = lambda row: {'scheme_name': row['encounter_scheme'],
                                                  'color':  row['color_scheme'],
                                                  'count': row['encounter_count']}
                       )


    @classmethod
    def get_mortality_per_scheme(cls, params: Params):
        query = '''
        SELECT
            COUNT(*) as encounter_count,
            isc.scheme_name as encounter_scheme,
            isc.color_scheme as color_scheme
        FROM encounters as ec
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on ec.facility_id = fc.id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(Encounter, 'scheme')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query= query, **res)
        return cls._run_query(query=query,
                        params=args,
                        row_mapper = lambda row: {'scheme_name': row['encounter_scheme'],
                                                  'color':  row['color_scheme'],
                                                  'count': row['encounter_count']}
                       )
    @classmethod
    def case_fatality(cls, params: Params):
        query = '''
        SELECT
            CASE WHEN COUNT(ec.id) = 0 THEN 0
            ELSE
                (SUM(CASE WHEN LOWER(tc.type) = 'death' THEN 1 ELSE 0 END) * 1.0/
                COUNT(ec.id)) * 100.0
            END as fatality_count
        FROM encounters as ec
        JOIN facility as fc on ec.facility_id = fc.id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()
        return row['fatality_count'] if row else 0.0

    @classmethod
    def get_utilization_per_scheme(cls, params: Params):
        query = '''
        SELECT
            COUNT(*) as encounter_count,
            isc.scheme_name as encounter_scheme,
            isc.color_scheme as color_scheme
        FROM encounters as ec
        LEFT JOIN  view_utilization_items as vui on vui.encounter_id = ec.id
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on ec.facility_id = fc.id
        '''

        params = params.group(Encounter, 'scheme')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query= query, **res)
        return cls._run_query(query=query,
                        params=args,
                        row_mapper = lambda row: {'scheme_name': row['encounter_scheme'],
                                                  'color':  row['color_scheme'],
                                                  'count': row['encounter_count']}
                       )
    @classmethod
    def get_treatment_outcome_distribution(cls, params: Params):
        inner_query = '''
        SELECT
            CASE WHEN tc.type = 'Death' THEN 'Death' ELSE tc.name END AS outcome
        FROM encounters AS ec
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome AS tc ON tc.id = ec.outcome
        '''
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(base_query = inner_query, **res)
        new_query = f'SELECT outcome, COUNT(*) as outcome_count FROM ({query}) GROUP BY outcome'
        return cls._run_query(query =new_query,
                      params = args,
                      row_mapper = lambda row: {'outcome': row['outcome'],
                                                'count': row['outcome_count']})

    @classmethod
    def get_referral_count(cls, params: Params):
        query = '''
        SELECT
            COUNT(*) as referral_count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'name', '=', 'Referral')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args =  cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()
        return row['referral_count'] if row else 0


    @classmethod
    def get_total_utilization(cls, params: Params, start_date, end_date):
        diff = end_date - start_date
        prev_start_date = start_date - diff
        prev_end_date = start_date - timedelta(days=1)

        query = '''
        SELECT
            COALESCE(SUM(CASE WHEN ec.date BETWEEN ? AND ? THEN 1 ELSE 0 END), 0) AS current_count,
            COALESCE(SUM(CASE WHEN ec.date BETWEEN ? AND ? THEN 1 ELSE 0 END), 0) AS prev_count
        FROM encounters AS ec
        LEFT JOIN  view_utilization_items as vui on vui.encounter_id = ec.id
        JOIN facility AS fc ON fc.id = ec.facility_id
        '''

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, filter_args = cls._apply_filter(query, **res)

        args = [start_date, end_date, prev_start_date, prev_end_date] + filter_args

        db = get_db()
        row = db.execute(query, args).fetchone()
        current = row['current_count'] if row else 0
        prev = row['prev_count'] if row else 0

        pct_change = 0.0
        if prev:
            pct_change = ((current - prev) / prev) * 100
        else:
            pct_change = 100.0 if current else 0.0

        return current, pct_change


    @classmethod
    def get_total_encounters(cls, params: Params, start_date, end_date):
        diff = end_date - start_date
        prev_start_date = start_date - diff
        prev_end_date = start_date - timedelta(days=1)

        query = '''
        SELECT
            COALESCE(SUM(CASE WHEN ec.date BETWEEN ? AND ? THEN 1 ELSE 0 END), 0) AS current_count,
            COALESCE(SUM(CASE WHEN ec.date BETWEEN ? AND ? THEN 1 ELSE 0 END), 0) AS prev_count
        FROM encounters AS ec
        JOIN facility AS fc ON fc.id = ec.facility_id
        '''

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, filter_args = cls._apply_filter(query, **res)

        args = [start_date, end_date, prev_start_date, prev_end_date] + filter_args

        db = get_db()
        row = db.execute(query, args).fetchone()
        current = row['current_count'] if row else 0
        prev = row['prev_count'] if row else 0

        pct_change = 0.0
        if prev:
            pct_change = ((current - prev) / prev) * 100
        else:
            pct_change = 100.0 if current else 0.0

        return current, pct_change


    @classmethod
    def encounter_distribution_across_lga(cls, params: Params):
        query = '''
        SELECT
            fc.local_government as lga,
            COUNT(*) as count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        params = params.group(Facility, 'local_government')
        params = params.sort(None, 'count', 'DESC')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        rows = db.execute(query, args).fetchall()
        result = dict.fromkeys([x.upper() for x in ONDO_LGAS_LOWER], 0)
        for row in rows:
            result[row['lga'].upper()] = row['count']
        return sorted([{'lga': key, 'count': value} for key, value in result.items()], key = lambda row: row['count'])


    @classmethod
    def utilization_distribution_across_lga(cls, params: Params):
        query = '''
        SELECT
            fc.local_government as lga,
            COUNT(*) as count
        FROM encounters as ec
        LEFT JOIN  view_utilization_items as vui on vui.encounter_id = ec.id
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        params = params.group(Facility, 'local_government')
        params = params.sort(Facility, 'local_government', 'DESC')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        rows = db.execute(query, args).fetchall()
        result = dict.fromkeys([x.upper() for x in ONDO_LGAS_LOWER], 0)
        for row in rows:
            result[row['lga'].upper()] = row['count']
        return sorted([{'lga': key, 'count': value} for key, value in result.items()], key = lambda row: row['count'])


    @classmethod
    def total_utilization_by_scheme_grouped(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            ec.date,
            isc.scheme_name,
            isc.color_scheme
        FROM encounters as ec
        LEFT JOIN  view_utilization_items as vui on vui.encounter_id = ec.id
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        if (end_date - start_date).days < (365 * 5): #minumum of 5 display years
            start_date = end_date.replace(day=1, month=1, year = end_date.year - 5)

        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        # params = params.group(Encounter, 'date').group(InsuranceScheme, 'scheme_name')\
                # .group(InsuranceScheme, 'color_scheme')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            return {}

        df['date'] =  pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('Y')
        df.sort_values('date')
        df = df.groupby(['date', 'scheme_name', 'color_scheme']).size().reset_index(name='count')
        all_schemes = df[['scheme_name', 'color_scheme']].drop_duplicates()
        all_years = pd.period_range(start_date, end_date, freq='Y')
        full_index = []
        for year in all_years:
            for scheme in all_schemes.itertuples(name=None, index=False):
                full_index.append((year, *scheme))
        df = (
            df.set_index(['date', 'scheme_name', 'color_scheme'])
            .reindex(full_index, fill_value=0)
            .reset_index()
        )
        df['date'] = df['date'].astype('str')
        return df.to_dict(orient="records")

    @classmethod
    def mortality_distribution_by_type(cls, params: Params):
        query = '''
        SELECT
            tc.name,
            COUNT(*) as count
        FROM encounters as ec
        JOIN facility as fc ON ec.facility_id = fc.id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(TreatmentOutcome, 'name')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        return cls._run_query(query, args,
                            lambda row: {'death_type': row['name'], 'count': row['count']})

    @classmethod
    def mortality_distribution_by_age_group(cls, params: Params):
        query = '''
        SELECT
            ec.age_group,
            COUNT(*) as age_group_count
        FROM encounters as ec
        JOIN facility as fc ON ec.facility_id = fc.id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.group(Encounter, 'age_group')
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        return cls.get_age_group(query, args)

    @classmethod
    def get_top_cause_of_mortality(cls, params: Params):
        query = '''
        SELECT
         vui.item_name || " (" || vui.item_type || ")" as cause_name,
         COUNT(*) as count
        FROM view_utilization_items as vui
        JOIN encounters as ec on vui.encounter_id = ec.id
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(None, 'cause_name')
        params = params.sort(None, 'count', 'DESC')

        if not params.limit:
            params = params.set_limit(10)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        return cls._run_query(query, args,
                              lambda row: {'name': row['cause_name'], 'count': row['count']})

    @classmethod
    def get_mortality_distribution_by_gender(cls, params: Params):
        query = '''
        SELECT
            CASE
                WHEN ec.gender = 'M' THEN 'Male'
                WHEN ec.gender = 'F' THEN 'Female'
            END as gender,
            COUNT(ec.id) as count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(Encounter, 'gender')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        result =  cls._run_query(query,
                             args,
                             lambda row: {'gender': row['gender'], 'count': row['count']})

        gender = ['Male', 'Female']
        used_gender = set(row['gender'] for row in result)
        for row in gender:
            if row not in used_gender:
                result.append({'gender': row, 'count': 0})

        return result

    @classmethod
    def total_mortality_by_scheme_grouped(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            ec.date ,
            isc.scheme_name,
            isc.color_scheme
        FROM encounters as ec
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        if (end_date - start_date).days < 365 * 5:
            start_date = end_date.replace(year = end_date.year - 5, day = 1, month = 1)
        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            return {}
        df['date'] =  pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('Y')
        df.sort_values('date')
        df = df.groupby(['date', 'scheme_name', 'color_scheme']).size().reset_index(name='count')
        all_schemes = df[['scheme_name', 'color_scheme']].drop_duplicates()
        all_years = pd.period_range(start_date, end_date, freq='Y')
        full_index = []
        for year in all_years:
            for scheme in all_schemes.itertuples(name=None, index=False):
                full_index.append((year, *scheme))
        df = (
            df.set_index(['date', 'scheme_name', 'color_scheme'])
            .reindex(full_index, fill_value=0)
            .reset_index()
        )
        df['date'] = df['date'].astype('str')
        return df.to_dict(orient='records')



    @classmethod
    def total_encounter_by_scheme_grouped(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            ec.date,
            isc.scheme_name,
            isc.color_scheme
        FROM encounters as ec
        JOIN insurance_scheme as isc on isc.id = ec.scheme
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        if (end_date - start_date).days < 365 * 5:
            start_date = end_date.replace(year = end_date.year - 5, day = 1, month = 1)
        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        # params = params.group(Encounter, 'date').group(InsuranceScheme, 'scheme_name')\
                # .group(InsuranceScheme, 'color_scheme')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            return  {}

        df['date'] =  pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period('Y')
        df.sort_values('date')
        df = df.groupby(['date', 'scheme_name', 'color_scheme']).size().reset_index(name='count')
        all_schemes = df[['scheme_name', 'color_scheme']].drop_duplicates()
        all_years = pd.period_range(start_date, end_date, freq='Y')
        full_index = []
        for year in all_years:
            for scheme in all_schemes.itertuples(name=None, index=False):
                full_index.append((year, *scheme))
        df = (
            df.set_index(['date', 'scheme_name', 'color_scheme'])
            .reindex(full_index, fill_value=0)
            .reset_index()
        )
        df['date'] = df['date'].astype('str')
        return df.to_dict(orient='records')

    @classmethod
    def get_mortality_count_per_facility(cls, params: Params):
        query = '''
            SELECT
                fc.name as facility_name,
                COUNT(*) as count
            FROM encounters as ec
            JOIN facility as fc on ec.facility_id = fc.id
            JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(Facility, 'id')
        params = params.sort(None, 'count', 'desc')
        if not params.limit:
            params = params.set_limit(10)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        return cls._run_query(query,
                              args,
                              lambda row: {'facility_name': row['facility_name'],
                                            'count': row['count']})

    @classmethod
    def get_mortality_trend(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            ec.date,
            tc.name as death_type,
            COUNT(ec.id) as death_count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        if (end_date - start_date).days < 30 * 6:
            start_date = end_date.replace(day = 1) - relativedelta(month=6)
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.where(Encounter, 'date', '>=', start_date)
        params = params.where(Encounter, 'date', '<=', end_date)
        params = params.group(Encounter, 'date')
        params = params.group(TreatmentOutcome, 'name')

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        rows = db.execute(query, args).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            return {}

        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.to_period(freq='M')
        rows =  db.execute('SELECT tc.name  as death_type from \
                                  treatment_outcome as tc where tc.type = "Death"').fetchall()
        death_type = [ row['death_type'] for row in rows]
        all_month = list(set(pd.period_range(start=start_date, end=end_date, freq='M')))
        df = df.groupby(['date', 'death_type'], as_index=False)['death_count'].sum()
        df = df.set_index(['date', 'death_type'])
        df = df.reindex(pd.MultiIndex.from_product([all_month, death_type], names = ['date', 'death_type']),
                        fill_value=0).reset_index()
        df.date = df.date.astype(str)
        pivot = df.pivot_table(
            index = 'date',
            columns = 'death_type',
            values= 'death_count',\
            aggfunc= 'sum',
        ).reset_index()
        pivot['total'] = pivot.drop('date', axis=1).sum(axis=1)
        return pivot.to_dict(orient='records')


    @classmethod
    def get_mortality_by_lga(cls, params: Params):
        query = '''
        SELECT
            fc.local_government as lga,
            COUNT(*) as count
        FROM encounters as ec
        JOIN facility as fc on ec.facility_id = fc.id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        params = params.group(Facility, 'local_government')
        params = params.sort(None, 'count', 'DESC')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        rows = db.execute(query, args)
        result = dict.fromkeys([lga.upper() for lga in ONDO_LGAS_LOWER], 0)
        for row in rows:
            result[row['lga'].upper()] = row['count']
        return sorted([{'lga': key, 'count': value} for key, value in result.items()], key= lambda row: row['count'])

    @classmethod
    def get_average_mortality_per_day(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            CASE WHEN COUNT(DISTINCT ec.date) = 0 THEN 0
            ELSE COUNT(*) * 1.0/ COUNT(DISTINCT ec.date)
            END as count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        JOIN treatment_outcome as tc on tc.id = ec.outcome
        '''
        params = params.where(Encounter, 'date', '>=', start_date)\
                        .where(Encounter, 'date', "<=", end_date)
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()
        res = row['count'] if row else 0
        return res or 0

    @classmethod
    def get_average_encounter_per_day(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            CASE WHEN COUNT(DISTINCT ec.date) = 0 THEN 0
            ELSE COUNT(*) * 1.0/ COUNT(DISTINCT ec.date)
            END as count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        params = params.where(Encounter, 'date', '>=', start_date)\
                        .where(Encounter, 'date', "<=", end_date)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()
        res = row['count'] if row else 0
        return res or 0


    @classmethod
    def get_average_utilization_per_day(cls, params: Params, start_date, end_date):
        query = '''
        SELECT
            CASE WHEN COUNT(DISTINCT ec.date) = 0 THEN 0
            ELSE COUNT(*) * 1.0/ COUNT(DISTINCT ec.date)
            END as count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        LEFT JOIN view_utilization_items as vui on ec.id =  vui.encounter_id
        '''
        params = params.where(Encounter, 'date', '>=', start_date)\
                        .where(Encounter, 'date', "<=", end_date)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)

        db = get_db()
        row = db.execute(query, args).fetchone()
        res = row['count'] if row else 0
        return res or 0

    @classmethod
    def get_service_utilization_rate(cls, params: Params, start_date: date, end_date: date):
        query = '''
        SELECT
            CASE WHEN COUNT(DISTINCT ec.id) = 0 THEN 0
            ELSE ((COUNT(DISTINCT ec.id) * 1.0) /COUNT(*)) *100
            END AS rate
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        LEFT JOIN view_utilization_items as vui on ec.id =  vui.encounter_id
        '''
        params = params.where(Encounter, 'date', '>=', start_date)\
                        .where(Encounter, 'date', "<=", end_date)

        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()

        res = row['rate'] if row else 0
        return res or 0

    @classmethod
    def get_total_death_outcome(cls, params: Params, start_date: date, end_date: date):
        ''''
        Return total death outcome and the percentage difference based on the prevous month
        Params should not filter based on date. but instead parse the start_date and end_date
        '''

        diff = end_date - start_date
        prev_start_date = start_date - diff
        prev_end_date = start_date - timedelta(days=1)
        query = '''
        SELECT
            COALESCE(SUM(CASE WHEN ec.date >= ? and ec.date <= ? THEN 1 ELSE 0 END), 0)  as prev_count,
            COALESCE(SUM(CASE WHEN ec.date >= ? and ec.date <= ? THEN 1 ELSE 0 END), 0)  as current_count
        FROM encounters as ec
        JOIN facility as fc on ec.facility_id = fc.id
        JOIN treatment_outcome as tc on ec.outcome = tc.id
        '''
        params = params.where(TreatmentOutcome, 'type', '=', 'Death')
        db = get_db()
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)


        query, args = cls._apply_filter(base_query= query, **res)
        args = [prev_start_date, prev_end_date, start_date, end_date] + args
        row = db.execute(query, args).fetchone()

        prev = row['prev_count'] if row else 0
        current = row['current_count'] if row else 0
        pct_change = 0

        if prev:
            pct_change =  ((current - prev)/prev) * 100
        elif current:
            pct_change = 100
        return (current, pct_change)


    @classmethod
    def get_active_encounter_facility(cls, params: Params):
        query = '''
        SELECT
            COUNT (DISTINCT fc.id) as facility_count
        FROM encounters as ec
        JOIN facility as fc on fc.id = ec.facility_id
        '''
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query, **res)
        db = get_db()
        row = db.execute(query, args).fetchone()
        return row['facility_count'] if row else 0

    @classmethod
    def get_top_facilities_summaries(cls, params: Params, start_date, end_date,):
        query = '''
        SELECT
            fc.name AS facility_name,
            COUNT(ec.id) as encounter_count,
            (
                SELECT dis.name
                FROM encounters AS ec2
                JOIN encounters_diseases as ecd ON ec2.id = ecd.encounter_id
                JOIN diseases AS dis ON ecd.disease_id = dis.id
                WHERE ec2.facility_id = ec.facility_id
                 GROUP BY ecd.disease_id
                 ORDER BY COUNT(ecd.disease_id) DESC
                 LIMIT 1
            ) AS top_disease,
           MAX(ec.created_at) as last_submission
           FROM encounters as ec
            JOIN facility as fc on ec.facility_id = fc.id
          '''
        params = (params.where(Encounter, 'date', '>=', start_date)
                        .where(Encounter, 'date', '<=', end_date)
                        .group(Encounter, 'facility_id')
                        .sort(None, 'encounter_count', 'DESC'))
        if not params.limit:
            params = params.set_limit(10)
        res = FilterParser.parse_params(params, cls.MODEL_ALIAS_MAP)
        query, args = cls._apply_filter(query,  **res)
        # print(query)

        return cls._run_query(query, args,
                              lambda row: {'facility_name': row['facility_name'],
                                           'encounter_count': row['encounter_count'],
                                           'top_disease': row['top_disease'],
                                           'last_submission': row['last_submission']})

