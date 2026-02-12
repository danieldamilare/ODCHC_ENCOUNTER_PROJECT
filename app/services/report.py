import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple, Optional, List

from .base import BaseServices
from .facility import FacilityServices

from app.models import Encounter, Facility, TreatmentOutcome, InsuranceScheme, EncounterDiseases
from app.db import get_db
from app.constants import AgeGroup
from app.exceptions import MissingError, ValidationError

class ReportServices(BaseServices):

    MODEL_ALIAS_MAP = {Encounter: 'ec',
         Facility: 'fc',
         TreatmentOutcome: 'tc',
         InsuranceScheme: 'isc',
         EncounterDiseases: 'ecd'}

    @classmethod
    def get_start_end_date(cls, month: Optional[int], year: Optional[int]):
        filter_date = datetime.now().date()
        if month is not None:
            filter_date = filter_date.replace(month=month)
        if year is not None:
            filter_date = filter_date.replace(year=year)

        start_date = filter_date.replace(day=1)
        if filter_date.month == 12:
            end_date = filter_date.replace(
                year=filter_date.year+1, month=1, day=1) - timedelta(days=1)
        else:
            end_date = filter_date.replace(
                month=filter_date.month+1, day=1) - timedelta(days=1)
        return start_date, end_date

    @classmethod
    def generate_service_utilization_report(cls, facility: int, month: Optional[int] = None,
                                            year: Optional[int] = None,
                                            ) -> Tuple:
        try:
            facility_name = FacilityServices.get_by_id(facility)
        except MissingError:
            raise MissingError("Facility does not exist. No report generated")

        if month and month > 12:
            raise ValidationError("Invalid month selection")
        start_date, end_date = cls.get_start_end_date(month, year)

        query = '''
            SELECT
                ec.policy_number,
                vui.item_name AS disease_name,
                ec.gender,
                ec.age_group
            FROM encounters AS ec
            LEFT JOIN view_utilization_items as vui on vui.encounter_id = ec.id
            WHERE ec.date >= ? and ec.date <= ?
            AND ec.facility_id = ?
        '''
        args = (start_date, end_date, facility)
        db = get_db()

        rows = db.execute(query, args)
        df = pd.DataFrame([dict(row) for row in rows])

        if df.empty:
            raise MissingError("No report available for this timeframe!")

        age_groups = [ag.value for ag in AgeGroup]
        gender = ['M', 'F']

        table = df.pivot_table(
            index='disease_name',
            values='policy_number',
            columns=['age_group', 'gender'],
            aggfunc='count',
            fill_value=0
        ).reindex(
            pd.MultiIndex.from_product([age_groups, gender]),
            axis=1,
            fill_value=0
        )
        # table

        table[('TOTAL', 'M')] = table.loc[:, (slice(None), 'M')].sum(axis=1)
        table[('TOTAL', 'F')] = table.loc[:, (slice(None), 'F')].sum(axis=1)
        table['GRAND TOTAL'] = table[('TOTAL', 'M')] + table[('TOTAL', 'F')]
        table.loc['TOTAL'] = table.sum()
        table = table.reset_index()
        table.index = range(1, len(table) + 1)
        table.index.name = 'S/N'
        table.columns.name = ''
        table.rename(columns={'disease_name': 'Diseases'}, inplace=True)

        return facility_name, start_date, table

    @classmethod
    def generate_encounter_report(cls, month: Optional[int] = None,
                                  year: Optional[int] = None,
                                  ) -> Tuple:

        if month and (month > 12 or month < 1):
            raise ValidationError("Invalid month selection")

        start_date, end_date = cls.get_start_end_date(month, year)

        query = '''
            SELECT
                f.name as facility_name,
                ec.policy_number,
                ec.gender,
                ec.age_group
            FROM encounters as ec
            JOIN facility as f ON ec.facility_id = f.id
            WHERE  ec.date >= ? AND ec.date <= ?
        '''

        db = get_db()
        rows = db.execute(query, (start_date, end_date))
        df = pd.DataFrame([dict(row) for row in rows])

        if df.empty:
            raise MissingError("No report available for this timeframe!")

        age_groups = [ag.value for ag in AgeGroup]
        gender = ['M', 'F']

        table = df.pivot_table(
            index='facility_name',
            values='policy_number',
            columns=['age_group', 'gender'],
            aggfunc='count',
            fill_value=0,

        ).reindex(
            pd.MultiIndex.from_product([age_groups, gender]),
            fill_value=0,
            axis=1
        )

        table[('TOTAL', 'M')] = table.loc[:, (slice(None), 'M')].sum(axis=1)
        table[('TOTAL', 'F')] = table.loc[:, (slice(None), 'F')].sum(axis=1)
        table['GRAND TOTAL'] = table[('TOTAL', 'M')] + table[('TOTAL', 'F')]
        table.loc['TOTAL'] = table.sum()
        table = table.reset_index()
        table.index.name = 'S/N'
        table.index = range(1, len(table) + 1)
        table.columns.name = ''
        table.rename(columns={'facility_name': 'Facilities'}, inplace=True)
        return start_date, table


    @classmethod
    def generate_nhia_encounter_report(cls, month: Optional[int] = None,
                                       year: Optional[int] = None) -> Tuple:
        # 1. Base Query (Note: No GROUP BY yet)
        if month and (month > 12 or month < 1):
            raise ValidationError("Invalid month selection")

        start_date, end_date = cls.get_start_end_date(month, year)

        query = '''
        SELECT
            "Ondo State" as STATE,
            fc.name as "FACILITY NAME",
            fc.facility_type as "FACILITY TYPE",
            fc.ownership as "FACILITY OWNERSHIP",
            fc.local_government as LGA,
            ec.date as "ENCOUNTER DATE(DD/MM/YYYY)",
            ec.nin as NIN,
            ec.hospital_number as "HOSPITAL NUMBER",
            ec.policy_number as "ENROLLEE NUMBER",
            ec.client_name as "ENROLLEE NAME",
            ec.address as "RESIDENTIAL ADDRESS",
            ec.phone_number as "PHONE NUMBER",
            ec.gender as GENDER,
            isc.scheme_name as "TYPE OF PROGRAMME",
            ec.age_group as "AGE GROUP",
            ec.mode_of_entry as "MODE OF ENTRY",
            ec.investigation as INVESTIGATION,
            COALESCE(ec.investigation_cost, 0)/100.0 as "COST OF INVESTIGATION",
            ec.treatment as TREATMENT,
            COALESCE(ec.treatment_cost, 0)/100.0 as "COST OF TREATMENT",
            ec.medication as MEDICATION,
            COALESCE(ec.medication_cost, 0)/100.0 as "COST OF MEDICATION",
            tc.name as OUTCOME,
            ec.referral_reason as "REASON FOR REFERRAL",
            GROUP_CONCAT(dis.name, ', ') as DIAGNOSIS
        FROM encounters as ec
        LEFT JOIN facility as fc on fc.id = ec.facility_id
        LEFT JOIN treatment_outcome as tc on tc.id = ec.outcome
        LEFT JOIN insurance_scheme as isc on isc.id = ec.scheme
        LEFT JOIN encounters_diseases as ecd on ecd.encounter_id = ec.id
        LEFT JOIN diseases as dis on dis.id = ecd.disease_id
        WHERE ec.date >= ? AND ec.date <= ?
        GROUP BY ec.id
        '''

        db = get_db()

        rows = db.execute(query, (start_date, end_date))
        # print("query", query, "start_date", start_date, "end_date", end_date)

        if not rows:
            raise MissingError("No report available for this timeframe!")

        df = pd.DataFrame([dict(row) for row in rows])
        # print("df", df)
        cols_to_sum = ['COST OF INVESTIGATION', 'COST OF TREATMENT', 'COST OF MEDICATION']

        for col in cols_to_sum:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        df['TOTAL COST'] = df[cols_to_sum].sum(axis=1)

        df['CLAIMS AMOUNT SUBMITTED'] = ''
        df['CLAIMS PAID AMOUNT'] = ''
        df['CLAIMS AMOUNT REJECTED'] = ''
        df['REASON FOR REJECTION'] = ''

        date_col = 'ENCOUNTER DATE(DD/MM/YYYY)'
        if date_col in df.columns:
             df[date_col] = pd.to_datetime(df[date_col]).dt.strftime("%d/%m/%Y")

        new_order = [
            'STATE', 'LGA', 'FACILITY NAME', 'FACILITY TYPE', 'FACILITY OWNERSHIP',
            'ENCOUNTER DATE(DD/MM/YYYY)', 'NIN', 'HOSPITAL NUMBER', 'ENROLLEE NUMBER',
            'TYPE OF PROGRAMME', 'ENROLLEE NAME', 'RESIDENTIAL ADDRESS', 'PHONE NUMBER',
            'GENDER', 'AGE GROUP', 'MODE OF ENTRY', # Removed 'NATURE OF VISIT', 'TYPE OF SERVICE' as they aren't in query
            'INVESTIGATION', "COST OF INVESTIGATION", 'DIAGNOSIS', 'TREATMENT', 'COST OF TREATMENT',
            'MEDICATION', 'COST OF MEDICATION', 'TOTAL COST', 'OUTCOME', 'REASON FOR REFERRAL',
            'CLAIMS AMOUNT SUBMITTED', 'CLAIMS PAID AMOUNT', 'CLAIMS AMOUNT REJECTED', "REASON FOR REJECTION"
        ]
        df.index = range(1, len(df) + 1)
        df.index.name = 'S/N'
        df = df.reindex(columns=new_order)
        return start_date, df

    @classmethod
    def generate_categorization_report(cls, month: Optional[int], year: Optional[int]):
        if month and (month < 1 or month > 12):
            raise ValidationError("Invalid Month selection")
        start_date, end_date = cls.get_start_end_date(month, year)
        query = '''
            SELECT
              ec.policy_number,
              cg.category_name,
              f.name as facility_name
            FROM encounters as ec
            JOIN facility as f on ec.facility_id = f.id
            LEFT JOIN encounters_diseases as ed on ed.encounter_id = ec.id
            JOIN diseases as dis on dis.id = ed.disease_id
            JOIN diseases_category as cg on cg.id = dis.category_id
            WHERE ec.date >= ? AND ec.date <= ?
        '''
        db = get_db()
        rows = db.execute(query, (start_date, end_date))
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            raise MissingError("No report available for the time frame")

        table = df.pivot_table(
            index='facility_name',
            values='policy_number',
            columns=['category_name'],
            aggfunc='count',
            fill_value=0
        )
        rows = db.execute('SELECT category_name from diseases_category')
        categories = [row['category_name'] for row in rows]
        table.reindex(categories, axis=1, fill_value=0)
        table['TOTAL'] = table.sum(axis=1)
        table.loc['TOTAL'] = table.sum()
        table.reset_index(inplace=True)
        table.index.name = 'S/N'
        table.columns.name = ''
        table.rename(columns={'facility_name': 'Facilities'}, inplace=True)

        return start_date, table
