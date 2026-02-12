import sqlite3
from datetime import date, datetime
from typing import Dict, List, Optional, Iterator, Literal
from collections import defaultdict

from app.db import get_db
from app.filter_parser import Params, FilterParser
from app.exceptions import (
    ValidationError, MissingError, InvalidReferenceError,
    ServiceError, QueryParameterError
)
from app.constants import EncType, DeliveryMode, BabyOutcome
from app.models import (
    Encounter, Facility, TreatmentOutcome, InsuranceScheme,
    EncounterDiseases, ANCRegistry, DeliveryBaby,
    ServiceCategory, Service, FacilityView, DeliveryEncounter,
    ServiceView, DiseaseCategory, DiseaseView, ANCEncounterView,
    ChildHealthEncounterView, DeliveryEncounterView, ChildHealth,
    EncounterView, User
)

from .base import BaseServices, _legacy_to_params
from .facility import FacilityServices

class EncounterServices(BaseServices):
    table_name = 'encounters'
    model = Encounter
    columns = {'id', 'facility_id', 'date', 'policy_number', 'client_name',
               'gender', 'age',  'enc_type', 'address',  'scheme', 'nin', 'phone_number',
               'hospital_number', 'referral_reason', 'age_group', "mode_of_entry",
               "treatment", "treatment_cost", "medication", "medication_cost", "investigation", "investigation_cost", "doctor_name", "outcome",
               'created_by', 'created_at'
               }

    MODEL_ALIAS_MAP = {Encounter: 'ec',
         Facility: 'fc',
         TreatmentOutcome: 'tc',
         InsuranceScheme: 'isc',
         EncounterDiseases: 'ecd'}

    @classmethod
    def get_total(cls,
                  params: Optional[Params] = None,
                  **kwargs) -> int:

        query = f'''
            SELECT COUNT(*) from {cls.table_name} as ec
            JOIN insurance_scheme as isc on isc.id = ec.scheme
            JOIN facility as fc on ec.facility_id = fc.id
            JOIN treatment_outcome as tc on ec.outcome = tc.id
            LEFT JOIN users AS u ON ec.created_by = u.id
        '''

        res ={}
        if params is not None:
            if params.group_by or params.order_by:
                raise QueryParameterError("You can't groupby or order by to get_total")

            mapper = cls.MODEL_ALIAS_MAP
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
    def create_encounter(cls, facility_id: int,
                         date: date,
                         policy_number: str,
                         client_name: str,
                         gender: str,
                         age: int,
                         treatment: Optional[str],
                         doctor_name: Optional[str],
                         scheme: int,
                         nin: str,
                         phone_number: str,
                         mode_of_entry: str,
                         address: str,
                         hospital_number: str,
                         age_group: str,
                         referral_reason: Optional[str],
                         treatment_cost:  Optional[float],
                         investigation: Optional[str],
                         investigation_cost: Optional[float],
                         medication: Optional[str],
                         medication_cost: Optional[float],
                         outcome: int,
                         created_by: int,
                         diseases_id: Optional[List[int]] = None,
                         enc_type: EncType = EncType.GENERAL,
                         services_id: Optional[List[int]] = None,
                         commit: bool=True) -> Encounter:
        db = get_db()
        gender = gender.upper().strip()
        policy_number = policy_number.strip()
        client_name = client_name.strip()
        treatment = treatment.strip() if treatment else treatment
        doctor_name = doctor_name .strip() if doctor_name else doctor_name
        nin = nin.strip()
        phone_number = phone_number.strip()
        created_at = datetime.now().date()
        medication_cost_in_kobo = int(medication_cost * 100) if medication_cost else 0
        investigation_cost_in_kobo = int(investigation_cost * 100) if investigation_cost else 0
        treatment_cost_in_kobo = int(treatment_cost * 100) if treatment_cost else 0

        try:

            cur = db.execute(f'''INSERT INTO {cls.table_name} (facility_id, date, policy_number
                   , client_name, gender, age, age_group, scheme, nin, phone_number,
                   enc_type, referral_reason, mode_of_entry, treatment, treatment_cost,
                   medication, medication_cost, investigation, investigation_cost, outcome,
                   doctor_name, address, hospital_number, created_by, created_at) VALUES( ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                   (facility_id, date, policy_number, client_name, gender, age,
                    age_group, scheme, nin, phone_number, enc_type.value, referral_reason,
                    mode_of_entry, treatment, treatment_cost_in_kobo, medication, medication_cost_in_kobo,
                    investigation, investigation_cost_in_kobo, outcome, doctor_name, address, hospital_number, created_by,
                    created_at))

            new_id = cur.lastrowid
            if diseases_id:
                diseases_list = list(set((new_id, x) for x in diseases_id))
                db.executemany('''INSERT into encounters_diseases(encounter_id, disease_id)
                            VALUES(?, ?)''', diseases_list)
            if services_id:
                services_list = list(set((new_id, x) for x in services_id))
                db.executemany('''INSERT into encounters_services(encounter_id, service_id)
                               VALUES(?, ?)''', services_list)
            if commit:
                db.commit()
            return cls.get_by_id(new_id)

        except sqlite3.IntegrityError as e:
            db.rollback()
            error_msg = str(e).lower()
            if 'foreign key' in error_msg:
                raise InvalidReferenceError(
                    "Invalid reference to facility, disease, or user"
                )
            elif 'check constraint' in error_msg:
                raise ValidationError(
                    "Data validation failed: check age and gender values"
                )
            else:
                raise InvalidReferenceError(f"Database error: {str(e)}")

    @classmethod
    def create_delivery_encounter(cls,
                                facility_id: int,
                                date: date,
                                policy_number: str,
                                client_name: str,
                                gender: str,
                                age: int,
                                treatment: Optional[str],
                                doctor_name: Optional[str],
                                scheme: int,
                                nin: str,
                                mode_of_entry: str,
                                age_group: str,
                                address: str,
                                hospital_number: str,
                                referral_reason: Optional[str],
                                treatment_cost:  Optional[float],
                                investigation: Optional[str],
                                investigation_cost: Optional[float],
                                medication: Optional[str],
                                medication_cost: Optional[float],
                                phone_number: str,
                                created_by: int,
                                anc_id: int,
                                anc_count: int,
                                mode_of_delivery: DeliveryMode,
                                mother_outcome: int,
                                baby_details: List[Dict],
                                commit: bool = True
                                ):

        db = get_db()
        try:
            new_enc = cls.create_encounter(
                facility_id = facility_id,
                date = date,
                policy_number = policy_number,
                client_name = client_name,
                gender = gender,
                age =  age,
                age_group = age_group,
                scheme = scheme,
                address = address,
                hospital_number = hospital_number,
                nin = nin,
                phone_number = phone_number,
                enc_type  = EncType.DELIVERY,
                referral_reason = referral_reason,
                mode_of_entry = mode_of_entry,
                treatment= treatment,
                treatment_cost = treatment_cost,
                medication = medication,
                medication_cost = medication_cost,
                investigation = investigation,
                investigation_cost = investigation_cost,
                doctor_name = doctor_name,
                outcome = mother_outcome,
                created_by = created_by,
                commit = False
            )
            cur = db.execute('''
            INSERT INTO delivery_encounters(anc_id, encounter_id, anc_count,
                              mode_of_delivery)
            VALUES(?, ?, ?, ?)''',

            (anc_id, new_enc.id, anc_count, mode_of_delivery.value))
            identifier = 'cesarean' if (mode_of_delivery == DeliveryMode.CS) else "delivery"
            service_id = db.execute('''SELECT id from services WHERE LOWER(name) LIKE ?''', (f'%{identifier}%',)).fetchone()
            if not service_id:
                raise MissingError(f"Service {identifier} not found")

            db.execute("INSERT INTO encounters_services(encounter_id, service_id) VALUES(?, ?)", (new_enc.id, service_id['id']))

            db.execute('''
            UPDATE anc_registry SET status = "inactive" WHERE id = ?
            ''', (anc_id,))

            baby_list = [(new_enc.id, baby['gender'], baby['outcome']) for baby in baby_details]

            db.executemany("""INSERT INTO delivery_babies(encounter_id, gender, outcome)
                           VALUES (?, ?, ?)""", baby_list)
            if commit:
                db.commit()
            return new_enc
        except Exception as e:
            db.rollback()
            raise ServiceError(f"Failed to create delivery encounter: {str(e)}")

    @classmethod
    def create_anc_encounter(cls,
                             lmp: date,
                             policy_number: str,
                             kia_date: date,
                             client_name: str,
                             booking_date: date,
                             parity: int,
                             place_of_issue: str,
                             date: date,
                             mode_of_entry: str,
                             age_group: str,
                             address: str,
                             hospital_number: str,
                             referral_reason: Optional[str],
                             treatment_cost:  Optional[float],
                             investigation: Optional[str],
                             investigation_cost: Optional[float],
                             medication: Optional[str],
                             medication_cost: Optional[float],
                             expected_delivery_date: date,
                             anc_count: int,
                             facility_id: int,
                             gender: Literal["M", "F"],
                             age: int,
                             scheme: int,
                             nin: str,
                             phone_number: str,
                             doctor_name: str,
                             outcome: int,
                             created_by: int,
                             treatment: Optional[str],
                             commit: bool = True
                             ):
        db = get_db()
        try:
            new_enc = cls.create_encounter(
                facility_id = facility_id,
                date = date,
                policy_number = policy_number,
                client_name = client_name,
                gender = gender,
                age =  age,
                age_group = age_group,
                scheme = scheme,
                nin = nin,
                phone_number = phone_number,
                enc_type  = EncType.ANC,
                referral_reason = referral_reason,
                mode_of_entry = mode_of_entry,
                address = address,
                hospital_number = hospital_number,
                treatment= treatment,
                treatment_cost = treatment_cost,
                medication = medication,
                medication_cost = medication_cost,
                investigation = investigation,
                investigation_cost = investigation_cost,
                doctor_name = doctor_name,
                outcome = outcome,
                created_by = created_by,
                commit = False
            )

            anc_service = db.execute('''SELECT id from services where LOWER(name) LIKE ?''', ('%antenatal%',)).fetchone()
            if not anc_service:
                raise MissingError("ANC service not found in services table")
            db.execute('''
            INSERT INTO encounters_services(encounter_id, service_id) VALUES(?, ?)''', (new_enc.id, anc_service['id']))

            existing = db.execute(
                "SELECT id FROM anc_registry where orin = ? AND status = 'active'",
                (policy_number, )).fetchone()
            anc_id = None;
            if existing:
                db.execute("UPDATE anc_registry SET anc_count = anc_count+1 WHERE id = ?",
                           (existing['id'], ))
                anc_id = existing['id']
            else:
                cur = db.execute(
                '''INSERT INTO anc_registry(orin, kia_date, client_name,
                booking_date, parity, place_of_issue, hospital_number, address, lmp,
                expected_delivery_date, anc_count, status, nin, phone_number, age, age_group) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (policy_number, kia_date, client_name, booking_date, parity,
                place_of_issue, hospital_number, address, lmp, expected_delivery_date,
                anc_count, "active", nin, phone_number, age, age_group))
                anc_id = cur.lastrowid

            new_id = new_enc.id

            db.execute('''INSERT INTO anc_encounters(encounter_id, anc_id, anc_count) VALUES (
                       ?, ?, ?)''', (new_enc.id, anc_id, anc_count))
            if commit:
                db.commit()
            return new_enc
        except Exception as e:
            db.rollback()
            raise ServiceError(f"Failed to create ANC encounter: {str(e)}")

    @classmethod
    def create_child_health_encounter(cls,
                         facility_id: int,
                         date: date,
                         policy_number: str,
                         client_name: str,
                         gender: str,
                         age: int,
                         treatment: Optional[str],
                         doctor_name: Optional[str],
                         scheme: int,
                         nin: str,
                         hospital_number: str,
                         phone_number: str,
                         outcome: int,
                         created_by: int,
                         address: str,
                         guardian_name: str,
                         mode_of_entry: str,
                         age_group: str,
                         referral_reason: Optional[str],
                         treatment_cost:  Optional[float],
                         investigation: Optional[str],
                         investigation_cost: Optional[float],
                         medication: Optional[str],
                         medication_cost: Optional[float],
                         dob: date,
                         diseases_id: Optional[List[int]] = None,
                         services_id: Optional[List[int]] = None,
                         commit: bool= True):

        db = get_db()
        try:
            new_enc = cls.create_encounter(
                facility_id = facility_id,
                date = date,
                policy_number = policy_number,
                client_name = client_name,
                gender = gender,
                age =  age,
                age_group = age_group,
                scheme = scheme,
                nin = nin,
                hospital_number = hospital_number,
                address= address,
                phone_number = phone_number,
                enc_type  = EncType.CHILDHEALTH,
                referral_reason = referral_reason,
                mode_of_entry = mode_of_entry,
                treatment= treatment,
                treatment_cost = treatment_cost,
                medication = medication,
                medication_cost = medication_cost,
                investigation = investigation,
                investigation_cost = investigation_cost,
                doctor_name = doctor_name,
                outcome = outcome,
                created_by = created_by,
                services_id= services_id,
                diseases_id = diseases_id,
                commit = False
            )

            query = '''
            INSERT INTO child_health_encounters(encounter_id, orin, dob, address, guardian_name)
            VALUES(?, ?, ?, ?, ?)'''
            db.execute(query, (new_enc.id, policy_number, dob, address, guardian_name))
            if commit:
                db.commit()
            return new_enc
        except Exception as e:
            db.rollback()
            raise ServiceError(f"Failed to create child health encounter: {str(e)}")

    @classmethod
    def get_anc_record_by_registry(cls, orin: str) -> ANCRegistry:
        orin = orin.strip()
        query = '''
        SELECT * FROM anc_registry
        WHERE orin = ? AND status = 'active'
        '''
        db = get_db()
        row = db.execute(query, (orin,)).fetchone()
        if not row:
            raise MissingError("Can't find record from registry")
        return ANCRegistry(
            id = row['id'],
            orin = row['orin'],
            kia_date = row['kia_date'],
            booking_date= row['booking_date'],
            client_name = row['client_name'],
            parity = row['parity'],
            place_of_issue= row['place_of_issue'],
            hospital_number= row['hospital_number'],
            address = row['address'],
            lmp = row['lmp'],
            nin = row['nin'],
            phone_number= row['phone_number'],
            expected_delivery_date= row['expected_delivery_date'],
            anc_count= row['anc_count'],
            age = row['age'],
            age_group = row['age_group'],
            status= row['status']
        )

    @classmethod
    def _get_base_encounter(cls,
                params: Optional[Params] = None,
                **kwargs
                ):
        query = '''
            SELECT
                ec.id,
                ec.facility_id,
                ec.date,
                ec.policy_number,
                ec.client_name,
                ec.gender,
                ec.age,
                ec.age_group,
                ec.enc_type,
                ec.nin,
                ec.phone_number,
                isc.id as scheme_id,
                isc.scheme_name,
                isc.color_scheme,
                ec.doctor_name,
                ec.referral_reason,
                ec.mode_of_entry,
                COALESCE(ec.treatment_cost, 0)/100.0 as treatment_cost,
                ec.medication,
                COALESCE(ec.medication_cost, 0) /100.0 as medication_cost,
                ec.investigation,
                COALESCE(ec.investigation_cost, 0)/100.0 as investigation_cost,
                tc.name as treatment_outcome,
                tc.type as treatment_type,
                tc.id as treatment_id,
                ec.created_at,
                ec.treatment,
                ec.address,
                ec.hospital_number,
                fc.name as facility_name,
                fc.facility_type,
                fc.local_government as lga,
                fc.ownership,
                u.username AS created_by
            FROM encounters AS ec
            JOIN insurance_scheme as isc on isc.id = ec.scheme
            JOIN facility as fc on ec.facility_id = fc.id
            JOIN treatment_outcome as tc on ec.outcome = tc.id
            LEFT JOIN users AS u ON ec.created_by = u.id
        '''   #polymorphism for all encounters

        if params:
            res =FilterParser.parse_params(params,cls.MODEL_ALIAS_MAP)
        else:
            res = _legacy_to_params(**kwargs)

        query, args = cls._apply_filter(
            base_query=query,
            base_arg=[],
            **res
        )

        db = get_db()
        return db.execute(query, args).fetchall()

    @classmethod
    def _get_diseases_mapping(cls, encounter_ids: List) -> Dict:
        if not encounter_ids:
            return {}

        db = get_db()

        placeholders = ','.join('?' * len(encounter_ids))
        diseases_query = f'''
            SELECT
                ecd.encounter_id,
                dis.id AS disease_id,
                dis.name AS disease_name,
                cg.id AS category_id,
                cg.category_name
            FROM encounters_diseases AS ecd
            JOIN diseases AS dis ON ecd.disease_id = dis.id
            JOIN diseases_category AS cg ON dis.category_id = cg.id
            WHERE ecd.encounter_id IN ({placeholders})
            ORDER BY ecd.encounter_id
        '''
        diseases_rows = db.execute(diseases_query, encounter_ids).fetchall()
        diseases_by_encounter = defaultdict(list)

        for row in diseases_rows:
            encounter_id = row['encounter_id']
            category = DiseaseCategory(
                id=row['category_id'],
                category_name=row['category_name']
            )
            disease = DiseaseView(
                id=row['disease_id'],
                name=row['disease_name'],
                category=category
            )
            diseases_by_encounter[encounter_id].append(disease)
        return diseases_by_encounter

    @classmethod
    def _get_services_mapping(cls, encounter_ids: List)-> Dict:
        if not encounter_ids:
            return {}

        db = get_db()
        placeholders = ', '.join('?' * len(encounter_ids))
        services_query = f'''
            SELECT
                ecs.encounter_id,
                srv.id as service_id,
                srv.name as service_name,
                scg.id as category_id,
                scg.name as category_name
            FROM encounters_services as ecs
            JOIN services as srv on srv.id = ecs.service_id
            JOIN service_category as scg on scg.id = srv.category_id
            WHERE ecs.encounter_id in ({placeholders})
        '''

        service_rows = db.execute(services_query, encounter_ids).fetchall()
        services_by_encounter = defaultdict(list)
        for row in service_rows:
            encounter_id = row['encounter_id']
            category = ServiceCategory(
                id = row['category_id'],
                name = row['category_name']
            )
            service = ServiceView(
                id = row['service_id'],
                name = row['service_name'],
                category = category
            )
            services_by_encounter[encounter_id].append(service)
        return services_by_encounter

    @classmethod
    def _get_babies_mapping(cls, delivery_ids: List) -> Dict:
        if not delivery_ids:
            return {}

        placeholders = ', '.join('?' * len(delivery_ids))
        db = get_db()
        babies_query = f'''
            SELECT
                db.id as baby_id,
                db.encounter_id as encounter_id,
                db.gender,
                db.outcome
            FROM delivery_babies as db WHERE db.encounter_id in ({placeholders})
        '''
        babies_row = db.execute(babies_query, delivery_ids).fetchall()
        babies_list = defaultdict(list)

        for row in babies_row:
            encounter_id = row['encounter_id']
            babies_list[encounter_id].append(DeliveryBaby(
                id = row['baby_id'],
                gender = row['gender'],
                outcome = BabyOutcome(row['outcome'])
            ))
        return babies_list

    @classmethod
    def _get_anc_mapping(cls, anc_ids: List) -> Dict[int, ANCRegistry]:
        if not anc_ids:
            return {}

        placeholders = ','.join('?' * len(anc_ids))
        query = f'''
        SELECT
            ae.encounter_id,
            ar.id as anc_id,
            ar.kia_date,
            ar.orin,
            ar.booking_date,
            ar.parity,
            ar.place_of_issue,
            ar.hospital_number,
            ar.client_name,
            ar.address,
            ar.lmp,
            ar.nin,
            ar.phone_number,
            ar.age_group,
            ar.age,
            ar.expected_delivery_date as edd,
            ae.anc_count,
            ar.status as anc_status
        FROM anc_encounters as ae
        JOIN anc_registry as ar on ar.id = ae.anc_id
        WHERE ae.encounter_id IN ({placeholders})
        '''
        db = get_db()
        rows = db.execute(query, anc_ids).fetchall()
        mapping = {}
        for row in rows:
            encounter_id = row['encounter_id']

            anc_encounter = ANCRegistry(
                id = row['anc_id'],
                orin = row['orin'],
                kia_date = row['kia_date'],
                client_name= row['client_name'],
                booking_date = row['booking_date'],
                parity= row['parity'],
                place_of_issue= row['place_of_issue'],
                hospital_number= row['hospital_number'],
                address= row['address'],
                lmp = row['lmp'],
                nin = row['nin'],
                phone_number = row['phone_number'],
                age = row['age'],
                age_group = row['age_group'],
                expected_delivery_date= row['edd'],
                anc_count= row['anc_count'],
                status = row['anc_status']
            )
            mapping[encounter_id] = anc_encounter
        return mapping

    @classmethod
    def _get_delivery_mapping(cls, delivery_ids: List) -> Dict[int, DeliveryEncounter]:
        if not delivery_ids:
            return {}

        placeholders = ','.join('?' * len(delivery_ids))
        query = f'''
        SELECT
            de.encounter_id,
            de.id,
            de.anc_count,
            de.mode_of_delivery
        FROM delivery_encounters as de
        WHERE de.encounter_id in ({placeholders})
        '''

        db = get_db()
        rows = db.execute(query, delivery_ids).fetchall()
        delivery_babies = cls._get_babies_mapping(delivery_ids=delivery_ids)
        mapping = {}
        for row in rows:
            encounter_id = row['encounter_id']
            mapping[encounter_id]  = DeliveryEncounter(
                id =row['id'],
                mode_of_delivery= DeliveryMode(row['mode_of_delivery']),
                babies = delivery_babies.get(encounter_id, []),
                anc_count = row['anc_count']
            )
        return mapping

    @classmethod
    def _get_child_health_mapping(cls, child_health_ids: List) -> Dict:
        if not child_health_ids:
            return {}

        placeholders = ','.join('?' * len(child_health_ids))
        query = f'''
        SELECT
            che.encounter_id,
            che.id as id,
            che.orin,
            che.dob,
            che.address as address,
            che.guardian_name
        FROM child_health_encounters as che
        WHERE che.encounter_id in ({placeholders})
        '''

        db = get_db()
        rows = db.execute(query, child_health_ids).fetchall()
        mapping = {}
        for row in rows:
            encounter_id = row['encounter_id']
            mapping[encounter_id] = ChildHealth(
                id = row['id'],
                dob = row['dob'],
                address = row['address'],
                guardian_name = row['guardian_name'],
                orin = row['orin']
            )
        return mapping

    @classmethod
    def _build_general_encounter(cls,
                                  facility: FacilityView,
                                  isc: InsuranceScheme,
                                  services_list: List,
                                  diseases_list: List,
                                  row: Dict):
        encounter = EncounterView(
                id=row['id'],
                facility= facility,
                insurance_scheme= isc,
                diseases=diseases_list,
                services = services_list,
                policy_number=row['policy_number'],
                client_name=row['client_name'],
                gender=row['gender'],
                hospital_number= row['hospital_number'],
                address = row['address'],
                date=row['date'],
                mode_of_entry= row['mode_of_entry'],
                treatment_cost= row['treatment_cost'],
                age_group = row['age_group'],
                investigation= row['investigation'],
                investigation_cost = row['investigation_cost'],
                medication= row['medication'],
                medication_cost = row['medication_cost'],
                referral_reason= row['referral_reason'],
                treatment_outcome= TreatmentOutcome(id = row['treatment_id'],
                                            name = row['treatment_outcome'],
                                            type = row['treatment_type']),
                age=row['age'],
                nin = row['nin'],
                enc_type = EncType(row['enc_type']),
                phone_number= row['phone_number'],
                treatment=row['treatment'],
                doctor_name=row['doctor_name'],
                created_by=row['created_by'],
                created_at=row['created_at']
            )
        return encounter

    @classmethod
    def _build_anc_encounter(cls,
                              facility: FacilityView,
                              isc: InsuranceScheme,
                              services_list: List,
                              diseases_list: List,
                              anc_registry: ANCRegistry,
                              row: Dict) -> ANCEncounterView:
        encounter = cls._build_general_encounter(facility = facility,
                                                 isc = isc,
                                                 services_list = services_list,
                                                 diseases_list= diseases_list,
                                                 row = row)
        return ANCEncounterView(**encounter.__dict__, anc = anc_registry)


    @classmethod
    def _build_delivery_encounter(cls,
                              facility: FacilityView,
                              isc: InsuranceScheme,
                              services_list: List,
                              diseases_list: List,
                              delivery_encounter: DeliveryEncounter,
                              row: Dict) -> DeliveryEncounterView:
        encounter = cls._build_general_encounter(facility = facility,
                                                 isc = isc,
                                                 services_list = services_list,
                                                 diseases_list= diseases_list,
                                                 row = row)
        return DeliveryEncounterView(**encounter.__dict__, delivery= delivery_encounter)

    @classmethod
    def _build_child_health_encounter(cls,
                                       facility: FacilityView,
                                       isc: InsuranceScheme,
                                       services_list: List,
                                       diseases_list: List,
                                       child_encounter: ChildHealth,
                                       row: Dict):
        encounter = cls._build_general_encounter(facility = facility,
                                                  isc = isc,
                                                  services_list= services_list,
                                                  diseases_list= diseases_list,
                                                  row = row)
        return ChildHealthEncounterView(
            **encounter.__dict__, health_details=child_encounter
        )

    @classmethod
    def get_all(cls,
                params: Optional[Params] = None,
                **kwargs
                ) -> Iterator:

        encounters_rows = cls._get_base_encounter(params, **kwargs)

        encounter_ids = []
        delivery_ids = []
        anc_ids = []
        child_health_id = []

        for row in encounters_rows:
            encounter_ids.append(row['id'])
            if row['enc_type'] == EncType.DELIVERY.value:
                delivery_ids.append(row['id'])
            elif row['enc_type'] == EncType.ANC.value:
                anc_ids.append(row['id'])
            elif row['enc_type'] == EncType.CHILDHEALTH.value:
                child_health_id.append(row['id'])

        if not encounter_ids:
            return []

        diseases_by_encounter = cls._get_diseases_mapping(encounter_ids)
        services_by_encounter = cls._get_services_mapping(encounter_ids=encounter_ids)
        anc_by_encounters = cls._get_anc_mapping(anc_ids)
        delivery_by_encounters = cls._get_delivery_mapping(delivery_ids)
        child_health_by_encounters = cls._get_child_health_mapping(child_health_ids=child_health_id)

        facility_ids = [row['facility_id'] for row in encounters_rows]
        scheme_map = FacilityServices.get_insurance_list(facility_ids)

        for row in encounters_rows:
            encounter = None

            facility=FacilityView(
                id=row['facility_id'],
                name=row['facility_name'],
                lga=row['lga'],
                scheme=scheme_map[row['facility_id']],
                facility_type=row['facility_type'],
                ownership = row['ownership']
            )

            insurance_scheme=InsuranceScheme(id=row['scheme_id'],
                            scheme_name=row['scheme_name'],
                            color_scheme=row['color_scheme'])

            if  row['enc_type'] == EncType.ANC.value:
                encounter = cls._build_anc_encounter(
                    facility = facility,
                    isc = insurance_scheme,
                    services_list = services_by_encounter.get(row['id'], []),
                    diseases_list = diseases_by_encounter.get(row['id'], []),
                    anc_registry= anc_by_encounters[row['id']],
                    row = row
                )
            elif row['enc_type'] == EncType.DELIVERY.value:
                encounter = cls._build_delivery_encounter(
                    facility = facility,
                    isc = insurance_scheme,
                    services_list = services_by_encounter.get(row['id'], []),
                    diseases_list = diseases_by_encounter.get(row['id'], []),
                    delivery_encounter = delivery_by_encounters[row['id']],
                    row = row
                )

            elif row['enc_type'] == EncType.GENERAL.value:
                encounter = cls._build_general_encounter(
                    facility = facility,
                    isc = insurance_scheme,
                    services_list = services_by_encounter.get(row['id'], []),
                    diseases_list = diseases_by_encounter.get(row['id'], []),
                    row = row
                )
            elif row['enc_type'] == EncType.CHILDHEALTH.value:
                encounter  = cls._build_child_health_encounter(

                    facility = facility,
                    isc = insurance_scheme,
                    services_list = services_by_encounter.get(row['id'], []),
                    diseases_list = diseases_by_encounter.get(row['id'], []),
                    child_encounter = child_health_by_encounters[row['id']],
                    row = row
                )

            yield encounter

    @classmethod
    def get_encounter_by_facility(cls, facility_id: int) -> Iterator:
        return cls.get_all(params=Params().where(Encounter, 'id', '=', facility_id))

    @classmethod
    def get_view_by_id(cls, id: int) -> EncounterView:
        filters = Params().where(Encounter, 'id', '=', id)
        try:
            return next(cls.get_all(params=filters))
        except StopIteration:
            raise MissingError("Encounter does not exist in database")

    @classmethod
    def update_data(cls, model):
        # do not allow update of encounter
        raise NotImplementedError(
            "Encounter are immutable and cannot be updated")
