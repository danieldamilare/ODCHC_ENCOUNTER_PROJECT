from datetime import datetime
from faker import Faker
from app.db import get_db
from app.constants import ONDO_LGAS_LIST, AgeGroup, ModeOfEntry, OutcomeEnum
from app.models import Role
import click
from werkzeug.security import generate_password_hash
from datetime import date, timedelta
from app.constants import EncType, DeliveryMode, BabyOutcome
from app.filter_parser import Params
from app.config import BASE_DIR
from app.utils import calculate_edd
from tqdm import tqdm
import pandas as pd
import os

from app.services import UserServices, DiseaseServices, DiseaseCategoryServices, EncounterServices, ServiceCategoryServices
from app.services import InsuranceSchemeServices, TreatmentOutcomeServices, FacilityServices, ServiceServices
import random

fake = Faker()

db = get_db()
cur = db.cursor()
DATA_DIR = f"{os.path.dirname(BASE_DIR)}/data"
SERVICE_FILE = f'{DATA_DIR}/service_catalog.csv'
DISEASE_FILE = f'{DATA_DIR}/disease_icd10_catalog.csv'
FACILITY_FILE = f'{DATA_DIR}/done facilities.xlsx'

def seed_services():
    df = pd.read_csv(SERVICE_FILE)
    categories = df['category'].unique().tolist()
    for category in tqdm(categories, desc="Seeding Service Categories"):
        ServiceCategoryServices.create_category(
            str(category) )
    category_list =  list(ServiceCategoryServices.get_all())
    cat_map = {}
    for cat in category_list:
        cat_map[cat.name] = cat.id

    for  idx, row in tqdm(df.iterrows(), desc="Seeding Services"):
        category_id = cat_map[str(row['category'])]
        service = row['service_name']
        ServiceServices.create_service(service, category_id)
    print("Successfully Seeded Services")


def seed_diseases():
    df = pd.read_csv(DISEASE_FILE)
    categories = df['Category'].unique().tolist()
    cat_map = {}
    for category in tqdm(categories, desc="Seeding Disease Categories"):
        res = DiseaseCategoryServices.create_category(str(category))
        cat_map[res.category_name] = res.id

    for idx, row in tqdm(df.iterrows(), desc="Seeding Diseases"):
        category_id = cat_map[str(row['Category'])]
        disease = row['Diagnosis']
        DiseaseServices.create_disease(row['Diagnosis'], category_id)
    print("Successfully Seeded Diseases")


def seed_insurance_scheme():
    scheme = [('BHCPFP', '#448264'),
              ('ORANGHIS', '#fc9d03'),
              ('AMCHIS', '#0066ff')]
    for sc in tqdm(scheme, desc="Seeding Insurance Scheme"):
        InsuranceSchemeServices.create_scheme(sc[0], sc[1], commit = False)
    print("Successfully Seeded Insurance Scheme")

def seed_facilities():
    df = pd.read_excel(FACILITY_FILE)
    scheme_map = {s.scheme_name.upper(): s.id for s in InsuranceSchemeServices.get_all()}

    for idx, row in tqdm(df.iterrows(), desc="Seeding Facilities"):
        scheme_list = []
        if bool(row['BHCPF']):
            scheme_list.append(scheme_map['BHCPFP'])
        if bool(row['ORANGHIS']):
            scheme_list.append(scheme_map['ORANGHIS'])
        if bool(row['AMCHIS']):
            scheme_list.append(scheme_map['AMCHIS'])

        facility = FacilityServices.create_facility(
            name= row['HOSPITAL'],
            lga = row['LGA'],
            facility_type = row['TYPE'],
            scheme = scheme_list,
            ownership = row['OWNERSHIP'],
            commit = False
        )
    print("Successfully seeded facilities in the database")

def seed_users():
    print("Creating admin user")
    UserServices.create_user('odchc', None, "password", role=Role.admin, commit= False)
    facilities_list = list(FacilityServices.get_all())
    for facility in tqdm(facilities_list, desc="Creating user for all facilities"):
        user_name = f'facility_user{facility.id}'
        password = f'password{facility.id}'
        UserServices.create_user(user_name, facility.id, password, None, False)
    print("Successfully Seeded User")

def seed_treatment_outcome():
    outcomes = [(OutcomeEnum.INPATIENT.value, 'General'), 
                (OutcomeEnum.OUTPATIENT.value, 'General'), 
                (OutcomeEnum.REFERRED.value, 'General'),
                (OutcomeEnum.NEONATAL_DEATH.value, 'Death'),
                (OutcomeEnum.INFANT_DEATH.value, 'Death'),
                (OutcomeEnum.UNDER_FIVE_DEATH.value, 'Death'), 
                (OutcomeEnum.MATERNAL_DEATH.value, 'Death'),
                (OutcomeEnum.OTHER_DEATH.value, 'Death')]
    for outcome in tqdm(outcomes, desc="Creating treatment outcomes"):
        TreatmentOutcomeServices.create_treatment_outcome(
            name = outcome[0],
            treatment_type=outcome[1],
            commit = False
        )
    print("Successfully seeded Treatment Outcome")

def seed_encounter(num: int = 1000, start_date: date = datetime.now().replace(month=1), end_date: date = datetime.now()):
    policy_start = ['AKS', 'AKN', 'OWO', 'IFE', 'KTP', 'ONW', 'ODG', 'ESE', 'ANW', 'ANE', 'IRL',
                'ILJ', 'ASE', 'IDR', 'ONE', 'ASW']
    facilities = list(FacilityServices.get_all())
    diseases_ids = [d.id for d in DiseaseServices.get_all()]
    service_ids = [s.id for s in ServiceServices.get_all()]
    user_ids = list(UserServices.get_all())
    facility_user_link = {u.facility.id: u.id for u in user_ids if u.facility != None}
    outcome_ids = list(TreatmentOutcomeServices.get_all())
    schemes = list(InsuranceSchemeServices.get_all())

    if not all([facilities, diseases_ids, service_ids, user_ids, outcome_ids, schemes]):
        print("Warning: Missing base data (facilities, diseases, etc.). Encounter seeding may be incomplete.")
        return

    delivery_count = 0
    anc_count = 0
    child_health = 0

    for i in tqdm(range(num), desc="Creating Encounters"):
        facility = random.choice(facilities)
        selected_scheme = random.choice(facility.scheme)
        date = fake.date_between(start_date=start_date, end_date = end_date)
        policy_number = f"{random.choice(policy_start)}/00{random.randint(1000, 99999)}/{random.randint(23, 26)}/C/{random.randint(0, 6)}"
        age = random.randint(0, 90)
        age_group = random.choice(list(AgeGroup))
        treatment_cost = random.randint(500, 10000)
        investigation = fake.sentence(nb_words=4)
        investigation_cost  = random.randint(500, 10000)
        medication = fake.sentence(nb_words=4)
        mode_of_entry = random.choice(list(ModeOfEntry))
        medication_cost = random.randint(500, 10000)
        treatment = fake.sentence(nb_words=4)
        hospital_number = fake.sentence(nb_words = 4)
        address = fake.sentence(nb_words = 3)
        outcome = random.choice(outcome_ids)
        doctor_name = random.choice(["Dr. Owolabi", "Dr. Musa", "Dr. Adeola"])
        created_by = facility_user_link[facility.id]
        address = fake.address()
        client_name = fake.name()
        referral_reason = fake.sentence(nb_words = 4) if outcome.name == 'Referred' else None
        gender = random.choice(["M", "F"])
        phone_number = "08020007040"
        nin  = ''.join(random.choices('0123456789', k=11))

        if selected_scheme.scheme_name == 'AMCHIS':
            policy_number =  ''.join(random.choices('0123456789', k=10))
            age = random.randint(15, 60)
            encounter_type = random.choice([EncType.ANC, EncType.CHILDHEALTH, EncType.DELIVERY])

            if encounter_type == EncType.ANC:
                lmp = date - timedelta(21)
                parity = random.choice(range(1, 5))

                EncounterServices.create_anc_encounter(
                    lmp= lmp,
                    policy_number = policy_number,
                    kia_date= date,
                    client_name = client_name,
                    booking_date = date,
                    parity = parity,
                    place_of_issue= "Hosptital",
                    expected_delivery_date=calculate_edd(date),
                    anc_count = random.choice(range(1, 9)),
                    address=  address,
                    facility_id = facility.id,
                    gender = 'F',
                    treatment_cost = treatment_cost,
                    medication = medication,
                    medication_cost = medication_cost,
                    referral_reason = referral_reason,
                    investigation= investigation,
                    investigation_cost = investigation_cost,
                    age_group = age_group.value,
                    mode_of_entry= mode_of_entry.value,
                    age = age,
                    scheme= selected_scheme.id,
                    phone_number = phone_number,
                    doctor_name = doctor_name,
                    outcome= outcome.id,
                    created_by= created_by,
                    treatment= treatment,
                    nin= nin,
                    hospital_number= phone_number,
                    date = date,
                    commit=False
                )
                anc_count +=1
            elif encounter_type == EncType.CHILDHEALTH:
                guardian_name = random.choice(['Afe', 'Omotayo', 'Abosede', "Eniola", "Temitope"])
                EncounterServices.create_child_health_encounter(
                    facility_id = facility.id,
                    date = date,
                    policy_number = policy_number,
                    client_name= client_name,
                    gender = gender,
                    treatment_cost = treatment_cost,
                    medication = medication,
                    medication_cost = medication_cost,
                    referral_reason = referral_reason,
                    investigation= investigation,
                    investigation_cost = investigation_cost,
                    age_group = age_group.value,
                    mode_of_entry= mode_of_entry.value,
                    treatment = treatment,
                    nin = nin,
                    hospital_number = hospital_number,
                    scheme = selected_scheme.id,
                    doctor_name= doctor_name,
                    phone_number= phone_number,
                    age= age,
                    outcome = outcome.id,
                    created_by = facility_user_link[facility.id],
                    address= address,
                    guardian_name= guardian_name,
                    dob = date,
                    services_id= random.choices(service_ids, k= random.randint(0,3)),
                    diseases_id= random.choices(diseases_ids, k = random.randint(1, 3)),
                    commit=False
                )
                child_health +=1
            elif encounter_type == EncType.DELIVERY:
                delivery_count +=1
        else:
            EncounterServices.create_encounter(
                        facility_id=facility.id,
                        date = date,
                        policy_number=policy_number,
                        client_name= client_name,
                        gender= gender,
                        nin = nin,
                        phone_number= phone_number,
                        outcome= outcome.id,
                        age= age,
                        hospital_number= hospital_number,
                        address = address,
                        treatment = treatment,
                        treatment_cost = treatment_cost,
                        medication = medication,
                        medication_cost = medication_cost,
                        referral_reason = referral_reason,
                        investigation= investigation,
                        investigation_cost = investigation_cost,
                        age_group = age_group.value,
                        mode_of_entry= mode_of_entry.value,
                        doctor_name= doctor_name,
                        scheme = selected_scheme.id,
                        created_by = facility_user_link[facility.id],
                        services_id= random.choices(service_ids, k= random.randint(0,3)),
                        diseases_id= random.choices(diseases_ids, k = random.randint(1, 3)),
                        commit=False
                        )

    remaining = 0
    if delivery_count:
        from app.models import Encounter
        anc_encounters = list(EncounterServices.get_all(Params().where(Encounter, 'enc_type', '=', EncType.ANC.value )))
        to_deliver = int(min(delivery_count, len(anc_encounters)//2)) #assume only half of anc has delivered
        remaining = delivery_count - to_deliver
        print(f"ANC Encounter: {anc_count} Child Health Encounter: {child_health} Delivery: {to_deliver} Remaining: {remaining}")

        for i in tqdm(range(to_deliver), desc="Delivering pregnant womens"):
            facility = random.choice(facilities)
            selected_scheme = random.choice(facility.scheme)
            date = fake.date_between(start_date=start_date, end_date = end_date)
            policy_number =  ''.join(random.choices('0123456789', k=10))
            age = random.randint(15, 60)
            treatment = fake.sentence(nb_words=10)
            outcome = random.choice(outcome_ids)
            doctor_name = random.choice(["Dr. Owolabi", "Dr. Musa", "Dr. Adeola"])
            created_by = facility_user_link[facility.id]
            address = fake.address()
            client_name = fake.name()
            phone_number = "08020007040"
            age_group = random.choice(list(AgeGroup))
            treatment_cost = random.randint(500, 10000)
            investigation = fake.sentence(nb_words=4)
            investigation_cost  = random.randint(500, 10000)
            medication = fake.sentence(nb_words=4)
            mode_of_entry = random.choice(list(ModeOfEntry))
            medication_cost = random.randint(500, 10000)
            referral_reason = fake.sentence(nb_words = 3) if outcome.name == 'Referred' else None
            hospital_number = fake.sentence(nb_words = 1)
            address = fake.sentence(nb_words = 4)

            nin  = ''.join(random.choices('0123456789', k=11))

            baby_details = [{'gender': random.choice(['M', 'F']), 'outcome': random.choice(list(BabyOutcome)).value}
                            for i in range(random.randint(1, 4))]

            current = anc_encounters[i]
            EncounterServices.create_delivery_encounter(
                facility_id= facility.id,
                date = date,
                client_name = client_name,
                policy_number = policy_number,
                gender = 'F',
                age = age,
                treatment= treatment,
                treatment_cost = treatment_cost,
                medication = medication,
                medication_cost = medication_cost,
                referral_reason = referral_reason,
                investigation= investigation,
                investigation_cost = investigation_cost,
                age_group = age_group.value,
                mode_of_entry= mode_of_entry.value,
                doctor_name= doctor_name,
                hospital_number = hospital_number,
                address = address,
                scheme = selected_scheme.id,
                nin = nin,
                phone_number = phone_number,
                created_by= facility_user_link[facility.id],
                anc_id = current.anc.id,
                anc_count = current.anc.anc_count,
                mode_of_delivery= random.choice(list(DeliveryMode)),
                mother_outcome= outcome.id,
                baby_details = baby_details,
                commit = False
            )

        for i in tqdm(range(remaining), desc="Creating Remaining Encounter"):
            facility = random.choice(facilities)
            selected_scheme = random.choice(facility.scheme)
            date = fake.date_between(start_date=start_date, end_date = end_date)
            policy_number = f"{random.choice(policy_start)}/00{random.randint(1000, 99999)}/{random.randint(23, 26)}/C/{random.randint(0, 6)}"
            age = random.randint(0, 90)
            treatment = fake.sentence(nb_words=4)
            outcome = random.choice(outcome_ids)
            doctor_name = random.choice(["Dr. Owolabi", "Dr. Musa", "Dr. Adeola"])
            created_by = facility_user_link[facility.id]
            address = fake.address()
            client_name = fake.name()
            gender = random.choice(["M", "F"])
            phone_number = "08020007040"
            age_group = random.choice(list(AgeGroup))
            treatment_cost = random.randint(500, 10000)
            investigation = fake.sentence(nb_words=4)
            investigation_cost  = random.randint(500, 10000)
            medication = fake.sentence(nb_words=4)
            mode_of_entry = random.choice(list(ModeOfEntry))
            medication_cost = random.randint(500, 10000)
            nin  = ''.join(random.choices('0123456789', k=11))
            address = fake.sentence(nb_words = 7)
            hospital_number = fake.sentence(nb_words = 4)
            referral_reason = fake.sentence(nb_words = 7) if outcome.name == 'Referred' else None

            EncounterServices.create_encounter(
                        facility_id=facility.id,
                        date = date,
                        policy_number=policy_number,
                        client_name= client_name,
                        gender= gender,
                        nin = nin,
                        phone_number= phone_number,
                        outcome= outcome.id,
                        age= age,
                        hospital_number= hospital_number,
                        address = address,
                        treatment = treatment,
                        treatment_cost = treatment_cost,
                        medication = medication,
                        medication_cost = medication_cost,
                        referral_reason = referral_reason,
                        investigation= investigation,
                        investigation_cost = investigation_cost,
                        age_group = age_group.value,
                        mode_of_entry= mode_of_entry.value,
                        doctor_name= doctor_name,
                        scheme = selected_scheme.id,
                        created_by = facility_user_link[facility.id],
                        services_id= random.choices(service_ids, k= random.randint(0,3)),
                        diseases_id= random.choices(diseases_ids, k = random.randint(1, 3)),
                        commit=False
                        )
    print(f"Successfully Seeded {num} Encounter")
