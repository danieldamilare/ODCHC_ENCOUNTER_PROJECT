import sqlite3
import random
from datetime import datetime, timedelta
from faker import Faker
from app.db import get_db
from werkzeug.security import generate_password_hash

fake = Faker()

db = get_db()
cur = db.cursor()

# --- Basic inserts ---

facilities = [
"COLLEGE OF HEALTH TECHNOLOGY CLINIC, ODA ROAD, AKURE",
"COMPLETE CARE CLINIC AGADAGBA-OBON",
"COMPREHENSIVE HEALTH CENTRE OWASE OKA",
"COMPREHENSIVE HEALTH CENTRE ZION PEPE",
"ETON CLINIC OBAILE AKURE",
"FIRST MERCY SPECIALIST HOSPITAL AKURE",
"GENERAL HOSPITAL ARAROMI-OBU",
"GENERAL HOSPITAL BOLORUNDURO",
"GENERAL HOSPITAL IDO-ANI",
"GENERAL HOSPITAL IFON",
"GENERAL HOSPITAL IGBARA-OKE", 
"GENERAL HOSPITAL IGBEKEBO", 
"GENERAL HOSPITAL IGBOKODA",
"GENERAL HOSPITAL IJU ITAOGBOLU",
"GENERAL HOSPITAL IPE-AKOKO",
"GENERAL HOSPITAL IRUN AKOKO",
"GENERAL HOSPITAL IWARO OKA",
"GENERAL HOSPITAL ODE-IRELE",
"GENERAL HOSPITAL ORE",
"GENERAL HOSPITAL OWO",
"GENERAL HOSPITAL, IDANRE",
"GENERAL HOSPITAL, IGBOTAKO",
"GENERAL HOSPITAL, ILE-OLUJI",
"HOPE HOSPITAL & MATERNAL, PALACE ROAD IDANRE",
"J&E FATULA HOSPITAL OKEARO AKURE",
"MAO HOSPITAL LIMITED, IROWO, AKURE",
"MERCYLAND MEDICAL CENTER, ONDO",
"MINISTRY OF HEALTH HOSPITAL, IGBATORO ROAD AKURE",
"NETCARE MULTISPECIALIST HOSPITAL IJAPO",
"NEWDAY MEDICAL CENTER, IJAPO, AKURE",
"NIGERIAN POLICE MEDICAL SERVICES, AKURE",
"PIMA HOSPITAL ORE",
"PRIMARY HEALTH CENTRE AJAGBA",
"PRIMARY HEALTH CENTRE ARIGIDI",
"PRIMARY HEALTH CENTRE AROGBO",
"PRIMARY HEALTH CENTRE EMURE ILE",
"PRIMARY HEALTH CENTRE IJARE",
"PRIMARY HEALTH CENTRE ILARA II",
"PRIMARY HEALTH CENTRE IPELE",
"PRIMARY HEALTH CENTRE ISHAKUNMI",
"PRIMARY HEALTH CENTRE ISUA OKE",
"PRIMARY HEALTH CENTRE MAROKO",
"PRIMARY HEALTH CENTRE ODE AYE",
"PRIMARY HEALTH CENTRE OKEIGBO",
"PRIMARY HEALTH CENTRE OWENA ,",
"PRIMARY HEALTH CENTRE OWENA BRIDGE",
"PRIMARY HEALTH CENTRE PANAPANA ORE",
"PRIMARY HEALTH CENTRE, ARAKALE",
"PRIMARY HEALTH CLINIC DANJUMA AKURE",
"PRIMARY HEALTH CLINIC IGOBA",
"PRIMARY HEALTH CLINIC OKELUSE COTTAGE",
"RUFUS GIWA MEMORIAL HOSPITAL OKE AGBE",
"SCKYE HOSPITAL, AKURE",
"SIMLON MEDICAL CENTER, LAFE WAY, AKURE",
"STATE SPECIALIST HOSPITAL IKARE-AKOKO",
"STATE SPECIALIST HOSPITAL, OKE-ARO, AKURE",
"STATE SPECIALIST HOSPITAL, OKITIPUPA",
"ST MICHAEL HOSPITAL DANJUMA AKURE",
"UNIMEDTHC, AKURE",
"UNIMEDTHC, ONDO",
"SSH ONDO",
"GOODIES HOSPITAL ONDO,",
"OMOLOLU HOSPITAL",
"KINGS MEDIC HOSPITAL",
"DEBORAH HOSPITAL",
]

random_facilities = []


LOCAL_GOVERNMENT = list(set([
    "Akoko North-East".lower(),
    "Akoko North-West".lower(),
    "Akoko South-East".lower(),
    "Akoko South-West".lower(),
    "Akure North".lower(),
    "Akure South".lower(),
    "Emure-Ile".lower(),
    "Idanre".lower(),
    "Ifedore".lower(),
    "Igbara-oke".lower(),
    "Ilaje".lower(),
    "Ese-Odo"
    "Ile Oluji".lower(),
    "Irele".lower(),
    "Isua Akoko".lower(),
    "Odigbo".lower(),
    "Oka Akoko".lower(),
    "Okitipupa".lower(),
    "Ondo East".lower(),
    "Ondo West".lower(),
    "Ose".lower(),
    "Owo".lower()]))

for f in facilities:
    random_facilities.append((f, random.choice(LOCAL_GOVERNMENT), 
                              random.choice(['primary', 'tertiary', 'secondary']),
                              ))
 
print("Inserting into facility")
cur.executemany("INSERT INTO facility (name, local_government, facility_type) VALUES (?, ?, ?)", random_facilities)

users = [
    ("admin", generate_password_hash("password"), None, "admin"),
]

for i in range(1, len(facilities) + 1):
    users.append((f'user{i}', generate_password_hash(f'password{i}'), i,  'user'))


print("Inserting into users")

cur.executemany("INSERT INTO users (username, password_hash, facility_id, role) VALUES (?, ?, ?, ?)", users)

facility_ids = [r['id'] for r in cur.execute('SELECT id FROM facility')]


print("Inserting into insurance_scheme")
cur.executemany("INSERT INTO insurance_scheme (scheme_name, color_scheme) VALUES (?, ?)",
                [('BHCPF', '#448264'), ('ORANGHIS', '#fc9d03')])

print("Inserting into treatment outcome")
outcomes = [('Admitted/In Patient', 'General'), ('Out Patient', 'General'), ('Referral', 'General'),
 ('Neonatal Death (0 - 28 days)', 'Death'), ('Infant Death', 'Death'), 
 ('Under 5 deaths (1 - 5 years)', 'Death'), ('Maternal Death (Pregnant women)', 'Death'),
 ('Other Death', 'Death')]

cur.executemany('INSERT INTO treatment_outcome (name, type) VALUES (?, ?)', 
                outcomes)

scheme_ids = [1, 2]
scheme_list = set()
for fid in facility_ids:
    num = random.randint(1, 2)
    for i in range(num):
        scheme_list.add((fid, random.choice(scheme_ids)))

print("inserting into facility_scheme")
cur.executemany('INSERT INTO facility_scheme(facility_id, scheme_id) VALUES(?, ?)',
                list(scheme_list))

# --- Encounters generation ---
disease_ids = [r['id'] for r in cur.execute("SELECT id FROM diseases").fetchall()]
user_ids = [r['id'] for r in cur.execute('SELECT id FROM users')]
outcome_ids = [r['id'] for r in cur.execute('SELECT id from treatment_outcome')]

encounters_data = []

num_encounters = 5000
policy_start = ['AKS', 'AKN', 'OWO', 'IFE', 'KTP', 'ONW', 'ODG', 'ESE', 'ANW', 'ANE', 'IRL',
                'ILJ', 'ASE', 'IDR', 'ONE', 'ASW']

print("Generating encounter datas...")
for _ in range(num_encounters):
    facility_id = random.choice(facility_ids)
    start_date = datetime(month=7, year=2025, day = 1)
    date = fake.date_between(start_date=start_date, end_date="today")
    policy_number = f"{random.choice(policy_start)}/00{random.randint(1000, 99999)}/{random.randint(23, 26)}/C/{random.randint(0, 6)}"
    client_name = fake.name()
    gender = random.choice(["M", "F"])
    age = random.randint(0, 90)
    treatment = fake.sentence(nb_words=10)
    scheme = random.choice(scheme_ids)
    outcome = random.choice(outcome_ids)
    doctor_name = random.choice(["Dr. Owolabi", "Dr. Musa", "Dr. Adeola"])
    created_by = random.choice(user_ids)
    created_at = date

    encounters_data.append((
        facility_id, date, policy_number, client_name, gender,
        age, treatment, scheme, outcome, doctor_name, created_by, created_at
    ))


print("Inserting into encounters")
cur.executemany("""
INSERT INTO encounters (
    facility_id, date, policy_number, client_name, gender, age, 
    treatment, scheme, outcome, doctor_name, created_by, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", encounters_data)

# --- Encounter Diseases (link table) ---
encounter_ids = [r[0] for r in cur.execute("SELECT id FROM encounters").fetchall()]
encounter_diseases_data = set()

print("Generating encounter diseases")
for eid in encounter_ids:
    for _ in range(random.randint(1, 3)):
        disease_id = random.choice(disease_ids)
        encounter_diseases_data.add((eid, disease_id))

print("Inserting into encounter diseases")
cur.executemany("""
INSERT INTO encounters_diseases (encounter_id, disease_id)
VALUES (?, ?)
""", list(encounter_diseases_data))

db.commit()

print("✅ Database populated successfully.")
