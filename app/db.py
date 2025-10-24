import sqlite3
from datetime import datetime

from app import app
from flask import  g
import click

def get_db():
    if 'db' not in g:
       g.db = sqlite3.connect(
                app.config['DATABASE'],
                 detect_types=sqlite3.PARSE_DECLTYPES
                )
    g.db.row_factory = sqlite3.Row
    g.db.execute('PRAGMA foreign_keys = ON')
    return g.db

@app.teardown_appcontext
def close_db(exception=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()

    with app.open_resource('schema.sql') as f:
        db.executescript(f.read().decode('utf8'))


@click.command('seed-db')
def seed_db():
    from app.services import FacilityServices, UserServices, DiseaseServices, DiseaseCategoryServices
    import random
    category_list = ['SURGICAL', 'OBSTERIC', 'NON-COMMUNICABLE DISEASE', 'COMMUNICABLE DISEASE', 'PSYCHIATRY', 
                        'GYNAECOLOGICAL', 'PEADIATRICS']
    disease_list = ["ABSCESS","ANEMIA","ANEMIA PREGNANCY","ANXIETY","A. P. H.","APPENDICITIS","ARTRITIS /RHEUMATISM","ASSITED DEV.","ASTHMA","BITE (HUMAN)",
    "BITE (SCORPION)","BITE (SNAKE","BITE(DOG)","BOIL","BREACH DEL","BRONCHITIS","C. C.  F.","C.V.D.","C/S","CELLULITIS.","CERE.VAS ACC",
    "CEREBRAL PALSY","CHICKEN POX","CHOLECYSTITIS","CHOLERA","CONJUCTIVITIS","DERMATITIS & OTHER SKIN DIS. ","DIAB. MELLIT (DM)","DIARRHOEA",
    "DISLOCATION","DRUG REACTION ","DYSENTARY","DYSPLEGIA","ECLAMSIA","ECTOPIC PREG.","ENDOCROUTE/ NUTRITIONAL DIS. ","EPILESY ","FIBROID",
    "FILARIASIS","FRACTURE","GASTRO ENT.","GINGIVITIS","GUN SHOT","HAEMATOMA","HAEMORRHAGE","HEMORRHOID","HEPATITIS","HERNIA","HIV/AID",
    "HYDROCEPHALUS","HYPERTENSION (HTN)","HYPOGLYCAEMIA","HYPOTENSION","INJURY ","INTESTNAL OBS","JAUNDICE","KERATITIS","LIPOMA","CYESIS",
    "MALARIA ","MALARIA IN PREGNANCY","MASTITIS","MEASLES","MENINGITIS"," MENTAL DISORDERS ","MIGRAINE","MUMPS","NEPHRITIS & OTHER KIDNEY  DIS.",
    "NORMAL DEL.","OSTEOMYELITIS","OTITIS MED","OVARIAN  CYST","P . P. H ","PERITONITIS","PID","PLACENTAL PREVIA","PNEUMOMIA","POISON",
    "PREG. INDUCE HYPERTENSION","R.T.A & OTHER ACC","REP. TRAC. INFEC. (RTI)","RUPTURED UTERINE","SEPSIS","SICKLE CELL ANAEMIA","STD",
    "STILL BIRTH","STOMATITIS","TETANUS","THREATEN ABORTION","TINEA CAPITI./CORPORIS","TONGUE TIE","TONSILITIS","TRAUMA","TUBERCULOSIS",
    "TYPHOID FEVER","ULCER","HYSTECTOCMY","UTI","VESICO-VAGINAL-FISTULA"] 
    db =  get_db()
    for x in category_list:
        DiseaseCategoryServices.create_category(x, commit=False)
    click.echo(f"Added {len(category_list)} disease categories to the database")
    for x in disease_list:
        #random disease cateogry for testing
        DiseaseServices.create_disease(x, random.randint(1, len(category_list)), commit = False)
    click.echo(f"Added {len(disease_list)} diseases to the database")
    db.commit()

    import app.seed
    from app.models import Role
    u = UserServices.create_user('odchc', None, 'password', role = Role.admin)
    click.echo(f"Created user username: {u.username}, facility_id: {u.facility_id}, role: {u.role.name}")
    db.commit()

    click.echo("Seeded the database")

@click.command('init-db')
def init_db_command():
    """Clear the existing data and create new tables."""
    init_db()
    click.echo('Initialized the database.')


sqlite3.register_converter(
    "timestamp", lambda v: datetime.fromisoformat(v.decode())
)
