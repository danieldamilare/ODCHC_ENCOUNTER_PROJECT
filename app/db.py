import sqlite3
from datetime import datetime
from app.models import Role
from app import app
from flask import g
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
    from app.seed import (
        seed_diseases,
        seed_services,
        seed_insurance_scheme,
        seed_treatment_outcome,
        seed_facilities,
        seed_users,
        seed_encounter
    )
    db = get_db()
    print("Seeding Diseases...")
    seed_diseases()
    print("Seeding Services...")
    seed_services()
    print("Seeding Insurance Scheme...")
    seed_insurance_scheme()
    print("Seeding Treatment Outcome...")
    seed_treatment_outcome()
    print("Seeding Facilities...")
    seed_facilities()
    print("Seeding Users...")
    seed_users()
    encounter_number = 20000
    end_date = datetime.now()
    start_date = end_date.replace(year =2021, month=7, day=1)
    print(f"Seeding Encounter with {encounter_number} number starting from {start_date} to {end_date}...")
    seed_encounter(encounter_number, start_date, end_date)
    db.commit()
    print(" Database populated successfully.")

@click.command('init-db')
def init_db_command():
    """Clear the existing data and create new tables."""
    init_db()
    click.echo('Initialized the database.')


sqlite3.register_converter(
    "timestamp", lambda v: datetime.fromisoformat(v.decode())
)
