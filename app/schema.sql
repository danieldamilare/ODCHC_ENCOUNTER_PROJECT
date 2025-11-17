CREATE TABLE facility (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        local_government VARCHAR(100) NOT NULL COLLATE NOCASE,
        facility_type VARCHAR(10) NOT NULL
    );

CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        username VARCHAR(64) UNIQUE NOT NULL COLLATE NOCASE,
        password_hash VARCHAR(255) NOT NULL,
        facility_id INTEGER,
        role VARCHAR(20) NOT NULL DEFAULT 'user' COLLATE NOCASE,
        FOREIGN KEY (facility_id) REFERENCES facility (id)
    );

CREATE TABLE diseases_category (
        id INTEGER PRIMARY KEY,
        category_name VARCHAR(50) UNIQUE NOT NULL COLLATE NOCASE
    );

CREATE TABLE diseases (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL COLLATE NOCASE,
        category_id INTEGER NOT NULL,
        FOREIGN KEY (category_id) REFERENCES diseases_category (id)
    );

CREATE TABLE encounters (
        id INTEGER PRIMARY KEY,
        facility_id INTEGER NOT NULL,
        date DATE NOT NULL,
        policy_number VARCHAR(40) NOT NULL,
        client_name TEXT NOT NULL,
        gender CHAR(1) NOT NULL CHECK (gender IN ('M', 'F')),
        age INTEGER NOT NULL CHECK (age >= 0 AND age <= 120),
        enc_type TEXT NOT NULL COLLATE NOCASE CHECK (enc_type IN ('general', 'anc', 'delivery', 'child_health')),
        scheme INTEGER NOT NULL,
        nin TEXT NOT NULL CHECK(LENGTH(nin) = 11),
        phone_number TEXT NOT NULL,
        age_group VARCHAR(10) GENERATED ALWAYS AS (
            CASE
                WHEN age < 1 THEN '<1'
                WHEN age <= 5 THEN '1-5'
                WHEN age <= 14 THEN '6-14'
                WHEN age <= 19 THEN '15-19'
                WHEN age <= 44 THEN '20-44'
                WHEN age <= 64 THEN '45-64'
                ELSE '65&AB'
            END
        ) STORED,
        treatment TEXT,
        doctor_name VARCHAR(255) NOT NULL,
        outcome INTEGER NOT NULL,
        created_by INTEGER NOT NULL,
        created_at DATE NOT NULL,
        FOREIGN KEY (facility_id) REFERENCES facility (id) ON DELETE RESTRICT,
        FOREIGN KEY (created_by) REFERENCES users (id) ON DELETE SET NULL,
        FOREIGN KEY (outcome) REFERENCES treatment_outcome(id),
        FOREIGN KEY (scheme) REFERENCES insurance_scheme(id));

CREATE TABLE encounters_services(
    encounter_id INTEGER NOT NULL,
    service_id INTEGER NOT NULL,
    FOREIGN KEY (encounter_id) REFERENCES encounters (id) ON DELETE CASCADE,
    FOREIGN KEY (service_id) REFERENCES services (id) ON DELETE RESTRICT,
    PRIMARY key (encounter_id, service_id)
);

CREATE TABLE services(
    id INTEGER  PRIMARY KEY,
    name TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    FOREIGN KEY (category_id) REFERENCES service_category(id)
);

CREATE TABLE service_category (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- To be updated once the whole application is working
CREATE TABLE encounters_diseases(
    encounter_id INTEGER NOT NULL,
    disease_id INTEGER NOT NULL,
    FOREIGN KEY (encounter_id) REFERENCES encounters (id) ON DELETE CASCADE,
    FOREIGN KEY (disease_id) REFERENCES diseases (id) ON DELETE RESTRICT,
    PRIMARY KEY(encounter_id, disease_id)
);

CREATE TABLE insurance_scheme(
    id INTEGER PRIMARY KEY,
    scheme_name TEXT UNIQUE NOT NULL,
    color_scheme TEXT DEFAULT "#33a9f2"
);

CREATE TABLE facility_scheme(
    facility_id INTEGER NOT NULL,
    scheme_id INTEGER NOT NULL,
    PRIMARY KEY(facility_id, scheme_id)
);

CREATE TABLE treatment_outcome(
    id INTEGER PRIMARY KEY  AUTOINCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL,
    type VARCHAR(255) NOT NULL COLLATE NOCASE
);


CREATE TABLE anc_registry(
    id INTEGER PRIMARY KEY,
    orin CHAR(10) NOT NULL CHECK(LENGTH(orin) = 10),
    kia_date date NOT NULL,
    client_name TEXT NOT NULL COLLATE NOCASE,
    booking_date date NOT NULL,
    parity INTEGER NOT NULL,
    place_of_issue TEXT NOT NULL,
    hospital_number TEXT NOT NULL,
    nin TEXT NOT NULL CHECK(LENGTH(nin) = 11),
    phone_number TEXT NOT NULL,
    address TEXT NOT NULL,
    lmp date NOT NULL,
    expected_delivery_date DATE NOT NULL,
    anc_count INTEGER NOT NULL,
    status TEXT NOT NULL COLLATE NOCASE  CHECK(status IN ('active' , 'inactive')) --set to inactive after delivery
);

CREATE TABLE anc_encounters(
    encounter_id INTEGER PRIMARY KEY,
    anc_id INTEGER NOT NULL,
    anc_count INTEGER NOT NULL,
    FOREIGN KEY(encounter_id) REFERENCES encounters(id) ON DELETE RESTRICT,
    FOREIGN KEY(anc_id) REFERENCES anc_registry(id) ON DELETE RESTRICT
);


CREATE TABLE  child_health_encounters(
    id INTEGER PRIMARY KEY,
    encounter_id INTEGER UNIQUE NOT NULL,
    orin CHAR(10) NOT NULL CHECK(LENGTH(orin) = 10),
    dob date NOT NULL,
    address TEXT NOT NULL,
    guardian_name TEXT NOT NULL,
    FOREIGN key(encounter_id) REFERENCES encounters(id) ON DELETE RESTRICT
);

CREATE TABLE delivery_encounters(
    id INTEGER PRIMARY KEY,
    anc_id INTEGER NOT NULL,
    encounter_id INTEGER NOT NULL,
    anc_count INTEGER NOT NULL,
    mode_of_delivery TEXT NOT NULL COLLATE NOCASE,
    FOREIGN KEY(encounter_id) REFERENCES encounters(id) ON DELETE RESTRICT,
    FOREIGN KEY(anc_id) REFERENCES anc_registry(id) ON DELETE RESTRICT
);

CREATE TABLE delivery_babies(
    id INTEGER PRIMARY KEY,
    encounter_id INTEGER NOT NULL,
    gender CHAR(1) NOT NULL CHECK (gender IN ('M', 'F')),
    outcome VARCHAR(50) NOT NULL,
    FOREIGN KEY(encounter_id) REFERENCES encounters(id) ON DELETE RESTRICT
);

CREATE INDEX idx_anc_status ON anc_registry(status);
CREATE INDEX idx_anc_orin ON anc_registry(orin);
CREATE iNDEX idx_child_health_encounter_id ON child_health_encounters(encounter_id);
CREATE iNDEX idx_child_health_orin ON child_health_encounters(orin);
CREATE INDEX idx_delivery_encounter_encounter_id ON delivery_encounters(encounter_id);
CREATE INDEX idx_delivery_babies_encounter ON delivery_babies(encounter_id);
CREATE INDEX idx_delivery_babies_gender ON delivery_babies(gender);
CREATE INDEX idx_delivery_babies_outcome ON delivery_babies(outcome);
CREATE INDEX idx_encounters_created_at ON encounters (created_at);
CREATE INDEX idx_facility_scheme_id ON facility_scheme (scheme_id);
CREATE INDEX idx_facility_scheme_facility_id ON facility_scheme (facility_id);
CREATE INDEX idx_encounters_facility_date ON encounters (facility_id, date);
CREATE INDEX idx_user_facility_id ON users (facility_id);
CREATE INDEX idx_diseases_category_id ON diseases (category_id); CREATE INDEX idx_encounters_scheme ON encounters (scheme);
CREATE INDEX idx_encounters_outcome ON encounters (outcome);
CREATE INDEX idx_encounters_date_gender ON encounters (date, gender);
CREATE INDEX idx_encounters_diseases_encounter ON encounters_diseases (encounter_id);
CREATE INDEX idx_encounters_diseases_disease ON encounters_diseases (disease_id);
CREATE INDEX idx_facility_local_government ON facility (local_government);
CREATE INDEX idx_treatment_outcome_type ON treatment_outcome (type);
CREATE INDEX idx_encounters_date_scheme_facility ON encounters (date, scheme, facility_id);
CREATE INDEX idx_encounters_date_outcome ON encounters (date, outcome);
CREATE INDEX idx_encounters_policy_number ON encounters (policy_number);
CREATE INDEX idx_encounters_services_encounter ON encounters_services(encounter_id);
CREATE INDEX idx_encounters_services_service ON encounters_services(service_id);
CREATE INDEX idx_services_category_id ON services(category_id);
CREATE INDEX idx_encounters_age_group ON encounters(age_group);
CREATE INDEX idx_anc_encounters_anc_id ON anc_encounters(anc_id);
CREATE INDEX idx_delivery_encounters_anc_id ON delivery_encounters(anc_id);
CREATE INDEX idx_encounters_scheme_date ON encounters(scheme, date);
CREATE INDEX idx_encounters_gender_facility ON encounters(gender, facility_id, date);
CREATE INDEX idx_encounters_date_age_group ON encounters(date, age_group);
CREATE INDEX idx_encounters_outcome_type_date ON encounters(outcome, date);
