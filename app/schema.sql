CREATE TABLE facility (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,  
        local_government VARCHAR(100) NOT NULL,
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
        scheme INTEGER NOT NULL,
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
        created_by INTEGER,
        created_at DATE NOT NULL, 
        FOREIGN KEY (facility_id) REFERENCES facility (id) ON DELETE RESTRICT,
        FOREIGN KEY (created_by) REFERENCES users (id) ON DELETE SET NULL
        FOREIGN KEY (outcome) REFERENCES treatment_outcome(id)
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
    type VARCHAR(255) NOT NULL
);

CREATE INDEX idx_encounters_facility_id ON encounters (facility_id);
CREATE INDEX idx_encounters_date ON encounters (date);
CREATE INDEX idx_encounters_created_by ON encounters (created_by);
CREATE INDEX idx_encounters_created_at ON encounters (created_at);
CREATE INDEX idx_facility_facility_id ON facility_scheme (facility_id);
CREATE INDEX idx_insurance_scheme_id ON insurance_scheme (id);
CREATE INDEX idx_facility_scheme_id ON facility_scheme (scheme_id);
CREATE INDEX idx_treatment_outcome_id ON treatment_outcome(id);
CREATE INDEX idx_encounters_facility_date ON encounters (facility_id, date);
CREATE INDEX idx_user_facility_id ON users (facility_id);
CREATE INDEX idx_diseases_category_id ON diseases (category_id);
