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
        facility_id INTEGER NOT NULL,
        FOREIGN KEY (facility_id) REFERENCES facility (id)
    );

CREATE TABLE diseases_category (
        id INTEGER PRIMARY KEY,
        category_name UNIQUE NOT NULL COLLATE NOCASE
    );

CREATE TABLE diseases (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL COLLATE NOCASE,
        category_id INTEGER NOT NULL,
        FOREIGN KEY (category_id) REFERENCES diseases_category (id)
    );

CREATE INDEX idx_user_facility_id ON users (facility_id);

CREATE INDEX idx_diseases_category_id ON diseases (category_id);

CREATE TABLE encounters (
        id INTEGER PRIMARY KEY,
        facility_id INTEGER NOT NULL,
        disease_id INTEGER NOT NULL,
        date DATE NOT NULL,
        policy_number VARCHAR(120) NOT NULL,
        client_name TEXT NOT NULL,
        gender CHAR(1) NOT NULL CHECK (gender IN ('M', 'F')),
        age INTEGER NOT NULL CHECK (age >= 0),
        age_group VARCHAR(10) GENERATED ALWAYS AS (
            CASE
                WHEN age < 1 THEN '<1'
                WHEN age <= 5 THEN '1-5'
                WHEN age <= 14 THEN '6-14'
                WHEN age <= 19 THEN '15-19'
                WHEN age <= 44 THEN '20-44'
                WHEN age <= 64 THEN '45-64'
                ELSE '≥65'
            END
        ) STORED,
        treatment TEXT,
        referral INTEGER NOT NULL DEFAULT 0, -- should be boolean 0 and 1
        doctor_name VARCHAR(255) NOT NULL,
        professional_service TEXT,
        created_by INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (facility_id) REFERENCES facility (id) ON DELETE SET NULL,
        FOREIGN KEY (disease_id) REFERENCES diseases (id) ON DELETE SET NULL,
        FOREIGN KEY (created_by) REFERENCES users (id) ON DELETE SET NULL
    );

CREATE INDEX idx_encounters_facility_id ON encounters (facility_id);

CREATE INDEX idx_encounters_disease_id ON encounters (disease_id);

CREATE INDEX idx_encounters_date ON encounters (date);