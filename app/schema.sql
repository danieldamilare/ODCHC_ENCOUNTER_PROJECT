CREATE TABLE facility (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        local_government VARCHAR(100) NOT NULL COLLATE NOCASE,
        facility_type VARCHAR(10) NOT NULL,
        ownership VARCHAR(10) COLLATE NOCASE CHECK (ownership IN ('public', 'private'))
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
        address TEXT NOT NULL,
        scheme INTEGER NOT NULL,
        nin TEXT NOT NULL CHECK(LENGTH(nin) = 11),
        phone_number TEXT NOT NULL,
        hospital_number TEXT NOT NULL,
        referral_reason TEXT,
        age_group VARCHAR(20) NOT NULL,
        mode_of_entry VARCHAR(25) NOT NULL,
        treatment TEXT,
        treatment_cost INTEGER NOT NULL DEFAULT 0,
        medication TEXT,
        medication_cost INTEGER NOT NULL DEFAULT 0,
        investigation TEXT,
        investigation_cost INTEGER NOT NULL DEFAULT 0,
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
    PRIMARY KEY(facility_id, scheme_id),
    FOREIGN KEY(facility_id) REFERENCES facility(id) ON DELETE CASCADE,
    FOREIGN KEY(scheme_id) REFERENCES insurance_scheme(id) ON DELETE CASCADE
);

CREATE TABLE treatment_outcome(
    id INTEGER PRIMARY KEY  AUTOINCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL,
    type VARCHAR(255) NOT NULL COLLATE NOCASE
);


CREATE TABLE anc_registry(
    id INTEGER PRIMARY KEY,
    orin CHAR(10) NOT NULL CHECK(LENGTH(orin) = 10),
    age INTEGER NOT NULL CHECK(age >= 15 AND age <= 60),
    age_group TEXT NOT NULL,
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

CREATE VIRTUAL TABLE encounters_fts USING fts5(
    policy_number,
    client_name,
    nin,
    phone_number,
    address,
    treatment,
    medication,
    investigation,
    doctor_name,
    mode_of_entry,
    content='encounters',
    content_rowid='id'
);

CREATE VIEW view_utilization_items AS
SELECT
    ecd.encounter_id,
    ecd.disease_id as item_id,
    dis.name as item_name,
    "Disease" as item_type
FROM encounters_diseases as ecd
JOIN diseases as dis on dis.id = ecd.disease_id
UNION ALL
SELECT
    ecs.encounter_id,
    ecs.service_id as item_id,
    srv.name as item_name,
    "Service" as item_type
FROM encounters_services as ecs
JOIN services as srv on srv.id = ecs.service_id;

CREATE VIEW master_encounter_view AS
WITH
    BabyStats AS (
        SELECT
            encounter_id,
            COUNT(*) as total_babies,
            SUM(CASE WHEN outcome = 'Live Birth' THEN 1 ELSE 0 END) as live_births,
            SUM(CASE WHEN outcome = 'Still Birth' THEN 1 ELSE 0 END) as still_births
        FROM delivery_babies
        GROUP BY encounter_id
    ),

    DiseaseAgg AS (
        SELECT
            ed.encounter_id,
            GROUP_CONCAT(d.name, ', ') as disease_list
        FROM encounters_diseases ed
        JOIN diseases d ON d.id = ed.disease_id
        GROUP BY ed.encounter_id
    ),

    ServiceAgg AS (
        SELECT
            es.encounter_id,
            GROUP_CONCAT(s.name, ', ') as service_list
        FROM encounters_services es
        JOIN services s ON s.id = es.service_id
        GROUP BY es.encounter_id
    )

SELECT
    "Ondo State" as State,
    fc.id as "Facility ID",
    fc.ownership as Ownership,
    fc.local_government as "Local Government",
    fc.name as "Facility Name",
    e.date as "Date of Encounter",
    e.policy_number as "Policy Number",
    e.client_name as "Client Name",
    e.gender as Gender,
    e.age as Age,
    e.enc_type as "Encounter Type",
    isc.scheme_name as "Scheme",
    e.phone_number as "Phone Number",

    COALESCE(e.treatment_cost, 0)/100.0 as "Treatment Cost",
    COALESCE(e.medication_cost, 0)/100.0 as "Medication Cost",
    COALESCE(e.investigation_cost, 0)/100.0 as "Investigation Cost",

    da.disease_list as "Diseases",
    sa.service_list as "Services",
    e.treatment as "Treatment",
    tc.name as "Outcome",

    ar.lmp as "LMP",
    ar.expected_delivery_date as "EDD",
    ar.parity as "Parity",
    COALESCE(ae.anc_count, de.anc_count) as "ANC Count",

    de.mode_of_delivery as "Mode of Delivery",
    COALESCE(bs.total_babies, 0) as "Number of Babies",
    COALESCE(bs.live_births, 0) as "Live Births",
    COALESCE(bs.still_births, 0) as "Still Births",

    ch.guardian_name as "Guardian Name"

FROM encounters e
JOIN facility fc ON fc.id = e.facility_id
JOIN insurance_scheme isc ON isc.id = e.scheme
JOIN treatment_outcome tc ON tc.id = e.outcome

LEFT JOIN BabyStats bs ON bs.encounter_id = e.id
LEFT JOIN DiseaseAgg da ON da.encounter_id = e.id
LEFT JOIN ServiceAgg sa ON sa.encounter_id = e.id

LEFT JOIN anc_encounters ae ON ae.encounter_id = e.id
LEFT JOIN delivery_encounters de ON de.encounter_id = e.id
LEFT JOIN child_health_encounters ch ON ch.encounter_id = e.id
LEFT JOIN anc_registry ar ON ar.id = COALESCE(ae.anc_id, de.anc_id);


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
CREATE INDEX idx_diseases_category_id ON diseases (category_id);
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
CREATE INDEX idx_encounters_nin ON encounters(nin);
CREATE INDEX idx_encounters_phone_number ON encounters(phone_number);
CREATE INDEX idx_encounters_client_name ON encounters(LOWER(client_name));
