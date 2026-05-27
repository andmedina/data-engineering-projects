CREATE TABLE IF NOT EXISTS patients (
    patient_id VARCHAR(20) PRIMARY KEY,
    age INTEGER NOT NULL,
    gender VARCHAR(20),
    state VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS providers (
    provider_id VARCHAR(20) PRIMARY KEY,
    provider_name VARCHAR(100),
    specialty VARCHAR(100),
    state VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS claims (
    claim_id VARCHAR(20) PRIMARY KEY,
    patient_id VARCHAR(20) NOT NULL,
    provider_id VARCHAR(20) NOT NULL,
    diagnosis_code VARCHAR(20),
    procedure_code VARCHAR(20),
    claim_date DATE NOT NULL,
    claim_year INTEGER,
    claim_month INTEGER,
    claim_amount NUMERIC(10, 2) NOT NULL,
    insurance_plan VARCHAR(100),
    claim_status VARCHAR(30),

    CONSTRAINT fk_patient
        FOREIGN KEY (patient_id)
        REFERENCES patients(patient_id),

    CONSTRAINT fk_provider
        FOREIGN KEY (provider_id)
        REFERENCES providers(provider_id)
);