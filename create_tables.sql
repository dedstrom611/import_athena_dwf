
CREATE TABLE athenadwh_medical_history_clone (
   id INT,
   patient_id INT,
   chart_id INT,
   medical_history_key VARCHAR(1000),
   question VARCHAR(150),
   answer VARCHAR(4000),
   created_datetime TIMESTAMP,
   created_by VARCHAR(20),
   deleted_datetime TIMESTAMP,
   deleted_by VARCHAR(20),
   PRIMARY KEY (id));

 CREATE TABLE athenadwh_social_history_clone (
    id INT,
    patient_id INT,
    chart_id INT,
    social_history_key VARCHAR(1000),
    question VARCHAR(1000),
    answer VARCHAR(4000),
    created_datetime TIMESTAMP,
    created_by VARCHAR(20),
    deleted_datetime TIMESTAMP,
    deleted_by VARCHAR(20),
    PRIMARY KEY (id));

  CREATE TABLE athenadwh_document_crosswalk_clone (
     document_id INT,
     patient_id INT,
     chart_id INT);

 CREATE TABLE athenadwh_provider_clone (
    provider_id INT,
    provider_first_name VARCHAR(50),
    provider_last_name VARCHAR(50),
    provider_user_name VARCHAR(20),
    provider_type VARCHAR(10),
    provider_type_name VARCHAR(100),
    provider_type_category VARCHAR(20),
    provider_npi_number VARCHAR(50),
    provider_group_id INT,
    supervising_provider_id INT,
    taxonomy VARCHAR(250),
    specialty VARCHAR(50),
    created_datetime TIMESTAMP,
    PRIMARY KEY (provider_id));

CREATE TABLE athenadwh_clinical_result_clone (
   clinical_result_id INT,
   document_id INT,
   clinical_provider_id INT,
   specimen_source VARCHAR(200),
   clinical_order_type VARCHAR(4000),
   clinical_provider_order_type VARCHAR(200),
   clinical_order_genus VARCHAR(100),
   created_datetime TIMESTAMP,
   created_by VARCHAR(20),
   observation_datetime TIMESTAMP,
   specimen_received_datetime TIMESTAMP,
   results_reported_datetime TIMESTAMP,
   PRIMARY KEY (clinical_result_id));

CREATE TABLE athenadwh_clinical_encounters_clone_full (
  clinical_encounter_id INT,
  patient_id INT,
  chart_id INT,
  appointment_id INT,
  provider_id SMALLINT,
  encounter_date DATE,
  encounter_status VARCHAR(20),
  created_datetime TIMESTAMP,
  closed_datetime TIMESTAMP,
  closed_by VARCHAR(20),
  PRIMARY KEY (clinical_encounter_id));

CREATE INDEX index_clinical_encounter_clone_full
  ON athenadwh_clinical_encounters_clone_full (clinical_encounter_id, patient_id, chart_id);

GRANT SELECT, INSERT, UPDATE, DELETE
  ON looker_scratch.athenadwh_clinical_encounters_clone_full
  TO looker;

  # A Table that holds the transaction data temporarily until it can be
  # upserted into the final table
  CREATE TABLE IF NOT EXISTS looker_scratch.clinical_encounters_transactions (
    clinical_encounter_id INT,
    patient_id INT,
    chart_id INT,
    appointment_id INT,
    provider_id SMALLINT,
    encounter_date DATE,
    encounter_status VARCHAR(20),
    created_datetime TIMESTAMP,
    closed_datetime TIMESTAMP,
    closed_by VARCHAR(20));

CREATE INDEX index_athenadwh_provider_clone
 ON athenadwh_provider_clone (provider_id, supervising_provider_id);

 GRANT SELECT, INSERT, UPDATE, DELETE
 ON looker_scratch.athenadwh_provider_clone
 TO looker;

CREATE INDEX index_document_crosswalk_clone
 ON athenadwh_document_crosswalk_clone (document_id, patient_id, chart_id);

 GRANT SELECT, INSERT, UPDATE, DELETE
 ON looker_scratch.athenadwh_document_crosswalk_clone
 TO looker;

 CREATE TABLE athenadwh_clinical_result_clone (
    clinical_result_id INT,
    document_id INT,
    clinical_provider_id INT,
    specimen_source VARCHAR(200),
    clinical_order_type VARCHAR(4000),
    clinical_provider_order_type VARCHAR(200),
    clinical_order_genus VARCHAR(100),
    created_datetime TIMESTAMP,
    created_by VARCHAR(20),
    observation_datetime TIMESTAMP,
    specimen_received_datetime TIMESTAMP,
    results_reported_datetime TIMESTAMP,
    PRIMARY KEY (clinical_result_id));



CREATE INDEX index_athenadwh_clinical_result_clone
 ON athenadwh_clinical_result_clone (clinical_result_id, document_id, clinical_provider_id);

 GRANT SELECT, INSERT, UPDATE, DELETE
 ON looker_scratch.athenadwh_clinical_result_clone
 TO looker;

CREATE INDEX index_athenadwh_social_history
  ON athenadwh_social_history_clone (id, patient_id, chart_id);

CREATE INDEX index_athenadwh_medical_history
  ON athenadwh_medical_history_clone (id, patient_id, chart_id);

GRANT SELECT, INSERT, UPDATE, DELETE
ON looker_scratch.athenadwh_social_history_clone
TO looker;

GRANT SELECT, INSERT, UPDATE, DELETE
ON looker_scratch.athenadwh_medical_history_clone
TO looker;

CREATE TABLE IF NOT EXISTS postal_codes (
  postalcode VARCHAR(5),
  city VARCHAR(180),
  state_description VARCHAR(20),
  state_abbreviation VARCHAR(2),
  region VARCHAR(100),
  latitude FLOAT,
  longitude FLOAT,
  PRIMARY KEY (postalcode));

CREATE INDEX index_postal_codes
  ON postal_codes (postalcode);

GRANT SELECT, INSERT, UPDATE, DELETE
  ON looker_scratch.postal_codes
  TO looker;


CREATE TABLE IF NOT EXISTS athenadwh_medication_clone (
  medication_id INT,
  medication_name VARCHAR(100),
  fdb_med_id INT,
  med_name_id FLOAT,
  rxnorm VARCHAR(50),
  ndc VARCHAR(11),
  hic3_code VARCHAR(3),
  hic3_description VARCHAR(50),
  hic1_code VARCHAR(1),
  hic1_description VARCHAR(50),
  gcn_clinical_forumulation_id INT,
  hic2_pharmacological_class VARCHAR(2),
  hic4_ingredient_base VARCHAR(4),
  dea_schedule VARCHAR(12),
  PRIMARY KEY (medication_id));

CREATE INDEX index_athenadwh_medication_clone
  ON athenadwh_medication_clone (medication_id, fdb_med_id);

GRANT SELECT, INSERT, UPDATE, DELETE
  ON athenadwh_medication_clone
  TO looker;

CREATE TABLE IF NOT EXISTS athenadwh_patient_medication_clone (
  patient_medication_id INT,
  medication_type VARCHAR(2000),
  patient_id INT,
  chart_id INT,
  document_id INT,
  medication_id INT,
  sig VARCHAR(4000),
  medication_name VARCHAR(100),
  dosage_form VARCHAR(200),
  dosage_action VARCHAR(20),
  dosage_strength VARCHAR(100),
  dosage_strength_units VARCHAR(40),
  dosage_quantity INT,
  dosage_route VARCHAR(200),
  frequency VARCHAR(100),
  prescription_fill_quantity INT,
  number_of_refills_prescribed INT,
  fill_date DATE,
  pharmacy_name VARCHAR(100),
  med_administered_datetime TIMESTAMP,
  created_datetime TIMESTAMP,
  created_by VARCHAR(20),
  deactivation_datetime TIMESTAMP,
  deactivated_by VARCHAR(200),
  prescribed_yn VARCHAR(1),
  administered_yn VARCHAR(1),
  dispensed_yn VARCHAR(1));

CREATE INDEX index_athenadwh_patient_medication_clone
  ON athenadwh_patient_medication_clone (patient_medication_id, patient_id, chart_id, document_id);

GRANT SELECT, INSERT, UPDATE, DELETE
  ON athenadwh_patient_medication_clone
  TO looker;

CREATE TABLE IF NOT EXISTS provider_address (
  first_name VARCHAR(50),
  last_name VARCHAR(100),
  job_title_description VARCHAR(100),
  location VARCHAR(100),
  address1 VARCHAR(100),
  address2 VARCHAR(40),
  city VARCHAR(100),
  state VARCHAR(2),
  zip_code VARCHAR(10);

GRANT SELECT, INSERT, UPDATE, DELETE
  ON provider_address
  TO looker;

CREATE TABLE IF NOT EXISTS oversight_provider (
  user_name	VARCHAR(40),
  first_name VARCHAR(40),
  last_name	VARCHAR(40),
  position VARCHAR(40),
  oversight_user_name	VARCHAR(40),
  oversight_first_name VARCHAR(40),
  oversight_last_name	VARCHAR(40),
  oversight_position VARCHAR(50));

GRANT SELECT, INSERT, UPDATE, DELETE
  ON oversight_provider
  TO looker;
