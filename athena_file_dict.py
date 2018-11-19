'''Store information for each Athena data import as JSON object.
For each file, include:
prefix: The root key of the JSON object and the prefix to identify the CSV file in the Athena DWH feed
columns: A dictionary of the CSV variable names as keys and their data types as values
rename: A dictionary of CSV variable as keys and PostgreSQL variables as values if the variable should be
renamed.  Note that by default, the program turns Camel case to Snake case E.G. 'Created Datetime' = 'created_datetime'.
If no variables are renamed, set this to an emtpy dictionary E.G. 'rename': dict()
postgres_table: The name of the Postgres table to hold the data.
'''
def get_dictionary():
    file_dict = {
      'provider_': {
        'columns':{
          'Provider ID':int,
          'Provider First Name':str,
          'Provider Last Name':str,
          'Provider User Name':str,
          'Provider Type':str,
          'Provider Type Name':str,
          'Provider Type Category':str,
          'Provider NPI Number':str,
          'Provider Group ID':int,
          'Supervising Provider ID':int,
          'Taxonomy':str,
          'Specialty':str,
          'Created Datetime':str,
          'Deleted By':str
        },
        'rename': dict(),
        'postgres_table': 'athenadwh_provider_clone'
        },
        'patientpastmedicalhistory': {
          'columns':{
            'Past Medical History ID':int,
            'Patient ID':float,
            'Chart ID':int,
            'Past Medical History Key':str,
            'Past Medical History Question':str,
            'Past Medical History Answer':str,
            'Created Datetime':str,
            'Created By':str
          },
          'rename': {
            'Past Medical History ID':'id',
            'Past Medical History Key':'medical_history_key',
            'Past Medical History Question':'question',
            'Past Medical History Answer':'answer'
          },
          'postgres_table': 'athenadwh_medical_history_clone'
        },
        'patientsocialhistory': {
          'columns':{
            'Social History ID':int,
            'Patient ID':int,
            'Chart ID':int,
            'Social History Key':str,
            'Social History Name':str,
            'Social History Answer':str,
            'Created Datetime':str,
            'Created By':str
          },
          'rename':{
            'Social History ID':'id',
            'Social History Name':'question',
            'Social History Answer':'answer'
          },
          'postgres_table': 'athenadwh_social_history_clone'
        },
        'clinicalresult_': {
          'columns':{
            'Clinical Result ID':int,
            'Document ID':int,
            'Clinical Provider ID':float,
            'Specimen Source':str,
            'Clinical Order Type':str,
            'Clinical Order Type Group':str,
            'Clinical Order Genus':str,
            'Created Datetime':str,
            'Created By':str
            },
          'rename': dict(),
          'postgres_table': 'athenadwh_clinical_results_clone'
        },
        'document_': {
          'columns':{
            'Document ID':int,
            'Patient ID':float,
            'Chart ID':float
          },
          'rename':dict(),
          'postgres_table': 'athenadwh_document_crosswalk_clone'
        },
        'clinicalprovider_': {
          'columns': {
            'Clinical Provider ID': int,
            'Fax': str
          },
          'rename': dict(),
          'postgres_table': 'athenadwh_clinical_providers_fax_clone'
        },
        'clinicalencounter_': {
          'columns': {
            'Clinical Encounter ID': int,
            'Patient ID': int,
            'Chart ID': int,
            'Appointment ID': float,
            'Provider ID': int,
            'Encounter Date': str,
            'Encounter Status': str,
            'Created Datetime': str,
            'Closed Datetime': str,
            'Closed By': str
          },
          'rename': dict(),
          'postgres_table': 'athenadwh_clinical_encounters_clone_full'
        },
        'medication_': {
          'columns': {
            'Medication ID': int,
            'Medication Name': str,
            'FDB Med ID': float,
            'Med Name ID': float,
            'RxNorm': str,
            'NDC':str,
            'HIC3 Code': str,
            'HIC3 Description': str,
            'HIC1 Code': str,
            'HIC1 Description': str,
            'GCN Clinical Forumulation ID': float,
            'HIC2 Pharmacological Class': str,
            'HIC4 Ingredient Base': str,
            'DEA Schedule': str,
          },
          'rename': dict(),
          'postgres_table': 'athenadwh_medication_clone'
        },
        'patientmedication_': {
          'columns': {
            'Patient Medication ID': int,
            'Medication Type': str,
            'Patient ID': float,
            'Chart ID': float,
            'Document ID': float,
            'Medication ID': float,
            'Sig': str,
            'Medication Name': str,
            'Dosage Form': str,
            'Dosage Action': str,
            'Dosage Strength': str,
            'Dosage Strength Units': str,
            'Dosage Quantity': float,
            'Dosage Route': str,
            'Frequency': str,
            'Prescription Fill Quantity': float,
            'Number of Refills Prescribed': float,
            'Fill Date': str,
            'Pharmacy Name': str,
            'Med Administered Datetime': str,
            'Created Datetime': str,
            'Created By': str,
            'Deactivation Datetime': str,
            'Deactivated By': str,
            'Prescribed YN': str,
            'Administered YN': str,
            'Dispensed YN': str,
          },
          'rename': dict(),
          'postgres_table': 'athenadwh_patient_medication_clone'
        }
    }
    return file_dict
