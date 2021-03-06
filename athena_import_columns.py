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
      'Created Datetime':str
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
    }
}
