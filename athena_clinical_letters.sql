USE jasperdb;


SELECT * FROM
athenadwh_clinical_encounters enc
LEFT JOIN athenadwh_documents doc
	ON enc.clinical_encounter_id = doc.clinical_encounter_id AND doc.document_class = 'LETTER' AND
    (doc.document_subclass != 'LETTER_PATIENTCORRESPONDENCE' OR doc.document_subclass IS NULL)
LEFT JOIN athenadwh_clinical_letters ltr
	ON ltr.document_id = doc.document_id -- AND ltr.role != 'Patient'
LEFT JOIN athenadwh_clinical_providers prv
	ON ltr.clinical_provider_recipient_id = prv.clinical_provider_id
LEFT JOIN athenadwh_patients pt
	ON enc.patient_id = pt.patient_id
    -- WHERE pt.patient_id = 20422
    ORDER BY ltr.updated_at DESC LIMIT 500;

-- select * from athenadwh_clinical_letters order by document_id desc LIMIT 500;











    
            