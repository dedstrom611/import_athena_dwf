/*
ALTER TABLE looker_scratch.athenadwh_social_history_clone
        ALTER COLUMN patient_id TYPE float(4);


ALTER TABLE athenadwh_social_history_clone
        DROP CONSTRAINT athenadwh_social_history_clone_pkey;
 */
/*
select * from looker_scratch.athenadwh_documents_clone
        WHERE document_class IN ('DME', 'IMAGINGRESULT', 'LABRESULT')
        ORDER BY created_at DESC
        LIMIT 1000;
     

CREATE TABLE athenadwh_document_crosswalk_clone (
     document_id INT,
     patient_id INT,
     chart_id INT,
     PRIMARY KEY (document_id));

CREATE INDEX index_document_crosswalk_clone
 ON athenadwh_document_crosswalk_clone (document_id, patient_id, chart_id);
*/

--select * from looker_scratch.athenadwh_documents_clone LIMIT 500;

/*
select document_class, count(*) as cnt 
        from looker_scratch.athenadwh_documents_clone 
        GROUP BY document_class
        ORDER BY document_class
        LIMIT 1000;
*/
/*
SELECT *
        FROM looker_scratch.athenadwh_document_crosswalk_clone AS cw
        JOIN looker_scratch.athenadwh_documents_clone AS doc
        ON cw.document_id = doc.document_id
        LIMIT 2000; 
*/



/*
DELETE FROM looker_scratch.athenadwh_document_crosswalk_clone
WHERE id IN
    (SELECT id
    FROM 
        (SELECT id,
         ROW_NUMBER() OVER( PARTITION BY document_id, patient_id, chart_id
        ORDER BY  id ) AS row_num
        FROM looker_scratch.athenadwh_document_crosswalk_clone ) t
        WHERE t.row_num > 1 );
*/
/*
DELETE FROM looker_scratch.athenadwh_document_crosswalk_clone
WHERE id IN
    (SELECT id
    FROM 
        (SELECT id,
         ROW_NUMBER() OVER( PARTITION BY document_id
        ORDER BY  id ) AS row_num
        FROM looker_scratch.athenadwh_document_crosswalk_clone ) t
        WHERE t.row_num > 1 );
*/
/*
select document_class, count(*) as cnt from looker_scratch.athenadwh_documents_clone
where status != 'DELETED'
group by document_class
order by cnt
--IN ('LABRESULT', 'IMAGINGRESULT', 'DME')
LIMIT 2000;
*/

/*
select 
cw.patient_id,
        cw.chart_id,
        cw.document_id,
        doc.clinical_encounter_id,
        document_class,
        clinical_order_type,
        status
        FROM looker_scratch.athenadwh_documents_clone doc
        JOIN looker_scratch.athenadwh_document_crosswalk_clone cw
        ON doc.document_id = cw.document_id
        WHERE document_class IN ('ELECTRONICREFERRAL') AND status != 'DELETED'
        ORDER BY DATE(created_datetime) DESC, patient_id
limit 2000;
*/

/*
select *
        from looker_scratch.athenadwh_document_crosswalk_clone cw
        JOIN looker_scratch.athenadwh_documents_clone doc
        ON cw.document_id = doc.document_id
        where status != 'DELETED' and patient_id = 23508
        ORDER BY clinical_encounter_id DESC, cw.document_id;
*/
/*
SELECT *
        FROM athenadwh_documents_clone
        WHERE document_class IN ('ORDER')
        ORDER BY DATE(created_at)
        LIMIT 500;

SELECT * 
        FROM athenadwh_documents_clone d
        JOIN athenadwh_clinical_providers_clone p
        ON d.clinical_provider_id = p.clinical_provider_id
        JOIN athenadwh_clinical_encounters_clone e
        ON d.clinical_encounter_id = e.clinical_encounter_id
                WHERE document_id = 128219;
*/

select * from athenadwh_documents_clone d
JOIN athenadwh_document_crosswalk_clone c
ON d.document_id = c.document_id
WHERE patient_id = 19017

LIMIT 500;
        

        

;

