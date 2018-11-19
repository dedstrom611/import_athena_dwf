SELECT
  rd.chart_id,
  rd.answer AS review_date,
  notes.answer AS notes,
  hyp.answer AS hypertension,
  hch.answer AS high_cholesterol,
  db.answer AS diabetes,
  copd.answer AS copd,
  ast.answer AS asthma,
  cnc.answer AS cancer,
  kd.answer AS kidney_disease,
  stk.answer AS stroke,
  dep.answer AS depression,
  cad.answer AS coronary_artery_disease,
  pe.answer AS pulmonary_embolism

  FROM athenadwh_medical_history_clone rd
    LEFT JOIN athenadwh_medical_history_clone notes
      ON rd.chart_id = notes.chart_id AND notes.medical_history_key = 'PASTMEDICALHISTORYANSWERS.FREETEXT' AND rd.medical_history_key = 'REVIEWED.PASTMEDICALHISTORY'
    LEFT JOIN athenadwh_medical_history_clone hyp
      ON rd.chart_id = hyp.chart_id AND hyp.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND hyp.question = 'Hypertension'
    LEFT JOIN athenadwh_medical_history_clone hch
      ON rd.chart_id = hch.chart_id AND hch.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND hch.question = 'High Cholesterol'
    LEFT JOIN athenadwh_medical_history_clone db
      ON rd.chart_id = db.chart_id AND db.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND db.question = 'Diabetes'
    LEFT JOIN athenadwh_medical_history_clone copd
      ON rd.chart_id = copd.chart_id AND copd.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND copd.question = 'COPD'
    LEFT JOIN athenadwh_medical_history_clone ast
      ON rd.chart_id = ast.chart_id AND ast.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND ast.question = 'Asthma'
    LEFT JOIN athenadwh_medical_history_clone cnc
      ON rd.chart_id = cnc.chart_id AND cnc.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND cnc.question = 'Cancer'
    LEFT JOIN athenadwh_medical_history_clone kd
      ON rd.chart_id = kd.chart_id AND kd.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND kd.question = 'Kidney Disease'
    LEFT JOIN athenadwh_medical_history_clone stk
      ON rd.chart_id = stk.chart_id AND stk.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND stk.question = 'Stroke'
    LEFT JOIN athenadwh_medical_history_clone dep
      ON rd.chart_id = dep.chart_id AND dep.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND dep.question = 'Depression'
    LEFT JOIN athenadwh_medical_history_clone cad
      ON rd.chart_id = cad.chart_id AND cad.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND cad.question = 'Coronary Artery Disease'
    LEFT JOIN athenadwh_medical_history_clone pe
      ON rd.chart_id = pe.chart_id AND pe.medical_history_key = 'PASTMEDICALHISTORYANSWERS.ANSWERYN' AND pe.question = 'Pulmonary Embolism';
