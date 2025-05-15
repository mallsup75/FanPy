SELECT 
    gr.golden_record_id,
    gr.first_name,
    gr.last_name,
    gr.email,
    gr.address,
    gr.phone_number,
    ss.system_name,
    scr.source_customer_id,
    scr.email AS source_email,
    rm.confidence_score
FROM Golden_Record gr
JOIN Record_Mapping rm ON gr.golden_record_id = rm.golden_record_id
JOIN Source_Customer_Record scr ON rm.record_id = scr.record_id
JOIN Source_System ss ON scr.system_id = ss.system_id
WHERE gr.golden_record_id IN (1, 4); -- John Doe and Bob Brown
