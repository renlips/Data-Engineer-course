TRUNCATE TABLE de1m.ospv_stg_rep_fraud;

INSERT INTO de1m.ospv_stg_rep_fraud
SELECT
	event_dt,
	passport,
	fio,
	phone,
	event_type,
	report_dt
FROM de1m.ospv_rep_fraud_source
WHERE event_dt > coalesce( (SELECT max_update_dt
						   FROM de1m.ospv_meta_dwh
						   WHERE schema_name = 'DE1M' AND table_name = 'OSPV_REP_FRAUD'), to_date( '1800.01.01', 'YYYY.MM.DD' ));

INSERT INTO de1m.ospv_rep_fraud
SELECT *
FROM de1m.ospv_stg_rep_fraud;

MERGE INTO de1m.ospv_meta_dwh m
USING (SELECT 'DE1M' schema_name, 'OSPV_REP_FRAUD' table_name, (SELECT MAX(event_dt) FROM de1m.ospv_stg_rep_fraud) max_update_dt FROM dual) src
ON (m.schema_name = src.schema_name AND m.table_name = src.table_name)
WHEN MATCHED THEN
	UPDATE SET m.max_update_dt = src.max_update_dt
	WHERE src.max_update_dt IS NOT NULL
WHEN NOT MATCHED THEN
	INSERT (schema_name, table_name, max_update_dt)
	VALUES ('DE1M', 'OSPV_REP_FRAUD', COALESCE(src.max_update_dt, TO_DATE('1900-01-01', 'YYYY-MM-DD')));