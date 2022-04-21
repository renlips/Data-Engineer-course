TRUNCATE TABLE de1m.ospv_stg_clients;
TRUNCATE TABLE de1m.ospv_stg_clients_del;

INSERT INTO de1m.ospv_stg_clients (client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,create_dt,update_dt)
SELECT  
	client_id,
	last_name,
	first_name,
	patronymic,
	date_of_birth,
	passport_num,
	COALESCE(passport_valid_to, TO_DATE('9999-12-31','YYYY-MM-DD')) AS passport_valid_to,
	phone,
	create_dt, 
	coalesce (update_dt, create_dt) update_dt 
FROM BANK.CLIENTS 
WHERE COALESCE (update_dt, create_dt) > coalesce( (SELECT max_update_dt
	    										   FROM de1m.ospv_meta_dwh
	    										   WHERE schema_name = 'BANK' AND table_name = 'CLIENTS'), to_date( '1800.01.01', 'YYYY.MM.DD' ));

INSERT INTO de1m.ospv_stg_clients_del ( client_id )
SELECT client_id FROM bank.clients;	

MERGE INTO de1m.ospv_dwh_dim_clients_hist c
USING de1m.ospv_stg_clients s
ON (s.client_id = c.client_id AND deleted_flg = 'N')
WHEN MATCHED THEN
	UPDATE SET c.effective_to = s.update_dt - INTERVAL '1' SECOND
	WHERE 	(1=0
			OR s.passport_num <> c.passport_num
			OR (s.passport_num IS NULL AND c.passport_num IS NOT NULL)
			OR (s.passport_num IS NOT NULL AND c.passport_num IS NULL)
			)
WHEN NOT MATCHED THEN
	INSERT (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
	VALUES (
			s.client_id,
			s.last_name,
			s.first_name,
			s.patronymic,
			s.date_of_birth,
			s.passport_num,
			s.passport_valid_to,
			s.phone,
			s.update_dt,
			TO_DATE('9999-12-31','YYYY-MM-DD'),
			'N'
			);

INSERT INTO de1m.ospv_dwh_dim_clients_hist(client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,effective_from,effective_to,deleted_flg)
SELECT
	s.client_id,
	s.last_name,
	s.first_name,
	s.patronymic,
	s.date_of_birth,
	s.passport_num,
	s.passport_valid_to,
	s.phone,
	s.update_dt,
	TO_DATE('9999-12-31','YYYY-MM-DD'),
	'N'
FROM de1m.ospv_stg_clients s
LEFT JOIN de1m.ospv_dwh_dim_CLIENTS_hist c
	ON s.client_id = c.client_id AND c.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD') AND deleted_flg = 'N'
WHERE	(1=0
		OR s.passport_num <> c.passport_num
		OR (s.passport_num IS NULL AND c.passport_num IS NOT NULL)
		OR (s.passport_num IS NOT NULL AND c.passport_num IS NULL)
		);

INSERT INTO de1m.ospv_dwh_dim_clients_hist(client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,effective_from,effective_to,deleted_flg)
SELECT
	client_id,
	last_name,
	first_name,
	patronymic,
	date_of_birth,
	passport_num,
	passport_valid_to,
	phone,
	effective_to + INTERVAL '1' SECOND,
	TO_DATE('9999-12-31','YYYY-MM-DD'),
	'Y'
FROM de1m.ospv_dwh_dim_clients_hist c
WHERE c.client_id IN ( SELECT DISTINCT c.client_id
					 FROM de1m.ospv_dwh_dim_CLIENTS_hist c
					 LEFT JOIN de1m.ospv_stg_clients_del sd
					 	ON c.client_id = sd.client_id
					 WHERE sd.client_id IN NULL
				);

MERGE INTO de1m.ospv_meta_dwh m
USING (SELECT 'BANK' schema_name, 'CLIENTS' table_name, (SELECT MAX(update_dt) FROM de1m.ospv_stg_clients) max_update_dt FROM dual) src
ON (m.schema_name = src.schema_name AND m.table_name = src.table_name)
WHEN MATCHED THEN
	UPDATE SET m.max_update_dt = src.max_update_dt
	WHERE src.max_update_dt IS NOT NULL
WHEN NOT MATCHED THEN
	INSERT (schema_name, table_name, max_update_dt)
	VALUES ('BANK', 'CLIENTS', COALESCE(src.max_update_dt, TO_DATE('1900-01-01', 'YYYY-MM-DD')));

COMMIT;