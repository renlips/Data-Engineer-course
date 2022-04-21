TRUNCATE TABLE de1m.ospv_stg_terminals;
TRUNCATE TABLE de1m.ospv_stg_terminals_del;

INSERT INTO de1m.ospv_stg_terminals t
SELECT 
	terminal_id,
	terminal_type,
	terminal_city,
	terminal_address, 
	create_dt, 
	coalesce (update_dt, create_dt) update_dt 
FROM de1m.ospv_terminals
WHERE COALESCE (update_dt, create_dt) > coalesce( (SELECT max_update_dt
	    										   FROM de1m.ospv_meta_dwh
	    										   WHERE schema_name = 'DE1M' AND table_name = 'OSPV_TERMINALS'), to_date( '1800.01.01', 'YYYY.MM.DD' ));


INSERT INTO de1m.ospv_stg_terminals_del ( terminal_id )
SELECT terminal_id FROM de1m.ospv_terminals;	

MERGE INTO de1m.ospv_dwh_dim_terminals_hist t
USING de1m.ospv_stg_terminals s
ON (s.terminal_id = t.terminal_id AND deleted_flg = 'N')
WHEN MATCHED THEN
	UPDATE SET t.effective_to = s.update_dt - INTERVAL '1' SECOND
	WHERE 	(1=0
			OR s.terminal_id <> t.terminal_id
    		OR (s.terminal_id IS NULL AND t.terminal_id IS NOT NULL)
    		OR (s.terminal_id IS NOT NULL AND t.terminal_id IS NULL)
    		)
WHEN NOT MATCHED THEN
	INSERT (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
	VALUES (
			s.terminal_id,
			s.terminal_type,
			s.terminal_city,
			s.terminal_address,
			s.update_dt,
			TO_DATE('9999-12-31','YYYY-MM-DD'),
			'N'
			);

INSERT INTO de1m.ospv_dwh_dim_terminals_hist
SELECT
	s.terminal_id,
	s.terminal_type,
	s.terminal_city,
	s.terminal_address,
	s.update_dt,
	TO_DATE('9999-12-31','YYYY-MM-DD'),
	'N'
FROM de1m.ospv_stg_terminals s
LEFT JOIN de1m.ospv_dwh_dim_TERMINALS_hist t
	ON s.terminal_id = t.terminal_id AND t.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD') AND deleted_flg = 'N'
WHERE	(1=0
		OR s.terminal_id <> t.terminal_id
		OR (s.terminal_id IS NULL AND t.terminal_id IS NOT NULL)
		OR (s.terminal_id IS NOT NULL AND t.terminal_id IS NULL)
		);

INSERT INTO de1m.ospv_dwh_dim_terminals_hist
SELECT
	terminal_id,
	terminal_type,
	terminal_city,
	terminal_address,
	effective_to + INTERVAL '1' SECOND,
	TO_DATE('9999-12-31','YYYY-MM-DD'),
	'Y'
FROM de1m.ospv_dwh_dim_terminals_hist t
WHERE t.terminal_id IN ( SELECT DISTINCT t.terminal_id
					 FROM de1m.ospv_dwh_dim_TERMINALS_hist t
					 LEFT JOIN de1m.ospv_stg_terminals_del sd
					 	ON t.terminal_id = sd.terminal_id
					 WHERE sd.terminal_id IN NULL
				);

MERGE INTO de1m.ospv_meta_dwh m
USING (SELECT 'DE1M' schema_name, 'OSPV_TERMINALS' table_name, (SELECT MAX(update_dt) FROM de1m.ospv_stg_terminals) max_update_dt FROM dual) src
ON (m.schema_name = src.schema_name AND m.table_name = src.table_name)
WHEN MATCHED THEN
	UPDATE SET m.max_update_dt = src.max_update_dt
	WHERE src.max_update_dt IS NOT NULL
WHEN NOT MATCHED THEN
	INSERT (schema_name, table_name, max_update_dt)
	VALUES ('DE1M', 'OSPV_TERMINALS', COALESCE(src.max_update_dt, TO_DATE('1900-01-01', 'YYYY-MM-DD')));

COMMIT;