TRUNCATE TABLE de1m.ospv_stg_accounts;
TRUNCATE TABLE de1m.ospv_stg_accounts_del;

INSERT INTO de1m.ospv_stg_accounts
SELECT 
	account, 
	valid_to, 
	client, 
	create_dt, 
	coalesce (update_dt, create_dt) update_dt 
FROM BANK.ACCOUNTS 
WHERE COALESCE (update_dt, create_dt) > coalesce( (SELECT max_update_dt
	    										   FROM de1m.ospv_meta_dwh
	    										   WHERE schema_name = 'BANK' AND table_name = 'ACCOUNTS'), to_date( '1800.01.01', 'YYYY.MM.DD' ));

INSERT INTO de1m.ospv_stg_accounts_del ( account )
SELECT account FROM bank.accounts;	

MERGE INTO de1m.ospv_dwh_dim_accounts_hist a
USING de1m.ospv_stg_accounts s
ON (s.account = a.account AND deleted_flg = 'N')
WHEN MATCHED THEN
	UPDATE SET a.effective_to = s.update_dt - INTERVAL '1' SECOND
	WHERE 	(1=0
			OR s.client <> a.client
    		OR (s.client IS NULL AND a.client IS NOT NULL)
    		OR (s.client IS NOT NULL AND a.client IS NULL)
    		)
WHEN NOT MATCHED THEN
	INSERT (account, valid_to, client, effective_from, effective_to, deleted_flg)
	VALUES (s.account,
			s.valid_to,
			s.client,
			s.update_dt,
			TO_DATE('9999-12-31','YYYY-MM-DD'),
			'N'
			);

INSERT INTO de1m.ospv_dwh_dim_accounts_hist
SELECT
	s.account,
	s.valid_to,
	s.client,
	s.update_dt,
	TO_DATE('9999-12-31','YYYY-MM-DD'),
	'N'
FROM de1m.ospv_stg_accounts s
LEFT JOIN de1m.ospv_dwh_dim_accounts_hist a
	ON s.account = a.account AND a.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD') AND deleted_flg = 'N'
WHERE	(1=0
		OR s.client <> a.client
		OR (s.client IS NULL AND a.client IS NOT NULL)
		OR (s.client IS NOT NULL AND a.client IS NULL)
		);

INSERT INTO de1m.ospv_dwh_dim_accounts_hist
SELECT
	account,
	valid_to,
	client,
	effective_from + INTERVAL '1' SECOND,
	TO_DATE('9999-12-31','YYYY-MM-DD'),
	'Y'
FROM de1m.ospv_dwh_dim_accounts_hist a
WHERE a.account IN ( SELECT DISTINCT a.account
					 FROM de1m.ospv_dwh_dim_accounts_hist a
					 LEFT JOIN de1m.ospv_stg_accounts_del sd
					 	ON a.account = sd.account
					 WHERE sd.account IN NULL
				);

MERGE INTO de1m.ospv_meta_dwh m
USING (SELECT 'BANK' schema_name, 'ACCOUNTS' table_name, (SELECT MAX(update_dt) FROM de1m.ospv_stg_accounts) max_update_dt FROM dual) src
ON (m.schema_name = src.schema_name AND m.table_name = src.table_name)
WHEN MATCHED THEN
	UPDATE SET m.max_update_dt = src.max_update_dt
	WHERE src.max_update_dt IS NOT NULL
WHEN NOT MATCHED THEN
	INSERT (schema_name, table_name, max_update_dt)
	VALUES ('BANK', 'ACCOUNTS', COALESCE(src.max_update_dt, TO_DATE('1900-01-01', 'YYYY-MM-DD')));

COMMIT;