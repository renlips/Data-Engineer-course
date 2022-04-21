TRUNCATE TABLE de1m.ospv_stg_cards;
TRUNCATE TABLE de1m.ospv_stg_cards_del;

INSERT INTO de1m.ospv_stg_cards
SELECT 
	card_num, 
	account,  
	create_dt, 
	coalesce (update_dt, create_dt) update_dt 
FROM BANK.CARDS 
WHERE COALESCE (update_dt, create_dt) > coalesce( (SELECT max_update_dt
	    										   FROM de1m.ospv_meta_dwh
	    										   WHERE schema_name = 'BANK' AND table_name = 'CARDS'), to_date( '1800.01.01', 'YYYY.MM.DD' ));

INSERT INTO de1m.ospv_stg_cards_del ( account )
SELECT account FROM bank.cards;	

MERGE INTO de1m.ospv_dwh_dim_cards_hist c
USING de1m.ospv_stg_cards s
ON (s.account = c.account AND deleted_flg = 'N')
WHEN MATCHED THEN
	UPDATE SET c.effective_to = s.update_dt - INTERVAL '1' SECOND
	WHERE 	(1=0
			OR s.card_num <> c.card_num
    		OR (s.card_num IS NULL AND c.card_num IS NOT NULL)
    		OR (s.card_num IS NOT NULL AND c.card_num IS NULL)
    		)
WHEN NOT MATCHED THEN
	INSERT (card_num, account, effective_from, effective_to, deleted_flg)
	VALUES (
			s.card_num,
			s.account,
			s.update_dt,
			TO_DATE('9999-12-31','YYYY-MM-DD'),
			'N'
			);

INSERT INTO de1m.ospv_dwh_dim_cards_hist
SELECT
	s.card_num,
	s.account,
	s.update_dt,
	TO_DATE('9999-12-31','YYYY-MM-DD'),
	'N'
FROM de1m.ospv_stg_cards s
LEFT JOIN de1m.ospv_dwh_dim_CARDS_hist c
	ON s.account = c.account AND c.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD') AND deleted_flg = 'N'
WHERE	(1=0
		OR (s.card_num IS NULL AND c.card_num IS NOT NULL)
		OR (s.card_num IS NOT NULL AND c.card_num IS NULL)
		);

INSERT INTO de1m.ospv_dwh_dim_cards_hist
SELECT
	card_num,
	account,
	effective_to + INTERVAL '1' SECOND,
	TO_DATE('9999-12-31','YYYY-MM-DD'),
	'Y'
FROM de1m.ospv_dwh_dim_cards_hist c
WHERE c.account IN ( SELECT DISTINCT c.account
					 FROM de1m.ospv_dwh_dim_CARDS_hist c
					 LEFT JOIN de1m.ospv_stg_cards_del sd
					 	ON c.account = sd.account
					 WHERE sd.account IN NULL
				);

MERGE INTO de1m.ospv_meta_dwh m
USING (SELECT 'BANK' schema_name, 'CARDS' table_name, (SELECT MAX(update_dt) FROM de1m.ospv_stg_cards) max_update_dt FROM dual) src
ON (m.schema_name = src.schema_name AND m.table_name = src.table_name)
WHEN MATCHED THEN
	UPDATE SET m.max_update_dt = src.max_update_dt
	WHERE src.max_update_dt IS NOT NULL
WHEN NOT MATCHED THEN
	INSERT (schema_name, table_name, max_update_dt)
	VALUES ('BANK', 'CARDS', COALESCE(src.max_update_dt, TO_DATE('1900-01-01', 'YYYY-MM-DD')));

COMMIT;