INSERT INTO de1m.ospv_rep_fraud_source
SELECT
    t.transaction_date AS event_dt,
    cl.PASSPORT_NUM AS passport,
    cl.LAST_NAME ||' '|| cl.FIRST_NAME ||' '|| cl.PATRONYMIC AS fio,
    cl.PHONE AS phone,
    '0' AS event_type,
    TO_DATE(TO_CHAR(t.transaction_date,'YYYY-MM-DD'),'YYYY-MM-DD') AS report_dt
FROM de1m.ospv_dwh_fact_transactions t
LEFT JOIN de1m.ospv_dwh_dim_cards_hist ca
    ON t.card_num = ca.card_num
LEFT JOIN de1m.ospv_dwh_dim_accounts_hist a
    ON ca.account = a.account
LEFT JOIN de1m.ospv_dwh_dim_clients_hist cl
    ON a.client = cl.client_id AND t.transaction_date > cl.passport_valid_to
WHERE cl.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD') 
UNION ALL
SELECT
    t.transaction_date AS event_dt,
    cl.PASSPORT_NUM AS passport,
    cl.LAST_NAME ||' '|| cl.FIRST_NAME ||' '|| cl.PATRONYMIC AS fio,
    cl.PHONE AS phone,
    '1' AS event_type,
    TO_DATE(TO_CHAR(t.transaction_date,'YYYY-MM-DD'),'YYYY-MM-DD') AS report_dt
FROM de1m.ospv_dwh_fact_transactions t
LEFT JOIN de1m.ospv_dwh_dim_cards_hist ca
    ON t.card_num = ca.card_num
LEFT JOIN de1m.ospv_dwh_dim_accounts_hist a
    ON ca.account = a.account
LEFT JOIN de1m.ospv_dwh_dim_clients_hist cl
    ON a.client = cl.client_id
JOIN de1m.ospv_dwh_fact_pssprt_blacklst bl
    ON cl.PASSPORT_NUM = bl.PASSPORT_NUM
WHERE cl.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD');



INSERT INTO de1m.ospv_rep_fraud_source
SELECT
    t.transaction_date AS event_dt,
    cl.PASSPORT_NUM AS passport,
    cl.LAST_NAME ||' '|| cl.FIRST_NAME ||' '|| cl.PATRONYMIC AS fio,
    cl.PHONE AS phone,
    '2' AS event_type,
    TO_DATE(TO_CHAR(t.transaction_date,'YYYY-MM-DD'),'YYYY-MM-DD') AS report_dt
FROM de1m.ospv_dwh_fact_transactions t
JOIN de1m.ospv_dwh_dim_cards_hist ca
    ON t.card_num = ca.card_num
JOIN de1m.ospv_dwh_dim_accounts_hist a
    ON ca.account = a.account AND t.TRANSACTION_DATE >= a.VALID_TO 
JOIN de1m.ospv_dwh_dim_clients_hist cl
    ON a.client = cl.client_id
WHERE cl.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD');



INSERT INTO de1m.ospv_rep_fraud_source
SELECT
    transaction_date1 AS event_dt,
    PASSPORT_NUM AS passport,
    LAST_NAME ||' '|| FIRST_NAME ||' '|| PATRONYMIC AS fio,
    PHONE AS phone,
    '3' AS event_type,
    TO_DATE(TO_CHAR(transaction_date1,'YYYY-MM-DD'),'YYYY-MM-DD') AS report_dt
FROM
    (SELECT
        t.TRANSACTION_ID,
        LAG(t.TRANSACTION_DATE) OVER (PARTITION BY ca.CARD_NUM ORDER BY t.TRANSACTION_DATE) TRANSACTION_DATE1,
        t.TRANSACTION_DATE TRANSACTION_DATE2,
        t.AMOUNT ,
        t.OPER_TYPE ,
        t.OPER_RESULT ,
        LAG(ter.TERMINAL_CITY) OVER (PARTITION BY ca.CARD_NUM ORDER BY t.TRANSACTION_DATE) city1,
        ter.TERMINAL_CITY city2,
        ter.TERMINAL_TYPE ,
        a.ACCOUNT ,
        cl.LAST_NAME ,
        cl.FIRST_NAME,
        cl.PATRONYMIC ,
        cl.PASSPORT_NUM,
        cl.PHONE        
    FROM de1m.ospv_dwh_fact_transactions t
    LEFT JOIN de1m.OSPV_DWH_DIM_TERMINALS_HIST ter
        ON t.TERMINAL = ter.TERMINAL_ID 
    LEFT JOIN de1m.ospv_dwh_dim_cards_hist ca
        ON t.card_num = ca.card_num
    LEFT JOIN de1m.ospv_dwh_dim_accounts_hist a
        ON ca.account = a.account
    LEFT JOIN de1m.ospv_dwh_dim_clients_hist cl
        ON a.client = cl.client_id
    WHERE cl.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
    ) x
WHERE city1 <> city2
      AND OPER_TYPE <> 'PAYMENT'
      AND TRANSACTION_DATE2 - TRANSACTION_DATE1 < 8600/86400;


INSERT INTO de1m.ospv_rep_fraud_source
SELECT
    transaction_date AS event_dt,
    PASSPORT_NUM AS passport,
    LAST_NAME ||' '|| FIRST_NAME ||' '|| PATRONYMIC AS fio,
    PHONE AS phone,
    '4' AS event_type,
    TO_DATE(TO_CHAR(transaction_date,'YYYY-MM-DD'),'YYYY-MM-DD') AS report_dt
FROM
    (SELECT
        t.TRANSACTION_ID,
        t.TRANSACTION_DATE,
        LAG(t.TRANSACTION_DATE,3) OVER (PARTITION BY ca.CARD_NUM ORDER BY t.TRANSACTION_DATE) time1,
        LAG(t.amount,3) OVER (PARTITION BY ca.CARD_NUM ORDER BY t.TRANSACTION_DATE) amt1, 
        LAG(t.amount,2) OVER (PARTITION BY ca.CARD_NUM ORDER BY t.TRANSACTION_DATE) amt2,
        LAG(t.TRANSACTION_DATE) OVER (PARTITION BY ca.CARD_NUM ORDER BY t.TRANSACTION_DATE) time3,
        LAG(t.amount) OVER (PARTITION BY ca.CARD_NUM ORDER BY t.TRANSACTION_DATE) amt3,
        t.AMOUNT amt4,
        t.OPER_TYPE,
        LAG(t.OPER_result,3) OVER (PARTITION BY ca.CARD_NUM ORDER BY t.TRANSACTION_DATE) res1,
        LAG(t.OPER_RESULT,2) OVER (PARTITION BY ca.CARD_NUM ORDER BY t.TRANSACTION_DATE) res2,
        LAG(t.OPER_result) OVER (PARTITION BY ca.CARD_NUM ORDER BY t.TRANSACTION_DATE) res3, 
        t.OPER_result res4,
        cl.LAST_NAME,
        cl.FIRST_NAME,
        cl.PATRONYMIC ,
        cl.PASSPORT_NUM,
        cl.PHONE            
    FROM de1m.ospv_dwh_fact_transactions t
    LEFT JOIN de1m.OSPV_DWH_DIM_TERMINALS_HIST ter
        ON t.TERMINAL = ter.TERMINAL_ID 
    LEFT JOIN de1m.ospv_dwh_dim_cards_hist ca
        ON t.card_num = ca.card_num
    LEFT JOIN de1m.ospv_dwh_dim_accounts_hist a
        ON ca.account = a.account
    LEFT JOIN de1m.ospv_dwh_dim_clients_hist cl
        ON a.client = cl.client_id
    WHERE cl.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
    ) x
WHERE   OPER_TYPE = 'WITHDRAW'
        AND AMT1>AMT2 AND AMT2>AMT3 AND AMT3>AMT4
        AND res1 = 'REJECT' AND res2 = 'REJECT' AND res3 = 'REJECT' AND res4 = 'SUCCESS'
        AND time3 - time1 <= 1200/86400;

