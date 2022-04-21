#!/usr/bin/python
print('===   Starting ETL script   ===')

import pandas as pd
import jaydebeapi
from datetime import datetime
import shutil
import os, fnmatch

def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


print('Establishing JDBC connection... [ ]')

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
                            'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
                            ['de1m', 'samwisegamgee'],
                            '/home/de1m/ojdbc8.jar'
                         )
conn.jconn.setAutoCommit(False)
curs = conn.cursor()

print('JDBC connection established [v]')



#Upload TRANSACTIONS
print('Loading transactions source...[ ]')

trans_file = find('transactions_*.txt', '/home/de1m/ospv/')
print(trans_file[0] + ' file has been found')


df_trans = pd.read_csv(trans_file[0], sep=';', decimal=',')
curs.executemany( '''insert into de1m.ospv_dwh_fact_transactions
                    values(?,to_date( ?, 'YYYY-MM-DD HH24:MI:SS' ),?,?,?,?,?)''', df_trans.values.tolist()
                )

conn.commit()
print('Transactions source loaded   [v]')



#Upload TERMIMALS
print('Loading terminals source...[ ]')

term_file = find('terminals_*.xlsx', '/home/de1m/ospv/')
print(term_file[0] + ' file has been found')

#ter_src_file = 'data/{0}'.format(term_file[0])
df_term = pd.read_excel(term_file[0], sheet_name='terminals', header=0, index_col=None)
name, ext = os.path.splitext(term_file[0])
date = datetime.strptime(str(name.split('_')[-1]), '%d%m%Y')
dt = date.date()
sql = 'insert into de1m.ospv_terminals values(?,?,?,?,TO_DATE(\'{0}\',\'YYYY-MM-DD\'),NULL)'.format('2021-03-02')
curs.executemany( sql, df_term.values.tolist() )
conn.commit()

print('Terminals source loaded   [v]')



#Upload PASSPORT BLACKLIST
print('Loading passport blacklist source...[ ]')

pass_bl_file = find('passport_b*.xlsx', 'data/')
print(pass_bl_file[0] + ' file has been found')

df_passbl = pd.read_excel(pass_bl_file[0], sheet_name='blacklist', header=0, index_col=None)
df_passbl = df_passbl.astype(str)
curs.executemany( '''insert into de1m.ospv_dwh_fact_pssprt_blacklst
                    values(to_date(?, 'YYYY-MM-DD'),?)''', df_passbl.values.tolist()
                )

conn.commit()
print('Passport blacklist source loaded  [v]')



### SQL scripts ###
print('---===   SQL scipts   ===---')

#Accounts SCD2 Processing
print('Accouts SCD2 processing...[ ]')
SCD2_accounts = open('sql_scripts/SCD2_accounts_scr.sql','r')
sql = SCD2_accounts.read()
sql_coms = sql.replace('\n', ' ').split(';')[:-1]
for sql_com in sql_coms:
    curs.execute(sql_com)

conn.commit()
print('Accouts SCD2 data has been processed successfully   [v]')



#Cards SCD2 processing
print('Cards SCD2 processing...[ ]')

SCD2_cards = open('sql_scripts/SCD2_cards_scr.sql','r')
sql = SCD2_cards.read()
sql_coms = sql.replace('\n', ' ').split(';')[:-1]
for sql_com in sql_coms:
    curs.execute(sql_com)

conn.commit()
print('Cards SCD2 data has been processed successfully   [v]')



#Clients SCD2 processing
print('Clients SCD2 processing...[ ]')

SCD2_clients = open('sql_scripts/SCD2_clients_scr.sql','r')
sql = SCD2_clients.read()
sql_coms = sql.replace('\n', ' ').split(';')[:-1]
for sql_com in sql_coms:
    curs.execute(sql_com)

conn.commit()
print('Clients SCD2 data has been processed successfully   [v]')



#Terminals SCD2 processing
print('Terminals SCD2 processing...[ ]')

SCD2_terminals = open('sql_scripts/SCD2_terminals_scr.sql','r')
sql = SCD2_terminals.read()
sql_coms = sql.replace('\n', ' ').split(';')[:-1]
for sql_com in sql_coms:
    curs.execute(sql_com)

conn.commit()
print('Terminals SCD2 data has been processed successfully   [v]')

#ANTI-FRAUD SQL script
print('Starting Anti-Fraud engine... 0%')

fraud = open('sql_scripts/Anti-fraud_scr.sql','r')
sql = fraud.read()
sql_coms = sql.replace('\n', ' ').split(';')[:-1]
for sql_com in sql_coms:
    curs.execute(sql_com)
    print('processing...')
conn.commit()

print('Anti-Fraud engine compleded 100%')

print('Preparing REP_FRAUD table...')

report = open('sql_scripts/Report.sql','r')
sql = report.read()
sql_coms = sql.replace('\n', ' ').split(';')[:-1]
for sql_com in sql_coms:
    curs.execute(sql_com)

conn.commit()
print('  REP_FRAUD table is ready  ')

print('Moving files into archive...[ ]')

trans_move_to = str('/home/de1m/ospv/archive/'+ str(trans_file[0][trans_file[0].find('transactions'):] + '.backup'))
term_move_to = str('/home/de1m/ospv/archive/'+ str(term_file[0][term_file[0].find('terminals'):] + '.backup'))
pass_bl_move_to = str('/home/de1m/ospv/archive/'+ str(pass_bl_file[0][pass_bl_file[0].find('passport'):] + '.backup'))

shutil.move(trans_file[0], trans_move_to)
shutil.move(term_file[0], term_move_to)
shutil.move(pass_bl_file[0], pass_bl_move_to)
print('Files moving completed   [v]')

curs.close()
conn.close()
print('===   ETL script has been successfully finished   ===')
