from psycopg2 import extras
from psycopg2 import connect
import logging
import pandas as pd
import sys
import os
import time

logger = logging.getLogger()
console = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(levelname)s - %(name)s - %(asctime)s - %(message)s")
console.setFormatter(formatter)
logger.addHandler(console)
logger.setLevel(logging.INFO)

conn = connect(database='postgres',
               user='postgres',
               password='postgres',
               host='database-1.czujkruu3gwv.eu-west-1.rds.amazonaws.com',
               port=5432)

cur = conn.cursor()
raw_table = 'raw.teams_revenue'
clean_table = 'cleaned.teams_revenue'
metadata_table = 'metadata.teams_name'
file_folder = 'data'
create_table_stmt = """CREATE TABLE IF NOT EXISTS {}
                        (
                        record_id serial,
                        valuation_year varchar(255)  null,
                        rank varchar(255)  null,
                        team_name varchar(255) not null,
                        current_value varchar(255)  null,
                        one_year_value_change varchar(255)  null,
                        debt_value varchar(255)  null,
                        revenue_m varchar(255)  null,
                        operating_income_m varchar(255)  null,
                        created_at timestamp default now()
                        )
                        ;

                        CREATE TABLE IF NOT EXISTS {}
                        (
                        record_id serial,
                        team_name varchar(255) not null,
                        team_id integer not null,
                        valuation_year varchar(255)  null,
                        current_value_m numeric  null,
                        one_year_value_change_pct numeric  null,
                        debt_value_pct numeric  null,
                        revenue_m numeric  null,
                        operating_income_m numeric  null,
                        created_at timestamp default now()
                        )
                        ;""".format(raw_table, clean_table)
cur.execute(create_table_stmt)

clean_table_stmt = """insert into {} (
                        team_name, 
                        team_id,
                        valuation_year,
                        current_value_m,
                        one_year_value_change_pct,
                        debt_value_pct,
                        revenue_m,
                        operating_income_m
                        )
       select m.team_full_name as team_name,
       m.team_id as team_id,
       cast(valuation_year as integer) as valuation_year,
       case 
       when current_value like '%B%' then cast(replace(replace(replace(current_value,'$',''),',','.'),'B','') as numeric)*1000
            when current_value like '%M%' then cast(replace(replace(replace(current_value,'$',''),',','.'),'M','') as numeric)
           when current_value like '%,%' then cast(replace(replace(current_value,'$',''),',','.') as numeric)*1000
           ELSE cast(replace(replace(current_value,'$',''),',','.') as numeric)
        END as current_value_m,
       cast(replace(one_year_value_change,'%','') as numeric)/100 as one_year_value_change_pct,
       cast(replace(debt_value,'%','') as numeric)/100 as debt_value_pct,
       case 
       when revenue_m like '%B%' then cast(replace(replace(replace(revenue_m,'$',''),',','.'),'B','') as numeric)*1000
            when revenue_m like '%M%' then cast(replace(replace(replace(revenue_m,'$',''),',','.'),'M','') as numeric)
           when revenue_m like '%,%' then cast(replace(replace(revenue_m,'$',''),',','.') as numeric)*1000
           when revenue_m like '%K%' then cast(replace(replace(replace(revenue_m,'$',''),',','.'),'K','') as numeric)/1000
           ELSE cast(replace(replace(revenue_m,'$',''),',','.') as numeric)
        END as revenue_m,
       case 
       when operating_income_m like '%B%' then cast(replace(replace(replace(operating_income_m,'$',''),',','.'),'B','') as numeric)*1000
       when operating_income_m like '%M%' then cast(replace(replace(replace(operating_income_m,'$',''),',','.'),'M','') as numeric)
       when operating_income_m like '%,%' then cast(replace(replace(operating_income_m,'$',''),',','.') as numeric)*1000
       when operating_income_m like '%K%' then cast(replace(replace(replace(operating_income_m,'$',''),',','.'),'K','') as numeric)/1000
       ELSE cast(replace(replace(operating_income_m,'$',''),',','.') as numeric)
       END as operating_income_m
from {} d
left join {} m
on lower(m.team_full_name) = rtrim(ltrim(lower(replace(replace(replace(replace(d.team_name,E'\n',''),'Bobcats','hornets'),'New Orleans Hornets','New Orleans Pelicans'),'New Jersey','Brooklyn')),' '),' ')
;""".format(clean_table, raw_table,metadata_table)

truncate_clean_table_stmt = "truncate table {};".format(clean_table)
truncate_raw_table_stmt = "truncate table {};".format(raw_table)
file_list = ['revenues.csv']
cur.execute(truncate_raw_table_stmt)
for file in file_list:
    try:
        logger.info('Read file: {}'.format(file))
        data = pd.read_csv('{}/{}'.format(file_folder, file), sep='|')
        data = data.dropna()
        team = data['team'][0]
        del_team_data_stmt = "delete from {} where team_name = E'{}';".format(raw_table, team)
        logger.info('Delete data for team : {}'.format(team))
        cur.execute(del_team_data_stmt)
        team_ins_stmt = '''insert into {} (
                        valuation_year,
                        rank, 
                        team_name,
                        current_value,
                        one_year_value_change,
                        debt_value,
                        revenue_m,
                        operating_income_m
                        ) values %s;'''.format(
            raw_table)
        logger.info('Insert data for team : {}'.format(team))
        extras.execute_values(cur, team_ins_stmt, data.values, page_size=1000)
        conn.commit()
        logger.info('Commited data insertion for team : {}'.format(team))
    except Exception as e:
        logger.error('Failed at file: {} with error:{}'.format(file, e))
logger.info('Truncate cleaned table : {}'.format(clean_table))
cur.execute(truncate_clean_table_stmt)
logger.info('insert data into cleaned table : {}'.format(clean_table))
cur.execute(clean_table_stmt)
conn.commit()
conn.close()