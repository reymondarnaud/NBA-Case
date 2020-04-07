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
raw_table = 'raw.yearly_attendance'
clean_table = 'cleaned.yearly_attendance'
metadata_table = 'metadata.teams_name'
file_folder = 'data'

create_table_stmt = """CREATE TABLE IF NOT EXISTS {}
                        (
                        record_id serial,
                        computed_year varchar(255)  null,
                        rank varchar(255)  null,
                        team_name varchar(255) not null,
                        home_gms varchar(255)  null,
                        home_total varchar(255)  null,
                        home_avg varchar(255)  null,
                        home_pct varchar(255)  null,
                        road_gms varchar(255)  null,
                        road_avg varchar(255)  null,
                        road_pct varchar(255)  null,
                        overall_gms varchar(255)  null,
                        overall_avg varchar(255)  null,
                        overall_pct varchar(255)  null,
                        created_at timestamp default now()
                        )
                        ;

                        CREATE TABLE IF NOT EXISTS {}
                        (
                        record_id serial,
                        computed_year varchar(255) not null,
                        team_name varchar(255) not null,
                        team_id integer not null,
                        home_gms integer  null,
                        home_total integer null,
                        home_avg numeric  null,
                        home_pct numeric  null,
                        road_gms integer  null,
                        road_avg numeric  null,
                        road_pct numeric  null,
                        overall_gms integer  null,
                        overall_avg numeric  null,
                        overall_pct numeric  null,
                        created_at timestamp default now()
                        )
                        ;""".format(raw_table, clean_table)
cur.execute(create_table_stmt)

clean_table_stmt = """insert into {} (
                        computed_year ,
                        team_name ,
                        team_id ,
                        home_gms ,
                        home_total ,
                        home_avg ,
                        home_pct ,
                        road_gms ,
                        road_avg ,
                        road_pct ,
                        overall_gms ,
                        overall_avg ,
                        overall_pct 
                        )
       select cast(computed_year as integer) as computed_year,
           coalesce(m.team_full_name,m2.team_full_name) as team_name,
           coalesce(m.team_id,m2.team_id) as team_id,
           cast(replace(home_gms,',','') as integer) as home_gms,
           cast(replace(home_total,',','') as integer) as home_total,
           cast(replace(home_avg,',','') as integer) as home_avg,
           case
               when home_pct = '--' then NULL
                else   cast(home_pct as numeric)
           end as home_pct,
           cast(replace(road_gms,',','') as integer) as road_gms,
           cast(replace(road_avg,',','') as integer) as road_avg,
            case
               when road_pct = '--' then NULL
                else   cast(road_pct as numeric)
           end as road_pct,
           cast(replace(overall_gms,',','') as integer) as overall_gms,
           cast(replace(overall_avg,',','') as integer) as overall_avg,
            case
               when overall_pct = '--' then NULL
                else   cast(overall_pct as numeric)
           end as overall_pct
from {} d
left join {} m
on lower(m.team_full_name) = rtrim(ltrim(lower(d.team_name),' '),' ')
left join {} m2
on lower(m2.team_name) = rtrim(ltrim(regexp_replace(lower(d.team_name), '^.* ', ''),' '),' ')
where coalesce(m.team_id,m2.team_id) is not null
;""".format(clean_table, raw_table,metadata_table,metadata_table)

truncate_clean_table_stmt = "truncate table {};".format(clean_table)
truncate_raw_table_stmt = "truncate table {};".format(raw_table)
file_list = ['attendance.csv']
cur.execute(truncate_raw_table_stmt)

for file in file_list:
    try:
        logger.info('Read file: {}'.format(file))
        data = pd.read_csv('{}/{}'.format(file_folder, file), sep='|')
        data = data.dropna()
        team = data['team'][0]
        del_team_data_stmt = "delete from {} where team_name = '{}';".format(raw_table, team)
        logger.info('Delete data for team : {}'.format(team))
        cur.execute(del_team_data_stmt)
        team_ins_stmt = '''insert into {} (
                        computed_year ,
                        rank ,
                        team_name ,
                        home_gms ,
                        home_total ,
                        home_avg ,
                        home_pct ,
                        road_gms ,
                        road_avg ,
                        road_pct ,
                        overall_gms ,
                        overall_avg ,
                        overall_pct 
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