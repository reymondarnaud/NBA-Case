from psycopg2 import extras
from psycopg2 import connect
import logging
import pandas as pd
import sys
import os
import time

logger = logging.getLogger()
console = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(levelname)s - %(name)s - line:%(lineno)s- %(asctime)s - %(message)s")
console.setFormatter(formatter)
logger.addHandler(console)
logger.setLevel(logging.INFO)

conn = connect(database='postgres',
               user='postgres',
               password='postgres',
               host='database-1.czujkruu3gwv.eu-west-1.rds.amazonaws.com',
               port=5432)

cur = conn.cursor()
raw_table = 'raw.trackalytics_teams_twitter'
clean_table = 'cleaned.trackalytics_teams_twitter'
clean_yearly_view = 'cleaned.yearly_teams_twitter_changes'
metadata_table = 'metadata.teams_name'
file_folder = 'data/trackalytics/twitter'



create_table_stmt = """CREATE TABLE IF NOT EXISTS {}
                        (
                        record_id serial,
                        account_name varchar(255) not null,
                        team_name varchar(255) not null,
                        count_date varchar(255) not null,
                        followers varchar(255) ,
                        following varchar(255) ,
                        tweets varchar(255) ,
                        lists varchar(255) ,
                        favourites varchar(255) ,
                        created_at timestamp default now()
                        )
                        ;

                        CREATE TABLE IF NOT EXISTS {}
                        (
                        record_id serial,
                        account_name varchar(255) not null,
                        team_name varchar(255) not null,
                        team_id integer not null,
                        count_date date not null,
                        total_followers integer not null,
                        change_followers integer not null,
                        total_following integer not null,
                        change_following integer not null,
                        total_tweets integer not null,
                        change_tweets integer not null,
                        total_lists integer not null,
                        change_lists integer not null,
                        total_favourites integer not null,
                        change_favourites integer not null,
                        created_at timestamp default now()
                        )
                        ;""".format(raw_table, clean_table)
cur.execute(create_table_stmt)

clean_table_stmt = """insert into {} (account_name,
                        team_name,
                        team_id,
                        count_date,
                        total_followers,
                        change_followers,
                        total_following,
                        change_following,
                        total_tweets,
                        change_tweets,
                        total_lists,
                        change_lists,
                        total_favourites,
                        change_favourites)
       select 
       account_name,
       coalesce(coalesce(m.team_full_name,m2.team_full_name),m3.team_full_name) as team_name,
       coalesce(coalesce(m.team_id,m2.team_id),m3.team_id) as team_id,
       to_date(lower(count_date),'month dd, yyyy') count_date,
       cast(replace(split_part(followers,'  ',1),',','') as integer) as total_followers,
       cast(translate(split_part(followers,'  ',2),'()+,','') as integer) as change_followers,
       cast(replace(split_part(following,'  ',1),',','') as integer) as total_following,
       cast(translate(split_part(following,'  ',2),'()+,','') as integer) as change_following,
       cast(replace(split_part(tweets,'  ',1),',','') as integer) as total_tweets,
       cast(translate(split_part(tweets,'  ',2),'()+,','') as integer) as change_tweets,
       cast(replace(split_part(lists,'  ',1),',','') as integer) as total_lists,
       cast(translate(split_part(lists,'  ',2),'()+,','') as integer) as change_lists,
       cast(replace(split_part(favourites,'  ',1),',','') as integer) as total_favourites,
       cast(translate(split_part(favourites,'  ',2),'()+,','') as integer) as change_favourites
from {} d
left join {} m
on lower(m.team_full_name) = rtrim(ltrim(lower(d.team_name),' '),' ')
left join {} m2
on lower(m2.team_name) = rtrim(ltrim(regexp_replace(lower(d.team_name), '^.* ', ''),' '),' ')
left join metadata.teams_name m3
on lower(m3.team_name) = rtrim(ltrim(replace(lower(d.account_name),'@',''),' '),' ');""".format(clean_table, raw_table,
                                                                                           metadata_table,
                                                                                           metadata_table,
                                                                                           metadata_table)

truncate_clean_table_stmt = "truncate table {};".format(clean_table)

file_list = os.listdir(file_folder)

for file in file_list:
    try:
        logger.info('Read file: {}'.format(file))
        data = pd.read_csv('{}/{}'.format(file_folder, file), sep='|')
        team = data['team'][0]
        del_team_data_stmt = "delete from {} where team_name = '{}';".format(raw_table, team)
        logger.info('Delete data for team : {}'.format(team))
        cur.execute(del_team_data_stmt)
        team_ins_stmt = 'insert into {} (account_name,team_name,count_date,followers,following,tweets,lists,favourites) values %s;'.format(raw_table)
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

create_yearly_view = """CREATE OR REPLACE VIEW {} AS                      
                    with temp as
                             (
                                 select team_name,
                                        team_id,
                                        date_part('year', count_date) date_year,
                                        sum(change_followers)             yearly_followers_change,
                                        sum(change_following)     yearly_following_change,
                                        sum(change_tweets)         yearly_tweets_change,
                                        sum(change_lists)         yearly_lists_change,
                                        sum(change_favourites)         yearly_favourites_change
                                 from cleaned.trackalytics_teams_twitter
                                 group by team_name, team_id, date_part('year', count_date)
                                 --order by team_name, team_id, date_part('year', count_date) desc
                             )
                    select temp.team_id, cast(date_year as varchar(255)), yearly_followers_change, yearly_following_change, yearly_tweets_change,yearly_lists_change,yearly_favourites_change, count_date, total_followers, total_following,total_lists,total_favourites,total_tweets,relative_followers_change
                    from temp
                    left join (
                        select tt.team_id ,
                         count_date, 
                         total_followers, 
                         total_following,
                         total_lists,
                         total_favourites,
                         total_tweets,
                         cast(total_followers as numeric)/lag(total_followers,-1) over (partition by tt.team_id order by count_date desc) -1 relative_followers_change
                        from cleaned.trackalytics_teams_twitter tt
                        inner join
                        (select team_id, max(count_date) as max_date
                        from cleaned.trackalytics_teams_twitter
                        group by team_id, date_part('year',count_date)
                        ) dates
                        on tt.team_id = dates.team_id and tt.count_date = max_date
                        ) total
                    on temp.team_id = total.team_id and temp.date_year =  date_part('year',count_date);""".format(clean_yearly_view)
cur.execute(create_yearly_view)
conn.commit()
conn.close()