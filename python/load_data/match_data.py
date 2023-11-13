import psycopg2
import requests
import pandas as pd
import json
import os
import time
import logging

from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('API_KEY')
logging.basicConfig(level=logging.INFO)

# api functions
def get_match_ids(puuid):
    logging.info(f'Getting match ids for puuid: {puuid}')
    url = f'https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids'
    res = requests.get(
        url=url,
        params={
            'startTime': '1696888052',
            'endTime': '1699570052',
            'type': 'ranked',
            'start': '0',
            'count': '100'
        },
        headers={'X-Riot-Token': API_KEY}
    )
    match_ids = res.json()

    return match_ids

def get_match_data(match_id):
    logging.info(f'Getting match data for match: {match_id}')
    url = f'https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}'
    res = requests.get(
        url=url,
        headers={'X-Riot-Token': API_KEY}
    )
    match_df = pd.DataFrame(res.json()['info']['participants'])
    match_df['matchID'] = match_id
    match_df = match_df[[
        'matchID',
        'puuid',
        'championId',
        'teamPosition',
        'win'
    ]].copy()
    match_df.columns = [x.lower() for x in match_df.columns]

    return match_df

def get_champion_mastery(puuid, championID):
    logging.info(f'Getting champion {championID} mastery data for {puuid}')
    url = f'https://na1.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}/by-champion/{championID}'
    res = requests.get(
        url=url,
        headers={'X-Riot-Token': API_KEY}
    )
    return res.json()

# db functions
def upload_match_ids_to_queue(match_ids, conn):
    logging.info(f'Loading match ids to db')
    cursor = conn.cursor()
    for m in match_ids:
        sql = f"""INSERT INTO matchID_queue (matchid) VALUES ('{m}') ON CONFLICT DO NOTHING;"""
        cursor.execute(sql)
    conn.commit()
    cursor.close()

    return

def delete_match_id_from_queue(match_id, conn):
    logging.info(f'Deleting match id {match_id} from db')
    cursor = conn.cursor()
    sql = f"""DELETE FROM matchID_queue WHERE matchid = '{match_id}';"""
    cursor.execute(sql)
    conn.commit()
    cursor.close()

    return

def upload_champion_mastery(mastery_dict, conn):
    logging.info(f'Loading mastery data to db')
    puuid = mastery_dict['puuid']
    championid = mastery_dict['championId']
    points = mastery_dict['championPoints']

    cursor = conn.cursor()
    sql = f"""
        INSERT INTO dim_champion_mastery(puuid, championId, championPoints) 
        VALUES ('{puuid}', {championid}, {points})
        ON CONFLICT DO NOTHING
        ;
    """
    cursor.execute(sql)
    conn.commit()
    cursor.close()

def load_matches_from_root_nodes(conn):
    root_names = ['4l3c4', 'Riot%20GalaxySmash', 'weirdosuper2']
    puuids = []

    for n in root_names:
        url = f'https://na1.api.riotgames.com/lol/summoner/v4/summoners/by-name/{n}'
        res = requests.get(
            url=url,
            headers={'X-Riot-Token': API_KEY}
        )
        res_json = json.loads(res.text)
        puuids.append(res_json['puuid'])

    for p in puuids:
        match_ids = get_match_ids(p)
        upload_match_ids_to_queue(match_ids, conn)

    return

def matches_in_queue(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM matchid_queue;')
    rc = cursor.fetchone()[0]
    cursor.close()

    logging.info(f'{rc} matches in queue.')

    return rc

def batch_query(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT matchid FROM matchid_queue ORDER BY RANDOM() LIMIT 4;')
    batch = cursor.fetchall()[0]
    cursor.close()

    return batch

if __name__ == '__main__':
    conn = psycopg2.connect(
        database='riot',
        user='admin',
        password='admin',
        host='db',
        port='5432'
    )

    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)

    queue_cnt = matches_in_queue(conn)
    if queue_cnt == 0:
        logging.info('Match queue empty. Regnerating from root nodes.')
        load_matches_from_root_nodes(conn)

    while True:
        logging.info('Beginning batch')
        start = time.time()

        batch = batch_query(conn)

        for m in batch:
            logging.info(f'Processing match: {m}')

            # get match info (1 request)
            match_df = get_match_data(m)
            
            # upload required match data
            match_df.to_sql(
                name='fct_matches',
                con=engine,
                if_exists='append',
                index=False,
                method='multi'
            )

            # get mastery info (10 requests)
            for puuid, cid in match_df[['puuid', 'championid']].values:
                mastery_dict = get_champion_mastery(puuid, cid)
            
                # upload to mastery table
                upload_champion_mastery(mastery_dict, conn)

            # get matches from players (10 requests)
            for puuid in match_df['puuid'].values:
                # get matches from puuid
                match_ids = get_match_ids(puuid)

                # add to match id queue
                upload_match_ids_to_queue(match_ids, conn)

            # remove match id from queue
            delete_match_id_from_queue(m, conn)

            break

        logging.info('Completed batch')
        elapsed = time.time() - start

        if elapsed < 120:
            logging.info(f'Sleeping for {120 - elapsed} seconds to avoid rate limits')
            time.sleep(120 - elapsed)

        break

    conn.close()