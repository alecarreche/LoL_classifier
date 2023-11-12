import psycopg2
import requests
import pandas as pd
import json

from sqlalchemy import create_engine

if __name__ == '__main__':
    conn = psycopg2.connect(
        database='riot',
        user='admin',
        password='admin',
        host='db',
        port='5432'
    )

    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)

    # fetch champion data from DDragon API
    champions_url = 'https://ddragon.leagueoflegends.com/cdn/13.22.1/data/en_US/champion.json'
    res = requests.get(champions_url)
    res_dict = json.loads(res.text)
    champion_df_raw = pd.DataFrame(res_dict['data']).T
    champion_df = champion_df_raw.set_index('key', drop=True)

    # convert JSON cols to multiple columns
    info_unpacked = champion_df.apply(lambda x: pd.Series(x['info']), axis=1, result_type='expand')
    stats_unpacked = champion_df.apply(lambda x: pd.Series(x['stats']), axis=1, result_type='expand')
    champion_df = champion_df \
                    .join(info_unpacked) \
                    .join(stats_unpacked)

    # load into db
    champion_df.drop(
            columns=['info', 'stats', 'image']
        ) \
        .to_sql(
            name='dim_champions',
            con=engine,
            if_exists='replace',
            index=True,
            index_label='champion_id',
            method='multi'
        )