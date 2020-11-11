import psycopg2
import numpy as np
import pandas as pd
import re

def create_connection(user,password,host,port,database):
    try:
        connection = psycopg2.connect(user = user,
                                        password = password,
                                        host = host,
                                        port = port,
                                        database = database)
        return connection
    except (Exception, psycopg2.Error) as error :
        print ("Error", error)
def uuid_from_caption(caption,connection):
    cursor = connection.cursor()
    query = f"SELECT uuid FROM cohorte_v2 WHERE classname = 'Partaker'  AND information ->> 'Caption' = '{caption}'"
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 1:
        raise Exception
    if len(result) == 0:
        raise Exception
    uuid = result[0][0]
    return uuid

def extract_code(x):
    try:
        m = re.search(r'([A-Za-z0-9]+_[A-Za-z0-9]+_?[A-Za-z0-9]*)',x)
    except:
        m = False
    if m:
        return m.group(1).lower()
    else:
        return np.nan

def get_duplicated_partakers(conn):
    cursor = conn.cursor()
    query = """
    SELECT
    uuid AS partaker_uuid,
    information ->> 'Caption'  AS caption
    FROM public.cohorte_v2
    WHERE classname = 'Partaker'
        AND deleted = 'false';

    """
    cursor.execute(query)
    result = cursor.fetchall()
    partakers = pd.DataFrame(result,columns=["partaker_uuid","caption"])
    partakers['partaker_id'] = partakers.caption.apply(extract_code)
    duplicated_partakers = partakers[partakers.duplicated(subset='partaker_id', keep=False)].sort_values(by=['caption']).dropna().reset_index(drop=True)
    duplicated_partakers_dict = duplicated_partakers.groupby('partaker_id')['partaker_uuid'].apply(list).to_dict()
    return duplicated_partakers_dict

def get_partaker_surveys(conn,partaker_uuids):
    cursor = conn.cursor()
    query = f"""
    SELECT
    uuid AS survey_uuid,
    information ->> 'PartakerID' AS partaker_uuid,
    information ->> 'InstrumentID' AS instrument_uuid,
    information ->> 'Caption' AS survey_name,
    information ->> 'Description' AS survey_description,
    information ->> 'Data' AS data
    FROM public.cohorte_v2
    WHERE classname = 'Survey'
        AND deleted = 'false'
        AND super IN {str(tuple(partaker_uuids))};

    """
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    surveys = pd.DataFrame(result,columns=columns).fillna(value=np.nan)
    return surveys

def fuse_partakers(conn,partaker_uuids,partaker_caption):
    surveys = get_partaker_surveys(conn,partaker_uuids)
    surveys_to_delete = surveys[surveys.duplicated(subset=['instrument_uuid','data'])]
    surveys_to_move = surveys.drop_duplicates(subset=['instrument_uuid','data'])
    if len(surveys_to_move) > 0:
        for survey in surveys_to_move.survey_uuid:
            cur = conn.cursor()
            query = f""" UPDATE public.cohorte_v2
                    SET super = '{partaker_uuids[0]}'
                    WHERE uuid = '{survey}'"""
            cur.execute(query)
            query = f""" UPDATE public.cohorte_v2
                    SET information = information::jsonb || '{{"PartakerID":"{partaker_uuids[0]}"}}'
                    WHERE uuid = '{survey}'"""
            cur.execute(query)
    if len(surveys_to_delete.survey_uuid) > 0:
        for survey in surveys_to_delete.survey_uuid:
            query = f""" UPDATE public.cohorte_v2
                    SET deleted = true
                    WHERE uuid = '{survey}'"""
            cur.execute(query)
    if len(partaker_uuids) > 1:
        for partaker_id in partaker_uuids[1:]:
            query = f""" UPDATE public.cohorte_v2
                    SET deleted = true
                    WHERE uuid = '{partaker_id}'"""
            cur.execute(query)
    change_partaker_caption(conn,partaker_uuids[0],partaker_caption)
    conn.commit()
    cur.close()

def change_partaker_caption(conn,partaker_uuid,new_caption):
    cur = conn.cursor()
    query = f""" UPDATE public.cohorte_v2
    SET information = information::jsonb || '{{"Caption":"{new_caption}"}}'
    WHERE uuid = '{partaker_uuid}'"""
    cur.execute(query)
    conn.commit()
    cur.close()