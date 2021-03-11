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
def uuid_from_caption(caption,connection,deleted=False):
    deleted = "true" if deleted else "false"
    cursor = connection.cursor()
    query = f"SELECT uuid FROM cohorte_v2 WHERE classname = 'Partaker'  AND information ->> 'Caption' = '{caption}' AND deleted = {deleted}"
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 1:
        raise Exception(f"partaker {caption} has more than one uuid")
    if len(result) == 0:
        raise Exception(f"partaker {caption} has no uuid")
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

def get_partaker_surveys(conn,partaker_uuids,deleted=False):
    deleted = "true" if deleted else "false"
    cursor = conn.cursor()
    query = f"""
    SELECT
        uuid AS survey_uuid,
        created,
        information ->> 'PartakerID' AS partaker_uuid,
        information ->> 'InstrumentID' AS instrument_uuid,
        information ->> 'Caption' AS survey_name,
        information ->> 'Description' AS survey_description,
        information ->> 'BookletID' AS booklet_id,
        information ->> 'Instant' AS instant,
        information ->> 'Data' AS data
    FROM public.cohorte_v2
    WHERE classname = 'Survey'
        AND deleted = {deleted}
        AND super IN {str(partaker_uuids).replace("[","(").replace("]",")")};
    """
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    surveys = []
    for survey in result:
        surveys.append(dict(zip(columns,survey)))
    return surveys

def fuse_partakers(conn,partaker_uuids,partaker_caption):
    surveys = pd.DataFrame(get_partaker_surveys(conn,partaker_uuids))
    surveys_to_delete = surveys[surveys.duplicated(subset=['instrument_uuid','data'])]
    surveys_to_move = surveys.drop_duplicates(subset=['instrument_uuid','data'])
    if len(surveys_to_move) > 0:
        for survey in surveys_to_move.survey_uuid:
            move_survey_to_partaker(conn,survey,partaker_uuids[0])
    if len(surveys_to_delete.survey_uuid) > 0:
        for survey in surveys_to_delete.survey_uuid:
            delete_object(conn,survey)
    if len(partaker_uuids) > 1:
        for partaker_id in partaker_uuids[1:]:
            delete_object(conn,partaker_id)
    change_partaker_caption(conn,partaker_uuids[0],partaker_caption)

def change_partaker_caption(conn,partaker_uuid,new_caption):
    cur = conn.cursor()
    query = f""" UPDATE public.cohorte_v2
    SET information = information::jsonb || '{{"Caption":"{new_caption}"}}'
    WHERE uuid = '{partaker_uuid}'"""
    cur.execute(query)
    conn.commit()
    cur.close()

def delete_object(conn,object_uuid):
    cur = conn.cursor()
    query = f""" UPDATE public.cohorte_v2
                    SET deleted = true
                    WHERE uuid = '{object_uuid}'"""
    cur.execute(query)
    conn.commit()
    cur.close()

def move_survey_to_partaker(conn,survey_uuid,partaker_uuid):
    cur = conn.cursor()
    query = f""" UPDATE public.cohorte_v2
            SET super = '{partaker_uuid}'
            WHERE uuid = '{survey_uuid}'"""
    cur.execute(query)
    query = f""" UPDATE public.cohorte_v2
            SET information = information::jsonb || '{{"PartakerID":"{partaker_uuid}"}}'
            WHERE uuid = '{survey_uuid}'"""
    cur.execute(query)
    conn.commit()
    cur.close()

def get_objects(conn,object_uuids):
    cur = conn.cursor()
    query = f"""
    SELECT
        *
    FROM 
        public.cohorte_v2
    WHERE
        uuid in {str(object_uuids).replace("[","(").replace("]",")")}
    """
    cur.execute(query)
    results = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    os = []
    for result in results:
        o = dict(zip(columns,result))
        os.append(o)
    return os

def get_partaker_booklets(conn,partaker_uuid):
    surveys = get_partaker_surveys(conn,[partaker_uuid])
    booklets = set([(survey["booklet_id"],partaker_uuid,survey["created"]) for survey in surveys])
    return booklets

def get_surveys_from_booklet(conn,booklet):
    cursor = conn.cursor()
    query = f"""
    SELECT
        uuid
    FROM public.cohorte_v2
    WHERE classname = 'Survey'
        AND deleted = 'false'
        AND information ->> 'BookletID' = '{booklet[0]}'
        AND created = {booklet[2]}
        AND super = '{booklet[1]}';
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return [survey[0] for survey in result]

def change_object_creation_time(conn,object_uuid,creation_time):
    cur = conn.cursor()
    query = f""" UPDATE public.cohorte_v2
    SET created = {creation_time}
    WHERE uuid = '{object_uuid}'"""
    cur.execute(query)
    conn.commit()
    cur.close()

def change_survey_instant(conn,object_uuid,instant):
    cur = conn.cursor()
    query = f""" UPDATE public.cohorte_v2
    SET information = information::jsonb || '{{"Instant":"{instant}"}}'
    WHERE uuid = '{object_uuid}'"""
    cur.execute(query)
    conn.commit()
    cur.close()