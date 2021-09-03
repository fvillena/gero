import psycopg2
import numpy as np
import pandas as pd
import re
import json

GROUPS = {
    "G001":"GERO Sin grupo",
    "G011":"GERO Elegible",
    "G012":"GERO No elegible",
    "G013":"GERO Elegible Excluído (evaluadoras)",
    "G014":"GERO Elegible Excluido (participante)",
    "G021":"GERO Incluido",
    "G022":"GERO Excluido PV",
    "G031":"GERO Cohorte T1",
    "G033":"GERO Cohorte T2",
    "G034":"GERO Cohorte T3",
    "G032":"GERO Excluido MD",
    "G041":"GERO Seguimiento",
    "G042":"GERO Desertor",
    "G051":"GERO Genética",
    "C001":"Control Sano T1",
    "C003":"Control Sano T2",
    "C004":"Control Sano T3",
    "C101":"Control Paciente AD",
    "C102":"Control Paciente EP",
    "C103":"Control Paciente UAI",
    "C104":"Control Paciente Excluído",
    "C105":"Control Paciente DFT",
    "C106":"Control Paciente APP",
    "C199":"Control Paciente Otro",
    "$999":"Basura (NO SE EXPORTA)"
}

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

def partaker_caption_from_uuid(connection,uuid):
    cursor = connection.cursor()
    query = f"SELECT information ->> 'Caption' FROM cohorte_v2 WHERE uuid = '{uuid}'"
    cursor.execute(query)
    result = cursor.fetchall()
    caption = result[0][0]
    return caption

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
    cohorte_v2.uuid AS survey_uuid, 
    created, 
    information ->> 'PartakerID' AS partaker_uuid, 
    partaker_caption, 
    information ->> 'InstrumentID' AS instrument_uuid, 
    information ->> 'Caption' AS survey_name, 
    information ->> 'Description' AS survey_description, 
    information ->> 'BookletID' AS booklet_id, 
    booklet_caption, 
    information ->> 'Instant' AS instant, 
    information ->> 'Interviewer' AS interviewer, 
    information ->> 'SurveyDate' AS survey_date, 
    information ->> 'Status' AS status, 
    information ->> 'Data' AS data 
    FROM 
    public.cohorte_v2 
    LEFT JOIN (
        SELECT 
        uuid, 
        information ->> 'Caption' AS partaker_caption 
        FROM 
        public.cohorte_v2 
        WHERE 
        classname = 'Partaker'
    ) pa ON information ->> 'PartakerID' = pa.uuid 
    LEFT JOIN (
        SELECT 
        uuid, 
        information ->> 'Caption' AS booklet_caption 
        FROM 
        public.cohorte_v2 
        WHERE 
        classname = 'Booklet'
    ) bo ON information ->> 'BookletID' = bo.uuid 
    WHERE 
    classname = 'Survey' 
    AND deleted = {deleted} 
    AND super IN (
        {str(partaker_uuids).replace("[","(").replace("]",")")}
    );
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

def get_surveys(conn,instrument_uuid,deleted=False):
    deleted = "true" if deleted else "false"
    cursor = conn.cursor()
    query = f"""
    SELECT 
    cohorte_v2.uuid AS survey_uuid, 
    created, 
    information ->> 'PartakerID' AS partaker_uuid, 
    partaker_caption,
    partaker_group,
    information ->> 'InstrumentID' AS instrument_uuid, 
    information ->> 'Caption' AS survey_name, 
    information ->> 'Description' AS survey_description, 
    information ->> 'BookletID' AS booklet_id, 
    booklet_caption, 
    information ->> 'Instant' AS instant, 
    information ->> 'Interviewer' AS interviewer, 
    information ->> 'SurveyDate' AS survey_date, 
    information ->> 'Status' AS status, 
    information ->> 'Data' AS data,
    information
    FROM 
    public.cohorte_v2 
    LEFT JOIN (
        SELECT 
        uuid, 
        information ->> 'Caption' AS partaker_caption,
        information ->> 'Group' AS partaker_group,
        deleted AS partaker_deleted
        FROM 
        public.cohorte_v2 
        WHERE 
        classname = 'Partaker'
    ) pa ON information ->> 'PartakerID' = pa.uuid 
    LEFT JOIN (
        SELECT 
        uuid, 
        information ->> 'Caption' AS booklet_caption 
        FROM 
        public.cohorte_v2 
        WHERE 
        classname = 'Booklet'
    ) bo ON information ->> 'BookletID' = bo.uuid 
    WHERE 
    classname = 'Survey' 
        AND deleted = {deleted}
        AND partaker_deleted = false
        AND information ->> 'InstrumentID' = '{str(instrument_uuid)}';
    """
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    surveys = []
    for survey in result:
        surveys.append(dict(zip(columns,survey)))
    for i,survey in enumerate(surveys):
        data = json.loads(survey["data"]) if survey["data"] else {}
        surveys[i] = {**surveys[i], **data}
        surveys[i].pop('data', None)
    return surveys

def justify_na(x):
    na_columns = x.index[x.isna()]
    if "Justification" in x.information:
        for c in na_columns:
            if c in x.information["Justification"]:
                x[c] = x.information["Justification"][c]
        return x
    else:
        return x

def list_to_element(l):
    for e in l:
        if e != None:
            return e

def cell_parser(c):
    if isinstance(c,list):
        return list_to_element(c)
    else:
        return c

def get_surveys_df(conn, istrument_uuid):
    surveys = get_surveys(conn,istrument_uuid)
    surveys = pd.DataFrame(surveys)
    surveys = surveys.applymap(cell_parser)
    surveys = surveys.apply(justify_na,axis=1)
    surveys = surveys[surveys.information.apply(lambda x: "Data" in x)]
    surveys["partaker_group"] = surveys.partaker_group.apply(lambda x: GROUPS[x] if x in GROUPS else x)
    surveys["created"] = pd.to_datetime(surveys["created"],unit='ms')
    return surveys

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

def get_instruments_from_booklet(conn,booklet_uuid):
    cursor = conn.cursor()
    query = f"""
    SELECT 
	    information 
    FROM 
        public.cohorte_v2
    WHERE 
        classname = 'Booklet'
        AND uuid = '{booklet_uuid}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    assert(len(result) == 1)
    information = result[0][0]
    return information["References"]

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