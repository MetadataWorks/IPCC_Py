# load packages
import copy
import datetime
import json
# import numpy as np
import os
import pandas as pd
import requests
import mysql.connector
import db_config

CWD = os.getcwd()
DATA_PATH = os.path.join(CWD, 'lcl_data', 'IPCC_dmd_extract')


def main():
    print_timestamp(f"start '{__file__}'")

    db_schema = db_config.DB_SCHEMA
    db_logon = db_config.DB_CONNECTION
    db_connection = connect_to_db(db_logon)

    ftl = ftl_py_id(db_schema, db_connection)
    dmd = read_dmd(db_schema, db_connection, ftl)
    excel_out = prepare_excel_output(dmd)
    fname = os.path.join(DATA_PATH, f"IPCC Descriptive Metadata {datetime.datetime.now().strftime('%Y-%m-%d')} {db_config.__db_id__}.xlsx")
    write_excel(fname, excel_out)
    print(f" '{fname}'")

    db_connection.close()
    print_timestamp(f"done")
    return


def prepare_excel_output(dmd):
    org_dlists = {}
    for org_name, org_dms in dmd.items():
        if not org_dlists.get(org_name, None):
            org_dlists[org_name] = {}
        for dm_id, dm_dict in org_dms.items():
            for kv_key, kv_value in dm_dict.items():
                if not org_dlists[org_name].get(kv_key, None):
                    org_dlists[org_name][kv_key] = []
                if isinstance(kv_value, datetime.date):
                    org_dlists[org_name][kv_key].append(kv_value.strftime('%d/%m/%Y'))
                elif isinstance(kv_value, int) or isinstance(kv_value, float):
                    org_dlists[org_name][kv_key].append(kv_value)
                else:
                    org_dlists[org_name][kv_key].append(str(kv_value))
    excel_out = {}
    okeys = sorted(list(org_dlists.keys()))
    for org_name in okeys:
        excel_out[org_name] = pd.DataFrame(org_dlists[org_name])
    return excel_out


def read_dmd(schema, connection, first_this_last):
    out_structure = {'persistent_id': 'persid',
                     'properties/summary/title': 'string',
                     'properties/summary/abstract': 'string',
                     'properties/summary/contactPoint': 'string',
                     'properties/summary/keywords': 'list',
                     'properties/summary/doiName': 'string',
                     'properties/summary/alternateIdentifier': 'list',
                     'properties/summary/publicationDate': 'date',
                     'properties/summary/publisher/identifier': 'string',
                     'properties/summary/publisher/name': 'string',
                     'properties/summary/publisher/logo': 'string',
                     'properties/summary/publisher/description': 'string',
                     'properties/summary/publisher/contactPoint': 'string',
                     'properties/documentation/description': 'string',
                     'properties/documentation/associatedMedia': 'list',
                     'properties/documentation/isPartOf': 'list',
                     'properties/coverage/spatialCoverage': 'string',
                     'properties/coverage/spatialAggregation': 'string',
                     'properties/coverage/spatialResolution': 'string',
                     'properties/coverage/startDate': 'date',
                     'properties/coverage/endDate': 'date',
                     'properties/coverage/temporalResolution': 'string',
                     'properties/coverage/geographicBoundingBox/lowerLeftLatitude': 'string',
                     'properties/coverage/geographicBoundingBox/lowerLeftLongitude': 'string',
                     'properties/coverage/geographicBoundingBox/upperRightLatitude': 'string',
                     'properties/coverage/geographicBoundingBox/upperRightLongitude': 'string',
                     'properties/provenance/purpose': 'string',
                     'properties/provenance/source': 'string',
                     'properties/accessibility/usage/license': 'string',
                     'properties/accessibility/usage/resourceCreator': 'list',
                     'properties/accessibility/usage/investigations': 'list',
                     'properties/accessibility/usage/isReferencedBy': 'list',
                     'properties/accessibility/usage/references': 'list',
                     'properties/accessibility/access/accessUrl': 'string',
                     'properties/accessibility/access/accessService': 'string',
                     'properties/accessibility/access/jurisdiction': 'list',
                     'properties/accessibility/access/language': 'list',
                     'properties/accessibility/access/format': 'list',
                     'properties/enrichmentAndLinkage/qualifiedRelation': 'list',
                     'properties/enrichmentAndLinkage/tools': 'list',
                     'structuralMetadata': 'smd', }
    dm_ids = set()
    for dm_id, dm_data in first_this_last['dms'].items():
        if dm_data['live']:
            dm_ids.add(dm_data['live'])
        if dm_data['draft']:
            dm_ids.add(dm_data['draft'])
    dm_ids = [str(idx) for idx in dm_ids]

    list_of_dicts = {}
    sql_statement = f"SELECT element_id AS dm_id, name AS kv_key, extension_value AS kv_value FROM {schema}.extension_value " \
                    f"WHERE element_id IN ({', '.join(dm_ids)}) " \
                    f"ORDER BY element_id, name;"
    sql_result = sql_select_to_json(connection, sql_statement)
    for sr in sql_result:
        try:
            ftl = first_this_last['dms'][sr['dm_id']]
            org_name = ftl['dmd']['org_name']
            if not list_of_dicts.get(org_name, None):
                list_of_dicts[org_name] = {}
            if not list_of_dicts[org_name].get(sr['dm_id'], None):
                list_of_dicts[org_name][sr['dm_id']] = {out_key: '' for out_key in out_structure}
                list_of_dicts[org_name][sr['dm_id']]['persistent_id'] = f"id{ftl['first']}@{ftl['dmd']['version']}({ftl['dmd']['status']})"
            if 'date' == out_structure.get(sr['kv_key'], None):
                list_of_dicts[org_name][sr['dm_id']][sr['kv_key']] = datetime.datetime.strptime(sr['kv_value'], '%Y-%m-%d').date()
            elif 'list' == out_structure.get(sr['kv_key'], None):
                if sr['kv_value'].startswith('[') and sr['kv_value'].endswith(']'):
                    list_of_dicts[org_name][sr['dm_id']][sr['kv_key']] = json.loads(sr['kv_value'])
                else:
                    list_of_dicts[org_name][sr['dm_id']][sr['kv_key']] = [sr['kv_value']]
            elif 'string' == out_structure.get(sr['kv_key'], None):
                list_of_dicts[org_name][sr['dm_id']][sr['kv_key']] = sr['kv_value']
            elif 'smd' == out_structure.get(sr['kv_key'], None):
                list_of_dicts[org_name][sr['dm_id']][sr['kv_key']] = 'structural metadata'
        except Exception as e:
            prorates = str(e)

    return list_of_dicts


def print_timestamp(out_text=''):
    now = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    print(f"{now} {out_text}")
    return


def sql_select_to_pd(db, sql_statement):
    sql_select = None
    try:
        sql_select = pd.read_sql(sql_statement, con=db)
    except Exception as e:
        print_timestamp(f"SQL select error {e}")

    return sql_select


def sql_select_to_json(db, sql_statement):
    json_select = None
    df_select = sql_select_to_pd(db, sql_statement)
    try:
        json_select = df_select.to_dict(orient='records')
    except Exception as e:
        print_timestamp(f"{e}")
    return json_select


def ftl_py_id(db_schema, db_connection):
    print_timestamp(f"FirstThisLast the Python way")

    # from the org setup
    sql_statement = f"SELECT DISTINCT ugroup.id AS org_id, ugroup.name AS org_name, data_model.id AS dm_id " \
                    f"FROM {db_schema}.ugroup JOIN {db_schema}.role ON ugroup.acl_role_id = role.id " \
                    f"JOIN {db_schema}.acl_sid ON role.authority = acl_sid.sid " \
                    f"JOIN {db_schema}.acl_entry ON acl_sid.id = acl_entry.sid " \
                    f"JOIN {db_schema}.acl_object_identity ON acl_entry.acl_object_identity = acl_object_identity.id " \
                    f"JOIN {db_schema}.data_model ON data_model.id = acl_object_identity.object_id_identity " \
                    f"WHERE acl_entry.mask = 16 AND data_model.id > 25 ORDER BY ugroup.id, data_model.id;"
    sql_result = sql_select_to_json(db_connection, sql_statement)
    org_group = {}
    for sr in sql_result:
        org_group[sr['dm_id']] = {'org_id': sr['org_id'], 'org_name': sr['org_name']}

    sql_statement = f"SELECT DISTINCT data_model.id AS dm_id, catalogue_element.name AS dm_name, " \
                    f"data_model.semantic_version AS dm_version, catalogue_element.status AS dm_status, " \
                    f"COALESCE(catalogue_element.latest_version_id, catalogue_element.id) AS first_dm, " \
                    f"catalogue_element.date_created AS date_created, " \
                    f"catalogue_element.last_updated AS last_updated " \
                    f"FROM {db_schema}.data_model INNER JOIN {db_schema}.catalogue_element " \
                    f"ON data_model.id = catalogue_element.id WHERE data_model.id > 25 ORDER BY data_model.id;"
    sql_result = sql_select_to_json(db_connection, sql_statement)

    if len(sql_result) < 1:
        return {}

    data_models, firsts = {}, {}
    for sr in sql_result:
        first = sr['first_dm']
        if not firsts.get(first, None):
            firsts[first] = []
        firsts[first].append(sr['dm_id'])
        dm = {'this': None, 'first': None, 'live': None, 'draft': None,
              'prev': None, 'next': None, 'last': None, 'dmd': None, 'sequence': None}
        dm['this'] = sr['dm_id']
        dm['first'] = sr['first_dm']
        dm['dmd'] = {'name': sr['dm_name'],
                     'version': sr['dm_version'],
                     'status': sr['dm_status'],
                     'org_id': org_group[sr['dm_id']]['org_id'],
                     'org_name': org_group[sr['dm_id']]['org_name'],
                     'date_created': sr['date_created'].strftime('%Y-%m-%d'),
                     'last_updated': sr['last_updated'].strftime('%Y-%m-%d'),
                     }
        data_models[sr['dm_id']] = dm
        if 'FINALIZED' == sr['dm_status']:
            data_models[sr['first_dm']]['live'] = sr['dm_id']
        if 'DRAFT' == sr['dm_status']:
            data_models[sr['first_dm']]['draft'] = sr['dm_id']

    for first, seq in firsts.items():
        live, draft = data_models[first]['live'], data_models[first]['draft']
        n = len(seq)
        for idx in range(n):
            this = seq[idx]
            data_models[this]['live'] = live
            data_models[this]['draft'] = draft
            data_models[this]['sequence'] = seq
            data_models[this]['last'] = seq[-1]
            if idx > 0:
                data_models[this]['prev'] = seq[(idx - 1)]
            if idx < n - 1:
                data_models[this]['next'] = seq[(idx + 1)]

    first_ids = list(firsts.keys())
    first_ids.sort()

    print_timestamp(f"FirstThisLast found {len(data_models.keys())} dataset(s)")
    print()
    return {'dms': data_models, 'first': first_ids}


def connect_to_db(db_logon):
    db = None
    try:
        db = mysql.connector.connect(**db_logon)
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
            return None, None
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
            print()
            return None, None
        else:
            print(err)
            print()
            return None, None

    return db


def write_excel(fname, worksheets, idx=False):
    with pd.ExcelWriter(fname) as writer:
        for sheetname, df_worksheet in worksheets.items():
            df_worksheet.to_excel(writer, sheet_name=sheetname, index=idx)
    return


def write_json(data, filename, indent=2):
    with open(filename, 'w') as jsonfile:
        json.dump(data, jsonfile, indent=indent)
    return


def read_json(json_uri):
    if os.path.isfile(json_uri):
        with open(json_uri, 'r') as json_file:
            return json.load(json_file)
    elif json_uri.startswith('http'):
        return requests.get(json_uri).json()
    else:
        raise Exception


def read_excel_pd(fname, fill_na=False):
    excel_reader = pd.ExcelFile(fname)
    work_sheets = excel_reader.sheet_names
    excel = {}

    for sheet in work_sheets:
        df_sheet = pd.DataFrame()
        df_sheet = pd.read_excel(excel_reader, sheet_name=sheet)
        if fill_na is not None:
            df_sheet.fillna('', inplace=True)
        excel[sheet] = copy.deepcopy(df_sheet)

    return excel


def read_excel_json(fname, fill_na=None):
    excel_reader = pd.ExcelFile(fname)
    work_sheets = excel_reader.sheet_names

    excel = {}
    for sheet in work_sheets:
        df_sheet = pd.DataFrame()
        df_sheet = pd.read_excel(excel_reader, sheet_name=sheet)
        if fill_na is not None:
            df_sheet.fillna(fill_na, inplace=True)
        excel[sheet] = df_sheet.to_dict(orient='records')

    return excel


if '__main__' == __name__:
    main()
