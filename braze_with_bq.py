import traceback
from datetime import datetime, timedelta, date

import os
import re
import json
import logging
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account


with open('bq_schemas.json') as f:
    bq_schema = json.load(f)

BRAZE_TOKEN = ''    # PLEASE INPUT THE BRAZE TOKEN
HEADER = {'Authorization': 'Bearer ' + str(BRAZE_TOKEN)}
GCP_PROJECT = 'elandmallbigquery'
GOOGLE_APPLICATION_CREDENTIALS = 'elandmallbigquery-privatekey.json'    # PLEASE ADD THE SERVICE ACCOUNT PRIVATE KEY

YESTERDAY4BQ = datetime.strftime(datetime.now()-timedelta(1), '_%Y%m%d')
TDB_YESTERDAY = datetime.strftime(datetime.now()-timedelta(2), '%Y-%m-%d')
YESTERDAY = datetime.strftime(datetime.now()-timedelta(1), '%Y-%m-%d')
TODAY = datetime.strftime(datetime.now(), '%Y-%m-%d')


def connect_to_bq():
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
    client = bigquery.client.Client(project=GCP_PROJECT, credentials=credentials)
    return client


BQ = connect_to_bq()
job_config = bigquery.LoadJobConfig()


def get_all_campaign_list():
    """
    :return: list of all campaigns info (id, name, is_api_campaign, tags, last_edited)
    """
    # 처음에만 전체 리스트 가져와서 빅쿼리 테이블에 저장. 그 이후에는 하루 전에 생성된 캠페인 있는지 get_updated_campaign_list()
    url = 'https://rest.iad-06.braze.com/campaigns/list?&page='
    response = requests.get(url+str(0), headers=HEADER)
    result = response.json()
    # print(type(result), result)
    print("Getting all campaigns in braze", end="")
    campaigns = []
    if 'campaigns' in result:
        next_page = 0
        while len(result['campaigns']) != 0:
            campaigns.extend(result['campaigns'])
            print("...", end="")
            next_page += 1
            response = requests.get(url + str(next_page), headers=HEADER)
            result = response.json()

    print("\n", len(campaigns), campaigns)
    return campaigns


def get_updated_campaign_list(requested_date=TDB_YESTERDAY):
    # 지금 22일 오후 10시. 어제 업데이트된 캠페인의 리스트를 알고싶다. 21일 데이터 = 2021-08-20T15:00:00 ~ 2021-08-21T15:00:00
    tbd_date = datetime.strftime(requested_date - timedelta(days=2), '%Y-%m-%d')
    url = 'https://rest.iad-06.braze.com/campaigns/list?&page='
    response = requests.get(url+str(0)+'&last_edit.time[gt]='+tbd_date+'T15:00:00', headers=HEADER) #  이 시간 이후부터 지금까지 수정된 캠페인 리스트 조회
    result = response.json()
    # print(type(result), result)
    print("Getting updated campaigns in braze.", end="")
    updated_campaigns = []
    if 'campaigns' in result:
        next_page = 0
        while len(result['campaigns']) != 0:
            updated_campaigns.extend(result['campaigns'])
            print("...", end="")
            next_page += 1
            response = requests.get(url+str(next_page)+'&last_edit.time[gt]='+tbd_date+'T15:00:00', headers=HEADER)
            result = response.json()

    print("\n", len(updated_campaigns), updated_campaigns)
    return updated_campaigns


def get_campaign_details(campaigns):
    url = 'https://rest.iad-06.braze.com/campaigns/details'
    print("\nGetting campaigns details...")
    campaigns_detail = []
    for campaign in campaigns:
        if campaign['id']:
            response = requests.get(url=url+'?campaign_id='+campaign['id'], headers=HEADER)
            result = response.json()
            # print(result)

            details_dict = {'id': campaign['id']}
            for key, val in result.items():
                details_dict[key] = val
                details_dict['messages'] = len(result['messages'])   # messages의 내용 양이 많아서 생략, 갯수만 받아
                # if result.get('channels'):
                #     if any(ch in ['android_push', 'ios_push'] for ch in result['channels']):
                #         details_dict['messages'] = result['messages']
                #     else:
                #         details_dict['messages'] = ''
            del details_dict['message']
            campaigns_detail.append(details_dict)
            # print(details_dict)

    print(len(campaigns_detail), campaigns_detail)
    return campaigns_detail


def get_campaign_details_from_ids(campaign_ids):
    url = 'https://rest.iad-06.braze.com/campaigns/details'
    print("\nGetting campaigns details...")
    campaigns_detail = []
    for campaign_id in campaign_ids:
        response = requests.get(url=url+'?campaign_id='+campaign_id, headers=HEADER)
        result = response.json()
        # print(result)

        details_dict = {'id': campaign_id}
        for key, val in result.items():
            details_dict[key] = val
            details_dict['messages'] = len(result['messages'])   # messages의 내용 양이 많아서 생략, 갯수만 받아

        del details_dict['message']
        campaigns_detail.append(details_dict)
        print(details_dict)

    print(len(campaigns_detail), campaigns_detail)
    return campaigns_detail


def get_latest_campaign_details_from_ids(campaign_ids):
    # 지금 22일 오후 10시. 어제 21일 업데이트된 캠페인의 디테일 알고싶다. 21일 데이터 = 2021-08-20T15:00:00 ~ 2021-08-21T15:00:00
    url = 'https://rest.iad-06.braze.com/campaigns/details'
    print("\nGetting campaigns details...")
    campaigns_detail = {}
    for campaign_id in campaign_ids:
        response = requests.get(url=url+'?campaign_id='+campaign_id, headers=HEADER)
        result = response.json()
        # print(result)
        if result['last_sent']:
            last_sent_time = None
            if re.match('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$', result['last_sent'][:-6]):
                last_sent_time = datetime.strptime(result['last_sent'][:-6], "%Y-%m-%dT%H:%M:%S")
            elif re.match('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', result['last_sent']):
                last_sent_time = datetime.strptime(result['last_sent'][:-1], "%Y-%m-%dT%H:%M:%S")

            if last_sent_time is not None:
                yesterday_strt = datetime.strptime(TDB_YESTERDAY+'T15:00:00', "%Y-%m-%dT%H:%M:%S") #TDB_YESTERDAY
                yesterday_end = datetime.strptime(YESTERDAY+'T15:00:00', "%Y-%m-%dT%H:%M:%S")
                if yesterday_strt < last_sent_time < yesterday_end:
                    print(f"sent yesterday ({last_sent_time})")
                    details = {'id': campaign_id}
                    for key, val in result.items():
                        details[key] = val
                    details['messages'] = len(result['messages'])   # messages의 내용 양이 많아서 생략, 갯수만 받아
                    del details['conversion_behaviors']
                    del details['archived']
                    del details['draft']
                    del details['message']
                    print(details)

                    if any(ch in ['android_push', 'ios_push'] for ch in result['channels']):
                        campaigns_ch = 'push'
                    elif any(ch in ['trigger_in_app_message'] for ch in result['channels']):
                        campaigns_ch = 'inappmsg'
                    else:
                        campaigns_ch = result['channels'][0]

                    try:
                        campaigns_detail[campaigns_ch].append(details)
                    except KeyError:
                        campaigns_detail[campaigns_ch] = [details]
            else:
                continue
        else:
            continue    # pass는 단순히 실행할 코드 없다는 뜻. continue는 다음 순법의 loop 돌도록 강제로

    print("all scanned")
    for channel in campaigns_detail:
        print(f"{channel}:", len(campaigns_detail[channel]), campaigns_detail[channel])
    return campaigns_detail


def insert_data_to_bq(client: bigquery.Client, data, destination_table_id: str):
    print("\nInserting data to bq table:", destination_table_id)
    print("data", data)
    try:
        sql = f"""INSERT INTO `{GCP_PROJECT}.{destination_table_id}`
                    (id, name, tags, last_edited, is_api_campaign)
                    VALUES ("{data['id']}", "{data['name']}", {data['tags']}, "{data['last_edited']}", {data['is_api_campaign']})"""
        query_job = client.query(sql)
        result = query_job.result()
        print("result:", result)
    except NotFound:
        _handle_error()
        return


def select_all_ids_from_bq(target_table_id: str):
    client = connect_to_bq()
    print(f"\nGetting all ids from bq table: {target_table_id}")
    try:
        sql = f"""SELECT id FROM `{GCP_PROJECT}.{target_table_id}`"""
        query_job = client.query(sql)
        rows = query_job.result()
    except NotFound:
        return []
    return [row[0] for row in rows]


def select_all_ids_names_from_bq(client: bigquery.Client, target_table_id: str):
    client = connect_to_bq()
    print(f"\nGetting all ids from bq table: {target_table_id}")
    try:
        sql = f"""SELECT id, name FROM `{GCP_PROJECT}.{target_table_id}`"""
        query_job = client.query(sql)
        rows = query_job.result()
    except NotFound:
        return []
    return [list(row.items()) for row in rows]


def update_detail_data_to_bq(client: bigquery.Client, data, destination_table_id: str):
    print("\nUpdating data to bq table:", destination_table_id)
    print("data", data)
    sql = f"""UPDATE `{GCP_PROJECT}.{destination_table_id}` 
                SET last_sent = (CASE 
                    WHEN last_sent IS NULL THEN "{data['last_sent']}"
                    WHEN last_sent<"{data['last_sent']}" THEN "{data['last_sent']}"
                    ELSE last_sent END), 
                updated_at = (CASE
                    WHEN updated_at IS NULL THEN "{data['updated_at']}"
                    WHEN updated_at<"{data['updated_at']}" THEN "{data['updated_at']}"
                    ELSE updated_at END)
                WHERE id="{data['id']}" """
    query_job = client.query(sql)
    result = query_job.result()
    print("result:", result)


def update_list_data_to_bq(client: bigquery.client, data, destination_table_id: str):
    print("\nUpdating list data to bq table:", destination_table_id)
    print("data", data)
    sql = f"""UPDATE `{GCP_PROJECT}.{destination_table_id}` 
                    SET name = "{data['name']}",
                    last_edited = "{data['last_edited']}",
                    tags = (CASE
                        WHEN name != "{data['name']}" THEN ARRAY_CONCAT(tags, [CONCAT("Name changes: ", name)])
                        ELSE tags END)
                    WHERE id="{data['id']}" """
    query_job = client.query(sql)
    result = query_job.result()
    print("result:", result)


def get_campaign_analytics(campaigns):
    url = 'https://rest.iad-06.braze.com/campaigns/data_series'
    print("-----------------------analytics---------------------------")
    campaigns_analytics = []
    for campaign in campaigns:
        if campaign['id']:
            response = requests.get(url=url+'?campaign_id='+campaign['id']+'&length=1&ending_at='+TDB_YESTERDAY+'T15:00:00', headers=HEADER)
            result = response.json()
            # print("id: ", campaign['id'], ", name: ", campaign['name'])
            print(result)
            msgs_dict = {'id': campaign['id'], 'name': campaign['name'], 'utm_source':campaign['name']}
            if result.get('data'):
                morning_messages = result['data'][0]['messages']
                # afternoon_messages = result['data'][1]['messages']
                for key in morning_messages.keys():
                    filtered_msgs = [msg for msg in morning_messages[key]]
                    msgs_dict[key] = filtered_msgs
                # for key in afternoon_messages.keys():
                #     filtered_msgs = [msg for msg in afternoon_messages[key] if key in ['android_push', 'ios_push']]
                #     msgs_dict[key] += filtered_msgs

                """
                if 'ios_push' in msgs_dict.keys():
                    ios_push_msgs = msgs_dict.get('ios_push')
                    try:
                        if ios_push_msgs[0]['variation_api_id'] == ios_push_msgs[1]['variation_api_id']:
                            ios_push_msgs[0]['sent'] = ios_push_msgs[0]['sent'] + ios_push_msgs[1]['sent']
                            ios_push_msgs[0]['direct_opens'] = ios_push_msgs[0]['direct_opens'] + ios_push_msgs[1]['direct_opens']
                            ios_push_msgs[0]['total_opens'] = ios_push_msgs[0]['total_opens'] + ios_push_msgs[1]['total_opens']
                            ios_push_msgs[0]['bounces'] = ios_push_msgs[0]['bounces'] + ios_push_msgs[1]['bounces']
                            ios_push_msgs[0]['body_clicks'] = ios_push_msgs[0]['body_clicks'] + ios_push_msgs[1]['body_clicks']

                            ios_push_msgs[0]['conversions'] = ios_push_msgs[0]['conversions'] + ios_push_msgs[1]['conversions']
                            ios_push_msgs[0]['revenue'] = ios_push_msgs[0]['revenue'] + ios_push_msgs[1]['revenue']
                            ios_push_msgs[0]['unique_recipients'] = ios_push_msgs[0]['unique_recipients'] + ios_push_msgs[1]['unique_recipients']
                            del ios_push_msgs[1]  # msgs_dict['ios_push'] = ios_push_msgs[0]
                    except (KeyError, IndexError):
                        pass

                if 'android_push' in msgs_dict.keys():
                    android_push_msgs = msgs_dict.get('android_push')
                    try:
                        if android_push_msgs[0]['variation_api_id'] == android_push_msgs[1]['variation_api_id']:
                            android_push_msgs[0]['sent'] = android_push_msgs[0]['sent'] + android_push_msgs[1]['sent']
                            android_push_msgs[0]['direct_opens'] = android_push_msgs[0]['direct_opens'] + android_push_msgs[1]['direct_opens']
                            android_push_msgs[0]['total_opens'] = android_push_msgs[0]['toftal_opens'] + android_push_msgs[1]['total_opens']
                            android_push_msgs[0]['bounces'] = android_push_msgs[0]['bounces'] + android_push_msgs[1]['bounces']
                            android_push_msgs[0]['body_clicks'] = android_push_msgs[0]['body_clicks'] + android_push_msgs[1]['body_clicks']
                            del android_push_msgs[1]    # msgs_dict['android_push'] = android_push_msgs[0]
                    except (KeyError, IndexError):
                        pass

                if ('ios_push' or 'android_push') not in msgs_dict.keys():
                    continue
                    """

            print(msgs_dict)
            campaigns_analytics.append(msgs_dict)

    print(len(campaigns_analytics), "campaigns analytics!")
    return campaigns_analytics


def get_campaign_analytics_from_id(ids):
    # 지금 22일 오후 10시. 어제 21일 업데이트된 캠페인의 분석 알고싶다. 21일 데이터 = 2021-08-20T15:00:00 ~ 2021-08-21T15:00:00
    url = 'https://rest.iad-06.braze.com/campaigns/data_series'
    print("-----------------------analytics---------------------------")
    campaigns_analytics = []

    for id in ids:
        response = requests.get(url=url+'?campaign_id='+id+'&length=1&ending_at='+TDB_YESTERDAY, headers=HEADER)    # 지금 23일. 21일 데이터 알고싶어. TDB_YESTERDAY
        result = response.json()
        print(result)


def set_gcp_credentials():
    if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') is None:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "mightyhive-kr-privatekey.json"


# --------------------- 10/12
def etl():
    print("Start!")
    try:
        for table in bq_schema:
            table_id = table.get('id')
            table_schema = table.get('schema')
            _check_if_table_exists(table_id, table_schema)   # check if table exists, otherwise create
            # _load_table_from_result()

    except Exception:
        _handle_error()


def _handle_error():
    message = 'Error. Cause: %s' % (traceback.format_exc())
    print(message)


def _check_if_table_exists(table_id, table_schema):
    try:
        BQ.get_table(table_id)
    except NotFound:
        logging.warn('Creating table: %s' % table_id)
        schema = create_schema_from_json(table_schema)
        table = bigquery.Table(table_id, schema=schema)
        table = BQ.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


def create_schema_from_json(table_schema):
    schema = []
    for column in table_schema:
        schemaField = bigquery.SchemaField(column['name'], column['type'], column['mode'])
        schema.append(schemaField)

        if column['type'] == 'RECORD':
            schemaField._fields = create_schema_from_json(column['fields'])
    return schema


def _load_data_from_result(result):
    get_latest_campaign_details_from_ids()


def load_table_from_analytics_result(client: bigquery.client, analytics, table_id, table_schema):
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.schema = table_schema
    # for result in analytics:
    load_job = client.load_table_from_json(analytics, table_id, job_config=job_config)
    load_job.result()  # Waits for table load to complete.
    print("Job finished.")


def get_today_campaign_analytics_from_id_name(campaign_id_name, day):
    url = 'https://rest.iad-06.braze.com/campaigns/data_series'
    print(f"Getting campaigns analytics... for {day}")
    id = campaign_id_name[0]
    name = campaign_id_name[1]

    response = requests.get(url=url + '?campaign_id=' + id + '&length=1&ending_at=' + day, headers=HEADER)
    result = response.json()

    campaigns_analytics = []
    if result.get('data'):
        messages = result['data'][0]['messages']  # 캠페인의 하루치(길이 = 1) analytics 데이터.
        campaigns_analytic = {"date": day, "id": id, "original_name": name}
        utm = name.split('$')

        if len(utm) > 3:
            campaigns_analytic.update(
                {"utm_campaign_source": utm[-3], "utm_campaign_medium": utm[-2], "utm_campaign_name": utm[-1]})
        else:
            campaigns_analytic.update({"utm_campaign_source": '', "utm_campaign_medium": '', "utm_campaign_name": ''})
            print(f"utm naming conversion is wrong: {campaigns_analytic}")

        abtest = campaigns_analytic["utm_campaign_name"].split(',')

        if len(messages) > 1:  # 통합 채널 = ios/aos & variation 없음
            try:
                for ch in messages.keys():
                    try:
                        print(f"통합 채널 ch:{ch}, name:{name}\nraw_result:{messages[ch][0]}")
                    except IndexError:
                        break

                    if int(messages[ch][0]['sent']) == 0:
                        print(f"Not sent. No data today")
                        continue

                    campaigns_analytic[ch] = {
                        "sent": int(messages[ch][0]['sent']) if messages[ch][0].get('sent') else None,
                        "direct_opens": int(messages[ch][0]['direct_opens']) if messages[ch][0].get(
                            'direct_opens') else None,
                        "total_opens": int(messages[ch][0]['total_opens']) if messages[ch][0].get(
                            'total_opens') else None,
                        "bounces": int(messages[ch][0]['bounces']) if messages[ch][0].get('bounces') else None,
                        "body_clicks": int(messages[ch][0]['body_clicks']) if messages[ch][0].get(
                            'body_clicks') else None}

                campaigns_analytic.update({"channel": ','.join([ch for ch in messages.keys()]),
                                           "conversions": int(result['data'][0]['conversions']) if result['data'][
                                               0].get('conversions') else None,
                                           "conversions1": int(result['data'][0]['conversions1']) if result['data'][
                                               0].get('conversions1') else None,
                                           "conversions2": int(result['data'][0]['conversions2']) if result['data'][
                                               0].get('conversions2') else None,
                                           "conversions3": int(result['data'][0]['conversions3']) if result['data'][
                                               0].get('conversions3') else None,
                                           "unique_recipients": int(result['data'][0]['unique_recipients']) if
                                           result['data'][0].get('unique_recipients') else None,
                                           "revenue": float(result['data'][0]['revenue']) if result['data'][0].get(
                                               'revenue') else None})
            except Exception:
                _handle_error()
                return None

            if campaigns_analytic.get('ios_push') or campaigns_analytic.get('android_push'):
                print(f"1 ios/aos campaigns analytic added: {campaigns_analytic}")
                campaigns_analytics.append(campaigns_analytic)
            else:
                return None


        else:  # 개별 채널 & variation 있을수 있음
            ch = next(iter(messages))
            print(f"개별 채널 ch:{ch}, name:{name}\nraw_result:{messages[ch]}")
            for index, var in enumerate(messages[ch]):  # variation loop
                print(f"<{index + 1}> variation: {var}")

                # variation별 필드 update
                utm_campaign_name = ''
                if var.get('variation_name'):
                    if var['variation_name'] == 'Control Group':
                        print("Control Group. not analytics data")
                        continue
                    elif var['variation_name'] in abtest:
                        utm_campaign_name = var['variation_name']
                        variation_name = var['variation_name']
                    else:
                        utm_campaign_name = campaigns_analytic['utm_campaign_name']
                        variation_name = var['variation_name']
                else:
                    variation_name = ""
                    print(f"no variation name")

                # channel별 필드 update
                if ch in ['ios_push', 'android_push']:
                    var_analytics = campaigns_analytic.copy()
                    try:
                        if int(var['sent']) == 0:
                            print(f"Not sent. No data today")
                            continue
                        var_analytics[ch] = {"sent": int(var['sent']) if var.get('sent') else None,
                                             "direct_opens": int(var['direct_opens']) if var.get(
                                                 'direct_opens') else None,
                                             "total_opens": int(var['total_opens']) if var.get('total_opens') else None,
                                             "bounces": int(var['bounces']) if var.get('bounces') else None,
                                             "body_clicks": int(var['body_clicks']) if var.get('body_clicks') else None}

                        var_analytics.update({"channel": ch,
                                              "variation_name": variation_name,
                                              "utm_campaign_name": utm_campaign_name,
                                              "conversions": int(var['conversions']) if var.get(
                                                  'conversions') else None,
                                              "conversions1": int(var['conversions1']) if var.get(
                                                  'conversions1') else None,
                                              "conversions2": int(var['conversions2']) if var.get(
                                                  'conversions2') else None,
                                              "conversions3": int(var['conversions3']) if var.get(
                                                  'conversions3') else None,
                                              "unique_recipients": int(var['unique_recipients']) if var.get(
                                                  'unique_recipients') else None,
                                              "revenue": float(var['revenue']) if var.get('revenue') else None})
                    except KeyError:
                        continue

                elif ch == 'webhook':
                    var_analytics = campaigns_analytic.copy()
                    try:
                        if int(var['sent']) == 0:
                            print(f"Not sent. No data today")
                            continue
                        var_analytics.update(
                            {"sent": int(var['sent']) if var.get('sent') else None,  # "errors": int(var['errors'])
                             "channel": ch,
                             "variation_name": variation_name,
                             "utm_campaign_name": utm_campaign_name,
                             "conversions": int(var['conversions']) if var.get('conversions') else None,
                             "conversions1": int(var['conversions1']) if var.get('conversions1') else None,
                             "conversions2": int(var['conversions2']) if var.get('conversions2') else None,
                             "conversions3": int(var['conversions3']) if var.get('conversions3') else None,
                             "unique_recipients": int(var['unique_recipients']) if var.get(
                                 'unique_recipients') else None,
                             "revenue": float(var['revenue']) if var.get('revenue') else None})

                    except KeyError:
                        continue

                elif ch == 'email':
                    var_analytics = campaigns_analytic.copy()
                    try:
                        if int(var['sent']) == 0:
                            print(f"Not sent. No data today")
                            continue
                        var_analytics.update({"sent": int(var['sent']) if var.get('sent') else None,
                                              "opens": int(var['opens']) if var.get('opens') else None,
                                              "unique_opens": int(var['unique_opens']) if var.get(
                                                  'unique_opens') else None,
                                              "clicks": int(var['clicks']) if var.get('clicks') else None,
                                              "unique_clicks": int(var['unique_clicks']) if var.get(
                                                  'unique_clicks') else None,
                                              "delivered": int(var['delivered']) if var.get('delivered') else None,
                                              "channel": ch,
                                              "variation_name": variation_name,
                                              "utm_campaign_name": utm_campaign_name,
                                              "conversions": int(var['conversions']) if var.get(
                                                  'conversions') else None,
                                              "conversions1": int(var['conversions1']) if var.get(
                                                  'conversions1') else None,
                                              "conversions2": int(var['conversions2']) if var.get(
                                                  'conversions2') else None,
                                              "conversions3": int(var['conversions3']) if var.get(
                                                  'conversions3') else None,
                                              "unique_recipients": int(var['unique_recipients']) if var.get(
                                                  'unique_recipients') else None,
                                              "revenue": float(var['revenue']) if var.get('revenue') else None})
                    except KeyError:
                        continue

                elif ch == 'trigger_in_app_message':
                    var_analytics = campaigns_analytic.copy()
                    try:
                        if int(var['impressions']) == 0:
                            print(f"Not impressed. No data today")
                            continue
                        var_analytics.update(
                            {"impressions": int(var['impressions']) if var.get('impressions') else None,
                             "clicks": int(var['clicks']) if var.get('clicks') else None,
                             "first_button_clicks": int(var['first_button_clicks']) if var.get(
                                 'first_button_clicks') else None,
                             "second_button_clicks": int(var['second_button_clicks']) if var.get(
                                 'second_button_clicks') else None,
                             "channel": ch,
                             "variation_name": variation_name,
                             "utm_campaign_name": utm_campaign_name,
                             "conversions": int(var['conversions']) if var.get('conversions') else None,
                             "conversions1": int(var['conversions1']) if var.get('conversions1') else None,
                             "conversions2": int(var['conversions2']) if var.get('conversions2') else None,
                             "conversions3": int(var['conversions3']) if var.get('conversions3') else None,
                             "unique_recipients": int(var['unique_recipients']) if var.get(
                                 'unique_recipients') else None,
                             "revenue": float(var['revenue']) if var.get('revenue') else None})
                    except KeyError:
                        continue

                print(f"1 {ch} campaigns analytic added: {var_analytics}")
                campaigns_analytics.append(var_analytics)

    print(campaigns_analytics)
    return campaigns_analytics


def insert_date_to_joined_all_table(client: bigquery.Client, target_table_id: str, ga_table=TABLE_DATE, campaign=TDB_YESTERDAY):
    print(f"\nInserting data to the all joined tables: {target_table_id}")

    try:
        sql = f"""INSERT INTO braze_campaigns.ga_bi_joined_analytics 
        (date, id, original_name, utm_campaign_source, utm_campaign_medium, utm_campaign_name, channel,
        ios_push, android_push, sent, opens, unique_opens, unique_clicks, delivered,
        impressions, clicks, first_button_clicks, second_button_clicks, conversions, conversions1, conversions2, conversions3, 
        unique_recipients, revenue, GA_visit, GA_bounces, GA_transaction, GA_revenue, BI_conversion, BI_revenue)

        WITH ga AS ( SELECT * FROM `elandmallbigquery.118452709.ga_sessions_{ga_table}`),
            braze AS ( SELECT * FROM `elandmallbigquery.braze_campaigns.campaign_analytics` WHERE date = '{campaign}'),
            bi AS ( SELECT * FROM `elandmallbigquery.braze_campaigns.eland_internal_bi`)

        SELECT braze.date, id, original_name, utm_campaign_source, utm_campaign_medium, utm_campaign_name, ANY_VALUE(braze.channel) AS channel,
            ANY_VALUE(ios_push) AS ios_push, ANY_VALUE(android_push) AS android_push,
            ANY_VALUE(sent) AS sent, ANY_VALUE(opens) AS opens, ANY_VALUE(unique_opens) AS unique_opens, ANY_VALUE(unique_clicks) AS unique_clicks, ANY_VALUE(delivered) AS delivered,
            ANY_VALUE(impressions) AS impressions, ANY_VALUE(clicks) AS clicks, ANY_VALUE(first_button_clicks) AS first_button_clicks, ANY_VALUE(second_button_clicks) AS second_button_clicks, 
            ANY_VALUE(conversions) AS conversions, ANY_VALUE(conversions1) AS conversions1, ANY_VALUE(conversions2) AS conversions2, ANY_VALUE(conversions3) AS conversions3, 
            ANY_VALUE(unique_recipients) AS unique_recipients, ANY_VALUE(revenue) AS revenue,
            COUNT(totals.visits) as GA_visit, COUNT(totals.bounces) as GA_bounces, COUNT(totals.transactions) as GA_transaction, SUM(totals.totalTransactionRevenue) as GA_revenue,
            ANY_VALUE(bi.conversion) as BI_conversion, ANY_VALUE(bi.conversion_revenue) as BI_revenue
        FROM braze 
        LEFT JOIN ga
            ON braze.utm_campaign_source = ga.trafficSource.source 
            AND braze.utm_campaign_medium = ga.trafficSource.medium
            AND braze.utm_campaign_name = ga.trafficSource.campaign
            AND braze.date = PARSE_DATE("%Y%m%d", ga.date)
        LEFT JOIN bi
            ON braze.utm_campaign_source = bi.chnl_no
            AND braze.utm_campaign_name = bi.chnl_detail_no_num
            AND braze.date = bi.date
        GROUP BY braze.date, id, original_name, utm_campaign_source, utm_campaign_medium, utm_campaign_name
        ORDER BY date ASC"""
        query_job = client.query(sql)
        result = query_job.result()
    except Exception:
        _handle_error()
        return
    print("Job finished.")


if __name__ == '__main__':
    """누락된 날짜를 입"""
    requested_date = '2022-06-01'

    date = datetime.fromisoformat(requested_date)
    # tbd_date = datetime.strftime(date - timedelta(days=2), '%Y-%m-%d')
    # ytb_date = datetime.strftime(date - timedelta(days=1), '%Y-%m-%d')
    hyphens_rm_date = requested_date.replace('-', '')
    print(f"start date: {requested_date}, now:", datetime.now().isoformat(),", utcnow:", datetime.utcnow().isoformat()) # 현재 시간 확인

    table_campaigns_list = 'braze_campaigns.campaigns_list'
    """(1) list에 캠페인이 누락 됐을 경우, 해당 일자부터 오늘까지 수정된 캠페인 조회하는 campaign_list API 다시 호출해서 빅쿼리에 없는 데이터 적재"""
    """
    SELECT *
    FROM `elandmallbigquery.braze_campaigns.campaigns_list`
    WHERE name like '%2206%'
    """
    updated_campaigns = get_updated_campaign_list(date)
    existed_ids = select_all_ids_from_bq(table_campaigns_list)
    for campaign in updated_campaigns:
        if campaign['id'] not in existed_ids:
            # 기존 list 테이블에 없는 캠페인들은 campaign_list 테이블에 삽입
            insert_data_to_bq(BQ, campaign, table_campaigns_list)


    """(2) analytics에 일회성 캠페인이 누락 됐을 경우, list에 있는 해당 날짜의 일회성 캠페인을 campaign analytics API 다시 호출해서 삽입"""
    """
    SELECT date, id, original_name, count(original_name), sent, android_push.sent, ios_push.sent, FROM `elandmallbigquery.braze_campaigns.campaign_analytics`
    where date > '2022-05-30'
    group by 1,2,3,5,6,7
    order by date, original_name, id
    """
    table_id = bq_schema[0]['id']
    table_schema = bq_schema[0]['schema']
    _check_if_table_exists(table_id, table_schema)  # check if table exists, otherwise create

    campaign_ids_names = select_all_ids_names_from_bq(BQ, table_campaigns_list)
    for campaign in campaign_ids_names:
        c_id = campaign[0][1]
        c_name = campaign[1][1]

        oneoff_c_p = re.compile('^(\d{6}_.+)')  # 일회성 캠페인만 확인
        if oneoff_c_p.match(c_name):
            if c_name.startswith(hyphens_rm_date[2:8]):  # 해당 날짜의 일회성 캠페인만..
                print(f"Calling BRAZE API for the one-off campaign... id: {c_id}, name: {c_name}")
                today_analytics = get_today_campaign_analytics_from_id_name([c_id, c_name], requested_date)
                if today_analytics is not None:
                    if len(today_analytics) != 0:
                        # print(today_analytics)
                        load_table_from_analytics_result(BQ, today_analytics, table_id, table_schema)

    table_joined_all = 'braze_campaigns.ga_bi_joined_analytics'
    """(3) analytics에 일회성 캠페인이 누락 됐을 경우, list에 있는 해당 날짜의 일회성 캠페인을 campaign analytics API 다시 호출해서 삽입"""
    """
    SELECT date, id, original_name, count(original_name), sent, android_push.sent, ios_push.sent, GA_visit, BI_conversion FROM `elandmallbigquery.braze_campaigns.ga_bi_joined_analytics`
    where date > '2022-06-05'
    group by 1,2,3,5,6,7,8,9
    order by date, original_name, id
    """
    insert_date_to_joined_all_table(BQ, table_joined_all, hyphens_rm_date, requested_date)
    print("DONE for the table: ga_bi_joined_analytics")

    """2개이상 중복되어 삽입되었을 경우는 해당 날짜 삭제하고 다시 진행"""
    """
    DELETE FROM `elandmallbigquery.braze_campaigns.ga_bi_joined_analytics`
    WHERE date = '2022-03-12'
    """