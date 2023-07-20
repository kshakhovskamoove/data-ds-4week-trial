import boto3
from botocore.client import BaseClient
import base64
import redshift_connector
import datetime
import json
import pandas as pd


SECRETS_CLIENT = boto3.client(service_name="secretsmanager", region_name='eu-west-1')


def get_secret(secret_id: str, client: BaseClient = SECRETS_CLIENT) -> dict:
    get_secret_value_response = client.get_secret_value(SecretId=secret_id)
    if "SecretString" in get_secret_value_response:
        secret = get_secret_value_response["SecretString"]
    else:
        secret = base64.b64decode(get_secret_value_response["SecretBinary"])
    return json.loads(secret)


sql = '''
with all_data as (select week_no, drn 
from public.vw_uber_driver_aggr_daily
union all 
select week_start as week_no , drn
from public.vw_uber_old_metrics
left join quality_reporting.gsheet_driver_data on driver_id= driver_uuid
where partition_date = current_date-1),
week_no_score as (
Select
  ROW_NUMBER() over (partition by drn order by week_no) as moove_week
 ,drn,week_no
 from all_data
 group by week_no, drn
 order by moove_week, drn
)
select s.drn,ws.moove_week,json_extract_path_text(s.input_json, 'country') as country,
json_extract_path_text(s.input_json, 'product') as product,
       s.week_date,
       s.score                                                                      as score,
       lag(s.score) over (partition by s.drn order by s.week_date)                  as last_week_ago_score,
       lag(s.score, 2) over (partition by s.drn order by s.week_date)               as two_weeks_ago_score,
	   r.asked_remittance,
       lag(r.asked_remittance) over (partition by r.drn order by r.week_date)      as last_week_ago_asked_remittance,
       lag(r.asked_remittance, 2) over (partition by r.drn order by r.week_date)   as two_weeks_ago_asked_remittance,
       r.cumulative_outstanding
from scoring.scoring s
         inner join public.vw_reconciliation_aggregated_weekly_mcs r
                    on s.drn = r.drn and r.week_date = s.week_date
         inner join week_no_score ws on s.drn = ws.drn and s.week_date = ws.week_no
where not s.predicted and json_extract_path_text(input_json, 'country')='{}'
order by s.drn, s.week_date
'''


def get_all_data(country):
    credentials = get_secret('data-de-redshift-credentials-prod-rw')
    with redshift_connector.connect(
            host=credentials['host'],
            database='dev',
            user=credentials['username'],
            password=credentials['password']) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql.format(country))
            score_data: pd.DataFrame = cursor.fetch_dataframe()
    return score_data


def try_out_rule(row):
    if row['moove_week']<4:
        return 'Continue Trial'
    elif row['try_out_score']>=6 and row['score']>=6 and row['cumulative_outstanding']<(row['asked_remittance']+row['last_week_ago_asked_remittance']+row['two_weeks_ago_asked_remittance'])*0.35:
        return 'Qualify'
    elif row['moove_week']==4 and (row['score']<6 and row['last_week_ago_score']>=6 and row['two_weeks_ago_score']>=6):
        return 'Extend trial'
    elif row['moove_week']==4 and (pd.isna(row['score']) or pd.isna(row['last_week_ago_score']) or pd.isna(row['two_weeks_ago_score'])):
        return 'Extend trial'
    elif pd.isna(row['score']) and row['moove_week']>=6 and row['try_out_score']>=6 and row['cumulative_outstanding']<(row['last_week_ago_asked_remittance']+row['two_weeks_ago_asked_remittance'])*0.35:
        return 'Qualify'
    elif pd.isna(row['score']) and pd.isna(row['last_week_ago_score']) and pd.isna(row['two_weeks_ago_score']) and row['moove_week']>=6:
        return 'No data for 3 weeks'
    else:
        return 'Does not Qualify'


def try_out_process(df):
    ts = datetime.datetime.now()
    df['median_try_out'] = df.filter(like='score').apply(lambda x: x.median(), axis=1)
    df['mean_try_out'] = df.filter(like='score').apply(lambda x: x.mean(), axis=1)
    df['median_try_out'] = df['median_try_out'].round(0).astype(int)
    df['mean_try_out'] = df['mean_try_out'].round(0).astype(int)
    df['try_out_score'] = df.filter(like='try_out').apply(lambda x: x.min(), axis=1)
    df['try_out_label'] = df.apply(try_out_rule, axis=1)
    df['request_time'] = ts
    df['compound_key'] = df.apply(lambda x:f"{x['drn']}_{str(x['moove_week'])}",axis=1)
    df = df[['drn', 'moove_week','country','product', 'week_date', 'try_out_label', 'score', 'last_week_ago_score',
             'two_weeks_ago_score', 'try_out_score', 'cumulative_outstanding', 'request_time', 'compound_key']]
    df.rename(columns={'score': 'week_score'}, inplace=True)
    return df


def write_to_db(output,table):
    credentials = get_secret('data-de-redshift-credentials-prod-rw')
    with redshift_connector.connect(
            host=credentials['host'],
            database='dev',
            user=credentials['username'],
            password=credentials['password']) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT distinct compound_key FROM " + table)
            existing_keys: pd.DataFrame = cursor.fetch_dataframe()
            existing_keys = existing_keys['compound_key'].tolist()
            updated_df = output[output.compound_key.isin(existing_keys)]
            if not updated_df.empty:
                to_delete = "( "
                for key_val in updated_df.compound_key.tolist():
                    to_delete += "'" + str(key_val) + "'"
                    to_delete += ", "
                to_delete = to_delete[:-2]
                to_delete += ")"
                sql_delete = "DELETE FROM " + table + " WHERE compound_key in " + to_delete
                cursor.execute(sql_delete)
            cursor.write_dataframe(output, table)
        conn.commit()


def four_week_trial(event, context):
    country = event['country']
    if event.get('week', False):
        week = datetime.datetime.strptime(event['week'], '%Y-%m-%d')
        week = week.date()
    else:
        today = datetime.date.today()
        week = today - datetime.timedelta(days=today.weekday(), weeks=1)
    recon = get_all_data(country)
    recon['week_date'] = pd.to_datetime(recon['week_date']).dt.date
    recon['week_date'] = recon['week_date']
    recon = recon[recon.week_date==week]
    try_out = try_out_process(recon)
    write_to_db(try_out,"scoring.four_week_trial")
