#pip install --upgrade tapi-yandex-metrika==2021.5.28
import p4e_ym_settings as app_settings
import pandas as pd
import pymysql
import json
from tapi_yandex_metrika import YandexMetrikaLogsapi
from urllib.parse import urlparse
from sqlalchemy import create_engine

client = YandexMetrikaLogsapi(
    access_token=app_settings.ACCESS_TOKEN,
    default_url_params={'counterId': app_settings.COUNTER_ID},
    wait_report=True
)
START_DATE = app_settings.start_date
END_DATE = app_settings.end_date
params = {
    'fields': 'ym:pv:watchID,ym:pv:counterID,ym:pv:date,ym:pv:dateTime,ym:pv:title,ym:pv:URL,ym:pv:referer,ym:pv:browser,ym:pv:browserMajorVersion,ym:pv:browserMinorVersion,ym:pv:browserCountry,ym:pv:browserEngine,ym:pv:clientTimeZone,ym:pv:cookieEnabled,ym:pv:deviceCategory,ym:pv:from,ym:pv:ipAddress,ym:pv:javascriptEnabled,ym:pv:mobilePhone,ym:pv:mobilePhoneModel,ym:pv:operatingSystem,ym:pv:operatingSystemRoot,ym:pv:physicalScreenHeight,ym:pv:physicalScreenWidth,ym:pv:regionCity,ym:pv:regionCountry,ym:pv:artificial,ym:pv:pageCharset,ym:pv:isPageView,ym:pv:link,ym:pv:download,ym:pv:notBounce,ym:pv:lastSocialNetwork,ym:pv:httpError,ym:pv:clientID,ym:pv:networkType,ym:pv:lastSocialNetworkProfile,ym:pv:goalsID,ym:pv:shareService,ym:pv:shareURL,ym:pv:shareTitle,ym:pv:params',
    "source": "hits",
    "date1": START_DATE,
    "date2": END_DATE
}
DB_HOST = app_settings.DATABASE_HOST
DB_NAME = app_settings.DATABASE_NAME
DB_LOGIN = app_settings.DATABASE_LOGIN
DB_PASS = app_settings.DATABASE_PASS

TABLE_NAME = 'p4e_ym_hits'
LOGS_TABLE_NAME = 'p4e_ym_hits_logs'

engine = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(DB_LOGIN, DB_PASS, DB_HOST, DB_NAME))


info = client.create().post(params=params)
request_id = info["log_request"]["request_id"]

report = client.download(requestId=request_id).get()
data = report.data
data_as_json_columns = report.columns
data_as_json = report().to_values()
p4e_ym_hits = pd.DataFrame(data_as_json[:], columns=data_as_json_columns)

parsed = p4e_ym_hits['ym:pv:URL'].apply(urlparse)

env = pd.Series([i.netloc.split('.')[0] if len(i.netloc.split('.')) > 2 else None for i in parsed])
env = [i if i and i != 'tariff' else 'prod' for i in env]
seg = pd.Series(
    [
        i.path.split('/')[2] if len(i.path.split('/')) > 2 and i.path.split('/')[2][:2] == 'ru'
            else None for i in parsed
    ]
)
ws = pd.Series([i.fragment.split('/')[1] if len(i.fragment.split('/')) > 1 else 'prod' for i in parsed])
application = pd.Series([i.fragment.split('/')[2] if len(i.fragment.split('/')) > 2 else None for i in parsed])
# username = pd.Series([i.fragment.split('/')[-1] if i.fragment.split('/')[-1].isdigit() else None for i in parsed])

p4e_ym_hits['enviroment'] = env
p4e_ym_hits['segment'] = seg
p4e_ym_hits['ws'] = ws
p4e_ym_hits['application'] = application
p4e_ym_hits.to_sql(LOGS_TABLE_NAME,con = engine,if_exists='append',index=False)

# with open('data_as_json_hits', 'w') as f:
#     json.dump(data, f)

p4e_ym_hits.to_excel(
    "Трафик_h.xlsx",
    sheet_name='data',
    index=False
)
