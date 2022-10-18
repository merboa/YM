#pip install --upgrade tapi-yandex-metrika==2021.5.28
import pandas as pd
import p4e_ym_settings as app_settings
import json
from tapi_yandex_metrika import YandexMetrikaLogsapi
from sqlalchemy import create_engine

ACCESS_TOKEN = "AQAEA7qkK4hpAAhJhXa3XHe32U1zopOwNpd-pCk"
COUNTER_ID = "84555478"

client = YandexMetrikaLogsapi(
    access_token=ACCESS_TOKEN,
    default_url_params={'counterId': COUNTER_ID},
    wait_report=True,
    recieve_all_data=True
)
START_DATE = app_settings.start_date
END_DATE = app_settings.end_date
params = {
    "fields": "ym:s:visitID,ym:s:counterID,ym:s:watchIDs,ym:s:dateTime,ym:s:isNewUser,ym:s:startURL,ym:s:endURL,ym:s:pageViews,ym:s:visitDuration,ym:s:bounce,ym:s:regionCountry,ym:s:regionCity,ym:s:clientID,ym:s:networkType,ym:s:goalsID,ym:s:goalsDateTime,ym:s:lastTrafficSource,ym:s:lastAdvEngine,ym:s:lastReferalSource,ym:s:lastSearchEngine,ym:s:lastSocialNetwork,ym:s:lastSocialNetworkProfile,ym:s:referer,ym:s:lastDirectClickOrder,ym:s:lastDirectBannerGroup,ym:s:lastDirectClickBanner,ym:s:lastDirectClickOrderName,ym:s:lastClickBannerGroupName,ym:s:lastDirectClickBannerName,ym:s:lastDirectPhraseOrCond,ym:s:lastDirectPlatformType,ym:s:lastDirectPlatform,ym:s:UTMContent,ym:s:deviceCategory,ym:s:mobilePhone,ym:s:mobilePhoneModel,ym:s:operatingSystem,ym:s:browser,ym:s:screenWidth,ym:s:physicalScreenWidth,ym:s:windowClientWidth",
    "source": "visits",
    "date1": START_DATE,
    "date2": END_DATE
}
DB_HOST = app_settings.DATABASE_HOST
DB_NAME = app_settings.DATABASE_NAME
DB_LOGIN = app_settings.DATABASE_LOGIN
DB_PASS = app_settings.DATABASE_PASS

TABLE_NAME = 'p4e_ym_visits_logs'
LOGS_TABLE_NAME = 'p4e_ym_visits_logs'

engine = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(DB_LOGIN, DB_PASS, DB_HOST, DB_NAME))

info = client.create().post(params=params)
request_id = info["log_request"]["request_id"]

report = client.download(requestId=request_id).get()
data = report.data
print(report.columns)
print(report().to_values())

data_as_json_columns = report.columns
data_as_json = report().to_values()
p4e_ym_visits = pd.DataFrame(data_as_json[:], columns=data_as_json_columns)
print('loaded')
p4e_ym_visits.to_sql(LOGS_TABLE_NAME,con = engine,if_exists='append',index=False)

# with open ('data_as_json', 'w') as f:
#             json.dump(data, f)

p4e_ym_visits.to_excel(
    "Трафик_v.xlsx",
    sheet_name='data',
    index=False
)


