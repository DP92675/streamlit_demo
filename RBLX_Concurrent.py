import urllib
import requests
import time
from pandas.io.json import json_normalize
import json
import pandas as pd
import csv
import datetime
from datetime import timedelta
from datetime import datetime
import sys
import itertools
import os
import glob
import shutil
import gzip
import zipfile
from urllib.request import Request, urlopen
import re
import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq

server = 'localhost\SQLEXPRESS'
db_name = 'Dev_DB_1'

cnxn = pyodbc.connect(r'Driver=SQL Server;Server=' + server + ';Database=' + db_name + ';Trusted_Connection=yes;')
cursor=cnxn.cursor()


Date = datetime.today().date()
Date_Name = Date.strftime('%m%d%Y')

today = datetime.now()
today = today.date()

csv_Path = 'C:\\Users\\MichaelChristensen\\Flight Deck Dropbox\\Research\\_Data Science\\RBLX\\Hourly Concurrent Player CSVs\\'
pqt_path = "C:\\Users\\MichaelChristensen\\Flight Deck Dropbox\\Research\\_Data Science\\RBLX\\Daily Parquet Files\\"

now = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
fileDate = datetime.now().strftime('%Y-%m-%d-%H.%M')

startcnt = 0
endcnt = 100
sortPosition = 17
hasMoreRows = 'true'

Game_List = []
while startcnt <= 10000:

    url = 'https://games.roblox.com/v1/games/list?startRows=' + str(startcnt) + '&maxRows=' + str(endcnt) + '&hasMoreRows=' + str(hasMoreRows) + '&sortPosition=' + str(sortPosition) + '&pageContext.pageId=b2a815d7-20fc-4813-9618-281a1e20b243&pageContext.isSeeAllPage=true'

    response = requests.get(url)
    data = json.loads(response.content)

    responseList = data['games']

    for item in responseList:

        Name = item['name']
        Name = Name.replace(',','')
        PlaceID = item['placeId']
        PlayerCount = item['playerCount']

        insert_val = """'""" + str(Name).replace("'","") + """','""" + str(PlaceID) + """','""" + str(PlayerCount) + """',GetDate()"""

        CSV_val = str(Name) + """,""" + str(PlaceID) + """,""" + str(PlayerCount) + """,""" + str(now)
        Game_List.append(CSV_val)

        print(insert_val)

        cursor.execute("""INSERT INTO RBLX_Players ([Name], [PlaceID], [PlayerCount], [Date]) values (""" + insert_val + """)""")
        cnxn.commit()
    startcnt += endcnt

player_df = pd.DataFrame(Game_List)
player_df[['Name','PlaceID', 'PlayerCount', 'Date']] = player_df[0].str.split(',', expand=True)
player_df = player_df.drop(player_df.columns[[0]], axis=1)
player_df.to_csv('{0}RBLX_Concurrent_Players_{1}.csv'.format(csv_Path, str(today)), index=False)

fields = [
    ('Name', pa.string(), False),
    ('PlaceID', pa.string(), False),
    ('PlayerCount', pa.int32()),
    ('Date', pa.timestamp('ms'))
                                    ]
schema = pa.schema(fields)
schema = schema.remove_metadata()
# while beg_date < today:
data_df = pd.read_sql("Select [Name], [PlaceID], Cast([PlayerCount] as float) as [PlayerCount], Cast([Date] as datetime) as Date From RBLX_Players where cast(date as date) = '" + str(today) + "'", cnxn)
print(data_df)

data_df['PlaceID'] = data_df['PlaceID'].astype(str)

table = pa.Table.from_pandas(data_df, schema, preserve_index=False).replace_schema_metadata()
writer = pq.ParquetWriter(pqt_path + 'Daily_RBLX_Concurrent_Players_' + str(today) + '.parquet', schema=schema)
writer.write_table(table)

    # beg_date = beg_date + timedelta(days=1)

writer.close()

os.system('aws s3 sync "C:\\Users\\MichaelChristensen\\Flight Deck Dropbox\\Research\\_Data Science\\RBLX\\Daily Parquet Files\\\\" s3://fdc-ds-s3-1/RBLX_Players/')

cnxn.close()
