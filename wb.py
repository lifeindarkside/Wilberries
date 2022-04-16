import requests
import pandas as pd
import time
from datetime import timedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import datetime as DT

server = "SQLServer"
db = "databasename"
username = "SQLUserName"
password = "SQLUserPassword"
tablename = "TableNameInSQL"

WB_key = "keyfromWB"
startdate = DT.date(2021,1,1)
enddate = DT.date(2022,3,31)
limit = 1000
method = "https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod"

def create_link(startdate,enddate,limit,rrdid,key,method):
    link = method+"?dateFrom="+startdate+"&key="+key+"&limit="+limit+"&rrdid="+rrdid+"&dateto="+enddate
    return link

def get_json_answer(link):
    report_data = requests.get(link)
    json_report_data = report_data.json()
    return json_report_data

def json_to_df(json_report_data):
    df = pd.json_normalize(json_report_data, errors='ignore', sep='.', max_level=None)
    return df

def date_generate(startdate,enddate):
    date = startdate
    dates = [startdate]
    while date < enddate:
        date += DT.timedelta(days=2)
        dates.append(date)
    return dates

def dates_transform_start(i):
    date_format = '%Y-%m-%d'
    start_dt = i.strftime(date_format)
    start_dt = start_dt
    return start_dt

def dates_transform_end(i):
    date_format = '%Y-%m-%d'
    end_dt = i + timedelta(days=1)
    end_dt = end_dt.strftime(date_format)
    end_dt = end_dt
    return end_dt


def get_engine(server,db,username,password):
    conn = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+db+";UID="+username+";PWD="+password
    quoted = quote_plus(conn)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(new_con, fast_executemany=True)
    return engine

def transfer_to_SQL(engine,df,tablename):
    df.to_sql(tablename, engine, index=False, if_exists='append', chunksize=None)


def parse_json_to_SQL(startdate,enddate,limit,engine,key,tablename):
    dates = date_generate(startdate,enddate)
    for i in dates:
        df = pd.DataFrame()
        start_dt = dates_transform_start(i)
        end_dt = dates_transform_end(i)
        rrdid = 0
        link = create_link(start_dt,end_dt,limit,rrdid,key,method)
        json_report_data = get_json_answer(link)
        if json_report_data == None:
            print('Blank data in period: start date - ',start_dt,' end date - ', end_dt)
            time.sleep(10)
            pass
        else:
            df = json_to_df(json_report_data)
            transfer_to_SQL(engine,df,tablename)
            if len(df.index) == 1000:
                rrdid += 1
                link = create_link(start_dt,end_dt,limit,rrdid,key,method)
                time.sleep(10)
                json_report_data = get_json_answer(link)
                df = json_to_df(json_report_data)
                transfer_to_SQL(engine,df,tablename)
            else:
                pass 
            print('Dates done: start date - ',start_dt,' end date - ', end_dt)
            time.sleep(30)


engine = get_engine(server,db,username,password)
parse_json_to_SQL(startdate,enddate,limit,engine,WB_key,tablename)
