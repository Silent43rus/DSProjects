# Скрипт записи периодов проката в pg

import inspect
import os
import sys
import time
from os.path import abspath
import pandas as pd
import traceback
from sqlalchemy import create_engine
import datetime
sys.path.insert(1, r'')
from ETLOmzСonstruct import Construct


class Stand45PeriogsWritter(Construct):
    def __init__(self):
        super(Stand45PeriogsWritter, self).__init__()
        self.script_name = os.path.basename(__file__)[:-3]
        self.file_directory = abspath(inspect.getsourcefile(lambda: 0)).split(os.sep)[-3]
        self.log_stat = ['Successful write period in pg', 'Waiting new period']
        pgsql_work_connection = ''
        pgsql_ds_connection = ''
        self.pgsql_work_con = create_engine(pgsql_work_connection)
        self.pgsql_ds_con = create_engine(pgsql_ds_connection)
        self.pg_query_read_calcperiods = """
                                SELECT  m.ref_profile as ref_profile, t.dt_start as dt_start, t.dt_stop as dt_stop
                                FROM (
                                        SELECT ref_nomenclature, MIN(dt_start) as dt_start, MAX(dt_stop) as dt_stop, dt_start::DATE as dt
                                        FROM aatp.stand45_passageway
                                        GROUP BY ref_nomenclature, dt_start::DATE
                                     ) t 
                                JOIN aatp.stand45_passageway m ON m.ref_nomenclature = t.ref_nomenclature AND t.dt_start = m.dt_start
                                ORDER BY t.dt_start
                                """

        self.pg_query_read_periods = """
                                SELECT ref_profile, dt_start, dt_stop
                                FROM aatp.periods
                                """
        self.pg_query_writte_period = """
                                 INSERT INTO aatp.periods (ref_profile,dt_start,dt_stop) 
                                                                   VALUES(%s,%s,%s)
                                 """

        self.mytekperiod = None


    def checkTekPass(self, oldpass, df):
        try:
            flag_updater = True
            sravlist = list(df['dt_stop'])[0]
            if oldpass is None:
                flag_updater = True
            else:
                if sravlist <= oldpass:
                    flag_updater = False
            if flag_updater:
                oldpass = sravlist
            return flag_updater, oldpass
        except:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))


    def Main(self):
        try:
            df = pd.read_sql(self.pg_query_read_calcperiods, self.pgsql_ds_con)
            maindf = pd.read_sql(self.pg_query_read_periods, self.pgsql_ds_con)
            df = df[-2:-1] if len(df) > 1 else df
            flag_updater, self.mytekperiod = self.checkTekPass(oldpass=self.mytekperiod,df=df)
            if flag_updater:
                row = []
                row.append(list(df['ref_profile'])[0])
                row.append(datetime.datetime.strptime((str(list(df['dt_start'])[0])), "%Y-%m-%d %H:%M:%S.%f"))
                row.append(datetime.datetime.strptime((str(list(df['dt_stop'])[0])), "%Y-%m-%d %H:%M:%S.%f"))
                self.pgsql_ds_con.execute(self.pg_query_writte_period, row)
                res = self.logWritter(self.script_name, self.file_directory, self.log_stat[0])
                time.sleep(60)
            else:
                res = self.logWritter(self.script_name, self.file_directory, self.log_stat[1])
                time.sleep(60)
            res = self.statusUpdater(self.script_name, self.file_directory, 1)
        except:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))
            res = self.statusUpdater(self.script_name, self.file_directory, 2)


process = Stand45PeriogsWritter()
while True:
    try:
        process.Main()
    except:
        print(traceback.format_exc())