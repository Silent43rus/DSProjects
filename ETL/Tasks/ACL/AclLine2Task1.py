# Скрипт записи таблицы кривизна-воздействие при ее смене на 2 линии правки

import inspect
import traceback
from os.path import abspath
import pandas as pd
import os
import time
from sqlalchemy import create_engine
import datetime
import sys
sys.path.insert(1, r'')
from ETLOmzСonstruct import Construct


class AclUpdateCurvTabe(Construct):

    def __init__(self):
        super(AclUpdateCurvTabe, self).__init__()
        self.script_name = os.path.basename(__file__)[:-3]
        self.file_directory = abspath(inspect.getsourcefile(lambda: 0)).split(os.sep)[-3]
        self.log_stat = ['New CurvTabe has been added', 'Waiting new CurvTable']

        self.init_path = ''
        pgsql_ds_connection = ''
        self.pgsql_ds_con = create_engine(pgsql_ds_connection)
        self.pg_query_add_curvtable = """INSERT INTO acl.line2_curvature_table (dt_start, curvature1, action1, curvature2, action2, 
                                                                curvature3, action3, curvature4, action4, curvature5, action5,
                                                                curvature6, action6, curvature7, action7, curvature8, action8,
                                                                curvature9, action9, curvature10, action10)
                                                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        self.pg_query_upd_curvtable = """UPDATE acl.line2_curvature_table SET dt_stop = CURRENT_TIMESTAMP WHERE dt_stop is null"""
        self.pg_query_select_curvtable = """SELECT dt_start, dt_stop, curvature1, action1, curvature2, action2, 
                                                                curvature3, action3, curvature4, action4, curvature5, action5,
                                                                curvature6, action6, curvature7, action7, curvature8, action8,
                                                                curvature9, action9, curvature10, action10
                           FROM acl.line2_curvature_table"""


    def main(self):
        try:
            df = pd.read_csv(self.init_path, header=None, sep=';')
            df.columns = ['dt', 'curvature1', 'action1', 'curvature2', "action2",
                          'curvature3', 'action3', 'curvature4', 'action4', 'curvature5', 'action5',
                          'curvature6', 'action6', 'curvature7', 'action7', 'curvature8', 'action8',
                          'curvature9', 'action9', 'curvature10', 'action10']
            for i in range(len(df.columns)):
                df = df.loc[(df[df.columns[i]] != '1,#INF') &
                            (df[df.columns[i]].notna()) &
                            (df[df.columns[i]] != '-1,#IND') &
                            (df[df.columns[i]] != 'nan')]
                df[df.columns[i]].loc[df[df.columns[i]] != '0'] = df[df.columns[i]].astype(str).str.replace(',', '.')
                if i == 0:
                    df[df.columns[i]] = pd.to_datetime(df[df.columns[i]])
                else:
                    df[df.columns[i]] = pd.to_numeric(df[df.columns[i]]).astype(float)
            temptab = []
            for i in df.columns:
                temptab.append(list(df[i])[0])
            temptab.remove(temptab[0])
            pgs = pd.read_sql(self.pg_query_select_curvtable, self.pgsql_ds_con)
            if len(pgs) == 0:
                self.pgsql_ds_con.execute(self.pg_query_add_curvtable,
                                          [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')] + temptab)
            else:
                temptab_pg = []
                for i in pgs.columns:
                    temptab_pg.append(list(pgs[i])[-1])
                temptab_pg.remove(temptab_pg[0])
                temptab_pg.remove(temptab_pg[0])
                if temptab != temptab_pg:
                    self.pgsql_ds_con.execute(self.pg_query_upd_curvtable)
                    self.pgsql_ds_con.execute(self.pg_query_add_curvtable,
                                              [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')] + temptab)
                    self.logWritter(self.script_name, self.file_directory, self.log_stat[0])
                else:
                    self.logWritter(self.script_name, self.file_directory, self.log_stat[1])
                self.statusUpdater(self.script_name, self.file_directory, 1)
            time.sleep(1200)
        except:
            self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))
            self.statusUpdater(self.script_name, self.file_directory, 2)
            time.sleep(5)


process = AclUpdateCurvTabe()
while True:
    try:
        process.main()
    except:
        print(traceback.format_exc())