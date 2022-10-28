# Скрипт записи в БД 5 проходов после выхода заготовки с 3 клети

import inspect
import sys
import traceback
from os.path import abspath
import os
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import gc
from sqlalchemy import create_engine
import datetime
import time
sys.path.insert(1, r'')
from ETLOmzСonstruct import Construct


class stand3PassageWritterBd(Construct):
    def __init__(self):
        super(stand3PassageWritterBd, self).__init__()
        self.script_name = os.path.basename(__file__)[:-3]
        self.file_directory = abspath(inspect.getsourcefile(lambda: 0)).split(os.sep)[-3]
        self.log_stat = ['No data', 'Successful write in pg', 'Waiting new bar', 'Profile is not defined']

        self.path = ''
        self.mytekpass = None
        pgsql_work_connection = ''
        pgsql_ds_connection = ''
        self.pgsql_read_con = create_engine(pgsql_work_connection)
        self.pgsql_ds_con = create_engine(pgsql_ds_connection)
        self.pg_query_prokat = """ SELECT description, ref_nomenclature 
                                FROM sved.sved_prokat
                                WHERE ref_posad LIKE '%%{}%%' """
        self.pg_query_writter = """INSERT INTO aatp.stand3_passageway (ref_nomenclature,description_nomenclature,passage,dt_start,dt_stop, 
                                                       amperage_mean,amperage_max,speed,temperature,interformation_pause,
                                                       vibration_mean,vibration_max,id_posad,deviation_amperage,deviation_speed,
                                                       deviation_interformation_pause,deviation_vibration_mean,deviation_vibration_max, ref_profile) 
                                                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        self.pg_query_ref_profile = """SELECT obj_id
                                    FROM aatp.acceptable_profiles
                                    WHERE ref_location = 'nsi.spr_terminal_techpa_location,51' and profile_name = '{}'"""
        self.pg_query_read_profiles = """ SELECT *
                                     FROM aatp.acceptable_profiles
                                     WHERE ref_location = 'nsi.spr_terminal_techpa_location,51' """


    def identProfile(self, df):
        df = df[df['in_work'] == True]
        if len(df) > 0:
            profile = list(df['profile_name'])[0]
            dirr = profile.replace(' ','_').replace('/','')
            count_pass = list(df['passages_num'])[0]
        else:
            profile = 'No identified'
            dirr = 'No identified'
            count_pass = 0
        return dirr, profile, count_pass


    def dataLoader(self, path):
        try:
            if os.path.exists(path):
                # Открываем файл делим колонки на списки и присваиваем датафрейму
                with open(path) as f:
                    data = [list(map(str.strip, x.split(','))) for x in f.read().split('\n')]
                temp_par = [i for i in range(17)]
                for i in range(17):
                    temp_par[i] = []
                for i in range(len(data) - 1):
                    for j in range(17):
                        temp_par[j].append(data[i][j])
                dfmean = pd.DataFrame()
                dfmean['dt_start'] = temp_par[0]
                dfmean['dt_stop'] = temp_par[1]
                dfmean['num_proh'] = temp_par[2]
                dfmean['amperage_mean'] = temp_par[3]
                dfmean['amperage_max'] = temp_par[4]
                dfmean['speed_vs'] = temp_par[5]
                dfmean['temp'] = temp_par[6]
                dfmean['independ_pause'] = temp_par[7]
                dfmean['id_posad'] = temp_par[8]
                dfmean['vibration1_mean'] = temp_par[9]
                dfmean['vibration1_max'] = temp_par[10]
                dfmean['res_amperage'] = temp_par[11]
                dfmean['res_vibr1_mean'] = temp_par[12]
                dfmean['res_vibr1_max'] = temp_par[13]
                dfmean['res_independ_pause'] = temp_par[14]
                dfmean['res_speed'] = temp_par[15]
                dfmean['nomen'] = temp_par[16]

                # Типизация столбцов
                for i in range(len(dfmean.columns)):
                    if i <= 10 and i > 1:
                        dfmean[dfmean.columns[i]] = pd.to_numeric(dfmean[dfmean.columns[i]])
                dfmean['num_proh'] = pd.to_numeric(dfmean['num_proh'].astype(int))
                dfmean['dt_start'] = pd.to_datetime(dfmean['dt_start'])
                dfmean['dt_stop'] = pd.to_datetime(dfmean['dt_stop'])
                del temp_par
                gc.collect()
                return dfmean
            else:
                res = self.logWritter(self.script_name, self.file_directory, self.log_stat[0])
                return None
        except Exception as ex:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))


    def dataProcessing(self, data, count_proh):
        try:
            num_bar = [0 for _ in list(data.num_proh)]
            num_proh = list(data.num_proh)
            tek_bar = 0
            for i in range(1, len(num_bar)):
                if num_proh[i] == 1:
                    tek_bar += 1
                    num_bar[i] = tek_bar
                elif num_proh[i] - 1 == num_proh[i - 1]:
                    num_bar[i] = tek_bar
            data['num_bar'] = num_bar
            data = data[data['num_bar'] > 0]
            dfmean = data
            dfmean = dfmean.sort_index(ascending=False)
            if count_proh == 5:
                dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 1) &
                                              (dfmean['num_proh'].shift(1) == 2) &
                                              (dfmean['num_proh'].shift(2) == 3) &
                                              (dfmean['num_proh'].shift(3) == 4) &
                                              (dfmean['num_proh'].shift(4) == 5), True, False)
                dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 2) &
                                              (dfmean['num_proh'].shift(-1) == 1) &
                                              (dfmean['num_proh'].shift(1) == 3) &
                                              (dfmean['num_proh'].shift(2) == 4) &
                                              (dfmean['num_proh'].shift(3) == 5), True, dfmean['flagtrue'])
                dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 3) &
                                              (dfmean['num_proh'].shift(-1) == 2) &
                                              (dfmean['num_proh'].shift(-2) == 1) &
                                              (dfmean['num_proh'].shift(1) == 4) &
                                              (dfmean['num_proh'].shift(2) == 5), True, dfmean['flagtrue'])
                dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 4) &
                                              (dfmean['num_proh'].shift(-1) == 3) &
                                              (dfmean['num_proh'].shift(-2) == 2) &
                                              (dfmean['num_proh'].shift(-3) == 1) &
                                              (dfmean['num_proh'].shift(1) == 5), True, dfmean['flagtrue'])
                dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 5) &
                                              (dfmean['num_proh'].shift(-1) == 4) &
                                              (dfmean['num_proh'].shift(-2) == 3) &
                                              (dfmean['num_proh'].shift(-3) == 2) &
                                              (dfmean['num_proh'].shift(-4) == 1), True, dfmean['flagtrue'])
            else:
                dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 1) &
                                              (dfmean['num_proh'].shift(1) == 2) &
                                              (dfmean['num_proh'].shift(2) == 3), True, False)
                dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 2) &
                                              (dfmean['num_proh'].shift(-1) == 1) &
                                              (dfmean['num_proh'].shift(1) == 3), True, dfmean['flagtrue'])
                dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 3) &
                                              (dfmean['num_proh'].shift(-1) == 2) &
                                              (dfmean['num_proh'].shift(-2) == 1), True, dfmean['flagtrue'])
            listdel = dfmean[dfmean['flagtrue'] == 0].groupby('num_bar').mean()
            listdel = listdel.reset_index()
            for i in list(listdel['num_bar']):
                dfmean = dfmean[dfmean['num_bar'] != i]
            dfvisual = dfmean
            dfvisual = dfvisual.sort_index()
            dfmean = dfmean[:count_proh]
            dfmean = dfmean.sort_index()
            dfmean = dfmean.reset_index()
            dfmean = dfmean.drop(columns=['index'])
            return dfmean, dfvisual
        except Exception as ex:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))


    def checkTekPass(self, oldpass,dfmean):
        try:
            flag_updater = True
            sravlist = list(dfmean['dt_stop'][-1:])[0]
            if oldpass is None:
                flag_updater = True
            else:
                if sravlist <= oldpass:
                    flag_updater = False
            if flag_updater:
                oldpass = sravlist
            return flag_updater, oldpass, sravlist
        except Exception as ex:
            res = self.logWritter(self.script_name, self.file_directory, traceback.format_exc())


    def Main(self):
        try:
            init_path, nomen, count_pass = self.identProfile(pd.read_sql_query(self.pg_query_read_profiles,self.pgsql_ds_con))
            if init_path != 'No identified' and init_path != 'Error':
                init_path = os.path.join(self.path,init_path)
                pathlog = os.path.join(init_path, 'log')
                filelog = os.path.join(pathlog, f'log_prohod_{str(time.localtime().tm_mday)}_{str(time.localtime().tm_mon)}.txt')
                dfmean = self.dataLoader(filelog)
                if dfmean is not None:
                    dfmean, dfvisual = self.dataProcessing(dfmean, count_proh=count_pass)
                    ps = list(dfvisual['id_posad'].astype(int))[-1]
                    if (len(dfvisual) > count_pass):
                        flag_updater, self.mytekpass, sra = self.checkTekPass(oldpass=self.mytekpass,dfmean=dfmean)
                        if flag_updater:
                            df = (dfmean)
                            rows = []
                            for i in range(len(df)):
                                newrow = []
                                newrow.append(pd.read_sql_query(self.pg_query_prokat.format(str(ps)), self.pgsql_read_con)['ref_nomenclature'][0])
                                newrow.append(pd.read_sql_query(self.pg_query_prokat.format(str(ps)), self.pgsql_read_con)['description'][0].split(';')[2][1:])
                                newrow.append(list(df['num_proh'])[i])
                                newrow.append(datetime.datetime.strptime((str((df['dt_start'])[i])), "%Y-%m-%d %H:%M:%S.%f"))
                                newrow.append(datetime.datetime.strptime((str((df['dt_stop'])[i])), "%Y-%m-%d %H:%M:%S.%f"))
                                newrow.append(list(df['amperage_mean'])[i])
                                newrow.append(list(df['amperage_max'])[i])
                                newrow.append(list(df['speed_vs'])[i])
                                newrow.append(list(df['temp'])[i])
                                newrow.append(list(df['independ_pause'])[i])
                                newrow.append(list(df['vibration1_mean'])[i])
                                newrow.append(list(df['vibration1_max'])[i])
                                newrow.append(int(list(df['id_posad'])[i]))
                                newrow.append(list(df['res_amperage'])[i])
                                newrow.append(list(df['res_speed'])[i])
                                newrow.append(list(df['res_independ_pause'])[i])
                                newrow.append(list(df['res_vibr1_mean'])[i])
                                newrow.append(list(df['res_vibr1_max'])[i])
                                newrow.append(pd.read_sql_query(self.pg_query_ref_profile.format(nomen), self.pgsql_ds_con)['obj_id'][0])
                                rows.append(newrow)
                            idd = self.pgsql_ds_con.execute(self.pg_query_writter, rows)
                            res = self.logWritter(self.script_name, self.file_directory, self.log_stat[1])
                        else:
                            res = self.logWritter(self.script_name, self.file_directory, self.log_stat[2])
                            time.sleep(1)
                else:
                    time.sleep(1)
            else:
                res = self.logWritter(self.script_name, self.file_directory, self.log_stat[3])
                time.sleep(1)
            res = self.statusUpdater(self.script_name, self.file_directory, 1)

        except Exception as ex:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))
            res = self.statusUpdater(self.script_name, self.file_directory, 2)
            time.sleep(1)


process = stand3PassageWritterBd()

while True:
    try:
        process.Main()
    except:
        print(traceback.format_exc())


