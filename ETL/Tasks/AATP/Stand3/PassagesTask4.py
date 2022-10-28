# Скрипт авто обнавления acceptable values, stand3_periods и данных для расчета уставок по каждому профилю

import inspect
import sys
import time
from os.path import abspath
import pandas as pd
import traceback
from sqlalchemy import create_engine
import numpy as np
import os
sys.path.insert(1, r'')
from ETLOmzСonstruct import Construct


class stand3UpdateAccept(Construct):
    def __init__(self):
        super(stand3UpdateAccept, self).__init__()
        self.script_name = os.path.basename(__file__)[:-3]
        self.file_directory = abspath(inspect.getsourcefile(lambda:0)).split(os.sep)[-3]
        self.log_stat = ['New period has been added for the {}', 'New period has been deleted for the {}', 'Waiting new changes']
        self.init_path = ''
        pgsql_work_connection = ''
        pgsql_ds_connection = ''
        self.pgsql_work_con = create_engine(pgsql_work_connection)
        self.pgsql_ds_con = create_engine(pgsql_ds_connection)
        self.pg_query_read_periods = """
                                SELECT pro.passages_num, per.obj_id, ref_profile, pro.profile_name, dt_start, dt_stop, is_in_model, is_changed
                                FROM aatp.periods per
                                LEFT JOIN aatp.acceptable_profiles pro ON pro.obj_id = per.ref_profile
                                WHERE is_changed = True
                                """
        self.pg_query_read_passageway = """ 
                                    SELECT dt_start, dt_stop, passage as num_proh, amperage_mean, speed as speed_vs,
                                            temperature as max_temp, interformation_pause as independ_pause,  vibration_mean as vibration1_mean,
                                            vibration_max as vibration1_max
                                    FROM aatp.stand3_passageway
                                    WHERE ref_profile = '{}' and dt_start > '{}' and dt_stop < '{}'
                                    order by id 
                                    """
        self.pg_query_update_periods = """
                                  UPDATE aatp.periods
                                  SET (is_in_model, is_changed) = (%s,%s)
                                  WHERE is_changed = True
                                  """
        self.pg_query_update_acceptabe = """
                                    UPDATE aatp.acceptable_values 
                                    SET (q1,q2,q3,x1,x2) = (%s,%s,%s,%s,%s)
                                    WHERE ref_profile = '{}' and ref_parameter = '{}' and passage = {}
                                    """


    def correctData(self, df):
        try:
            data = df
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
            data = data[(data['num_bar'] > 0) & (data['num_bar'] < max(data['num_bar']))]
            data = data.drop(columns=['num_bar'])
            return data
        except:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))


    def updateAcceptable(self, dfperiods, maxproh, path):
        try:
            newdf = pd.read_csv(os.path.join(path, 'worktable.txt'))
            profile = list(dfperiods['ref_profile'])[0]
            listpar = ['amperage_mean', 'speed_vs', 'independ_pause', 'vibration1_mean', 'vibration1_max']
            param = ['nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,11', 'nsi.spr_terminal_techpa,10',
                     'nsi.spr_terminal_techpa,12', 'nsi.spr_terminal_techpa,13']
            for par in range(len(listpar)):
                for i in range(1, maxproh + 1):
                    temparr = newdf[(newdf['num_proh'] == i) & (newdf[f'{listpar[par]}'] > 0)][f'{listpar[par]}']
                    q1 = float(np.quantile(temparr, 0.25))
                    q3 = float(np.quantile(temparr, 0.75))
                    iqr = 1.5 * (q3 - q1)
                    minn = float(np.min(temparr))
                    maxx = float(np.max(temparr))
                    mid = float(np.median(temparr))
                    x1 = q1 - iqr if q1 - iqr > minn else minn
                    x2 = q3 + iqr if q3 + iqr < maxx else maxx
                    newrow = [q1, mid, q3, x1, x2]
                    self.pgsql_ds_con.execute(self.pg_query_update_acceptabe.format(profile, param[par], i), newrow)
        except:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))


    def Main(self):
        try:
            dfperiods = pd.read_sql(self.pg_query_read_periods, self.pgsql_ds_con)
            if len(dfperiods) > 0:
                path = os.path.join(self.init_path, list(dfperiods['profile_name'])[0].replace(' ', '_').replace('/', ''))
                rows = []
                max_pass = int(list(dfperiods['passages_num'])[0])
                for i in range(len(dfperiods)):
                    in_model = list(dfperiods['is_in_model'])[i]
                    if in_model is False:
                        df = self.correctData(pd.read_sql(self.pg_query_read_passageway.format(list(dfperiods['ref_profile'])[i],
                                                                                      str(list(dfperiods['dt_start'])[i]),
                                                                                      str(list(dfperiods['dt_stop'])[i])
                                                                                      ), self.pgsql_ds_con))
                        df.to_csv(os.path.join(path, 'worktable.txt'), header=False, index=False, mode='a')
                        res = self.logWritter(self.script_name, self.file_directory, self.log_stat[0].format(list(dfperiods['profile_name'])[0]))
                    else:
                        df = pd.read_csv(os.path.join(path, 'worktable.txt'))
                        df = df[(df['dtstart'] < str(list(dfperiods['dt_start'])[i])) | (
                                df['dtstop'] > str(list(dfperiods['dt_stop'])[i]))]
                        df.to_csv(os.path.join(path, 'worktable.txt'), index=False)
                        res = self.logWritter(self.script_name, self.file_directory, self.log_stat[1].format(list(dfperiods['profile_name'])[0]))
                    rows.append([not list(dfperiods['is_in_model'])[i], False])

                self.pgsql_ds_con.execute(self.pg_query_update_periods, rows)  # Обнавляем таблицу stand3_periods
                self.updateAcceptable(dfperiods, max_pass, path)
            else:
                res = self.logWritter(self.script_name, self.file_directory,  self.log_stat[2])
            res = self.statusUpdater(self.script_name, self.file_directory, 1)
            time.sleep(1)
        except:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))
            res = self.statusUpdater(self.script_name, self.file_directory, 2)
            time.sleep(5)


process = stand3UpdateAccept()
while True:
    try:
        process.Main()
    except:
        print(traceback.format_exc())