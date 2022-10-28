# Скрипт формирования логов по текущему профилю, если он идентифицирован и записи в pg


import inspect
import sys
from os.path import abspath
import pandas as pd
import os
import time
pd.options.mode.chained_assignment = None
from sqlalchemy import create_engine
import pandas as pd
import datetime
import numpy as np
import traceback
sys.path.insert(1, r'')
from ETLOmzСonstruct import Construct


class Stand45PassageWritter(Construct):
    def __init__(self):
        super(Stand45PassageWritter, self).__init__()
        self.script_name = os.path.basename(__file__)[:-3]
        self.file_directory = abspath(inspect.getsourcefile(lambda: 0)).split(os.sep)[-3]
        self.log_stat = ['Acceptable profiles has been updated, new profile', 'Successfully added new bars',
                         'Waiting new bars',
                         'This profile is not defined', 'Error input buffer', 'Выходные данные завершились с ошибкой']
        pgsql_work_connection = ''
        pgsql_ds_connection = ''
        self.pgsql_work_con = create_engine(pgsql_work_connection)
        self.pgsql_ds_con = create_engine(pgsql_ds_connection)
        self.pg_query_prokat = """ SELECT description, ref_nomenclature
                                FROM sved.sved_prokat
                                WHERE ref_posad LIKE '%%{}%%' """
        self.pg_query_read_profiles = """SELECT *
                                    FROM aatp.acceptable_profiles
                                    WHERE ref_location = 'nsi.spr_terminal_techpa_location,85' """
        self.pg_query_update_allprofiles = "UPDATE aatp.acceptable_profiles SET in_work = False WHERE ref_location = 'nsi.spr_terminal_techpa_location,85'"
        self.pg_query_update_chprofile = "UPDATE aatp.acceptable_profiles SET in_work = True WHERE ref_location = 'nsi.spr_terminal_techpa_location,85' and profile_name = '{}'"
        self.pg_query_select_acceptvalue = """SELECT passage, q1,q2,q3,x1,x2
                                        FROM aatp.acceptable_values 
                                        WHERE ref_profile = '{}' and ref_parameter = '{}'
                                        """
        self.pg_query_writter = """INSERT INTO aatp.stand45_passageway (ref_nomenclature, description_nomenclature, passage, dt_start, dt_stop, 
                                                       amperage_max, amperage_mean, power_max, power_mean, speed, temperature_max, temperature_mean,
                                                       interformation_pause, id_posad, deviation_amperage_max, deviation_amperage_mean, deviation_power_max,
                                                       deviation_power_mean, deviation_speed, deviation_temperature_max, deviation_temperature_mean, deviation_interformation_pause,
                                                       ref_profile
                                                       ) 
                                                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        self.old_nom = ''
        self.path_inner = ''
        self.old_df = pd.DataFrame(index=range(1), columns=['dtstop'])
        self.old_df['dtstop'][0] = datetime.datetime.now()
        self.new_df = pd.DataFrame()


    def writePeriods(self, class_p, class_param, new_df, tek_nomen, ps, refnom, path):
        try:
            flag_added = True
            for i in class_p:
                new_df[f'res_{i}'] = ''
                for j in range(len(new_df)):
                    npr = list(class_param[i]['passage'])[j]
                    q1 = list(class_param[i]['q1'])[j]
                    q3 = list(class_param[i]['q3'])[j]
                    x1 = list(class_param[i]['x1'])[j]
                    x2 = list(class_param[i]['x2'])[j]
                    tdf = new_df[new_df['num_proh'] == npr]
                    tdf[f'res_{i}'] = np.where((tdf[i] >= x1) & (tdf[i] <= x2), 'yellow', 'red')
                    tdf[f'res_{i}'] = np.where((tdf[i] >= q1) & (tdf[i] <= q3), 'green', tdf[f'res_{i}'])
                    new_df[new_df['num_proh'] == npr] = tdf
            new_df['nomen'] = tek_nomen
            rows = []
            for i in range(len(new_df)):
                newrow = []
                if list(new_df['res_amperage_mean'])[i] == '':
                    flag_added = False
                if not flag_added:
                    return 0
                newrow.append(
                    pd.read_sql(self.pg_query_prokat.format(str(ps)), self.pgsql_work_con)['ref_nomenclature'][0])
                newrow.append(list(new_df['nomen'])[i])
                newrow.append(int((new_df['num_proh'])[i]))
                newrow.append(datetime.datetime.strptime((str((new_df['dtstart'])[i])), "%Y-%m-%d %H:%M:%S.%f"))
                newrow.append(datetime.datetime.strptime((str((new_df['dtstop'])[i])), "%Y-%m-%d %H:%M:%S.%f"))
                newrow.append(list(new_df['amperage_max'])[i])
                newrow.append(list(new_df['amperage_mean'])[i])
                newrow.append(list(new_df['power_max'])[i])
                newrow.append(list(new_df['power_mean'])[i])
                newrow.append(list(new_df['speed'])[i])
                newrow.append(list(new_df['tstand_max'])[i])
                newrow.append(list(new_df['tstand_mean'])[i])
                newrow.append(list(new_df['independ_pause'])[i])
                newrow.append(ps)
                newrow.append(list(new_df['res_amperage_max'])[i])
                newrow.append(list(new_df['res_amperage_mean'])[i])
                newrow.append(list(new_df['res_power_max'])[i])
                newrow.append(list(new_df['res_power_mean'])[i])
                newrow.append(list(new_df['res_speed'])[i])
                newrow.append(list(new_df['res_tstand_max'])[i])
                newrow.append(list(new_df['res_tstand_mean'])[i])
                newrow.append(list(new_df['res_independ_pause'])[i])
                newrow.append(refnom)
                rows.append(newrow)
            idd = self.pgsql_ds_con.execute(self.pg_query_writter, rows)
            for i in range(len(rows)):
                a = ';'.join([str(rows[i][j]) for j in range(len(rows[i])) if j != 0 and j != 1])
                with open(path, 'a') as f:
                    f.write(a)
                    f.write('\n')
            return 1
        except:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))


    def Main(self):
        try:
            # Считаем буфер
            df = pd.read_csv(os.path.join(self.path_inner, 'signals_buffer_OPC.txt'), sep=';', header=None,
                             names=['dt', 'Amperage', 'Move', 'Controler4',
                                    'Controler5', 'Speed', 'Power', 'Tstand', 'id_posad'],
                             converters={'dt' \
                                             : str, 'Amperage': str, 'Move': str, 'Controler4': str,
                                         'Controler5': str, 'Speed': str, 'Power': str, 'Tstand': str, 'id_posad': str})

            for i in range(len(df.columns)):
                df = df.loc[(df[df.columns[i]] != '1,#INF') &
                            (df[df.columns[i]].notna()) &
                            (df[df.columns[i]] != '-1,#IND') &
                            (df[df.columns[i]] != 'nan') & (df[df.columns[i]] != '1.#INF')]
                if df.columns[i] != 'Controler5' and df.columns[i] != 'Controler4':
                    df[df.columns[i]].loc[df[df.columns[i]] != '0'] = df[df.columns[i]].astype(str).str.replace(',', '.')
                    if i == 0:
                        df[df.columns[i]] = pd.to_datetime(df[df.columns[i]], errors="coerce")
                    else:
                        df[df.columns[i]] = pd.to_numeric(df[df.columns[i]])
            if len(df) > 1000:
                ps = list(df['id_posad'].astype(int))[-1]
                tek_nomen = \
                pd.read_sql_query(self.pg_query_prokat.format(str(ps)), self.pgsql_work_con)['description'][0].split(';')[2]
                profiles = pd.read_sql_query(self.pg_query_read_profiles, self.pgsql_ds_con)
                path_model = os.path.join(self.path_inner, 'models_OPC')
                nom = 'No identified'
                for i in range(len(profiles)):
                    if list(profiles['profile_name'])[i] in tek_nomen:
                        nom = list(profiles['profile_name'])[i]
                        refnom = list(profiles['obj_id'])[i]
                        count_proh = list(profiles['passages_num'])[i]
                if nom != self.old_nom:
                    self.pgsql_ds_con.execute(self.pg_query_update_allprofiles)
                    if nom != 'No identified':
                        self.pgsql_ds_con.execute(self.pg_query_update_chprofile.format(str(nom)))
                        res = self.logWritter(self.script_name, self.file_directory, nom)
                    self.old_nom = nom
                    res = self.logWritter(self.script_name, self.file_directory, self.log_stat[0])

                if nom != 'No identified':
                    dirr = nom.replace(' ', '_')
                    dirr = dirr.replace('/', '')
                    inital_directory = os.path.join(path_model, dirr)
                    class_p = ['amperage_max', 'amperage_mean', 'power_max', 'power_mean', 'speed', 'tstand_max',
                               'tstand_mean', 'independ_pause']
                    param = ['nsi.spr_terminal_techpa,14', 'nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,16',
                             'nsi.spr_terminal_techpa,15', 'nsi.spr_terminal_techpa,11', 'nsi.spr_terminal_techpa,49',
                             'nsi.spr_terminal_techpa,1', 'nsi.spr_terminal_techpa,10']
                    class_param = {}
                    for i in range(len(param)):
                        class_param[class_p[i]] = pd.read_sql_query(
                            self.pg_query_select_acceptvalue.format(refnom, param[i]), self.pgsql_ds_con)

                    low_ground_amperage = 20  # Минимальный ток для прохода
                    low_time_amperage = 10

                    tok = [0 for _ in range(len(df))]
                    count_proh = [0 for _ in range(len(df))]
                    proh = [0 for _ in range(len(df))]
                    temper = [0 for _ in range(len(df))]
                    count_temper = [0 for _ in range(len(df))]
                    for i in range(len(df)):
                        if df['Amperage'][i] > low_ground_amperage:
                            tok[i] = 1
                    count = 0

                    for i in range(len(df)):
                        if tok[i] == 1:
                            count += 1
                        if tok[i] == 0:
                            if count < low_time_amperage and count != 0:
                                j = i - 1
                                while tok[j] == 1 and j > 0:
                                    tok[j] = 0
                                    j -= 1
                            count = 0
                    count = 0
                    for i in range(1, len(df)):
                        if tok[i] == 1 and tok[i - 1] == 0:
                            count += 1
                        if df['Controler4'][i] == 'True' and tok[i] == 1:
                            proh[i] = 1
                        if df['Controler5'][i] == 'True' and tok[i] == 1:
                            proh[i] = 2
                        if tok[i] == 1:
                            count_proh[i] = count
                    for i in range(len(df)):
                        if df['Tstand'][i] > 100:
                            temper[i] = 1
                    count = 0
                    for i in range(1, len(df)):
                        if temper[i] == 1 and temper[i - 1] == 0:
                            j = i
                        if count_proh[i] != 0 and count_proh[i - 1] == 0 and temper[i] == 1:
                            count = count_proh[i]
                            if not (j is None):
                                while j < i:
                                    count_temper[j] = count
                                    j += 1
                        if temper[i] == 1:
                            count_temper[i] = count

                    df['num_proh'] = proh
                    df['count_proh'] = count_proh
                    df['count_temper'] = count_temper

                    # Преобразование конечных данных
                    g = df.groupby('count_proh').mean()[['num_proh', 'Amperage', 'Power', 'Tstand', 'id_posad']]
                    g.columns = ['num_proh', 'amperage_mean', 'power_mean', 'tstand_mean', 'id_posad']
                    g[['dtstop', 'amperage_max', 'power_max', 'tstand_max', 'speed']] = df.groupby('count_proh').max()[
                        ['dt', 'Amperage', 'Power', 'Tstand', 'Speed']]
                    if len(g) > 1:
                        g = g.drop([0])
                        temper_data = [list(df.groupby('count_temper').mean()[['Tstand']]['Tstand']),
                                       list(df.groupby('count_temper').max()[['Tstand']]['Tstand'])]
                        for num, a in enumerate(temper_data):
                            a.pop(0)
                            if list(g['num_proh'])[0] == 2:
                                a.pop(0)
                            a1 = []
                            for i in (a):
                                for _ in range(2):
                                    a1.append(i)
                            if list(g['num_proh'])[0] == 2:
                                a1.insert(0, None)
                            if len(a1) < len(g):
                                a1.append(None)
                            if num == 0:
                                g['tstand_mean'] = a1
                            else:
                                g['tstand_max'] = a1
                        g['dtstart'] = df.groupby('count_proh').min()['dt']
                        g['independ_pause'] = (g['dtstart'] - g['dtstop'].shift(1)).dt.seconds + (
                                g['dtstart'] - g['dtstop'].shift(1)).dt.microseconds / 1000000
                        g = g[['dtstart', 'dtstop', 'num_proh', 'independ_pause', 'amperage_mean', 'amperage_max',
                               'power_mean', 'power_max',
                               'tstand_mean', 'tstand_max', 'speed', 'id_posad']]
                        g = g[g['tstand_mean'].notna()]
                        g = g.reset_index()

                        # Проверяем если полученный проход лежит между двумя другими и он новый то добавляем
                        if len(g) >= 6 and list(g['num_proh'])[2] == 1 and list(g['num_proh'])[3] == 2:
                            new_df = g.iloc[2:4]
                            new_df = new_df.reset_index()
                            if list(new_df['dtstart'])[0] > list(self.old_df['dtstop'])[0]:
                                # Выполняем добавление в базу и в лог новые проходы
                                path = os.path.join(inital_directory, 'log')
                                namelogfile = f'log_prohod_{str(time.localtime().tm_mday)}_{str(time.localtime().tm_mon)}.txt'
                                path = os.path.join(path, namelogfile)
                                self.old_df = new_df
                                a = self.writePeriods(class_p, class_param, new_df, tek_nomen, ps, refnom, path)
                                if a == 1:
                                    res = self.logWritter(self.script_name, self.file_directory, self.log_stat[1])

                                else:
                                    res = self.logWritter(self.script_name, self.file_directory, self.log_stat[5])
                            else:
                                res = self.logWritter(self.script_name, self.file_directory, self.log_stat[2])
                                time.sleep(1)
                        else:
                            res = self.logWritter(self.script_name, self.file_directory, self.log_stat[2])
                            time.sleep(1)
                    else:
                        res = self.logWritter(self.script_name, self.file_directory, self.log_stat[2])
                        time.sleep(1)
                else:
                    res = self.logWritter(self.script_name, self.file_directory, self.log_stat[3])
                    time.sleep(30)
            else:
                res = self.logWritter(self.script_name, self.file_directory, self.log_stat[4])
                time.sleep(10)
        except:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))


process = Stand45PassageWritter()

while True:
    try:
        process.Main()
    except:
        print(traceback.format_exc())