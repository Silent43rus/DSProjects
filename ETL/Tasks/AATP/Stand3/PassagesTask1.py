# Скрипт формирования логов по текущему профилю, если он идентифицирован

import inspect
import sys
from os.path import abspath
import pandas as pd
import os
import time
pd.options.mode.chained_assignment = None
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import traceback
sys.path.insert(1, r'')
from ETLOmzСonstruct import Construct


class stand3PassageWritter(Construct):
    def __init__(self):
        super(stand3PassageWritter, self).__init__()
        self.script_name = os.path.basename(__file__)[:-3]
        self.file_directory = abspath(inspect.getsourcefile(lambda:0)).split(os.sep)[-3]
        self.log_stat = ['Acceptable profiles has been updated, new profile', 'Successfully', 'This profile is not defined']
        pgsql_work_connection = ''
        pgsql_ds_connection = ''
        self.pgsql_work_con = create_engine(pgsql_work_connection)
        self.pgsql_ds_con = create_engine(pgsql_ds_connection)
        self.pg_query_prokat = """ SELECT description
                                FROM sved.sved_prokat
                                WHERE ref_posad LIKE '%%{}%%' """
        self.pg_query_read_profiles = """SELECT *
                                    FROM aatp.acceptable_profiles
                                    WHERE ref_location = 'nsi.spr_terminal_techpa_location,51' """
        self.pg_query_update_allprofiles = "UPDATE aatp.acceptable_profiles SET in_work = False WHERE ref_location = 'nsi.spr_terminal_techpa_location,51'"
        self.pg_query_update_chprofile = "UPDATE aatp.acceptable_profiles SET in_work = True WHERE ref_location = 'nsi.spr_terminal_techpa_location,51' and profile_name = '{}'"
        self.pg_query_select_acceptvalue = """SELECT passage, q1,q2,q3,x1,x2
                                        FROM aatp.acceptable_values 
                                        WHERE ref_profile = '{}' and ref_parameter = '{}'
                                        """
        self.old_nom = ''
        self.path_inner = ''


    def Main(self):
        try:
            # Считываем буфер
            df = pd.read_csv(os.path.join(self.path_inner, 'signals_buffer_OPC.txt'), low_memory=False, index_col=None, header=0, sep=';')
            df.columns = ['dt', 'amperage_mean', 'temp', 'speed_vs', 'zad', 'id_posad', 'vibration1_mean', 'vibr2',
                          'vibr3', 'vibr4']
            for i in range(len(df.columns)):
                df = df.loc[(df[df.columns[i]] != '1,#INF') &
                            (df[df.columns[i]].notna()) &
                            (df[df.columns[i]] != '-1,#IND') &
                            (df[df.columns[i]] != 'nan')]
                df[df.columns[i]].loc[df[df.columns[i]] != '0'] = df[df.columns[i]].astype(str).str.replace(',', '.')
                if i == 0:
                    df[df.columns[i]] = pd.to_datetime(df[df.columns[i]])
                else:
                    df[df.columns[i]] = pd.to_numeric(df[df.columns[i]])
            df['amperage_max'] = 0
            df['vibration1_max'] = 0
            df['independ_pause'] = 0
            df['dt'] = pd.to_datetime(df['dt'], errors="coerce")
            ps = list(df['id_posad'].astype(int))[-1]
            tek_nomen = pd.read_sql_query(self.pg_query_prokat.format(str(ps)), self.pgsql_work_con)['description'][0].split(';')[2]
            path_model = os.path.join(self.path_inner,'models_OPC')

            # Сравниваем текущий профиль с нашими из базы
            profiles = pd.read_sql_query(self.pg_query_read_profiles, self.pgsql_ds_con)
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
                self.old_nom = nom
                # print('Acceptable profiles has been updated, new profile')
                res = self.logWritter(self.script_name, self.file_directory, self.log_stat[0])
            if nom != 'No identified':
                dirr = nom.replace(' ', '_')
                dirr = dirr.replace('/', '')
                inital_directory = os.path.join(path_model, dirr)

                # Считываем средние значения
                class_p = ['amperage_mean', 'vibration1_mean', 'vibration1_max', 'independ_pause', 'speed_vs']
                param = ['nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,12', 'nsi.spr_terminal_techpa,13',
                         'nsi.spr_terminal_techpa,10', 'nsi.spr_terminal_techpa,11']
                class_param = {}
                for i in range(len(param)):
                    class_param[class_p[i]] = pd.read_sql_query(self.pg_query_select_acceptvalue.format(refnom,param[i]), self.pgsql_ds_con)

                # Считаем проходы
                tok = list(df['amperage_mean'])
                task_controller = list(df['zad'])
                sbros = [0 for _ in tok]
                csbros = 0
                numb = [0 for _ in tok]  # Номер прохода
                ftok = [0 for _ in tok]
                sumproh = [0 for _ in tok]  # Количество проходов
                low_ground_amperage = 20 # Минимальный ток для прохода
                low_ground_times = 20 # Минимальная длительность паузы перед проходом
                low_task_controller = 1600 # Минимальная длина кантователя для сброса проходов
                low_time_amperage = 10 # Минимальная длительность прохода
                # Ставим флаг прохода где ток больше low_ground_amperage
                for i in range(len(tok)):
                    if tok[i] > low_ground_amperage:
                        ftok[i] = 1
                count = 0
                # Удаляем флаги прохода там где был ток но его длительность была меньше low_time_amperage
                for i in range(len(tok)):
                    if ftok[i] == 1:
                        count += 1
                    if ftok[i] == 0:
                        if count < low_time_amperage and count != 0:
                            j = i - 1
                            while ftok[j] == 1:
                                ftok[j] = 0
                                j -= 1
                        count = 0
                for i in range(len(tok)):
                    if ftok[i] == 0:
                        csbros += 1
                    if ftok[i] == 1:
                        csbros = 0
                    if csbros > 20:
                        if task_controller[i] >= low_task_controller and tok[i] < low_ground_amperage:
                            sbros[i] = 1
                teknum = 1
                proh = 1
                fperehod = False
                cperehod = 0
                for i in range(len(tok)):
                    if ftok[i] == 1:
                        numb[i] = teknum
                        sumproh[i] = proh
                        fperehod = True
                        cperehod = 0
                    if ftok[i] == 0 and fperehod and cperehod > low_ground_times:
                        teknum += 1
                        proh += 1
                        fperehod = False
                    if ftok[i] == 0:
                        cperehod += 1
                    if sbros[i] == 1:
                        teknum = 1
                df['num_proh'] = numb
                df['count_proh'] = sumproh
                sum_numb = np.sum(df['num_proh'][-5:])
                if sum_numb == 0:
                    g = df.groupby('count_proh').mean()[
                        ['num_proh', 'amperage_mean', 'temp', 'speed_vs', 'id_posad', 'independ_pause',
                         'vibration1_mean', 'vibration1_max']]
                    g[['dtstop', 'amperage_max', 'vibration1_max']] = df.groupby('count_proh').max()[
                        ['dt', 'amperage_mean', 'vibration1_mean']]
                    ds = list(g['dtstop'])
                    maxt = [0 for _ in ds]
                    for i in range(len(ds)):
                        maxt[i] = list(df[df['dt'] == ds[i]]['temp'])[0]
                    g['temp'] = maxt
                    g['dtstart'] = df.groupby('count_proh').min()['dt']
                    g = g[['dtstart', 'dtstop', 'num_proh', 'amperage_mean', 'amperage_max', 'speed_vs', 'temp', 'independ_pause',
                           'id_posad',
                           'vibration1_mean', 'vibration1_max']]
                    g['independ_pause'] = (g['dtstart'] - g['dtstop'].shift(1)).dt.seconds + (
                            g['dtstart'] - g['dtstop'].shift(1)).dt.microseconds / 1000000

                    # Откидываем последнюю строку т.к если проход не завершен то средние значения будут не правильными
                    g = g.reset_index()
                    g = g[2:]
                    g = g.drop(columns='count_proh')

                    # Добавляем результат значения (цвет)
                    g = g.reset_index()
                    g = g.drop(columns='index')
                    for i in class_p:
                        g[f'res_{i}'] = ''
                        for j in range(count_proh):
                            npr = list(class_param[i]['passage'])[j]
                            q1 = list(class_param[i]['q1'])[j]
                            q3 = list(class_param[i]['q3'])[j]
                            x1 = list(class_param[i]['x1'])[j]
                            x2 = list(class_param[i]['x2'])[j]
                            tdf = g[g['num_proh'] == npr]
                            if i == 'vibration1_mean' or i == 'vibration1_max':
                                tdf[f'res_{i}'] = np.where((tdf[i] < x2), 'yellow', 'red')
                                tdf[f'res_{i}'] = np.where((tdf[i] < q3), 'green', tdf[f'res_{i}'])
                            else:
                                tdf[f'res_{i}'] = np.where((tdf[i] >= x1) & (tdf[i] <= x2), 'yellow', 'red')
                                tdf[f'res_{i}'] = np.where((tdf[i] >= q1) & (tdf[i] <= q3), 'green', tdf[f'res_{i}'])
                            g[g['num_proh'] == npr] = tdf

                    # Считываем номенклатуру

                    id_posad = list(g['id_posad'].astype(int))
                    nomen = []
                    for i in range(len(id_posad)):
                        nomen.append(
                            pd.read_sql_query(self.pg_query_prokat.format(str(id_posad[i])), self.pgsql_work_con)[
                                'description'][0].split(';')[2])
                    g['nomen'] = nomen
                    namelogfile = f'log_prohod_{str(time.localtime().tm_mday)}_{str(time.localtime().tm_mon)}.txt'

                    # Запись в лог
                    path_log = os.path.join(inital_directory,'log')
                    if not os.path.exists(os.path.join(path_log, namelogfile)):
                        g.to_csv(os.path.join(path_log, namelogfile), index=None, header=None)
                    else:
                        with open(os.path.join(path_log, namelogfile)) as f:
                            old_lenfile = len(f.readlines())
                        numadd = []
                        for i in range(len(g)):
                            dframe = []
                            res = False
                            a = ','.join([str(k) for k in list(g.iloc[i])])
                            with open(os.path.join(path_log, namelogfile), 'r') as f:
                                for strr in f:
                                    if a == strr[:-1]:
                                        res = True
                            if not res:
                                numadd.append(i)
                        if len(numadd) > 0:
                            for i in numadd:
                                with open(os.path.join(path_log, namelogfile), 'a') as f:
                                    f.write(','.join([str(k) for k in list(g.iloc[i])]))
                                    f.write('\n')

                    # Переписываем правильный лог
                    with open(os.path.join(path_log, namelogfile)) as f:
                        lines = f.readlines()
                    res = []
                    for i in range(len(lines)):
                        flag = False
                        for k in res:
                            if lines[i].split(',')[0] in k.split(',')[0]:
                                flag = True
                        if not flag:
                            res.append(lines[i])
                    with open(os.path.join(path_log, namelogfile), 'w') as wr:
                        for strr in res:
                            wr.write(strr)
                    with open(os.path.join(path_log, namelogfile)) as f:
                        new_lenfile = len(f.readlines())
                    time.sleep(2)
                    res = self.logWritter(self.script_name, self.file_directory, self.log_stat[1])
            else:
                res = self.logWritter(self.script_name, self.file_directory, self.log_stat[2])
                time.sleep(30)
            res = self.statusUpdater(self.script_name, self.file_directory, 1)
        except Exception as ex:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))
            res = self.statusUpdater(self.script_name, self.file_directory, 2)


process = stand3PassageWritter()

while True:
    try:
        process.Main()
    except:
        print(traceback.format_exc())