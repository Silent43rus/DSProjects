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
sys.path.insert(1, r'')          # Путь к etl конструктору
from ETLOmzСonstruct import Construct


class cuttingCutsWritter(Construct):
    def __init__(self):
        super(cuttingCutsWritter, self).__init__()
        self.script_name = os.path.basename(__file__)[:-3]
        self.file_directory = abspath(inspect.getsourcefile(lambda:0)).split(os.sep)[-3]
        self.log_stat = ['Acceptable profiles has been updated, new profile', 'Successfully added new cuts', 'Waiting new cuts', 'This profile is not defined']
        pgsql_work_connection = ''
        pgsql_ds_connection = ''
        self.pgsql_work_con = create_engine(pgsql_work_connection)
        self.pgsql_ds_con = create_engine(pgsql_ds_connection)
        self.pg_query_prokat = """ SELECT description, ref_nomenclature
                                FROM sved.sved_prokat
                                WHERE ref_posad LIKE '%%{}%%' """
        self.pg_query_read_profiles = """SELECT *
                                    FROM aatp.acceptable_profiles
                                    WHERE ref_location = 'nsi.spr_terminal_techpa_location,52' """
        self.pg_query_update_allprofiles = "UPDATE aatp.acceptable_profiles SET in_work = False WHERE ref_location = 'nsi.spr_terminal_techpa_location,52'"
        self.pg_query_update_chprofile = "UPDATE aatp.acceptable_profiles SET in_work = True WHERE ref_location = 'nsi.spr_terminal_techpa_location,52' and profile_name = '{}'"
        self.pg_query_select_acceptvalue = """SELECT passage, q1,q2,q3,x1,x2
                                        FROM aatp.acceptable_values 
                                        WHERE ref_profile = '{}' and ref_parameter = '{}'
                                        """
        self.pg_query_writter = """INSERT INTO aatp.cutting_cuts (ref_nomenclature, description_nomenclature, cut_number, dt_start, dt_stop, 
                                                       amperage_mean, amperage_max, power_mean, power_max, vibration_mean, vibration_max, interformation_pause,
                                                       time_cycle, time_rez, disc_speed, disc_diameter, id_posad, deviation_amperage_mean, deviation_amperage_max,
                                                       deviation_power_mean, deviation_power_max,deviation_vibration_mean, deviation_vibration_max,
                                                       deviation_interformation_pause, deviation_time_cycle, deviation_time_rez, deviation_disc_speed,
                                                       deviation_disc_diameter, ref_profile) 
                                                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        self.old_nom = ''
        self.path_inner = ''

        self.old_df = pd.DataFrame(index=range(1), columns=['dtstop'])
        self.old_df['dtstop'][0] = datetime.datetime.now()
        self.new_df = pd.DataFrame()

    # Запись резов в базу
    def writeCuts(self, class_p, class_param, new_df, tek_nomen, ps, refnom, path):
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
                    tdf = new_df[new_df['cycle'] == npr]
                    if i == 'vibration_mean' or i == 'vibration_max':
                        tdf[f'res_{i}'] = np.where((tdf[i] < x2), 'yellow', 'red')
                        tdf[f'res_{i}'] = np.where((tdf[i] < q3), 'green', tdf[f'res_{i}'])
                    else:
                        tdf[f'res_{i}'] = np.where((tdf[i] >= x1) & (tdf[i] <= x2), 'yellow', 'red')
                        tdf[f'res_{i}'] = np.where((tdf[i] >= q1) & (tdf[i] <= q3), 'green', tdf[f'res_{i}'])
                    new_df[new_df['cycle'] == npr] = tdf
            new_df['nomen'] = tek_nomen
            rows = []
            for i in range(len(new_df)):
                newrow = []
                if list(new_df['res_amperage_mean'])[i] == '':
                    flag_added = False
                if not flag_added:
                    return 0
                newrow.append(pd.read_sql(self.pg_query_prokat.format(str(ps)), self.pgsql_work_con)['ref_nomenclature'][0])
                newrow.append(list(new_df['nomen'])[i])
                newrow.append(list(new_df['cycle'])[i])
                newrow.append(datetime.datetime.strptime((str((new_df['dtstart'])[i])), "%Y-%m-%d %H:%M:%S.%f"))
                newrow.append(datetime.datetime.strptime((str((new_df['dtstop'])[i])), "%Y-%m-%d %H:%M:%S.%f"))
                newrow.append(list(new_df['amperage_mean'])[i])
                newrow.append(list(new_df['amperage_max'])[i])
                newrow.append(list(new_df['power_mean'])[i])
                newrow.append(list(new_df['power_max'])[i])
                newrow.append(list(new_df['vibration_mean'])[i])
                newrow.append(list(new_df['vibration_max'])[i])
                newrow.append(list(new_df['interformation_pause'])[i])
                newrow.append(list(new_df['time_cycle'])[i])
                newrow.append(list(new_df['time_rez'])[i])
                newrow.append(list(new_df['disc_speed'])[i])
                newrow.append(list(new_df['disc_diameter'])[i])
                newrow.append(ps)
                newrow.append(list(new_df['res_amperage_mean'])[i])
                newrow.append(list(new_df['res_amperage_max'])[i])
                newrow.append(list(new_df['res_power_mean'])[i])
                newrow.append(list(new_df['res_power_max'])[i])
                newrow.append(list(new_df['res_vibration_mean'])[i])
                newrow.append(list(new_df['res_vibration_max'])[i])
                newrow.append(list(new_df['res_interformation_pause'])[i])
                newrow.append(list(new_df['res_time_cycle'])[i])
                newrow.append(list(new_df['res_time_rez'])[i])
                newrow.append(list(new_df['res_disc_speed'])[i])
                newrow.append(list(new_df['res_disc_diameter'])[i])
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
            df = pd.read_csv(os.path.join(self.path_inner, 'signals_buffer_OPC.txt'), sep=';', header=None)
            df.columns = ['dt', 'amperage', 'power', 'disc_diameter', 'disc_speed',
                  'time_cycle', 'time_rez', 'flag_r_rezka', 'flag_r_5stand', 'id_posad', 'vibration']
            df['dt'] = pd.to_datetime(df['dt'])
            ps = list(df['id_posad'].astype(int))[-1]
            tek_nomen = pd.read_sql(self.pg_query_prokat.format(str(ps)), self.pgsql_work_con)['description'][0].split(';')[2][1:]
            profiles = pd.read_sql_query(self.pg_query_read_profiles, self.pgsql_ds_con)
            path_model = os.path.join(self.path_inner, 'models_OPC')
            nom = 'No identified'

            for i in range(len(profiles)):
                if list(profiles['profile_name'])[i] in tek_nomen:
                    nom = list(profiles['profile_name'])[i]
                    refnom = list(profiles['obj_id'])[i]
                    count_rez = list(profiles['passages_num'])[i]
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
                class_p = ['amperage_mean', 'amperage_max', 'power_mean', 'power_max', 'vibration_mean', 'vibration_max',
                           'interformation_pause', 'time_cycle', 'time_rez', 'disc_speed', 'disc_diameter']
                param = ['nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,14', 'nsi.spr_terminal_techpa,15',
                         'nsi.spr_terminal_techpa,16', 'nsi.spr_terminal_techpa,12', 'nsi.spr_terminal_techpa,13',
                         'nsi.spr_terminal_techpa,10', 'nsi.spr_terminal_techpa,17', 'nsi.spr_terminal_techpa,18',
                         'nsi.spr_terminal_techpa,19', 'nsi.spr_terminal_techpa,20']
                class_param = {}
                for i in range(len(param)):
                    class_param[class_p[i]] = pd.read_sql_query(self.pg_query_select_acceptvalue.format(refnom, param[i]), self.pgsql_ds_con)
                count_bar = 1

                # Расчет заготовок
                flag_rr5 = False
                flag_rr = False
                count_rr = 0
                fl_rr5 = list(df.flag_r_5stand)
                fl_rr = list(df.flag_r_rezka)
                bar = [1]
                countrr = [0]
                for i in range(1, len(df)):
                    if fl_rr5[i] == 0 and fl_rr5[i - 1] == 1:
                        flag_rr5 = True
                    if fl_rr[i] == 1 and fl_rr[i - 1] == 0:
                        flag_rr = True
                    if fl_rr[i] == 0 and fl_rr[i - 1] == 1:
                        flag_rr = False
                        count_rr = 0
                    if flag_rr:
                        count_rr += 1
                    if flag_rr and flag_rr5 and count_rr > 10:
                        count_bar += 1
                        flag_rr5 = False
                        flag_rr = False
                        count_rr = 0
                    countrr.append(count_rr)
                    bar.append(count_bar)

                # Расчет циклов
                flag = False
                count_cycle = 1
                count_same = 0
                time_cycle = list(df.time_cycle)
                cycle = [1]
                flagif = False
                for i in range(1, len(df)):
                    if time_cycle[i] > time_cycle[i - 1]:
                        flag = True
                        flagif = False
                    if time_cycle[i] == time_cycle[i - 1]:
                        count_same += 1
                    else:
                        count_same = 0
                    if count_same == 5 and not flagif:
                        flag = False
                        count_cycle += 1
                        flagif = True
                    if time_cycle[i] < time_cycle[i - 1] and not flagif:
                        flag = False
                        count_cycle += 1
                        flagif = True
                    if flag:
                        cycle.append(count_cycle)
                    else:
                        cycle.append(0)
                    if bar[i - 1] != bar[i]:
                        count_cycle = 1

                # Расчет резов
                flag = False
                count_rez = 1
                count_same = 0
                time_rez = list(df.time_rez)
                rez = [1]
                flagif = False
                for i in range(1, len(df)):
                    if time_rez[i] > time_rez[i - 1]:
                        flag = True
                        flagif = False
                    if time_rez[i] == time_rez[i - 1]:
                        count_same += 1
                    else:
                        count_same = 0
                    if count_same == 5 and not flagif:
                        flag = False
                        count_rez += 1
                        flagif = True
                    if time_rez[i] < time_rez[i - 1] and not flagif:
                        flag = False
                        count_rez += 1
                        flagif = True
                    if flag:
                        rez.append(count_rez)
                    else:
                        rez.append(0)
                    if bar[i - 1] != bar[i]:
                        count_rez = 1
                df['bar'] = bar
                df['cycle'] = cycle
                df['rez'] = rez
                count_bar += 1
                # Построение таблицы резов

                g = df.groupby(['bar', 'cycle']).mean()[['vibration']]
                g.columns = ['vibration_mean']
                g[['amperage_mean', 'power_mean']] = df.groupby(['bar', 'rez']).mean()[['amperage', 'power']]
                g['vibration_max'] = df.groupby(['bar', 'cycle']).max()[['vibration']]
                g[['amperage_max', 'power_max']] = df.groupby(['bar', 'rez']).max()[['amperage', 'power']]
                g['dtstart'] = df.groupby(['bar', 'rez']).min()['dt']
                g[['time_rez', 'dtstop']] = df.groupby(['bar', 'rez']).max()[['time_rez', 'dt']]
                g['time_cycle'] = df.groupby(['bar', 'cycle']).max()['time_cycle']
                g[['disc_speed', 'disc_diameter']] = df.groupby(['bar', 'cycle']).min()[['disc_speed', 'disc_diameter']]
                g['time_rez'] = g['time_rez'] / 10
                g['time_cycle'] = g['time_cycle'] / 10
                g = g.reset_index()
                null_amp = g[g['cycle'] == 0].amperage_mean.mean()
                g = g[g['amperage_mean'] > null_amp + (null_amp / 100 * 20)]
                g['interformation_pause'] = (g['dtstart'] - g['dtstop'].shift(1)).dt.seconds + (
                        g['dtstart'] - g['dtstop'].shift(1)).dt.microseconds / 1000000
                g = g[['dtstart', 'dtstop', 'bar', 'cycle', 'amperage_mean', 'amperage_max', 'power_mean', 'power_max',
                       'vibration_mean', 'vibration_max', 'interformation_pause', 'time_cycle', 'time_rez', 'disc_speed',
                       'disc_diameter']]
                g = g.reset_index()
                g = g.drop(columns=['index'])
                if len(g[g['bar'] == 1]) > 1:
                    if list(g['bar'])[0] == 1 and list(g['bar'])[-1] > 2:
                        new_df = g[g['bar'] == 2]
                        new_df = new_df.reset_index()
                        new_df = new_df.drop(columns='index')
                        cycle = [int(i) for i in list(new_df['cycle'])]
                        f = True
                        for i in range(1, len(cycle)):
                            if cycle[0] != 1:
                                f = False
                            if cycle[i - 1] != cycle[i] - 1:
                                f = False
                        if list(new_df['dtstart'])[0] > list(self.old_df['dtstop'])[-1] and len(new_df) <= count_rez and f:

                            # Выполняем добавление в базу и в лог новые резы
                            path = os.path.join(inital_directory, 'log')
                            namelogfile = f'log_prohod_{str(time.localtime().tm_mday)}_{str(time.localtime().tm_mon)}.txt'
                            path = os.path.join(path, namelogfile)
                            self.old_df = new_df
                            a = self.writeCuts(class_p, class_param, new_df, tek_nomen, ps, refnom, path)
                            if a == 1:
                                res = self.logWritter(self.script_name, self.file_directory, self.log_stat[1])

                        else:
                            res = self.logWritter(self.script_name, self.file_directory, self.log_stat[2])
                    else:
                        res = self.logWritter(self.script_name, self.file_directory, self.log_stat[2])
                    time.sleep(1)
            else:
                res = self.logWritter(self.script_name, self.file_directory, self.log_stat[3])
                time.sleep(30)
        except:
            res = self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))
            print(traceback.format_exc())


process = cuttingCutsWritter()

while True:
    try:
        process.Main()
    except:
        print(traceback.format_exc())
