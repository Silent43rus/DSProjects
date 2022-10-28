# Скрипт обработки и записи воздействий по 1 линии правки

import inspect
from os.path import abspath
import pandas as pd
import os
import time
import numpy as np
from sqlalchemy import create_engine
import datetime
import traceback
import sys
sys.path.insert(1, r'')
from ETLOmzСonstruct import Construct


class AclAddNewAction(Construct):

    def __init__(self):
        super(AclAddNewAction, self).__init__()
        self.script_name = os.path.basename(__file__)[:-3]
        self.file_directory = abspath(inspect.getsourcefile(lambda: 0)).split(os.sep)[-3]
        self.log_stat = ['New Actions has been added', 'Waiting new actions', 'Waiting correct input data']

        self.path = ''
        pgsql_ds_connection = ''
        pgsql_work_connection = ''
        self.pgsql_ds_con = create_engine(pgsql_ds_connection)
        self.pgsql_work_con = create_engine(pgsql_work_connection)

        self.pg_query_get_nom = """
                           SELECT ref_nom
                           FROM terminal.route_operations
                           WHERE ref_serial LIKE '%%{}%%' and 
                           --completion_date is null
                           --order by "order"
                           description LIKE '%%правка%%'
                           limit 1
                           """
        self.pg_query_get_pack = """
                            SELECT pack_full_number, ref_nlz_info
                            FROM sved.sved_packet_info
                            WHERE id = '{}'
                            """
        self.pg_query_get_plav = """
                            SELECT plav_num
                            FROM sved.sved_nlz_info
                            WHERE obj_id = '{}'
                            """
        self.pg_query_insert_actions = """
                                  INSERT INTO acl.line1_actions (bar, curvature, action, out_curvature, length_bar,
                                                                 first_correct, upper_bound, lower_bound, tilter_pos,
                                                                 diff_action, nomen, id_packed, ref_nomenclature, n_plav, n_packet, dt_action)
                                                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                  """
        self.pg_query_get_maxbar = """
                              SELECT bar
                              FROM acl.line1_actions
                              ORDER by bar desc
                              LIMIT 1
                              """
        self.maindt = None


    def main(self, num_line):
        try:
            data = pd.read_csv(self.path, header=None, sep=';;')
            if num_line == 3:
                data['diff_action'] = 0
                data.columns = ['dt', 'predetermined_action', 'upper_line1', 'lower_line1', "current_curvature",
                                'upper_line2', 'lower_line2', 'lenght_bar', 'id_packed', 'nomen', 'series',
                                'upor_flag1', 'upor_flag2', 'diff_action']
            else:
                data.columns = ['dt','predetermined_action', 'upper_line1', 'lower_line1', "current_curvature",
                          'upper_line2', 'lower_line2', 'lenght_bar', 'id_packed', 'nomen', 'series',
                          'diff_action', 'upor_flag1', 'upor_flag2']
            data['upper_line'] = ''
            data['lower_line'] = ''
            data = data[['dt', 'predetermined_action', 'upper_line', 'lower_line', 'current_curvature',
                         'lenght_bar', 'upor_flag1', 'upor_flag2',
                         'upper_line1', 'lower_line1', 'upper_line2', 'lower_line2', 'id_packed', 'diff_action', 'nomen',
                         'series']]
            data['dt'] = pd.to_datetime(data['dt'], format='%Y.%m.%d %H:%M:%S.%f')
            data['id_packed'] = pd.to_numeric(data['id_packed']).astype(int)
            for i in range(1, len(data.columns) - 2):
                data = data.loc[(data[data.columns[i]] != '1,#INF') &
                                (data[data.columns[i]].notna()) &
                                (data[data.columns[i]] != '-1,#IND') &
                                (data[data.columns[i]] != 'nan')]
                data[data.columns[i]].loc[data[data.columns[i]] != '0'] = data[data.columns[i]].astype(str).str.replace(',', '.')
                if i != 6 and i != 7:
                    data[data.columns[i]] = pd.to_numeric(data[data.columns[i]]).astype(float)
            a = list(data[data['lenght_bar'] - data['lenght_bar'].shift(-1) > 2000].index)
            if len(a) >= 3:
                mydate = list(data['dt'])[a[0]]
                if mydate > self.maindt:
                    # Расчет верхней и нижней граници кривизны
                    f1 = data['upor_flag1']
                    f2 = data['upor_flag2']
                    pos = [0 for _ in f1]
                    up1 = data['upper_line1']  # 11
                    up2 = data['upper_line2']  # 8
                    low1 = data['lower_line1']  # 12
                    low2 = data['lower_line2']  # 10
                    up = [None for _ in f1]
                    low = [None for _ in f1]
                    c = 0
                    for i in range(len(f1)):
                        # Для main
                        #     if f1[i] == True and f1[i-1] == False:
                        #         c = 1
                        #     elif f2[i] == True and f2[i-1] == False:
                        #         c = 2
                        if f1[i] == 1:
                            c = 1
                        elif f2[i] == 1:
                            c = 2
                        if c == 1:
                            up[i] = up2[i]
                            low[i] = low2[i]
                            pos[i] = 1
                        elif c == 2:
                            up[i] = up1[i]
                            low[i] = low1[i]
                            pos[i] = 2
                        else:
                            up[i] = up1[i]
                            low[i] = low1[i]
                            pos[i] = 2
                    data['upper_line'] = up
                    data['lower_line'] = low
                    data['position'] = pos
                    data = data.drop(
                        columns=['upor_flag1', 'upor_flag2', 'upper_line1', 'lower_line1', 'upper_line2', 'lower_line2'])

                    # Считаем интервал начала и окончания прогона заготовки
                    g = data
                    lenght_bar = list(g['lenght_bar'])
                    flag_bar = [0 for i in lenght_bar]
                    flag = 0
                    for i in range(1, len(flag_bar) - 1):
                        if lenght_bar[i] > 1 and lenght_bar[i - 1] < 1 and (flag == 0 or flag == 2):
                            flag_bar[i] = 1
                            flag = 1
                        elif lenght_bar[i] > 1 and lenght_bar[i + 1] < 1 and (flag == 0 or flag == 1):
                            flag_bar[i] = 2
                            flag = 2
                    g['flag_bar'] = flag_bar
                    g = g.query("flag_bar == 1 or flag_bar == 2")
                    g = g.reset_index()
                    g = g.rename({'index': 'id_bar'}, axis=1)
                    if g.flag_bar[0] == 2:
                        g = g.iloc[1:, :]
                    if g.flag_bar[len(g) - 1] == 1:
                        g = g.iloc[:-1, :]

                    # Строим таблицу прогона заготовок
                    dt_bar = pd.DataFrame()
                    temp = g
                    temp = temp.query("flag_bar == 1")
                    temp = temp.reset_index()
                    dt_bar['dt_start'] = temp['dt']
                    temp = g
                    temp = temp.query("flag_bar == 2")
                    temp = temp.reset_index()
                    dt_bar['dt_stop'] = temp['dt']
                    dt_bar['flag'] = True
                    dt_bar = dt_bar.loc[dt_bar['dt_stop'].notna()]

                    # Убираем ошибочно добавленные заготовки
                    start = list(dt_bar['dt_start'])
                    stop = list(dt_bar['dt_stop'])
                    flag = list(dt_bar['flag'])
                    for i in range(len(start)):
                        temp_dt = data.query(f"dt >= '{str(start[i])}' and dt <= '{str(stop[i])}'")
                        if temp_dt.empty:
                            flag[i] = False
                            continue
                        elif max(temp_dt['lenght_bar']) < 2500:
                            flag[i] = False
                    dt_bar['flag'] = flag
                    dt_bar = dt_bar.query("flag == True")
                    dt_bar = dt_bar.reset_index()
                    dt_bar = dt_bar.drop(columns=['index', 'flag'])
                    dt_bar = dt_bar.reset_index()
                    dt_bar = dt_bar.rename({'index': 'id_bar'}, axis=1)

                    # Строим таблицу всех воздействий
                    action = data
                    action = action.reset_index()
                    action['flag_action'] = np.where(
                        (action['predetermined_action'] != action['predetermined_action'].shift(1))
                        & (action.index != 0) & (action['predetermined_action'] != 0), 1, 0)
                    action = action.query("flag_action == 1")
                    action = action.drop(columns=['index', 'flag_action'])
                    action = action.reset_index()
                    action = action.drop(columns=['index'])

                    # Группируем по id закотовки
                    start = list(dt_bar['dt_start'])
                    stop = list(dt_bar['dt_stop'])
                    id_bar = list(dt_bar['id_bar'])
                    action['id_bar'] = ''
                    for i in range(len(id_bar)):
                        action['id_bar'] = np.where((start[i] < action['dt']) & (action['dt'] < stop[i]), id_bar[i],
                                                    action['id_bar'])

                    action = action.query("id_bar != ''")
                    action['lenght_bar'] = list(np.ceil(list(action['lenght_bar'] / 10)) * 10)
                    action['flag_first_curv'] = np.where((action['lenght_bar'] != action['lenght_bar'].shift(-1)), 1, 0)

                    # Расчет полученной кривизны в результате воздействия
                    ggg = [0 for i in range(len(action['dt']))][:-2]
                    gg = [0 for i in range(len(action['dt']))][:-2]
                    dtt = list(action[:-2]['dt'])
                    dtt2 = list(action[2:]['dt'])
                    temp_dt = data
                    temp_dt2 = data
                    for i in range(len(ggg) - 2):
                        # temp_dt = data.query(f"dt > '{dtt[i]}'")
                        temp_dt = temp_dt.loc[
                            (data['current_curvature'] != data['current_curvature'].shift(1)) & (data['dt'] > dtt[i])]
                        df = temp_dt2.loc[(data['diff_action'] != data['diff_action'].shift(1)) & (data['dt'] > dtt[i]) & (
                                    data['dt'] < dtt2[i])]
                        if len(df) > 0:
                            gg[i] = list(df['diff_action'])[0]
                        else:
                            gg[i] = 0

                        a = list(temp_dt['current_curvature'])
                        ggg[i] = a[0]
                    action = action[:-2]
                    action['res_curv'] = ggg
                    action['difference'] = gg
                    action = action.loc[action['lenght_bar'] != action['lenght_bar'].shift(1)]

                    # Вытягиваем номенклатуру
                    g = action.groupby(['id_packed']).mean()
                    g = g.reset_index()
                    id_packed = g['id_packed']
                    action['nomen'] = action['nomen'].astype(str)
                    action['series'] = action['series'].astype(str)
                    action['nomen'] = action['nomen'] + ', ' + action['series']
                    action = action.reset_index()
                    action['flag_first_curv'] = action['flag_first_curv'].map({1: True, 0: False})

                    # Оставляем только воздействия на первую заготовку
                    action = action[action['dt'] > mydate]
                    action = action.reset_index()
                    action = action[action['id_bar'] == action['id_bar'][0]]

                    # Вытягиваем доп данные
                    packed = int(list(action['id_packed'])[0])
                    nom = list(pd.read_sql(self.pg_query_get_nom.format(packed), self.pgsql_work_con)['ref_nom'])[0]
                    pack_info = pd.read_sql(self.pg_query_get_pack.format(packed), self.pgsql_work_con)
                    pack = list(pack_info['pack_full_number'])[0]
                    ref_nlz = list(pack_info['ref_nlz_info'])[0]
                    plav = list(pd.read_sql(self.pg_query_get_plav.format(ref_nlz), self.pgsql_work_con)['plav_num'])[0]
                    max_bar = list(pd.read_sql(self.pg_query_get_maxbar, self.pgsql_ds_con)['bar'])[0]

                    # Записываем в список и отправляем в базу
                    rows = []
                    for i in range(len(action)):
                        newrow = []
                        newrow.append(max_bar + 1)
                        newrow.append(list(action['current_curvature'])[i])
                        newrow.append(list(action['predetermined_action'])[i])
                        newrow.append(list(action['res_curv'])[i])
                        newrow.append(list(action['lenght_bar'])[i])
                        newrow.append(list(action['flag_first_curv'])[i])
                        newrow.append(list(action['upper_line'])[i])
                        newrow.append(list(action['lower_line'])[i])
                        newrow.append(list(action['position'])[i])
                        newrow.append(list(action['difference'])[i])
                        newrow.append(list(action['nomen'])[i])
                        newrow.append(packed)
                        newrow.append(nom)
                        newrow.append(plav)
                        newrow.append(pack)
                        newrow.append(list(action['dt'])[i])
                        rows.append(newrow)
                    self.pgsql_ds_con.execute(self.pg_query_insert_actions, rows)
                    self.maindt = mydate
                    self.logWritter(self.script_name, self.file_directory, self.log_stat[0])
                    self.statusUpdater(self.script_name, self.file_directory, 1)
                    time.sleep(5)
                else:
                    self.logWritter(self.script_name, self.file_directory, self.log_stat[1])
                    self.statusUpdater(self.script_name, self.file_directory, 1)
                    time.sleep(2)
            else:
                self.logWritter(self.script_name, self.file_directory, self.log_stat[2])
                self.statusUpdater(self.script_name, self.file_directory, 1)
                time.sleep(2)

        except:
            self.logWritter(self.script_name, self.file_directory, str(traceback.format_exc()))
            self.statusUpdater(self.script_name, self.file_directory, 2)


process = AclAddNewAction()
process.maindt = datetime.datetime.now()
while True:
    try:
        process.main(1)
    except:
        print(traceback.format_exc())
