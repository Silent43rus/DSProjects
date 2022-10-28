# Скрипт для расчета итоговой таблицы

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os

start_date = '11082022'
end_date = '11082022'
input_path = ''
line = 'LINE2'
input_path = os.path.join(input_path, line)

pgsql_connection = ''
pgsql_con_backup = create_engine(pgsql_connection)
sql_get_nomen = """ SELECT description
                        FROM sved.sved_current_info_serial_arh
                        WHERE ref_serial = 'sved.sved_packet_info,{}' and description like '%%равка%%'
                        ORDER BY dt DESC
                        LIMIT 1"""


path = f"{input_path}/DataPreprocessing_cln_CurvAnalysis_{start_date}_{end_date}/"
file_name = f"cln_press_{start_date}_{end_date}.txt"
data = pd.read_csv(path + file_name, sep = ",", low_memory = False)
data['dt'] = pd.to_datetime(data['dt'])

# Расчет верхней и нижней граници кривизны

f1 = data['upor_flag1']
f2 = data['upor_flag2']
pos = [0 for _ in f1]
up1 = data['upper_line1'] # 11
up2 = data['upper_line2'] # 8
low1 = data['lower_line1'] # 12
low2 = data['lower_line2'] # 10
up = [None for _ in f1]
low = [None for _ in f1]

c = 0
for i in range(len(f1)):
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
data = data.drop(columns=['upor_flag1', 'upor_flag2', 'upper_line1', 'lower_line1', 'upper_line2', 'lower_line2'])

# Считаем интервал начала и окончания прогона заготовки

g = data
lenght_bar = list(g['lenght_bar'])
flag_bar = [0 for i in lenght_bar]
flag = 0
for i in range(1, len(flag_bar)-1):
    if lenght_bar[i] > 1 and lenght_bar[i-1] < 1 and (flag == 0 or flag == 2):
        flag_bar[i] = 1
        flag = 1
    elif lenght_bar[i] > 1 and lenght_bar[i+1] < 1 and (flag == 0 or flag == 1):
        flag_bar[i] = 2
        flag = 2
g['flag_bar'] = flag_bar
g = g.query("flag_bar == 1 or flag_bar == 2")
g = g.reset_index()
g = g.rename({'index' : 'id_bar'}, axis = 1)
if g.flag_bar[0] == 2:
    g = g.iloc[1:,:]
if g.flag_bar[len(g)-1] == 1:
    g = g.iloc[:-1,:]

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
    elif max(temp_dt['lenght_bar']) < 3000:
        flag[i] = False

dt_bar['flag'] = flag
dt_bar = dt_bar.query("flag == True")
dt_bar = dt_bar.reset_index()
dt_bar = dt_bar.drop(columns = ['index', 'flag'])

dt_bar = dt_bar.reset_index()
dt_bar = dt_bar.rename({'index' : 'id_bar'}, axis = 1)

# Строим таблицу всех воздействий

action = data
action = action.reset_index()
action['flag_action'] = np.where((action['predetermined_action'] != action['predetermined_action'].shift(1))
                                 & (action.index != 0) & (action['predetermined_action'] != 0), 1, 0)

action = action.query("flag_action == 1")
action = action.drop(columns = ['index', 'flag_action'])
action = action.reset_index()
action = action.drop(columns = ['index'])

# Группируем по id закотовки

start = list(dt_bar['dt_start'])
stop = list(dt_bar['dt_stop'])
id_bar = list(dt_bar['id_bar'])

action['id_bar'] = ''
for i in range(len(id_bar)):
    action['id_bar'] = np.where((start[i] < action['dt']) & (action['dt'] < stop[i]), id_bar[i], action['id_bar'])

action = action.query("id_bar != ''")
action['lenght_bar'] = list(np.ceil(list(action['lenght_bar'] / 10)) * 10)
action['flag_first_curv'] = np.where((action['lenght_bar'] != action['lenght_bar'].shift(-1)), 1, 0)

# Расчет полученной кривизны в результате воздействия

ggg = [0 for i in range(len(action['dt']))][:-2]
gg  = [0 for i in range(len(action['dt']))][:-2]
dtt = list(action[:-2]['dt'])
dtt2 = list(action[2:]['dt'])
temp_dt = data
temp_dt2 = data
for i in range(len(ggg)-2):
    #temp_dt = data.query(f"dt > '{dtt[i]}'")
    temp_dt = temp_dt.loc[(data['current_curvature'] != data['current_curvature'].shift(1)) & (data['dt'] > dtt[i])]
    df = temp_dt2.loc[(data['diff_action'] != data['diff_action'].shift(1)) & (data['dt'] > dtt[i]) & (data['dt'] < dtt2[i])]
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
action['nomen'] = action['nomen'] + action['series']
#action['nomen'] = ''
#for i in id_packed:
#    nom_df = pd.read_sql_query(sql_get_nomen.format(i), pgsql_con_backup)
#    if len(nom_df) == 0 :
#        action['nomen'] = np.where(action['id_packed'] == i, "Unidentified packet № " + str(i), action['nomen'])
#    else:
#        a = nom_df['description'][0]
#        action['nomen'] = np.where(action['id_packed'] == i, a[:a.find(',') + 2] + a[a.find(','):][(a[a.find(','):].find('(')):], action['nomen'])


result = pd.DataFrame()
result['id_bar'] = action['id_bar']
result['dt'] = action['dt']
result['curvature'] = action['current_curvature']
result['action'] = action['predetermined_action']
result['out_curvature'] = action['res_curv']
result['lenght_bar'] = action['lenght_bar']
result['first_correct'] = action['flag_first_curv']
result['up'] = action['upper_line']
result['low'] = action['lower_line']
result['position'] = action['position']
result['diff_action'] = action['difference']
result['nomen'] = action['nomen']
output_path = path + f"actions_{file_name[10:-4]}.xlsx"
result = result[:-1]

result.to_excel(output_path, index=False)