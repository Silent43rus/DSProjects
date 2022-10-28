# Скрипт для расчета производительности по 500 ст за каждый час

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


class MycustomError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return 'MyCustomError, {0} '.format(self.message)
        else:
            return 'MyCustomError has been raised'


start_date = '29012022'                             # Начальная дата
end_date = '30012022'                               # Конечная дата
parent_dir = 'C:/'                                  # Путь к выходной папке с результатами

user_time = 360
nameTable1 = f'gidrosbiv_{start_date}_{end_date}.txt'
input_path = f"{parent_dir}DataPreprocessing_spc500_{start_date}_{end_date}/"
nameTable2 = 'energytab.txt'
if not os.path.exists(input_path + nameTable1):
    ex = "Incorrect data entry"
    raise MycustomError(ex)
if os.path.exists(input_path + f'result_1_{start_date}_{end_date}.xlsx'):
    ex = "This script has already been executed"
    raise MycustomError(ex)
perftab = pd.read_csv(input_path + nameTable1)
energytab = pd.read_csv(input_path + nameTable2)

# Связываем энергоучет с гидросбивом

result = pd.merge(energytab, perftab, how="inner", on=["yyyy", "mm", "dd", 'hh'])

# Добавляем поле myflag с признаком прогона заготовки
result['myflag'] = ''
result['myflag'] = np.where((result['gidroflag'] == 1) & (result['gidroflag'].shift(1) == 1), 0, result['myflag'])
result['myflag'] = np.where((result['gidroflag'] == 1) & (result['gidroflag'].shift(1) == 0), 1, result['myflag'])
result['myflag'] = np.where((result['gidroflag'] == 0), 0, result['myflag'])

# Добавляем поле gazrash с расходом газа по интервалам

result['gazrash'] = np.where((result['gazcount'] < result['gazcount'].shift(-1)),
                             (result['gazcount'].shift(-1) - result['gazcount']), 0)

result['myflag'] = pd.to_numeric(result['myflag'])
# Пишем в таблицу counperf промежуточный результат расчета производительности

temp = result.groupby(['realtime']).sum()
countperf = pd.DataFrame({'perfcount': []})
countperf['perfcount'] = temp['myflag']

# Пишем в таблицу countenergy промежуточный результат расчета расхода электроэнергии

temp = result.groupby(['realtime']).mean()
countenergy = pd.DataFrame({'energycount': []})
countenergy['energycount'] = temp['rashodsumm']

# Пишем в таблицу countgaz промежуточный результат расчета расхода газа

temp = result.groupby(['realtime']).sum()
countgaz = pd.DataFrame({'gazcount': []})
countgaz['gazcount'] = np.round(temp['gazrash'], 1)

# Связываем все таблицы по времени

res = pd.merge(countperf, countgaz, how='inner', on=['realtime'])
res = pd.merge(res, countenergy, how='inner', on=['realtime'])
res = res.reset_index()
res['realtime'] = pd.to_datetime(res['realtime'])
#        dt -idle_start_time::text::interval::time without time zone dt_start,
sql_query_prostoi = """ SELECT * 
FROM (SELECT id_inc id_int_dt, 
        dt,
        case when(idle_time) != 0 
        then(idle_time - case when idle_start_time > 110 
                then(idle_start_time - 109)
                 else idle_start_time 
                end)::text::interval::time without time zone else null 
        end idle,
        case when(idle_time) != 0 
        then dt +(idle_time - idle_start_time - case 
                when idle_start_time> 110 
                then(idle_start_time - 109) * 2 
                else 0 
         end )::text::interval::time without time zone  else null end dt_end,
        case when id_vid is Null
                then 0 
                else id_vid
         end id_vid,
        kl_select,
        idle_time,
        manual_enter,
        idle_start_time,
        nastr_invisible_row,
        deleted
FROM spc500_l2.prostoi) tab1
WHERE idle_time >= {} and nastr_invisible_row is Null 
        and dt > '{}' and dt < '{}'
        and deleted is Null
        and (manual_enter is Null or id_vid != 0)
        and ((id_vid < 6) or (id_vid = 6 and (kl_select like '%%1%%' or kl_select like '%%2%%')))
ORDER BY dt
"""

pgsql_connection = ''
pgsql_con_backup = create_engine(pgsql_connection)

downtimetab = pd.read_sql_query(sql_query_prostoi.format(user_time, min(res['realtime']), max(res['realtime'])), pgsql_con_backup)
downtimetab['id_vid'] = pd.to_numeric(downtimetab['id_vid']).astype(int)
downtimetab['kl_select'] = downtimetab['kl_select'].astype(str)


# Сравнение дат

def sr_date(a, b):
    if a.year == b.year and a.month == b.month and a.day == b.day and a.hour == b.hour:
        return True
    else:
        return False


# Обновление строки видов

def check_vid(check, val, t, it, vid0, vid1, vid2, vid3, vid4, vid5, vid6):
    if check == 0:
        vid0[it] += val
    elif check == 1:
        vid1[it] += val
    elif check == 2:
        vid2[it] += val
    elif check == 3:
        vid3[it] += val
    elif check == 4:
        vid4[it] += val
    elif check == 5:
        vid5[it] += val
    elif check == 6:
        vid6[it] += val


# Расчет простоев по часу

def downtime_calc():
    t = res['realtime']
    vid1 = [0 for i in t]
    vid2 = [0 for i in t]
    vid3 = [0 for i in t]
    vid4 = [0 for i in t]
    vid5 = [0 for i in t]
    vid6 = [0 for i in t]
    vid0 = [0 for i in t]
    summ_d = [0 for i in t]
    ostt = [0 for i in t]
    down_time = downtimetab['idle_time'] - downtimetab['idle_start_time']
    down_dt = downtimetab['dt']
    down_vid = downtimetab['id_vid']

    prost = 0
    ost = 0
    ost_v = 0
    flag_hour = False

    for it in range(len(t)):
        for i in range(len(down_time)):
            tek_vid = down_vid[i]
            if sr_date(down_dt[i], t[it]) and ost >= down_dt[i].minute * 60 + down_dt[i].second:
            	ost = (down_dt[i].minute * 60 + down_dt[i].second) - 1
            if sr_date(down_dt[i], t[it]) and ost < down_dt[i].minute * 60 + down_dt[i].second:
                prost = 3600 - (down_dt[i].minute * 60 + down_dt[i].second)
                if down_time[i] > 3600:
                    summ_d[it] += ost + prost
                    check_vid(tek_vid, prost, t, it, vid0, vid1, vid2, vid3, vid4, vid5, vid6)
                    check_vid(ost_v, ost, t, it, vid0, vid1, vid2, vid3, vid4, vid5, vid6)
                    ost = down_time[i] - prost
                    ost_v = tek_vid
                    break
                else:
                    if ost > 3600:
                        ost -= 3600
                        ost_v = tek_vid
                        summ_d[it] = 3600
                        check_vid(tek_vid, 3600, t, it, vid0, vid1, vid2, vid3, vid4, vid5, vid6)
                        continue
                    else:
                        if ost != 0:
                            summ_d[it] += ost
                            check_vid(ost_v, ost, t, it, vid0, vid1, vid2, vid3, vid4, vid5, vid6)
                        if prost > down_time[i]:
                            summ_d[it] += down_time[i]
                            ost = 0
                            ost_v = tek_vid
                            check_vid(tek_vid, down_time[i], t, it, vid0, vid1, vid2, vid3, vid4, vid5, vid6)
                            continue
                        else:
                            ost = down_time[i] - prost
                            ost_v = tek_vid
                            summ_d[it] += prost
                            check_vid(tek_vid, prost, t, it, vid0, vid1, vid2, vid3, vid4, vid5, vid6)
            else:
                flag_hour = False
                for j in down_dt:
                    if sr_date(j, t[it]):
                        flag_hour = True
                        break
                if not flag_hour:
                    if ost > 3600:
                        ost -= 3600
                        summ_d[it] = 3600
                        check_vid(ost_v, 3600, t, it, vid0, vid1, vid2, vid3, vid4, vid5, vid6)
                        break
                    else:
                        summ_d[it] += ost
                        check_vid(ost_v, ost, t, it, vid0, vid1, vid2, vid3, vid4, vid5, vid6)
                        ost = 0
                        break
        ostt[it] = ost

    return summ_d, ostt, vid0, vid1, vid2, vid3, vid4, vid5, vid6


summ, ostt, v0, v1, v2, v3, v4, v5, v6 = downtime_calc()

res['downtime'] = summ
res['vid0'] = v0
res['vid1'] = v1
res['vid2'] = v2
res['vid3'] = v3
res['vid4'] = v4
res['vid5'] = v5
res['vid6'] = v6

res.to_excel(input_path + f'result_1_{start_date}_{end_date}.xlsx')
output_table_1 = res