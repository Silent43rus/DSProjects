from django.shortcuts import render
import json

from django.shortcuts import render
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime
from django.http import HttpResponse
import traceback
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

# Create your views here.

def regr(x,y):
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size = 1/3)
    lr = LinearRegression()
    lr.fit(x, y)
    return lr

def calcAccuracy(temp):
    a = []
    cc = min(temp['action'])
    while cc < max(temp['action']):
        a.append(round(cc, 2))
        cc += 0.1
    ud = [0 for _ in a]
    ln = [0 for _ in a]
    for i in range(len(a)):
        if i > 0:
            ln[i] = len(temp.loc[(temp['action'] > a[i-1]) & (temp['action'] <= a[i])])
            ud[i] = len(temp.loc[(temp['action'] > a[i-1]) & (temp['action'] <= a[i]) & (temp['first_correct'] == 1)])
        else:
            ln[i] = len(temp.loc[(temp['action'] > 0) & (temp['action'] <= a[i])])
            ud[i] = len(temp.loc[(temp['action'] > 0) & (temp['action'] <= a[i]) & (temp['first_correct'] == 1)])
    acc = []
    for i in range(len(ln)):
        acc.append(ud[i]/ln[i]*100) if ln[i] > 0 else acc.append(0)
    return [str(i) for i in a], [str(i) for i in acc]

def index(request):
    context = {}
    

    return render(request, 'mainapplication/base.html', context)

def acl(request):
    context = {}

    pg_query_get_tab = """SELECT * FROM "acl"."line{}_curvature_table" WHERE dt_start < '{}' and (dt_stop > '{}' or dt_stop is null)"""
    pg_query_get_packed = """SELECT id_packed FROM "acl"."line{}_actions" where dt_action > '{}' and dt_action < '{}' and tilter_pos = 2  GROUP BY id_packed """
    pg_query_get_nomen = """SELECT nomen FROM "acl"."line{}_actions" where id_packed = {} order by id limit 1"""
    pg_query_get_actions = """SELECT * FROM "acl"."line{}_actions" where id_packed = '{}' and dt_action > '{}' and dt_action < '{}' and tilter_pos = 2 order by id """
    if request.method == "POST":
        pgsql_connection = ''
        connection = create_engine(pgsql_connection)
        param = json.loads(request.body)
        if ('start_dt' in param and 'stop_dt' in param and 'line_num' in param):
            start_dt = datetime.datetime.strptime(param['start_dt'], '%Y-%m-%dT%H:%M')
            stop_dt = datetime.datetime.strptime(param['stop_dt'], '%Y-%m-%dT%H:%M')
            line = param['line_num'][0]
            if start_dt != '' and stop_dt != '':
                df_tab = pd.read_sql(pg_query_get_tab.format(line, stop_dt, start_dt), connection)
                df_tab = df_tab.reset_index()
                df_tab['dt_stop'] = df_tab['dt_stop'].fillna(datetime.datetime.now())
                tables = []
                for i in range(len(df_tab)):
                    start = start_dt if list(df_tab['dt_start'])[i] < start_dt else list(df_tab['dt_start'])[i]
                    stop = stop_dt if list(df_tab['dt_stop'])[i] > stop_dt else list(df_tab['dt_stop'])[i]
                    tables.append(str(start)[:16] + '_to_' + str(stop)[:16])
                context = {"tables" : tables, "start_dt" : param['start_dt'], "stop_dt" : param['stop_dt']}
                return HttpResponse(json.dumps(context))

        elif ('select_period' in param and 'line_num' in param):
            req = param['select_period']
            line = param['line_num'][0]
            period = req.split('_to_')
            packed = list(pd.read_sql(pg_query_get_packed.format(line, period[0], period[1]), connection)['id_packed'])
            nomen = []
            for pack in packed:
                nomen.append(list(pd.read_sql(pg_query_get_nomen.format(line, pack), connection)['nomen'])[0])
            context = {'nomen' : nomen, 'period' : period, 'tables' : [req], 'packed' : packed}
            return HttpResponse(json.dumps(context))

        elif ('select_packed' in param and 'start_table' in param and 'stop_table' in param and 'line_num' in param):
            id_nom = param['select_packed']
            start_table = param['start_table']
            stop_table = param['stop_table']
            line = param['line_num'][0]
            df_action = pd.read_sql(pg_query_get_actions.format(line, id_nom, start_table, stop_table), connection)
            df_action = df_action.reset_index()
            nomen = list(df_action['nomen'])[0]
            count_bar = len(df_action.groupby(['bar']).mean())
            count_actions = len(df_action)
            accuracy = round(round(len(df_action[df_action['first_correct'] == 1]) / len(df_action), 3) * 100, 2)

            # Отрицательные взд
            data = df_action.loc[df_action['action'] < 0]
            neg_overfill =  len(data.loc[(data['first_correct'] == 0) & (data['out_curvature'] < 0)])
            neg_underfill = len(data.loc[(data['first_correct'] == 0) & (data['out_curvature'] > 0)])
            neg_accuracy = round(len(data.loc[(data['first_correct'] == 1)]) / len(data) * 100, 1)
            neg_action = [str(round(i, 2)) for i in list(data['action'])]
            neg_curvature = [str(i) for i in list(data['curvature'])]
            neg_out_curvature = [str(i) for i in list(data['out_curvature'])]
            neg_firs_correct = [str(i) for i in list(data['first_correct'])]
            neg_x, neg_y = calcAccuracy(data)
            lx = np.array(data['curvature']).reshape(-1, 1)
            ly = np.array(data['action']).reshape(-1, 1)
            l = regr(lx, ly)
            l = l.predict(lx)
            neg_l = [str(round(list(i)[0], 2)) for i in l]



            # Положительные взд
            data = df_action.loc[df_action['action'] > 0]
            pos_overfill = len(data.loc[(data['first_correct'] == 0) & (data['out_curvature'] > 0)])
            pos_underfill = len(data.loc[(data['first_correct'] == 0) & (data['out_curvature'] < 0)])
            pos_accuracy = round(len(data.loc[(data['first_correct'] == 1)]) / len(data) * 100, 1)
            pos_action = [str(i) for i in list(data['action'])]
            pos_curvature = [str(i) for i in list(data['curvature'])]
            pos_out_curvature = [str(i) for i in list(data['out_curvature'])]
            pos_firs_correct = [str(i) for i in list(data['first_correct'])]
            pos_x, pos_y = calcAccuracy(data)
            lx = np.array(data['curvature']).reshape(-1, 1)
            ly = np.array(data['action']).reshape(-1, 1)
            l = regr(lx, ly)
            l = l.predict(lx)
            pos_l = [str(i) for i in l]
            dt_start = str(list(df_action['dt_action'])[0])[:-10]
            dt_stop = str(list(df_action['dt_action'])[-1])[:-10]
            context = {"nomen" : nomen,
                        "dt_start" : dt_start,
                        "dt_stop" : dt_stop,
                        "count_bar" : count_bar,
                        "count_actions" : count_actions,
                        "accuracy" : accuracy,
                        "neg_overfill" : str(neg_overfill),
                        "neg_underfill" : str(neg_underfill),
                        "neg_accuracy" : str(neg_accuracy),
                        "pos_overfill" : str(pos_overfill),
                        "pos_underfill" : str(pos_underfill),
                        "pos_accuracy" : str(pos_accuracy),
                        "neg_action" : neg_action,
                        "neg_out_curvature" : neg_out_curvature,
                        "neg_firs_correct" : neg_firs_correct,
                        "neg_curvature" : neg_curvature,
                        "neg_l" : neg_l,
                        "pos_action" : pos_action,
                        "pos_out_curvature" : pos_out_curvature,
                        "pos_firs_correct" : pos_firs_correct,
                        "pos_curvature" : pos_curvature,
                        "pos_l" : pos_l,
                        "neg_x" : neg_x,
                        "neg_y" : neg_y,
                        "pos_x" : pos_x,
                        "pos_y" : pos_y

                        }
            return HttpResponse(json.dumps(context))
    return render(request, 'mainapplication/acl.html', context)


def update_plotsAatp3(dfvisual, dfmean, list_mean, res_list, tekparam):
    df = dfvisual
    xdata = [i for i in range(len(dfmean))]
    ydata = [i for i in range(len(dfmean))]
    listcolors = [i for i in range(len(dfmean))]
    for i in range(len(dfmean)):
        xdata[i] = np.array(df[df['num_proh'] == i + 1]['dt_start'])
        ydata[i] = np.array(df[df['num_proh'] == i + 1][list_mean[tekparam]])
        listcolors[i] = np.array(df[df['num_proh'] == i + 1][res_list[tekparam]])
    x = []
    for arr in xdata:
        xx = []
        for i in arr:
            xx.append(str(i)[:19].replace('T', ' '))
        x.append(xx)
    y = []
    for arr in ydata:
        yy = []
        for i in arr:
            yy.append(float(i))
        y.append(yy)
    z = []
    for arr in listcolors:
        zz = []
        for i in arr:
            zz.append(str(i))
        z.append(zz)
    return x, y, z

def update_validparamAatp3(valparam):
    stats = []
    for i in range(len(valparam)):
        q1 = float(valparam['q1'][i])
        mid = float(valparam['q2'][i])
        q3 = float(valparam['q3'][i])
        x1 = float(valparam['x1'][i])
        x2 = float(valparam['x2'][i])
        stats.append({'med': round(mid, 2),
                        'q1': round(q1, 2),
                        'q3': round(q3, 2),
                        'whislo': round(x1, 2),
                        'whishi': round(x2, 2)
                        })
    return stats
    
# Обработка запросов со страницы ААТП 3 клети
def aatp3(request):

    pg_query_read_profiles = """SELECT *
                                        FROM aatp.acceptable_profiles
                                        WHERE ref_location = 'nsi.spr_terminal_techpa_location,51' """
    pg_query_read_periods = """ SELECT m.ref_nomenclature, m.description_nomenclature, m.ref_profile, t.dt_start, t.dt_stop, t.dt as dt
                                        FROM (
                                            SELECT ref_nomenclature, MIN(dt_start) as dt_start, MAX(dt_stop) as dt_stop, dt_start::DATE as dt
                                            FROM aatp.stand3_passageway
                                            GROUP BY ref_nomenclature, dt_start::DATE
                                            ) t JOIN aatp.stand3_passageway m ON m.ref_nomenclature = t.ref_nomenclature AND t.dt_start = m.dt_start
                                        ORDER BY t.dt """     
    pg_query_read_profile = """ SELECT profile_name, passages_num
                                    FROM aatp.acceptable_profiles
                                    WHERE obj_id = '{}' """   
    pg_query_read_passageway = """ SELECT dt_start, dt_stop, passage as num_proh, amperage_mean, amperage_max, speed as speed_vs,
                                                temperature as temp, interformation_pause as independ_pause, id_posad, vibration_mean as vibration1_mean,
                                                vibration_max as vibration1_max, deviation_amperage as res_amperage, deviation_vibration_mean as res_vibr1_mean,
                                                deviation_vibration_max as res_vibr1_max, deviation_interformation_pause as res_independ_pause, deviation_speed as res_speed, 
                                                description_nomenclature as nomen
                                        FROM aatp.stand3_passageway
                                        WHERE ref_profile = '{}' and dt_start > '{}' and dt_stop < '{}'
                                        order by id """                  
    pg_query_read_valpar = """SELECT passage, q1,q2,q3,x1,x2
                                            FROM aatp.acceptable_values 
                                            WHERE ref_profile = '{}' and ref_parameter = '{}'"""
    ruparam = ['Ток', 'Скорость', 'Паузы перед проходами', 'Средняя вибрация', 'Максимальная вибрация']
    list_mean = ['amperage_mean', 'speed_vs', 'independ_pause', 'vibration1_mean',
                     'vibration1_max', 'num_proh']
    res_list = ['res_amperage', 'res_speed', 'res_independ_pause', 'res_vibr1_mean', 'res_vibr1_max']
    pg_param_name = ['nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,11', 'nsi.spr_terminal_techpa,10',
                         'nsi.spr_terminal_techpa,12', 'nsi.spr_terminal_techpa,13']                                                                                                                                                                                                   
    context = {}
    if request.method == "POST":
        pgsql_connection = ''
        connection = create_engine(pgsql_connection)
        param = json.loads(request.body)
        
        # Инициализация данных по профилям
        if 'init_param' in param:
            df = pd.read_sql_query(pg_query_read_profiles, connection)
            context = {'profiles' :  list(df['profile_name']), 'ref_profiles' : list(df['obj_id'])}
            return HttpResponse(json.dumps(context))
        
        # Обработка запроса на таблицу проката по дате или профилю
        elif 'profile_choice' in param:
            df = pd.read_sql(pg_query_read_periods, connection)
            if param['profile_choice'] == 'profile':
                df = df[df['ref_profile'] == param['data']]
            else:
                df = df[df['dt'].astype(str) == param['data']]
            df = df.reset_index()
            df = df.drop(columns=['index'])
            context = {'ref_profile' : list(df['ref_profile']), 'description_profile' : list(df['description_nomenclature']),
                       'dt_start' : [str(i)[:-7] for i in list(df['dt_start'])], 'dt_stop' : [str(i)[:-7] for i in list(df['dt_stop'])], 'dt' : [str(i) for i in list(df['dt'])]}
            return HttpResponse(json.dumps(context))

        # Обработка запроса на формирование отчета по выбранному профилю, параметру и времени масштабирования
        elif 'raport_par' in param:
            tekparam = int(param['tekparam'])
            refnomen = param['refnomen']
            startperiod = param['startperiod']
            stopperiod = param['stopperiod']
            tekdata = param['tekdata']
            starttime = param['starttime']
            endtime = param['endtime']
            count_pass = int(list(pd.read_sql_query(pg_query_read_profile.format(refnomen), connection)['passages_num'])[0])
            valparam = pd.read_sql_query(pg_query_read_valpar.format(refnomen,pg_param_name[tekparam]), connection)
            dfvisual = pd.read_sql_query(pg_query_read_passageway.format(refnomen, startperiod, stopperiod), connection)
            dfvisual = dfvisual[(dfvisual['dt_start'] > f'{tekdata} {starttime}') & (dfvisual['dt_stop'] < f'{tekdata} {endtime}')]
            dfmean = dfvisual[int(f'-{count_pass}'):]
            xd, yd, listcol = update_plotsAatp3(dfvisual, dfmean, list_mean, res_list, tekparam)
            val_stats = update_validparamAatp3(valparam)
            context = {'xdata': xd, 'ydata': yd, 'listcol': listcol,
                       'ruparam': ruparam, 'count_pass': count_pass, 
                       'tekparam': tekparam, 'val_stats': val_stats}
            return HttpResponse(json.dumps(context))


    return render(request, 'mainapplication/aatp3.html', context)

# Обработка запросов со страницы ААТП резки
def aatp_rezka(request):

    pg_query_read_profiles = """SELECT *
                                        FROM aatp.acceptable_profiles
                                        WHERE ref_location = 'nsi.spr_terminal_techpa_location,52' """
    pg_query_read_periods = """ SELECT m.ref_nomenclature, m.description_nomenclature, m.ref_profile, t.dt_start, t.dt_stop, t.dt as dt
                                        FROM (
                                            SELECT ref_nomenclature, MIN(dt_start) as dt_start, MAX(dt_stop) as dt_stop, dt_start::DATE as dt
                                            FROM aatp.cutting_cuts
                                            GROUP BY ref_nomenclature, dt_start::DATE
                                            ) t JOIN aatp.cutting_cuts m ON m.ref_nomenclature = t.ref_nomenclature AND t.dt_start = m.dt_start
                                        ORDER BY t.dt """     
    pg_query_read_profile = """ SELECT profile_name, passages_num
                                    FROM aatp.acceptable_profiles
                                    WHERE obj_id = '{}' """   
    pg_query_read_passageway = """ SELECT dt_start, dt_stop, cut_number as num_proh, amperage_mean, amperage_max, power_mean,
                                                power_max, vibration_mean, vibration_max, interformation_pause,
                                                time_cycle, time_rez, disc_speed, disc_diameter, id_posad, deviation_amperage_mean as res_amperage_mean, deviation_amperage_max as res_amperage_max,
                                                deviation_power_mean as res_power_mean, deviation_power_max as res_power_max,deviation_vibration_mean as res_vibration_mean,
                                                deviation_vibration_max as res_vibration_max, deviation_interformation_pause as res_interformation_pause, deviation_time_cycle as res_time_cycle,
                                                deviation_time_rez as res_time_rez, deviation_disc_speed as res_disc_speed, 
                                                deviation_disc_diameter as res_disc_diameter, description_nomenclature as nomen
                                        FROM aatp.cutting_cuts
                                        WHERE ref_profile = '{}' and dt_start > '{}' and dt_stop < '{}'
                                        order by id """                  
    pg_query_read_valpar = """SELECT passage, q1,q2,q3,x1,x2
                                            FROM aatp.acceptable_values 
                                            WHERE ref_profile = '{}' and ref_parameter = '{}'"""
    ruparam = ['Средний Ток', 'Максимальный Ток', 'Средняя мощность', 'Максимаьлная мощность', 'Средняя вибрация', 'Максимальная вибрация',
                   'Паузы перед проходами', 'Время цикла', 'Время реза', 'Скорость вращения диска', 'Диаметр диска']
    list_mean = ['amperage_mean', 'amperage_max', 'power_mean', 'power_max', 'vibration_mean',
                     'vibration_max', 'interformation_pause', 'time_cycle', 'time_rez',
                     'disc_speed', 'disc_diameter', 'num_proh']
    res_list = ['res_amperage_mean', 'res_amperage_max', 'res_power_mean', 'res_power_max', 'res_vibration_mean',
                     'res_vibration_max', 'res_interformation_pause', 'res_time_cycle', 'res_time_rez',
                     'res_disc_speed', 'res_disc_diameter']
    pg_param_name = ['nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,14', 'nsi.spr_terminal_techpa,15',
                         'nsi.spr_terminal_techpa,16', 'nsi.spr_terminal_techpa,12', 'nsi.spr_terminal_techpa,13',
                         'nsi.spr_terminal_techpa,10', 'nsi.spr_terminal_techpa,17', 'nsi.spr_terminal_techpa,18',
                         'nsi.spr_terminal_techpa,19', 'nsi.spr_terminal_techpa,20']                                                                                                                                                                                              
    context = {}
    if request.method == "POST":
        pgsql_connection = ''
        connection = create_engine(pgsql_connection)
        param = json.loads(request.body)

        # Инициализация данных по профилям
        if 'init_param' in param:
            df = pd.read_sql_query(pg_query_read_profiles, connection)
            context = {'profiles' :  list(df['profile_name']), 'ref_profiles' : list(df['obj_id'])}
            return HttpResponse(json.dumps(context))
        
        # Обработка запроса на таблицу проката по дате или профилю
        elif 'profile_choice' in param:
            df = pd.read_sql(pg_query_read_periods, connection)
            if param['profile_choice'] == 'profile':
                df = df[df['ref_profile'] == param['data']]
            else:
                df = df[df['dt'].astype(str) == param['data']]
            df = df.reset_index()
            df = df.drop(columns=['index'])
            context = {'ref_profile' : list(df['ref_profile']), 'description_profile' : list(df['description_nomenclature']),
                       'dt_start' : [str(i)[:-7] for i in list(df['dt_start'])], 'dt_stop' : [str(i)[:-7] for i in list(df['dt_stop'])], 'dt' : [str(i) for i in list(df['dt'])]}
            return HttpResponse(json.dumps(context))
        
        # Обработка запроса на формирование отчета по выбранному профилю, параметру и времени масштабирования
        elif 'raport_par' in param:
            tekparam = int(param['tekparam'])
            refnomen = param['refnomen']
            startperiod = param['startperiod']
            stopperiod = param['stopperiod']
            tekdata = param['tekdata']
            starttime = param['starttime']
            endtime = param['endtime']
            count_pass = int(list(pd.read_sql_query(pg_query_read_profile.format(refnomen), connection)['passages_num'])[0])
            valparam = pd.read_sql_query(pg_query_read_valpar.format(refnomen,pg_param_name[tekparam]), connection)
            dfvisual = pd.read_sql_query(pg_query_read_passageway.format(refnomen, startperiod, stopperiod), connection)
            dfvisual = dfvisual[(dfvisual['dt_start'] > f'{tekdata} {starttime}') & (dfvisual['dt_stop'] < f'{tekdata} {endtime}')]
            dfmean = dfvisual[int(f'-{count_pass}'):]
            xd, yd, listcol = update_plotsAatp3(dfvisual, dfmean, list_mean, res_list, tekparam)
            val_stats = update_validparamAatp3(valparam)
            context = {'xdata': xd, 'ydata': yd, 'listcol': listcol,
                       'ruparam': ruparam, 'count_pass': count_pass, 
                       'tekparam': tekparam, 'val_stats': val_stats}
            return HttpResponse(json.dumps(context))

    return render(request, 'mainapplication/aatp_rezka.html', context)

def aatp_45(request):

    pg_query_read_profiles = """SELECT *
                                        FROM aatp.acceptable_profiles
                                        WHERE ref_location = 'nsi.spr_terminal_techpa_location,85' """
    pg_query_read_periods = """ SELECT m.ref_nomenclature, m.description_nomenclature, m.ref_profile, t.dt_start, t.dt_stop, t.dt as dt
                                        FROM (
                                            SELECT ref_nomenclature, MIN(dt_start) as dt_start, MAX(dt_stop) as dt_stop, dt_start::DATE as dt
                                            FROM aatp.stand45_passageway
                                            GROUP BY ref_nomenclature, dt_start::DATE
                                            ) t JOIN aatp.stand45_passageway m ON m.ref_nomenclature = t.ref_nomenclature AND t.dt_start = m.dt_start
                                        ORDER BY t.dt """   
    pg_query_read_profile = """ SELECT profile_name, passages_num
                                    FROM aatp.acceptable_profiles
                                    WHERE obj_id = '{}' """         
    pg_query_read_valpar = """SELECT passage, q1,q2,q3,x1,x2
                                            FROM aatp.acceptable_values 
                                            WHERE ref_profile = '{}' and ref_parameter = '{}'"""
    pg_query_read_passageway = """ SELECT dt_start, dt_stop, passage as num_proh, amperage_mean, amperage_max, power_mean,
                                                power_max, speed, temperature_max, temperature_mean, interformation_pause, id_posad,
                                                deviation_amperage_mean as res_amperage_mean, deviation_amperage_max as res_amperage_max,
                                                deviation_power_mean as res_power_mean, deviation_power_max as res_power_max, deviation_speed as res_speed,
                                                deviation_temperature_mean as res_temperature_mean, deviation_temperature_max as res_temperature_max, 
                                                deviation_interformation_pause as res_interformation_pause, description_nomenclature as nomen
                                        FROM aatp.stand45_passageway
                                        WHERE ref_profile = '{}' and dt_start > '{}' and dt_stop < '{}'
                                        order by id """                                            
    ruparam = ['Средний Ток', 'Максимальный Ток', 'Средняя мощность', 'Максимаьлная мощность', 'Скорость', 'Средняя температура',
                'Максимальная температура', 'Паузы перед проходами']
    list_mean = ['amperage_mean', 'amperage_max', 'power_mean', 'power_max', 'speed',
                     'temperature_mean', 'temperature_max', 'interformation_pause', 'num_proh']
    res_list = ['res_amperage_mean', 'res_amperage_max', 'res_power_mean', 'res_power_max', 'res_speed',
                     'res_temperature_mean', 'res_temperature_max', 'res_interformation_pause']
    pg_param_name = ['nsi.spr_terminal_techpa,14', 'nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,16',
                        'nsi.spr_terminal_techpa,15', 'nsi.spr_terminal_techpa,11', 'nsi.spr_terminal_techpa,49',
                        'nsi.spr_terminal_techpa,1', 'nsi.spr_terminal_techpa,10']

    context = {}
    if request.method == "POST":
        pgsql_connection = ''
        connection = create_engine(pgsql_connection)
        param = json.loads(request.body)

        # Инициализация данных по профилям
        if 'init_param' in param:
            df = pd.read_sql_query(pg_query_read_profiles, connection)
            context = {'profiles' :  list(df['profile_name']), 'ref_profiles' : list(df['obj_id'])}
            return HttpResponse(json.dumps(context))

        # Обработка запроса на таблицу проката по дате или профилю
        elif 'profile_choice' in param:
            df = pd.read_sql(pg_query_read_periods, connection)
            if param['profile_choice'] == 'profile':
                df = df[df['ref_profile'] == param['data']]
            else:
                df = df[df['dt'].astype(str) == param['data']]
            df = df.reset_index()
            df = df.drop(columns=['index'])
            context = {'ref_profile' : list(df['ref_profile']), 'description_profile' : list(df['description_nomenclature']),
                       'dt_start' : [str(i)[:-7] for i in list(df['dt_start'])], 'dt_stop' : [str(i)[:-7] for i in list(df['dt_stop'])], 'dt' : [str(i) for i in list(df['dt'])]}
            return HttpResponse(json.dumps(context))
        
        # Обработка запроса на формирование отчета по выбранному профилю, параметру и времени масштабирования
        elif 'raport_par' in param:
            tekparam = int(param['tekparam'])
            refnomen = param['refnomen']
            startperiod = param['startperiod']
            stopperiod = param['stopperiod']
            tekdata = param['tekdata']
            starttime = param['starttime']
            endtime = param['endtime']
            count_pass = int(list(pd.read_sql_query(pg_query_read_profile.format(refnomen), connection)['passages_num'])[0])
            valparam = pd.read_sql_query(pg_query_read_valpar.format(refnomen,pg_param_name[tekparam]), connection)
            dfvisual = pd.read_sql_query(pg_query_read_passageway.format(refnomen, startperiod, stopperiod), connection)
            dfvisual = dfvisual[(dfvisual['dt_start'] > f'{tekdata} {starttime}') & (dfvisual['dt_stop'] < f'{tekdata} {endtime}')]
            dfmean = dfvisual[int(f'-{count_pass}'):]
            xd, yd, listcol = update_plotsAatp3(dfvisual, dfmean, list_mean, res_list, tekparam)
            val_stats = update_validparamAatp3(valparam)
            context = {'xdata': xd, 'ydata': yd, 'listcol': listcol,
                       'ruparam': ruparam, 'count_pass': count_pass, 
                       'tekparam': tekparam, 'val_stats': val_stats}
            return HttpResponse(json.dumps(context))        

    return render(request, 'mainapplication/aatp_45.html', context)