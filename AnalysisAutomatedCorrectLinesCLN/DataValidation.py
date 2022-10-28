# Скрипт для валидации полученных из hd данных

import pandas as pd
import numpy as np
import os

# Входные параметры

start_date = '11082022'
end_date = '11082022'
input_path = ''
line = 'LINE2'
input_path = os.path.join(input_path, line)

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

if int(start_date[:2] > end_date[:2]) or not os.path.exists(os.path.join(input_path, start_date + '.txt'))\
        or not os.path.exists(os.path.join(input_path, end_date + '.txt')):
    ex = "Incorrect data entry"
    raise MycustomError(ex)

directory = f"DataPreprocessing_cln_CurvAnalysis_{start_date}_{end_date}"
parent_dir = input_path
path = os.path.join(parent_dir, directory)
if not os.path.exists(path):
    os.mkdir(path)


for i in range(int(start_date[:2]), int(end_date[:2]) + 1):

    true_process = path + '/true_process.txt'
    if os.path.exists(true_process):
        ex = "This script has already been executed"
        raise MycustomError(ex)

    teki = str(i) if i >= 10 else "0" + str(i)
    name_day = teki + start_date[2:] if i >= int(start_date[:2]) else teki + end_date[2:]

    data = pd.read_csv(f"{input_path}/{name_day}.txt", sep=",", header=None, low_memory=False, encoding='ansi')
    if line == 'LINE3':
    	data['diff_action'] = 0
    	data.columns = ['dt', 'predetermined_action', 'upper_line1', 'lower_line1', 'upper_line2', 'lower_line2',
                    'current_curvature', 'lenght_bar', 'id_packed', 'upor_flag1', 'upor_flag2', 'nomen', 'series', 'diff_action']
    else:
    	data.columns = ['dt', 'predetermined_action', 'upper_line1', 'lower_line1', 'upper_line2', 'lower_line2',
                     'current_curvature', 'lenght_bar', 'id_packed', 'diff_action', 'upor_flag1', 'upor_flag2', 'nomen', 'series']
    data['upper_line'] = ''
    data['lower_line'] = ''
    data = data[['dt', 'predetermined_action', 'upper_line', 'lower_line', 'current_curvature',
                 'lenght_bar', 'upor_flag1', 'upor_flag2',
                 'upper_line1', 'lower_line1', 'upper_line2', 'lower_line2','id_packed', 'diff_action', 'nomen', 'series']]

    data = data.sort_values(by=['dt'])


    data = data.loc[data['current_curvature'] != '1,#INF']
    data = data.loc[data['predetermined_action'] != '1,#INF']
    data = data.loc[data['lenght_bar'] != '1,#INF']

    data['dt'] = pd.to_datetime(data['dt'], format='%Y.%m.%d %H:%M:%S.%f')
    data['id_packed'] = pd.to_numeric(data['id_packed']).astype(int)
    data['upor_flag1'] = np.where(data['upor_flag1'].str.isalnum(), None, data['upor_flag1'])
    data['upor_flag2'] = np.where(data['upor_flag2'].str.isalnum(), None, data['upor_flag2'])
    output_path = path + f'/cln_press_{start_date}_{end_date}.txt'
    nomen = ''
    series = ''
    n = []
    s = []
    for i in range(len(data)):
        if data['nomen'][i] != 'None':
            nomen = data['nomen'][i]
        if data['series'][i] != 'None':
            series = data['series'][i]
        n.append(nomen)
        s.append(series)
    data['nomen'] = n
    data['series'] = s
    data.to_csv(output_path, mode='a', header=not os.path.exists(output_path), index=False)



df = pd.DataFrame()

df.to_csv(path + "/true_process.txt")

