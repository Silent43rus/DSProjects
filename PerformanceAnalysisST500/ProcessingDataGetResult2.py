# Скрипт для расчета расхода енергии по ст500 за каждый час

import os
import pandas as pd
import numpy as np


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

nameTable1 = f'gidrosbiv_{start_date}_{end_date}.txt'
input_path = f"{parent_dir}DataPreprocessing_spc500_{start_date}_{end_date}/"
nameTable2 = 'energytab.txt'
if not os.path.exists(input_path + nameTable1):
    ex = "Incorrect data entry"
    raise MycustomError(ex)
if os.path.exists(input_path + f'result_2_{start_date}_{end_date}.xlsx'):
    ex = "This script has already been executed"
    raise MycustomError(ex)
perftab = pd.read_csv(input_path + nameTable1)
energytab = pd.read_csv(input_path + nameTable2)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.options.display.expand_frame_repr = False

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
result = result.sort_values('dt')

# Пишем в таблицу counperf промежуточный результат расчета производительности

temp = result.groupby(['id_posad', 'realtime']).sum()
countperf = pd.DataFrame({'perfcount': []})
countperf['perfcount'] = temp['myflag']

# Пишем в таблицу countenergy промежуточный результат расчета расхода электроэнергии

temp = result.groupby(['id_posad', 'realtime']).sum()
countgaz = pd.DataFrame({'gazcount': []})
countgaz['gazcount'] = np.round(temp['gazrash'], 1)

res = pd.merge(countperf, countgaz, how='inner', on=['id_posad', 'realtime'])

res.to_excel(input_path + f'result_2_{start_date}_{end_date}.xlsx')
output_table_1 = res