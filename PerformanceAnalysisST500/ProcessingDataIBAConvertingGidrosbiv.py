import os
import pandas as pd
import pypyodbc

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
input_path = ''                                     # Путь к экспортированным из IBA данным
parent_dir = 'C:/'                                  # Путь к выходной папке с результатами


if int(start_date[:2] > end_date[:2]) or not os.path.exists(input_path + start_date + '.txt') or not os.path.exists(input_path + end_date + '.txt'):
    ex = "Incorrect data entry"
    raise MycustomError(ex)
for i in range(int(start_date[:2]),int(end_date[:2])+1):

    directory = f"DataPreprocessing_spc500_{start_date}_{end_date}"

    path = os.path.join(parent_dir, directory)
    true_process = path + '/true_process.txt'
    if os.path.exists(true_process):
        ex = "This script has already been executed"
        raise MycustomError(ex)
    if not os.path.exists(path):
        os.mkdir(path)

    teki = str(i) if i >= 10 else "0" + str(i)
    name_day = teki + start_date[2:] if i >= int(start_date[:2]) else teki + end_date[2:]

    data = pd.read_csv(f'{input_path}{name_day}.txt', sep = "	", header=None, low_memory = False)
    data.columns = ["dt", "tok", "id_posad", "gidroflag", "gazcount" ]

    data = data.sort_values(by =['dt'])
    data = data.iloc[:-1 , :]

    data = data.loc[data['tok'] != '1,#INF']
    data = data.loc[data['gazcount'] != '1,#INF']
    data = data.loc[data['id_posad'].str.isdigit() == True]

    data['dt'] = pd.to_datetime(data['dt'], format = '%d.%m.%Y %H:%M:%S.%f')
    data['tok'] = data['tok'].str.replace(',','.')
    data['tok'] = pd.to_numeric(data['tok'])
    data['id_posad'] = pd.to_numeric(data['id_posad'])
    data['gidroflag'] = pd.to_numeric(data['gidroflag'])
    data['gazcount'] = data['gazcount'].str.replace(',','.')
    data['gazcount'] = pd.to_numeric(data['gazcount'])

    data['yyyy'] = data['dt'].dt.year
    data['mm'] = data['dt'].dt.month
    data['dd'] = data['dt'].dt.day
    data['hh'] = data['dt'].dt.hour

    output_path = path + f'/gidrosbiv_{start_date}_{end_date}.txt'
    data.to_csv(output_path, mode = 'a', header = not os.path.exists(output_path), index = False)



if os.path.exists(true_process):
    ex = "This script has already been executed"
    raise MycustomError(ex)
else:
    mssql_connection = ''
    nameSchema = 'data_performance_processing_spc'
    mssql_con = pypyodbc.connect(mssql_connection)

    mssql_query_getenergy = """SELECT UA#hour_ZRU_F601.realtime, 
    	(sum(UA#hour_ZRU_F601.rashod) + sum(UA#hour_GRU_F601.rashod) - sum(UA#hour_ABK_NB_v1.rashod) - sum(UA#hour_ABK_OB_v1.rashod) - sum(UA#hour_ABK_OB_v2.rashod) - 
    	sum(UA#hour_prim_settler.rashod) + sum(UA#hour_ZRU_F616.rashod) + sum(UA#hour_LPZ_st500.rashod) + sum(UA#hour_RU_st500_v1.rashod) + sum(UA#hour_Gidrosbiv.rashod) + 
		sum(UA#hour_RU_st500_v2.rashod) + sum(UA#hour_AQ1.rashod) - sum(UA#hour_SPC500_ELight.rashod) - sum(UA#hour_SPC500_WLight.rashod)) as rashodsumm
		FROM UA#hour_ZRU_F601, UA#hour_GRU_F601, UA#hour_ABK_NB_v1, UA#hour_ABK_OB_v1, UA#hour_ABK_OB_v2, UA#hour_prim_settler, UA#hour_ZRU_F616, UA#hour_LPZ_st500, 
		UA#hour_SPC500_ELight, UA#hour_SPC500_WLight,  UA#hour_RU_st500_v1, UA#hour_Gidrosbiv, UA#hour_RU_st500_v2, UA#hour_AQ1 
		WHERE (UA#hour_ZRU_F601.time2 = UA#hour_GRU_F601.time2 AND UA#hour_ZRU_F601.time2 = UA#hour_ABK_NB_v1.time2 AND UA#hour_ZRU_F601.time2 = UA#hour_ABK_OB_v1.time2 
		AND UA#hour_ZRU_F601.time2 = UA#hour_ABK_OB_v2.time2 AND UA#hour_ZRU_F601.time2 = UA#hour_prim_settler.time2 
		AND UA#hour_ZRU_F601.time2 = UA#hour_ZRU_F616.time2 AND UA#hour_ZRU_F601.time2 = UA#hour_LPZ_st500.time2 AND UA#hour_ZRU_F601.time2 = UA#hour_RU_st500_v1.time2 
		AND UA#hour_ZRU_F601.time2 = UA#hour_Gidrosbiv.time2 AND UA#hour_ZRU_F601.time2 = UA#hour_RU_st500_v2.time2 AND UA#hour_ZRU_F601.time2 = UA#hour_AQ1.time2 AND UA#hour_ZRU_F601.time2 = UA#hour_SPC500_ELight.time2 AND UA#hour_ZRU_F601.time2 = UA#hour_SPC500_WLight.time2)
		GROUP BY UA#hour_ZRU_F601.realtime """


    energytab = pd.read_sql_query(mssql_query_getenergy,mssql_con)

    energytab['realtime'] = pd.to_datetime(energytab['realtime'].astype('str') , format = '%H:%M:%S %d.%m.%Y')
    energytab = energytab.sort_values('realtime')

    energytab['hh'] = energytab['realtime'].dt.hour
    energytab['dd'] = energytab['realtime'].dt.day
    energytab['mm'] = energytab['realtime'].dt.month
    energytab['yyyy'] = energytab['realtime'].dt.year

    energytab = energytab.reset_index()
    energytab = energytab.drop(columns = ['index'], axis = 1)

    energytab.to_csv(path + '/energytab.txt', index = False)
    df = pd.DataFrame()
    df.to_csv(path + "/true_process.txt")