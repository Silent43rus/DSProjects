# Скрипт описания ссылок на конкретный прокат

import os
import pandas as pd
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

nameTable1 = f'gidrosbiv_{start_date}_{end_date}.txt'
input_path = f"{parent_dir}DataPreprocessing_spc500_{start_date}_{end_date}/"
nameTable2 = 'sved_prokat'  # Вытянуть кол-во заготовок и их вес
nameTable3 = 'sved_packet_info'  # Вытянуть id бригады
nameTable4 = 'sut_zad_stan_in_work'  # вытянуть id вх и исх номенклатуры
nameTable5 = 'spr_nomenclature'  # вытянуть вх и исх номенклатуру
nameTable6 = 'spr_brigades'  # Вытянуть бригады

nameSchema2 = 'sved'
nameSchema3 = 'spc500_l2'
nameSchema4 = 'nsi'

pgsql_connection = ''
pgsql_backup_con = create_engine(pgsql_connection)
pg_query_staninword = """SELECT id_inc, spr_nomenclature_in as nom_in, spr_nomenclature_out as nom_out 
                         FROM {}.{} 
                         WHERE id_inc = {} """
pg_query_nsi = """ SELECT full_name
                    FROM {}.{}
                    WHERE obj_id = '{}' """
pg_query_prokat = """ SELECT obj_id
                        FROM {}.{}
                        WHERE ref_posad LIKE '%%{}%%' """
pg_query_brigade = """ SELECT ref_brigade
                        FROM {}.{}
                        WHERE ref_prokat = '{}' """
pg_query_brigadensi = """ SELECT description 
                            FROM {}.{}
                            WHERE obj_id = '{}' """
pg_query = "SELECT * FROM {}.{}"

if not os.path.exists(input_path + nameTable1):
    ex = "Incorrect data entry"
    raise MycustomError(ex)
if os.path.exists(input_path + f'result_3_{start_date}_{end_date}.xlsx'):
    ex = "This script has already been executed"
    raise MycustomError(ex)
prokattab = pd.read_csv(input_path + nameTable1)

prokattab = prokattab.sort_values('dt')
result = pd.DataFrame({'id_posad': []})
temptab = prokattab.groupby(['id_posad']).mean()
temptab = temptab.reset_index()
result['id_posad'] = temptab['id_posad']

# Считаем дату начала проката

result['dt_start'] = ''
idpro = list(prokattab['id_posad'])
idprores = list(result['id_posad'])
flag = list(prokattab['gidroflag'])
dtt = list(prokattab['dt'])
dtstart = list()
for j in range(len(idprores)):
    for i in range(len(flag)):
        if flag[i] == 1 and idpro[i] == idprores[j]:
            dtstart.append(dtt[i])
            break

result['dt_start'] = pd.Series(dtstart)

# Считаем дату окончания проката

result['dt_stop'] = ''
idpro = list(prokattab['id_posad'])
idprores = list(result['id_posad'])
flag = list(prokattab['gidroflag'])
dtt = list(prokattab['dt'])
dtstop = list()
for j in idprores[::-1]:
    for i in range(len(flag) - 1, -1, -1):
        if flag[i] == 1 and idpro[i] == j:
            dtstop.append(dtt[i])
            break

result['dt_stop'] = pd.Series(dtstop[::-1])

svedprokattab = pd.read_sql_query(pg_query.format(nameSchema2, nameTable2), pgsql_backup_con)

# Считаем количество заготовок и вес

result['weight'] = ''
result['amount'] = ''
refposad = list(svedprokattab['ref_posad'])
amount = list(svedprokattab['amount'])
weight = list(svedprokattab['weight'])
idprores = list(result['id_posad'])
tempw, tempa = list(), list()
for c in idprores:
    for i in range(len(refposad)):
        if str(c) in str(refposad[i]):
            tempw.append(weight[i])
            tempa.append(amount[i])
result['weight'] = tempw
result['amount'] = tempa

id_posad = list(result['id_posad'])
id_nomen_in = list()
id_nomen_out = list()
nomen_in = list()
nomen_out = list()

for i in range(len(id_posad)):
    stanworktab = pd.read_sql_query(pg_query_staninword.format(nameSchema3, nameTable4, id_posad[i]),
                                    pgsql_backup_con)
    id_nomen_in.append(list(stanworktab['nom_in'])[0])
    id_nomen_out.append(list(stanworktab['nom_out'])[0])

for i in range(len(id_posad)):
    nsitab = pd.read_sql_query(pg_query_nsi.format(nameSchema4, nameTable5, str(id_nomen_in[i])), pgsql_backup_con)
    nomen_in.append(list(nsitab['full_name'])[0])
for i in range(len(id_posad)):
    nsitab = pd.read_sql_query(pg_query_nsi.format(nameSchema4, nameTable5, str(id_nomen_out[i])), pgsql_backup_con)
    nomen_out.append(list(nsitab['full_name'])[0])

result['nom_in'] = nomen_in
result['nom_out'] = nomen_out

id_posad = list(result['id_posad'])
ref_posad = list()
brigades = list()

for i in range(len(id_posad)):
    prokattab = pd.read_sql_query(pg_query_prokat.format(nameSchema2, nameTable2, str(id_posad[i])),
                                  pgsql_backup_con)
    # ref_posad.append(list(prokattab['obj_id'])[0])
    brigadeidtab = pd.read_sql_query(pg_query_brigade.format(nameSchema2, nameTable3, list(prokattab['obj_id'])[0]),
                                     pgsql_backup_con)
    a = list(brigadeidtab['ref_brigade'])
    temp = list()
    for i in range(len(a)):
        if a[i] not in temp and not (a[i] is None):
            temp.append(a[i])
    brig = ''
    for i in range(len(temp)):
        brigadetab = pd.read_sql_query(pg_query_brigadensi.format(nameSchema4, nameTable6, temp[i]),
                                       pgsql_backup_con)
        brig += list(brigadetab['description'])[0] + ';'
    brigades.append(brig)

result['brigades'] = brigades

result.to_excel(input_path + f'result_3_{start_date}_{end_date}.xlsx')
output_table_1 = result