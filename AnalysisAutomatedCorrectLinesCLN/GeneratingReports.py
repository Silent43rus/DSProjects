# Скрипт генерации отчетов

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from PIL import Image, ImageDraw, ImageFont

path = ''
start_date = '11082022'
end_date = '11082022'
install_repos = ''
line = 'LINE2'
path = os.path.join(path, line)

local_path = f"{path}/DataPreprocessing_cln_CurvAnalysis_{start_date}_{end_date}"
curv_tab = f'{local_path}/curvtable'
path_font = install_repos
repname = "reports"
data = pd.read_excel(f"{local_path}/actions_{start_date}_{end_date}.xlsx")

# Считываем таблицу и генерим 2 листа с воздействием и кривизной

def def_table(index):
    df = pd.read_csv(os.path.join(os.path.join(local_path, 'curvtable'), 'curvature_table.txt'))
    df = df[df.index == index]
    curv = []
    act = []
    newdf = pd.DataFrame()
    for i in range(1,11):
        curv.append(float(list(df[f'curvature{i}'])[0]))
        act.append(float(list(df[f'action{i}'])[0]))
    newdf['curvature'] = curv
    newdf['action'] = act
    fig, ax = plt.subplots(1, 1, figsize=(16,7))
    ax.scatter(newdf['action'], newdf['curvature'])
    return list(newdf['curvature']), list(newdf['action'])

# Точность от воздействия

def accuracy(temp):
    a = []
    cc = min(temp['action'])
    while cc < max(temp['action']):
        a.append(cc)
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
    return a, acc

# Расчет линии регрессии

def regr(x,y):
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size = 1/3)
    lr = LinearRegression()
    lr.fit(x, y)
    return lr

def gen_report(index, data, file_name, path_inner, nom=""):
    # 3 графика с отрицательным воздействием

    data_image1 = data.loc[data['action'] < 0]

    fig, ax = plt.subplots(3, 2, figsize=(25, 10))
    if len(data_image1) <= 1:
        ax[0, 0].grid(True)
        ax[0, 0].scatter(data_image1['action'], data_image1['out_curvature'], c=data_image1['first_correct'])
    else:
        temp1 = data_image1.loc[data_image1['first_correct'] == 1]
        temp2 = data_image1.loc[data_image1['first_correct'] == 0]
        ax[0, 0].grid(True)
        ax[0, 0].scatter(data_image1['action'], data_image1['out_curvature'], c=data_image1['first_correct'])
        ax[0, 0].set_title('Перегнули: ' + str(
            len(data_image1.loc[(data_image1['first_correct'] == 0) & (data_image1['out_curvature'] < 0)])) +
                           ' Не догнули: ' + str(
            len(data_image1.loc[(data_image1['first_correct'] == 0) & (data_image1['out_curvature'] > 0)]))
                           + '\n' + '\n' + "Точность: " + str(
            len(data_image1.loc[(data_image1['first_correct'] == 1)]) / len(data_image1))
                           + '\n' + '\n' + "Зависимость воздействия от выходной кривизны")
        x, y = accuracy(data_image1)
        ax[1, 0].plot(x, y)
        ax[1, 0].grid(True)
        ax[1, 0].set_title("Точность от воздействия")
        x = np.array(data_image1['curvature']).reshape(-1, 1)
        y = np.array(data_image1['action']).reshape(-1, 1)
        l = regr(x, y)
        ax[2, 0].plot(x, l.predict(x), color='red')
        ax[2, 0].scatter(data_image1['curvature'], data_image1['action'], c=data_image1['first_correct'])
        ax[2, 0].grid(True)
        ax[2, 0].set_xlabel("curvature")
        ax[2, 0].set_ylabel("action")
        ax[2, 0].set_title("Таблица воздействий от кривизны")

    # 3 графика с положительным воздействием

    data_image2 = data.loc[data['action'] > 0]
    if len(data_image2) <= 1:
        ax[0, 1].grid(True)
        ax[0, 1].scatter(data_image2['action'], data_image2['out_curvature'], c=data_image2['first_correct'],
                         label="Попали/Не попали")
    else:
        temp1 = data_image2.loc[data_image2['first_correct'] == 1]
        temp2 = data_image2.loc[data_image2['first_correct'] == 0]
        ax[0, 1].grid(True)
        ax[0, 1].scatter(data_image2['action'], data_image2['out_curvature'], c=data_image2['first_correct'],
                         label="Попали/Не попали")
        ax[0, 1].set_title('Перегнули: ' + str(
            len(data_image2.loc[(data_image2['first_correct'] == 0) & (data_image2['out_curvature'] > 0)])) +
                           ' Не догнули: ' + str(
            len(data_image2.loc[(data_image2['first_correct'] == 0) & (data_image2['out_curvature'] < 0)]))
                           + '\n' + '\n' + "Точность: " + str(
            len(data_image2.loc[(data_image2['first_correct'] == 1)]) / len(data_image2))
                           + '\n' + '\n' + "Зависимость воздействия от выходной кривизны")
        x, y = accuracy(data_image2)
        ax[1, 1].plot(x, y)
        ax[1, 1].grid(True)
        ax[1, 1].set_title("Точность от воздействия")
        x = np.array(data_image2['curvature']).reshape(-1, 1)
        y = np.array(data_image2['action']).reshape(-1, 1)
        l = regr(x, y)
        ax[2, 1].plot(x, l.predict(x), color='red')
        ax[2, 1].scatter(data_image2['curvature'], data_image2['action'], c=data_image2['first_correct'])
        ax[2, 1].grid(True)
        ax[2, 1].set_xlabel("curvature")
        ax[2, 1].set_ylabel("action")
        ax[2, 1].set_title("Таблица воздействий от кривизны")

    plt.savefig(path_inner + '/' + file_name + '.png')
    plt.close()

    # Подпись к изображению с данными по таблице

    img = Image.open(path_inner + '/' + file_name + '.png')
    img_draw = ImageDraw.Draw(img)
    font = ImageFont.truetype(path_font + "/CalibriL.ttf", size=25)
    kr, act = def_table(index)
    kr = [f"{i:.{3}f}" for i in kr]
    act = [f"{i:.{3}f}" for i in act]
    img_draw.text((250, 940), "Исходная таблица: Кривизна       " + '  '.join([str(i) for i in kr]),
                  fill='black', font=font)
    img_draw.text((409, 960), " Воздействие   " + '   '.join([str(i) for i in act]),
                  fill='black', font=font)
    img_draw.text((1150, 10), "Количество заготовок: " + str(
        len(data.groupby(['id_bar']).mean())), fill='black', font=font)
    img_draw.text((1150, 40), 'Количество воздействий: ' + str(len(data)),
                  fill='black', font=font)
    img_draw.text((1150, 70), 'Общая точность: ' + str(round(
        len(data[data['first_correct'] == 1]) / len(data), 3) * 100) + ' %', fill='black', font=font)
    if nom != "":
        img_draw.text((1500, 940), "Номенклатура пачки: " + str(nom),
                      fill='black', font=font)

    img.save(path_inner + '/' + file_name + '.png')


# Для каждой сгенерированной таблицы где кол-во строк больше 2 генерим файл отчета

ct = pd.read_csv(os.path.join(curv_tab, 'curvature_table.txt'))
curv_table = ct.dt_start.str[2:10] + '_' + ct.dt_start.str[11:16] + '_' + ct.dt_stop.str[11:16]
curv_table = list([i.replace(':', '.') for i in curv_table])
if not os.path.exists(os.path.join(local_path, repname)):
    os.mkdir(os.path.join(local_path, repname))
for i, dirr in enumerate(curv_table):
    print(i, dirr)
    start_smena = list(ct.dt_start)[i]
    stop_smena = list(ct.dt_stop)[i]
    data_image = data.loc[data['position'] == 2]
    data_image = data_image.loc[(data_image['dt'] > start_smena) & (data_image['dt'] < stop_smena)]
    nom_list = data_image.groupby(['nomen']).mean().reset_index()['nomen']
    path_inner = os.path.join(local_path, repname)
    path_inner = os.path.join(path_inner, dirr)
    if not os.path.exists(path_inner):
        os.mkdir(path_inner)
    if len(nom_list) > 0:
        for j, nom in enumerate(nom_list):
            file_name = str(data_image[data_image['nomen'] == nom_list[j]].sort_values('dt').reset_index()['dt'][0])[
                        5:-10].replace(':', '-') + "_to_" + \
                        str(data_image[data_image['nomen'] == nom_list[j]].sort_values('dt',
                                                                                       ascending=False).reset_index()[
                                'dt'][0])[5:-10].replace(':', '-')
            gen_report(i, data=data_image[data_image['nomen'] == nom_list[j]], file_name=file_name,
                       path_inner=path_inner, nom=nom_list[j])
        gen_report(i, data=data_image, file_name="General", path_inner=path_inner)