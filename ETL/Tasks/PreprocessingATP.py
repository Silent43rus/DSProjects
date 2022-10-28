# Для расчета модели средних значений технологических параметров для каждого отдельного профиля на отдельном участке производства

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
pd.options.display.expand_frame_repr = False
pd.options.mode.chained_assignment = None
from sqlalchemy import create_engine
import datetime


class MetalProcessingCycleMill500():
    "Класс обработки данных по участкам стана 500"

    def __init__(self):
        self.path = None
        self.profilename = None
        self.count_proh = None


    def ProcessingIbaSignals3Stand(self, path, profilename, count_proh):

        """
        Метод по предобработке сигналов из ибы в итоговую таблицу проходов по участку 3 клети

        Атрибуты:
        :param path: str - Каталог с проэкспортированными периодами проката
        :param profilename: str - Название профиля как в pg
        :param count_proh: int - Количество проходов на 3 клети
        """

        self.path = path
        self.profilename = profilename
        self.count_proh = count_proh
        tempdf = []
        path = os.path.join(self.path, self.profilename.replace(' ', '_').replace('/', ''))
        for file in os.listdir(path):
            df = pd.read_csv(os.path.join(path, file), low_memory=False, index_col=None, header=0, sep='	')

            # Преобразование таблицы
            if len(df.columns) == 10:
                df.columns = ['dt', 'tilter3', 'tilter4', 'speed_vx', 'amperage_mean', 'task_controller', 'speed_vs',
                              'caliber_selection3', 'caliber_selection4', 'max_temp']
                df['vibration1_mean'] = 0
                df['vibration2_mean'] = 0
                df['vibration3_mean'] = 0
                df['vibration4_mean'] = 0
                df['vibration1_max'] = 0
                df['vibration2_max'] = 0
                df['vibration3_max'] = 0
                df['vibration4_max'] = 0
                df['independ_pause'] = 0
                df['amperage_max'] = 0
            else:
                df.columns = ['dt', 'tilter3', 'tilter4', 'speed_vx', 'amperage_mean', 'task_controller', 'speed_vs',
                              'caliber_selection3', 'caliber_selection4', 'max_temp', 'vibration1_mean',
                              'vibration2_mean',
                              'vibration3_mean', 'vibration4_mean']
                df['vibration1_max'] = 0
                df['vibration2_max'] = 0
                df['vibration3_max'] = 0
                df['vibration4_max'] = 0
                df['independ_pause'] = 0
                df['amperage_max'] = 0

            # Валидация
            for i in range(len(df.columns)):
                df = df.loc[(df[df.columns[i]] != '1,#INF') &
                            (df[df.columns[i]].notna()) &
                            (df[df.columns[i]] != '-1,#IND') &
                            (df[df.columns[i]] != 'nan')]
                df[df.columns[i]].loc[df[df.columns[i]] != '0'] = df[df.columns[i]].astype(str).str.replace(',', '.')
                if i == 0:
                    df[df.columns[i]] = pd.to_datetime(df[df.columns[i]], format='%d.%m.%Y %H:%M:%S.%f')
                else:
                    df[df.columns[i]] = pd.to_numeric(df[df.columns[i]])

            # Расчет номера прохода
            tok = list(df['amperage_mean'])
            task_controller = list(df['task_controller'])
            cal_cel = list(df['caliber_selection3'])
            sbros = [0 for _ in tok]
            csbros = 0
            numb = [0 for _ in tok]  # Номер прохода
            ftok = [0 for _ in tok]
            sumproh = [0 for _ in tok]  # Количество проходов
            low_ground_amperage = 20
            low_ground_times = 20
            low_task_controller = 1600
            low_time_amperage = 10
            # Ставим флаг прохода где ток больше low_ground_amperage
            for i in range(len(tok)):
                if tok[i] > low_ground_amperage:
                    ftok[i] = 1
            count = 0
            # Удаляем флаги прохода там где был ток но его длительность была меньше low_ground_times
            for i in range(len(tok)):
                if ftok[i] == 1:
                    count += 1
                if ftok[i] == 0:
                    if count < low_time_amperage and count != 0:
                        j = i - 1
                        while ftok[j] == 1:
                            ftok[j] = 0
                            j -= 1
                    count = 0
            for i in range(len(tok)):
                if ftok[i] == 0:
                    csbros += 1
                if ftok[i] == 1:
                    csbros = 0
                if csbros > 20:
                    if task_controller[i] >= low_task_controller and tok[i] < low_ground_amperage:
                        sbros[i] = 1
            teknum = 1
            proh = 1
            fperehod = False
            cperehod = 0
            for i in range(len(tok)):
                if ftok[i] == 1:
                    numb[i] = teknum
                    sumproh[i] = proh
                    fperehod = True
                    cperehod = 0
                if ftok[i] == 0 and fperehod and cperehod > low_ground_times:
                    teknum += 1
                    proh += 1
                    fperehod = False
                if ftok[i] == 0:
                    cperehod += 1
                if sbros[i] == 1:
                    teknum = 1
            df['num_proh'] = numb
            df['count_proh'] = sumproh

            # Построение таблицы проходов
            g = df.groupby('count_proh').mean()[
                ['num_proh', 'amperage_mean', 'amperage_max', 'speed_vs', 'max_temp', 'independ_pause',
                 'vibration1_mean', 'vibration2_mean', 'vibration3_mean', 'vibration4_mean',
                 'vibration1_max', 'vibration2_max', 'vibration3_max', 'vibration4_max']]
            g[['dtstop', 'amperage_max', 'vibration1_max', 'vibration2_max',
               'vibration3_max', 'vibration4_max']] = df.groupby('count_proh').max()[
                ['dt', 'amperage_mean', 'vibration1_mean',
                 'vibration2_mean', 'vibration3_mean', 'vibration4_mean']]
            ds = list(g['dtstop'])
            maxt = [0 for _ in ds]
            for i in range(len(ds)):
                maxt[i] = list(df[df['dt'] == ds[i]]['max_temp'])[0]
            g['max_temp'] = maxt
            g['dtstart'] = df.groupby('count_proh').min()['dt']
            g = g[['dtstart', 'dtstop', 'num_proh', 'amperage_mean', 'amperage_max', 'speed_vs', 'max_temp',
                   'independ_pause',
                   'vibration1_mean', 'vibration2_mean', 'vibration3_mean', 'vibration4_mean',
                   'vibration1_max', 'vibration2_max', 'vibration3_max', 'vibration4_max']]
            g = g[1:]
            g['independ_pause'] = (g['dtstart'] - g['dtstop'].shift(1)).dt.seconds + (
                    g['dtstart'] - g['dtstop'].shift(1)).dt.microseconds / 1000000
            g = g.reset_index()
            tempdf.append(g)
        maindf = pd.concat(tempdf, axis=0, ignore_index=True)

        # Перерасчет неправильных проходов
        count_proh = self.count_proh
        data = maindf
        num_bar = [0 for _ in list(data.num_proh)]
        num_proh = list(data.num_proh)
        tek_bar = 0
        for i in range(1, len(num_bar)):
            if num_proh[i] == 1:
                tek_bar += 1
                num_bar[i] = tek_bar
            elif num_proh[i] - 1 == num_proh[i - 1]:
                num_bar[i] = tek_bar
        data['num_bar'] = num_bar
        data = data[data['num_bar'] > 0]
        dfmean = data
        dfmean = dfmean.sort_index(ascending=False)
        if count_proh == 5:
            dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 1) &
                                          (dfmean['num_proh'].shift(1) == 2) &
                                          (dfmean['num_proh'].shift(2) == 3) &
                                          (dfmean['num_proh'].shift(3) == 4) &
                                          (dfmean['num_proh'].shift(4) == 5), True, False)
            dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 2) &
                                          (dfmean['num_proh'].shift(-1) == 1) &
                                          (dfmean['num_proh'].shift(1) == 3) &
                                          (dfmean['num_proh'].shift(2) == 4) &
                                          (dfmean['num_proh'].shift(3) == 5), True, dfmean['flagtrue'])
            dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 3) &
                                          (dfmean['num_proh'].shift(-1) == 2) &
                                          (dfmean['num_proh'].shift(-2) == 1) &
                                          (dfmean['num_proh'].shift(1) == 4) &
                                          (dfmean['num_proh'].shift(2) == 5), True, dfmean['flagtrue'])
            dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 4) &
                                          (dfmean['num_proh'].shift(-1) == 3) &
                                          (dfmean['num_proh'].shift(-2) == 2) &
                                          (dfmean['num_proh'].shift(-3) == 1) &
                                          (dfmean['num_proh'].shift(1) == 5), True, dfmean['flagtrue'])
            dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 5) &
                                          (dfmean['num_proh'].shift(-1) == 4) &
                                          (dfmean['num_proh'].shift(-2) == 3) &
                                          (dfmean['num_proh'].shift(-3) == 2) &
                                          (dfmean['num_proh'].shift(-4) == 1), True, dfmean['flagtrue'])
        else:
            dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 1) &
                                          (dfmean['num_proh'].shift(1) == 2) &
                                          (dfmean['num_proh'].shift(2) == 3), True, False)
            dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 2) &
                                          (dfmean['num_proh'].shift(-1) == 1) &
                                          (dfmean['num_proh'].shift(1) == 3), True, dfmean['flagtrue'])
            dfmean['flagtrue'] = np.where((dfmean['num_proh'] == 3) &
                                          (dfmean['num_proh'].shift(-1) == 2) &
                                          (dfmean['num_proh'].shift(-2) == 1), True, dfmean['flagtrue'])
        listdel = dfmean[dfmean['flagtrue'] == 0].groupby('num_bar').mean()
        listdel = listdel.reset_index()

        print('Из ' + str(len(dfmean.groupby('num_bar').mean())) + ' заготовок неправильно были посчитаны: ' +
              str(len(listdel)))
        print('Это ' + str(round(len(listdel) / len(dfmean.groupby('num_bar').mean()) * 100, 2)) + ' %')

        # Удаляем неправильно посчитанные проходы
        for i in list(listdel['num_bar']):
            dfmean = dfmean[dfmean['num_bar'] != i]
        maindf = dfmean
        maindf = maindf.sort_index()

        maindf = maindf.drop(columns='count_proh')
        maindf.to_csv(os.path.join(path, 'maintable.txt'))
        df = maindf.drop(
            columns=['amperage_max', 'vibration2_mean', 'vibration3_mean', 'vibration4_mean', 'vibration2_max',
                     'vibration3_max', 'vibration4_max', 'num_bar', 'flagtrue'])
        df.to_csv(os.path.join(path, 'basetable.txt'), index=False)
        df.to_csv(os.path.join(path, 'worktable.txt'), index=False)
        return maindf


    def CalculateAcceptableValues3Stand(self):
        """
        Метод по расчету распределения данных по проходам и запись текущего профиля и данныхих в бб
        """

        if self.path == None:
            print('Данные не обработаны')
        else:
            path = os.path.join(self.path, self.profilename.replace(' ', '_').replace('/', ''))
            df = pd.read_csv(os.path.join(path, 'maintable.txt'))

            pgsql_connetcion = ''
            pgsql_read_con = create_engine(pgsql_connetcion)
            pg_query_writter = """INSERT INTO aatp.acceptable_values (ref_profile,description_profile,passage,q1,q2,q3,x1,x2, 
                                                           ref_parameter,ref_location,ref_equipment) 
                                                           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            pg_query_read_profile = """SELECT obj_id
                                        FROM aatp.acceptable_profiles
                                        WHERE ref_location = 'nsi.spr_terminal_techpa_location,51' and profile_name = '{}' """
            pg_query_write_profile = "INSERT INTO aatp.acceptable_profiles (profile_name, ref_location, passages_num) VALUES (%s,%s,%s)"

            profile = list(pd.read_sql_query(pg_query_read_profile.format(self.profilename), pgsql_read_con)['obj_id'])[
                0] if len(list(
                pd.read_sql_query(pg_query_read_profile.format(self.profilename), pgsql_read_con)[
                    'obj_id'])) == 1 else ''

            param = ['nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,11', 'nsi.spr_terminal_techpa,10',
                     'nsi.spr_terminal_techpa,12', 'nsi.spr_terminal_techpa,13']
            location = 'nsi.spr_terminal_techpa_location,51'
            equipment = 'nsi.spr_equipment,100164'

            # Записываем профиль и центральное распределение данных в базу, если они не были добавлены ранее
            listpar = ['amperage_mean', 'speed_vs', 'independ_pause', 'vibration1_mean', 'vibration1_max']
            if profile != '':
                print("Этот профиль уже был добавлен")
            else:
                row = [self.profilename, location, self.count_proh]
                pgsql_read_con.execute(pg_query_write_profile, row)
                profile = \
                list(pd.read_sql_query(pg_query_read_profile.format(self.profilename), pgsql_read_con)['obj_id'])[0]
                for par in range(len(listpar)):
                    meanfile = []
                    for i in range(1, self.count_proh + 1):
                        temparr = df[(df['num_proh'] == i) & (df[f'{listpar[par]}'] > 0)][f'{listpar[par]}']
                        q1 = float(np.quantile(temparr, 0.25))
                        q3 = float(np.quantile(temparr, 0.75))
                        iqr = 1.5 * (q3 - q1)
                        minn = float(np.min(temparr))
                        maxx = float(np.max(temparr))
                        mid = float(np.median(temparr))
                        x1 = q1 - iqr if q1 - iqr > minn else minn
                        x2 = q3 + iqr if q3 + iqr < maxx else maxx
                        newrow = [profile, self.profilename, i, q1, mid, q3, x1, x2, param[par], location, equipment]
                        meanfile.append(newrow)
                    idd = pgsql_read_con.execute(pg_query_writter, meanfile)
                print("Профиль успешно добавлен")


    def VisualizedAcceptableValues3Stand(self):

        "Метод по визуализации распределения данных по проходам показ"

        listpar = ['amperage_mean', 'speed_vs', 'independ_pause', 'vibration1_mean', 'vibration1_max']

        if self.path == None:
            print('Данные не обработаны')
        else:
            path = os.path.join(self.path, self.profilename.replace(' ', '_').replace('/', ''))
            df = pd.read_csv(os.path.join(path, 'maintable.txt'))
            for par in range(len(listpar)):
                meanfile = []
                stats = []
                for i in range(1, self.count_proh + 1):
                    temparr = df[(df['num_proh'] == i) & (df[f'{listpar[par]}'] > 0)][f'{listpar[par]}']
                    q1 = float(np.quantile(temparr, 0.25))
                    q3 = float(np.quantile(temparr, 0.75))
                    iqr = 1.5 * (q3 - q1)
                    minn = float(np.min(temparr))
                    maxx = float(np.max(temparr))
                    mid = float(np.median(temparr))
                    x1 = q1 - iqr if q1 - iqr > minn else minn
                    x2 = q3 + iqr if q3 + iqr < maxx else maxx
                    stats.append({'med': mid,
                                  'q1': q1,
                                  'q3': q3,
                                  'whislo': x1,
                                  'whishi': x2
                                  })
                fig, ax = plt.subplots(figsize=(16, 7))
                b = ax.bxp(stats, showfliers=False, patch_artist=True)
                for element in ['boxes', 'whiskers', 'fliers', 'means', 'medians', 'caps']:
                    plt.setp(b[element], color='blue', linewidth=2)
                for patch in b['boxes']:
                    patch.set(facecolor='lime', linewidth=2)
                ax.grid(True)
                ax.set_title(listpar[par])
                plt.show()


    def ProcessingIbaSignals45Stand(self):

        """
        Метод по предобработке сигналов из ибы в итоговую таблицу проходов по участку 4 и 5 клети

        Атрибуты:
        :param path: str - Каталог с проэкспортированными периодами проката
        :param profilename: str - Название профиля как в pg
        """

        # Функция для возврата отдельных значений даных для отдельных профилей
        def hg(prof, ct, pt):
            if prof == 'ПодкатЛН_2_3А-2':
                if pt > 10:
                    return True
                else:
                    return False
            else:
                if ct > 20:
                    return True
                else:
                    return False

        err = 0
        prof = self.profilename
        # Создание массива с именами файлов в папке отдельного профиля
        direct1 = os.listdir(
            path=self.path + '/' + prof)
        # Вывод имени обрабатываемого файла в консоль
        print('Обработка профиля ', prof, '\n')
        df2 = pd.DataFrame()
        # Цикл обработки периодов по заданному профилю
        for i in range(len(direct1)):
            # Если требуется обработать файлы еще раз, итоговые файлы не пройдут по условию
            if ('maintable' in direct1[i]) or ('basetable' in direct1[i]) or ('worktable' in direct1[i]):
                continue
            # Добавление файла в датафрейм
            print('\rОбработка файла', direct1[i])
            df1 = pd.read_csv(
                self.path + '/' + prof + '/' +
                direct1[i], sep='	', converters={'[1:72]' \
                                                        : str, '[1:142]': str, '[88.16]': str, '[88.18]': str,
                                                    '[1:141]': str, '[1:143]': str, '[170:7]': str})
            # Переименование столбцов
            if df1.columns[1] == '[1:72]':
                df1.rename(columns={'[1:72]': 'Move'}, inplace=True)
                df1.rename(columns={'[1:142]': 'Amperage'}, inplace=True)
                df1.rename(columns={'[88.16]': 'Controler4'}, inplace=True)
                df1.rename(columns={'[88.18]': 'Controler5'}, inplace=True)
                df1.rename(columns={'[1:141]': 'Speed'}, inplace=True)
                df1.rename(columns={'[1:143]': 'Power'}, inplace=True)
                df1.rename(columns={'[170:7]': 'Tstand'}, inplace=True)
            # Отсеивание таблиц от 'мусорных' данных
            for i in range(len(df1.columns)):
                df1 = df1.loc[(df1[df1.columns[i]] != '1,#INF') &
                              (df1[df1.columns[i]].notna()) &
                              (df1[df1.columns[i]] != '-1,#IND') &
                              (df1[df1.columns[i]] != 'nan') & (df1[df1.columns[i]] != '1.#INF')]
            # Задание списков с данными из столбцов
            timelist = list(df1['Time'])
            amplist = list(df1['Amperage'])
            powlist = list(df1['Power'])
            speedlist = list(df1['Speed'])
            temprlist = list(df1['Tstand'])
            c4list = list(df1['Controler4'])
            c5list = list(df1['Controler5'])
            # Цикл преобразования данных в числа
            for i in range(len(amplist)):
                amplist[i] = amplist[i].replace(',', '.')
                amplist[i] = float(amplist[i])
                powlist[i] = powlist[i].replace(',', '.')
                powlist[i] = float(powlist[i])
                speedlist[i] = speedlist[i].replace(',', '.')
                speedlist[i] = float(speedlist[i])
                temprlist[i] = temprlist[i].replace(',', '.')
                temprlist[i] = float(temprlist[i])
                c4list[i] = int(c4list[i])
                c5list[i] = int(c5list[i])
            # Удаление данных 5 клети, если они попадаются как первый проход, и 4 клети, если проход уже идет на момент времени в первой строке файла
            if float(amplist[0]) > 20:
                while int(c5list[0]) == 0:
                    amplist.pop(0)
                    powlist.pop(0)
                    speedlist.pop(0)
                    temprlist.pop(0)
                    c4list.pop(0)
                    c5list.pop(0)
                    timelist.pop(0)
            if int(c4list[0]) == 0:
                while int(c4list[0]) == 0:
                    amplist.pop(0)
                    powlist.pop(0)
                    speedlist.pop(0)
                    temprlist.pop(0)
                    c4list.pop(0)
                    c5list.pop(0)
                    timelist.pop(0)
            # Объявление переменных, списков и меток(флагов)
            prevtn = 5  # Предыдущий номер прохода
            numstand = list()  # Список с номерами клети
            ft = 1  # First time - метка времени начала прохода
            instand = list()  # Список заполняющийся 1 если заготовка в клети, и 0 если нет (для графика)
            counttimes = 0  # Время в миллисекундах нахождения заготовки в клети
            flagcountstarted = 0  # Флаг наличия заготовки в клети в данный момент времени и начала подсчета данных
            flagcountstarted2 = 0  # Флаг наличия данных о температуре, и начала их подсчета
            # Временные списки со значениями данных для каждого прохода. Очищаются после зансения данных по ним в датафрейм
            amplisttemp = list()
            powlisttemp = list()
            speedlisttemp = list()
            temprlisttemp = list()
            #
            iprev = 0 #Предыдущее значение тока (Можно было сделать через индексы, но я сделал через присваивание в самом конце цикла
            sdstand = 0 # Переменная для записи индексов начального времени для определения номера клети в которой была заготовка на момент начала прохода
            lableiprev = 1  # Метка, которая меняет iprev только если проход не начался
            lablenullstand = 1  # Метка, которая добавляет 0 в список номера клети если проход не начался
            instandlable = 1  # То же самое, только для факта наличия прохода
            instandtemp = list()  # Временный список который добавляется в общий список instand после каждого прохода
            numstandtemp = list()  # То же самое для numstand
            evercountlable = 0  # Время начала первого прохода
            ttmx = 0  # Максимальная температура
            ttmn = 0  # Средняя температура
            preved = 0  # Предыдущая дата окончания прохода
            # Цикл для нахождения проходов, и добавления их в конечный файл. За динамическую переменную берется текущее значение тока.
            # В этом цикле при определенных условиях могут менятся значения меток(флагов)
            for i in range(len(amplist)):
                # Если в клети нету заготовки, то на графике номер клети равен 0
                if lablenullstand == 1:
                    numstand.append(0)
                if instandlable == 1:
                    instand.append(0)
                # Добавление данных тепрературы, если она больше 0
                if temprlist[i] > 0:
                    temprlisttemp.append(temprlist[i])
                    flagcountstarted2 = 1
                # Запись средней и максимальной теммпературы, если тепература снова равна 0
                if flagcountstarted2 == 1:
                    try:
                        if temprlist[i] == 0 and temprlist[i + 1] == 0 and temprlist[i + 2] == 0:
                            ttmx = max(temprlisttemp)
                            ttmn = np.mean(temprlisttemp)
                            df2.loc[len(df2['TempMax']) - 1, 'TempMax'] = ttmx
                            df2.loc[len(df2['TempMean']) - 1, 'TempMean'] = ttmn
                            flagcountstarted2 = 0
                            temprlisttemp.clear()
                    except IndexError:
                        if temprlist[i] == 0:
                            ttmx = max(temprlisttemp)
                            ttmn = np.mean(temprlisttemp)
                            df2.loc[len(df2['TempMax']) - 1, 'TempMax'] = ttmx
                            df2.loc[len(df2['TempMean']) - 1, 'TempMean'] = ttmn
                            flagcountstarted2 = 0
                            temprlisttemp.clear()
                # Проверка нахождения заготовки в клети через разницу между текущим и предыдущим значениями тока.
                if amplist[i] - iprev > 10:  # Заготовка в клети
                    lableiprev = 0
                    # Здесь переменной sd задается время начала только в первый проход цикла
                    if ft == 1:
                        sd = datetime.datetime.strptime(timelist[i], "%d.%m.%Y %H:%M:%S.%f")
                        sdstand = i
                        # Вычисление паузы между проходами, используя дату начала и предыдущую дату окончания прохода.
                        if type(preved) == int:
                            pause = 'null'
                        else:
                            pause = str(sd - preved)
                        ft = 0
                    # Переменная evercount нужна для вывода графика начиная с момента первого прохода. Задается только один раз за весь главный цикл
                    if evercountlable == 0:
                        evercount = i
                        evercountlable = 1
                    counttimes += 1
                    flagcountstarted = 1
                    # Добавление текущих значений параметров во временные списки
                    amplisttemp.append(amplist[i])
                    powlisttemp.append(powlist[i])
                    speedlisttemp.append(speedlist[i])
                    if c4list[i] == 1:
                        numstandtemp.append(1)
                    if c5list[i] == 1:
                        numstandtemp.append(2)
                    lablenullstand = 0
                    instandtemp.append(1)
                    instandlable = 0
                if amplist[i] - iprev <= 10 and flagcountstarted == 1:  # Итоговый подсчёт
                    # Сброс значений меток
                    lableiprev = 1
                    lablenullstand = 1
                    instandlable = 1
                    flagcountstarted = 0
                    ft = 1
                    # Вычисление максимальной мощности
                    if powlisttemp != []:
                        powmax = max(powlisttemp)
                    else:
                        powmax = 0
                    # Добавление данных в итоговую таблицу. За условия берутся время подьема значения тока или максимальная мощнось
                    if hg(prof, counttimes, powmax):
                        counttimes = 0
                        # Нахождение номера клети
                        if c4list[sdstand] == 1:
                            tn = 1
                        if c5list[sdstand] == 1:
                            tn = 2
                        # Вычисление всех необходимых параметров, исходя из данных во временных списках
                        ed = datetime.datetime.strptime(timelist[i - 1], "%d.%m.%Y %H:%M:%S.%f")
                        numstand += numstandtemp
                        instand += instandtemp
                        tamx = max(amplisttemp)
                        tamn = np.mean(amplisttemp)
                        tpmx = max(powlisttemp)
                        tpmn = np.mean(powlisttemp)
                        tsmx = max(speedlisttemp)
                        # Добавление данных в таблицу, толко если предыдущий номер клети не равен текущему, иначе, +1 к счетчику неправильно подсчитанных проходов
                        if prevtn == tn:
                            err += 1
                        else:
                            newrow = pd.DataFrame(
                                {'Startdate': [sd], 'Enddate': [ed], 'Pause': [pause], 'NumStand': [tn],
                                 'AmpMax': [tamx],
                                 'AmpMean': [tamn], \
                                 'PowMax': [tpmx], 'PowMean': [tpmn], 'SpeedMax': [tsmx], 'TempMax': [ttmx],
                                 'TempMean': [ttmn]})
                            df2 = pd.concat([df2, newrow], ignore_index=True)
                        ttmx = 0
                        ttmn = 0
                        # Теперь значение текущих переменных tn и ed присвоены к предыдущим
                        prevtn = tn
                        preved = ed
                        # Очищение временных списков
                        amplisttemp.clear()
                        powlisttemp.clear()
                        speedlisttemp.clear()
                        numstandtemp.clear()
                        instandtemp.clear()
                    else:
                        counttimes = 0
                        amplisttemp.clear()
                        powlisttemp.clear()
                        speedlisttemp.clear()
                        numstandtemp.clear()
                        instandtemp.clear()
                if lableiprev == 1:
                    iprev = amplist[i]
            # Удаление последней строки из таблицы, если в ней данные по проходу на 4 клети
            numlist = list(df2['NumStand'])
            lastnumber = len(numlist) - 1
            if numlist[lastnumber] == 1:
                df2 = df2.drop(labels=[lastnumber], axis=0)
            print('\nОшибочно обработаных проходов:', err)

            # if ((err / len(amplist)) * 100) <= 5:
            #     df2.to_csv(
            #         self.path + '/' + prof + '/' + 'maintable.txt')
            #     df2.to_csv(
            #         self.path + '/' + prof + '/' + 'worktable.txt')
            #     df2.to_csv(
            #         self.path + '/' + prof + '/' + 'basetable.txt')
            # numlist = list(df2['NumStand'])
            # Вывод графика (В данный момент отключен)
            yn = 'n'
            if yn == 'y':
                low_ground = 0
                high_ground = len(amplist)
                fig, ax = plt.subplots(3, 1, figsize=(16, 15))
                ax[0].plot(amplist[low_ground:high_ground])
                ax[0].set_title("Ток")
                ax[1].plot(numstand[low_ground:high_ground])
                ax[1].set_title("Номер клети")
                ax[2].plot(instand[low_ground:high_ground])
                ax[2].set_title("Флаг наличия заготовки в клети")
                plt.show()
        return df2


    def CalculateAcceptableValues45Stand(self):
        """
        Метод по расчету распределения данных по проходам и запись текущего профиля и данныхих в бб
        """
        pth = self.path
        if pth == None:
            print("Данные не обработаны")
        else:
            # Вывод файлов в директории, чтобы пользователь легко мог выбрать файл, который ему нужно обработать
            filename = '/maintable.txt'
            # Цикл, обрабатывающий все файлы если пользователь ввел 'all', либо проходящий всего 1 раз
            # Определение имени обрабатываемого файла/файлов из массива с именами файлов
            print(self.profilename)
            self.count_proh = 2
            df = pd.read_csv(
                pth + '/' + self.profilename.replace(' ', '_').replace('/', '') + filename)
            pgsql_connetcion = ''
            pgsql_read_con = create_engine(pgsql_connetcion)
            pg_query_writter = """INSERT INTO aatp.acceptable_values (ref_profile,description_profile,passage,q1,q2,q3,x1,x2, 
                                                                       ref_parameter,ref_location,ref_equipment) 
                                                                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            pg_query_read_profile = """SELECT obj_id
                                                    FROM aatp.acceptable_profiles
                                                    WHERE ref_location = 'nsi.spr_terminal_techpa_location,85' and profile_name = '{}' """
            pg_query_write_profile = "INSERT INTO aatp.acceptable_profiles (profile_name, ref_location, passages_num) VALUES (%s,%s,%s)"
            profile = list(pd.read_sql_query(pg_query_read_profile.format(self.profilename), pgsql_read_con)['obj_id'])[
                0] if len(list(
                pd.read_sql_query(pg_query_read_profile.format(self.profilename), pgsql_read_con)[
                    'obj_id'])) == 1 else ''
            location = 'nsi.spr_terminal_techpa_location,85'
            equipment = 'nsi.spr_equipment,100164'
            param = ['nsi.spr_terminal_techpa,14', 'nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,16',
                     'nsi.spr_terminal_techpa,15', 'nsi.spr_terminal_techpa,11', 'nsi.spr_terminal_techpa,49',
                     'nsi.spr_terminal_techpa,1']
            listpar = ['AmpMax', 'AmpMean', 'PowMax', 'PowMean', 'SpeedMax', 'TempMax', 'TempMean']
            if profile != '':
                print('Этот профиль уже был добавлен')
            else:
                row = [self.profilename, location, self.count_proh]
                pgsql_read_con.execute(pg_query_write_profile, row)
                profile = \
                    list(pd.read_sql_query(pg_query_read_profile.format(self.profilename), pgsql_read_con)['obj_id'])[0]
                for par in range(len(listpar)):
                    meanfile = []
                    for i in range(1, self.count_proh + 1):
                        temparr = df[(df['NumStand'] == i) & (df[f'{listpar[par]}'] > 0)][f'{listpar[par]}']
                        q1 = float(np.quantile(temparr, 0.25))
                        q3 = float(np.quantile(temparr, 0.75))
                        iqr = 1.5 * (q3 - q1)
                        minn = float(np.min(temparr))
                        maxx = float(np.max(temparr))
                        mid = float(np.median(temparr))
                        x1 = q1 - iqr if q1 - iqr > minn else minn
                        x2 = q3 + iqr if q3 + iqr < maxx else maxx
                        newrow = [profile, self.profilename, i, q1, mid, q3, x1, x2, param[par], location,
                                  equipment]
                        meanfile.append(newrow)
                    idd = pgsql_read_con.execute(pg_query_writter, meanfile)
                print('Профиль успешно добавлен')


    def ProcessingIbaSignalsRezka(self, path, profilename):

        """
        Метод по предобработке сигналов из ибы в итоговую таблицу резов по участку резки

        Атрибуты:
        :param path: str - Каталог с проэкспортированными периодами проката по участку резки
        :param profilename: str - Название профиля как в pg
        """

        self.path = path
        self.profilename = profilename
        path = os.path.join(self.path, self.profilename.replace(' ', '_').replace('/', ''))
        tempdf = []
        count_bar = 1
        for file in os.listdir(path):

            # Считваем период
            df = pd.read_csv(os.path.join(path, file), low_memory=False, index_col=None, header=0, sep='	')
            df.columns = ['dt', 'disc_diameter', 'disc_speed', 'vibration', 'time_rez', 'time_cycle', 'power',
                          'amperage',
                          'flag_r_5stand', 'flag_r_rezka']

            # Валидация
            for i in range(len(df.columns)):
                df = df.loc[(df[df.columns[i]] != '1,#INF') &
                            (df[df.columns[i]].notna()) &
                            (df[df.columns[i]] != '-1,#IND') &
                            (df[df.columns[i]] != 'nan')]
                df[df.columns[i]].loc[df[df.columns[i]] != '0'] = df[df.columns[i]].astype(str).str.replace(',', '.')
                if i == 0:
                    df[df.columns[i]] = pd.to_datetime(df[df.columns[i]], format='%d.%m.%Y %H:%M:%S.%f')
                else:
                    df[df.columns[i]] = pd.to_numeric(df[df.columns[i]])

            # Расчет заготовок резов и ТП

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
            g = g[1:]
            # g = g.drop(columns=['bar'])
            tempdf.append(g)
        maindf = pd.concat(tempdf, axis=0, ignore_index=True)
        g = maindf.groupby(['bar']).max()['cycle']
        count = []
        for i in range(1, g.max() + 1):
            count.append(len(g[g == i]))
        print(count)
        print('Введите ограничение по количеству резов')
        self.count_proh = int(input())
        g = maindf.groupby('bar').max()
        g = g.reset_index()
        maindf['del'] = False
        for i in list(g[g['cycle'] > self.count_proh]['bar']):
            maindf['del'] = np.where(maindf['bar'] == i, True, maindf['del'])
        maindf = maindf[maindf['del'] == False]
        maindf.to_csv(os.path.join(path, 'maintable.txt'))
        maindf = maindf.drop(columns=['bar'])
        maindf.to_csv(os.path.join(path, 'basetable.txt'), index=False)
        maindf.to_csv(os.path.join(path, 'worktable.txt'), index=False)
        return maindf


    def CalculateAcceptableValuesRezka(self):
        if self.path == None:
            print('Данные не обработаны')
        else:
            path = os.path.join(self.path, self.profilename.replace(' ', '_').replace('/', ''))
            df = pd.read_csv(os.path.join(path, 'maintable.txt'))
            if self.count_proh == None:
                self.count_proh = int(df.cycle.max())
            g = df.groupby(['bar']).max()['cycle']
            count = []
            for i in range(1, g.max() + 1):
                count.append(len(g[g == i]))
            print(count, 'Макс резов: ', self.count_proh, sep=' ')
            print('Записываем данные в pg? y/n')
            r = input()
            if r == 'y':
                pgsql_connetcion = ''
                pgsql_read_con = create_engine(pgsql_connetcion)
                pg_query_writter = """INSERT INTO aatp.acceptable_values (ref_profile,description_profile,passage,q1,q2,q3,x1,x2, 
                                                               ref_parameter,ref_location,ref_equipment) 
                                                               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                pg_query_read_profile = """SELECT obj_id
                                            FROM aatp.acceptable_profiles
                                            WHERE ref_location = 'nsi.spr_terminal_techpa_location,52' and profile_name = '{}' """
                pg_query_write_profile = "INSERT INTO aatp.acceptable_profiles (profile_name, ref_location, passages_num) VALUES (%s,%s,%s)"
                profile = \
                list(pd.read_sql_query(pg_query_read_profile.format(self.profilename), pgsql_read_con)['obj_id'])[
                    0] if len(list(
                    pd.read_sql_query(pg_query_read_profile.format(self.profilename), pgsql_read_con)[
                        'obj_id'])) == 1 else ''

                param = ['nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,14', 'nsi.spr_terminal_techpa,15',
                         'nsi.spr_terminal_techpa,16',
                         'nsi.spr_terminal_techpa,12', 'nsi.spr_terminal_techpa,13', 'nsi.spr_terminal_techpa,10',
                         'nsi.spr_terminal_techpa,17', 'nsi.spr_terminal_techpa,18', 'nsi.spr_terminal_techpa,19',
                         'nsi.spr_terminal_techpa,20']
                listpar = ['amperage_mean', 'amperage_max', 'power_mean', 'power_max', 'vibration_mean',
                           'vibration_max',
                           'interformation_pause', 'time_cycle', 'time_rez', 'disc_speed', 'disc_diameter']
                location = 'nsi.spr_terminal_techpa_location,52'
                equipment = 'nsi.spr_equipment,100164'

                if profile != '':
                    print('Этот профиль уже был добавлен')
                else:
                    row = [self.profilename, location, self.count_proh]
                    pgsql_read_con.execute(pg_query_write_profile, row)
                    profile = \
                    list(pd.read_sql_query(pg_query_read_profile.format(self.profilename), pgsql_read_con)['obj_id'])[0]
                    for par in range(len(listpar)):
                        meanfile = []
                        stats = []
                        for i in range(1, self.count_proh + 1):
                            temparr = df[(df['cycle'] == i) & (df[f'{listpar[par]}'] > 0)][f'{listpar[par]}']
                            q1 = float(np.quantile(temparr, 0.25))
                            q3 = float(np.quantile(temparr, 0.75))
                            iqr = 1.5 * (q3 - q1)
                            minn = float(np.min(temparr))
                            maxx = float(np.max(temparr))
                            mid = float(np.median(temparr))
                            x1 = q1 - iqr if q1 - iqr > minn else minn
                            x2 = q3 + iqr if q3 + iqr < maxx else maxx
                            newrow = [profile, self.profilename, i, q1, mid, q3, x1, x2, param[par], location,
                                      equipment]
                            meanfile.append(newrow)
                        idd = pgsql_read_con.execute(pg_query_writter, meanfile)
                    print('Профиль успешно добавлен')
            else:
                print('Отказ')
