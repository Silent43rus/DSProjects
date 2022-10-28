import time
from PyQt5.Qt import *
from PyQt5.QtGui import QPixmap
from PyQt5 import uic
import sys
import traceback
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import gc
from sqlalchemy import create_engine
import datetime
import tempfile

pgsql_connection = ''
pgsql_work_con = create_engine(pgsql_connection)

class ModelUpdater(QMainWindow):

    """ Класс для обновления модели
        Атрибуты:
            mainform : QMainWindow
            pgsql_work_con : create_engine()
            profile = str
        Методы:
            init(mainform) - передаем атрибуты основной формы в дочернюю
            update_request
            change_profile
            update_table
            """

    def __init__(self, mainform, parent=None):
        super(ModelUpdater, self).__init__()
        self.mainform = mainform
        self.pgsql_work_con = pgsql_work_con
        self.profile = None

        # Load ui
        uic.loadUi("RezkaUpdateModel.ui", self)

        # Define widgets
        self.button_exit = self.findChild(QPushButton, 'pushButton_3')
        self.combobox_profile_load = self.findChild(QComboBox, 'comboBox_3')
        self.combobox_profile_load.addItems(list(self.mainform.profile_dataframe['profile_name']))
        self.profile = list(self.mainform.profile_dataframe[self.mainform.profile_dataframe['profile_name'] == self.combobox_profile_load.currentText()]['obj_id'])[0]
        self.table_periods = self.findChild(QTableWidget, 'tableView_2')
        self.button_update = self.findChild(QPushButton, 'pushButton_2')
        self.mainlayout = self.findChild(QFrame, 'verticalFrame')
        self.mainlayout.resize(self.size().width(), self.size().height())

        # Function
        self.button_exit.clicked.connect(self.exit)
        self.combobox_profile_load.currentTextChanged.connect(self.change_profile)
        self.button_update.clicked.connect(self.update_request)

        # Run function

    def resizeEvent(self, QResizeEvent):
        self.mainlayout.resize(self.size().width(),self.size().height())

    def update_request(self):
        "Функция расчета отмеченных пользователем строк и запрос на обновление в pg"

        pg_query_update_period = """UPDATE aatp.periods 
                                    SET is_changed = True 
                                    WHERE obj_id = '{}' """
        try:
            for row in range(self.table_periods.rowCount()):
                if self.table_periods.item(row, self.table_periods.columnCount() - 1).checkState() == Qt.CheckState.Checked:
                    self.pgsql_work_con.execute(pg_query_update_period.format(list(self.periods_table['obj_id'])[row]))
            time.sleep(7)
            self.update_table()
        except:
            print(traceback.format_exc())

    def change_profile(self):
        "Функция перерасчета профиля и вызов обновления таблицы"

        try:
            self.profile = list(self.mainform.profile_dataframe[self.mainform.profile_dataframe['profile_name'] == self.combobox_profile_load.currentText()]['obj_id'])[0]
            self.update_table()
        except:
            print(traceback.format_exc())

    def update_table(self):
        "Функция обновления таблицы с проходами по выбранному профилю"

        try:
            pg_query_read_profiles = """SELECT per.obj_id, ref_profile, prof.profile_name, dt_start, dt_stop, is_in_model, is_changed
                                        FROM aatp.periods as per
                                        LEFT JOIN aatp.acceptable_profiles as prof
                                        ON per.ref_profile = prof.obj_id
                                        WHERE ref_profile = '{}' """
            df = pd.read_sql(pg_query_read_profiles.format(self.profile),self.pgsql_work_con)
            df = df.reset_index()
            df = df.drop(columns=['index'])
            self.periods_table = df
            df = df.drop(columns=['ref_profile', 'is_changed', 'obj_id'])
            df.columns = ['Номенклатура', 'Начало проходов', 'Окончание проходов', 'Статус в модели']

            self.table_periods.setColumnCount(0)
            self.table_periods.setRowCount(0)
            self.table_periods.setColumnCount(len(df.columns.values.tolist()) + 1)
            self.table_periods.setHorizontalHeaderLabels(['Номенклатура', 'Начало проходов', 'Окончание проходов', 'Статус в модели', 'Изменить статус'])


            for i, row in df.iterrows():
                self.table_periods.setRowCount(self.table_periods.rowCount() + 1)
                for j in range(len(df.columns.values.tolist()) + 1):
                    if j == 1 or j == 2:
                        item = QTableWidgetItem(str(row[j])[:19])
                        self.table_periods.setItem(i, j, item)
                        item.setTextAlignment(Qt.AlignCenter)
                    elif j < len(df.columns.values.tolist()):
                        item = QTableWidgetItem(str(row[j]))
                        self.table_periods.setItem(i, j, item)
                        item.setTextAlignment(Qt.AlignCenter)
                    if j == len(df.columns.values.tolist()):
                        item = QTableWidgetItem()
                        item.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)
                        item.setCheckState(Qt.CheckState.Unchecked)
                        self.table_periods.setItem(i,j,item)
            self.table_periods.viewport().installEventFilter(self)
        except Exception as ex:
            print(traceback.format_exc())

    def exit(self):
        self.close()
        self.destroy()

class UpdaterThread(QThread):
    def __init__(self, mainform, parent=None):
        super(UpdaterThread, self).__init__()
        self.mainform = mainform

    def run(self):
        while True:
            self.mainform.label_date.setText(datetime.datetime.now().strftime('%d.%m.%Y'))
            self.mainform.label_time.setText(datetime.datetime.now().strftime('%H:%M:%S'))
            time.sleep(1)

class UI(QMainWindow):
    def __init__(self):
        super(UI, self).__init__()
        self.profile_dataframe = None
        self.periods_table = None
        self.dfvisual = None
        self.profile = None
        self.date = None
        self.row = None
        self.starttime = '00:00:00'
        self.endtime = '23:59:59'
        self.path = ''
        self.temp_path = ''
        self.textprofile = None
        self.tekdata = None


        # Load ui
        uic.loadUi("RezkaClientAS.ui", self)

        # Define Widgets
        self.label_plot1 = self.findChild(QLabel, 'label_6')
        self.label_plot2 = self.findChild(QLabel, 'label_7')
        self.label_plot3 = self.findChild(QLabel, 'label_5')
        self.label_plot4 = self.findChild(QLabel, 'label_13')
        self.label_plot5 = self.findChild(QLabel, 'label_12')
        self.label_plot6 = self.findChild(QLabel, 'label_29')
        self.choise_param = self.findChild(QComboBox, 'comboBox')
        self.label_accetp_value = self.findChild(QLabel, 'label_view')
        self.table_accetp_value = self.findChild(QTableWidget, 'tableView')
        self.button_save_range = self.findChild(QPushButton, 'btnSaveMas')
        self.timeedit_start = self.findChild(QTimeEdit, 'timeEdit')
        self.timeedit_stop = self.findChild(QTimeEdit, 'timeEdit_2')
        self.label_date = self.findChild(QLabel, 'dateLabel')
        self.label_time = self.findChild(QLabel, 'timeLabel')
        self.tabWidget_accept_value = self.findChild(QTabWidget, 'tabWidget')
        self.rbutton_profile = self.findChild(QRadioButton, 'radioButton')
        self.rbutton_date = self.findChild(QRadioButton, 'radioButton_2')
        self.label_date_load = self.findChild(QDateEdit, 'dateEdit')
        self.combobox_profile_load = self.findChild(QComboBox, 'comboBox_2')
        self.table_periods = self.findChild(QTableWidget, 'tableView_2')
        self.button_export = self.findChild(QPushButton, 'pushButton_3')
        self.button_save_file = self.findChild(QPushButton, 'pushButton_2')
        self.label_logo = self.findChild(QLabel, 'label')
        self.label_logo.setPixmap(QPixmap('logo.png'))
        self.button_open_model_updater = self.findChild(QPushButton, 'pushButton_4')
        self.mainlayout = self.findChild(QFrame, 'verticalFrame_5')
        self.mainlayout.resize(self.size().width(),self.size().height())


        # Function
        self.rbutton_date.clicked.connect(self.choice_date)
        self.rbutton_profile.clicked.connect(self.choice_profile)
        self.combobox_profile_load.currentTextChanged.connect(self.change_profile)
        self.label_date_load.dateChanged.connect(self.change_date)
        self.button_save_range.clicked.connect(self.runing)
        self.button_export.clicked.connect(self.save_dt)
        self.button_open_model_updater.clicked.connect(self.open_model_updater)

        # Start function
        self.updater = UpdaterThread(mainform=self)
        self.updater.start()
        self.profile_dataframe = self.load_all_profiles()
        self.init_temp()
        # Show App
        self.show()
        self.resize(self.size().width() + 5, self.size().height() + 5)

    def init_temp(self):
        self.showMaximized()
        self.labels = [self.label_plot1, self.label_plot2, self.label_plot3, self.label_plot4, self.label_plot5, self.label_plot6]
        path1 = os.path.join(tempfile.gettempdir(), 'aatp')
        path2 = os.path.join(path1, 'rezka_clientas')
        if not os.path.exists(path1):
            os.mkdir(path1)
        if not os.path.exists(path2):
            os.mkdir(path2)
        self.temp_path = path2


    def resizeEvent(self, QResizeEvent):
        self.mainlayout.resize(self.size().width()-20,self.size().height())
        self.table_accetp_value.resize(self.tabWidget_accept_value.size().width(),self.tabWidget_accept_value.size().height()-20)
        self.label_accetp_value.resize(self.tabWidget_accept_value.size().width(),self.tabWidget_accept_value.size().height()-20)

    def open_model_updater(self):
        self.modelUpdater = ModelUpdater(mainform=self)
        self.modelUpdater.show()

    def save_dt(self):
        try:
            nomen = self.textprofile.replace(' ','_').replace('/','')
            self.path = QFileDialog.getSaveFileName( self,f'Save File', f"{nomen}_{self.tekdata}_{self.starttime.replace(':','.')}_{self.endtime.replace(':','.')}",
                                                     filter='Книга Excel (*.xlsx)')[0]
            if self.path != '':
                df = self.dfvisual
                df = df.drop(columns=['id_posad'])
                df.columns = ['Дата начала реза', 'Дата окончания реза', 'Номер реза','Ср ток', 'Макс ток','Средняя мощность','Максимаьлная мощность',
                              'Средняя вибрация', 'Максимальная вибрация', 'Паузы перед резами', 'Время цикла', 'Время реза', 'Скорость вращения диска',
                              'Диаметр диска', 'Отклонение ср тока от нормы', 'Отклонение макс тока от нормы', 'Отклонение ср мощности от нормы', 'Отклонение макс мощности от нормы',
                              'Отклонение ср вибрации от нормы', 'Отклонение макс вибрации от нормы', 'Отклонение паузы перед резом от нормы', 'Отклонение времени цикла от нормы',
                              'Отклонение времени реза от нормы', 'Отклонение скорости вращения диска от нормы', 'Отклонение диаметра диска от нормы', 'Номенклатура проката']
                df.to_excel(self.path, index=False)

        except Exception as ex:
            print(traceback.format_exc())


    def gettime(self):
        try:
            dtstart = self.timeedit_start.time()
            dtend = self.timeedit_stop.time()
            self.starttime = dtstart.toString(self.timeedit_start.displayFormat())
            self.endtime = dtend.toString(self.timeedit_stop.displayFormat())
        except Exception as ex:
            print(traceback.format_exc())

    # Если в dateedit выбирается новая дата
    def change_date(self):
        try:
            self.button_save_range.setEnabled(False)
            self.button_export.setEnabled(False)
            dt = self.label_date_load.date()
            dt_str = dt.toString(self.label_date_load.displayFormat()).split('.')
            self.date = '-'.join(dt_str[::-1])
            self.update_table(self.load_period())
        except Exception as ex:
            print(traceback.format_exc())

    # Если в combobox выбирают другой профиль
    def change_profile(self):
        try:
            self.button_save_range.setEnabled(False)
            self.button_export.setEnabled(False)
            self.row = None
            if self.profile_dataframe is not None:
                self.profile = list(self.profile_dataframe[self.profile_dataframe['profile_name'] == self.combobox_profile_load.currentText()]['obj_id'])[0]
                self.update_table(self.load_period())
        except Exception as ex:
            print(traceback.format_exc())

    # Загрузка всех профилей
    def load_all_profiles(self):
        try:
            pg_query_read_profiles = """SELECT *
                                        FROM aatp.acceptable_profiles
                                        WHERE ref_location = 'nsi.spr_terminal_techpa_location,52' """

            df = pd.read_sql_query(pg_query_read_profiles, pgsql_work_con)
            self.combobox_profile_load.addItems(list(df['profile_name']))
            self.profile = list(df[df['profile_name'] == self.combobox_profile_load.currentText()]['obj_id'])[0]
            return df
        except Exception as ex:
            print(traceback.format_exc())

    def load_period(self):
        try:
            pg_query_read_periods = """ SELECT m.ref_nomenclature, m.description_nomenclature, m.ref_profile, t.dt_start, t.dt_stop, t.dt as dt
                                        FROM (
                                            SELECT ref_nomenclature, MIN(dt_start) as dt_start, MAX(dt_stop) as dt_stop, dt_start::DATE as dt
                                            FROM aatp.cutting_cuts
                                            GROUP BY ref_nomenclature, dt_start::DATE
                                            ) t JOIN aatp.cutting_cuts m ON m.ref_nomenclature = t.ref_nomenclature AND t.dt_start = m.dt_start
                                        ORDER BY t.dt """
            df = pd.read_sql_query(pg_query_read_periods,pgsql_work_con)
            if self.profile is None:
                df = df[df['dt'].astype(str) == self.date]
            if self.date is None:
                df = df[df['ref_profile'] == self.profile]
            df = df.reset_index()
            df = df.drop(columns=['index'])
            return df
        except Exception as ex:
            print(traceback.format_exc())

    def update_table(self,df):
        try:
            self.periods_table = df
            df = df.drop(columns=['ref_nomenclature', 'ref_profile', 'dt'])
            df.columns = ['Номенклатура', 'Начало резов', 'Окончание резов']

            self.table_periods.setColumnCount(0)
            self.table_periods.setRowCount(0)
            self.table_periods.setColumnCount(len(df.columns.values.tolist()))
            self.table_periods.setHorizontalHeaderLabels(['Номенклатура', 'Начало резов', 'Окончание резов'])

            for i, row in df.iterrows():
                self.table_periods.setRowCount(self.table_periods.rowCount() + 1)
                for j in range(len(df.columns.values.tolist())):
                    if j > 0:
                        item = QTableWidgetItem(str(row[j])[:19])
                        self.table_periods.setItem(i, j, item)
                        item.setTextAlignment(Qt.AlignCenter)
                    else:
                        item = QTableWidgetItem(str(row[j]))
                        self.table_periods.setItem(i, j, item)
            self.table_periods.viewport().installEventFilter(self)
            self.table_periods.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            self.table_periods.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        except Exception as ex:
            print(traceback.format_exc())

    def eventFilter(self, source, event):
        if self.table_periods.selectedIndexes() != []:
            if event.type() == QEvent.MouseButtonRelease:
                self.row = self.table_periods.currentRow()
                self.button_save_range.setEnabled(True)

        return QObject.event(source, event)


    # Выбираем по дате
    def choice_date(self):
        self.profile = None
        self.combobox_profile_load.setEnabled(False)
        self.label_date_load.setEnabled(True)
        self.change_date()


    # Выбираем по профилю
    def choice_profile(self):
        self.date = None
        self.label_date_load.setEnabled(False)
        self.combobox_profile_load.setEnabled(True)
        self.change_profile()


    def update_plots(self,dfvisual,dfmean, starttime, endtime, tekdata, list_mean,res_list,tekparam,count_pass, ruparam):
        try:
            print(dfvisual.num_proh.max())
            # Формируем данные
            df = dfvisual[(dfvisual['dt_start'] > f'{tekdata} {starttime}') & (dfvisual['dt_stop'] < f'{tekdata} {endtime}')]
            xdata = [i for i in range(dfvisual.num_proh.max())]
            ydata = [i for i in range(dfvisual.num_proh.max())]
            listcolors = [i for i in range(dfvisual.num_proh.max())]
            for i in range(dfvisual.num_proh.max()):
                xdata[i] = np.array(df[df['num_proh'] == i + 1]['dt_start'])
                ydata[i] = np.array(df[df['num_proh'] == i + 1][list_mean[tekparam]])
                listcolors[i] = np.array(df[df['num_proh'] == i + 1][res_list[tekparam]])

            # Создаем графики
            for j in range(dfvisual.num_proh.max()):
                fig, ax = plt.subplots(1, 1, figsize=(18, 8))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                for i in range(len(ydata[j])):
                    ax.plot(xdata[j][i - 1:i + 1], ydata[j][i - 1:i + 1], color=listcolors[j][i], linewidth=4)
                    ax.grid(True)
                plt.title(f'{j + 1} Рез')
                plt.xlabel('Время')
                plt.ylabel(ruparam[tekparam])
                plt.savefig(os.path.join(self.temp_path,f'imgpass_{j}.jpg'))
            print(xdata[j][i - 1:i + 1])
            print(ydata[j][i - 1:i + 1])
            # Обнавляем картинки
            a = [i for i in range(dfvisual.num_proh.max())]
            for i in range(dfvisual.num_proh.max()):
                a[i] = QPixmap(os.path.join(self.temp_path, f'imgpass_{i}.jpg'))
                a[i] = a[i].scaled(self.label_plot1.size(), Qt.IgnoreAspectRatio, Qt.SmoothTransformation)

            for i, pix in enumerate(a):
                self.labels[i].setPixmap(pix)

            # self.label_plot1.setPixmap(a[0])
            # self.label_plot2.setPixmap(a[1])
            # self.label_plot3.setPixmap(a[2])
            # if count_pass == 5:
            #     self.label_plot4.setPixmap(a[3])
            #     self.label_plot5.setPixmap(a[4])
            plt.clf()
            plt.close("all")
            gc.collect()
        except Exception as ex:
            print(traceback.format_exc())


    def update_validparam(self, valparam, tekparam):
        description = ['Средний Ток', 'Максимальный Ток', 'Средняя мощность', 'Максимальная мощность', 'Средняя вибрация', 'Максимальная вибрация',
                   'Паузы перед резами', 'Время цикла', 'Время реза', 'Скорость вращения диска', 'Диаметр диска']

        stats = []
        for i in range(len(valparam)):
            q1 = float(valparam['q1'][i])
            mid = float(valparam['q2'][i])
            q3 = float(valparam['q3'][i])
            x1 = float(valparam['x1'][i])
            x2 = float(valparam['x2'][i])
            stats.append({'med': mid,
                          'q1': q1,
                          'q3': q3,
                          'whislo': x1,
                          'whishi': x2
                          })

        # Рисуем коробки
        fig, ax = plt.subplots(figsize=(16, 7))
        b = ax.bxp(stats, showfliers=False, patch_artist=True)
        for element in ['boxes', 'whiskers', 'fliers', 'means', 'medians', 'caps']:
            plt.setp(b[element], color='blue', linewidth=2)
        for patch in b['boxes']:
            patch.set(facecolor='lime', linewidth=2)
        ax.grid(True)
        ax.set_title(description[tekparam])
        plt.savefig(os.path.join(self.temp_path, 'box.jpg'))
        a = QPixmap(os.path.join(self.temp_path, 'box.jpg'))
        a = a.scaled(self.label_accetp_value.size(), Qt.IgnoreAspectRatio, Qt.SmoothTransformation)
        self.label_accetp_value.setPixmap(a)

        # Строим таблицу

        class_param = valparam
        class_param.columns = ['Номер реза', 'Норма низ', 'Медиана', 'Норма верх', 'Ср отклонение низ','Ср отклонение верх']
        class_param = class_param.drop(columns=['Номер реза', 'Медиана'])
        class_param['Норма низ'] = list(np.ceil(list(class_param['Норма низ'] * 1000)) / 1000)
        class_param['Норма верх'] = list(np.ceil(list(class_param['Норма верх'] * 1000)) / 1000)
        class_param['Ср отклонение низ'] = list(np.ceil(list(class_param['Ср отклонение низ'] * 1000)) / 1000)
        class_param['Ср отклонение верх'] = list(np.ceil(list(class_param['Ср отклонение верх'] * 1000)) / 1000)

        self.table_accetp_value.setColumnCount(0)
        self.table_accetp_value.setRowCount(0)
        self.table_accetp_value.setColumnCount(len(class_param.columns.values.tolist()))
        self.table_accetp_value.setHorizontalHeaderLabels(class_param.columns.values.tolist())

        for i, row in class_param.iterrows():
            self.table_accetp_value.setRowCount(self.table_accetp_value.rowCount() + 1)
            for j in range(self.table_accetp_value.columnCount()):
                self.table_accetp_value.setItem(i, j, QTableWidgetItem(str(row[j])))
        self.table_accetp_value.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_accetp_value.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)

    def restart_plot(self):
        self.label_plot1.setPixmap(QPixmap())
        self.label_plot2.setPixmap(QPixmap())
        self.label_plot3.setPixmap(QPixmap())
        self.label_plot4.setPixmap(QPixmap())
        self.label_plot5.setPixmap(QPixmap())



    def runing(self):
        plt.rcParams['font.size'] = '25'
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
        list_mean = ['amperage_mean', 'amperage_max', 'power_mean', 'power_max', 'vibration_mean',
                     'vibration_max', 'interformation_pause', 'time_cycle', 'time_rez',
                     'disc_speed', 'disc_diameter', 'num_proh']
        res_list = ['res_amperage_mean', 'res_amperage_max', 'res_power_mean', 'res_power_max', 'res_vibration_mean',
                     'res_vibration_max', 'res_interformation_pause', 'res_time_cycle', 'res_time_rez',
                     'res_disc_speed', 'res_disc_diameter']
        ruparam = ['Средний Ток', 'Максимальный Ток', 'Средняя мощность', 'Максимаьлная мощность', 'Средняя вибрация', 'Максимальная вибрация',
                   'Паузы перед проходами', 'Время цикла', 'Время реза', 'Скорость вращения диска', 'Диаметр диска']
        pg_param_name = ['nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,14', 'nsi.spr_terminal_techpa,15',
                         'nsi.spr_terminal_techpa,16', 'nsi.spr_terminal_techpa,12', 'nsi.spr_terminal_techpa,13',
                         'nsi.spr_terminal_techpa,10', 'nsi.spr_terminal_techpa,17', 'nsi.spr_terminal_techpa,18',
                         'nsi.spr_terminal_techpa,19', 'nsi.spr_terminal_techpa,20']
        try:
            if self.row != None:
                self.gettime()
                self.restart_plot()
                tekparam = self.choise_param.currentIndex()
                refnomen = list(self.periods_table['ref_profile'])[self.row]
                startperiod = list(self.periods_table['dt_start'])[self.row]
                stopperiod = list(self.periods_table['dt_stop'])[self.row]
                self.textprofile = list(self.profile_dataframe[self.profile_dataframe['obj_id'] == list(self.periods_table['ref_profile'])[self.row]]['profile_name'])[0]
                tekdata = list(self.periods_table['dt'])[self.row]
                self.tekdata = tekdata
                count_pass = int(list(pd.read_sql_query(pg_query_read_profile.format(refnomen), pgsql_work_con)['passages_num'])[0])
                valparam = pd.read_sql_query(pg_query_read_valpar.format(refnomen,pg_param_name[tekparam]), pgsql_work_con)
                dfvisual = pd.read_sql_query(pg_query_read_passageway.format(refnomen, startperiod, stopperiod), pgsql_work_con)
                dfvisual = dfvisual[(dfvisual['dt_start'] > f'{tekdata} {self.starttime}') & (dfvisual['dt_stop'] < f'{tekdata} {self.endtime}')]
                self.dfvisual = dfvisual
                dfmean = dfvisual[int(f'-{count_pass}'):]
                self.update_plots(dfvisual, dfmean, self.starttime, self.endtime, tekdata, list_mean, res_list, tekparam, count_pass, ruparam)
                self.update_validparam(valparam, tekparam)
                self.button_export.setEnabled(True)
                print('Successfull update graphics')

        except Exception as ex:
            print(traceback.format_exc())


# Init App     border: 1px solid #8f8f91;
# stylesheet = """
# QLabel {
#     border: 1px solid #adadad;
#
#     background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
#                                       stop: 0 #ebebeb, stop: 1 #ebebeb);
# }
#        """

app = QApplication(sys.argv)
# app.setStyleSheet(stylesheet)
UIWindow = UI()
sys.exit(app.exec_())