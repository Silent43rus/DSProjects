import random
import time
import html
from PyQt5.Qt import *
from PyQt5.QtGui import QPixmap
from PyQt5 import uic
import sys
import traceback
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
import matplotlib.dates as mdates
import gc
from sqlalchemy import create_engine
import datetime
import tempfile


class Updater(QThread):
    dataformain = pyqtSignal(object, object,object,object,object,object,object,object, object, object, object, object, object)
    updcolors = pyqtSignal(object, object, object, object)
    updonregion = pyqtSignal(object)
    reset_plots = pyqtSignal()
    def __init__(self, mainform, parent=None):
        super(Updater, self).__init__()
        self.mainform = mainform
        self.mainform.btnSaveMax.clicked.connect(self.gettime)
        self.tekdata = None
        self.starttime = '00:00:00'
        self.endtime = '23:59:59'

    def gettime(self):
        try:
            dtstart = self.mainform.timeeditStart.time()
            dtend = self.mainform.timeeditStop.time()
            self.starttime = dtstart.toString(self.mainform.timeeditStart.displayFormat())
            self.endtime = dtend.toString(self.mainform.timeeditStop.displayFormat())
        except Exception as ex:
            print(traceback.format_exc())

    def ident_profile(self, df):
        df = df[df['in_work'] == True]
        if len(df) > 0:
            profile = list(df['profile_name'])[0]
            nomen = list(df['obj_id'])[0]
            count_pass = list(df['passages_num'])[0]
        else:
            profile = 'No identified'
            nomen = 'No identified'
            count_pass = 0
        return profile, nomen, count_pass

    def check_tek_pass(self,oldpass,dfmean):
        try:
            flag_updater = True
            sravlist = list(dfmean['dt_stop'][-1:])[0]
            if oldpass is None:
                flag_updater = True
            else:
                if sravlist <= oldpass:
                    flag_updater = False
            if flag_updater:
                oldpass = sravlist
            return flag_updater, oldpass
        except Exception as ex:
            print(traceback.format_exc())

    def pg_reader(self,count_proh, refnomen, pgsql_read_con, pg_query_read_passageway):
        dtstart = datetime.datetime.now().strftime('%Y-%m-%d 00:00:00')
        dtstop = datetime.datetime.now().strftime('%Y-%m-%d 23:59:59')
        dfvisual = pd.read_sql_query(pg_query_read_passageway.format(refnomen, dtstart, dtstop), pgsql_read_con)
        n = 1
        for i in range(len(dfvisual) - 2, len(dfvisual) // 2, -1):
            if dfvisual['num_proh'][i] < dfvisual['num_proh'][i + 1]:
                n += 1
            else:
                break
        dfmean = dfvisual[int(f'-{n}'):]
        return dfvisual, dfmean


    def run(self):
        mytekpass = None # Лист последних проходов

        # Статические входные данные
        list_mean = ['amperage_mean', 'amperage_max', 'power_mean', 'power_max', 'vibration_mean',
                     'vibration_max', 'interformation_pause', 'time_cycle', 'time_rez',
                     'disc_speed', 'disc_diameter', 'num_proh']
        ruparam = ['Средний Ток', 'Максимальный Ток', 'Средняя мощность', 'Максимаьлная мощность', 'Средняя вибрация', 'Максимальная вибрация',
                   'Паузы перед проходами', 'Время цикла', 'Время реза', 'Скорость вращения диска', 'Диаметр диска']
        res_list = ['res_amperage_mean', 'res_amperage_max', 'res_power_mean', 'res_power_max', 'res_vibration_mean',
                     'res_vibration_max', 'res_interformation_pause', 'res_time_cycle', 'res_time_rez',
                     'res_disc_speed', 'res_disc_diameter']
        pg_param_name = ['nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,14', 'nsi.spr_terminal_techpa,15',
                         'nsi.spr_terminal_techpa,16', 'nsi.spr_terminal_techpa,12', 'nsi.spr_terminal_techpa,13',
                         'nsi.spr_terminal_techpa,10', 'nsi.spr_terminal_techpa,17', 'nsi.spr_terminal_techpa,18',
                         'nsi.spr_terminal_techpa,19', 'nsi.spr_terminal_techpa,20']
        obj_param = ['nsi.spr_terminal_techpa,9', 'nsi.spr_terminal_techpa,14', 'nsi.spr_terminal_techpa,15',
                         'nsi.spr_terminal_techpa,16', 'nsi.spr_terminal_techpa,12', 'nsi.spr_terminal_techpa,13',
                         'nsi.spr_terminal_techpa,10', 'nsi.spr_terminal_techpa,17', 'nsi.spr_terminal_techpa,18',
                         'nsi.spr_terminal_techpa,19', 'nsi.spr_terminal_techpa,20']
        old_tekparam = self.mainform.choiseparam.currentIndex()   # Текущий параметр для обновления формы
        old_starttime = self.starttime
        old_endtime = self.endtime
        old_nomen = ''
        plt.rcParams['font.size'] = '25'
        nameSchema = 'equipment_downtime'
        nameTable = 'dwnt_mill'
        old_stanStatus = ''
        old_stanStatuskl = ''
        stanStatus = ''
        stanStatuskl = ''
        old_downtime = ''
        old_downtimeklet = ''
        teknomen = ''
        old_teknomen = ''
        nomenColor = ''
        downtime = ''
        downtimeklet = ''
        old_on_region = ''
        pgsql_work_connection = ''
        pgsql_ds_connection = ''
        pgsql_work_con = create_engine(pgsql_work_connection)
        pgsql_ds_con = create_engine(pgsql_ds_connection)
        pg_query_read_prokat = """ SELECT dt_begin, dt_end, id_equipment, id_posad, duration, description
                                FROM {}.{}
                                WHERE id_equipment = 501
                                ORDER BY dt desc
                                LIMIT 1"""
        pg_query_read_prokatklet = """ SELECT dt_begin, dt_end, id_equipment, id_posad, duration, description
                                FROM {}.{}
                                WHERE id_equipment = 502
                                ORDER BY dt desc
                                LIMIT 1"""
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
        pg_query_read_profiles = """SELECT *
                                    FROM aatp.acceptable_profiles
                                    WHERE ref_location = 'nsi.spr_terminal_techpa_location,52' """
        pg_query_read_valpar_all = """SELECT passage, q1,q2,q3,x1,x2, ref_parameter
                                    FROM aatp.acceptable_values 
                                    WHERE ref_profile = '{}'"""
        pg_query_read_on_region = "SELECT bool FROM aatp.st500_iba_online WHERE id = 5"

        while True:
            try:
                # Основной цикл обработки и отображения сигналов

                nomen, refnomen, count_pass = self.ident_profile(pd.read_sql_query(pg_query_read_profiles, pgsql_ds_con))

                self.mainform.dateLabel.setText(datetime.datetime.now().strftime('%d.%m.%Y'))
                self.mainform.timeLabel.setText(datetime.datetime.now().strftime('%H:%M:%S'))

                # Динамические входные данные

                tekparam = self.mainform.choiseparam.currentIndex()  # Текущий параметр
                equipment_downtime = pd.read_sql_query(pg_query_read_prokat.format(nameSchema, nameTable), pgsql_work_con)
                equipment_downtimeklet = pd.read_sql_query(pg_query_read_prokatklet.format(nameSchema,nameTable), pgsql_work_con)
                on_region = list(pd.read_sql(pg_query_read_on_region, pgsql_ds_con)['bool'])[0]
                if old_on_region != on_region:
                    statusColor1 = 'background-color: green; color: white'
                    statusColor2 = 'background-color: red; color: white'
                    self.updonregion.emit(statusColor1) if on_region else self.updonregion.emit(statusColor2)
                    old_on_region = on_region
                if len(equipment_downtime) > 0:
                    if equipment_downtime['dt_end'][0] is None:
                        stanStatus = 'Простой'
                        statusColor = 'background-color: red; color: white'
                    else:
                        stanStatus = 'В работе'
                        statusColor = 'background-color: green; color: white'
                    if list(equipment_downtime['duration'])[0] is not None:
                        downtime = datetime.datetime.strptime(str(datetime.timedelta(seconds=equipment_downtime['duration'][0].seconds)),
                                                                    '%H:%M:%S').strftime('%H:%M:%S')
                        if stanStatus != old_stanStatus or downtime != old_downtime:
                            self.updcolors.emit(stanStatus,statusColor, 2, downtime)
                if  len(equipment_downtimeklet) > 0:
                    if equipment_downtimeklet['dt_end'][0] is None:
                        stanStatuskl = 'Простой'
                        statusColorkl = 'background-color: red; color: white'
                    else:
                        stanStatuskl = 'В работе'
                        statusColorkl = 'background-color: green; color: white'
                    if list(equipment_downtimeklet['duration'])[0] is not None:
                        downtimeklet = datetime.datetime.strptime(str(datetime.timedelta(seconds=equipment_downtimeklet['duration'][0].seconds)),
                                                                    '%H:%M:%S').strftime('%H:%M:%S')
                        if stanStatuskl != old_stanStatuskl or downtimeklet != old_downtimeklet:
                            self.updcolors.emit(stanStatuskl,statusColorkl, 3, downtimeklet)
                if teknomen != old_teknomen and old_teknomen != '':
                    self.reset_plots.emit()
                old_teknomen = teknomen
                if nomen != 'No identified':
                    dfvisual, dfmean = self.pg_reader(count_pass,refnomen, pgsql_ds_con, pg_query_read_passageway)
                    if len(dfvisual) > count_pass:
                        self.tekdata = list(dfvisual['dt_start'][len(dfvisual)//2:len(dfvisual)//2+1].dt.strftime('%Y-%m-%d'))[0] #if len(dfmean) == count_pass else self.tekdata
                        flag_updater, mytekpass = self.check_tek_pass(oldpass=mytekpass,dfmean=dfmean)

                        if flag_updater or tekparam != old_tekparam or old_starttime != self.starttime or old_endtime != self.endtime or old_nomen != nomen:
                            #Передача данных в основной поток

                            teknomen = str(list(dfmean['nomen'][-1:])[0])
                            nomenColor = 'background-color: green; color: white'



                            valparam = pd.read_sql_query(pg_query_read_valpar.format(refnomen,pg_param_name[tekparam]), pgsql_ds_con)
                            valparam_all = pd.read_sql_query(pg_query_read_valpar_all.format(refnomen), pgsql_ds_con)

                            self.dataformain.emit(dfvisual,dfmean, self.starttime, self.endtime, self.tekdata, list_mean,
                                                  res_list, tekparam, count_pass, ruparam, valparam, valparam_all, obj_param)

                            # Метка значений параметров и времени выхода заготовки
                            self.mainform.label_timebar.setText(list(dfmean['dt_stop'][-1:].dt.strftime('%Y.%m.%d %H:%M:%S'))[0])

                            self.mainform.passagelabel.setText(str(count_pass))



                    else:
                        teknomen = 'Нет данных'
                        nomenColor = 'background-color: red; color: white'

                else:
                    teknomen = 'Профиль не идентифицирован'
                    nomenColor = 'background-color: red; color: white'

                if teknomen != old_teknomen:
                    self.updcolors.emit(teknomen, nomenColor, 1, None)
                old_stanStatus = stanStatus
                old_stanStatuskl = stanStatuskl
                old_downtime = downtime
                old_downtimeklet = downtimeklet
                old_nomen = nomen
                old_tekparam = tekparam
                old_starttime = self.starttime
                old_endtime = self.endtime
                time.sleep(0.1)
            except Exception as ex:
                print(traceback.format_exc())

class UI(QMainWindow):
    def __init__(self):
        super(UI, self).__init__()
        self.temp_path = ''

        # Load ui
        uic.loadUi("RezkaClientRTS.ui", self)

        # Define Widgets
        self.label_timebar = self.findChild(QLabel, 'label_24')
        self.label_timebar.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.label_37 = self.findChild(QLabel, 'label_37')
        self.label_37.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.labelplot1 = self.findChild(QLabel, 'label_6')
        self.labelplot2 = self.findChild(QLabel, 'label_7')
        self.labelplot3 = self.findChild(QLabel, 'label_5')
        self.labelplot5 = self.findChild(QLabel, 'label_12')
        self.labelplot6 = self.findChild(QLabel, 'label_29')
        self.labelplot4 = self.findChild(QLabel, 'label_15')
        self.labelpass1 = self.findChild(QLabel, 'label_23')
        self.labelpass2 = self.findChild(QLabel, 'label_25')
        self.labelpass3 = self.findChild(QLabel, 'label_26')
        self.labelpass4 = self.findChild(QLabel, 'label_27')
        self.labelpass5 = self.findChild(QLabel, 'label_28')
        self.labelhist = self.findChild(QLabel, 'label_13')
        self.label_9 = self.findChild(QLabel, 'label_9')
        self.label_9.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.labelSuccess = self.findChild(QLabel, 'label_10')
        self.labelSuccess.setStyleSheet("""border: 1px solid #adadad;
    background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                      stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.labelmem = self.findChild(QLabel, 'label_31')
        self.choiseparam = self.findChild(QComboBox, 'comboBox')
        self.label_view = self.findChild(QLabel, 'label_view')
        self.table_view = self.findChild(QTableWidget, 'tableView')
        self.labelstatus = self.findChild(QLabel, 'labelstatus')
        self.labelstatus.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.labeldowntime = self.findChild(QLabel, 'labeldowntime')
        self.labeldowntime.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.label_33 = self.findChild(QLabel, 'label_33')
        self.label_33.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")

        self.btnSaveMax = self.findChild(QPushButton, 'btnSaveMas')
        self.timeeditStart = self.findChild(QTimeEdit, 'timeEdit')
        self.timeeditStop = self.findChild(QTimeEdit, 'timeEdit_2')
        self.dateLabel = self.findChild(QLabel, 'dateLabel')
        self.timeLabel = self.findChild(QLabel, 'timeLabel')
        self.passagelabel = self.findChild(QLabel, 'label_14')
        self.passagelabel.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.label_11 = self.findChild(QLabel, 'label_11')
        self.label_11.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.labeldowntimeklet_text = self.findChild(QLabel, 'labeldowntimeklet_text')
        self.labeldowntimeklet_text.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.labeldowntimeklet_time = self.findChild(QLabel, 'labeldowntimeklet_time')
        self.labeldowntimeklet_time.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.label_35 = self.findChild(QLabel, 'label_35')
        self.label_35.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.label_3 = self.findChild(QLabel, 'label_3')
        self.label_3.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.label_30 = self.findChild(QLabel, 'label_30')
        self.label_30.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.label_32 = self.findChild(QLabel, 'label_32')
        self.label_32.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.label_31 = self.findChild(QLabel, 'label_31')
        self.label_31.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.label_36 = self.findChild(QLabel, 'label_36')
        self.label_36.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                          stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.label_onRegion = self.findChild(QLabel, 'label_onRegion')
        self.label_onRegion.setStyleSheet("""border: 1px solid #adadad;
        background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                      stop: 0 #ebebeb, stop: 1 #ebebeb);""")
        self.table_view2 = self.findChild(QTableWidget, 'tableView_2')
        self.mainlayout = self.findChild(QFrame, 'verticalFrame_5')
        self.mainlayout.resize(self.size().width(), self.size().height())
        self.init_temp()

        # Function
        self.label_logo = self.findChild(QLabel, 'label')
        self.label_logo.setPixmap(QPixmap('logo.png'))


        # Show App
        self.show()

        # Init new flow
        self.updater = Updater(mainform=self)
        self.updater.dataformain.connect(self.get_param)
        self.updater.updcolors.connect(self.update_colors)
        self.updater.reset_plots.connect(self.reset_plots)
        self.updater.updonregion.connect(self.update_onregion)
        self.updater.start()

    def init_temp(self):
        self.showMaximized()
        self.labels = [self.labelplot1, self.labelplot2, self.labelplot3, self.labelplot4, self.labelplot5, self.labelplot6]
        path1 = os.path.join(tempfile.gettempdir(), 'aatp')
        path2 = os.path.join(path1, 'rezka_clientrts')
        if not os.path.exists(path1):
            os.mkdir(path1)
        if not os.path.exists(path2):
            os.mkdir(path2)
        self.temp_path = path2

    def resizeEvent(self, QResizeEvent):
        self.mainlayout.resize(self.size().width()-20,self.size().height())

    def reset_plots(self):
        self.labelhist.setPixmap(QPixmap())
        self.labelplot1.setPixmap(QPixmap())
        self.labelplot2.setPixmap(QPixmap())
        self.labelplot3.setPixmap(QPixmap())
        self.labelplot4.setPixmap(QPixmap())
        self.labelplot5.setPixmap(QPixmap())
        self.labelplot6.setPixmap(QPixmap())

    def update_colors(self, text, style, number_label, downtime):
        print("Upd downtime")
        if number_label == 1:
            self.labelSuccess.setText(text)
            self.labelSuccess.setStyleSheet(style)
        elif number_label == 2:
            self.labelstatus.setText(text)
            self.labelstatus.setStyleSheet(style)
            if text == 'Простой':
                self.labeldowntime.setText(str(downtime))
            else:
                self.labeldowntime.setText('')
        elif number_label == 3:
            self.labeldowntimeklet_text.setText(text)
            self.labeldowntimeklet_text.setStyleSheet(style)
            if text == 'Простой':
                self.labeldowntimeklet_time.setText(str(downtime))
            else:
                self.labeldowntimeklet_time.setText('')

    def update_onregion(self, col):
        self.label_onRegion.setText('')
        self.label_onRegion.setStyleSheet(col)


    def update_hist(self,dfmean,list_mean,res_list,tekparam, ruparam):
        try:
            xax = list(dfmean['num_proh'])
            vall = [round(i, 2) for i in list(dfmean[list_mean[tekparam]])]
            resx = [str(xax[i]) + f' ({vall[i]})' for i in range(len(xax))]
            lines = plt.plot([1], [2], color='red', linewidth=8)
            lines += plt.plot([1], [2], color='yellow',linewidth=8)
            lines += plt.plot([1], [2], color='green',linewidth=8)
            plt.clf()
            plt.close("all")
            gc.collect()
            fig, ax = plt.subplots(1, 1, figsize=(18, 8))
            ax.bar(resx, dfmean[list_mean[tekparam]], color=dfmean[res_list[tekparam]])
            plt.title("Отклонение от нормы по резам")
            plt.xticks(resx)
            plt.xlabel('Резы (Значения)')
            plt.ylabel(ruparam[tekparam])
            plt.legend(lines, ['Сильное отклонение','Среднее отклонение', 'Норма'])

            plt.savefig(os.path.join(self.temp_path, 'imgres.jpg'))
            a = QPixmap(os.path.join(self.temp_path, 'imgres.jpg'))
            width = self.labelhist.contentsRect().width()
            height = self.labelhist.contentsRect().height()
            a = a.scaled(width, height,  Qt.IgnoreAspectRatio, Qt.SmoothTransformation)
            self.labelhist.setPixmap(a)
            plt.clf()
            plt.close("all")
            gc.collect()
        except Exception as ex:
            print(traceback.format_exc())

    def update_plots(self,dfvisual,dfmean, starttime, endtime, tekdata, list_mean,res_list,tekparam,count_pass, ruparam):
        try:
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
                plt.savefig(os.path.join(self.temp_path, f'imgpass_{j}.jpg'))

            # Обнавляем картинки
            a = [i for i in range(dfvisual.num_proh.max())]
            width = self.labelplot1.contentsRect().width()
            height = self.labelplot1.contentsRect().height()
            for i in range(dfvisual.num_proh.max()):
                a[i] = QPixmap(os.path.join(self.temp_path, f'imgpass_{i}.jpg'))
                a[i] = a[i].scaled(width, height, Qt.IgnoreAspectRatio, Qt.SmoothTransformation)

            for i, pix in enumerate(a):
                self.labels[i].setPixmap(pix)

            # self.labelplot1.setPixmap(a[0])
            # self.labelplot2.setPixmap(a[1])
            # self.labelplot3.setPixmap(a[2])
            # if count_pass == 5:
            #     self.labelplot4.setPixmap(a[3])
            #     self.labelplot5.setPixmap(a[4])
            plt.clf()
            plt.close("all")
            gc.collect()
        except Exception as ex:
            # print('hey ', listcolors[1])
            print(traceback.format_exc())

    def update_validparam(self, valparam, tekparam):
        description = ['Средний Ток', 'Максимальный Ток', 'Средняя мощность', 'Максимаьлная мощность', 'Средняя вибрация', 'Максимальная вибрация',
                   'Паузы перед проходами', 'Время цикла', 'Время реза', 'Скорость вращения диска', 'Диаметр диска']

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
        width = self.label_view.contentsRect().width()
        height = self.label_view.contentsRect().height()
        a = a.scaled(width, height, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        self.label_view.setPixmap(a)

        # Строим таблицу

        class_param = valparam
        class_param.columns = ['Номер реза', 'Норма низ', 'Медиана', 'Норма верх', 'Ср отклонение низ','Ср отклонение верх']
        class_param = class_param.drop(columns=['Номер реза', 'Медиана'])
        class_param['Норма низ'] = list(np.ceil(list(class_param['Норма низ'] * 1000)) / 1000)
        class_param['Норма верх'] = list(np.ceil(list(class_param['Норма верх'] * 1000)) / 1000)
        class_param['Ср отклонение низ'] = list(np.ceil(list(class_param['Ср отклонение низ'] * 1000)) / 1000)
        class_param['Ср отклонение верх'] = list(np.ceil(list(class_param['Ср отклонение верх'] * 1000)) / 1000)

        self.table_view.setColumnCount(0)
        self.table_view.setRowCount(0)
        self.table_view.setColumnCount(len(class_param.columns.values.tolist()))
        self.table_view.setHorizontalHeaderLabels(class_param.columns.values.tolist())

        for i, row in class_param.iterrows():
            self.table_view.setRowCount(self.table_view.rowCount() + 1)
            for j in range(self.table_view.columnCount()):
                self.table_view.setItem(i, j, QTableWidgetItem(str(row[j])))
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_view.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)

    def update_validtable2(self, dfmean, ruparam, list_mean, res_list, valparam_all, obj_param):

        dff = dfmean[list_mean[:-1]]
        dff.columns = ruparam
        dff = dff.reset_index()
        dff = dff.drop(columns='index')
        dff = dff.T
        dff = dff.reset_index()
        a = ['Параметры']
        a += [str(int(list(dff.columns)[i]) + 1) for i in range(1, len(list(dff.columns)))]
        dff.columns = a

        newdf = dfmean[res_list]
        newdf.columns = ruparam
        newdf = newdf.reset_index()
        newdf = newdf.drop(columns='index')
        newdf = newdf.T
        newdf = newdf.reset_index()
        a = ['Параметры']
        a += [str(int(list(newdf.columns)[i]) + 1) for i in range(1, len(list(newdf.columns)))]
        newdf.columns = a
        class_param = newdf

        # Обработка положения значения от первого и третьего квартиля

        ff = valparam_all
        for i in range(len(ff)):
            ff['ref_parameter'][i] = ruparam[obj_param.index(ff['ref_parameter'][i])]
        ff = ff.drop(columns=['q2', 'x1', 'x2'])

        temp1 = dfmean[res_list]
        temp1.columns = ruparam
        temp1 = temp1.reset_index()
        valdf = dfmean[list_mean[:-1]]
        valdf.columns = ruparam
        valdf = valdf.reset_index()
        valdf = valdf.drop(columns=['index'])
        for col in list(valdf.columns):
            for i in range(len(valdf)):
                val1 = list(ff[(ff['ref_parameter'] == col) & (ff['passage'] == i + 1)]['q1'])[0]
                val2 = list(ff[(ff['ref_parameter'] == col) & (ff['passage'] == i + 1)]['q3'])[0]
                if valdf[col][i] > val2:
                    if temp1[col][i] != 'green':
                        valdf[col][i] = 2
                    else:
                        valdf[col][i] = 0
                elif valdf[col][i] < val1:
                    if temp1[col][i] != 'green':
                        valdf[col][i] = 1
                    else:
                        valdf[col][i] = 0
                else:
                    valdf[col][i] = 0
        valdf = valdf.T
        valdf = valdf.reset_index()
        a = ['Параметры']
        a += [str(int(list(valdf.columns)[i]) + 1) for i in range(1, len(list(valdf.columns)))]
        valdf.columns = a
        trend = {0: '', 1: '&#9660', 2: '&#9650'}
        self.table_view2.setColumnCount(0)
        self.table_view2.setRowCount(0)
        self.table_view2.setColumnCount(len(class_param.columns.values.tolist()))
        self.table_view2.setHorizontalHeaderLabels(class_param.columns.values.tolist())
        for i, row in class_param.iterrows():
            self.table_view2.setRowCount(self.table_view2.rowCount() + 1)
            for j in range(self.table_view2.columnCount()):
                if j == 0:
                    self.table_view2.setItem(i, j, QTableWidgetItem(str(row[j])))
                else:
                    self.table_view2.setItem(i, j, QTableWidgetItem(''))
                if j != 0:
                    self.table_view2.item(i, j).setBackground(QColor(row[j]))
        for i, row in dff.iterrows():
            for j in range(self.table_view2.columnCount()):
                if j != 0:
                    self.table_view2.item(i, j).setText(str(round(row[j], 1)))
        for i, row in valdf.iterrows():
            for j in range(self.table_view2.columnCount()):
                if j != 0:
                    self.table_view2.item(i, j).setText(self.table_view2.item(i, j).text() + ' ' + html.unescape(trend[int(row[j])]))
        self.table_view2.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_view2.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)

    def get_param(self, dfvisual,dfmean, starttime, endtime, tekdata, list_mean, res_list, tekparam, count_pass, ruparam, valparam, valparam_all, obj_param):
        # Рисуем графики в основном потоке
        try:
            self.update_hist(dfmean,list_mean,res_list,tekparam,ruparam)
            self.update_plots(dfvisual,dfmean, starttime, endtime, tekdata, list_mean, res_list, tekparam, count_pass, ruparam)
            self.update_validparam(valparam, tekparam)
            self.update_validtable2(dfmean, ruparam, list_mean, res_list, valparam_all, obj_param)
            print('Successfull update graphics')
        except Exception as ex:
            print(traceback.format_exc())


# Init App
# stylesheet = """
# QLabel {
#     border: 1px solid #adadad;
#     background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
#                                       stop: 0 #ebebeb, stop: 1 #ebebeb);
# }
#        """

app = QApplication(sys.argv)
# app.setStyleSheet(stylesheet)
UIWindow = UI()
sys.exit(app.exec_())