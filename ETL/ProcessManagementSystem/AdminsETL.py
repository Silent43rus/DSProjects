import datetime
import mmap
import sys
import time
import warnings
import psutil
warnings.filterwarnings('ignore')
from PyQt5.Qt import *
from PyQt5 import uic
import os
import pandas as pd
import traceback
from sqlalchemy import create_engine
import shutil
from subprocess import Popen, PIPE


class Updater(QThread):
    dataformain = pyqtSignal(object)
    dataformain_all = pyqtSignal(object)

    def __init__(self, mainform, parent=None):
        super(Updater, self).__init__()
        self.mainform = mainform


    def run(self):
        try:
            query_get_logs = "SELECT obj_id, log, ref_projects FROM etl.processes ORDER BY id"
            while True:
                pg_df = pd.read_sql(query_get_logs, self.mainform.connection)
                columns = list(pg_df['obj_id'])
                df = pd.DataFrame(None, index=range(1), columns=columns)
                for strr in columns:
                    df[strr] = list(pg_df[pg_df['obj_id'] == strr]['log'])[0]
                self.dataformain.emit(df)
                self.dataformain_all.emit(pg_df)
                time.sleep(1)
        except:
            print(traceback.format_exc())


class UpdateProcess(QMainWindow):
    updproc = pyqtSignal(object)
    def __init__(self, mainform, parent=None):

        "Форма обновления выбранного процесса передается экземляр главной формы"

        super(UpdateProcess, self).__init__()
        self.mainform = mainform

        # Load ui
        uic.loadUi("UpdateETL.ui", self)

        #Define Widgets
        self.mainlayout = self.findChild(QFrame, 'MainFrame')
        self.mainlayout.resize(self.size().width(), self.size().height())
        self.btn_upd_proc = self.findChild(QPushButton, 'btn_upd_proc')
        self.edit_desc = self.findChild(QPlainTextEdit, 'edit_desc')
        self.label_project = self.findChild(QLabel, 'label_project')
        self.edit_path_file = self.findChild(QLineEdit, 'edit_path_file')
        self.btn_choice_file = self.findChild(QPushButton, 'btn_choice_file')
        self.label_process = self.findChild(QLabel, 'label_process')

        # Function
        self.btn_upd_proc.clicked.connect(self.UpdateProcess)
        self.btn_choice_file.clicked.connect(self.__choice_file)

        # Start function
        self.label_project.setText(self.mainform.current_project['name'])
        self.label_process.setText(self.mainform.current_processes['name'])


    def resizeEvent(self, QResizeEvent):
        self.mainlayout.resize(self.size().width() - 10,self.size().height() - 10)


    def __choice_file(self):
        path = QFileDialog.getOpenFileName(self, "Open File", "/home", filter="Python (*.py)" )[0]
        self.edit_path_file.setText(path)
        if path != '':
            self.btn_upd_proc.setEnabled(True)


    def UpdateProcess(self):
        try:
            self.updproc.emit(self.edit_path_file.text())
            self.close()
            self.destroy()
        except:
            print(traceback.format_exc())


class AddNewElement(QMainWindow):

    addelem = pyqtSignal(object, object, object)
    def __init__(self, mainform, project=False, parent=None):

        "Форма добавления нового элемента, передается экземляр главной формы и флаг выбора функции(проект или нет)"

        super(AddNewElement, self).__init__()
        self.mainform = mainform
        self.project = project

        # Load ui
        uic.loadUi("EditETL.ui", self)

        #Define Widgets
        self.mainlayout = self.findChild(QFrame, 'MainFrame')
        self.mainlayout.resize(self.size().width(), self.size().height())
        self.btn_add_elem = self.findChild(QPushButton, 'btn_add_elem')
        self.edit_name_elem = self.findChild(QLineEdit, 'edit_name_elem')
        self.edit_desc = self.findChild(QPlainTextEdit, 'edit_desc')
        self.label_project = self.findChild(QLabel, 'label_project')
        self.edit_path_file = self.findChild(QLineEdit, 'edit_path_file')
        self.label_path = self.findChild(QLabel, 'label_path')
        self.label_project_desc = self.findChild(QLabel, 'label_project_desc')
        self.btn_choice_file = self.findChild(QPushButton, 'btn_choice_file')

        # Function
        self.btn_add_elem.clicked.connect(self.AddElement)
        self.btn_choice_file.clicked.connect(self.__choice_file)

        # Start function
        self.__startInit()
        self.label_project.setText(self.mainform.current_project['name']) if not self.project else None


    def __startInit(self):
        if self.project:
            self.edit_path_file.setEnabled(False)
            self.label_path.setEnabled(False)
            self.label_project.setEnabled(False)
            self.label_project_desc.setEnabled(False)
            self.btn_choice_file.setEnabled(False)


    def resizeEvent(self, QResizeEvent):
        self.mainlayout.resize(self.size().width() - 10,self.size().height() - 10)


    def __choice_file(self):
        path = QFileDialog.getOpenFileName(self, "Open File", "/home", filter="Python (*.py)" )[0]
        self.edit_path_file.setText(path)


    def AddElement(self):
        try:
            self.addelem.emit(self.edit_name_elem.text(), self.edit_desc.toPlainText(), self.edit_path_file.text())
            self.close()
            self.destroy()
        except:
            print(traceback.format_exc())


class UI(QMainWindow):

    def __init__(self):
        super(UI, self).__init__()
        self.working_directory = r''
        self.new_project = {'name' : '', 'description' : ''}
        self.new_process = {'name' : '', 'description' : '', 'path' : ''}
        self.current_project = {'name' : '', 'obj_id' : '', 'description' : ''}
        self.current_processes = {'name' : '', 'description' : '', 'log' : '', 'obj_id' : '', 'pid' : ''}
        self.current_del_project = {'name' : '', 'obj_id' : '', 'description' : ''}
        self.current_del_processes = {'name' : '', 'description' : '', 'log' : '', 'obj_id' : ''}
        self.df_projects = None
        self.df_processes = None
        self.df_del_projects = None
        self.df_del_processes = None
        self.row_index_project = None
        self.row_index_process = None
        self.flag_upd = True
        pgsql_connection = ''
        self.connection = create_engine(pgsql_connection)

        # Load ui
        uic.loadUi("AdminsETL.ui", self)

        # Define Widgets
        self.MainFrame = self.findChild(QFrame, 'MainFrame')
        self.MainFrame.resize(self.size().width()-20, self.size().height()-20)
        self.table_projects = self.findChild(QTableWidget, 'table_projects')
        self.table_processes = self.findChild(QTableWidget, 'table_processes')
        self.btn_add_project = self.findChild(QPushButton, 'btn_add_project')
        self.btn_add_process = self.findChild(QPushButton, 'btn_add_process')
        self.btn_del_project = self.findChild(QPushButton, 'btn_del_project')
        self.btn_del_process = self.findChild(QPushButton, 'btn_del_process')
        self.btn_accep_proc = self.findChild(QPushButton, 'btn_accep_proc')
        self.btn_accep_del_proc = self.findChild(QPushButton, 'btn_accep_del_proc')
        self.btn_accep_proj = self.findChild(QPushButton, 'btn_accep_proj')
        self.btn_accep_del_proj = self.findChild(QPushButton, 'btn_accep_del_proj')
        self.table_del_process = self.findChild(QTableWidget, 'table_del_process')
        self.table_del_project = self.findChild(QTableWidget, 'table_del_project')
        self.btn_process_off = self.findChild(QPushButton, 'btn_process_off')
        self.btn_process_on = self.findChild(QPushButton, 'btn_process_on')
        self.btn_process_manualOff = self.findChild(QPushButton, 'btn_process_manualOff')
        self.label_choice_project = self.findChild(QLabel, 'label_choice_project')
        self.label_choice_process = self.findChild(QLabel, 'label_choice_process')
        self.table_log_process = self.findChild(QTableWidget, 'table_log_process')
        self.table_log_project = self.findChild(QTableWidget, 'table_log_project')
        self.tabWidget = self.findChild(QTabWidget, 'tabWidget')
        self.btn_stop_upd = self.findChild(QPushButton, 'btn_stop_upd')
        self.btn_start_upd = self.findChild(QPushButton, 'btn_start_upd')
        self.btn_upd_process = self.findChild(QPushButton, 'btn_upd_process')
        self.label_desc_project = self.findChild(QLabel, 'label_desc_project')
        self.label_desc_project.setWordWrap(True)
        self.label_desc_process = self.findChild(QLabel, 'label_desc_process')
        self.label_desc_process.setWordWrap(True)


        # Function
        self.btn_add_project.clicked.connect(self.__addProject)
        self.btn_add_process.clicked.connect(self.__addProcess)
        self.btn_del_project.clicked.connect(self.__deleteProject)
        self.btn_del_process.clicked.connect(self.__deleteProcess)
        self.btn_accep_proj.clicked.connect(self.__restoreProject)
        self.btn_accep_proc.clicked.connect(self.__restoreProcess)
        self.btn_accep_del_proc.clicked.connect(self.__acceptDelProcess)
        self.btn_accep_del_proj.clicked.connect(self.__acceptDelProject)
        self.btn_process_on.clicked.connect(self.__processOn)
        self.btn_process_manualOff.clicked.connect(self.__manualProcessOff)
        self.btn_process_off.clicked.connect(self.__processOff)
        self.btn_start_upd.clicked.connect(self.__startUpd)
        self.btn_stop_upd.clicked.connect(self.__stopUpd)
        self.btn_upd_process.clicked.connect(self.__updateFile)

        # Start function
        self.mainUpdProject()
        self.mainUpdProcess()
        self.__getDelProjects()
        self.__getDelProcess()

        # Show App
        self.show()

        # Init new flow
        self.updater = Updater(mainform=self)
        self.updater.dataformain.connect(self.get_param)
        self.updater.dataformain_all.connect(self.get_param_all)
        self.updater.start()


    # Событийные обработчики
    def resizeEvent(self, QResizeEvent):
        self.MainFrame.resize(self.size().width()-20,self.size().height())

    def eventFilter(self, source, event):

        "Обработчик события клика по таблицам"

        try:
            if event.type() == QEvent.MouseButtonRelease:
                if self.table_projects.selectedIndexes() != []:
                    self.row_index_project = self.table_projects.currentRow()
                    self.current_project['name'] = list(self.df_projects['name'])[self.row_index_project]
                    self.current_project['obj_id'] = list(self.df_projects['obj_id'])[self.row_index_project]
                    self.current_project['description'] = list(self.df_projects['description'])[self.row_index_project]
                    if hasattr(self, 'formAddProcess'):
                        self.formAddProcess.label_project.setText(self.current_project['name'])
                    if hasattr(self, 'formUpdateProcess'):
                        self.formUpdateProcess.label_project.setText(self.current_project['name'])
                    self.mainUpdProcess()
                    self.table_projects.clearSelection()
                    self.label_choice_project.setText(self.current_project['name'])
                    self.__updateDescriptionProject()

                if self.table_processes.selectedIndexes() != []:
                    self.row_index_process = self.table_processes.currentRow()
                    self.current_processes['name'] = list(self.df_processes['name'])[self.row_index_process]
                    self.current_processes['obj_id'] = list(self.df_processes['obj_id'])[self.row_index_process]
                    self.current_processes['description'] = list(self.df_processes['description'])[self.row_index_process]
                    self.current_processes['log'] = list(self.df_processes['log'])[self.row_index_process]
                    self.current_processes['pid'] = list(self.df_processes['pid'])[self.row_index_process]
                    self.table_processes.clearSelection()
                    if hasattr(self, 'formUpdateProcess'):
                        self.formUpdateProcess.label_process.setText(self.current_processes['name'])
                    if self.__getColors(list(self.df_processes['ref_status'])[self.row_index_process:self.row_index_process + 1])[1][0] == 0:
                        self.btn_process_off.setEnabled(False)
                        self.btn_process_on.setEnabled(True)
                        self.btn_upd_process.setEnabled(True)
                        self.btn_del_process.setEnabled(True)
                    else:
                        self.btn_process_off.setEnabled(True)
                        self.btn_process_on.setEnabled(False)
                        self.btn_upd_process.setEnabled(False)
                        self.btn_del_process.setEnabled(False)
                    self.label_choice_process.setText(self.current_processes['name'])
                    self.__updateDescriptionProcess()

                if self.table_del_project.selectedIndexes() != []:
                    self.row_del_index_project = self.table_del_project.currentRow()
                    self.current_del_project['name'] = list(self.df_del_projects['name'])[self.row_del_index_project]
                    self.current_del_project['obj_id'] = list(self.df_del_projects['obj_id'])[self.row_del_index_project]
                    self.current_del_project['description'] = list(self.df_del_projects['description'])[self.row_del_index_project]
                    self.table_del_project.clearSelection()

                if self.table_del_process.selectedIndexes() != []:
                    self.row_del_index_process = self.table_del_process.currentRow()
                    self.current_del_processes['name'] = list(self.df_del_processes['name'])[self.row_del_index_process]
                    self.current_del_processes['obj_id'] = list(self.df_del_processes['obj_id'])[self.row_del_index_process]
                    self.current_del_processes['description'] = list(self.df_del_processes['description'])[self.row_del_index_process]
                    self.current_del_processes['log'] = list(self.df_del_processes['log'])[self.row_del_index_process]
                    self.table_del_process.clearSelection()
            return QObject.event(source, event)
        except:
            print(traceback.format_exc())


    # Функции mainwindow
    def mainUpdProject(self):

        "Функция обнавления основной таблицы проектов"

        self.__updateProjectList()
        self.updateMainTable(self.df_projects[['name', 'process_count', 'process_count_work']],
                             ['Имя проекта', 'Количество процессов', 'Количество процессов в работе'],
                             self.__getColors(list(self.df_projects['ref_status']))[0], self.table_projects)
        if len(self.df_projects) == 0:
            self.btn_del_project.setEnabled(False)
            self.btn_add_process.setEnabled(False)
        else:
            self.btn_del_project.setEnabled(True)
            self.btn_add_process.setEnabled(True)
        self.label_choice_project.setText(self.current_project['name'])


    def mainUpdProcess(self):

        "Функция обнавления основной таблицы процессов"

        self.__updateProcessesList()

        self.updateMainTable(self.df_processes[['name']],
                                 ['Имя процесса'],
                                 self.__getColors(list(self.df_processes['ref_status']))[0], self.table_processes)
        if len(self.df_processes) == 0:
            self.btn_del_process.setEnabled(False)
        else:
            self.btn_del_process.setEnabled(True)
        self.label_choice_process.setText(self.current_processes['name'])


    def updateMainTable(self, df, colnames, colors, table):

        "Функция обналвения таблиц с проектами и процессами"

        try:
            class_param = df
            table.setColumnCount(0)
            table.setRowCount(0)
            table.setColumnCount(len(colnames) + 1)
            table.setHorizontalHeaderLabels(colnames + ['Статус'])
            for i, row in class_param.iterrows():
                table.setRowCount(table.rowCount() + 1)
                for j in range(table.columnCount()):
                    if j == table.columnCount() - 1:
                        table.setItem(i, j, QTableWidgetItem(str('')))
                    else:
                        table.setItem(i, j, QTableWidgetItem(str(row[j])))

            for i, row in enumerate(colors):
                j = table.columnCount() - 1
                table.item(i, j).setBackground(QColor(row))
            table.viewport().installEventFilter(self)
            table.horizontalHeader().setSectionResizeMode(len(colnames), QHeaderView.Stretch)
        except:
            print(traceback.format_exc())


    def __updateProjectList(self):

        "Функция обнавления текущего списка проектов"

        try:
            query_select_projects = "SELECT obj_id, description, name, process_count, process_count_work, ref_status FROM etl.projects WHERE is_del = False ORDER BY id"

            df_projects = pd.read_sql(query_select_projects, self.connection)
            if len(df_projects) > 0:
                self.current_project['name'] = list(df_projects['name'])[0]
                self.current_project['obj_id'] = list(df_projects['obj_id'])[0]
                self.current_project['description'] = list(df_projects['description'])[0]
            self.df_projects = df_projects
        except:
            print(traceback.format_exc())


    def __updateProcessesList(self):

        "Функция обновления текущего списка процессов по выбранновму проекту"

        try:
            query_select_processes = "SELECT obj_id, description, name, ref_status, log, pid FROM etl.processes WHERE ref_projects = '{}' and is_del = False ORDER BY id"

            df_processes = pd.read_sql(query_select_processes.format(self.current_project['obj_id']), self.connection)
            if len(df_processes) > 0:
                self.current_processes['name'] = list(df_processes['name'])[0]
                self.current_processes['description'] = list(df_processes['description'])[0]
                self.current_processes['log'] = list(df_processes['log'])[0]
                self.current_processes['obj_id'] = list(df_processes['obj_id'])[0]
                self.current_processes['pid'] = list(df_processes['pid'])[0]
            self.df_processes = df_processes
        except:
            print(traceback.format_exc())

    def __updateDescriptionProject(self):
        self.label_desc_project.setText(self.current_project['description'])

    def __updateDescriptionProcess(self):
        self.label_desc_process.setText(self.current_processes['description'])


    # Функции учета элементов
    def __getColors(self, status_list):

        "Вспомогательная функция получения цветов по статусам с obj_id"

        query_select_color_st = "SELECT status, color FROM etl.spr_status WHERE obj_id = '{}' ORDER BY id"
        colors = []
        status = []
        for strr in status_list:
            colors.append(list(pd.read_sql(query_select_color_st.format(strr), self.connection)['color'])[0])
            status.append(list(pd.read_sql(query_select_color_st.format(strr), self.connection)['status'])[0])
        return colors, status


    def __createNewProject(self):

        "Функция создания нового проекта"

        try:
            query_insert_project = """
                                    INSERT INTO etl.projects (description, name, process_count, process_count_work, ref_status) 
                                                              VALUES (%s, %s, %s, %s, %s)
                                    """
            path = os.path.join(self.working_directory, self.new_project['name'])
            if not os.path.exists(path):
                os.mkdir(path)
                os.mkdir(os.path.join(path, 'log'))
                os.mkdir(os.path.join(path, 'tasks'))
            else:
                # Такой проект уже есть
                return 0
            row = [self.new_project['description'], self.new_project['name'], 0, 0, 'etl.spr_status,1']
            self.connection.execute(query_insert_project, row)
            return 1
        except:
            print(traceback.format_exc())


    def __deleteProject(self):

        "Функция помечения на удаление выбранного проекта"

        query_upd_project = f"""
                                UPDATE etl.projects SET is_del = True
                                                    WHERE obj_id = '{self.current_project['obj_id']}'
                                """
        query_upd_process = f"""
                            UPDATE etl.processes SET is_del = True 
                                WHERE ref_projects = '{self.current_project['obj_id']}'
                            """
        self.connection.execute(query_upd_project)
        self.connection.execute(query_upd_process)
        self.mainUpdProject()
        self.mainUpdProcess()
        self.__getDelProjects()
        self.__getDelProcess()


    def __createNewProcess(self):

        "Функция создания нового процесса"

        try:
            query_insert_process = """
                                    INSERT INTO etl.processes (description, name, file, ref_status, ref_projects, log) 
                                                               VALUES (%s, %s, %s, %s, %s, %s)
                                    """
            path = os.path.join(self.working_directory, self.current_project['name'])
            path = os.path.join(path, 'tasks')
            path = os.path.join(path, self.new_process['name'] + '.py')
            if not os.path.exists(path):
                with open(self.new_process['path']) as file:
                    m = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
                    ba = bytearray(m)
                with open(path, 'wb') as f:
                    f.write(ba)
            else:
                return 0
            row = [self.new_process['description'], self.new_process['name'], ba, 'etl.spr_status,1', self.current_project['obj_id'], '']
            self.connection.execute(query_insert_process, row)
            return 1
        except:
            print(traceback.format_exc())


    def __deleteProcess(self):

        "Функция помечения на удаление выбранного проекта"

        query_delete_process = f"""
                                UPDATE etl.processes SET is_del = True 
                                WHERE ref_projects = '{self.current_project['obj_id']}' and obj_id = '{self.current_processes['obj_id']}'
                                """
        self.connection.execute(query_delete_process)
        self.mainUpdProcess()
        self.__getDelProjects()
        self.__getDelProcess()


    def __addProject(self):

        "Функция открытия новой формы для добавления нового проекта"

        try:
            self.formAddProject = AddNewElement(mainform=self, project=True)
            self.formAddProject.addelem.connect(self.addProject)
            self.formAddProject.show()
        except:
            print(traceback.format_exc())


    def __addProcess(self):

        "Функция открытия новой формы для добавления нового процесса"

        try:
            self.formAddProcess = AddNewElement(mainform=self)
            self.formAddProcess.addelem.connect(self.addProcess)
            self.formAddProcess.show()
        except:
            print(traceback.format_exc())


    def addProject(self, name, desc, path):

        "Функция непосредственного создания проекта"

        try:
            self.new_project['name'] = name
            self.new_project['description'] = desc
            res = self.__createNewProject()
            if res == 0:
                print('Проект уже создан')
                return 0
            self.mainUpdProject()
        except:
            print(traceback.format_exc())


    def addProcess(self, name, desc, path):

        "Функция непосредственного создания процесса"

        try:
            self.new_process['name'] = name
            self.new_process['description'] = desc
            self.new_process['path'] = path
            res = self.__createNewProcess()
            if res == 0:
                print('Процесс уже создан')
                return 0
            self.mainUpdProcess()
        except:
            print(traceback.format_exc())


    # Функции администрирования элементов
    def __restoreProject(self):

        "Функция востановления проекта"

        query_upd_project = f"UPDATE etl.projects SET is_del = False WHERE obj_id = '{self.current_del_project['obj_id']}'"
        self.connection.execute(query_upd_project)
        self.__getDelProjects()
        self.mainUpdProject()
        self.mainUpdProcess()


    def __restoreProcess(self):

        "Функция востановления процесса"

        query_upd_process = f"UPDATE etl.processes SET is_del = False WHERE obj_id = '{self.current_del_processes['obj_id']}'"
        self.connection.execute(query_upd_process)
        self.__getDelProcess()
        self.mainUpdProject()
        self.mainUpdProcess()


    def __getDelProjects(self):

        "Функция получения списка удаленных проектов"

        query_get_del_project = "SELECT obj_id, description, name, process_count, process_count_work, ref_status FROM etl.projects WHERE is_del = True ORDER BY id"

        df_del_projects = pd.read_sql(query_get_del_project, self.connection)
        self.updateMainTable(df_del_projects[['name', 'process_count']],
                             ['Имя проекта', 'Всего процессов'],
                             self.__getColors(list(df_del_projects['ref_status']))[0], self.table_del_project)
        self.df_del_projects = df_del_projects
        if len(self.df_del_projects) == 0:
            self.btn_accep_del_proj.setEnabled(False)
            self.btn_accep_proj.setEnabled(False)
        else:
            self.btn_accep_del_proj.setEnabled(True)
            self.btn_accep_proj.setEnabled(True)


    def __getDelProcess(self):

        "Функция получения списка удаленных процессов"

        query_get_del_project = """
                                SELECT  df.obj_id, df.description, df.name, df.ref_status, df.log, df.ref_projects, df1.name project
                                    FROM 
                                    (SELECT obj_id, description, name, ref_status, log, ref_projects 
                                    FROM etl.processes 
                                    WHERE is_del = True) df
                                    LEFT JOIN etl.projects df1 on df1.obj_id = df.ref_projects
                                    ORDER BY id
                                """
        df_del_process = pd.read_sql(query_get_del_project, self.connection)
        self.updateMainTable(df_del_process[['name', 'project']],
                             ['Имя процесса', 'Имя проекта'],
                             self.__getColors(list(df_del_process['ref_status']))[0], self.table_del_process)
        self.df_del_processes = df_del_process
        if len(self.df_del_processes) == 0:
            self.btn_accep_del_proc.setEnabled(False)
            self.btn_accep_proc.setEnabled(False)
        else:
            self.btn_accep_del_proc.setEnabled(True)
            self.btn_accep_proc.setEnabled(True)


    def __acceptDelProject(self):

        "Функция подтверждения удаления всего проекта"

        query_delete_project = f"DELETE FROM etl.projects WHERE obj_id = '{self.current_del_project['obj_id']}'"
        query_delete_processes = f"DELETE FROM etl.processes WHERE ref_projects = '{self.current_del_project['obj_id']}'"

        path = os.path.join(self.working_directory, self.current_del_project['name'])
        if os.path.exists(path):
            shutil.rmtree(path)
            self.connection.execute(query_delete_project)
            self.connection.execute(query_delete_processes)
        else:
            print('Проекта не существует', path)
            return 0
        self.__getDelProcess()
        self.__getDelProjects()


    def __acceptDelProcess(self):

        "Функция подтверждения удаления выбранного процесса"

        query_delete_process = f"DELETE FROM etl.processes WHERE obj_id = '{self.current_del_processes['obj_id']}'"
        path = os.path.join(self.working_directory, list(self.df_del_processes['project'])[self.row_del_index_process])
        path = os.path.join(path, 'tasks')
        path = os.path.join(path, self.current_del_processes['name'] + '.py')
        if os.path.exists(path):
            os.remove(path)
            self.connection.execute(query_delete_process)
        else:
            print('Файла не существует', path)
            return 0
        self.__getDelProcess()


    # Функции управления процессами и проектами
    def __processOn(self):
        try:
            query_upd_process = f"UPDATE etl.processes SET (ref_status, pid) = (%s, %s) WHERE obj_id = '{self.current_processes['obj_id']}'"

            path = os.path.join(self.working_directory, self.current_project['name'])
            path = os.path.join(path, 'tasks')
            path = os.path.join(path, self.current_processes['name'] + '.py')
            print(path)
            p = Popen([sys.executable, path], stdout=PIPE, stderr=PIPE, stdin=PIPE)
            self.connection.execute(query_upd_process, ['etl.spr_status,2', p.pid])
            p.stdin.flush()
            self.mainUpdProcess()

        except:
            print(traceback.format_exc())


    def __manualProcessOff(self):
        try:
            query_manualUpd_process = f"UPDATE etl.processes SET (ref_status, log, pid) = (%s, %s, %s) WHERE obj_ID = '{self.current_processes['obj_id']}'"
            print(f"Process {self.current_processes['name']} was succesfully manual killed by pid = {self.current_processes['pid']}")
            self.connection.execute(query_manualUpd_process, ['etl.spr_status,1', '', 0])
            self.mainUpdProcess()
        except:
            print(traceback.format_exc())


    def __processOff(self):
        try:
            query_upd_process = f"UPDATE etl.processes SET (ref_status, log, pid) = (%s, %s, %s) WHERE obj_id = '{self.current_processes['obj_id']}'"

            if self.current_processes['pid'] != '':
                for proc in psutil.process_iter():
                    if proc.pid == self.current_processes['pid']:
                        print(f"Process {self.current_processes['name']} was succesfully killed by pid = {self.current_processes['pid']}")
                        self.connection.execute(query_upd_process, ['etl.spr_status,1', '', 0])
                        self.mainUpdProcess()
                        proc.terminate()
            else:
                print(f"Process {self.current_processes['name']} no have pid")
        except:
            print(traceback.format_exc())


    def __updateFile(self):
        "Функция открытия новой формы для добавления нового проекта"

        try:
            self.formUpdateProcess = UpdateProcess(mainform=self)
            self.formUpdateProcess.updproc.connect(self.updateFile)
            self.formUpdateProcess.show()
        except:
            print(traceback.format_exc())


    def updateFile(self, file_path):
        "Функция создания нового процесса"

        try:
            query_update_process = f"UPDATE etl.processes SET dt_update = (%s) WHERE obj_id = '{self.current_processes['obj_id']}'"
            path = os.path.join(self.working_directory, self.current_project['name'])
            path = os.path.join(path, 'tasks')
            path = os.path.join(path, self.current_processes['name'] + '.py')
            if os.path.exists(path):
                with open(file_path) as file:
                    m = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
                    ba = bytearray(m)
                with open(path, 'wb') as f:
                    f.write(ba)
                self.connection.execute(query_update_process, datetime.datetime.now())
            else:
                return 0

            return 1
        except:
            print(traceback.format_exc())


    # Просмотр логов
    def __startUpd(self):
        self.flag_upd = True
        self.btn_start_upd.setEnabled(False)
        self.btn_stop_upd.setEnabled(True)

    def __stopUpd(self):
        self.flag_upd = False
        self.btn_start_upd.setEnabled(True)
        self.btn_stop_upd.setEnabled(False)

    def get_param_all(self, df):

        "Функция обновления таблицы с логамми проекта"

        try:
            if self.tabWidget.currentIndex() == 1 and self.flag_upd:
                tek_proj = self.current_project['obj_id']
                b = df[df['ref_projects'] == tek_proj]
                b = b.sort_values(by='obj_id')
                b = b.reset_index()
                b = b.drop(columns=['index'])
                res = pd.DataFrame(None, index=range(len(b)), columns=['name', 'time', 'log'])
                name = []
                for i in range(len(b)):
                    name.append(list(self.df_processes[self.df_processes['obj_id'] == b['obj_id'][i]]['name'])[0])
                res['name'] = name
                for i in range(len(b)):
                    res['time'][i] = b['log'][i].split(';;;')[-1].split('-')[0]
                    res['log'][i] = b['log'][i].split(';;;')[-1].split('-')[-1]
                class_param = res
                self.table_log_project.setColumnCount(0)
                self.table_log_project.setRowCount(0)
                self.table_log_project.setColumnCount(len(class_param.columns))
                self.table_log_project.setHorizontalHeaderLabels(['Имя процесса', 'Время', 'Ответ'])
                for i, row in class_param.iterrows():
                    self.table_log_project.setRowCount(self.table_log_project.rowCount() + 1)
                    for j in range(self.table_log_project.columnCount()):
                        self.table_log_project.setItem(i, j, QTableWidgetItem(str(row[j])))
                self.table_log_project.viewport().installEventFilter(self)
                self.table_log_project.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        except:
            print(traceback.format_exc())


    def get_param(self, df):

        "Функция обналвения таблицы с логами процесса"

        try:
            if self.tabWidget.currentIndex() == 1 and self.flag_upd:
                proc = self.current_processes['obj_id']
                log = list(df[proc])[0]
                df1 = pd.DataFrame()
                if log != '':
                    df1['time'] = [i.split('-')[0] for i in log.split(';;;')]
                    df1['answer'] = [i.split('-')[1] for i in log.split(';;;')]
                    df1 = df1.sort_index(ascending=False)
                    df1 = df1.reset_index()
                    df1 = df1.drop(columns=['index'])
                    class_param = df1
                    self.table_log_process.setColumnCount(0)
                    self.table_log_process.setRowCount(0)
                    self.table_log_process.setColumnCount(len(class_param.columns))
                    self.table_log_process.setHorizontalHeaderLabels(['Время', 'Ответ'])
                    for i, row in class_param.iterrows():
                        self.table_log_process.setRowCount(self.table_log_process.rowCount() + 1)
                        for j in range(self.table_log_process.columnCount()):
                            self.table_log_process.setItem(i, j, QTableWidgetItem(str(row[j])))
                    self.table_log_process.viewport().installEventFilter(self)
                    self.table_log_process.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
                else:
                    self.table_log_process.clear()
        except:
            print(traceback.format_exc())


app = QApplication(sys.argv)
UIWindow = UI()
sys.exit(app.exec_())
