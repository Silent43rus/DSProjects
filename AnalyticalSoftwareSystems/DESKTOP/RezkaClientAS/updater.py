import os
import datetime
import sys
import traceback
import subprocess
import importlib.util


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    from PyQt5.Qt import *
    from sqlalchemy import create_engine
    import pandas as pd
except ImportError:
    install('PyQt5')
    install('pandas')
    install('sqlalchemy')
    install('psycopg2')
    from PyQt5.Qt import *
    from sqlalchemy import create_engine
    import pandas as pd

class CustomDialog(QDialog):
    def __init__(self):
        try:
            super(CustomDialog, self).__init__()

            self.setWindowTitle('Доступны новые обновления')
            self.setGeometry(500,500,400,200)
            QBtn = QDialogButtonBox.Yes | QDialogButtonBox.No

            self.buttonBox = QDialogButtonBox(QBtn)
            self.buttonBox.accepted.connect(self.accept)
            self.buttonBox.rejected.connect(self.reject)

            self.layout = QVBoxLayout()
            self.message = QLabel("Обновить до последней версии?")
            self.edit = QPlainTextEdit()
            self.edit.setReadOnly(True)
            self.layout.addWidget(self.edit)
            self.layout.addWidget(self.message)
            self.layout.addWidget(self.buttonBox)
            self.setLayout(self.layout)
        except:
            print(traceback.format_exc())

app = QApplication(sys.argv)
dig = CustomDialog()

def file_loader(path, file):
    """Эта функция обновляет старые файлы"""
    try:
        btn = dig.exec()
        if btn == 1:
            for i in range(len(path)):
                with open(path[i], 'wb') as f:
                    f.write(file[i])
                print('File', path[i], 'is loaded', sep=' ')
        elif btn == 0:
            return None
    except:
        print(traceback.format_exc())

def file_updater():
    """Эта функция проверяет дату обнавления текущих файлов с серверными файлами
        и возвращает флаг необходимости обновления вместе с самими файлами если они есть
    """
    try:
        pgsql_connection = ''
        pgsql_work_con = create_engine(pgsql_connection)
        pg_query_select_allfiles = """SELECT * FROM aatp.update_aatp WHERE project_name = '{}'"""
        project_name = "DS4"
        df = pd.read_sql(pg_query_select_allfiles.format(project_name), pgsql_work_con)
        df = df.reset_index()
        df = df.drop(columns=['index'])
        flag_updater = False
        path_list = []
        file_list = []
        for i in range(len(df)):
            filename = list(df['file_name'])[i]
            if not os.path.exists(os.path.join(os.getcwd(), filename)):
                dig.edit.appendPlainText('Файл ' + filename)
                flag_updater = True
                path_list.append(os.path.join(os.getcwd(), filename))
                file_list.append(list(df['file'])[i])
                continue
            last_update_file = datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(os.getcwd(),filename )))
            if last_update_file < list(df['dt_update'])[i]:
                dig.edit.appendPlainText('Файл ' + filename)
                flag_updater = True
                path_list.append(os.path.join(os.getcwd(), filename))
                file_list.append(list(df['file'])[i])
        return flag_updater, path_list, file_list
    except:
        print(traceback.format_exc())

try:
    upd, path_list, file_list = file_updater()
    if upd:
        file_loader(path_list,file_list)
    flag = True
    for strr in path_list:
        if 'updater.py' in strr:
            flag = False
    if flag:
        list_pack = ["PyQt5", "pandas", "matplotlib", "numpy", "psutil", "sqlalchemy", "psycopg2", "openpyxl"]
        os.system("python -m pip install --upgrade pip")
        for str in list_pack:
            spam_spc = importlib.util.find_spec(str)
            if not (spam_spc is not None):
                install(str)
        os.system("python RezkaClientAS.py")
except:
    print(traceback.format_exc())