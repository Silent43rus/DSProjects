# Конструктор ETL скриптов между IBA и Postgress

import datetime
import traceback
from sqlalchemy import create_engine
import pandas as pd


class Construct():
    def __init__(self):
        """
        Конструктор ETL скриптов
        При иннициализации обязательные атрибуты:
        name: str Наименование скрипта
        projects: str Имя проекта
        path: str Путь к файлу
        """

        pgsql_connection = ''
        self.connection = create_engine(pgsql_connection)
        self.query_read_log = """
                                SELECT df1.log
                                FROM etl.processes df1
                                LEFT JOIN etl.projects df2 on df1.ref_projects = df2.obj_id
                                WHERE df1."name" = '{}' and df2."name" = '{}'
                         """
        self.query_upd_log = """
                              UPDATE etl.processes SET log = (%s) 
                                    WHERE name = '{}' and ref_projects = 
                                    (
                                    SELECT obj_id 
                                    FROM etl.projects
                                    WHERE name = '{}')
                              """
        self.query_upd_status = """
                                UPDATE etl.processes SET ref_status = (%s)
                                WHERE name = '{}' and ref_projects = 
                                (
                                SELECT obj_id
                                FROM etl.projects
                                WHERE name = '{}')
                                """
        self.len_buffer = 50
        self.ref_status = ['etl.spr_status,1', 'etl.spr_status,2', 'etl.spr_status,3', 'etl.spr_status,4']

    # Записиь логов в pg
    def logWritter(self, file, file_dir, status):
        try:
            dt = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            log = list(pd.read_sql(self.query_read_log.format(file, file_dir), self.connection)['log'])[0]
            logs = log.split(';;;')
            if len(logs) > self.len_buffer:
                logs.pop(0)
            if logs[0] == '':
                logs.pop(0)
            logs.append(dt + '-' + status)
            log = ';;;'.join(logs)
            self.connection.execute(self.query_upd_log.format(file, file_dir), log)
            return 1
        except:
            return traceback.format_exc()

    # Запись статуса лога
    def statusUpdater(self, file, file_dir, status):
        try:
            self.connection.execute(self.query_upd_status.format(file, file_dir), self.ref_status[status])
            return 1
        except:
            return traceback.format_exc()