import random
import sys
import traceback
import pandas.errors
from PyQt5.Qt import *
from PyQt5.QtGui import QPixmap
from PyQt5 import uic
from CalcStrat import GeneratorTradingSystem
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import tempfile


class UI(QMainWindow):
    def __init__(self):
        super(UI, self).__init__()

        # Input Data
        self.capital = 100000
        self.comission = 0.00001
        self.profit_tax = 0.00001
        self.metrics = None
        self.df = None
        self.mer = None
        self.path_data = r'D:\ashikh\work\tickers\data'            # Путь к данным с котировками
        self.row_ticks = None
        self.row_ticks_res = None
        self.strategy = None
        self.tickers = []
        self.default_tickers = None

        # Load ui
        uic.loadUi("TTS.ui", self)

        # Define Widgets
        self.label_res_ts = self.findChild(QLabel, 'label_res_ts')
        self.table_tickers_all = self.findChild(QTableWidget, 'table_tickers_all')
        self.mainlayout = self.findChild(QFrame, 'horizontalFrame')
        self.table_ts = self.findChild(QTableWidget, 'table_ts')
        self.table_tickers_res = self.findChild(QTableWidget, 'table_tickers_res')
        self.table_trades = self.findChild(QTableWidget, 'table_trades')
        self.table_metricks = self.findChild(QTableWidget, 'table_metricks')
        self.btn_add_ticker = self.findChild(QPushButton, 'btn_add_ticker')
        self.btn_add_ticker_2 = self.findChild(QPushButton, 'btn_add_ticker_2')
        self.btn_choice_ts = self.findChild(QPushButton, 'btn_choice_ts')
        self.btn_test = self.findChild(QPushButton, 'btn_test')
        self.btn_choice_ticker = self.findChild(QPushButton, 'btn_choice_ticker')
        self.btn_reset_data = self.findChild(QPushButton, 'btn_reset_data')
        self.label_res_ts = self.findChild(QLabel, 'label_res_ts')
        self.label_plot_trades = self.findChild(QLabel, 'label_plot_trades')
        self.label_plot_eq = self.findChild(QLabel, 'label_plot_eq')
        self.mainlayout.resize(self.size().width(), self.size().height())

        # Function
        self.default_tickers = self.__getTickers()
        self.btn_add_ticker.clicked.connect(self.addTicker)
        self.btn_add_ticker_2.clicked.connect(self.addTickersRand)
        self.btn_choice_ts.clicked.connect(self.addStrategy)
        self.btn_test.clicked.connect(self.startTest)
        self.btn_choice_ticker.clicked.connect(self.rewritterData)
        self.btn_reset_data.clicked.connect(self.resetData)

        # Start function
        self.__writtingTable(self.table_ts, 'Стратегия', self.__getStrategy())
        self.__writtingTable(self.table_tickers_all, 'Тикер', self.default_tickers)
        self.__crateTempDir()


        # Show App
        self.show()


    def resizeEvent(self, QResizeEvent):

        "Метод авторазмера формы"

        self.mainlayout.resize(self.size().width()-20,self.size().height())

    def __getColForVal(self,current_val, low, hight, step):

        def __getColorList(steps):

            def __hex_to_rgb(col):
                temp = [int(col[1:][i:i + 2], 16) for i in (0, 2, 4)]
                return temp[0], temp[1], temp[2]

            def __rgb_to_hex(a1, a2, a3):
                return '#%02x%02x%02x' % tuple([int(a1), int(a2), int(a3)])

            c1 = '#ff0000'
            c2 = '#00ff00'
            steps = steps
            r1, g1, b1 = __hex_to_rgb(c1)
            r2, g2, b2 = __hex_to_rgb(c2)
            rdelta, gdelta, bdelta = (r2 - r1) / steps, (g2 - g1) / steps, (b2 - b1) / steps
            output = []
            for step in range(steps):
                r1 += rdelta
                g1 += gdelta
                b1 += bdelta
                output.append(__rgb_to_hex(r1, g1, b1))
            return output

        def __frange(x, y, jump):
            while x < y:
                yield x
                x += jump

        npp = ([round(i, 2) for i in __frange(low, hight, step)])
        value = current_val
        icol = 0
        if value < min(npp):
            icol = 0
        else:
            icol = len(npp) - 1
        for i, val in enumerate(npp):
            if value == val:
                icol = i
        list_col = __getColorList(len(npp))
        col = list_col[icol]
        return col

    def __getStrategy(self):

        "Получить список всех стратегий в классе GenerateStrategy"

        return [i for i in dir(GeneratorTradingSystem) if 'Strategy' in i]

    def __writtingTable(self, val, name, list):

        """
        Рисовалка таблицы с одним столбцом с заданными параметрами:

        val: QTableWidget, экемпляр класса (в какую таблицу надо рисовать
        name: str, Название столбца
        list: List, Исходный набор данных
        """

        try:
            tickers = list
            val.setColumnCount(0)
            val.setRowCount(0)
            val.setColumnCount(1)
            val.setHorizontalHeaderLabels([name])
            for i, row in enumerate(tickers):
                val.setRowCount(val.rowCount() + 1)
                item = QTableWidgetItem(str(row))
                val.setItem(i, 0, item)
                item.setTextAlignment(Qt.AlignCenter)
            val.viewport().installEventFilter(self)
            val.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        except Exception:
            print(traceback.format_exc())

    def __choiceTrendStrategy(self, df):

        """
        Выбор стратегии, возвращяет уже расчитанный экземпляр класса для выбранной стратегии:

        df: pd.DataFrame(), Котировки

        """
        try:
            _strategy = GeneratorTradingSystem(self.capital, self.comission, self.profit_tax, df)
            if self.strategy == 'StrategyTrend1':
                return _strategy.StrategyTrend1()
            if self.strategy == 'StrategyTrend2':
                return _strategy.StrategyTrend2()
            if self.strategy == 'StrategyTrend3':
                return _strategy.StrategyTrend3()
            if self.strategy == 'StrategyRandom':
                return _strategy.StrategyRandom()
        except Exception:
            print(traceback.format_exc())

    def __getData(self, strr):

        "Получение данных по инструменту и их валидация"

        df = pd.read_csv(os.path.join(self.path_data, str('table_' + strr + '.txt')),
                         converters={'<TICKER>': str, '<PER>': str, '<DATE>': str, '<TIME>': str, '<OPEN>': float,
                                     '<HIGH>': float, '<LOW>': float, '<CLOSE>': float, '<VOL>': int}
                         )
        df.columns = ['tick', 'per', 'dt', 'time', 'open', 'high', 'low', 'close', 'vol']
        if df['per'][0] == 'D':
            df['time'] = '190000'
        df['dt'] = df['dt'].str[:4] + '-' + df['dt'].str[4:6] + '-' + df['dt'].str[6:] + ' ' + \
                   df['time'].str[:2] + ':' + df['time'].str[2:4] + ':' + df['time'].str[4:]
        df['dt'] = pd.to_datetime(df['dt'])
        return df


    def __clalculateTradingStrategy(self):

        "Расчет метрик для всех выбранных тикеров для указанной стратегии"

        try:
            df = [None for _ in self.tickers]
            metrics = [None for _ in self.tickers]
            mer = [None for _ in self.tickers]
            for i, strr in enumerate(self.tickers):
                try:
                    df[i] =  pd.read_csv(os.path.join(self.path_data, str(strr)),
                             converters={'<TICKER>': str, '<PER>': str, '<DATE>': str, '<TIME>': str, '<OPEN>': float,
                                         '<HIGH>': float, '<LOW>': float, '<CLOSE>': float, '<VOL>': int}
                             )
                    trend = self.__choiceTrendStrategy(df[i])
                    trend.Calculate()
                    metrics[i] = trend.metrics
                    df[i] = trend.df
                    mer[i] = trend.mer
                except pandas.errors.EmptyDataError:
                    print(strr)
            summ = []
            for i in range(12):
                summ.append(0)
            for i in range(len(metrics)):
                summ[0] += metrics[i]['net_profit']
                summ[1] += metrics[i]['net_profit_proc'] if (metrics[i]['net_profit_proc'] < 999999) else 0
                summ[2] += metrics[i]['PF'] if (metrics[i]['PF'] < 999999) and (metrics[i]['PF'] < 200) else 0
                summ[3] += metrics[i]['APF'] if (metrics[i]['APF'] < 999999) and (metrics[i]['APF'] < 200) else 0
                summ[4] += round(float(np.corrcoef([i for i in range(len(metrics[i]['EQ']))], metrics[i]['EQ'])[1, 0]), 2) if ((round(float(np.corrcoef([i for i in range(len(metrics[i]['EQ']))], metrics[i]['EQ'])[1, 0]), 2)) < 999999) \
                                                                                                                                and (round(float(np.corrcoef([i for i in range(len(metrics[i]['EQ']))], metrics[i]['EQ'])[1, 0]), 2)) != 1 else 0
                summ[5] += metrics[i]['AVG_DD'] if (metrics[i]['AVG_DD'] < 999999) else 0
                summ[6] += metrics[i]['AVG_Trade'] if (metrics[i]['AVG_Trade'] < 999999) else 0
                summ[7] += metrics[i]['P'] if (metrics[i]['P'] < 999999) and (metrics[i]['P'] != 1) else 0
                summ[8] += metrics[i]['Mx'] if (metrics[i]['Mx'] < 999999) else 0
                summ[9] += metrics[i]['Mx_proc'] if (metrics[i]['Mx_proc'] < 999999) and (metrics[i]['Mx_proc'] < 200) else 0
                summ[10] += metrics[i]['RF'] if (metrics[i]['RF'] < 999999) and (metrics[i]['RF'] < 200) else 0
                summ[11] += metrics[i]['Calmar_ratio'] if (metrics[i]['Calmar_ratio'] < 999999) and (metrics[i]['Calmar_ratio'] < 200) else 0
            for i, strr in enumerate(summ):
                summ[i] = strr / len(metrics)
            metrics.append({'net_profit': summ[0], 'net_profit_proc': summ[1], 'PF': summ[2], 'APF': summ[3], 'EQ': summ[4], 'AVG_DD': summ[5], 'AVG_Trade': summ[6], 'P': summ[7],
                            'Mx': summ[8], 'Mx_proc': summ[9], 'RF': summ[10], 'Calmar_ratio': summ[11]})
            print(summ)
            return metrics, df, mer
        except Exception:
            print(traceback.format_exc())

    def __getPlotTrades(self):

        "Построение графика сделок"

        df = self.df[self.tickers.index(self.row_ticks_res)]
        fig, ax = plt.subplots(1,1, figsize=(16,7))
        ax.plot(df['dt'], df['close'])
        ax.scatter(df[df['buy']].dt, df[df['buy']].close, marker='^', color='g', s=100)
        ax.scatter(df[df['sell']].dt, df[df['sell']].close, marker='v', color='r', s=100)
        plt.xlabel('Дата')
        plt.ylabel('Цена')
        plt.savefig(os.path.join(self.temp_path,f'img_closes_{self.row_ticks_res}.jpg'))

    def __getEqPlot(self):

        "Построение кривой капитала"

        eq_s = self.metrics[self.tickers.index(self.row_ticks_res)]['EQ']
        # eq_i = list(self.df[self.tickers.index(self.row_ticks_res)].
        #             loc[[int(i) for i in [0] + list(self.mer[self.tickers.index(self.row_ticks_res)]['index_sell'])]]['dt'])
        eq_i = list(self.df[self.tickers.index(self.row_ticks_res)].loc[[0]]['dt']) + list(self.mer[self.tickers.index(self.row_ticks_res)]['index_sell'])
        fig, ax = plt.subplots(1,1, figsize=(16,7))
        ax.plot(eq_i, eq_s, marker='.')
        ax.fill_between(eq_i, eq_s, self.capital, where=(np.array(eq_s) < self.capital), alpha=0.4, color='red', interpolate=True)
        ax.fill_between(eq_i, eq_s, self.capital, where=(np.array(eq_s) >= self.capital), alpha=0.4, color='green', interpolate=True)
        plt.xlabel('Дата')
        plt.ylabel('Цена')
        plt.savefig(os.path.join(self.temp_path, f'img_eq_{self.row_ticks_res}.jpg'))

    def __writeTableTrades(self):

        df = self.mer[self.tickers.index(self.row_ticks_res)]
        self.table_trades.setColumnCount(0)
        self.table_trades.setRowCount(0)
        self.table_trades.setColumnCount(12)
        self.table_trades.setHorizontalHeaderLabels(['Покупка','Продажа','Дата покупки','Дата продажи','Прибыль','Тиков в позиции',
                                                     'Сумма покупки', 'Сумма продажи', 'Комиссии', 'Налог', 'Итоговая прибыль', 'Капитал'])
        for i, row in df.iterrows():
            self.table_trades.setRowCount(self.table_trades.rowCount() + 1)
            for j in range(len(df.columns.values.tolist())):
                if j > 3:
                    item = QTableWidgetItem(str(round(row[j], 2)))
                else:
                    item = QTableWidgetItem(str(row[j]))
                self.table_trades.setItem(i, j, item)
                item.setTextAlignment(Qt.AlignCenter)

    def __writeTableMetrics(self):
        try:

            df = pd.DataFrame(columns=self.tickers + ['MEAN'])
            col_df = pd.DataFrame(columns=self.tickers + ['MEAN'])
            for i in range(len(self.tickers) + 1):
                ms = self.metrics[i]
                column = []
                col_column = []
                column.append(round(ms['net_profit'], 2))
                column.append(round(ms['net_profit_proc'], 2))
                column.append(round(ms['PF'], 2))
                column.append(round(ms['APF'], 2))
                column.append(round(float(np.corrcoef([i for i in range(len(ms['EQ']))], ms['EQ'])[1, 0]), 2)) if i < len(self.tickers) else column.append(round(float(ms['EQ']), 2))
                column.append(round(ms['AVG_DD'], 2))
                column.append(round(ms['AVG_Trade'], 2))
                column.append(round(ms['P'], 2))
                column.append(round(ms['Mx'], 2))
                column.append(round(ms['Mx_proc'], 2))
                column.append(round(ms['RF'], 2))
                column.append(round(ms['Calmar_ratio'], 2))
                col_column.append('#ffffff')
                col_column.append(self.__getColForVal(column[1], -0.3, 1.5, 0.01)) if str(column[1]) != 'nan' else col_column.append('#ffffff')
                col_column.append(self.__getColForVal(column[2], 1, 2.1, 0.01)) if str(column[2]) != 'nan' else col_column.append('#ffffff')
                col_column.append(self.__getColForVal(column[3], 1, 2.1, 0.01)) if str(column[3]) != 'nan' else col_column.append('#ffffff')
                col_column.append(self.__getColForVal(column[4], 0.7, 1, 0.01)) if str(column[4]) != 'nan' else col_column.append('#ffffff')
                col_column.append('#ffffff')
                col_column.append('#ffffff')
                col_column.append(self.__getColForVal(column[7], 0.5, 1, 0.01)) if str(column[7]) != 'nan' else col_column.append('#ffffff')
                col_column.append('#ffffff')
                col_column.append(self.__getColForVal(column[9], 0, 1, 0.01)) if str(column[9]) != 'nan' else col_column.append('#ffffff')
                col_column.append(self.__getColForVal(round(column[10], 1), 2, 20, 0.1)) if str(column[10]) != 'nan' else col_column.append('#ffffff')
                col_column.append(self.__getColForVal(column[11], 1, 3, 0.01)) if str(column[11]) != 'nan' else col_column.append('#ffffff')
                df[df.columns[i]] = column
                col_df[col_df.columns[i]] = col_column


            df.index = ['Чистая прибыль', 'Чистая прибыль %', 'Профит фактор', 'Достоверный профит фактор', 'Корреляция EQ',
                        'Средний убыток', 'Средняя прибыль', 'Верятность выигрыша', 'Мат ожидание', 'Мат ожидание %', 'Фактор востановления', 'Коэф калмара']
            df = df.reset_index()
            self.table_metricks.setColumnCount(0)
            self.table_metricks.setRowCount(0)
            self.table_metricks.setColumnCount(len(df.columns))
            self.table_metricks.setHorizontalHeaderLabels(['Параметры'] + self.tickers)
            for i, row in col_df.iterrows():
                self.table_metricks.setRowCount(self.table_metricks.rowCount() + 1)
                for j in range(self.table_metricks.columnCount()):
                    if j != 0:
                        self.table_metricks.setItem(i, j, QTableWidgetItem(''))
                        self.table_metricks.item(i, j).setBackground(QColor(row[j - 1]))
            for i, row in df.iterrows():
                for j in range(len(df.columns.values.tolist())):
                    if j == 0:
                        self.table_metricks.setItem(i, j, QTableWidgetItem(str(row[j])))
                    else:
                        self.table_metricks.item(i, j).setText(str(row[j]))
                    self.table_metricks.item(i, j).setTextAlignment(Qt.AlignCenter)
        except Exception:
            print(traceback.format_exc())

    def __updateForm(self):

        "Обновление формы"

        a = QPixmap(os.path.join(self.temp_path, f'img_closes_{self.row_ticks_res}.jpg'))
        a = a.scaled(self.label_plot_trades.size(), Qt.IgnoreAspectRatio, Qt.SmoothTransformation)
        self.label_plot_trades.setPixmap(a)
        a = QPixmap(os.path.join(self.temp_path, f'img_eq_{self.row_ticks_res}.jpg'))
        a = a.scaled(self.label_plot_eq.size(), Qt.IgnoreAspectRatio, Qt.SmoothTransformation)
        self.label_plot_eq.setPixmap(a)

    def __crateTempDir(self):
        path1 = os.path.join(tempfile.gettempdir(), 'TS')
        if not os.path.exists(path1):
            os.mkdir(path1)
        self.temp_path = path1

    def __getTickers(self):

        "Получить список всех тикеров по имеющимся котировкам"

        tickers = []
        for strr in os.listdir(self.path_data):
            tickers.append(strr)
        return tickers

    def eventFilter(self, source, event):

        "Получить текущее значения нажатой ячейки таблицы"

        if event.type() == QEvent.MouseButtonRelease:
            if self.table_tickers_all.selectedIndexes() != []:
                self.row_ticks = self.table_tickers_all.currentItem().text()
                self.btn_add_ticker.setEnabled(True)
                print(self.table_tickers_all.currentItem().text())
            if self.table_ts.selectedIndexes() != []:
                self.strategy = self.table_ts.currentItem().text()
                self.btn_choice_ts.setEnabled(True)
                print(self.table_ts.currentItem().text())
            if self.table_tickers_res.selectedIndexes() != []:
                self.row_ticks_res = self.table_tickers_res.currentItem().text()
                print(self.table_tickers_res.currentItem().text())

        return QObject.event(source, event)

    def addTicker(self):

        "Добавляем выбранный тикер в наш набор тикеров"
        try:
            self.tickers.append(self.row_ticks)
            self.default_tickers.remove(self.row_ticks)
            self.__writtingTable(self.table_tickers_all, 'Тикер', self.default_tickers)
            self.__writtingTable(self.table_tickers_res, 'Тикер', self.tickers)
            self.btn_add_ticker.setEnabled(False)
            if not self.strategy is None:
                self.btn_test.setEnabled(True)
        except Exception:
            print(traceback.format_exc())

    def addTickersRand(self):

        "Добавляем рандомно 10 тикеров"
        try:
            for i in range(10):
                row = random.randint(1,len(self.default_tickers) - 1)
                self.tickers.append(self.default_tickers[row])
                self.default_tickers.remove(self.default_tickers[row])
                self.__writtingTable(self.table_tickers_all, 'Тикер', self.default_tickers)
                self.__writtingTable(self.table_tickers_res, 'Тикер', self.tickers)
                # self.btn_add_ticker_2.setEnabled(False)
                if not self.strategy is None:
                    self.btn_test.setEnabled(True)
        except:
            print(traceback.format_exc())

    def addStrategy(self):

        "Добавляем стратегию с которой будем работать"

        try:
            self.label_res_ts.setText(self.strategy)
            if self.tickers != []:
                self.btn_test.setEnabled(True)
        except Exception:
            print(traceback.format_exc())
    def startTest(self):

        "Запускаем обработку стратегии по входным параметрам и вывод данных"
        try:
            plt.rcParams['font.size'] = '14'
            self.btn_test.setEnabled(False)
            self.btn_add_ticker.setEnabled(False)
            self.btn_choice_ts.setEnabled(False)
            self.table_tickers_all.setEnabled(False)
            self.table_ts.setEnabled(False)
            self.btn_reset_data.setEnabled(True)
            self.btn_choice_ticker.setEnabled(True)
            self.table_ts.clearSelection()

            self.row_ticks_res = self.tickers[0]
            self.metrics, self.df, self.mer = self.__clalculateTradingStrategy()

            # Строим график покупок и продаж по ценам закрытия
            self.__getPlotTrades()

            # Строим график кривой капитала
            self.__getEqPlot()

            # Обновляем форму
            self.__updateForm()

            # Рисуем таблицу трейдов
            self.__writeTableTrades()

            # Рисуем таблицу метрик
            self.__writeTableMetrics()

        except Exception:
            print(traceback.format_exc())

    def rewritterData(self):

        "Обновление данных для нового тикера"

        try:
            self.__getPlotTrades()
            self.__getEqPlot()
            self.__updateForm()
            self.__writeTableTrades()
        except Exception:
            print(traceback.format_exc())

    def resetData(self):

        "Сброс настроект для новых данных"

        try:
            self.label_res_ts.clear()
            self.label_plot_trades.clear()
            self.label_plot_eq.clear()

            self.table_trades.clear()
            self.table_metricks.clear()
            self.table_tickers_res.clear()

            self.metrics = None
            self.df = None
            self.mer = None

            self.row_ticks = None
            self.row_ticks_res = None
            self.strategy = None
            self.tickers = []
            self.default_tickers = self.__getTickers()
            self.__writtingTable(self.table_tickers_all, 'Тикер', self.default_tickers)

            # self.btn_reset_data.setEnabled(False)
            self.btn_choice_ticker.setEnabled(False)
            self.btn_test.setEnabled(False)

            self.table_tickers_all.setEnabled(True)
            self.table_ts.setEnabled(True)
        except Exception:
            print(traceback.format_exc())


app = QApplication(sys.argv)
UIWindow = UI()
sys.exit(app.exec_())