import os.path
import random

import pandas as pd
import requests
import urllib
import time
import warnings

import ta

warnings.simplefilter("ignore")
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import numpy as np

class ParsingQuotes():
    """
    Класс для представления тикеров с их id по конкретному рынку
    :param getIdTicker: write json file tick : id
    """

    def __init__(self, max_offset, url, path):
        """
        Параметры:
        :param max_offset: int, максимальное количество активов
        :param url: str, ссылка на finam с рынком (https://www.finam.ru/infinity/quotes/futures/moex/?offset={})
        :param path: str, путь с названием файла куда сохранить котировки
        """
        self.max_offset = max_offset
        self.start_offset = 0
        self.url = url
        self.path = path

    def __getTickerUrls(self):
        ticker_urls = []
        for i in range(self.start_offset, self.max_offset + 1, 50):
            response = requests.get(self.url.format(i))
            soup = str(BeautifulSoup(response.text))
            while soup.find('data-chpurl') != -1:
                soup = soup[soup.find('data-chpurl')+11:]
                ticker_urls.append(soup[soup.find('"')+1 : soup.find('" ')])
        return ticker_urls

    def getIdTicers(self):
        ticker_urls = self.__getTickerUrls()
        url = 'https://www.finam.ru/profile/{}/export'
        tickers = {}
        for i in range(len(ticker_urls)):
            try:
                response = requests.get(url.format(ticker_urls[i]))
                soup = str(BeautifulSoup(response.text))
                soup = soup[soup.find('Finam.IssuerProfile.Main.issue') : ]
                if soup != '\n':
                    soup = soup[soup.find('id') : ]
                    idd = int(soup[soup.find(': ') + 2 : soup.find(',')])
                    soup = soup[soup.find('code') : ]
                    code = soup[soup.find(' "') + 2 : soup.find(',') - 1]
                    tickers[code] = idd
                    time.sleep(0.5)
            except Exception as ex:
                print(ex)
        with open(self.path, 'w') as f:
            f.write(str(tickers))


class AppURLopener(urllib.request.FancyURLopener):
    "Класс переопределяет атрибут version"
    version = "Mozilla/5.0"


class DataFrameFromFinam():
    """
    Класс для представления объекта исторических данных
    """
    def __init__(self, tickers, dtstart, dtstop, ticker, output_format, strTick,filename):
        """
        Пареметры:
        :param tickers: dict, Набор входных данных вида
        :param dtstart: str, Начальная дата экспорта
        :param dtstop: str, Конечная дата экспорта
        :param ticker: str, Тикер инструмента
        :param output_format: str, Расширение выходного файла
        :param strTick: str, Интервал между строками ('1 мин', '5 мин', '10 мин', '15 мин', '30 мин', 1 час', '1 день', '1 неделя', '1 месяц')
        :param filename: str, Имя выходного файла
        """
        self.dtstart = dtstart
        self.dtstop = dtstop
        self.ticker = ticker
        self.output_format = output_format
        self.strTick = strTick
        self.tickers = tickers
        self.ticks = {'тик': 1, '1 мин': 2, '5 мин': 3, '10 мин': 4, '15 мин': 5, '30 мин': 6,
                      '1 час': 7, '1 день': 8, '1 неделя': 9, '1 месяц': 10}
        self.filename = filename

    def __getRequest(self):

        "Формирует запрос к сайту finam"

        url = "http://export.finam.ru"
        interval = self.ticks[self.strTick]
        output_file = '%s_%s_%s%s' % (
        self.ticker, f'{self.dtstart.split(".")[2][2:]}{self.dtstart.split(".")[1]}{self.dtstart.split(".")[0]}',
        f'{self.dtstop.split(".")[2][2:]}{self.dtstop.split(".")[1]}{self.dtstop.split(".")[0]}',
        self.output_format)
        params = {'market': '1', 'em': f'{self.tickers[self.ticker]}', 'code': f'{self.ticker}', 'apply': '0',
                  'df': (
                              int(f'{self.dtstart.split(".")[0][1] if self.dtstart.split(".")[0][0] == "0" else self.dtstart.split(".")[0]}') - 1) if
                  self.dtstart.split(".")[0] != "01" else 1,
                  'mf': int(
                      f'{self.dtstart.split(".")[1][1] if self.dtstart.split(".")[1][0] == "0" else self.dtstart.split(".")[1]}') - 1,
                  'yf': f'{self.dtstart.split(".")[2]}',
                  'from': f'{self.dtstart}',
                  'dt': int(
                      f'{self.dtstop.split(".")[0][1] if self.dtstop.split(".")[0][0] == "0" else self.dtstop.split(".")[0]}') - 1 if
                  self.dtstop.split(".")[0] != "01" else 1,
                  'mt': int(
                      f'{self.dtstop.split(".")[1][1] if self.dtstop.split(".")[1][0] == "0" else self.dtstop.split(".")[1]}') - 1,
                  'yt': f'{self.dtstop.split(".")[2]}',
                  'to': f'{self.dtstop}',
                  'p': f'{interval}',
                  'f': '%s_%s_%s' % (self.ticker,
                                     f'{self.dtstart.split(".")[2][2:]}{self.dtstart.split(".")[1]}{self.dtstart.split(".")[0]}',
                                     f'{self.dtstop.split(".")[2][2:]}{self.dtstop.split(".")[1]}{self.dtstop.split(".")[0]}'),
                  'e': f'{self.output_format}',
                  'cn': f'{self.ticker}',
                  'dtf': 1,
                  'tmf': 1,
                  'MSOR': 1,
                  'mstime': 'on',
                  'mstimever': 1,
                  'sep': 1,
                  'sep2': 1,
                  'datf': 1,
                  'at': 1}
        return url + '/' + output_file + '?' + urllib.parse.urlencode(params)

    def getDataframe(self):

        "Формирует датафрейм с заданными параметрами"

        request = self.__getRequest()
        loader = AppURLopener()
        with open(f'{self.filename}.txt', 'wb') as f:
            f.write(loader.open(request).read())
        return pd.read_csv(f'{self.filename}.txt')


# Классы различных торговых стратегий

class TrendStrategy():
    " Класс для бэктестинга трендовой пробойной стратегии"

    def __init__(self, df, path):
        """
        Праметры:

        :param df: DataFrame() - Исходный набор данных по инструменту

        """
        self.df = df
        self.lower_bound = 4
        self.upper_bound = 35
        self.countdf = (self.upper_bound - self.lower_bound - 1) // 2 + 1
        self.merged = [None for _ in range(self.countdf)]                   # Результирующий набор данных
        self.per = []                                                       # Период для линий сопротивления и поддержки
        self.tp = []                                                        # total profit для одного актива с разным периодом
        self.ball = [[None, None, None] for _ in range(2)]
        self.path = path

    def __Aplyindicator(self):

        "Функция построения разных периодов линий сопротивления и поддержки"

        for i in range(self.lower_bound, self.upper_bound, 2):
            self.df[f'max_{len(self.df)//i}'] = self.df.close.rolling(len(self.df) // i).max()
            self.df[f'min_{len(self.df)//i}'] = self.df.close.rolling(len(self.df) // i).min()

    def __Conditions(self):

        "Функция расчета торговых опепраций и построения результирующей таблица с трейдами"

        self.__Aplyindicator()
        count = 0
        self.per = []
        self.merged = [None for _ in range(self.countdf)]
        for i in range(self.lower_bound, self.upper_bound, 2):
            self.df[f'low_trend{i}'] = np.where(self.df[f'min_{len(self.df)//i}'] < self.df[f'min_{len(self.df)//i}'].shift(1), True, False)
            self.df[f'high_trend{i}'] = np.where(self.df[f'max_{len(self.df)//i}'] < self.df[f'max_{len(self.df)//i}'].shift(1), True, False)

            high = []
            low = []
            open_trend = False
            for j in range(1, len(self.df)):
                if self.df[f'max_{len(self.df)//i}'][j] > self.df[f'max_{len(self.df)//i}'][j - 1]:
                    if open_trend == False:
                        high.append(j)
                        open_trend = True
                elif self.df[f'min_{len(self.df)//i}'][j] < self.df[f'min_{len(self.df)//i}'][j - 1]:
                    if open_trend:
                        low.append(j)
                        open_trend = False
            self.df[f'low_trend{count}'] = False
            self.df[f'high_trend{count}'] = False
            for j in high:
                self.df[f'high_trend{count}'][j] = True
            for j in low:
                self.df[f'low_trend{count}'][j] = True
            self.merged[count] = self.df
            self.merged[count] = pd.concat([self.df.iloc[high].close, self.df.iloc[low].close], axis=1)
            self.merged[count].columns = ['High', 'Low']
            self.merged[count] = self.merged[count].sort_index()
            self.merged[count]['total_profit'] = self.merged[count].shift(-1).Low - self.merged[count].High
            self.merged[count]['proc_profit'] = ( self.merged[count].shift(-1).Low - self.merged[count].High) / self.merged[count].High
            count += 1
            self.per.append(len(self.df)//i)





    def PlotWritter(self):

        "Функция построения графиков по разным периодам линии сопротивления и поддержки"

        m = self.getMetrics()
        fig, ax = plt.subplots(len(self.merged), 1, figsize=(15, 6 * len(self.merged)))
        for i in range(len(self.merged)):
            index = i
            ax[i].plot(self.df[['close',f'max_{self.per[index]}', f'min_{self.per[index]}']])
            ax[i].scatter(self.df.index[self.df[f'high_trend{index}']], self.df[self.df[f'high_trend{index}']].close, marker='^', color='g', s=100)
            ax[i].scatter(self.df.index[self.df[f'low_trend{index}']], self.df[self.df[f'low_trend{index}']].close, marker='v', color='r', s=100)
            ax[i].legend(['Close',f'max_{self.per[index]}', f'min_{self.per[index]}'])
            ax[i].set_title('Дох : ' + str(round(max(self.tp) * 100, 2)) +
                            ' Проц от баров: ' + str(round(self.per[index]/len(self.df) * 100, 2)))
            # self.tp[index] = -9999
        plt.show()

    def getMetrics(self):

        "Функция расчета метрик по разным построеным периодам линии сопротивления и поддержки"

        self.__Conditions()
        metrics = []
        self.tp = []
        for i in range(len(self.per)):
            total_trades = len(self.merged[i][self.merged[i]['High'].notnull()])
            count_high_trades = len(self.merged[i][self.merged[i]['proc_profit'] > 0])
            count_lower_trades = len(self.merged[i][self.merged[i]['proc_profit'] < 0])
            min_proc_trade = self.merged[i].proc_profit.min()
            max_proc_trade = self.merged[i].proc_profit.max()
            mean_proc_trade = self.merged[i].proc_profit.mean()
            total_profit = self.merged[i].proc_profit.sum()
            metrics.append({'Количество трейдов': total_trades, 'Количество прибыльных трейдов': count_high_trades,
                       'Количество убыточных трейдов': count_lower_trades,
                       'Максимальная просадка': round(min_proc_trade * 100, 2),
                       'Максимальная прибыль': round(max_proc_trade * 100, 2),
                       'Средняя доходность трейда': round(mean_proc_trade * 100, 2),
                       'Итоговая доходность за весь период': round(total_profit * 100, 2)})
            self.tp.append(total_profit)
        return metrics

    def __PlotWritterParam(self, index, mean_year_profit):
        mean_year_profit_index = index
        fig, ax = plt.subplots(2, 1, figsize=(15, 12))
        ax[0].plot(self.df[['close', f'max_{self.per[mean_year_profit_index[0]]}',
                         f'min_{self.per[mean_year_profit_index[0]]}']])
        ax[0].scatter(self.df.index[self.df[f'high_trend{mean_year_profit_index[0]}']],
                   self.df[self.df[f'high_trend{mean_year_profit_index[0]}']].close,
                   marker='^', color='g', s=100)
        ax[0].scatter(self.df.index[self.df[f'low_trend{mean_year_profit_index[0]}']],
                   self.df[self.df[f'low_trend{mean_year_profit_index[0]}']].close,
                   marker='v', color='r', s=100)
        ax[0].legend(['Close', f'max_{self.per[mean_year_profit_index[0]]}', f'min_{self.per[mean_year_profit_index[0]]}'])
        ax[0].set_title('MIN Итг дох: ' + str(round(self.merged[mean_year_profit_index[0]].proc_profit.sum() * 100, 2)) +
                     ' Ср год дох : ' + str(round(mean_year_profit[0] * 100, 2)))

        ax[1].plot(self.df[['close', f'max_{self.per[mean_year_profit_index[1]]}',
                         f'min_{self.per[mean_year_profit_index[1]]}']])
        ax[1].scatter(self.df.index[self.df[f'high_trend{mean_year_profit_index[1]}']],
                   self.df[self.df[f'high_trend{mean_year_profit_index[1]}']].close,
                   marker='^', color='g', s=100)
        ax[1].scatter(self.df.index[self.df[f'low_trend{mean_year_profit_index[1]}']],
                   self.df[self.df[f'low_trend{mean_year_profit_index[1]}']].close,
                   marker='v', color='r', s=100)
        ax[1].legend(['Close', f'max_{self.per[mean_year_profit_index[1]]}', f'min_{self.per[mean_year_profit_index[1]]}'])
        ax[1].set_title('MAX Итг дох: ' + str(round(self.merged[mean_year_profit_index[1]].proc_profit.sum() * 100, 2)) +
                     ' Ср год дох : ' + str(round(mean_year_profit[1] * 100, 2)))


        plt.savefig(os.path.join(self.path,list(self.df['tick'])[0] + '.png'))


    def __getMeanYearProfit(self):

        "Функция расчета балла по макс среднегодовой доходности"

        self.__Conditions()
        mean_year_profit = []
        mean_year_profit_index = []
        tek_mean_year_profit = []
        for i in range(len(self.per)):
            g = self.merged[i].reset_index()
            count_unprofit = len(g[g['Low'].notna()])
            if count_unprofit > 0:
                g['total_days'] = g['index'].shift(-1) - g['index']
                tek_mean_year_profit.append(g.proc_profit.sum() / (g.total_days.sum() / 366))
            else:
                tek_mean_year_profit.append(None)
        mean_year_profit.append(min([i for i in tek_mean_year_profit if i is not None]))
        mean_year_profit_index.append(tek_mean_year_profit.index(min([i for i in tek_mean_year_profit if i is not None])))
        mean_year_profit.append(max([i for i in tek_mean_year_profit if i is not None]))
        mean_year_profit_index.append(tek_mean_year_profit.index(max([i for i in tek_mean_year_profit if i is not None])))
        for i in range(2):
            ball = 50 * round(mean_year_profit[i] * 100, 2) / 100
            if ball < 0:
                self.ball[i][0] = 0
            elif ball > 0 and ball < 50:
                self.ball[i][0] = ball
            else:
                self.ball[i][0] = 50
        self.__PlotWritterParam(mean_year_profit_index, mean_year_profit)
        return mean_year_profit_index

    def __getCurrentPhase(self):

        "Функция расчета балла по текущей фазе актива"

        index = self.__getMeanYearProfit()
        # Определяем является ли последняя позиция открыта и начисляем балл пропорционально отклонению
        for i in range(2):
            bal = 0
            last_high_line = list(self.df[f'max_{self.per[index[i]]}'][-1:])[0]
            last_price = list(self.df['close'][-1:])[0]
            if str(list(self.merged[index[i]]['High'][-1:])[0]) != 'nan':
                bal += 5
            dev = abs(round((1 - (last_price / last_high_line)) * 100, 2))
            if dev >= 0 and dev <= 30:
                deviation = dev
            else:
                deviation = 30
            calcbal = 25 * deviation / 30
            self.ball[i][1] = abs(calcbal - 25) + bal
        return index

    def getCalculateParam(self):

        "Функция расчета балла по проценту прибыльных сделок"

        index = self.__getCurrentPhase()
        for i in range(2):
            bal = 0
            count_profit = len(self.merged[index[i]][self.merged[index[i]]['proc_profit'] > 0])
            count_unprofit = len(self.merged[index[i]][self.merged[index[i]]['proc_profit'] < 0])
            sum_trades = count_profit + count_unprofit
            if count_profit > 0:
                deviation = round((count_profit / sum_trades) * 100, 2)
                bal += 20 * deviation / 100
            self.ball[i][2] = bal




class BolingerBand():
    " Класс для бэктестинга стратегии торговли по линиям болинджера"

    def __init__(self, df):
        """
        Параметры:

        :param df: DataFrame() - Исходный набор данных по инструменту
        """
        self.df = df
        self.merged = None

    def __Aplyindicator(self):
        self.df['SMA_200'] = self.df.close.rolling(200).mean()
        self.df['SMA_20'] = self.df.close.rolling(20).mean()
        self.df['stddef'] = self.df.close.rolling(20).std()
        self.df['Upper'] = self.df.SMA_20 + 2 * self.df.stddef
        self.df['Lower'] = self.df.SMA_20 - 2 * self.df.stddef

    def __Conditions(self):
        self.__Aplyindicator()
        buy = []
        sell = []
        open_pos = False
        for i in range(len(self.df)):
            if self.df.close[i] < self.df.Lower[i]:
                if open_pos == False:
                    buy.append(i)
                    open_pos = True
            elif self.df.close[i] > self.df.Upper[i]:
                if open_pos:
                    sell.append(i)
                    open_pos = False
        self.df['Buy'] = False
        self.df['Sell'] = False
        for i in buy:
            self.df.Buy[i] = True
        for i in sell:
            self.df.Sell[i] = True
        self.merged = pd.concat([self.df.iloc[buy].close, self.df.iloc[sell].close], axis=1)
        self.merged.columns = ['Buys', 'Sells']
        self.merged = self.merged.sort_index()
        self.merged['total_profit'] = self.merged.shift(-1).Sells - self.merged.Buys
        self.merged['proc_profit'] = (self.merged.shift(-1).Sells - self.merged.Buys) / self.merged.Buys

    def PlotWritter(self):
        self.__Conditions()
        plt.figure(figsize=(16, 7))
        plt.plot(self.df[['close', 'Upper', 'Lower', 'SMA_20', 'SMA_200']])
        plt.scatter(self.df.index[self.df.Buy], self.df[self.df.Buy].close, marker='^', color='g', s=100)
        plt.scatter(self.df.index[self.df.Sell], self.df[self.df.Sell].close, marker='v', color='r', s=100)
        plt.fill_between(self.df.index, self.df.Upper, self.df.Lower, color='grey', alpha=0.3)
        plt.legend(['Close', 'Upper', 'Lower', 'SMA_20', 'SMA_200'])
        plt.show()

    def getMetrics(self):
        self.__Conditions()
        total_trades = len(self.merged[self.merged['Buys'].notnull()])
        count_high_trades = len(self.merged[self.merged['proc_profit'] > 0])
        count_lower_trades = len(self.merged[self.merged['proc_profit'] < 0])
        min_proc_trade = self.merged.proc_profit.min()
        max_proc_trade = self.merged.proc_profit.max()
        mean_proc_trade = self.merged.proc_profit.mean()
        total_profit = self.merged.proc_profit.sum()
        metrics = {'Количество трейдов': total_trades, 'Количество прибыльных трейдов': count_high_trades,
                   'Количество убыточных трейдов': count_lower_trades,
                   'Максимальная просадка': round(min_proc_trade * 100, 2),
                   'Максимальная прибыль': round(max_proc_trade * 100, 2),
                   'Средняя доходность трейда': round(mean_proc_trade * 100, 2),
                   'Итоговая доходность за весь период': round(total_profit * 100, 2)}
        return metrics


class GeneratorTradingSystem():

    def __init__(self, capital, comission, profit_tax, df):

        """
        Класс для описания и тестирования торговых систем

        capital: int - Начальный капитал
        comission: float - Комиссия за совершение операции
        profit_tax: float - Комиссия на прибыль
        df: DataFrame - Котировки конкретного инструмента
        """

        self.capital = capital
        self.comission = comission
        self.profit_tax = profit_tax
        self.df = self.__validData(df)
        self.mer = None
        self.metrics = {}

    def Calculate(self):

        "Расчет оценочных метрик для стратегии"

        ff = self.mer.reset_index()
        ff = ff.drop(columns='index')
        ff['profit'] = ff.sell - ff.buy  # Кривая дох
        ff['total_days'] = ff.index_sell - ff.index_buy
        capital = self.capital
        ff['buys'] = None
        ff['sells'] = None
        ff['comission'] = None
        ff['profit_tax'] = None
        ff['total_profit'] = None
        ff['capital'] = None
        for i in range(len(ff)):
            count_position = capital // ff.buy[i]
            trade_buy = ff.buy[i] * count_position
            ff.buys[i] = trade_buy
            comission = trade_buy * self.comission
            trade_sell = ff.sell[i] * count_position
            ff.sells[i] = trade_sell
            comission += trade_sell * self.comission
            ff.comission[i] = comission
            ff.profit_tax[i] = (trade_sell - trade_buy) * self.profit_tax if (trade_sell - trade_buy) > 0 else 0
            ff.total_profit[i] = (trade_sell - trade_buy) - comission - ff.profit_tax[i]
            capital += ff.total_profit[i]
            ff.capital[i] = capital
        net_profit = ff.total_profit.sum()  # Чистая прибыль/убыток $
        net_profit_proc = net_profit / self.capital  # Чистая прибыль/убыток %
        PF = ff[ff['total_profit'] > 0].total_profit.sum() / abs(
            ff[ff['total_profit'] < 0].total_profit.sum()) if str(abs(
            ff[ff['total_profit'] < 0].total_profit.sum())) != 'nan' and abs(
            ff[ff['total_profit'] < 0].total_profit.sum()) != 0 else \
            ff[ff['total_profit'] > 0].total_profit.sum()                                   # Профит фактор
        APF = (ff[ff['total_profit'] > 0].total_profit.sum() - ff.total_profit.max()) / abs(
            ff[ff['total_profit'] < 0].total_profit.sum()) if str(abs(
            ff[ff['total_profit'] < 0].total_profit.sum())) != 'nan' and abs(
            ff[ff['total_profit'] < 0].total_profit.sum()) != 0 else \
            (ff[ff['total_profit'] > 0].total_profit.sum() - ff.total_profit.max())  # Достоверный профит фактор
        EQ = list(ff.capital)
        EQ.insert(0, self.capital)  # Кривая капитала чем более сглаженная тем лучше
        AVG_DD = ff[ff['total_profit'] < 0]['total_profit'].sum() / len(
            ff[ff['total_profit'] < 0]) if len(
            ff[ff['total_profit'] < 0]) != 0 else \
            ff[ff['total_profit'] < 0]['total_profit'].sum()  # Среднее падение капитала
        AVG_Trade = ff[ff['total_profit'] > 0]['total_profit'].sum() / len(
            ff[ff['total_profit'] > 0]) if len(
            ff[ff['total_profit'] > 0]) != 0 else \
            ff[ff['total_profit'] > 0]['total_profit'].sum()  # Средняя прибыьлность сделок
        P = len(ff[ff['total_profit'] > 0]) / len(ff) if len(ff) != 0 else 0  # Вероятность выигрыша > 0.5
        Mx = (P * AVG_Trade) + ((1 - P) * AVG_DD)  # Мат ожидание в $
        Mx_proc = (1 + (AVG_Trade / abs(AVG_DD))) * P - 1 if abs(AVG_DD) != 0 else  (1 + (AVG_Trade / 1)) * P - 1 # Мат ожидание в % > 0
        RF = ff.total_profit.sum() / abs(ff.total_profit.min()) \
            if str(abs(ff.total_profit.min())) != 'nan' and abs(ff.total_profit.min()) != 0 else 0  # Фактор востановления > 15
        Calmar_ratio = (ff.total_profit.sum() / (ff.total_days.sum() / 360)) / abs(
            ff.total_profit.min())  # Коэф калмара > 3
        ff['index_buy'] = list(self.df.loc[[int(i) for i in list(ff['index_buy'])]]['dt'])
        ff['index_sell'] = list(self.df.loc[[int(i) for i in list(ff['index_sell'])]]['dt'])
        self.mer = ff
        self.metrics = {
            'net_profit': net_profit,
            'net_profit_proc': net_profit_proc,
            'PF': PF,
            'APF': APF,
            'EQ': EQ,
            'AVG_DD': AVG_DD,
            'AVG_Trade': AVG_Trade,
            'P': P,
            'Mx': Mx,
            'Mx_proc': Mx_proc,
            'RF': RF,
            'Calmar_ratio': Calmar_ratio
        }
    def __getTradeTable(self, df):

        """
        Расчет сделок для расчитаных операций по активу

        df: pd.DataFrame() - таблица котировок, с обязательными столбцами buy, sell
        """

        mer = df[df['buy'] | df['sell']][['close', 'buy', 'sell']]
        mer = mer.reset_index()
        mer['index_buy'] = mer['index']
        mer['index_sell'] = mer['index']
        mer['buy'] = np.where(mer['buy'], mer['close'], None)
        mer['sell'] = np.where(mer['sell'], mer['close'], None)
        mer['sell'] = mer['sell'].shift(-1)
        mer['index_sell'] = mer['index_sell'].shift(-1)
        mer = mer.drop(columns=['index', 'close'])
        mer = mer.dropna()
        return mer

    def __validData(self, df):

        "Валидация данных"

        df.columns = ['tick', 'per', 'dt', 'time', 'open', 'high', 'low', 'close', 'vol']
        if df['per'][0] == 'D':
            df['time'] = '190000'
        df['dt'] = df['dt'].str[:4] + '-' + df['dt'].str[4:6] + '-' + df['dt'].str[6:] + ' ' + \
                   df['time'].str[:2] + ':' + df['time'].str[2:4] + ':' + df['time'].str[4:]
        df['dt'] = pd.to_datetime(df['dt'])
        return df

    # Дальше описываются конкретные торговые стратегии, примерная структура описана в методе StrategyTrend1

    def StrategyTrend1(self):

        """
        Простая пробойная стратегия:
        Покупаем при пробое сопротивления, которое строится по максимумам за последние 100 дней
        Продаем при пробое поддержки, которое также строится по минимумам за последние 100 дней
        """

        # Входные настройки
        self.mer = None

        # Торговая стратегия
        self.df['max_100'] = self.df.close.rolling(100).max()
        self.df['min_100'] = self.df.close.rolling(100).min()
        self.df['buy'] = False
        self.df['sell'] = False
        open_trend = False
        for i in range(1, len(self.df)):
            if self.df['max_100'][i] > self.df['max_100'][i - 1] and not open_trend:
                self.df.buy[i] = True
                open_trend = True
            elif self.df['min_100'][i] < self.df['min_100'][i - 1] and open_trend:
                self.df.sell[i] = True
                open_trend = False

        # Выходные настройки
        self.mer = self.__getTradeTable(self.df)

        return self

    def StrategyTrend2(self):

        """

        """

        # Входные настройки
        self.mer = None

        # Торговая стратегия
        df = self.df

        def par(x):
            y = []
            s = 0.70
            st = x[0] * s
            for i, val in enumerate(x):
                y.append(st)
                st = x[i] * s
                s = s + 0.005
            return y

        df['100MAX'] = df.close.rolling(100).max()
        df['100MIN'] = df.close.rolling(100).min()
        df['200MA'] = df.close.rolling(200).mean()
        df['buy'] = False
        df['sell'] = False
        open_position = False
        stop_close = False
        count_proboi_h = 0
        count_proboi_l = 0
        index_buy = 0
        for i in range(2, len(df)):
            if df['100MAX'][i] > df['100MAX'][i - 1]:
                if df['close'][i] > df['200MA'][i]:
                    if not open_position:
                        count_proboi_h += 1
            if open_position:
                if df['100MIN'][i] < df['100MIN'][i - 1]:
                    count_proboi_l += 1
                stop = par(list(df[index_buy:i + 1].close))
                stop_close = df.close[i] < stop[-1]
            open_rule = (count_proboi_h == 3)
            close_rule = (count_proboi_l == 3 or stop_close)
            if open_rule:
                df.buy[i] = True
                open_position = True
                count_proboi_h = 0
                count_proboi_l = 0
                index_buy = i
            elif close_rule:
                df.sell[i] = True
                open_position = False
                stop_close = False
                count_proboi_h = 0
                count_proboi_l = 0
        self.df = df
        # Выходные настройки
        self.mer = self.__getTradeTable(self.df)

        return self

    def StrategyTrend3(self):
        """
            Стратегия тестирования количество угаданных сделок
        """
        # Входные настройки
        self.mer = None
        # Торговая стратегия
        df = self.df
        df['100MAX'] = df.close.rolling(100).max()
        df['100MIN'] = df.close.rolling(100).min()
        df['200MA'] = df.close.rolling(200).mean()
        df['buy'] = False
        df['sell'] = False
        open_position = False
        close_rule = False
        count_proboi_h = 0
        sl = 0.05
        tp = 0.05
        for i in range(2, len(df)):
            if df['100MAX'][i] > df['100MAX'][i - 1]:
                if df['close'][i] > df['200MA'][i]:
                    if not open_position:
                        count_proboi_h += 1
            open_rule = (count_proboi_h == 3)
            if open_position:
                close_rule = (df.close[i] <= stop_close or df.close[i] >= teik_profit)
            if open_rule:
                df.buy[i] = True
                open_position = True
                count_proboi_h = 0
                stop_close = df.close[i] * (1 - sl)
                teik_profit = df.close[i] * (1 + tp)
            elif close_rule:
                df.sell[i] = True
                open_position = False
                close_rule = False
                count_proboi_h = 0
        self.df = df
        # Выходные настройки
        self.mer = self.__getTradeTable(self.df)
        return self

    def StrategyRandom(self):

        "Если гадалка сказала сегодня тебе повезет совершаем сделку"

        self.mer = None

        df = self.df
        open = False
        close = False
        open_pos = False
        stop = 0.05
        teik = 0.05
        df['buy'] = False
        df['sell'] = False
        for i in range(len(df)):
            num = random.randint(1,100)
            if num > 90:
                if not open_pos:
                    open = True
            if open_pos:
                close = (df.close[i] <= sl or df.close[i] >= tp)
            if open:
                df.buy[i] = True
                sl = df.close[i] * (1 - stop)
                tp = df.close[i] * (1 + teik)
                open_pos = True
                open = False
            elif close:
                df.sell[i] = True
                open_pos = False

        self.df = df
        self.mer = self.__getTradeTable(self.df)

        return self