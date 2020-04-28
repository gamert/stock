#获取5日，10日，20日均线数据
from datetime import datetime

import jqdatasdk
import numpy
import tushare as ts
import talib
from QUANTAXIS import QA_Setting
from matplotlib import pyplot as plt
import pandas as pd

import baostock as bs

from jqdatasdk import *


from QUANTAXIS.QAFetch import QATdx as QATdx
import QUANTAXIS as QA

# 合并
## pd.concat([df2,df3],axis=0,join='inner')
## pd.concat([df2,df3],axis=1,join='inner')
# df_merge =df1.merge(df3,on=['a','b'])

pd.set_option('display.max_columns', None)
#pd.set_option('max_colwidth',100)
pd.set_option('display.max_colwidth',1500)
pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

method = 3 # 0 tushare 1 Bao 2 jqdatasdk 3 tdx
pro = ts.pro_api()
dat = pro.query('stock_basic', fields='symbol,name')


if method == 1:
    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

if method == 2:
    jqdatasdk.auth("13761609001", "0")
    is_auth = is_auth()
    print(is_auth)
    print(jqdatasdk.__version__)
    print(get_query_count())

if method == 3:
    setting = QA_Setting()
    setting.env_config()

def get_name(stoke_code):
    '''通过股票代码导出公司名称'''
    company_name = list(dat.loc[dat['symbol'] == stoke_code].name)[0]
    return company_name

def _MA(closed, count):
    # 算术移动平均线
    # return talib.MA(closed.values,timeperiod=count, matype=0)
    return QA.MA(closed, count)

def Fetch5_20(codes, ktype='D', start='2020-01-01',end='2020-04-15'):
    # newdf = pd.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)
    newdf = pd.DataFrame(columns=['date', 'open', 'close', 'high', 'low', 'volume', 'code', 'ma5', 'ma10', 'ma20', 'ma60', 'ma120', 'ma250'])
    names = []
    duotou = []
    #newdf = nil
    for code in codes:
        names.append(get_name(code))
        # 通过tushare获取股票信息
        if method == 0:
            # df = pro.query()
            stock_fields = "trade_date, open, close, high, low, vol , ts_code "
            code_wm = code.startswith("6") and code+".SH" or code+".SZ";
            # df = pro.share_float(ts_code=code_wm, start_date=start, end_date=end, fields=stock_fields)
            #df = ts.pro_bar(ts_code=code_wm, adj='qfq', freq=ktype, start_date=start.replace("-", ""), end_date=end.replace("-",""))
            ## 更新时间晚于21:30
            df = ts.get_k_data(code,ktype=ktype, start=start, end=end)
            closed = df['close']
        # 提取收盘价
        elif method == 1:
            bcode = code.startswith("6") and "sh."+code or "sz."+code;
            rs = bs.query_history_k_data_plus(bcode,
                                              "date,open,high,low,close,volume,code",
                                              start_date=start, end_date=end,
                                              frequency=ktype, adjustflag="3")
            #### 打印结果集 ####
            data_list = []
            while (rs.error_code == '0') & rs.next():
                # 获取一条记录，将记录合并在一起
                data_list.append(rs.get_row_data())
            df = pd.DataFrame(data_list, columns=rs.fields)
            df['close'] = df['close'].apply(lambda x: float(x))
            # print('query_history_k_data_plus respond error_code:' + rs.error_code)
            # print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)
            closed = df['close']
            #closed = numpy.array([float(x) for x in closed])
        elif method == 2:
            # 根据时间计算bars个数
            if ktype=='D':
                count = 250*5;
                unit = '1d'
            if ktype=='W':
                count = 250*5*5;
                unit = '1w'
            code_wm = code.startswith("6") and code+".XSHG" or code+".XSHE";
            df = get_bars(code_wm, count, unit=unit, fields=['date', 'open', 'high', 'low', 'close', 'volume'], include_now=False,
                          end_dt=end)
            closed = df['close']
        elif method == 3:
            # df = QATdx.QA_fetch_get_stock_day(code,start, end, if_fq='00', frequence=ktype)
            # if ktype in ['day', 'd', 'D', 'DAY', 'Day']:
            df = QA.QA_fetch_stock_day_adv(code, start, end)
            df = df.to_qfq()
            if ktype in ['w', 'W', 'Week', 'week']:
                df = df.resample('w')
            else:
                df = df.data
            #closed = df.__getitem__('close')
            # df = df.drop('date_stamp', axis=1)
            # df = df.drop('vol', axis=1)
            # df = df.drop('amount', axis=1)
            closed = df['close']


        # 获取均线的数据，通过timeperiod参数来分别获取 5,10,20 日均线的数据。
        # talib.SMA
        ma5 = _MA(closed, 5)
        ma10 = _MA(closed, 10)
        ma20 = _MA(closed, 20)
        ma60 = _MA(closed, 60)
        ma120 = _MA(closed, 120)
        ma250 = _MA(closed, 250)

        if(ma250[-1]<ma120[-1] and ma120[-1] < ma60[-1] and ma60[-1] < ma20[-1] and ma20[-1] < ma10[-1] and ma10[-1] < ma5[-1]):
            duotou.append("多")
        else:
            duotou.append("")

        df['ma5'] = ma5
        df['ma10'] = ma10
        df['ma20'] = ma20
        df['ma60'] = ma60
        df['ma120'] = ma120
        df['ma250'] = ma250
        # print(df) #.iloc [-1,]
        newdf = newdf.append(df.iloc [-1,])
        # else:
        #     newdf = df.iloc [-1,]

    # 补全名字
    newdf = newdf.drop('date',axis=1)
    newdf = newdf.drop('open',axis=1)
    newdf = newdf.drop('code',axis=1)
    newdf = newdf.drop('high',axis=1)
    newdf = newdf.drop('low',axis=1)
    newdf = newdf.drop('volume',axis=1)
    newdf.insert(0, 'name', names)
    newdf['duotou'] = duotou


    #print(newdf.to_html())
    print(newdf)
    # 选择保存
    ## df.to_csv('c:/day/000875.csv', columns=['open', 'high', 'low', 'close'])

    # 打印出来每一个数据
    # print(closed)
    # print(ma5)
    # print(ma10)
    # print(ma20)

    #通过plog函数可以很方便的绘制出每一条均线
# plt.plot(closed)
# plt.plot(ma5)
# plt.plot(ma10)
# plt.plot(ma20)
#     #添加网格，可有可无，只是让图像好看点
# plt.grid()
#     #记得加这一句，不然不会显示图像
# plt.show()


if __name__ == '__main__':
    # "002400",
    codes = [
        "300002", # 300002 神州泰岳
        "600845",# 600845 宝信软件
        "002157",# 002157 正邦科技
        "002727",# 002727 一心堂
        "300142",# 300142 沃森生物
        "300168",# 300168 万达信息
        "300558",# 300558 贝达药业
        "603882", # 603882 金域医学
        "002568"# 002568 百润股份
    ]
    print("日")
    today = datetime.now().strftime('%Y-%m-%d') #
    Fetch5_20(codes,start='2018-01-01', end=today)
    print("周")
    Fetch5_20(codes, ktype='W', start='2012-01-01', end=today)
