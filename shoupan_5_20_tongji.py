#获取5日，10日，20日均线数据

import tushare as ts
import talib
from matplotlib import pyplot as plt
import pandas as pd

pro = ts.pro_api()
dat = pro.query('stock_basic', fields='symbol,name')

def get_name(stoke_code):
    '''通过股票代码导出公司名称'''
    company_name = list(dat.loc[dat['symbol'] == stoke_code].name)[0]
    return company_name

pd.set_option('display.max_columns', None)
#pd.set_option('max_colwidth',100)
pd.set_option('display.max_colwidth',1500)
pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

def Fetch5_20(codes, ktype='D', start='2020-01-01',end='2020-04-14'):
    # newdf = pd.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)
    newdf = pd.DataFrame(columns=['date', 'open', 'close', 'high', 'low', 'volume', 'code', 'ma5', 'ma10', 'ma20', 'ma60', 'ma120', 'ma250'])
    names = []
    #newdf = nil
    for code in codes:
        names.append(get_name(code))
        # 通过tushare获取股票信息
        df = ts.get_k_data(code,ktype=ktype, start=start,
                           end=end)  # 以股票代码[601888]中国国旅为例，提取从2018-01-12到2018-10-30的收盘价
        # 提取收盘价
        closed = df['close'].values
        # 获取均线的数据，通过timeperiod参数来分别获取 5,10,20 日均线的数据。
        ma5 = talib.SMA(closed, timeperiod=5)
        ma10 = talib.SMA(closed, timeperiod=10)
        ma20 = talib.SMA(closed, timeperiod=20)
        ma60 = talib.SMA(closed, timeperiod=60)
        ma120 = talib.SMA(closed, timeperiod=120)
        ma250 = talib.SMA(closed, timeperiod=250)

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
    newdf = newdf.drop('open',axis=1)
    #newdf = newdf.drop('close',axis=1)
    newdf = newdf.drop('high',axis=1)
    newdf = newdf.drop('low',axis=1)
    newdf = newdf.drop('volume',axis=1)
    newdf.insert(3, 'name', names)


    #print(newdf.to_html())
    print(newdf)
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
    codes = ["002157", "002400", "002727", "300142", "300168", "300558",]
    print("日")
    Fetch5_20(codes,start='2018-01-01', end='2020-04-15')
    print("周")
    Fetch5_20(codes, ktype='W', start='2012-01-01', end='2020-04-15')
