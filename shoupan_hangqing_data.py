import tushare as ts
import pandas as pd
import time
import os

# 指定自己要存放文件的绝对路径
os.chdir('G:/_Stock/_all_trading_data/Every_days_analysis/input_data')
pd.set_option('expand_frame_repr', False)


# 从tushare获取指定日期
def get_today_all_ts(date):
    date_now = date

    # 先获得所有股票的收盘数据
    df_close = ts.get_today_all()

    # 获得所有股票的基本信息
    df_basics = ts.get_stock_basics()
    df_all = pd.merge(left=df_close, right=df_basics, on='name', how='outer')

    df_all['code'] = df_all['code'].astype(str) + ' '

    # 保存数据
    df_all.to_csv(str(date_now) + '_ts.csv', index=False, encoding='gbk')
    print('%s is downloaded.' % (str(date_now)))
    print(df_all)
    return df_all

if __name__ == '__main__':
    get_today_all_ts(date='20200410')
