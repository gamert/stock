import pandas as pd
import requests
import tushare as ts
import os
import time
import warnings

warnings.filterwarnings('ignore')
pd.set_option('expand_frame_repr', False)
os.chdir('G:/_Stock/_all_trading_data/Every_days_analysis/')
date_now = '20200410'
print(str(date_now))
# ====龙虎榜数据


# 读取TDX的每日股票数据
df = pd.read_csv('input_data/{}_ts.csv'.format(str(date_now)), encoding='gbk')
df.fillna(0, inplace=True)
df.replace('nan ', 0, inplace=True)
df['timeToMarket'] = pd.to_datetime(df['timeToMarket'])

df[['changepercent', 'pe', 'mktcap', 'nmc']] = df[['changepercent', 'pe', 'mktcap', 'nmc']].astype(float)
df['timeToMarket'] = pd.to_datetime(df['timeToMarket'])

df['code'] = df['code'].astype(str)  # 转化成str后，NAN也变成nan str格式
# 添加 交易所 列
df.loc[df['code'].str.startswith('3'), 'exchange'] = 'CY'
df.loc[df['code'].str.startswith('6'), 'exchange'] = 'SH'
df.loc[df['code'].str.startswith('0'), 'exchange'] = 'SZ'

# 找出上涨的股票
df_up = df[df['changepercent'] > 0.00]
# 走平股数
df_even = df[df['changepercent'] == 0.00]
# 找出下跌的股票
df_down = df[df['changepercent'] < 0.00]

# 找出涨停的股票
limit_up = df[df['changepercent'] >= 9.70]
limit_down = df[df['changepercent'] <= -9.70]

# 涨停股数中的未封板股，上市日期小于15天
limit_up_new = limit_up[pd.to_datetime(date_now) - limit_up['timeToMarket'] <= pd.Timedelta(15)]
# 涨停股数中次新股，上市日期小于1年
limit_up_fresh = limit_up[pd.to_datetime(date_now) - limit_up['timeToMarket'] <= pd.Timedelta(365)]

# 涨停股数中的未封板股，上市日期小于15天
limit_down_new = limit_down[pd.to_datetime(date_now) - limit_down['timeToMarket'] <= pd.Timedelta(15)]
# 涨停股数中次新股，上市日期小于1年
limit_down_fresh = limit_down[pd.to_datetime(date_now) - limit_down['timeToMarket'] <= pd.Timedelta(365)]

print('A股上涨个数： %d, A股下跌个数： %d, A股走平个数: %d。' % (df_up.shape[0], df_down.shape[0], df_even.shape[0]))
print('A股总成交额：%d, 总成交量：%d' % (df['amount'].sum(), df['volume'].sum()))
print('A股平均市盈率：%.2f， 平均流通市值 %.2f 亿, 平均总市值 %.2f 亿' % (df['pe'].mean(), df['nmc'].mean(), df['mktcap'].mean()))
print('涨停数量：%d 个, 涨停中上市日期小于15天的：%d, 涨停中上市日期小于1年的：%d' % (
limit_up.shape[0], limit_up_new.shape[0], limit_up_fresh.shape[0]))
print('跌停数量：%d 个, 涨停中上市日期小于15天的：%d, 涨停中上市日期小于1年的：%d' % (
limit_down.shape[0], limit_down_new.shape[0], limit_down_fresh.shape[0]))


# 获取指定列的分析统计结果
def get_output(df, columns='industry', name='_limit_up'):
    df = df.copy()

    output = pd.DataFrame()
    output = pd.DataFrame(df.groupby(columns)['code'].count())

    output['pe_mean'] = df.groupby(columns)['pe'].mean()
    output['pe_median'] = df.groupby(columns)['pe'].median()

    output['nmc_mean'] = df.groupby(columns)['nmc'].mean()
    output['nmc_median'] = df.groupby(columns)['nmc'].median()

    output['mktcap_mean'] = df.groupby(columns)['mktcap'].mean()
    output['mktcap_median'] = df.groupby(columns)['mktcap'].median()

    output['volume_mean'] = df.groupby(columns)['volume'].mean()
    output['volume_median'] = df.groupby(columns)['volume'].median()

    output['amount_mean'] = df.groupby(columns)['amount'].mean()
    output['amount_median'] = df.groupby(columns)['amount'].median()

    output.sort_values('code', ascending=False, inplace=True)
    output.rename(columns={'code': name + '_count'}, inplace=True)
    return output

for i in ['industry', 'exchange', 'area']:
    # 对涨停的股票分析
    output_limit_up = get_output(limit_up, columns=i, name='limit_up')
    print(output_limit_up)
    # 对跌停的股票分析
    output_limit_down = get_output(limit_down, columns=i, name='limit_down')
    print(output_limit_down)
    # 对全量的股票分析
    output_total = get_output(df, columns=i, name='total')
    print(output_total)
