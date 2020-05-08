from datetime import datetime

import tushare as ts

def gen_save_exchange_calendar(pro, start_date='20120101'):
    # 从TuShare读取开市日历，然后自己计算week/month/querter/year sart/end 属性
    # 然后保存在本地以供以后使用 , end_date='20181231'
    cal_dates = pro.query('trade_cal', start_date=start_date, is_open='1')
    # # exchange默认为上交所,start_date和end_date不是必填,is_open不填是全部,is_open可以使用0和1,0为不交易的日期,1为交易日
    #cal_dates = data['cal_date']
    #cal_dates = pro.trade_cal()
    cal_dates['isWeekStart'] = 0
    cal_dates['isWeekEnd'] = 0
    cal_dates['isMonthStart'] = 0
    cal_dates['isMonthEnd'] = 0
    cal_dates['isQuarterStart'] = 0
    cal_dates['isQuarterEnd'] = 0
    cal_dates['isYearStart'] = 0
    cal_dates['isYearEnd'] = 0
    previous_i = -1
    previous_open_week = -1
    previous_open_month = -1
    previous_open_year = -1

    #print(cal_dates)
    for i in cal_dates.index:
        str_date = cal_dates.loc[i]['cal_date'] #
        isOpen = cal_dates.loc[i]['is_open']# isOpen
        if not isOpen:
            continue
        date = datetime.strptime(str_date, '%Y%m%d').date()
        # 设置isWeekStart和isWeekEnd
        current_open_week = date.isocalendar()[1]
        if current_open_week != previous_open_week:
            cal_dates.ix[i, 'isWeekStart'] = 1
            if previous_open_week != -1:
                cal_dates.ix[previous_i, 'isWeekEnd'] = 1

        # 设置isMonthStart和isMonthEnd
        current_open_month = date.month
        if current_open_month != previous_open_month:
            cal_dates.ix[i, 'isMonthStart'] = 1
            if previous_open_month != -1:
                cal_dates.ix[previous_i, 'isMonthEnd'] = 1
            # 顺便根据月份设置isQuarterStart和isQuarterEnd
            if current_open_month in [1, 4, 7, 10]:
                cal_dates.ix[i, 'isQuarterStart'] = 1
                if previous_open_month != -1:
                    cal_dates.ix[previous_i, 'isQuarterEnd'] = 1
            # 有个特殊情况是交易所开始第一天应为QuarterStart
            if previous_open_month == -1:
                cal_dates.ix[i, 'isQuarterStart'] = 1

        # 设置isYearStart和isYearEnd
        current_open_year = date.year
        if current_open_year != previous_open_year:
            cal_dates.ix[i, 'isYearStart'] = 1
            if previous_open_year != -1:
                cal_dates.ix[previous_i, 'isYearEnd'] = 1

        previous_i = i
        previous_open_week = current_open_week
        previous_open_month = current_open_month
        previous_open_year = current_open_year

    return cal_dates

if __name__ == '__main__':
    str_date = '19901219'
    date = datetime.strptime(str_date, '%Y%m%d').date()
    print(date)