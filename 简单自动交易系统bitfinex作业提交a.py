from datetime import datetime, timedelta
import pandas as pd
from time import sleep
import ccxt
from program.class9.TradeBitfinex import next_run_time, place_order, get_bitfinex_candle_data, auto_send_email
from program.class8.Signals import signal_moving_average, signal_bolling_with_stop_lose, signal_bolling
from program.dingding import send_ding_msg

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

"""
自动交易主要流程
 
# 通过while语句，不断的循环

# 每次循环中需要做的操作步骤
    1. 更新账户信息
    2. 获取实时数据
    3. 根据最新数据计算买卖信号 
    4. 根据目前仓位、买卖信息，结束本次循环，或者进行交易
    5. 交易
"""

# =====参数
time_interval = '5m'  # 间隔运行时间，不能低于5min

# =====创建bitfinex交易所

bitfinex = ccxt.bitfinex()
bitfinex.apiKey = ''  # mot
bitfinex.secret = ''

bitfinex2 = ccxt.bitfinex2()
bitfinex2.apiKey = ''
bitfinex2.secret = ''
# exchange = bitfinex
exchange = bitfinex2

symbol = 'ETH/USDT'  # 交易品种
base_coin = symbol.split('/')[-1]
trade_coin = symbol.split('/')[0]

para = [100, 3]  # 策略参数

balance = bitfinex2.fetch_balance()['info']

# print(balance)
#
# position_info = bitfinex2.private_post_auth_r_positions()
# for i in position_info:
#     if (i[0] == 'tEOSUSD'):
#         eth_amount = i[2]
#         print(eth_amount)
#         print(position_info)
#         a=eth_amount
#         print(a)
# exit()

# for i in balance:
#     if i[0] == 'margin' and i[1] == 'USD':
#         usd_amount = i[2]
#
# for i in balance:
#     if i[0] == 'margin' and i[1] == 'ETH':
#         eth_amount = i[2]
#
# print(usd_amount)
# exit()

# =====主程序
while True:

    # ===基本参数
    leverage_rate = 3  # bfx交易所最多提供3倍杠杆，leverage_rate可以在(0, 3]区间选择
    # init_cash = 100  # 初始资金
    # init_cash = usd_amount  # 初始资金
    c_rate = 2.0 / 1000  # 手续费
    min_margin_rate = 0.15  # 最低保证金比例，必须占到借来资产的15%
    # min_margin = init_cash * leverage_rate * min_margin_rate  # 最低保证金

    #     # # ===sleep直到运行时间

    run_time = next_run_time(time_interval)
    print(run_time)
    # exit()
    # sleep(max(0, (run_time - datetime.now()).seconds))
    # while True:  # 在靠近目标时间时
    #     if datetime.now() < run_time:
    #         continue
    #     else:
    #         break

    # ===获取最新数据
    while True:
        # 获取数据
        df = get_bitfinex_candle_data(exchange, symbol, time_interval)
        # print(df)
        # exit()
        # 判断是否包含最新的数据
        _temp = df[df['candle_begin_time_GMT8'] == (run_time - timedelta(minutes=int(time_interval.strip('m'))))]
        if _temp.empty:
            print('获取数据不包含最新的数据，重新获取')
            continue
        else:
            break

    # ===产生交易信号
    df = df[df['candle_begin_time_GMT8'] < pd.to_datetime(run_time)]  # 去除target_time周期的数据
    df = signal_bolling(df, para=para)
    # print(df)
    # exit()
    signal = df.iloc[-1]['signal']
    pos = df.iloc[-1]['pos']

    print('\n交易信号', signal)
    # exit()

    # ===第一种情况==做多买入品种

    if pos == 0 and signal == 1:
        print('\n做多买入')

        balance = bitfinex2.fetch_balance()['info']

        for i in balance:
            if i[0] == 'margin' and i[1] == 'USD':
                usd_amount = i[2]

        for i in balance:
            if i[0] == 'margin' and i[1] == 'ETH':
                eth_amount = i[2]

        # 获取最新的买入价格
        price = exchange.fetch_ticker(symbol)['ask']  # 获取卖一价格
        # 计算买入数量
        init_cash = usd_amount
        min_margin = init_cash * leverage_rate * min_margin_rate  # 最低保证金
        buy_amount_long = (init_cash * leverage_rate - min_margin) / price

        # 下单
        place_order(exchange, order_type='limit', buy_or_sell='buy', symbol=symbol, price=price * 1.02,
                    amount=buy_amount_long)
        # 钉钉标题
        email_title += '_买入_' + trade_coin
        # 钉钉内容
        email_content += '买入信息：\n'
        email_content += '买入数量：' + str(buy_amount) + '\n'
        email_content += '买入价格：' + str(price) + '\n'
        content = email_content
        send_ding_msg(content)

    # ===第二种情况==做多平仓品种

    if pos == 1 and signal == 0:
        print('\n做多平仓卖出')
        position_info = bitfinex2.private_post_auth_r_positions()
        for i in position_info:
            if (i[0] == 'tETHUSD'):
                eth_amount = i[2]

                  # 获取最新的卖出价格
        price = exchange.fetch_ticker(symbol)['bid']  # 获取买一价格
        # print(price)
        # exit()
        # 计算卖出数量

        amount = eth_amount

        # 下单
        place_order(exchange, order_type='limit', buy_or_sell='sell', symbol=symbol, price=price * 0.98, amount=amount)
        # 钉钉标题
        email_title += '_卖出_' + trade_coin
        # 钉钉内容
        email_content += '卖出信息：\n'
        email_content += '卖出数量：' + str(trade_coin_amount) + '\n'
        email_content += '卖出价格：' + str(price) + '\n'
        # print('当前资产:\n', base_coin, base_coin_amount, trade_coin, trade_coin_amount)

        content = email_content
        send_ding_msg(content)
        # exit()

    # ===第三种情况==做多穿下轨平仓开空仓品种

    if pos == 1 and signal == -1:
        print('\n做多平仓开空仓')
        position_info = bitfinex2.private_post_auth_r_positions()
        for i in position_info:
            if (i[0] == 'tETHUSD'):
                eth_amount = i[2]

            # 先平仓卖出==获取最新的卖出价格
        price = exchange.fetch_ticker(symbol)['bid']  # 获取买一价格
        # print(price)
        # exit()
        # 计算卖出数量

        amount = eth_amount

        # 下单
        place_order(exchange, order_type='limit', buy_or_sell='sell', symbol=symbol, price=price * 0.98, amount=amount)

        # 再开空仓

        # 获取最新的卖出价格
        price = exchange.fetch_ticker(symbol)['bid']  # 获取买一价格
        # print(price)
        # exit()
        # 计算卖出数量

        balance = bitfinex2.fetch_balance()['info']

        for i in balance:
            if i[0] == 'margin' and i[1] == 'USD':
                usd_amount = i[2]
        init_cash = usd_amount
        sell_amount_short = (init_cash * leverage_rate - min_margin) / price

        # 下单
        place_order(exchange, order_type='limit', buy_or_sell='sell', symbol=symbol, price=price * 0.98,
                    amount=sell_amount_short)

        # 钉钉标题
        email_title += '_卖出_' + trade_coin
        # 钉钉内容
        email_content += '卖出信息：\n'
        email_content += '卖出数量：' + str(trade_coin_amount) + '\n'
        email_content += '卖出价格：' + str(price) + '\n'
        # print('当前资产:\n', base_coin, base_coin_amount, trade_coin, trade_coin_amount)

        content = email_content
        send_ding_msg(content)
        # exit()

    # ===第四种情况==做空卖出品种

    if pos == 0 and signal == -1:

        print('\n做空卖出')

        balance = bitfinex2.fetch_balance()['info']

        for i in balance:
            if i[0] == 'margin' and i[1] == 'USD':
                usd_amount = i[2]

        for i in balance:
            if i[0] == 'margin' and i[1] == 'ETH':
                eth_amount = i[2]

        # 获取最新的卖出价格
        price = exchange.fetch_ticker(symbol)['bid']  # 获取买一价格
        # print(price)
        # exit()
        # 计算卖出数量
        init_cash = usd_amount
        sell_amount_short = (init_cash * leverage_rate - min_margin) / price

        # 下单
        place_order(exchange, order_type='limit', buy_or_sell='sell', symbol=symbol, price=price * 0.98,
                    amount=sell_amount_short)
        # 钉钉标题
        email_title += '_卖出_' + trade_coin
        # 钉钉内容
        email_content += '卖出信息：\n'
        email_content += '卖出数量：' + str(trade_coin_amount) + '\n'
        email_content += '卖出价格：' + str(price) + '\n'
        # print('当前资产:\n', base_coin, base_coin_amount, trade_coin, trade_coin_amount)

        content = email_content
        send_ding_msg(content)
        # exit()

    # ===第五种情况==做空平仓品种
    if pos == -1 and signal == 0:
        print('\n做空平仓买入')
        position_info = bitfinex2.private_post_auth_r_positions()
        for i in position_info:
            if (i[0] == 'tETHUSD'):
                eth_amount = i[2]

        # 获取最新的买入价格
        price = exchange.fetch_ticker(symbol)['ask']  # 获取卖一价格

        # 计算买入数量

        buy_amount_short = eth_amount
        # 下单
        place_order(exchange, order_type='limit', buy_or_sell='buy', symbol=symbol, price=price * 1.02,
                    amount=buy_amount_short)
        # 钉钉标题
        email_title += '_买入_' + trade_coin
        # 钉钉内容
        email_content += '买入信息：\n'
        email_content += '买入数量：' + str(buy_amount) + '\n'
        email_content += '买入价格：' + str(price) + '\n'
        content = email_content
        send_ding_msg(content)

    # ===第六种情况==做空穿上轨平仓开多仓品种

    if pos == -1 and signal == 1:
        print('\n做空平仓开多仓')

        # 先平仓
        position_info = bitfinex2.private_post_auth_r_positions()
        for i in position_info:
            if (i[0] == 'tETHUSD'):
                eth_amount = i[2]

        # 获取最新的买入价格
        price = exchange.fetch_ticker(symbol)['ask']  # 获取卖一价格

        # 计算买入数量

        buy_amount_short = eth_amount
        # 下单
        place_order(exchange, order_type='limit', buy_or_sell='buy', symbol=symbol, price=price * 1.02,
                    amount=buy_amount_short)

        # 再开多仓

        balance = bitfinex2.fetch_balance()['info']

        for i in balance:
            if i[0] == 'margin' and i[1] == 'USD':
                usd_amount = i[2]

        for i in balance:
            if i[0] == 'margin' and i[1] == 'ETH':
                eth_amount = i[2]

                # 获取最新的买入价格
                price = exchange.fetch_ticker(symbol)['ask']  # 获取卖一价格
                # 计算买入数量
                buy_amount = eth_amount
                # 获取最新的卖出价格
                place_order(exchange, order_type='limit', buy_or_sell='buy', symbol=symbol, price=price * 1.02,
                            amount=buy_amount)

            # 钉钉标题
        email_title += '_卖出_' + trade_coin
        # 钉钉内容
        email_content += '卖出信息：\n'
        email_content += '卖出数量：' + str(trade_coin_amount) + '\n'
        email_content += '卖出价格：' + str(price) + '\n'
        # print('当前资产:\n', base_coin, base_coin_amount, trade_coin, trade_coin_amount)

        content = email_content
        send_ding_msg(content)
        # exit()

    # =====发送钉钉
    # 每个半小时发送钉钉
    if run_time.minute % 30 == 0:
        # # 发送dingding
        content = email_content
        send_ding_msg(content)
    # =====本次交易结束
    print(email_title)
    print(email_content)
    print('=====本次运行完毕\n')
    sleep(60 * 1)
