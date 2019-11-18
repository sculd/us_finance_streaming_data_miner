import json, os
from pytz import timezone
import us_finance_streaming_data_miner.ingest.streaming.aggregation
from us_finance_streaming_data_miner.ingest.streaming.aggregation import AggregationsRun
import websockets
from us_finance_streaming_data_miner import util
import us_finance_streaming_data_miner.util.logging as logging
import us_finance_streaming_data_miner.util.symbols

_TZ_US_EAST = timezone('US/EASTERN')
_WEB_SOCKET_BASE_ADDRESS = 'wss://socket.polygon.io/stocks'
_URL_BASE = 'https://api.polygon.io/v2'
_QUERY_PATH_INTRADAY_PRICE  = '/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}?apiKey={apiKey}'
_API_KEY = os.environ['API_KEY_POLYGON']

_cnt_T = 0
_cnt_A = 0
_cnt_MA = 0

def get_auth_msg():
    return json.dumps({
        "action":"auth",
        "params": _API_KEY
    })

def get_subscribe_msg():
    symbols = us_finance_streaming_data_miner.util.symbols.get_symbols_nasdaq()
    params_value = ','.join(map(lambda s: 'A.' + s, symbols))
    #params_value = 'T.AAPL,T.GOOG'

    return json.dumps({
        "action":"subscribe",
        "params": params_value
    })

async def run(polygon_aggregations_run):
    async with websockets.connect(_WEB_SOCKET_BASE_ADDRESS) as websocket:
        greeting = await websocket.recv()
        print(f"< {greeting}")

        await websocket.send(get_auth_msg())
        msg = await websocket.recv()
        print(f"< {msg}")

        await websocket.send(get_subscribe_msg())
        while True:
            msg = await websocket.recv()
            on_messages(polygon_aggregations_run, msg)

def _on_status_message(polygon_aggregations_run, msg):
    print(f"< (status) {msg}")

def _t_msg_to_trade(msg):
    keys = ['sym', 'p', 's', 't']
    for key in keys:
        if key not in msg:
            logging.errror('"{key}" field not present in the message: {msg}'.format(key=key, msg=msg))
    symbol = msg['sym']
    price = msg['p']
    volume = msg['s']
    timestamp_milli = int(msg['t'])
    timestamp_second = timestamp_milli // 1000
    trade = us_finance_streaming_data_miner.ingest.streaming.aggregation.Trade(timestamp_second, symbol, price, volume)
    return trade

def _a_msg_to_trade(msg):
    keys = ['sym', 'c', 'v', 's']
    for key in keys:
        if key not in msg:
            raise Exception('"{key}" field not present in the message: {msg}'.format(key=key, msg=msg))

    symbol = msg['sym']
    price = msg['c']
    volume = msg['v']
    timestamp_milli = int(msg['s'])
    timestamp_second = timestamp_milli // 1000
    trade = us_finance_streaming_data_miner.ingest.streaming.aggregation.Trade(timestamp_second, symbol, price, volume)
    return trade

def _on_T_message(polygon_aggregations_run, msg):
    global _cnt_T
    _cnt_T += 1
    print(f"< (T) {msg}, _cnt_T: {_cnt_T}, _cnt_AM: {_cnt_AM}, _cnt_A: {_cnt_A}")
    trade = _t_msg_to_trade(msg)
    polygon_aggregations_run.on_trade(trade)

def _on_Q_message(polygon_aggregations_run, msg):
    print(f"< (Q) {msg}")

def _on_A_message(polygon_aggregations_run, msg):
    global _cnt_A
    _cnt_A += 1
    print(f"< (A) {msg}")
    trade = _a_msg_to_trade(msg)
    polygon_aggregations_run.on_trade(trade)

def _on_AM_message(polygon_aggregations_run, msg):
    global _cnt_AM
    _cnt_AM += 1
    print(f"< (AM) {msg}")

def _on_undefined_message(polygon_aggregations_run, msg):
    print(f"< (undefined) {msg}")

def on_message(polygon_aggregations_run, msg):
    print(f"< (undefined) {msg}")

    if not msg:
        logging.errror('the message is not valid')

    if 'ev' not in msg:
        logging.errror('"ev" field not present in the message: {msg}'.format(msg=msg))
    ev = msg['ev']
    if ev == 'status':
        _on_status_message(polygon_aggregations_run, msg)
    elif ev == 'T':
        _on_T_message(polygon_aggregations_run, msg)
    elif ev == 'Q':
        _on_Q_message(polygon_aggregations_run, msg)
    elif ev == 'A':
        _on_A_message(polygon_aggregations_run, msg)
    elif ev == 'AM':
        _on_AM_message(polygon_aggregations_run, msg)
    else:
        _on_undefined_message(polygon_aggregations_run, msg)

def on_messages(polygon_aggregations_run, msg_strs):
    if not msg_strs:
        logging.errror('the message is not valid')

    msgs = json.loads(msg_strs)
    for msg in msgs:
        on_message(polygon_aggregations_run, msg)

import asyncio
class PolygonAggregationsRun(AggregationsRun):
    def __init__(self):
        super(PolygonAggregationsRun, self).__init__()
        asyncio.get_event_loop().run_until_complete(run(self))

