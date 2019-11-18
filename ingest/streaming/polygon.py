import json, os
from pytz import timezone
import ingest.streaming.aggregation
import websockets
import util.symbols
import util.logging as logging

_TZ_US_EAST = timezone('US/EASTERN')
_WEB_SOCKET_BASE_ADDRESS = 'wss://socket.polygon.io/stocks'
_URL_BASE = 'https://api.polygon.io/v2'
_QUERY_PATH_INTRADAY_PRICE  = '/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}?apiKey={apiKey}'
_API_KEY = os.environ['API_KEY_POLYGON']

_aggregations = ingest.streaming.aggregation.Aggregations()

def get_auth_msg():
    return json.dumps({
        "action":"auth",
        "params": _API_KEY
    })

def get_subscribe_msg():
    symbols = util.symbols.get_symbols_nasdaq()
    params_value = ','.join(map(lambda s: 'T.' + s, symbols))
    #params_value = 'T.AAPL,T.GOOG'

    return json.dumps({
        "action":"subscribe",
        "params": params_value
    })

async def run():
    async with websockets.connect(_WEB_SOCKET_BASE_ADDRESS) as websocket:
        greeting = await websocket.recv()
        print(f"< {greeting}")

        await websocket.send(get_auth_msg())
        msg = await websocket.recv()
        print(f"< {msg}")

        await websocket.send(get_subscribe_msg())
        while True:
            msg = await websocket.recv()
            on_messages(msg)

def _on_status_message(msg):
    print(f"< (status) {msg}")

def _on_T_message(msg):
    keys = ['sym', 'p', 's', 't']
    for key in keys:
        if key not in msg:
            logging.errror('"{key}" field not present in the message: {msg}'.format(key=key, msg=msg))
    symbol = msg['sym']
    price = msg['p']
    volume = msg['s']
    timestamp_milli = int(msg['t'])
    timestamp_second = timestamp_milli // 1000
    print(f"< (T) {msg}")
    trade = ingest.streaming.aggregation.Trade(timestamp_second, symbol, price, volume)
    _aggregations.on_trade(trade)

def _on_Q_message(msg):
    print(f"< (Q) {msg}")

def _on_A_message(msg):
    print(f"< (A) {msg}")

def _on_AM_message(msg):
    print(f"< (AM) {msg}")

def _on_undefined_message(msg):
    print(f"< (undefined) {msg}")

def on_message(msg):
    if not msg:
        logging.errror('the message is not valid')

    if 'ev' not in msg:
        logging.errror('"ev" field not present in the message: {msg}'.format(msg=msg))
    ev = msg['ev']
    if ev == 'status':
        _on_status_message(msg)
    elif ev == 'T':
        _on_T_message(msg)
    elif ev == 'Q':
        _on_Q_message(msg)
    elif ev == 'A':
        _on_A_message(msg)
    elif ev == 'AM':
        _on_AM_message(msg)
    else:
        _on_undefined_message(msg)

def on_messages(msg_strs):
    if not msg_strs:
        logging.errror('the message is not valid')

    msgs = json.loads(msg_strs)
    for msg in msgs:
        on_message(msg)
