import json, os
from google.cloud import pubsub_v1
from pytz import timezone
import us_finance_streaming_data_miner.ingest.streaming.aggregation
from us_finance_streaming_data_miner.ingest.streaming.aggregation import AggregationsRun, Aggregations
from threading import Thread
import us_finance_streaming_data_miner.util.logging as logging
import us_finance_streaming_data_miner.util.symbols

_cnt_T = 0
_cnt_A = 0
_cnt_MA = 0

def run_loop(polygon_aggregations_run):
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    subscription_id = os.getenv('FINANCE_STREAM_PUBSUB_SUBSCRIPTION_ID')

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project_id, subscription_id
    )

    def callback(message):
        msg_str = json.loads(message.data.decode('utf-8'))
        message.ack()
        on_message(polygon_aggregations_run, json.loads(msg_str)[0])

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback
    )
    print("Listening for messages on {}\n".format(subscription_path))

    try:
        streaming_pull_future.result()
    except Exception as ex:  # noqa
        logging.error(ex)
        streaming_pull_future.cancel()

def run_mock_loop(polygon_aggregations_run):
    pass

def _on_status_message(polygon_aggregations_run, msg):
    print("< (status) {msg}".format(msg=msg))

def _t_msg_to_trade(msg):
    keys = ['sym', 'p', 's', 't']
    for key in keys:
        if key not in msg:
            print('"{key}" field not present in the message: {msg}'.format(key=key, msg=msg))
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
    trade = _t_msg_to_trade(msg)
    polygon_aggregations_run.on_trade(trade)

def _on_Q_message(polygon_aggregations_run, msg):
    print("< (Q) {msg}".format(msg=msg))

def _on_A_message(polygon_aggregations_run, msg):
    global _cnt_A
    _cnt_A += 1
    if _cnt_A % 100 == 0:
        print("< (A) {msg}".format(msg=msg))
    trade = _a_msg_to_trade(msg)
    polygon_aggregations_run.on_trade(trade)

def _on_AM_message(polygon_aggregations_run, msg):
    global _cnt_AM
    _cnt_AM += 1
    print("< (AM) {msg}".format(msg=msg))

def _on_undefined_message(polygon_aggregations_run, msg):
    print("< (undefined) {msg}".format(msg=msg))
    logging.error("< (undefined) {msg}".format(msg=msg))

def on_message(polygon_aggregations_run, msg):
    if not msg:
        logging.error('the message is not valid')

    if 'ev' not in msg:
        logging.error('"ev" field not present in the message: {msg}'.format(msg=msg))
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

class PolygonAggregationsMockRun(AggregationsRun):
    def __init__(self):
        super(PolygonAggregationsMockRun, self).__init__()
        Thread(target=run_mock_loop, args=(self,)).start()

class PolygonAggregationsRun(AggregationsRun):
    def __init__(self, aggregations = None):
        super(PolygonAggregationsRun, self).__init__()
        self.aggregations = aggregations if aggregations else Aggregations()
        Thread(target=run_loop, args=(self,)).start()
