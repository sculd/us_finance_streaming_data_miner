from google.cloud import logging

LOG_NAME = 'us_finance_streaming_data_miner'

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = logging.Client()
    return _client

def get_logger(log_name=LOG_NAME):
    return _get_client().logger(log_name)

def _print_with_severity_prefix(severity, text):
    print('{severity}: {text}'.format(severity=severity, text=text))

def _log_print_with_severity(severity, text):
    _print_with_severity_prefix(severity, text)
    logger = get_logger()
    logger.log_text(text, severity=severity)

def info(*messages):
    text = ', '.join(list(map(lambda m: str(m), messages)))
    _log_print_with_severity('INFO', text)

def errror(*messages):
    text = ', '.join(list(map(lambda m: str(m), messages)))
    _log_print_with_severity('ERROR', text)

def warning(*messages):
    text = ', '.join(list(map(lambda m: str(m), messages)))
    _log_print_with_severity('WARNING', text)
