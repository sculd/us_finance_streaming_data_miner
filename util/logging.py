from google.cloud import logging
import config

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = logging.Client()
    return _client

def get_logger(cfg):
    return _get_client().logger(config.get_log_name(cfg))

def _print_with_severity_prefix(severity, text):
    print('{severity}: {text}'.format(severity=severity, text=text))

def _log_print_with_severity(cfg, severity, text):
    _print_with_severity_prefix(severity, text)
    logger = get_logger(cfg)
    logger.log_text(text, severity=severity)

def info(cfg, *messages):
    text = ', '.join(list(map(lambda m: str(m), messages)))
    _log_print_with_severity(cfg, 'INFO', text)

def errror(cfg, *messages):
    text = ', '.join(list(map(lambda m: str(m), messages)))
    _log_print_with_severity(cfg, 'ERROR', text)

def warning(cfg, *messages):
    text = ', '.join(list(map(lambda m: str(m), messages)))
    _log_print_with_severity(cfg, 'WARNING', text)
