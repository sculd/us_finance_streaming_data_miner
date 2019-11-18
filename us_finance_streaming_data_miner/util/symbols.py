_FILENAME_NASDAQ = 'nasdaq.txt'
_FILENAME_QUANDL = 'symbols.quandl.txt'

def get_symbols_nasdaq():
    symbols = []
    for symbol in open(_FILENAME_NASDAQ, 'r'):
        symbol = symbol.strip()
        symbols.append(symbol)
    return symbols

def get_symbols_quandl():
    symbols = []
    for symbol in open(_FILENAME_QUANDL, 'r'):
        symbol = symbol.strip()
        symbols.append(symbol)
    return symbols
