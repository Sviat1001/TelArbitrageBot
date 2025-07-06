import ccxt
from concurrent.futures import ThreadPoolExecutor, as_completed

def init_and_load(name):
    try:
        exchange_class = getattr(ccxt, name)
        exchange = exchange_class()
        exchange.load_markets()

        spot_symbols = {
            symbol for symbol, market in exchange.markets.items()
            if market.get('type') == 'spot' or market.get('spot') is True
        }

        exchange.spot_symbols = spot_symbols
        return name, exchange

    except Exception as e:
        print(f"❌ Failed to load {name}: {e}")
        return name, None  # return None so we can skip it later



def load_exchanges(exchange_names):
    exchange_objects = {}
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(init_and_load, name): name for name in exchange_names}
        for future in as_completed(futures):
            name, exchange = future.result()
            if exchange is not None:
                exchange_objects[name] = exchange
            else:
                print(f"⚠️ Skipping {name} due to earlier failure.")
    return exchange_objects



def get_common_symbols(exchange_objects):
    # Use spot symbols only
    symbol_sets = {name: exchange.spot_symbols for name, exchange in exchange_objects.items()}

    common_symbols = {}
    for name in symbol_sets:
        other_symbols = set()
        for other_name, symbols in symbol_sets.items():
            if other_name != name:
                other_symbols |= symbols
        common = symbol_sets[name] & other_symbols
        common_symbols[name] = sorted(common)
    
    return common_symbols


# Example usage
exchange_names = ['binance', 'kucoin', 'kraken', 'bybit']
def load(exchange_names):
    exchange_objects = load_exchanges(exchange_names)
    common_symbols = get_common_symbols(exchange_objects)

    # Print results
    for name, symbols in common_symbols.items():
        print(f"{name} has {len(symbols)} common spot symbols: {symbols[:5]} ...")  # Show only first 5 for brevity


load(exchange_names)