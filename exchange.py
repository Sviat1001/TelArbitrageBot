import ccxt
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

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
    return exchange_objects, common_symbols

exchange_objects, common_symbols = load(exchange_names)

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

def fetch_exchange_tickers(name, exchange, batch_size=500):
    tickers = {}

    if not exchange.has.get('fetchTickers'):
        print(f"⚠️ {name} does not support fetchTickers(), skipping.")
        return name, tickers

    symbols = common_symbols[name]

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        try:
            partial = exchange.fetch_tickers(batch)
            tickers.update(partial)
        except TypeError:
            print(f"⚠️ {name} does not support filtered fetchTickers(). Using full fetch once.")
            try:
                tickers = exchange.fetch_tickers()
            except Exception as e:
                print(f"❌ {name} full fetchTickers() failed: {e}")
            break
        except Exception as e:
            print(f"⚠️ {name} batch {i}-{i+batch_size} failed: {e}")
            sleep(1)  # optional: pause between batches on error

    return name, tickers


def get_all_tickers(exchange_objects, batch_size=500):
    all_tickers = {}

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_exchange_tickers, name, exchange, batch_size): name
            for name, exchange in exchange_objects.items()
        }

        for future in as_completed(futures):
            name, tickers = future.result()
            if tickers:
                all_tickers[name] = tickers

    return all_tickers

from datetime import datetime

def is_fresh(ticker, max_age_ms=5000):
    ts = ticker.get("timestamp")
    if ts is None:
        return False
    now = datetime.now(timezone.utc).timestamp() * 1000  # milliseconds
    return (now - ts) < max_age_ms

def write_tickers_to_file(tickers, filename="tickers_output.txt"):
    with open(filename, "w") as f:
        for exchange_name, symbol_dict in tickers.items():
            f.write(f"\n📈 {exchange_name.upper()} Tickers:\n")
            for symbol, ticker in symbol_dict.items():
                bid = ticker.get("bid", "N/A")
                ask = ticker.get("ask", "N/A")
                last = ticker.get("last", "N/A")
                ts = ticker.get("datetime") or ticker.get("timestamp") or "N/A"
                fresh = "✅" if is_fresh(ticker) else "⚠️ stale"
                f.write(f"{symbol:15} | bid: {bid} | ask: {ask} | last: {last} | time: {ts} | {fresh}\n")
    print(f"✅ Ticker results written to {filename}")


tickers = get_all_tickers(exchange_objects)

write_tickers_to_file(tickers)

def fmt(value):
    return f"{value:.8f}" if isinstance(value, (int, float)) else "   ---   "

for exchange_name, symbol_dict in tickers.items():
    print(f"\n📈 {exchange_name.upper()} Tickers:")
    for symbol, ticker in list(symbol_dict.items())[:3]:
        bid = ticker.get('bid')
        ask = ticker.get('ask')
        last = ticker.get('last')
        print(f"  {symbol:15} | bid: {fmt(bid)} | ask: {fmt(ask)} | last: {fmt(last)}")