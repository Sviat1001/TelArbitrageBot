import ccxt
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from datetime import datetime, timezone
import time
import threading

print_lock = threading.Lock()

def init_and_load(name):
    start_time = time.monotonic()
    print(f"  - Starting fetch for {name}...")
    try:
        exchange_class = getattr(ccxt, name)
        exchange = exchange_class()
        exchange.load_markets()

        spot_symbols = {
            symbol for symbol, market in exchange.markets.items()
            if market.get('type') == 'spot' or market.get('spot') is True
        }

        exchange.spot_symbols = spot_symbols
        duration = time.monotonic() - start_time
        print(f"✅ Successfully loaded {name} with {len(spot_symbols)} spot markets in {duration:.2f} seconds.")
        return name, exchange

    except Exception as e:
        print(f"❌ Failed to load {name}: {e}")
        return name, None  # return None so we can skip it later



def load_exchanges(exchange_names):
    print("\n--- Loading Exchanges ---")
    start_time = time.monotonic()
    exchange_objects = {}
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(init_and_load, name): name for name in exchange_names}
        for future in as_completed(futures):
            name, exchange = future.result()
            if exchange is not None:
                exchange_objects[name] = exchange
            else:
                print(f"⚠️ Skipping {name} due to earlier failure.")
    duration = time.monotonic() - start_time
    print(f"--- Finished loading exchanges in {duration:.2f} seconds. ---\n")
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


exchange_names = ['binance', 'kucoin', 'kraken', 'bybit']
def load_markets(exchange_names):
    exchange_objects = load_exchanges(exchange_names)
    common_symbols = get_common_symbols(exchange_objects)

    # Print results
    for name, symbols in common_symbols.items():
        print(f"✅ {name} has {len(symbols)} common spot symbols")  # Show only first 5 for brevity
    return exchange_objects, common_symbols

def fetch_exchange_tickers(name, exchange, common_symbols, batch_size=500, timeout=10):
    start_time = time.monotonic()
    with print_lock:
        print(f"  - Starting fetch for {name}...")
        print(f"  > {name} rate limit: {exchange.rateLimit} ms")
    tickers = {}

    def finish():  # Acts like a "jmp target"
        duration = time.monotonic() - start_time
        print(f"  - ✅ Finished fetch for {name} in {duration:.2f} seconds, got {len(tickers)} tickers.")
        return name, tickers
    
    if not exchange.has.get('fetchTickers'):
        print(f"⚠️ {name} does not support fetchTickers(), skipping.")
        return name, tickers

    symbols = common_symbols[name]

    if name == 'kucoin':
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(exchange.fetch_tickers)
                all_tickers = future.result(timeout=timeout)  # ⏱ Timeout here
                tickers = {sym: data for sym, data in all_tickers.items() if sym in symbols}
        except concurrent.futures.TimeoutError:
            print(f"⏳ Timeout while fetching all tickers from KuCoin, returning partial results.")
        except Exception as e:
            print(f"❌ {name} full fetchTickers() failed: {e}")
        return finish()
    
    for i in range(0, len(symbols), batch_size):
        if time.monotonic() - start_time > timeout:
            print(f"⏳ Timeout reached for {name}, returning partial results.")
            break
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

    return finish()

def get_all_tickers(exchange_objects, common_symbols, batch_size=500):
    all_tickers = {}

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_exchange_tickers, name, exchange, common_symbols, batch_size): name
            for name, exchange in exchange_objects.items()
        }

        for future in as_completed(futures):
            name, tickers = future.result()
            if tickers:
                all_tickers[name] = tickers

    return all_tickers

from datetime import datetime

def is_fresh(ticker, max_age_ms=50000):
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




def find_arbitrage_opportunities(exchange_objects, common_symbols, threshold=0.05):
    """
    Find arbitrage opportunities across exchanges for common symbols.

    Args:
        common_symbols (dict): {exchange_name: [symbols]}
        tickers (dict): {exchange_name: {symbol: ticker_dict}}
        threshold (float): minimum relative price difference to flag (e.g. 0.005 for 0.5%)

    Returns:
        list of dicts: each dict contains details of arbitrage opportunity.
    """
    tickers = get_all_tickers(exchange_objects, common_symbols)

    write_tickers_to_file(tickers)
    checked = set()  # to avoid checking pairs twice, store (exchange1, exchange2, symbol)
    opportunities = []

    # Get the list of exchanges we have tickers for
    exchanges = list(tickers.keys())

    for i in range(len(exchanges)):
        ex1 = exchanges[i]
        symbols1 = set(common_symbols.get(ex1, []))
        for j in range(i + 1, len(exchanges)):
            ex2 = exchanges[j]
            symbols2 = set(common_symbols.get(ex2, []))
            
            # Find common symbols between these two exchanges
            common_syms = symbols1.intersection(symbols2)
            
            for symbol in common_syms:
                # Avoid re-checking
                if (ex1, ex2, symbol) in checked or (ex2, ex1, symbol) in checked:
                    continue
                
                checked.add((ex1, ex2, symbol))

                ticker1 = tickers[ex1].get(symbol)
                ticker2 = tickers[ex2].get(symbol)
                if not ticker1 or not ticker2:
                    continue

                bid1, ask1 = ticker1.get('bid'), ticker1.get('ask')
                bid2, ask2 = ticker2.get('bid'), ticker2.get('ask')

                # Skip if any price is missing or zero
                if not bid1 or not ask1 or not bid2 or not ask2:
                    continue
                if bid1 <= 0 or ask1 <= 0 or bid2 <= 0 or ask2 <= 0:
                    continue

                # Calculate relative differences
                # Opportunity 1: buy on ex2 at ask2, sell on ex1 at bid1
                diff1 = (bid1 - ask2) / ask2

                # Opportunity 2: buy on ex1 at ask1, sell on ex2 at bid2
                diff2 = (bid2 - ask1) / ask1

                # Check if either opportunity exceeds threshold
                if diff1 > threshold:
                    opportunities.append({
                        'symbol': symbol,
                        'buy_exchange': ex2,
                        'buy_price': ask2,
                        'sell_exchange': ex1,
                        'sell_price': bid1,
                        'profit_pct': diff1 * 100,
                    })

                if diff2 > threshold:
                    opportunities.append({
                        'symbol': symbol,
                        'buy_exchange': ex1,
                        'buy_price': ask1,
                        'sell_exchange': ex2,
                        'sell_price': bid2,
                        'profit_pct': diff2 * 100,
                    })

    return opportunities
