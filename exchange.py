import ccxt

binance = ccxt.binance()
kucoin = ccxt.kucoin()

binance.load_markets()
kucoin.load_markets()

binance_symbols = set(binance.symbols)
kucoin_symbols = set(kucoin.symbols)

common_symbols = sorted(binance_symbols & kucoin_symbols)
binance_only = sorted(binance_symbols - kucoin_symbols)
kucoin_only = sorted(kucoin_symbols - binance_symbols)

print("Which symbols would you like to display?")
print("Options: common / binance / kucoin")
choice = input("Your choice: ").strip().lower()

if choice == "common":
    print("\n✅ Common symbols:")
    for symbol in common_symbols:
        print(symbol)
    print(f"Total: {len(common_symbols)}")

elif choice == "binance":
    print("\n🟡 Binance-exclusive symbols:")
    for symbol in binance_only:
        print(symbol)
    print(f"Total: {len(binance_only)}")

elif choice == "kucoin":
    print("\n🔵 KuCoin-exclusive symbols:")
    for symbol in kucoin_only:
        print(symbol)
    print(f"Total: {len(kucoin_only)}")
else:
    print("\n❌ Invalid choice. Please enter: common / binance / kucoin")
