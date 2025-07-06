import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import filters, MessageHandler, ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
from datetime import datetime, timedelta
from exchange import load_markets, find_arbitrage_opportunities
import json

SUBSCRIBERS_FILE = "subscribers.json"

def load_subscribers():
    if os.path.exists(SUBSCRIBERS_FILE):
        with open(SUBSCRIBERS_FILE, "r") as f:
            try:
                return set(json.load(f))
            except json.JSONDecodeError:
                logging.warning("Subscriber file corrupted, starting fresh.")
                return set()
    return set()

def save_subscribers():
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump(list(subscribed_users), f)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def load_env(filepath: str = ".env") -> None:
    if not os.path.exists(filepath):
        logging.warning(f"'.env' file not found at {filepath}. Environment variables might be missing.")
        return

    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key, value)
            logging.debug(f"Loaded {key} from .env")

load_env()

# Global set to store subscribed user IDs
subscribed_users = set()
subscribed_users = load_subscribers()
logging.info(f"Loaded {len(subscribed_users)} subscribers from file.")

sent_opportunities = {}

def opportunity_key(opp):
    return f"{opp['symbol']}:{opp['buy_exchange']}:{opp['sell_exchange']}"

# Initialize exchange data once when the bot starts
data_ready = False
exchange_names = ['binance', 'kraken', 'bybit', 'kucoin']
logging.info("Loading exchange markets and common symbols...")
exchange_objects, common_symbols = load_markets(exchange_names)
data_ready = True
logging.info("Exchange data loaded.")

async def clear_sent_opportunities(context: ContextTypes.DEFAULT_TYPE):
    for user_id in sent_opportunities:
        sent_opportunities[user_id].clear()
    logging.info("Cleared sent opportunities cache for all users.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command. Sends a welcome message with a 'Get Started' button."""
    user_id = update.effective_chat.id

    welcome_message = (
        "👋 Welcome! I'm your Crypto Arbitrage Bot. "
        "I'll help you find potential arbitrage opportunities across various exchanges.\n\n"
        "Press the button below to get started and subscribe to alerts!"
    )

    # Create an inline keyboard button with a specific callback_data
    # We'll use "subscribe_button" as the identifier for this button
    keyboard = [[
        InlineKeyboardButton("🚀 Get Started!", callback_data="subscribe_button")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Send the welcome message with the inline keyboard
    await context.bot.send_message(
        chat_id=user_id,
        text=welcome_message,
        reply_markup=reply_markup
    )
    logging.info(f"Sent welcome message to user {user_id}")

async def handle_button_press(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles callback queries from inline keyboard buttons."""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data

    # Always answer the callback query to remove the loading spinner on the button
    await query.answer()

    if data == "subscribe_button":
        if user_id not in subscribed_users:
            subscribed_users.add(user_id)
            save_subscribers()
            await query.edit_message_text(
                text=query.message.text + "\n\n✅ You've been subscribed to arbitrage alerts. "
                                        "I'll start sending you opportunities soon! "
                                        "To stop receiving alerts, just type /stop."
            )
            logging.info(f"User {user_id} subscribed via 'Get Started' button.")
            # Optionally, you can trigger the first arbitrage check immediately here
            # context.job_queue.run_once(send_arbitrage_alerts, 0, chat_id=user_id)
        else:
            await query.edit_message_text(
                text=query.message.text + "\n\n⚠️ You are already subscribed!"
            )
            logging.info(f"User {user_id} tried to subscribe but was already subscribed.")
    elif data == "resubscribe_button":
        if user_id not in subscribed_users:
            subscribed_users.add(user_id)
            save_subscribers()
            await context.bot.send_message(
                chat_id=user_id,
                text="✅ Welcome back! You've been resubscribed to arbitrage alerts. "
                    "To stop receiving alerts, just type /stop."
            )
            logging.info(f"User {user_id} resubscribed via 'Resubscribe' button.")
        else:
            await query.edit_message_text(
                text="⚠️ You are already subscribed!"
            )
            logging.info(f"User {user_id} tried to resubscribe but was already subscribed.")
    else:
        # Handle other potential callback_data values if you add more buttons later
        await query.edit_message_text(text=f"Unhandled button press: {data}")
        logging.warning(f"Unhandled callback_data: {data} from user {user_id}")


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /stop command. Unsubscribes the user from alerts."""
    user_id = update.effective_chat.id
    if user_id in subscribed_users:
        subscribed_users.discard(user_id)
        save_subscribers()
        keyboard = [[ InlineKeyboardButton("🔄 Resubscribe", callback_data="resubscribe_button")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await context.bot.send_message(
            chat_id=user_id,
            text="🛑 Unsubscribed from alerts. You will no longer receive opportunities.",
            reply_markup=reply_markup # Attach the button here
        )
        logging.info(f"User {user_id} unsubscribed.")
    else:
        await context.bot.send_message(chat_id=user_id, text="You are not currently subscribed.")
        logging.info(f"User {user_id} tried to unsubscribe but was not subscribed.")


# You will need a function to actually send the arbitrage alerts periodically.
# This is a placeholder; you'll integrate your find_arbitrage_opportunities here.
async def send_arbitrage_alerts(context: ContextTypes.DEFAULT_TYPE):
    if not data_ready:
        logging.info("Exchange data not ready yet, skipping arbitrage check.")
        return

    logging.info("Checking for arbitrage opportunities...")
    opportunities = find_arbitrage_opportunities(exchange_objects, common_symbols, threshold=0.005)
    if not opportunities:
        logging.info("No new arbitrage opportunities at the moment.")
        return

    for user_id in list(subscribed_users):
        # Initialize this user's sent_opportunities set if needed
        if user_id not in sent_opportunities:
            sent_opportunities[user_id] = set()

        new_opps_for_user = []
        for opp in opportunities:
            key = opportunity_key(opp)
            if key not in sent_opportunities[user_id]:
                sent_opportunities[user_id].add(key)
                new_opps_for_user.append(opp)

        if new_opps_for_user:
            message = "🔥 New Arbitrage Opportunities found!\n\n"
            for opp in new_opps_for_user:
                message += (
                    f"{opp['symbol']} | Buy {opp['buy_exchange']} at {opp['buy_price']:.6f} | "
                    f"Sell {opp['sell_exchange']} at {opp['sell_price']:.6f} | "
                    f"Profit: {opp['profit_pct']:.2f}%\n"
                )
            try:
                await context.bot.send_message(chat_id=user_id, text=message)
                logging.info(f"Sent arbitrage alert to user {user_id}")
            except Exception as e:
                logging.error(f"Failed to send message to user {user_id}: {e}")



if __name__ == '__main__':
    api_key = os.environ.get("TELEGRAM_API_KEY")
    if not api_key:
        logging.critical("TELEGRAM_API_KEY not found in environment variables. Please set it in a .env file or directly.")
        exit(1)

    application = ApplicationBuilder().token(api_key).build()

    # Add handlers
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('stop', stop))
    # This handler will catch all callback queries with data "subscribe_button"
    application.add_handler(CallbackQueryHandler(handle_button_press))

    job_queue = application.job_queue
    job_queue.run_repeating(send_arbitrage_alerts, interval=10, first=5)
    job_queue.run_repeating(clear_sent_opportunities, interval=3600, first=3600)
    logging.info("Bot started polling...")
    application.run_polling()