import asyncio
import datetime
import logging
import os
import re
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------------------------------------------------------
# RENDER HEALTH CHECK SERVER (PORT BINDING FIX)
# ---------------------------------------------------------
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Bot is live and running!")

    def log_message(self, format, *args):
        return

def run_health_check_server():
    port = int(os.environ.get("PORT", 8080))
    try:
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        server.serve_forever()
    except Exception as e:
        print(f"Health check server error: {e}")

threading.Thread(target=run_health_check_server, daemon=True).start()

# ---------------------------------------------------------
# LOGGING CONFIGURATION
# ---------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# ENVIRONMENT VARIABLES
# ---------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
RAW_DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID") or os.getenv("ADMIN_CHAT_ID", "0"))

if RAW_DATABASE_URL and RAW_DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = RAW_DATABASE_URL.replace("postgres://", "postgresql://", 1)
else:
    DATABASE_URL = RAW_DATABASE_URL

# Default QR Code File ID
QR_FILE_ID = os.getenv("QR_FILE_ID", "AgACAgUAAxkBAAMFamo9AXr8yxJhM9AJuipowCr2a9UAAvobaxtNyVFXq59REp-3CE8BAAMCAAN5AAM9BA")

USER_QR_MESSAGES = {}

# ---------------------------------------------------------
# DATABASE HELPERS
# ---------------------------------------------------------
def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id SERIAL PRIMARY KEY,
                file_id VARCHAR(255) UNIQUE NOT NULL,
                caption TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id BIGINT PRIMARY KEY,
                expiry_time TIMESTAMP NOT NULL
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database setup completed successfully.")
    except Exception as e:
        logger.error(f"Database Init Error: {e}")

def get_random_video_db():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT file_id, caption FROM videos ORDER BY RANDOM() LIMIT 1;")
        video = cur.fetchone()
        cur.close()
        conn.close()
        return video
    except Exception as e:
        logger.error(f"DB Error (get_random_video_db): {e}")
        return None

def is_user_subscribed_db(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT expiry_time FROM subscriptions WHERE user_id = %s;", (user_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row['expiry_time'] > datetime.datetime.now():
            return True
        return False
    except Exception as e:
        logger.error(f"DB Error (is_user_subscribed_db): {e}")
        return False

def set_user_subscription_db(user_id: int, hours: int):
    expiry = datetime.datetime.now() + datetime.timedelta(hours=hours)
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO subscriptions (user_id, expiry_time)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET expiry_time = EXCLUDED.expiry_time;
        """, (user_id, expiry))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"DB Error (set_user_subscription_db): {e}")

def remove_user_subscription_db(user_id: int) -> bool:
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM subscriptions WHERE user_id = %s;", (user_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        cur.close()
        conn.close()
        return deleted
    except Exception as e:
        logger.error(f"DB Error (remove_user_subscription_db): {e}")
        return False

def get_subscription_details_db(user_id: int):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT expiry_time FROM subscriptions WHERE user_id = %s;", (user_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row['expiry_time'].strftime("%Y-%m-%d %H:%M:%S") if row else None
    except Exception as e:
        logger.error(f"DB Error (get_subscription_details_db): {e}")
        return None

def get_stats_data_db():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as count FROM videos;")
        v_count = cur.fetchone()['count']
        cur.execute("SELECT COUNT(*) as count FROM subscriptions WHERE expiry_time > %s;", (datetime.datetime.now(),))
        u_count = cur.fetchone()['count']
        cur.close()
        conn.close()
        return v_count, u_count
    except Exception as e:
        logger.error(f"DB Error (get_stats_data_db): {e}")
        return 0, 0

def get_all_active_users_db():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM subscriptions WHERE expiry_time > %s;", (datetime.datetime.now(),))
        users = [row['user_id'] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return users
    except Exception as e:
        logger.error(f"DB Error (get_all_active_users_db): {e}")
        return []

# ---------------------------------------------------------
# KEYBOARD BUILDERS
# ---------------------------------------------------------
def get_main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💎 Buy 24 Hours Access (₹10)", callback_data="buy_24h")],
        [InlineKeyboardButton("🌟 Buy 7 Days Access (₹50)", callback_data="buy_7d")],
        [InlineKeyboardButton("👑 Buy 30 Days Access (₹150)", callback_data="buy_30d")],
        [InlineKeyboardButton("▶️ Open Purchased Content", callback_data="open_course")],
        [InlineKeyboardButton("📊 Check Subscription Status", callback_data="check_status")]
    ])

def get_nav_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("◀️ Previous", callback_data="nav_prev"),
            InlineKeyboardButton("Next ▶️", callback_data="nav_next")
        ],
        [
            InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
        ]
    ])

# ---------------------------------------------------------
# USER BOT HANDLERS
# ---------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    welcome_text = (
        f"👋 **Namaste {user.first_name}! Welcome to Premium Video Store.**\n\n"
        "📜 **PRICING PACKAGES:**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🥉 **Basic Pass:** ₹10 ➔ 24 Hours Access\n"
        "🥈 **Weekly Pass:** ₹50 ➔ 7 Days Access\n"
        "🥇 **Monthly Pass:** ₹150 ➔ 30 Days Access\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👇 Choose an option below:"
    )

    if update.callback_query:
        query = update.callback_query
        await query.answer()
        await query.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode="Markdown")
    else:
        await update.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode="Markdown")

async def handle_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    plan = query.data
    price, duration = "₹10", "24 Hours"
    if plan == "buy_7d":
        price, duration = "₹50", "7 Days"
    elif plan == "buy_30d":
        price, duration = "₹150", "30 Days"

    payment_text = (
        f"💳 **PAYMENT DETAILS ({duration} Plan)**\n\n"
        f"💵 **Amount to Pay:** {price}\n\n"
        "📍 **Payment Steps:**\n"
        "1. Scan the QR Code below and make payment.\n"
        "2. Send the payment **Screenshot directly to this chat**.\n\n"
        "⚡ Instant access will be granted after verification."
    )
    
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]])
    
    sent_msg = await context.bot.send_photo(
        chat_id=query.from_user.id,
        photo=QR_FILE_ID,
        caption=payment_text,
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    USER_QR_MESSAGES[query.from_user.id] = sent_msg.message_id

async def open_course(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    if not await asyncio.to_thread(is_user_subscribed_db, user_id):
        await query.message.reply_text("🔒 Aapki subscription active nahi hai ya expire ho chuki hai! Pehle /start karke plan khareedein.")
        return

    video = await asyncio.to_thread(get_random_video_db)
    if not video:
        await query.message.reply_text("📂 Database me abhi koi video available nahi hai.")
        return

    await context.bot.send_video(
        chat_id=user_id,
        video=video['file_id'],
        caption=video['caption'] or "✨ *Premium Video*",
        reply_markup=get_nav_keyboard(),
        parse_mode="Markdown"
    )

async def handle_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    if not await asyncio.to_thread(is_user_subscribed_db, user_id):
        await query.message.reply_text("🔒 Subscription Expired! Access lene ke liye /start karein.")
        return

    video = await asyncio.to_thread(get_random_video_db)
    if not video:
        await query.message.reply_text("⚠️ Database me koi video nahi mili.")
        return

    # Delete previous video message (Chat me hamesha sirf 1 video rahegi)
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"Failed to delete previous video: {e}")

    # Send new random video
    await context.bot.send_video(
        chat_id=query.message.chat_id,
        video=video['file_id'],
        caption=video['caption'] or "✨ *Premium Video*",
        reply_markup=get_nav_keyboard(),
        parse_mode="Markdown"
    )

async def check_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    if user_id == ADMIN_ID:
        await query.answer("👑 Admin Account: Unlimited Lifetime Access!", show_alert=True)
        return

    expiry = await asyncio.to_thread(get_subscription_details_db, user_id)
    if expiry:
        await query.answer(f"✅ Access Active Until:\n{expiry}", show_alert=True)
    else:
        await query.answer("❌ No active subscription found.", show_alert=True)

# ---------------------------------------------------------
# PAYMENT & MEDIA HANDLERS
# ---------------------------------------------------------
async def handle_photo_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    photo_file_id = update.message.photo[-1].file_id

    if user.id == ADMIN_ID:
        await update.message.reply_text(f"🖼️ **QR Code File ID:**\n\n`{photo_file_id}`", parse_mode="Markdown")
        return

    admin_markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve (24h)", callback_data=f"app_{user.id}_24"),
            InlineKeyboardButton("✅ Approve (7d)", callback_data=f"app_{user.id}_168"),
        ],
        [InlineKeyboardButton("✅ Approve (30d)", callback_data=f"app_{user.id}_720")],
        [InlineKeyboardButton("❌ Reject Payment", callback_data=f"rej_{user.id}")],
    ])

    await context.bot.send_photo(
        chat_id=ADMIN_ID,
        photo=photo_file_id,
        caption=f"📥 **NEW PAYMENT SCREENSHOT!**\n\n👤 **User:** {user.first_name}\n🆔 **User ID:** `{user.id}`",
        reply_markup=admin_markup,
        parse_mode="Markdown",
    )

    await update.message.reply_text("✅ **Screenshot Received!** Admin verification ke baad aapka access active ho jayega.")

async def handle_approval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.from_user.id != ADMIN_ID:
        return

    data = query.data.split("_")
    action, target_user_id = data[0], int(data[1])

    if target_user_id in USER_QR_MESSAGES:
        try:
            msg_id = USER_QR_MESSAGES.pop(target_user_id)
            await context.bot.delete_message(chat_id=target_user_id, message_id=msg_id)
        except Exception:
            pass

    if action == "app":
        hours = int(data[2])
        await asyncio.to_thread(set_user_subscription_db, target_user_id, hours)
        await query.edit_message_caption(caption=f"✅ **APPROVED:** User `{target_user_id}` ko {hours} Hours ka access de diya.", parse_mode="Markdown")

        await context.bot.send_message(
            chat_id=target_user_id,
            text=f"🎉 **Payment Verified!** Aapko **{hours} Ghante** ka access mil gaya hai. /start dabakar Open Course par click karein!",
            parse_mode="Markdown"
        )
    elif action == "rej":
        await query.edit_message_caption(caption=f"❌ **REJECTED:** User `{target_user_id}` ka payment reject kiya gaya.", parse_mode="Markdown")

        await context.bot.send_message(
            chat_id=target_user_id,
            text="❌ **Payment Verification Failed!** Please send valid payment screenshot.",
            parse_mode="Markdown"
        )

async def auto_upload_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    video_obj = update.message.video
    if not video_obj:
        return

    file_id = video_obj.file_id
    caption = update.message.caption or ""

    def _insert():
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO videos (file_id, caption) VALUES (%s, %s) ON CONFLICT (file_id) DO NOTHING;",
            (file_id, caption)
        )
        conn.commit()
        cur.close()
        conn.close()

    try:
        await asyncio.to_thread(_insert)
        await update.message.reply_text("✅ **Video Database me save ho gayi!**", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Error saving video: `{e}`", parse_mode="Markdown")

# ---------------------------------------------------------
# ADMIN COMMANDS
# ---------------------------------------------------------
async def grant_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        target_id = int(context.args[0])
        hours = int(context.args[1])
        await asyncio.to_thread(set_user_subscription_db, target_id, hours)
        await update.message.reply_text(f"✅ User `{target_id}` ko {hours} Hours ka access de diya gaya hai.", parse_mode="Markdown")
        await context.bot.send_message(
            target_id,
            f"🎉 **Access Granted!** Admin ne aapko {hours} Hours ka access de diya hai. /start karein!",
            parse_mode="Markdown"
        )
    except Exception:
        await update.message.reply_text("⚠️ Format: `/grant <USER_ID> <HOURS>`", parse_mode="Markdown")

async def revoke_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        target_id = int(context.args[0])
        removed = await asyncio.to_thread(remove_user_subscription_db, target_id)
        if removed:
            await update.message.reply_text(f"🔴 User `{target_id}` ka access revoke kar diya gaya hai.", parse_mode="Markdown")
            await context.bot.send_message(target_id, "⚠️ Aapka course access Revoke kar diya gaya hai.", parse_mode="Markdown")
        else:
            await update.message.reply_text(f"⚠️ User `{target_id}` active nahi mila.", parse_mode="Markdown")
    except Exception:
        await update.message.reply_text("⚠️ Format: `/revoke <USER_ID>`", parse_mode="Markdown")

async def userinfo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        target_id = int(context.args[0])
        expiry = await asyncio.to_thread(get_subscription_details_db, target_id)
        if expiry:
            await update.message.reply_text(f"👤 **User ID:** `{target_id}`\n⏳ **Expiry Date:** `{expiry}`", parse_mode="Markdown")
        else:
            await update.message.reply_text(f"❌ User `{target_id}` ki koi subscription record nahi mili.", parse_mode="Markdown")
    except Exception:
        await update.message.reply_text("⚠️ Format: `/userinfo <USER_ID>`", parse_mode="Markdown")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    v_count, u_count = await asyncio.to_thread(get_stats_data_db)
    msg = (
        "📊 **BOT DASHBOARD STATS**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎬 **Total Videos in DB:** {v_count}\n"
        f"👥 **Active Subscribers:** {u_count}\n"
        "━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("⚠️ Format: `/broadcast <Aapka Message>`", parse_mode="Markdown")
        return

    broadcast_msg = " ".join(context.args)
    users = await asyncio.to_thread(get_all_active_users_db)

    sent = 0
    for uid in users:
        try:
            await context.bot.send_message(uid, f"📢 **ANNOUNCEMENT:**\n\n{broadcast_msg}", parse_mode="Markdown")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    await update.message.reply_text(f"📢 Broadcast `{sent}/{len(users)}` active users ko bhej diya gaya.", parse_mode="Markdown")

# ---------------------------------------------------------
# APPLICATION ENTRY POINT
# ---------------------------------------------------------
def main():
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # User Command Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_buy, pattern="^buy_"))
    app.add_handler(CallbackQueryHandler(open_course, pattern="^open_course$"))
    app.add_handler(CallbackQueryHandler(start, pattern="^main_menu$"))
    app.add_handler(CallbackQueryHandler(handle_navigation, pattern="^nav_"))
    app.add_handler(CallbackQueryHandler(check_status_callback, pattern="^check_status$"))

    # Admin Command Handlers
    app.add_handler(CommandHandler("grant", grant_cmd))
    app.add_handler(CommandHandler("revoke", revoke_cmd))
    app.add_handler(CommandHandler("userinfo", userinfo_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))

    # Media & Approval Handlers
    app.add_handler(CallbackQueryHandler(handle_approval, pattern="^(app_|rej_)"))
    app.add_handler(MessageHandler(filters.VIDEO, auto_upload_video))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo_received))

    logger.info("Bot is active and polling...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
