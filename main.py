import asyncio
import datetime
import html
import logging
import os
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaVideo, Update
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------------------------------------------------------
# 1. HEALTH CHECK SERVER FOR RENDER (24/7 UPTIME)
# ---------------------------------------------------------
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"Bot status: ONLINE")

    def log_message(self, format, *args):
        return

def run_health_check_server():
    port = int(os.environ.get("PORT", 8080))
    try:
        server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        server.serve_forever()
    except Exception as e:
        print(f"Health Check Server Notice: {e}")

threading.Thread(target=run_health_check_server, daemon=True).start()

# ---------------------------------------------------------
# 2. CONFIGURATION & LOGGING
# ---------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
RAW_DATABASE_URL = os.getenv("DATABASE_URL")

ADMIN_ID = 7572036863
ALLOWED_CHANNEL_ID = -1004403159967

if RAW_DATABASE_URL and RAW_DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = RAW_DATABASE_URL.replace("postgres://", "postgresql://", 1)
else:
    DATABASE_URL = RAW_DATABASE_URL

if DATABASE_URL and "sslmode" not in DATABASE_URL:
    connector = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{connector}sslmode=require"

db_pool = None

QR_FILE_ID = os.getenv(
    "QR_FILE_ID",
    "AgACAgUAAxkBAAMFamo9AXr8yxJhM9AJuipowCr2a9UAAvobaxtNyVFXq59REp-3CE8BAAMCAAN5AAM9BA"
)

USER_QR_MESSAGES = {}
USER_LOCKS = {}
USER_SENT_MESSAGES = {}
USER_INACTIVITY_TASKS = {}
INACTIVITY_TIMEOUT = 300  # 5 Minutes
MAX_TRACKED_MESSAGES_PER_USER = 50

# ---------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------
def track_message(user_id: int, message_id: int):
    if user_id == ADMIN_ID:
        return
    if user_id not in USER_SENT_MESSAGES:
        USER_SENT_MESSAGES[user_id] = []
    
    USER_SENT_MESSAGES[user_id].append(message_id)
    if len(USER_SENT_MESSAGES[user_id]) > MAX_TRACKED_MESSAGES_PER_USER:
        USER_SENT_MESSAGES[user_id] = USER_SENT_MESSAGES[user_id][-MAX_TRACKED_MESSAGES_PER_USER:]

async def _silent_delete_job(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        await asyncio.sleep(INACTIVITY_TIMEOUT)
        messages = USER_SENT_MESSAGES.get(user_id, [])
        for msg_id in messages:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=msg_id)
            except Exception:
                pass
        USER_SENT_MESSAGES[user_id] = []
        if user_id in USER_LOCKS:
            del USER_LOCKS[user_id]
        if user_id in USER_INACTIVITY_TASKS:
            del USER_INACTIVITY_TASKS[user_id]
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Error in silent cleanup for user {user_id}: {e}")

def reset_inactivity_timer(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    if user_id == ADMIN_ID:
        return
    if user_id in USER_INACTIVITY_TASKS:
        task = USER_INACTIVITY_TASKS[user_id]
        if not task.done():
            task.cancel()
            
    task = asyncio.create_task(_silent_delete_job(user_id, context))
    USER_INACTIVITY_TASKS[user_id] = task

# ---------------------------------------------------------
# 3. DATABASE ENGINE
# ---------------------------------------------------------
def init_pool():
    global db_pool
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable missing!")
    try:
        # Reduced pool size for free database tiers (1 to 5)
        db_pool = pool.ThreadedConnectionPool(1, 5, dsn=DATABASE_URL)
        logger.info("✅ Database Connection Pool initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Connection Pool Initialization Error: {e}")

@contextmanager
def get_db_connection():
    global db_pool
    if not db_pool:
        init_pool()
    conn = db_pool.getconn()
    try:
        yield conn
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise e
    finally:
        if db_pool and conn:
            db_pool.putconn(conn)

def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS videos (
                        id SERIAL PRIMARY KEY,
                        file_id VARCHAR(255) NOT NULL,
                        caption TEXT
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS subscriptions (
                        user_id BIGINT PRIMARY KEY,
                        expiry_time TIMESTAMP WITH TIME ZONE NOT NULL
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        first_name TEXT,
                        joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                """)
                conn.commit()
            logger.info("✅ Database tables checked/initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Database Init Error: {e}")

def register_user_db(user_id: int, first_name: str):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users (user_id, first_name)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET first_name = EXCLUDED.first_name;
                """, (user_id, first_name))
                conn.commit()
    except Exception as e:
        logger.error(f"DB Error (register_user_db): {e}")

def get_random_video_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT file_id, caption FROM videos ORDER BY RANDOM() LIMIT 1;")
                return cur.fetchone()
    except Exception as e:
        logger.error(f"DB Error (get_random_video_db): {e}")
        return None

def get_video_by_file_id(file_id: str):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT file_id, caption FROM videos WHERE file_id = %s LIMIT 1;", (file_id,))
                return cur.fetchone()
    except Exception as e:
        logger.error(f"DB Error (get_video_by_file_id): {e}")
        return None

def is_user_subscribed_db(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT (expiry_time > NOW()) as is_valid 
                    FROM subscriptions 
                    WHERE user_id = %s;
                """, (user_id,))
                row = cur.fetchone()
                return row['is_valid'] if row else False
    except Exception as e:
        logger.error(f"DB Error (is_user_subscribed_db): {e}")
        return False

def set_user_subscription_db(user_id: int, hours: int):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                interval_str = f"{hours} hours"
                cur.execute("""
                    INSERT INTO subscriptions (user_id, expiry_time)
                    VALUES (%s, NOW() + %s::INTERVAL)
                    ON CONFLICT (user_id) DO UPDATE 
                    SET expiry_time = GREATEST(subscriptions.expiry_time, NOW()) + %s::INTERVAL;
                """, (user_id, interval_str, interval_str))
                conn.commit()
    except Exception as e:
        logger.error(f"DB Error (set_user_subscription_db): {e}")

def remove_user_subscription_db(user_id: int) -> bool:
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM subscriptions WHERE user_id = %s;", (user_id,))
                deleted = cur.rowcount > 0
                conn.commit()
                return deleted
    except Exception as e:
        logger.error(f"DB Error (remove_user_subscription_db): {e}")
        return False

def get_subscription_details_db(user_id: int):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT expiry_time FROM subscriptions WHERE user_id = %s;", (user_id,))
                row = cur.fetchone()
                return row['expiry_time'].strftime("%Y-%m-%d %H:%M:%S UTC") if row else None
    except Exception as e:
        logger.error(f"DB Error (get_subscription_details_db): {e}")
        return None

def get_stats_data_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT COUNT(*) as count FROM videos;")
                v_count = cur.fetchone()['count']
                cur.execute("SELECT COUNT(*) as count FROM subscriptions WHERE expiry_time > NOW();")
                u_count = cur.fetchone()['count']
                cur.execute("SELECT COUNT(*) as count FROM users;")
                total_users = cur.fetchone()['count']
                return v_count, u_count, total_users
    except Exception as e:
        logger.error(f"DB Error (get_stats_data_db): {e}")
        return 0, 0, 0

def get_all_active_users_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT user_id FROM subscriptions WHERE expiry_time > NOW();")
                return [row['user_id'] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"DB Error (get_all_active_users_db): {e}")
        return []

def get_all_users_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT user_id FROM users;")
                return [row['user_id'] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"DB Error (get_all_users_db): {e}")
        return []

# ---------------------------------------------------------
# 4. KEYBOARD LAYOUTS
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
# 5. USER HANDLERS
# ---------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    reset_inactivity_timer(user.id, context)
    
    await asyncio.to_thread(register_user_db, user.id, user.first_name)

    safe_name = html.escape(user.first_name) if user.first_name else "User"

    welcome_text = (
        f"👋 <b>Namaste {safe_name}! Welcome to Premium Video Store.</b>\n\n"
        "📜 <b>PRICING PACKAGES:</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🥉 <b>Basic Pass:</b> ₹10 ➔ 24 Hours Access\n"
        "🥈 <b>Weekly Pass:</b> ₹50 ➔ 7 Days Access\n"
        "🥇 <b>Monthly Pass:</b> ₹150 ➔ 30 Days Access\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👇 Choose an option below:"
    )

    if update.callback_query:
        query = update.callback_query
        await query.answer()
        msg = await query.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode="HTML")
        track_message(user.id, msg.message_id)
    else:
        msg = await update.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode="HTML")
        if update.message:
            track_message(user.id, update.message.message_id)
        track_message(user.id, msg.message_id)

async def handle_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    reset_inactivity_timer(user_id, context)

    plan = query.data
    price, duration = "₹10", "24 Hours"
    if plan == "buy_7d":
        price, duration = "₹50", "7 Days"
    elif plan == "buy_30d":
        price, duration = "₹150", "30 Days"

    payment_text = (
        f"💳 <b>PAYMENT DETAILS ({duration} Plan)</b>\n\n"
        f"💵 <b>Amount to Pay:</b> {price}\n\n"
        "📍 <b>Payment Steps:</b>\n"
        "1. Scan the QR Code below and make payment.\n"
        "2. Send the payment <b>Screenshot directly to this chat</b>.\n\n"
        "⚡ Instant access will be granted after verification."
    )

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]])

    sent_msg = await context.bot.send_photo(
        chat_id=user_id,
        photo=QR_FILE_ID,
        caption=payment_text,
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    USER_QR_MESSAGES[user_id] = sent_msg.message_id
    track_message(user_id, sent_msg.message_id)

async def open_course(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    reset_inactivity_timer(user_id, context)

    if not await asyncio.to_thread(is_user_subscribed_db, user_id):
        msg = await query.message.reply_text("🔒 Aapki subscription active nahi hai! Pehle /start karke plan khareedein.")
        track_message(user_id, msg.message_id)
        return

    video = await asyncio.to_thread(get_random_video_db)
    if not video:
        msg = await query.message.reply_text("📂 Database me abhi koi video available nahi hai.")
        track_message(user_id, msg.message_id)
        return

    context.user_data['history'] = [video['file_id']]
    context.user_data['history_idx'] = 0

    caption = video['caption'] if video['caption'] else "✨ <b>Premium Video</b>"

    sent_msg = await context.bot.send_video(
        chat_id=user_id,
        video=video['file_id'],
        caption=caption,
        reply_markup=get_nav_keyboard(),
        parse_mode="HTML"
    )
    track_message(user_id, sent_msg.message_id)

async def handle_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    reset_inactivity_timer(user_id, context)

    if user_id not in USER_LOCKS:
        USER_LOCKS[user_id] = asyncio.Lock()

    if USER_LOCKS[user_id].locked():
        await query.answer("⏳ Processing... please wait.")
        return

    async with USER_LOCKS[user_id]:
        await query.answer()

        if not await asyncio.to_thread(is_user_subscribed_db, user_id):
            msg = await query.message.reply_text("🔒 Subscription Expired! Access lene ke liye /start karein.")
            track_message(user_id, msg.message_id)
            return

        history = context.user_data.get('history', [])
        idx = context.user_data.get('history_idx', -1)
        action = query.data
        video = None

        if action == "nav_prev":
            if idx > 0:
                idx -= 1
                video = await asyncio.to_thread(get_video_by_file_id, history[idx])
            else:
                await query.answer("⚠️ Pehle koi video nahi hai!", show_alert=True)
                return
        else:
            if idx < len(history) - 1:
                idx += 1
                video = await asyncio.to_thread(get_video_by_file_id, history[idx])
            else:
                video = await asyncio.to_thread(get_random_video_db)
                if video:
                    history.append(video['file_id'])
                    idx += 1

        if not video:
            msg = await query.message.reply_text("⚠️ Video load nahi ho paayi.")
            track_message(user_id, msg.message_id)
            return

        context.user_data['history'] = history
        context.user_data['history_idx'] = idx

        caption = video['caption'] if video['caption'] else "✨ <b>Premium Video</b>"

        try:
            await query.edit_message_media(
                media=InputMediaVideo(
                    media=video['file_id'],
                    caption=caption,
                    parse_mode="HTML"
                ),
                reply_markup=get_nav_keyboard()
            )
        except Exception:
            try:
                await query.message.delete()
            except Exception:
                pass
            sent_msg = await context.bot.send_video(
                chat_id=query.message.chat_id,
                video=video['file_id'],
                caption=caption,
                reply_markup=get_nav_keyboard(),
                parse_mode="HTML"
            )
            track_message(user_id, sent_msg.message_id)

async def check_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    reset_inactivity_timer(user_id, context)

    if user_id == ADMIN_ID:
        await query.answer("👑 Admin Account: Unlimited Access!", show_alert=True)
        return

    expiry = await asyncio.to_thread(get_subscription_details_db, user_id)
    if expiry and await asyncio.to_thread(is_user_subscribed_db, user_id):
        await query.answer(f"✅ Access Active Until:\n{expiry}", show_alert=True)
    else:
        await query.answer("❌ Subscription Expired or Inactive.", show_alert=True)

# ---------------------------------------------------------
# 6. VIDEO UPLOAD & PAYMENT APPROVAL SYSTEM
# ---------------------------------------------------------
async def handle_photo_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    reset_inactivity_timer(user.id, context)
    photo_file_id = update.message.photo[-1].file_id

    track_message(user.id, update.message.message_id)

    if user and user.id == ADMIN_ID:
        await update.message.reply_text(f"🖼️ <b>QR Code File ID:</b>\n\n<code>{photo_file_id}</code>", parse_mode="HTML")
        return

    admin_markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve (24h)", callback_data=f"app_{user.id}_24"),
            InlineKeyboardButton("✅ Approve (7d)", callback_data=f"app_{user.id}_168"),
        ],
        [InlineKeyboardButton("✅ Approve (30d)", callback_data=f"app_{user.id}_720")],
        [InlineKeyboardButton("❌ Reject Payment", callback_data=f"rej_{user.id}")],
    ])

    safe_name = html.escape(user.first_name) if user.first_name else "User"

    await context.bot.send_photo(
        chat_id=ADMIN_ID,
        photo=photo_file_id,
        caption=f"📥 <b>NEW PAYMENT SCREENSHOT!</b>\n\n👤 <b>User:</b> {safe_name}\n🆔 <b>User ID:</b> <code>{user.id}</code>",
        reply_markup=admin_markup,
        parse_mode="HTML",
    )

    msg = await update.message.reply_text("✅ <b>Screenshot Received!</b> Admin verification ke baad aapka access active ho jayega.", parse_mode="HTML")
    track_message(user.id, msg.message_id)

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
        await query.edit_message_caption(caption=f"✅ <b>APPROVED:</b> User <code>{target_user_id}</code> ko {hours} Hours ka access de diya.", parse_mode="HTML")

        sent_msg = await context.bot.send_message(
            chat_id=target_user_id,
            text=f"🎉 <b>Payment Verified!</b> Aapko <b>{hours} Ghante</b> ka access mil gaya hai. /start dabakar Open Course par click karein!",
            parse_mode="HTML"
        )
        track_message(target_user_id, sent_msg.message_id)
    elif action == "rej":
        await query.edit_message_caption(caption=f"❌ <b>REJECTED:</b> User <code>{target_user_id}</code> ka payment reject kiya gaya.", parse_mode="HTML")

        sent_msg = await context.bot.send_message(
            chat_id=target_user_id,
            text="❌ <b>Payment Verification Failed!</b> Please send valid payment screenshot.",
            parse_mode="HTML"
        )
        track_message(target_user_id, sent_msg.message_id)

async def auto_upload_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if not msg:
        return

    if update.channel_post:
        logger.info(f"Incoming Post from Channel ID: {update.channel_post.chat.id}")
        if update.channel_post.chat.id != ALLOWED_CHANNEL_ID:
            logger.warning(f"Ignored video from unauthorized channel ID: {update.channel_post.chat.id}")
            return

    if update.message:
        if update.message.from_user and update.message.from_user.id != ADMIN_ID:
            await update.message.reply_text("⚠️ Access Denied! Only Admin can upload videos directly.")
            return

    file_id = None
    if msg.video:
        file_id = msg.video.file_id
    elif msg.document and msg.document.mime_type and msg.document.mime_type.startswith("video/"):
        file_id = msg.document.file_id

    if not file_id:
        return

    caption = msg.caption or ""

    def _quick_db_save():
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO videos (file_id, caption) VALUES (%s, %s);",
                        (file_id, caption)
                    )
                    conn.commit()
                    return cur.rowcount
        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            return 0

    try:
        rows = await asyncio.to_thread(_quick_db_save)

        if rows > 0:
            if update.message:
                await update.message.reply_text("✅ Video added in database")
            elif update.channel_post:
                logger.info(f"✅ Video successfully added to DB from Channel: {file_id}")
                try:
                    await context.bot.send_message(
                        chat_id=update.channel_post.chat.id,
                        text="✅ Video added in database",
                        reply_to_message_id=update.channel_post.message_id
                    )
                except Exception as channel_err:
                    logger.warning(f"Could not send reply to channel: {channel_err}")
        else:
            if update.message:
                await update.message.reply_text("❌ Failed to save video in database.")
            elif update.channel_post:
                logger.error(f"❌ Failed to save video in DB: {file_id}")

    except Exception as e:
        logger.error(f"Error processing auto_upload_video: {e}")

# ---------------------------------------------------------
# 7. ADMIN COMMANDS
# ---------------------------------------------------------
async def grant_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        target_id = int(context.args[0])
        hours = int(context.args[1])
        await asyncio.to_thread(set_user_subscription_db, target_id, hours)
        await update.message.reply_text(f"✅ User <code>{target_id}</code> ko {hours} Hours ka access de diya gaya hai.", parse_mode="HTML")
        
        sent_msg = await context.bot.send_message(
            target_id,
            f"🎉 <b>Access Granted!</b> Admin ne aapko {hours} Hours ka access de diya hai. /start karein!",
            parse_mode="HTML"
        )
        track_message(target_id, sent_msg.message_id)
    except Exception:
        await update.message.reply_text("⚠️ Format: <code>/grant &lt;USER_ID&gt; &lt;HOURS&gt;</code>", parse_mode="HTML")

async def revoke_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        target_id = int(context.args[0])
        removed = await asyncio.to_thread(remove_user_subscription_db, target_id)
        if removed:
            await update.message.reply_text(f"🔴 User <code>{target_id}</code> ka access revoke kar diya gaya hai.", parse_mode="HTML")
            sent_msg = await context.bot.send_message(target_id, "⚠️ Aapka course access Revoke kar diya gaya hai.", parse_mode="HTML")
            track_message(target_id, sent_msg.message_id)
        else:
            await update.message.reply_text(f"⚠️ User <code>{target_id}</code> active nahi mila.", parse_mode="HTML")
    except Exception:
        await update.message.reply_text("⚠️ Format: <code>/revoke &lt;USER_ID&gt;</code>", parse_mode="HTML")

async def userinfo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        target_id = int(context.args[0])
        expiry = await asyncio.to_thread(get_subscription_details_db, target_id)
        if expiry:
            await update.message.reply_text(f"👤 <b>User ID:</b> <code>{target_id}</code>\n⏳ <b>Expiry Date:</b> <code>{expiry}</code>", parse_mode="HTML")
        else:
            await update.message.reply_text(f"❌ User <code>{target_id}</code> ki koi subscription record nahi mili.", parse_mode="HTML")
    except Exception:
        await update.message.reply_text("⚠️ Format: <code>/userinfo &lt;USER_ID&gt;</code>", parse_mode="HTML")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    v_count, u_count, total_users = await asyncio.to_thread(get_stats_data_db)
    msg = (
        "📊 <b>BOT DASHBOARD STATS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎬 <b>Total Videos in DB:</b> {v_count}\n"
        f"👥 <b>Active Subscribers:</b> {u_count}\n"
        f"🌐 <b>Total Registered Users:</b> {total_users}\n"
        "━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("⚠️ Format:\n<code>/broadcast &lt;Message&gt;</code> (Sirf Active Users ko)\n<code>/broadcast all &lt;Message&gt;</code> (Sabhi Users ko)", parse_mode="HTML")
        return

    target_type = "active"
    args = context.args.copy()

    if args[0].lower() == "all":
        target_type = "all"
        args.pop(0)

    raw_text = " ".join(args)
    broadcast_msg = html.escape(raw_text)
    
    if not broadcast_msg:
        await update.message.reply_text("⚠️ Content khaali hai! Message likhein.", parse_mode="HTML")
        return

    if target_type == "all":
        users = await asyncio.to_thread(get_all_users_db)
    else:
        users = await asyncio.to_thread(get_all_active_users_db)

    sent = 0
    for uid in users:
        try:
            sent_m = await context.bot.send_message(uid, f"📢 <b>ANNOUNCEMENT:</b>\n\n{broadcast_msg}", parse_mode="HTML")
            track_message(uid, sent_m.message_id)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    await update.message.reply_text(f"📢 Broadcast <code>{sent}/{len(users)}</code> (<code>{target_type.upper()}</code>) users ko bhej diya gaya.", parse_mode="HTML")

# ---------------------------------------------------------
# 8. MAIN ENTRY POINT
# ---------------------------------------------------------
def main():
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN environment variable is missing!")
        return
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL environment variable is missing!")
        return

    init_pool()
    init_db()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .read_timeout(60)
        .write_timeout(60)
        .connect_timeout(30)
        .pool_timeout(30)
        .get_updates_read_timeout(60)
        .build()
    )

    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_buy, pattern="^buy_"))
    app.add_handler(CallbackQueryHandler(open_course, pattern="^open_course$"))
    app.add_handler(CallbackQueryHandler(start, pattern="^main_menu$"))
    app.add_handler(CallbackQueryHandler(handle_navigation, pattern="^nav_"))
    app.add_handler(CallbackQueryHandler(check_status_callback, pattern="^check_status$"))

    # Admin Handlers
    app.add_handler(CommandHandler("grant", grant_cmd))
    app.add_handler(CommandHandler("revoke", revoke_cmd))
    app.add_handler(CommandHandler("userinfo", userinfo_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))

    app.add_handler(CallbackQueryHandler(handle_approval, pattern="^(app_|rej_)"))

    # Channel and Media Handlers
    app.add_handler(
        MessageHandler(
            (filters.VIDEO | filters.Document.VIDEO) & (filters.ChatType.PRIVATE | filters.ChatType.CHANNEL),
            auto_upload_video
        )
    )
    app.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, handle_photo_received))

    logger.info("Bot is running seamlessly...")
    app.run_polling(allowed_updates=["message", "edited_message", "channel_post", "edited_channel_post", "callback_query"])

if __name__ == "__main__":
    main()
