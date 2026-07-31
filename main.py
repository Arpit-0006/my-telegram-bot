import asyncio
import datetime
import logging
import os
import threading
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
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Bot status: ONLINE")

    def log_message(self, format, *args):
        return

def run_health_check_server():
    port = int(os.environ.get("PORT", 8080))
    try:
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
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

# HARDCODED ADMIN ID & CHANNEL ID
ADMIN_ID = 7572036863
ALLOWED_CHANNEL_ID = -1004403159967

if RAW_DATABASE_URL and RAW_DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = RAW_DATABASE_URL.replace("postgres://", "postgresql://", 1)
else:
    DATABASE_URL = RAW_DATABASE_URL

if DATABASE_URL and "sslmode" not in DATABASE_URL:
    connector = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{connector}sslmode=require"

# DATABASE CONNECTION POOL
db_pool = None

QR_FILE_ID = os.getenv("QR_FILE_ID", "AgACAgUAAxkBAAMFamo9AXr8yxJhM9AJuipowCr2a9UAAvobaxtNyVFXq59REp-3CE8BAAMCAAN5AAM9BA")

USER_QR_MESSAGES = {}
USER_LOCKS = {}

# HIDDEN AUTO-DELETE TRACKERS (NO TEXT TO USER)
USER_SENT_MESSAGES = {}
USER_INACTIVITY_TASKS = {}
INACTIVITY_TIMEOUT = 300  # 5 Minutes = 300 Seconds

# ---------------------------------------------------------
# HIDDEN AUTO-DELETE UTILITIES
# ---------------------------------------------------------
def track_message(user_id: int, message_id: int):
    """Tracks sent messages for silent deletion later"""
    if user_id == ADMIN_ID:
        return
    if user_id not in USER_SENT_MESSAGES:
        USER_SENT_MESSAGES[user_id] = []
    USER_SENT_MESSAGES[user_id].append(message_id)

async def _silent_delete_job(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Silent background cleanup task - No Text Shown"""
    try:
        await asyncio.sleep(INACTIVITY_TIMEOUT)
        messages = USER_SENT_MESSAGES.get(user_id, [])
        for msg_id in messages:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=msg_id)
            except Exception:
                pass
        USER_SENT_MESSAGES[user_id] = []
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Error in silent cleanup for user {user_id}: {e}")

def reset_inactivity_timer(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Resets the 5-minute silent timer on any new user interaction"""
    if user_id == ADMIN_ID:
        return
    if user_id in USER_INACTIVITY_TASKS:
        task = USER_INACTIVITY_TASKS[user_id]
        if not task.done():
            task.cancel()
    
    task = asyncio.create_task(_silent_delete_job(user_id, context))
    USER_INACTIVITY_TASKS[user_id] = task

# ---------------------------------------------------------
# 3. DATABASE ENGINE (POSTGRESQL WITH CONNECTION POOL)
# ---------------------------------------------------------
def init_pool():
    global db_pool
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable missing!")
    try:
        db_pool = pool.SimpleConnectionPool(1, 20, dsn=DATABASE_URL)
        logger.info("✅ Database Connection Pool initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Connection Pool Initialization Error: {e}")

def get_db():
    global db_pool
    if not db_pool:
        init_pool()
    try:
        conn = db_pool.getconn()
        conn.autocommit = False
        return conn
    except Exception:
        # Fallback reconnect attempt
        init_pool()
        return db_pool.getconn()

def release_db(conn):
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)

def init_db():
    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    id SERIAL PRIMARY KEY,
                    file_id VARCHAR(255) UNIQUE NOT NULL,
                    caption TEXT
                );
            """)
            cur.execute("""
                ALTER TABLE videos 
                ADD COLUMN IF NOT EXISTS caption TEXT;
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
        if conn:
            conn.rollback()
        logger.error(f"❌ Database Init Error: {e}")
    finally:
        if conn:
            release_db(conn)

def register_user_db(user_id: int, first_name: str):
    """Saves or updates user profiles for broadcasts"""
    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (user_id, first_name)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET first_name = EXCLUDED.first_name;
            """, (user_id, first_name))
            conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"DB Error (register_user_db): {e}")
    finally:
        if conn:
            release_db(conn)

def get_random_video_db():
    conn = None
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT file_id, caption FROM videos ORDER BY RANDOM() LIMIT 1;")
            return cur.fetchone()
    except Exception as e:
        logger.error(f"DB Error (get_random_video_db): {e}")
        return None
    finally:
        if conn:
            release_db(conn)

def get_video_by_file_id(file_id: str):
    conn = None
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT file_id, caption FROM videos WHERE file_id = %s;", (file_id,))
            return cur.fetchone()
    except Exception as e:
        logger.error(f"DB Error (get_video_by_file_id): {e}")
        return None
    finally:
        if conn:
            release_db(conn)

def is_user_subscribed_db(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    conn = None
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT expiry_time FROM subscriptions WHERE user_id = %s;", (user_id,))
            row = cur.fetchone()
            if not row:
                return False
            cur.execute("SELECT (%s > NOW()) as is_valid;", (row['expiry_time'],))
            res = cur.fetchone()
            return res['is_valid'] if res else False
    except Exception as e:
        logger.error(f"DB Error (is_user_subscribed_db): {e}")
        return False
    finally:
        if conn:
            release_db(conn)

def set_user_subscription_db(user_id: int, hours: int):
    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO subscriptions (user_id, expiry_time)
                VALUES (%s, NOW() + (%s || ' hours')::INTERVAL)
                ON CONFLICT (user_id) DO UPDATE 
                SET expiry_time = GREATEST(subscriptions.expiry_time, NOW()) + (%s || ' hours')::INTERVAL;
            """, (user_id, str(hours), str(hours)))
            conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"DB Error (set_user_subscription_db): {e}")
    finally:
        if conn:
            release_db(conn)

def remove_user_subscription_db(user_id: int) -> bool:
    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM subscriptions WHERE user_id = %s;", (user_id,))
            deleted = cur.rowcount > 0
            conn.commit()
            return deleted
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"DB Error (remove_user_subscription_db): {e}")
        return False
    finally:
        if conn:
            release_db(conn)

def get_subscription_details_db(user_id: int):
    conn = None
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT expiry_time FROM subscriptions WHERE user_id = %s;", (user_id,))
            row = cur.fetchone()
            return row['expiry_time'].strftime("%Y-%m-%d %H:%M:%S UTC") if row else None
    except Exception as e:
        logger.error(f"DB Error (get_subscription_details_db): {e}")
        return None
    finally:
        if conn:
            release_db(conn)

def get_stats_data_db():
    conn = None
    try:
        conn = get_db()
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
    finally:
        if conn:
            release_db(conn)

def get_all_active_users_db():
    conn = None
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT user_id FROM subscriptions WHERE expiry_time > NOW();")
            return [row['user_id'] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"DB Error (get_all_active_users_db): {e}")
        return []
    finally:
        if conn:
            release_db(conn)

def get_all_users_db():
    conn = None
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT user_id FROM users;")
            return [row['user_id'] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"DB Error (get_all_users_db): {e}")
        return []
    finally:
        if conn:
            release_db(conn)

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
        msg = await query.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode="Markdown")
        track_message(user.id, msg.message_id)
    else:
        msg = await update.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode="Markdown")
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
        f"💳 **PAYMENT DETAILS ({duration} Plan)**\n\n"
        f"💵 **Amount to Pay:** {price}\n\n"
        "📍 **Payment Steps:**\n"
        "1. Scan the QR Code below and make payment.\n"
        "2. Send the payment **Screenshot directly to this chat**.\n\n"
        "⚡ Instant access will be granted after verification."
    )

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]])

    sent_msg = await context.bot.send_photo(
        chat_id=user_id,
        photo=QR_FILE_ID,
        caption=payment_text,
        reply_markup=keyboard,
        parse_mode="Markdown"
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

    sent_msg = await context.bot.send_video(
        chat_id=user_id,
        video=video['file_id'],
        caption=video['caption'] or "✨ *Premium Video*",
        reply_markup=get_nav_keyboard(),
        parse_mode="Markdown"
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
        else:  # nav_next
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

        try:
            await query.edit_message_media(
                media=InputMediaVideo(
                    media=video['file_id'],
                    caption=video['caption'] or "✨ *Premium Video*",
                    parse_mode="Markdown"
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
                caption=video['caption'] or "✨ *Premium Video*",
                reply_markup=get_nav_keyboard(),
                parse_mode="Markdown"
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
# 6. STRICT VIDEO AUTO-SAVE & APPROVAL SYSTEM
# ---------------------------------------------------------
async def handle_photo_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    reset_inactivity_timer(user.id, context)
    photo_file_id = update.message.photo[-1].file_id

    track_message(user.id, update.message.message_id)

    if user and user.id == ADMIN_ID:
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

    msg = await update.message.reply_text("✅ **Screenshot Received!** Admin verification ke baad aapka access active ho jayega.")
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
        await query.edit_message_caption(caption=f"✅ **APPROVED:** User `{target_user_id}` ko {hours} Hours ka access de diya.", parse_mode="Markdown")

        sent_msg = await context.bot.send_message(
            chat_id=target_user_id,
            text=f"🎉 **Payment Verified!** Aapko **{hours} Ghante** ka access mil gaya hai. /start dabakar Open Course par click karein!",
            parse_mode="Markdown"
        )
        track_message(target_user_id, sent_msg.message_id)
    elif action == "rej":
        await query.edit_message_caption(caption=f"❌ **REJECTED:** User `{target_user_id}` ka payment reject kiya gaya.", parse_mode="Markdown")

        sent_msg = await context.bot.send_message(
            chat_id=target_user_id,
            text="❌ **Payment Verification Failed!** Please send valid payment screenshot.",
            parse_mode="Markdown"
        )
        track_message(target_user_id, sent_msg.message_id)

async def auto_upload_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if not msg:
        return

    # 1. CHANNEL CHECK
    if update.channel_post:
        if update.channel_post.chat.id != ALLOWED_CHANNEL_ID:
            logger.warning(f"Ignored video from unauthorized channel ID: {update.channel_post.chat.id}")
            return

    # 2. PRIVATE CHAT CHECK (Only Admin allowed)
    if update.message:
        if update.message.from_user and update.message.from_user.id != ADMIN_ID:
            await update.message.reply_text("⚠️ Access Denied! Only Admin can upload videos directly.")
            return

    # 3. Extract File ID
    file_id = None
    if msg.video:
        file_id = msg.video.file_id
    elif msg.document and msg.document.mime_type and msg.document.mime_type.startswith("video/"):
        file_id = msg.document.file_id

    if not file_id:
        return

    caption = msg.caption or ""

    # 4. Database Save Helper
    def _quick_db_save():
        conn = None
        try:
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO videos (file_id, caption) VALUES (%s, %s) ON CONFLICT (file_id) DO NOTHING;",
                    (file_id, caption)
                )
                conn.commit()
                return cur.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database insertion error: {e}")
            return 0
        finally:
            if conn:
                release_db(conn)

    # 5. Async Execution and Replies
    try:
        rows = await asyncio.to_thread(_quick_db_save)

        if rows > 0:
            if update.message:
                await update.message.reply_text("Video added in data base")
            elif update.channel_post:
                logger.info(f"✅ Video successfully added to DB from Channel: {file_id}")
                await context.bot.send_message(
                    chat_id=update.channel_post.chat.id,
                    text="Video added in data base",
                    reply_to_message_id=update.channel_post.message_id,
                )
        else:
            if update.message:
                await update.message.reply_text("ℹ️ Ye Video pehle se Database me save hai.")
            elif update.channel_post:
                logger.info(f"ℹ️ Video already exists in DB: {file_id}")

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
        await update.message.reply_text(f"✅ User `{target_id}` ko {hours} Hours ka access de diya gaya hai.", parse_mode="Markdown")
        
        sent_msg = await context.bot.send_message(
            target_id,
            f"🎉 **Access Granted!** Admin ne aapko {hours} Hours ka access de diya hai. /start karein!",
            parse_mode="Markdown"
        )
        track_message(target_id, sent_msg.message_id)
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
            sent_msg = await context.bot.send_message(target_id, "⚠️ Aapka course access Revoke kar diya gaya hai.", parse_mode="Markdown")
            track_message(target_id, sent_msg.message_id)
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

    v_count, u_count, total_users = await asyncio.to_thread(get_stats_data_db)
    msg = (
        "📊 **BOT DASHBOARD STATS**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎬 **Total Videos in DB:** {v_count}\n"
        f"👥 **Active Subscribers:** {u_count}\n"
        f"🌐 **Total Registered Users:** {total_users}\n"
        "━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("⚠️ Format:\n`/broadcast <Message>` (Sirf Active Users ko)\n`/broadcast all <Message>` (Sabhi Users ko)", parse_mode="Markdown")
        return

    target_type = "active"
    args = context.args.copy()

    if args[0].lower() == "all":
        target_type = "all"
        args.pop(0)

    broadcast_msg = " ".join(args)
    if not broadcast_msg:
        await update.message.reply_text("⚠️ Content khaali hai! Message likhein.", parse_mode="Markdown")
        return

    if target_type == "all":
        users = await asyncio.to_thread(get_all_users_db)
    else:
        users = await asyncio.to_thread(get_all_active_users_db)

    sent = 0
    for uid in users:
        try:
            sent_m = await context.bot.send_message(uid, f"📢 **ANNOUNCEMENT:**\n\n{broadcast_msg}", parse_mode="Markdown")
            track_message(uid, sent_m.message_id)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    await update.message.reply_text(f"📢 Broadcast `{sent}/{len(users)}` (`{target_type.upper()}`) users ko bhej diya gaya.", parse_mode="Markdown")

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

    # User Handlers Registration
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

    # Media Handlers
    app.add_handler(CallbackQueryHandler(handle_approval, pattern="^(app_|rej_)"))
    app.add_handler(
        MessageHandler(
            (filters.VIDEO | filters.Document.VIDEO) & (filters.ChatType.PRIVATE | filters.ChatType.CHANNEL),
            auto_upload_video
        )
    )
    app.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, handle_photo_received))

    logger.info("Bot is running seamlessly...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
