import asyncio
import datetime
import logging
import os
import re
from typing import Optional, Tuple, List
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaVideo,
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
# DUMMY WEB SERVER FOR RENDER HEALTH CHECK
# ---------------------------------------------------------
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Bot is active and running!")

    def log_message(self, format, *args):
        return

def run_health_check_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    server.serve_forever()

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
# CONFIGURATION
# ---------------------------------------------------------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN Environment Variable missing!")

ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "7572036863"))

RAW_DATABASE_URL = os.getenv("DATABASE_URL")
if not RAW_DATABASE_URL:
    raise ValueError("DATABASE_URL Environment Variable missing!")

if RAW_DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = RAW_DATABASE_URL.replace("postgres://", "postgresql://", 1)
else:
    DATABASE_URL = RAW_DATABASE_URL

QR_FILE_ID = "AgACAgUAAxkBAAMFamo9AXr8yxJhM9AJuipowCr2a9UAAvobaxtNyVFXq59REp-3CE8BAAMCAAN5AAM9BA"

USER_QR_MESSAGES = {}
USER_INACTIVITY_TASKS = {}

# ---------------------------------------------------------
# DATABASE CONNECTION POOL & SETUP
# ---------------------------------------------------------
db_pool: Optional[SimpleConnectionPool] = None

def init_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = SimpleConnectionPool(1, 20, dsn=DATABASE_URL)

def get_db_connection():
    if db_pool is None:
        init_db_pool()
    return db_pool.getconn()

def release_db_connection(conn):
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error releasing db conn: {e}")

def init_db():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    id SERIAL PRIMARY KEY,
                    title TEXT,
                    file_id TEXT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id BIGINT PRIMARY KEY,
                    expiry_time TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_messages (
                    user_id BIGINT,
                    message_id BIGINT,
                    PRIMARY KEY (user_id, message_id)
                )
            """)
            conn.commit()
            logger.info("Database initialized successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error in init_db: {e}")
    finally:
        release_db_connection(conn)

def log_user_message(user_id: int, message_id: int):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO user_messages (user_id, message_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user_id, message_id)
            )
            conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error logging message ID for user {user_id}: {e}")
    finally:
        if conn:
            release_db_connection(conn)

def get_and_clear_user_messages(user_id: int) -> List[int]:
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT message_id FROM user_messages WHERE user_id = %s", (user_id,))
            rows = cursor.fetchall()
            cursor.execute("DELETE FROM user_messages WHERE user_id = %s", (user_id,))
            conn.commit()
            return [r["message_id"] for r in rows]
    except Exception as e:
        conn.rollback()
        logger.error(f"Error getting/clearing user messages: {e}")
        return []
    finally:
        release_db_connection(conn)

def cleanup_expired_users():
    now = datetime.datetime.now()
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM subscriptions WHERE expiry_time <= %s", (now,))
            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error cleaning expired users: {e}")
    finally:
        release_db_connection(conn)

def get_navigated_video(direction: str = "first", current_id: Optional[int] = None) -> Optional[Tuple[int, str, str]]:
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM videos")
            if cursor.fetchone()["count"] == 0:
                return None

            row = None
            if direction == "next" and current_id is not None:
                cursor.execute("SELECT id, title, file_id FROM videos WHERE id > %s ORDER BY id ASC LIMIT 1", (current_id,))
                row = cursor.fetchone()
                if not row:
                    cursor.execute("SELECT id, title, file_id FROM videos ORDER BY id ASC LIMIT 1")
                    row = cursor.fetchone()

            elif direction == "prev" and current_id is not None:
                cursor.execute("SELECT id, title, file_id FROM videos WHERE id < %s ORDER BY id DESC LIMIT 1", (current_id,))
                row = cursor.fetchone()
                if not row:
                    cursor.execute("SELECT id, title, file_id FROM videos ORDER BY id DESC LIMIT 1")
                    row = cursor.fetchone()

            else:
                cursor.execute("SELECT id, title, file_id FROM videos ORDER BY id ASC LIMIT 1")
                row = cursor.fetchone()

            return (row["id"], row["title"], row["file_id"]) if row else None
    except Exception as e:
        conn.rollback()
        logger.error(f"Error in get_navigated_video: {e}")
        return None
    finally:
        release_db_connection(conn)

def set_user_subscription(user_id: int, hours: int):
    expiry = datetime.datetime.now() + datetime.timedelta(hours=hours)
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO subscriptions (user_id, expiry_time)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET expiry_time = EXCLUDED.expiry_time
            """, (user_id, expiry))
            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error setting subscription: {e}")
    finally:
        release_db_connection(conn)

def remove_user_subscription(user_id: int) -> bool:
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM subscriptions WHERE user_id = %s", (user_id,))
            deleted = cursor.rowcount > 0
            conn.commit()
            return deleted
    except Exception as e:
        conn.rollback()
        logger.error(f"Error removing subscription: {e}")
        return False
    finally:
        release_db_connection(conn)

def get_subscription_details(user_id: int) -> Optional[str]:
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT expiry_time FROM subscriptions WHERE user_id = %s", (user_id,))
            row = cursor.fetchone()
            return row["expiry_time"].strftime("%Y-%m-%d %H:%M:%S") if row else None
    except Exception as e:
        conn.rollback()
        return None
    finally:
        release_db_connection(conn)

def is_user_active(user_id: int) -> bool:
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT expiry_time FROM subscriptions WHERE user_id = %s", (user_id,))
            row = cursor.fetchone()

            if not row:
                return False

            expiry_time = row["expiry_time"]
            if datetime.datetime.now() >= expiry_time:
                cursor.execute("DELETE FROM subscriptions WHERE user_id = %s", (user_id,))
                conn.commit()
                return False

            return True
    except Exception as e:
        conn.rollback()
        return False
    finally:
        release_db_connection(conn)

def get_all_active_users() -> List[int]:
    now = datetime.datetime.now()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT user_id FROM subscriptions WHERE expiry_time > %s", (now,))
            return [row["user_id"] for row in cursor.fetchall()]
    except Exception as e:
        conn.rollback()
        return []
    finally:
        release_db_connection(conn)

def get_stats_data() -> Tuple[int, int]:
    now = datetime.datetime.now()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM videos")
            total_videos = cursor.fetchone()["count"]
            cursor.execute("SELECT COUNT(*) as count FROM subscriptions WHERE expiry_time > %s", (now,))
            active_users = cursor.fetchone()["count"]
            return total_videos, active_users
    except Exception as e:
        conn.rollback()
        return 0, 0
    finally:
        release_db_connection(conn)

# Database Initialization
try:
    init_db_pool()
    init_db()
    cleanup_expired_users()
except Exception as err:
    logger.error(f"Database Init Error: {err}")

# ---------------------------------------------------------
# INACTIVITY AUTO-CLEANUP MANAGER
# ---------------------------------------------------------
async def start_inactivity_timer(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    if user_id in USER_INACTIVITY_TASKS:
        USER_INACTIVITY_TASKS[user_id].cancel()

    async def _inactivity_job():
        try:
            await asyncio.sleep(300)
            msg_ids = await asyncio.to_thread(get_and_clear_user_messages, user_id)
            for m_id in msg_ids:
                try:
                    await context.bot.delete_message(chat_id=user_id, message_id=m_id)
                except Exception:
                    pass

            if await asyncio.to_thread(is_user_active, user_id):
                reset_msg = await context.bot.send_message(
                    chat_id=user_id,
                    text="⏳ **Session expired due to inactivity.**\n\nMain menu par jaane ke liye /start dabayein.",
                    parse_mode="Markdown",
                )
                await asyncio.to_thread(log_user_message, user_id, reset_msg.message_id)

        except asyncio.CancelledError:
            pass
        finally:
            USER_INACTIVITY_TASKS.pop(user_id, None)

    task = asyncio.create_task(_inactivity_job())
    USER_INACTIVITY_TASKS[user_id] = task

# ---------------------------------------------------------
# USER BOT FUNCTIONS
# ---------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    menu_text = (
        f"👋 **Namaste {user.first_name}! Welcome to Premium Video Store.**\n\n"
        "📜 **OUR PRICING PACKAGES:**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🥉 **Basic Pass:** ₹10 ➔ 24 Hours Access\n"
        "🥈 **Weekly Pass:** ₹50 ➔ 7 Days Access\n"
        "🥇 **Monthly Pass:** ₹150 ➔ 30 Days Access\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👇 Buy karne ke liye niche button par click karein:"
    )

    keyboard = [
        [InlineKeyboardButton("💎 Buy 24 Hours Access (₹10)", callback_data="buy_24h")],
        [InlineKeyboardButton("🌟 Buy 7 Days Access (₹50)", callback_data="buy_7d")],
        [InlineKeyboardButton("👑 Buy 30 Days Access (₹150)", callback_data="buy_30d")],
        [InlineKeyboardButton("▶️ Open Purchased Course", callback_data="open_course")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        query = update.callback_query
        await query.answer()
        sent_msg = await query.message.reply_text(
            menu_text, reply_markup=reply_markup, parse_mode="Markdown"
        )
    else:
        sent_msg = await update.message.reply_text(
            menu_text, reply_markup=reply_markup, parse_mode="Markdown"
        )

    await asyncio.to_thread(log_user_message, user.id, sent_msg.message_id)

async def handle_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    plan = query.data
    price, duration = "", ""

    if plan == "buy_24h":
        price, duration = "₹10", "24 Hours"
    elif plan == "buy_7d":
        price, duration = "₹50", "7 Days"
    elif plan == "buy_30d":
        price, duration = "₹150", "30 Days"

    payment_text = (
        f"💳 **PAYMENT DETAILS ({duration} Plan)**\n\n"
        f"💵 **Amount to Pay:** {price}\n\n"
        "📍 **Payment Process:**\n"
        "1. Upar diye gaye QR Code ko scan karke pay karein.\n"
        "2. Payment ka **Screenshot DIRECT Bot chat me bhejey**.\n\n"
        "⚡ Screenshot bhejte hi humari team verify karke aapka access active kar degi!"
    )

    keyboard = [[InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]]

    sent_msg = await context.bot.send_photo(
        chat_id=query.from_user.id,
        photo=QR_FILE_ID,
        caption=payment_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )
    USER_QR_MESSAGES[query.from_user.id] = sent_msg.message_id
    await asyncio.to_thread(log_user_message, query.from_user.id, sent_msg.message_id)

async def open_course(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    if not await asyncio.to_thread(is_user_active, user_id):
        msg = await query.message.reply_text(
            "❌ Aapke paas koi active access nahi hai ya time limit KHATAM ho gaya hai! Pehle /start dabakar plan buy karein."
        )
        await asyncio.to_thread(log_user_message, user_id, msg.message_id)
        return

    await render_video_message(context, user_id, edit_query=None, direction="first")

async def render_video_message(context: ContextTypes.DEFAULT_TYPE, user_id: int, edit_query=None, direction: str = "first", current_video_id: Optional[int] = None):
    video = await asyncio.to_thread(get_navigated_video, direction, current_video_id)
    if not video:
        msg_text = "⚠️ Course me abhi koi video uploaded nahi hai."
        if edit_query:
            sent_msg = await edit_query.message.reply_text(msg_text)
        else:
            sent_msg = await context.bot.send_message(user_id, msg_text)
        await asyncio.to_thread(log_user_message, user_id, sent_msg.message_id)
        return

    video_id, title, file_id = video

    buttons = [
        [
            InlineKeyboardButton("◀️ Previous", callback_data=f"nav_prev_{video_id}"),
            InlineKeyboardButton("Next ▶️", callback_data=f"nav_next_{video_id}"),
        ],
        [
            InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
            InlineKeyboardButton("📊 Access Status", callback_data="check_status"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)

    clean_caption = "🎬 *Exclusive Premium Content*"
    if title:
        filtered_title = re.sub(r'(?i)(video\s*|index\s*|ep\s*|episode\s*)?part\s*[-_:]?\s*\d+', '', title).strip()
        filtered_title = re.sub(r'(?i)^(video|index|part)\s*[-_:]?\s*\d+', '', filtered_title).strip()
        if filtered_title:
            clean_caption = f"🎬 **{filtered_title}**\n\n✨ *Exclusive Premium Content*"

    if edit_query:
        try:
            await edit_query.edit_message_media(
                media=InputMediaVideo(
                    media=file_id, caption=clean_caption, parse_mode="Markdown"
                ),
                reply_markup=reply_markup,
            )
            await start_inactivity_timer(context, user_id)
            return
        except Exception as e:
            logger.warning(f"Failed to edit video media directly: {e}")
            try:
                await edit_query.message.delete()
            except Exception:
                pass

    sent_msg = await context.bot.send_video(
        chat_id=user_id,
        video=file_id,
        caption=clean_caption,
        reply_markup=reply_markup,
        parse_mode="Markdown",
        protect_content=True,
    )
    await asyncio.to_thread(log_user_message, user_id, sent_msg.message_id)
    await start_inactivity_timer(context, user_id)

async def handle_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Loading video...")
    user_id = query.from_user.id

    if not await asyncio.to_thread(is_user_active, user_id):
        msg = await query.message.reply_text("⌛ Access Expired! Naya plan khareedne ke liye /start karein.")
        await asyncio.to_thread(log_user_message, user_id, msg.message_id)
        return

    data_parts = query.data.split("_")
    direction = data_parts[1]
    current_video_id = int(data_parts[2]) if len(data_parts) > 2 and data_parts[2].isdigit() else None

    await render_video_message(context, user_id, edit_query=query, direction=direction, current_video_id=current_video_id)

async def check_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    expiry = await asyncio.to_thread(get_subscription_details, user_id)
    
    if expiry:
        await query.answer(f"✅ Your Access Expiry:\n{expiry}", show_alert=True)
    else:
        await query.answer("❌ No active access plan found.", show_alert=True)

# ---------------------------------------------------------
# ADMIN COMMANDS
# ---------------------------------------------------------
async def grant_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_CHAT_ID:
        return

    try:
        target_id = int(context.args[0])
        hours = int(context.args[1])
        await asyncio.to_thread(set_user_subscription, target_id, hours)
        await update.message.reply_text(f"✅ Success! User `{target_id}` ko {hours} Hours ka access diya gaya.", parse_mode="Markdown")

        msg = await context.bot.send_message(
            target_id,
            f"🎉 **Payment Verified!** Aapko {hours} Ghante ka access mil gaya hai. /start dabayein!",
            parse_mode="Markdown"
        )
        await asyncio.to_thread(log_user_message, target_id, msg.message_id)
    except Exception:
        await update.message.reply_text("❌ Format: `/grant <USER_ID> <HOURS>`", parse_mode="Markdown")

async def revoke_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_CHAT_ID:
        return

    try:
        target_id = int(context.args[0])
        removed = await asyncio.to_thread(remove_user_subscription, target_id)

        if removed:
            await update.message.reply_text(
                f"🔴 **Access Cancelled!** User `{target_id}` ka access revoke kar diya gaya hai.",
                parse_mode="Markdown",
            )

            if target_id in USER_INACTIVITY_TASKS:
                USER_INACTIVITY_TASKS[target_id].cancel()

            msg_ids = await asyncio.to_thread(get_and_clear_user_messages, target_id)
            for m_id in msg_ids:
                try:
                    await context.bot.delete_message(chat_id=target_id, message_id=m_id)
                except Exception:
                    pass

            await context.bot.send_message(
                chat_id=target_id,
                text="⚠️ **Notice:** Aapka course access Admin dwara Cancel kar diya gaya hai.",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(f"⚠️ User `{target_id}` ka koi active access nahi mila.", parse_mode="Markdown")

    except Exception:
        await update.message.reply_text("❌ Format: `/revoke <USER_ID>`", parse_mode="Markdown")

async def userinfo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_CHAT_ID:
        return

    try:
        target_id = int(context.args[0])
        expiry = await asyncio.to_thread(get_subscription_details, target_id)

        if expiry:
            active = await asyncio.to_thread(is_user_active, target_id)
            status = "🟢 ACTIVE" if active else "🔴 EXPIRED"
            await update.message.reply_text(
                f"👤 **User Info (`{target_id}`):**\n• Status: {status}\n• Expiry Date/Time: `{expiry}`",
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text(f"❌ User `{target_id}` ke paas koi active subscription record nahi hai.", parse_mode="Markdown")

    except Exception:
        await update.message.reply_text("❌ Format: `/userinfo <USER_ID>`", parse_mode="Markdown")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_CHAT_ID:
        return

    total_videos, active_users = await asyncio.to_thread(get_stats_data)
    stats_msg = (
        "📊 **ADMIN BOT DASHBOARD**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎬 **Total Videos in DB:** {total_videos}\n"
        f"👑 **Active Subscribers:** {active_users}\n"
        "━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(stats_msg, parse_mode="Markdown")

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_CHAT_ID:
        return

    if not context.args:
        await update.message.reply_text("❌ Usage: `/broadcast <Aapka Message>`", parse_mode="Markdown")
        return

    broadcast_message = " ".join(context.args)
    active_users = await asyncio.to_thread(get_all_active_users)

    sent_count = 0
    for uid in active_users:
        try:
            msg = await context.bot.send_message(
                chat_id=uid,
                text=f"📢 **ADMIN ANNOUNCEMENT:**\n\n{broadcast_message}",
                parse_mode="Markdown"
            )
            await asyncio.to_thread(log_user_message, uid, msg.message_id)
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.warning(f"Could not send broadcast to {uid}: {e}")

    await update.message.reply_text(f"📢 **Broadcast Sent!** Delivered to `{sent_count}/{len(active_users)}` active users.", parse_mode="Markdown")

# ---------------------------------------------------------
# PAYMENT & MEDIA HANDLERS
# ---------------------------------------------------------
async def handle_photo_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    photo_file_id = update.message.photo[-1].file_id

    if user.id == ADMIN_CHAT_ID:
        await update.message.reply_text(f"🖼️ **QR Code File ID:**\n\n`{photo_file_id}`", parse_mode="Markdown")
        return

    admin_markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve (24 Hours)", callback_data=f"app_{user.id}_24"),
            InlineKeyboardButton("✅ Approve (7 Days)", callback_data=f"app_{user.id}_168"),
        ],
        [InlineKeyboardButton("✅ Approve (30 Days)", callback_data=f"app_{user.id}_720")],
        [InlineKeyboardButton("❌ Reject Payment", callback_data=f"rej_{user.id}")],
    ])

    await context.bot.send_photo(
        chat_id=ADMIN_CHAT_ID,
        photo=photo_file_id,
        caption=f"📥 **NEW PAYMENT SCREENSHOT!**\n\n👤 **User:** {user.first_name}\n🆔 **User ID:** `{user.id}`",
        reply_markup=admin_markup,
        parse_mode="Markdown",
    )

    msg = await update.message.reply_text("✅ **Screenshot Received!** Admin verification ke baad aapka access start kar diya jayega.")
    await asyncio.to_thread(log_user_message, user.id, msg.message_id)

async def handle_approval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.from_user.id != ADMIN_CHAT_ID:
        return

    data = query.data.split("_")
    action, target_user_id = data[0], int(data[1])

    if target_user_id in USER_QR_MESSAGES:
        try:
            msg_id = USER_QR_MESSAGES.pop(target_user_id)
            await context.bot.delete_message(chat_id=target_user_id, message_id=msg_id)
        except Exception as e:
            logger.warning(f"Could not delete QR message for user {target_user_id}: {e}")

    if action == "app":
        hours = int(data[2])
        await asyncio.to_thread(set_user_subscription, target_user_id, hours)
        await query.edit_message_caption(caption=f"✅ **APPROVED:** User `{target_user_id}` ko {hours} Hours ka access de diya.", parse_mode="Markdown")

        msg = await context.bot.send_message(
            chat_id=target_user_id,
            text=f"🎉 **Payment Verified!** Aapko **{hours} Ghante** ka access mil gaya hai. /start dabakar Open Course par click karein!",
            parse_mode="Markdown"
        )
        await asyncio.to_thread(log_user_message, target_user_id, msg.message_id)

    elif action == "rej":
        await query.edit_message_caption(caption=f"❌ **REJECTED:** User `{target_user_id}` ka payment reject hua.", parse_mode="Markdown")

        msg = await context.bot.send_message(
            chat_id=target_user_id,
            text="❌ **Payment Verification Failed!** Please send valid screenshot.",
            parse_mode="Markdown"
        )
        await asyncio.to_thread(log_user_message, target_user_id, msg.message_id)

async def auto_upload_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_CHAT_ID:
        return

    video_obj = update.message.video
    if not video_obj:
        await update.message.reply_text("❌ Valid video file nahi mili.")
        return

    file_id = video_obj.file_id
    caption = update.message.caption or ""

    def _insert():
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO videos (title, file_id) VALUES (%s, %s) RETURNING id",
                    (caption, file_id)
                )
                inserted_id = cursor.fetchone()[0]
                conn.commit()
                return inserted_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting video: {e}")
            raise e
        finally:
            release_db_connection(conn)

    try:
        video_id = await asyncio.to_thread(_insert)
        await update.message.reply_text(
            f"✅ **Video Saved to Database!**\n🆔 **DB Video ID:** `{video_id}`",
            parse_mode="Markdown"
        )
    except Exception as err:
        await update.message.reply_text(
            f"❌ **Failed to Save Video!**\nError: `{err}`",
            parse_mode="Markdown"
        )

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_buy, pattern="^buy_"))
    app.add_handler(CallbackQueryHandler(open_course, pattern="^open_course$"))
    app.add_handler(CallbackQueryHandler(start, pattern="^main_menu$"))
    app.add_handler(CallbackQueryHandler(handle_navigation, pattern="^nav_"))
    app.add_handler(CallbackQueryHandler(check_status_callback, pattern="^check_status$"))

    app.add_handler(CommandHandler("grant", grant_cmd))
    app.add_handler(CommandHandler("revoke", revoke_cmd))
    app.add_handler(CommandHandler("userinfo", userinfo_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))

    app.add_handler(CallbackQueryHandler(handle_approval, pattern="^(app_|rej_)"))
    app.add_handler(MessageHandler(filters.VIDEO, auto_upload_video))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo_received))

    logger.info("🚀 Production Ready PostgreSQL Bot is Running!")
    app.run_polling(drop_pending_updates=True)
