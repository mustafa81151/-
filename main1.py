import os
import logging
import threading
import time
import asyncio
import sqlite3
import shutil
import json
from datetime import datetime, timedelta
from collections import defaultdict
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telegram.ext import ChatMemberHandler
from telegram.error import BadRequest

# ==================== استيراد database.py (SQLite) ====================
from database import (
    init_database,
    load_users,
    save_users,
    load_data,
    save_data,
    get_user_data,
    update_user_data,
    is_admin,
    is_banned,
    is_muted,
    update_stat,
    get_stat,
    create_default_user_data,
    get_channel_data,
    save_channel_data,
    load_channels,
    delete_channel,
    backup_database,
    DB_NAME
)
from datetime import datetime
from database import DB_NAME, load_data, save_data, get_user_data, update_user_data

logger = logging.getLogger(__name__)

# ==================== متغيرات التحكم بالتقارب ====================
_active_locks = {}  # لتتبع العمليات النشطة ومنع التضارب
"""
هذا القاموس يمنع تشغيل أكثر من عملية في نفس الوقت
المفاتيح:
- "monitor_running": مراقبة القنوات
- "subscription_check_running": الفحص الدوري
- "verify_lock_{user_id}_{channel_id}": التحقق من الانضمام
- "join_lock_{user_id}_{channel_id}": عملية الانضمام

القيمة: True (إذا كانت العملية نشطة)
"""

_join_locks = {}    # أقفال خاصة بعمليات الانضمام
"""
مثال: _join_locks["123456_@channel"] = True
يمنع المستخدم 123456 من الانضمام للقناة @channel أكثر من مرة في نفس الوقت
"""

_verify_locks = {}  # أقفال خاصة بعمليات التحقق
"""
مثال: _verify_locks["123456_@channel"] = True
يمنع التحقق المتكرر لنفس المستخدم والقناة
"""

# ==================== وظائف إدارة الأقفال ====================




def acquire_lock(lock_key, timeout=10):
    """
    الحصول على قفل مع فترة انتظار
    """
    start_time = time.time()
    while lock_key in _active_locks:
        if time.time() - start_time > timeout:
            return False
        time.sleep(0.1)
    _active_locks[lock_key] = True
    return True

def release_lock(lock_key):
    """
    تحرير قفل
    """
    if lock_key in _active_locks:
        del _active_locks[lock_key]
    return True

def is_locked(lock_key):
    """
    التحقق إذا كان القفل نشطاً
    """
    return lock_key in _active_locks

def clear_expired_locks():
    """
    تنظيف الأقفال المنتهية (أقدم من 5 دقائق)
    """
    current_time = time.time()
    expired_keys = []
    
    for key, lock_time in list(_active_locks.items()):
        if isinstance(lock_time, (int, float)):
            if current_time - lock_time > 300:  # 5 دقائق
                expired_keys.append(key)
        elif current_time - time.time() > 300:
            expired_keys.append(key)
    
    for key in expired_keys:
        if key in _active_locks:
            del _active_locks[key]
    
    if expired_keys:
        logger.info(f"🧹 تم تنظيف {len(expired_keys)} قفل منتهي")
    
    return len(expired_keys)

# ==================== مهمة تنظيف دورية للأقفال ====================

async def cleanup_locks_task(context: ContextTypes.DEFAULT_TYPE = None):
    """
    مهمة دورية لتنظيف الأقفال المنتهية
    """
    try:
        cleared = clear_expired_locks()
        if cleared > 0:
            logger.debug(f"✅ تم تنظيف {cleared} قفل منتهي")
    except Exception as e:
        logger.error(f"❌ خطأ في تنظيف الأقفال: {e}")

async def safe_edit(query, text, reply_markup=None):
    try:
        if query.message and query.message.text != text:
            await query.edit_message_text(text=text, reply_markup=reply_markup)
    except BadRequest:
        pass

# إعدادات البوت
TOKEN = "8653078106:AAExKb_61ifvuqFX1bkEv_KiB4R88iTm00k"
ADMIN_ID = 8401168362
BOT_CHANNEL = "@TUX3T"

# ========== المسارات المحلية ===========
current_dir = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(current_dir, "backups")

os.makedirs(BACKUP_DIR, exist_ok=True)

# نظام التحديثات
BACKUP_INTERVAL = 1800
_last_backup_time = 0

# إعدادات متقدمة
CACHE_TTL = 30
ACTION_COOLDOWNS = {
    "join_channel": 10,
    "verify_channel": 5,
    "daily_gift": 1,
    "store": 2,
    "admin": 0.5,
    "general": 1
}

# نظام Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot_debug.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# أنظمة التخزين المؤقت
_data_cache = {}
_cache_lock = threading.Lock()
_cache_last_update = {}
_cooldown_cache = {}
# ===================== مدير Cooldown =====================

class CooldownManager:
    def __init__(self):
        self.cooldowns = defaultdict(dict)
        self.transaction_ids = set()
        self.lock = threading.Lock()
    
    def can_proceed(self, user_id, action_type, transaction_id=None):
        user_id = str(user_id)
        
        with self.lock:
            if transaction_id and transaction_id in self.transaction_ids:
                return False, 0, "معاملة مكررة"
            
            current_time = time.time()
            
            if user_id in self.cooldowns and action_type in self.cooldowns[user_id]:
                last_time = self.cooldowns[user_id][action_type]
                cooldown = ACTION_COOLDOWNS.get(action_type, 2)
                
                if current_time - last_time < cooldown:
                    remaining = cooldown - (current_time - last_time)
                    return False, remaining, "في فترة انتظار"
            
            self.cooldowns[user_id][action_type] = current_time
            if transaction_id:
                self.transaction_ids.add(transaction_id)
            
            return True, 0, "يمكن المتابعة"
    
    def clear_old_transactions(self):
        with self.lock:
            current_time = time.time()
            self.transaction_ids = {tid for tid in self.transaction_ids 
                                  if not tid.startswith('tx_') or 
                                  current_time - int(tid.split('_')[-1]) / 1000 < 86400}
    
    def mark_transaction_complete(self, transaction_id):
        with self.lock:
            self.transaction_ids.discard(transaction_id)

cooldown_manager = CooldownManager()

# ===================== أقفال =====================

_user_locks = {}
_point_locks = {}
_verify_locks = {}
_daily_locks = {}
_store_locks = {}

# ===================== نظام إدارة النقاط =====================

import time
import threading

def safe_add_points(user_id, points, operation="add", action_type=None, transaction_id=None):
    """
    إضافة / خصم نقاط بشكل آمن مع السماح بالسالب
    """
    import time
    import threading
    
    user_id = str(user_id)

    if not transaction_id:
        transaction_id = f"auto_{user_id}_{int(time.time() * 1000)}"

    # قفل خاص بالمستخدم
    lock_key = f"points_{user_id}"
    
    # استيراد _point_locks من main إذا لزم
    # أو إنشاء واحد جديد
    try:
        from __main__ import _point_locks
    except:
        _point_locks = {}
    
    _point_locks.setdefault(lock_key, threading.Lock())

    with _point_locks[lock_key]:
        # إعادة تحميل البيانات من المصدر
        user_data = get_user_data(user_id, force_reload=True)

        # منع التكرار
        transactions = user_data.get("transactions", [])
        for tx in transactions:
            if tx.get("id") == transaction_id:
                logger.warning(f"⛔ معاملة مكررة تم منعها: {transaction_id}")
                return False, "⚠️ العملية منفذة مسبقاً"

        current_points = user_data.get("points", 0)

        # تحديد الرصيد الجديد
        if operation == "add":
            new_points = current_points + points
        elif operation == "subtract":
            # ✅ السماح بالنقاط السالبة
            new_points = current_points - points
            
            # تحذير إذا صار بالسالب
            if new_points < 0:
                logger.warning(f"⚠️ نقاط سالبة للمستخدم {user_id}: {new_points}")
        else:
            return False, "❌ نوع العملية غير معروف"

        updates = {
            "points": new_points
        }

        # تحديث الإحصائيات
        if operation == "add":
            updates["total_earned"] = user_data.get("total_earned", 0) + points
        else:
            updates["total_spent"] = user_data.get("total_spent", 0) + points

        # تحديث ذري مع retry
        success = update_user_data(
            user_id,
            updates,
            action_type or "points_update",
            transaction_id
        )

        if not success:
            return False, "❌ فشل تحديث النقاط"

        # تحديث إحصائيات النظام
        if points > 0 and action_type != "stats_update":
            update_stat(
                "total_points",
                points if operation == "add" else -points
            )

        logger.info(
            f"✅ نقاط محدثة | user={user_id} | "
            f"op={operation} | value={points} | "
            f"new={new_points} | tx={transaction_id}"
        )

        return True, "✅ تمت العملية بنجاح"



def check_and_fix_database():
    """التحقق وإصلاح قاعدة البيانات عند بدء التشغيل"""
    logger.info("🔍 التحقق من قاعدة البيانات...")
    
    try:
        # التحقق من وجود الملف
        if not os.path.exists(DB_NAME):
            logger.error(f"❌ ملف {DB_NAME} غير موجود!")
            return False
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # التحقق من الجداول الأساسية
        required_tables = ['users', 'channels', 'stats', 'transactions', 'admins']
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [table[0] for table in cursor.fetchall()]
        
        # التحقق من أعمدة users المهمة
        cursor.execute("PRAGMA table_info(users)")
        user_columns = [col[1] for col in cursor.fetchall()]
        
        required_columns = ['force_sub_left', 'force_sub_passed', 'registered']
        missing_columns = [col for col in required_columns if col not in user_columns]
        
        conn.close()
        
        if missing_columns:
            logger.warning(f"⚠️ أعمدة مفقودة: {missing_columns}")
            # محاولة الإصلاح
            init_database()
            return False
        else:
            logger.info("✅ قاعدة البيانات سليمة وجاهزة")
            
            # تسجيل بعض الإحصائيات
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*), SUM(points) FROM users")
            count, points = cursor.fetchone()
            logger.info(f"📊 النظام: {count} مستخدم، {points or 0} نقطة")
            conn.close()
            
            return True
            
    except Exception as e:
        logger.error(f"❌ خطأ في التحقق من قاعدة البيانات: {e}")
        # محاولة إعادة التهيئة
        init_database()
        return False
# ===================== نظام الكتم =====================

def add_muted_user(user_id, mute_duration=None, reason=""):
    """إضافة مستخدم مكتوم"""
    data = load_data()
    user_id = str(user_id)
    
    mute_info = {
        "muted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "reason": reason,
        "muted_by": ADMIN_ID
    }
    
    if mute_duration:
        mute_until = datetime.now() + timedelta(seconds=mute_duration)
        mute_info["until"] = mute_until.strftime("%Y-%m-%d %H:%M:%S")
        mute_info["duration"] = mute_duration
    
    data["muted_users"][user_id] = mute_info
    data["stats"]["total_mutes"] = data["stats"].get("total_mutes", 0) + 1
    save_data(data)
    
    return mute_info

def remove_muted_user(user_id):
    """إزالة مستخدم من قائمة المكتومين"""
    data = load_data()
    user_id = str(user_id)
    
    if user_id in data.get("muted_users", {}):
        del data["muted_users"][user_id]
        if save_data(data):
            return True
    
    return False

async def cleanup_expired_mutes(job=None):
    """تنظيف الكتم المنتهي"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # الحصول على جميع المستخدمين المكتومين
        cursor.execute('SELECT user_id FROM muted_users')
        muted_users = cursor.fetchall()
        
        current_time = datetime.now()
        
        for (user_id,) in muted_users:
            try:
                user_id_str = str(user_id)
                
                # التحقق من وقت انتهاء الكتم
                cursor.execute(
                    'SELECT until FROM muted_users WHERE user_id = ?',
                    (user_id_str,)
                )
                result = cursor.fetchone()
                
                if result:
                    until_str = result[0]
                    
                    # إذا كان الكتم دائمًا (قيمة "دائم")
                    if until_str == "دائم":
                        continue  # لا تحذف الكتم الدائم
                    
                    try:
                        # تحويل النص إلى وقت
                        until_time = datetime.strptime(until_str, "%Y-%m-%d %H:%M:%S")
                        
                        # التحقق إذا انتهى الكتم
                        if current_time >= until_time:
                            cursor.execute(
                                'DELETE FROM muted_users WHERE user_id = ?',
                                (user_id_str,)
                            )
                            logger.info(f"✅ تمت إزالة الكتم المنتهي عن المستخدم: {user_id_str}")
                            
                    except ValueError as e:
                        # إذا كان تنسيق الوقت غير صالح
                        logger.warning(f"⚠️ تنسوق وقت غير صالح للمستخدم {user_id_str}: {until_str}")
                        continue
                        
            except Exception as user_error:
                logger.warning(f"⚠️ خطأ في معالجة {user_id}: {user_error}")
                continue
        
        conn.commit()
        conn.close()
        
        logger.info("✅ تم تنظيف الكتم المنتهي بنجاح")
        
    except Exception as e:
        logger.error(f"❌ خطأ في cleanup_expired_mutes: {e}")
        import traceback
        traceback.print_exc()

def format_time(seconds):
    """تحويل الثواني إلى نص"""
    if seconds == 0:
        return "دائم"
    
    days = seconds // (24 * 3600)
    seconds %= (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    
    result = []
    if days > 0:
        result.append(f"{days} يوم")
    if hours > 0:
        result.append(f"{hours} ساعة")
    if minutes > 0:
        result.append(f"{minutes} دقيقة")
    if seconds > 0:
        result.append(f"{seconds} ثانية")
    
    return " و ".join(result) if result else "0 ثانية"

# ===================== وظائف مساعدة =====================

def find_user_by_username(username):
    """البحث عن مستخدم باليوزر"""
    users_data = load_users()
    username = username.replace("@", "").lower()
    
    for uid, user_data in users_data.items():
        if user_data.get("username", "").lower() == username:
            return uid
    return None

async def send_to_admin(bot, message):
    """إرسال رسالة للمالك"""
    try:
        await bot.send_message(ADMIN_ID, message, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error sending to admin: {e}")

def can_user_buy_channel(user_id, channel_id):
    """
    منع طلب أعضاء لقناة نشطة + منع الطلب المكرر
    """
    user_id = str(user_id)
    channel_id = str(channel_id)

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT status, completed 
        FROM channels 
        WHERE channel_id = ?
    """, (channel_id,))
    
    result = cursor.fetchone()
    conn.close()

    if not result:
        return False, "❌ القناة غير موجودة"

    status, completed = result

    # ✅ إذا القناة نشطة
    if status == "active":
        return False, "⚠️ القناة حالياً نشطة ولا يمكن طلب أعضاء مرة ثانية"

    # ✅ إذا القناة مكتملة
    if completed:
        return False, "✅ هذه القناة مكتملة بالفعل"

    return True, "يمكن الطلب"

def can_user_join_reactivated_channel(user_id, channel_id, channel_data):
    """التحقق من إمكانية الانضمام للقناة المُعاد تفعيلها - نسخة SQLite"""
    import json
    user_id = str(user_id)
    
    # إذا كان المستخدم صاحب القناة
    if user_id == channel_data.get("owner"):
        return False, "لا يمكنك الانضمام لقناتك!"
    
    user_data = get_user_data(user_id)
    joined_channels = user_data.get("joined_channels", {})
    if isinstance(joined_channels, str):
        joined_channels = json.loads(joined_channels) if joined_channels else {}
    
    # إذا لم ينضم للقناة من قبل
    if channel_id not in joined_channels:
        return True, ""
    
    join_info = joined_channels[channel_id]
    current_round = channel_data.get("reuse_count", 0)
    user_round = join_info.get("round", 0)
    
    # إذا كانت القناة أعيد تفعيلها (جولة جديدة)
    if current_round > user_round:
        return True, ""
    
    # نفس الجولة
    if user_round == current_round:
        if join_info.get("verified", False) and not join_info.get("left", False):
            return False, "لقد انضممت لهذه القناة مسبقاً!"
        if join_info.get("left", False):
            return False, "غادرت هذه القناة في هذه الدورة!"
    
    # حالة left_completed
    if join_info.get("left_completed", False):
        completed_round = join_info.get("completed_round", 0)
        
        if current_round > completed_round:
            return True, ""
        else:
            return False, "لا يمكنك الانضمام لنفس الجولة المكتملة!"
    
    return True, ""
    


def cleanup_old_left_completed_flags():
    """تنظيف علامات left_completed القديمة - نسخة SQLite"""
    import json
    from datetime import datetime
    
    try:
        users_data = load_users()
        channels = load_channels()
        cleaned = 0
        
        for user_id, user_data in users_data.items():
            joined_channels = user_data.get("joined_channels", {})
            if isinstance(joined_channels, str):
                joined_channels = json.loads(joined_channels) if joined_channels else {}
            
            if not joined_channels:
                continue
                
            for channel_id, join_info in list(joined_channels.items()):
                if join_info.get("left_completed", False):
                    channel_data = get_channel_data(channel_id)
                    
                    if not channel_data:
                        del joined_channels[channel_id]
                        cleaned += 1
                    elif not channel_data.get("completed", False):
                        join_info["left_completed"] = False
                        if "completed_round" in join_info:
                            del join_info["completed_round"]
                        joined_channels[channel_id] = join_info
                        cleaned += 1
            
            if cleaned > 0:
                updates = {"joined_channels": json.dumps(joined_channels)}
                update_user_data(user_id, updates, "cleanup_left_completed")
        
        if cleaned > 0:
            logger.info(f"🧹 تم تنظيف {cleaned} علامة left_completed قديمة")
        
        return cleaned
    except Exception as e:
        logger.error(f"❌ خطأ في cleanup_old_left_completed_flags: {e}")
        return 0

def cleanup_permanent_left_channels(context = None):
    """تنظيف القنوات المتروكة نهائياً (تغيير النظام)"""
    try:
        from database import load_users, save_users
        
        users_data = load_users()
        cleaned_count = 0
        
        for user_id, user_data in users_data.items():
            if "permanent_left_channels" in user_data and user_data["permanent_left_channels"]:
                temp_left = user_data.get("temp_left_channels", [])
                permanent_left = user_data["permanent_left_channels"]
                
                for channel_id in permanent_left:
                    if channel_id not in temp_left:
                        temp_left.append(channel_id)
                
                updates = {
                    "temp_left_channels": temp_left,
                    "permanent_left_channels": []
                }
                
                if update_user_data(user_id, updates, "cleanup_permanent_left"):
                    cleaned_count += len(permanent_left)
        
        if cleaned_count > 0:
            logger.info(f"🧹 تم تنظيف {cleaned_count} قناة من permanent_left_channels")
        
        return cleaned_count
        
    except Exception as e:
        logger.error(f"خطأ في تنظيف permanent_left_channels: {e}")
        return 0

def cleanup_channel_data():
    """تنظيف بيانات القنوات من الحقول غير المتسقة"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('SELECT channel_id, username, completed, current, required, completed_at FROM channels')
        channels = cursor.fetchall()
        
        cleaned_count = 0
        
        for channel_id, username, completed, current, required, completed_at in channels:
            needs_update = False
            new_completed = completed
            new_completed_at = completed_at
            
            # 1. إذا كانت completed=0 ولكن فيها completed_at
            if not completed and completed_at:
                new_completed_at = None
                needs_update = True
                logger.info(f"🧹 حذف completed_at من {username} (completed=false)")
            
            # 2. إذا كانت completed=1 ولكن current < required
            if completed and current < required:
                new_completed = False
                new_completed_at = None
                needs_update = True
                logger.info(f"🔧 صححت completed من true إلى false لـ {username} ({current}/{required})")
            
            if needs_update:
                cursor.execute('''
                    UPDATE channels 
                    SET completed = ?, completed_at = ?
                    WHERE channel_id = ?
                ''', (new_completed, new_completed_at, channel_id))
                cleaned_count += 1
        
        conn.commit()
        conn.close()
        
        if cleaned_count > 0:
            logger.info(f"✅ تم تنظيف {cleaned_count} قناة")
        
        return cleaned_count
        
    except Exception as e:
        logger.error(f"❌ خطأ في cleanup_channel_data: {e}")
        if conn:
            conn.close()
        return 0

def fix_left_completed_flags():
    """إصلاح علامات left_completed القديمة مع الجولات الجديدة"""
    try:
        from database import load_users, save_users
        
        users_data = load_users()
        fixed_count = 0
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        for user_id, user_data in users_data.items():
            if "joined_channels" not in user_data:
                continue
                
            for channel_id, join_info in user_data["joined_channels"].items():
                if join_info.get("left_completed", False):
                    # الحصول على بيانات القناة الحالية
                    cursor.execute('SELECT reuse_count FROM channels WHERE channel_id = ?', (channel_id,))
                    result = cursor.fetchone()
                    
                    if not result:
                        continue
                    
                    completed_round = join_info.get("completed_round", -1)
                    current_round = result[0] if result[0] is not None else 0
                    
                    completed_round_val = completed_round if completed_round is not None else -1
                    current_round_val = current_round if current_round is not None else 0
                    
                    # إذا كانت هناك جولة جديدة
                    if current_round_val > completed_round_val:
                        join_info["left_completed"] = False
                        if "completed_round" in join_info:
                            del join_info["completed_round"]
                        if "completed_at" in join_info:
                            del join_info["completed_at"]
                        
                        temp_left = user_data.get("temp_left_channels", [])
                        if channel_id not in temp_left:
                            temp_left.append(channel_id)
                            user_data["temp_left_channels"] = temp_left
                        
                        fixed_count += 1
        
        conn.close()
        
        if fixed_count > 0:
            save_users(users_data)
            logger.info(f"🔧 تم إصلاح {fixed_count} علامة left_completed قديمة")
            
        return fixed_count
        
    except Exception as e:
        logger.error(f"❌ خطأ في fix_left_completed_flags: {e}")
        if conn:
            conn.close()
        return 0

def fix_channel_data_consistency(context = None):
    """تصحيح تناسق بيانات القنوات"""
    try:
        from database import load_users
        
        users_data = load_users()
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        for user_id, user_data in users_data.items():
            active_subs = user_data.get("active_subscriptions", [])
            joined_channels = user_data.get("joined_channels", {})
            
            valid_active = []
            for channel_id in active_subs:
                if channel_id in joined_channels:
                    join_info = joined_channels[channel_id]
                    
                    # التحقق من وجود القناة وأنها غير مكتملة
                    cursor.execute('SELECT completed FROM channels WHERE channel_id = ?', (channel_id,))
                    result = cursor.fetchone()
                    
                    if (result and not result[0] and 
                        join_info.get("verified", False) and
                        not join_info.get("left", False)):
                        valid_active.append(channel_id)
            
            if len(valid_active) != len(active_subs):
                updates = {"active_subscriptions": valid_active}
                update_user_data(user_id, updates, "data_consistency_fix")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"خطأ في تصحيح بيانات القنوات: {e}")
        if conn:
            conn.close()

# ===================== دوال الإحصائيات =====================

def get_channel_counter_stats(channel_id):
    """الحصول على إحصائيات العداد للقناة"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT username, owner, current, required, completed
            FROM channels WHERE channel_id = ?
        ''', (channel_id,))
        
        result = cursor.fetchone()
        if not result:
            conn.close()
            return None
        
        username, owner, current, required, completed = result
        
        # حساب الإحصائيات من joined_users (محفوظة كـ JSON)
        cursor.execute('SELECT joined_users FROM channels WHERE channel_id = ?', (channel_id,))
        joined_result = cursor.fetchone()
        
        import json
        joined_users = []
        if joined_result and joined_result[0]:
            try:
                joined_users = json.loads(joined_result[0])
            except:
                joined_users = []
        
        stats = {
            "current": current or 0,
            "required": required or 0,
            "percentage": (current / max(required, 1)) * 100 if required else 0,
            "completed": bool(completed),
            "total_joins": len(joined_users),
            "total_leaves": 0,  # سيتم حسابه من leave_history
            "total_returns": 0,  # سيتم حسابه من return_history
            "net_change": len(joined_users),
            "channel_username": username,
            "owner": owner
        }
        
        conn.close()
        return stats
        
    except Exception as e:
        logger.error(f"خطأ في get_channel_counter_stats: {e}")
        if conn:
            conn.close()
        return None

# ===================== دوال أخرى =====================

def repair_corrupted_data():
    """إصلاح البيانات التالفة - نسخة SQLite"""
    repaired = False
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # إصلاح بيانات المستخدمين
        cursor.execute('SELECT user_id, points, invites FROM users')
        users = cursor.fetchall()
        
        for user_id, points, invites in users:
            needs_update = False
            new_points = points
            new_invites = invites
            
            if not isinstance(points, (int, float)):
                new_points = 0
                needs_update = True
            
            if not isinstance(invites, int):
                new_invites = 0
                needs_update = True
            
            if needs_update:
                cursor.execute('''
                    UPDATE users 
                    SET points = ?, invites = ?
                    WHERE user_id = ?
                ''', (new_points, new_invites, user_id))
                repaired = True
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"خطأ في repair_corrupted_data: {e}")
        if conn:
            conn.close()
    
    return repaired

def create_backup():
    """إنشاء نسخة احتياطية من قاعدة البيانات"""
    import shutil
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_files = []
    
    if os.path.exists(DB_NAME):
        backup_name = f"backups/{os.path.basename(DB_NAME)}.{timestamp}.bak"
        try:
            shutil.copy2(DB_NAME, backup_name)
            backup_files.append(backup_name)
            logger.info(f"✅ تم إنشاء نسخة احتياطية: {backup_name}")
        except Exception as e:
            logger.error(f"خطأ في نسخ قاعدة البيانات: {e}")
    
    # حذف النسخ القديمة (احتفظ بـ 5 نسخ فقط)
    try:
        import os
        if os.path.exists("backups"):
            backup_files_list = sorted([f for f in os.listdir("backups") if f.endswith(".bak")])
            for old_backup in backup_files_list[:-5]:
                os.remove(f"backups/{old_backup}")
    except Exception as e:
        logger.error(f"خطأ في حذف النسخ القديمة: {e}")
    
    return backup_files

async def check_channel_subscription(bot, user_id, channel_username):
    """
    التحقق من اشتراك المستخدم في قناة
    Returns:
        True: مشترك
        False: غير مشترك
        None: خطأ في التحقق
    """
    from database import logger
    
    try:
        channel_username = channel_username.replace("@", "").strip()
        
        # الحصول على معلومات القناة
        try:
            chat = await bot.get_chat(chat_id=f"@{channel_username}")
        except Exception as chat_error:
            logger.error(f"❌ خطأ في جلب القناة @{channel_username}: {chat_error}")
            return None
        
        # الحصول على حالة العضوية
        try:
            member = await bot.get_chat_member(chat_id=chat.id, user_id=user_id)
            
            # الحالات الصالحة للعضوية
            valid_statuses = ["member", "administrator", "creator"]
            
            if member.status in valid_statuses:
                return True
            else:
                return False
                
        except Exception as member_error:
            error_text = str(member_error).lower()
            
            # حالات الخطأ
            if "user not found" in error_text or "user not participant" in error_text:
                return False
            elif "forbidden" in error_text or "kicked" in error_text:
                return None
            else:
                logger.error(f"❌ خطأ في التحقق من العضوية: {member_error}")
                return None
                
    except Exception as e:
        logger.error(f"❌ خطأ عام في check_channel_subscription: {e}")
        return None

def check_user_channel_status(user_id, channel_id):
    """فحص شامل ودقيق لحالة المستخدم في القناة"""
    user_data = get_user_data(user_id, force_reload=True)
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # التحقق من أن القناة موجودة
        cursor.execute('SELECT completed, reuse_count FROM channels WHERE channel_id = ?', (channel_id,))
        channel_result = cursor.fetchone()
        
        if not channel_result:
            conn.close()
            return "not_found"
        
        is_completed, current_round = channel_result
        current_round = current_round or 0
        
        # 1. التحقق من القنوات النشطة
        active_subs = user_data.get("active_subscriptions", [])
        if channel_id in active_subs:
            joined_channels = user_data.get("joined_channels", {})
            join_data = joined_channels.get(channel_id, {})
            
            if join_data.get("verified", False) and not join_data.get("left", False):
                if is_completed:
                    conn.close()
                    return "joined_completed"
                conn.close()
                return "joined_active"
            else:
                # تصحيح البيانات غير المتسقة
                updates = {
                    "active_subscriptions": [c for c in active_subs if c != channel_id]
                }
                update_user_data(user_id, updates, "fix_active_subscriptions")
        
        # 2. التحقق من joined_channels
        joined_channels = user_data.get("joined_channels", {})
        if channel_id in joined_channels:
            join_data = joined_channels[channel_id]
            
            if join_data.get("left", False):
                if is_completed:
                    conn.close()
                    return "left_completed"
                else:
                    conn.close()
                    return "left_active"
        
        # 3. التحقق من القنوات المتروكة نهائياً
        permanent_left = user_data.get("permanent_left_channels", [])
        if channel_id in permanent_left:
            conn.close()
            return "permanent_left"
        
        # 4. التحقق من القنوات المتروكة مؤقتاً
        temp_left = user_data.get("temp_left_channels", [])
        if channel_id in temp_left:
            conn.close()
            return "temp_left"
        
        # 5. القنوات المتروكة القديمة
        left_channels = user_data.get("left_channels", [])
        if channel_id in left_channels:
            conn.close()
            return "temp_left"
        
        conn.close()
        return "not_joined"
        
    except Exception as e:
        logger.error(f"خطأ في check_user_channel_status: {e}")
        if conn:
            conn.close()
        return "error"

async def check_bot_is_admin(bot, channel_username):
    """
    التحقق مما إذا كان البوت مشرفاً في القناة
    Returns:
        True: البوت مشرف
        False: البوت ليس مشرف
    """
    from database import logger
    
    try:
        channel_username = channel_username.replace("@", "").strip()
        
        try:
            chat = await bot.get_chat(chat_id=f"@{channel_username}")
            
            bot_member = await bot.get_chat_member(
                chat_id=chat.id,
                user_id=(await bot.get_me()).id
            )
            
            if bot_member.status in ("administrator", "creator"):
                return True
            else:
                return False

        except Exception as e:
            logger.error(f"❌ خطأ في التحقق من إشراف البوت في @{channel_username}: {e}")
            return False

    except Exception as e:
        logger.error(f"❌ خطأ عام في check_bot_is_admin: {e}")
        return False

async def can_claim_daily_gift(user_id):
    """التحقق من الهدية اليومية مع تدقيق مضاعف"""
    user_data = get_user_data(user_id, force_reload=True)
    daily_gift = user_data.get("daily_gift", {})
    last_claimed = daily_gift.get("last_claimed")
    
    if not last_claimed:
        return True, 0
    
    try:
        last_claimed_date = datetime.strptime(last_claimed, "%Y-%m-%d %H:%M:%S")
        now = datetime.now()
        
        # التحقق الدقيق: يجب أن يمر 24 ساعة كاملة
        time_diff = now - last_claimed_date
        hours_passed = time_diff.total_seconds() / 3600
        
        if hours_passed >= 24:
            return True, 0
        else:
            # حساب الوقت المتبقي بدقة
            next_claim = last_claimed_date + timedelta(hours=24)
            remaining = next_claim - now
            hours = int(remaining.total_seconds() // 3600)
            minutes = int((remaining.total_seconds() % 3600) // 60)
            seconds = int(remaining.total_seconds() % 60)
            
            # إرجاع الوقت المتبقي بأكبر دقة
            return False, f"{hours}:{minutes:02d}:{seconds:02d}"
            
    except Exception as e:
        logger.error(f"❌ خطأ في تحقق الهدية للمستخدم {user_id}: {e}")
        # في حالة الخطأ، نسمح بالمطالبة للسلامة
        return True, 0

from telegram import Update
from telegram.ext import ContextTypes
from datetime import datetime

async def check_force_sub_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    user_id_str = str(user_id)
    chat_id = query.message.chat_id
    bot = context.bot

    # التحقق من أن المستخدم بالفعل مشترك في جميع القنوات
    can_use, missing_channels = await check_force_subscription(bot, user_id, chat_id)

    if not can_use:
        # إذا لا يزال غير مشترك، اعرض نفس الرسالة
        keyboard = []
        for channel in missing_channels:
            keyboard.append([
                InlineKeyboardButton(
                    f"📢 @{channel}", 
                    url=f"https://t.me/{channel.replace('@', '')}"
                )
            ])
        
        keyboard.append([
            InlineKeyboardButton("✅ تحقق من الاشتراك", callback_data="check_force_sub")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = "❌ بعدك مو مشترك بكل القنوات.\n\n"
        message += "📢 **يرجى الاشتراك في القنوات التالية:**\n\n"
        for i, channel in enumerate(missing_channels, 1):
            message += f"{i}. @{channel}\n"
        
        message += "\n✅ بعد الاشتراك، اضغط زر التحقق مرة أخرى."
        
        try:
            await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
        except:
            pass
        return

    # ✅ ✅ ✅ اكتمل الاشتراك
    user_data = get_user_data(user_id_str)
    is_new_user = not user_data.get("registered", False)
    
    # ✅ إذا كان مستخدم جديد فقط
    if is_new_user:
        # ✅ تسجيل المستخدم رسمياً
        updates = {
            "registered": True,
            "first_join": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "points": 0,
            "invites": 0,
            "total_earned": 0,
            "total_spent": 0
        }
        
        # الحفاظ على البيانات الموجودة
        if "username" not in user_data:
            updates["username"] = query.from_user.username or ""
        if "first_name" not in user_data:
            updates["first_name"] = query.from_user.first_name or ""
        if "last_name" not in user_data:
            updates["last_name"] = query.from_user.last_name or ""
        
        success = update_user_data(user_id_str, updates, "force_sub_complete")
        
        if not success:
            logger.error(f"❌ فشل تسجيل المستخدم {user_id_str} بعد الاشتراك الإجباري")
            await query.edit_message_text("❌ حدث خطأ في التسجيل، حاول مرة أخرى.")
            return
        
        # ✅ إشعار المالك بتسجيل جديد (فقط للمستخدمين الجدد)
        try:
            users_data = load_users()
            user_number = len(users_data)
            
            user_profile_link = get_user_profile_link(
                user_id_str,
                query.from_user.username,
                query.from_user.first_name
            )
            
            # تحديد نوع الدخول
            if "invite_ref" in context.user_data and context.user_data["invite_ref"]:
                entry_type = "🔗 دخول عبر رابط إحالة"
                ref_id = context.user_data["invite_ref"]
                ref_data = get_user_data(ref_id) if ref_id in users_data else None
                
                if ref_data:
                    ref_profile_link = get_user_profile_link(
                        ref_id,
                        ref_data.get("username", "بدون"),
                        ref_data.get("first_name", "")
                    )
                    entry_type += f"\n• المُحيل: {ref_profile_link}"
            else:
                entry_type = "🚪 دخول مباشر"
            
            completion_msg = (
                f"✅ **تم تسجيل مستخدم جديد!**\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🎉 **الحالة:** 🟢 مسجل رسمياً الآن\n"
                f"{entry_type}\n"
                f"🔢 **رقم المستخدم:** #{user_number}\n"
                f"👤 **اليوزر:** {user_profile_link}\n"
                f"🆔 **ID:** <code>{user_id_str}</code>\n"
                f"📛 **الاسم:** {query.from_user.first_name} {query.from_user.last_name or ''}\n"
                f"📅 **وقت التسجيل:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📢 **مشترك في:** جميع القنوات الإجبارية ✅\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💡 **بدء الاستخدام:** يمكنه الآن استخدام البوت"
            )
            
            await bot.send_message(
                ADMIN_ID,
                completion_msg,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            logger.info(f"✅ تم إرسال إشعار اكتمال التسجيل للآدمن: {user_id_str} (مستخدم جديد)")
            
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال إشعار اكتمال للآدمن: {e}")
        
        # ✅ معالجة الإحالة المعلقة إذا وجدت
        if "invite_pending" in context.user_data and context.user_data["invite_pending"]:
            await process_pending_invite(user_id_str, context, bot)
        
        # ✅ عرض رسالة ترحيبية بدون نقاط
        try:
            welcome_msg = (
                f"🎉 **تم التحقق بنجاح!**\n\n"
                f"✅ اشتراكك في القنوات الإجبارية مؤكد\n"
                f"🎊 أهلاً وسهلاً بك في البوت!\n\n"
                f"💡 يمكنك الآن البدء باستخدام البوت"
            )
            
            # عرض رسالة الترحيب
            try:
                await query.edit_message_text(welcome_msg, parse_mode="HTML")
            except:
                await query.message.reply_text(welcome_msg, parse_mode="HTML")
            
            # الانتظار قليلاً ثم الانتقال للقائمة الرئيسية
            await asyncio.sleep(2)
            await back_to_main(query, user_id_str)
            
        except Exception as e:
            logger.error(f"❌ خطأ في معالجة التحقق النهائي: {e}")
            await query.edit_message_text("✅ تم التحقق بنجاح! أهلاً بك 🎉")
            await asyncio.sleep(1)
            await back_to_main(query, user_id_str)
    
    else:
        # ✅ مستخدم مسجل مسبقاً
        # تحديث آخر نشاط
        update_user_data(
            user_id_str,
            {"last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            "user_return"
        )
        
        # رسالة ترحيبية قصيرة
        try:
            await query.edit_message_text("✅ تم التحقق بنجاح! أهلاً بعودتك 🎉", parse_mode="HTML")
        except:
            pass
        
        # الانتظار قليلاً ثم الانتقال للقائمة الرئيسية
        await asyncio.sleep(1)
        await back_to_main(query, user_id_str)


async def check_force_subscription(bot, user_id, chat_id=None):
    """التحقق من الاشتراك الإجباري"""
    data = load_data()
    force_channels = data.get("force_sub_channels", [])
    
    if not force_channels:
        return True, []
    
    not_subscribed = []
    
    for channel_username in force_channels:
        bot_is_admin = await check_bot_is_admin(bot, channel_username)
        
        if not bot_is_admin:
            continue
        
        is_subscribed = await check_channel_subscription(bot, user_id, channel_username)
        
        if is_subscribed is False:
            not_subscribed.append(channel_username)
    
    if not_subscribed:
        return False, not_subscribed
    
    return True, []
async def check_and_enforce_subscription(bot, user_id, chat_id, context):
    """التحقق وإنفاذ الاشتراك الإجباري مع منع إشعارات الأدمن المتكررة"""
    can_use, missing_channels = await check_force_subscription(bot, user_id, chat_id)
    
    if not can_use:
        keyboard = []
        for channel in missing_channels:
            keyboard.append([
                InlineKeyboardButton(
                    f"📢 @{channel}", 
                    url=f"https://t.me/{channel.replace('@', '')}"
                )
            ])
        
        keyboard.append([
            InlineKeyboardButton("✅ تحقق من الاشتراك", callback_data="check_force_sub")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = "📢 **يرجى الاشتراك في القنوات التالية لاستخدام البوت**\n\n"
        for i, channel in enumerate(missing_channels, 1):
            message += f"{i}. @{channel}\n"
        
        message += "\n✅ بعد الاشتراك، اضغط زر التحقق لتفعيل الحساب."
        
        if context.user_data.get('last_force_sub_message_id'):
            try:
                await bot.delete_message(
                    chat_id,
                    context.user_data['last_force_sub_message_id']
                )
            except:
                pass
        
        sent_msg = await bot.send_message(
            chat_id,
            message,
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
        
        context.user_data['last_force_sub_message_id'] = sent_msg.message_id
        
        return False  # ❌ بعده ما مشترك

    # ✅ التحقق إذا كان المستخدم مسجلاً مسبقاً
    user_data = get_user_data(str(user_id))
    
    if not user_data.get("registered", False):
        # مستخدم جديد - تسجيله وإرسال إشعار للأدمن
        update_user_data(
            str(user_id),
            {
                "registered": True,
                "first_join": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            "final_register"
        )
        
        # ✅ إرسال إشعار للأدمن عن المستخدم الجديد (مرة واحدة فقط)
        try:
            users_data = load_users()
            user_number = len(users_data)
            
            user_profile_link = get_user_profile_link(
                str(user_id),
                user_data.get("username", "بدون"),
                user_data.get("first_name", "")
            )
            
            admin_msg = (
                f"✅ **تم تسجيل مستخدم جديد!**\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🎉 **الحالة:** 🟢 مسجل رسمياً\n"
                f"🚪 **نوع الدخول:** مباشر\n"
                f"🔢 **رقم المستخدم:** #{user_number}\n"
                f"👤 **اليوزر:** {user_profile_link}\n"
                f"🆔 **ID:** <code>{user_id}</code>\n"
                f"📛 **الاسم:** {user_data.get('first_name', '')} {user_data.get('last_name', '')}\n"
                f"📅 **التاريخ:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📢 **مشترك في:** جميع القنوات الإجبارية ✅\n"
                f"━━━━━━━━━━━━━━━━━━━━"
            )
            
            await bot.send_message(
                ADMIN_ID,
                admin_msg,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            logger.info(f"📨 تم إرسال إشعار تسجيل جديد للآدمن: {user_id}")
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال إشعار تسجيل جديد للآدمن: {e}")
    
    return True

def can_user_report_channel(user_id, channel_id):
    """التحقق من إمكانية الإبلاغ عن القناة"""
    from database import get_user_data
    
    user_data = get_user_data(user_id)
    reported_channels = user_data.get("reported_channels", [])
    return channel_id not in reported_channels

def add_user_reported_channel(user_id, channel_id):
    """إضافة قناة مبلغ عنها"""
    user_data = get_user_data(user_id)
    reported_channels = user_data.get("reported_channels", [])
    
    if channel_id not in reported_channels:
        reported_channels.append(channel_id)
        updates = {
            "reported_channels": reported_channels,
            "reports_made": user_data.get("reports_made", 0) + 1
        }
        if update_user_data(user_id, updates, "report_channel"):
            return True
    return False

def get_user_statistics():
    """إحصائيات المستخدمين"""
    try:
        users_data = load_users()
        data = load_data()
        
        stats = {
            "total_users": len(users_data),
            "active_users": 0,
            "new_today": 0,
            "new_week": 0,
            "new_month": 0,
            "with_username": 0,
            "with_invites": 0,
            "banned_users": len(data.get("banned_users", [])),
            "muted_users": len(data.get("muted_users", {})),
            "total_points": 0,
            "total_invites": 0
        }
        
        today = datetime.now().date()
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)
        
        for uid, user_data in users_data.items():
            stats["total_points"] += user_data.get("points", 0)
            stats["total_invites"] += user_data.get("invites", 0)
            
            if user_data.get("username"):
                stats["with_username"] += 1
            
            if user_data.get("invites", 0) > 0:
                stats["with_invites"] += 1
            
            last_active_str = user_data.get("last_active", "")
            if last_active_str:
                try:
                    last_active_date = datetime.strptime(last_active_str, "%Y-%m-%d %H:%M:%S").date()
                    if last_active_date == today:
                        stats["active_users"] += 1
                except:
                    pass
            
            first_join_str = user_data.get("first_join", "")
            if first_join_str:
                try:
                    join_date = datetime.strptime(first_join_str, "%Y-%m-%d %H:%M:%S").date()
                    
                    if join_date == today:
                        stats["new_today"] += 1
                    elif join_date >= week_ago:
                        stats["new_week"] += 1
                    elif join_date >= month_ago:
                        stats["new_month"] += 1
                except:
                    pass
        
        return stats
        
    except Exception as e:
        logger.error(f"خطأ في get_user_statistics: {e}")
        return None


# ===================== معالجة الأوامر الرئيسية =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر /start مع نظام إشعارات شامل"""
    user = update.message.from_user
    user_id = str(user.id)
    
    # التحقق من الحظر
    if is_banned(user.id):
        await update.message.reply_text("❌ أنت محظور من استخدام البوت.")
        return
    
    # التحقق من الكتم
    is_user_muted, mute_until = is_muted(user_id)
    if is_user_muted:
        mute_time = mute_until if mute_until else "دائم"
        await update.message.reply_text(
            f"🔇 أنت مكتوم من استخدام البوت!\n\n"
            f"⏰ ينتهي الكتم في: {mute_time}\n\n"
            f"📞 للاستفسار تواصل مع الإدارة.",
            parse_mode="HTML"
        )
        return
    
    # ✅✅✅ **التحقق أولاً: هل المستخدم مسجل مسبقاً؟**
    users_data = load_users()
    was_user_in_db_before = (user_id in users_data)
    
    if was_user_in_db_before:
        # ✅ المستخدم مسجل مسبقاً - التحقق من اشتراكه الحالي
        logger.info(f"✅ المستخدم المسجل {user_id} عاد للبوت")
        
        # ✅ التحقق من الاشتراك الحالي في القنوات الإجبارية
        can_use, missing_channels = await check_force_subscription(
            context.bot, 
            user.id, 
            update.message.chat_id
        )
        
        if not can_use:
            # ✅ المستخدم مسجل سابقاً لكنه غير مشترك حالياً
            logger.info(f"⚠️ المستخدم المسجل {user_id} غير مشترك في القنوات الإجبارية")
            
            # تحديث آخر نشاط
            update_user_data(user_id, {
                "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "force_sub_left": True,  # علامة أنه غادر القنوات
                "force_sub_left_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, "returning_user_left_force_sub")
            
            # عرض رسالة الاشتراك الإجباري
            keyboard = []
            for channel in missing_channels:
                keyboard.append([
                    InlineKeyboardButton(
                        f"📢 @{channel}", 
                        url=f"https://t.me/{channel.replace('@', '')}"
                    )
                ])
            
            keyboard.append([
                InlineKeyboardButton("✅ تحقق من الاشتراك", callback_data="check_force_sub")
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            message = f"📢 **يرجى الاشتراك في القنوات التالية لاستخدام البوت**\n\n"
            
            for i, channel in enumerate(missing_channels, 1):
                message += f"{i}. @{channel}\n"
            
            message += "\n✅ بعد الاشتراك، اضغط زر التحقق لتفعيل حسابك من جديد."
            
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
            return  # ❌ **ننتظر اشتراك المستخدم**
        
        # ✅✅✅ **المستخدم مسجل ومشترك حالياً - يدخل مباشرة**
        user_data = get_user_data(user_id)
        
        # تحديث آخر نشاط
        update_user_data(user_id, {
            "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "force_sub_left": False  # إزالة علامة المغادرة إذا كان مشتركاً
        }, "returning_user_active")
        
        # رسالة الترحيب للعائدين
        welcome_msg = (
            f"👋 أهلاً بعودتك {user.first_name}!\n\n"
            f"🌟 **مرحباً بك مجدداً في بوت خدمات القنوات** 🌟\n\n"
            f"✅ **حسابك مفعل ومشترك في القنوات الإجبارية**\n\n"
            f"📊 **حسابك:**\n"
            f"🎯 النقاط: {user_data.get('points', 0)}\n"
            f"🔗 الدعوات: {user_data.get('invites', 0)}\n"
            f"📅 تاريخ التسجيل: {user_data.get('first_join', 'غير معروف')}\n\n"
            f"اختر من القائمة:"
        )
        
        keyboard = [
            [InlineKeyboardButton("🛒 المتجر", callback_data="store")],
            [InlineKeyboardButton("📊 جمع النقاط", callback_data="collect_points")],
            [InlineKeyboardButton("🎁 الهدية اليومية", callback_data="daily_gift")],
            [InlineKeyboardButton("🏆 التوب", callback_data="top")],
            [InlineKeyboardButton("🔗 رابط الدعوة", callback_data="invite_link")],
            [InlineKeyboardButton("🎟️ الأكواد", callback_data="codes")],
        ]
        
        if is_admin(user.id):
            keyboard.append([InlineKeyboardButton("👑 لوحة الإدمن", callback_data="admin_panel")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(welcome_msg, reply_markup=reply_markup, parse_mode="HTML")
        return  # ✅ انتهى - يدخل مباشرة
    
    # ✅✅✅ **المستخدم جديد - يحتاج الاشتراك الإجباري أولاً**
    can_use, missing_channels = await check_force_subscription(
        context.bot, 
        user.id, 
        update.message.chat_id
    )
    
    if not can_use:
        # ✅ إشعار دخول مباشر للآدمن
        users_data = load_users()
        user_number = len(users_data) + 1
        
        # إنشاء رابط بروفايل المستخدم
        user_profile_link = get_user_profile_link(
            user_id,
            user.username,
            user.first_name
        )
        
        # تحديد نوع الدخول
        if context.args and len(context.args) > 0:
            entry_type = "🔗 دخول عبر رابط إحالة"
            ref_id = context.args[0]
            
            # حفظ رابط الدعوة مؤقتاً
            context.user_data["invite_ref"] = ref_id
            context.user_data["invite_pending"] = True
            
            # الحصول على بيانات المُحيل
            ref_data = get_user_data(ref_id) if ref_id in users_data else None
            ref_username = ref_data.get("username", "بدون") if ref_data else "غير معروف"
            
            # إنشاء رابط بروفايل المُحيل
            ref_profile_link = get_user_profile_link(
                ref_id,
                ref_username,
                ref_data.get("first_name", "المحيل") if ref_data else "المحيل"
            )
            
            admin_msg = (
                f"🔗 **دخول جديد عبر رابط إحالة!**\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 **المُحيل:** {ref_profile_link}\n"
                f"   🆔 ID: <code>{ref_id}</code>\n"
                f"   👤 اليوزر: @{ref_username if ref_username != 'بدون' else 'بدون يوزر'}\n\n"
                f"👥 **الشخص الجديد:** {user_profile_link}\n"
                f"   🆔 ID: <code>{user_id}</code>\n"
                f"   👤 اليوزر: @{user.username if user.username else 'بدون يوزر'}\n"
                f"   📛 الاسم: {user.first_name} {user.last_name or ''}\n"
                f"   🌐 اللغة: {user.language_code or 'غير معروف'}\n\n"
                f"📢 **الحالة:** ⏳ ينتظر الاشتراك الإجباري\n"
                f"📋 **القنوات المطلوبة:** {len(missing_channels)}\n"
                f"⏰ **الوقت:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"━━━━━━━━━━━━━━━━━━━━"
            )
        else:
            entry_type = "🚪 دخول مباشر"
            admin_msg = (
                f"👤 **دخول مستخدم جديد!**\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🚪 **نوع الدخول:** مباشر\n"
                f"🔢 **رقم المستخدم:** #{user_number}\n"
                f"👤 **المستخدم:** {user_profile_link}\n"
                f"   🆔 ID: <code>{user_id}</code>\n"
                f"   👤 اليوزر: @{user.username if user.username else 'بدون يوزر'}\n"
                f"   📛 الاسم: {user.first_name} {user.last_name or ''}\n"
                f"   🌐 اللغة: {user.language_code or 'غير معروف'}\n"
                f"📢 **الحالة:** ⏳ ينتظر الاشتراك الإجباري\n"
                f"📋 **القنوات المطلوبة:** {len(missing_channels)}\n"
                f"⏰ **الوقت:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"━━━━━━━━━━━━━━━━━━━━"
            )
        
        # إرسال الإشعار للمالك
        try:
            await context.bot.send_message(
                ADMIN_ID,
                admin_msg,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            logger.info(f"📨 تم إرسال إشعار دخول للآدمن: {user_id} ({entry_type})")
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال إشعار دخول للآدمن: {e}")
        
        # ✅ حفظ بيانات الدخول الأولي
        context.user_data["pending_start"] = {
            "user_id": user_id,
            "username": user.username or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "language_code": user.language_code or "غير معروف",
            "invite_ref": context.args[0] if context.args else None,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "missing_channels": missing_channels,
            "entry_type": entry_type
        }
        
        # عرض رسالة الاشتراك الإجباري للمستخدم
        keyboard = []
        for channel in missing_channels:
            keyboard.append([
                InlineKeyboardButton(
                    f"📢 @{channel}", 
                    url=f"https://t.me/{channel.replace('@', '')}"
                )
            ])
        
        keyboard.append([
            InlineKeyboardButton("✅ تحقق من الاشتراك", callback_data="check_force_sub")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = f"📢 **يرجى الاشتراك في القنوات التالية لاستخدام البوت**\n\n"
        
        for i, channel in enumerate(missing_channels, 1):
            message += f"{i}. @{channel}\n"
        
        message += "\n✅ بعد الاشتراك، اضغط زر التحقق لتفعيل الحساب.\n\n"
        message += "💡 **ملاحظة:** بعد التسجيل الأولي، إذا غادرت هذه القنوات لاحقاً، ستحتاج للاشتراك مرة أخرى عند العودة."
        
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
        return  # ❌ **لا نكمل - ننتظر اشتراك المستخدم**
    
    # ✅✅✅ **المستخدم جديد وهو مشترك في القنوات الإجبارية - التسجيل الرسمي**
    
    # 🔢 حساب رقم المستخدم الترتيبي
    total_users_before = len(users_data)
    user_number = total_users_before + 1
    
    # تحديث بيانات المستخدم
    updates = {
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "first_join": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "points": 0,
        "invites": 0,
        "total_earned": 0,
        "total_spent": 0,
        "force_sub_passed": True,
        "force_sub_passed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "force_sub_left": False  # ليس مغادراً حالياً
    }
    
    update_user_data(user_id, updates, "user_first_registration")
    
    # ✅✅✅ **معالجة الإحالة**
    if context.args and len(context.args) > 0:
        ref_id = context.args[0]
        
        # التحقق من صحة رابط الإحالة
        if ref_id == user_id:
            logger.info(f"⚠️ المستخدم {user_id} حاول استخدام رابط دعوته الخاص")
        elif ref_id not in users_data:
            logger.info(f"⚠️ رابط إحالة غير صحيح: {ref_id}")
        else:
            # المستخدم الجديد أكمل الاشتراك ورابط الإحالة صحيح
            ref_data = get_user_data(ref_id, force_reload=True)
            invited_users = ref_data.get("invited_users", [])
            
            if user_id not in invited_users:
                # ✅ منح 3 نقاط للمحيل
                success, message = safe_add_points(ref_id, 3, "add", "invite_reward_complete")
                
                if success:
                    # تحديث قائمة المدعوين
                    invited_users.append(user_id)
                    update_user_data(ref_id, {
                        "invites": ref_data.get("invites", 0) + 1,
                        "invited_users": invited_users,
                        "last_invite_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }, "invite_update_complete")
                    
                    # ✅ تحديث إحصائيات النظام
                    update_system_stats("total_invites", increment=1)
                    update_system_stats("total_invite_points", increment=3)
                    
                    # 🔔 إرسال رسالة للمُحيل
                    try:
                        new_user_profile_link = get_user_profile_link(
                            user_id,
                            user.username,
                            user.first_name
                        )
                        
                        await context.bot.send_message(
                            int(ref_id),
                            f"🎊 **لقد حصل صديقك على 3 نقاط!**\n\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"👤 **صديقك:** {new_user_profile_link}\n\n"
                            f"✅ **حصلت على:**\n"
                            f"• 3 نقاط إضافية 💎\n"
                            f"• نقاطك الآن: {ref_data.get('points', 0) + 3}\n"
                            f"• دعواتك: {ref_data.get('invites', 0) + 1} شخص\n\n"
                            f"⏰ **الوقت:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"━━━━━━━━━━━━━━━━━━━━",
                            parse_mode="HTML",
                            disable_web_page_preview=True
                        )
                    except Exception as e:
                        logger.error(f"❌ خطأ في إرسال إشعار للمُحيل {ref_id}: {e}")
    
    # ✅✅✅ **إشعار المالك عن المستخدم الجديد**
    users_data_final = load_users()
    total_users_final = len(users_data_final)
    
    if total_users_final > total_users_before:
        logger.info(f"✅ اكتشاف مستخدم جديد مسجل: {user_id}")
        
        # إعداد رسالة الآدمن
        user_profile_link = get_user_profile_link(
            user_id,
            user.username,
            user.first_name
        )
        
        admin_msg = (
            f"👤 **تسجيل جديد ناجح للبوت!**\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ **الحالة:** 🟢 مسجل رسمياً الآن\n"
            f"🔢 **رقم المستخدم:** #{total_users_final}\n"
            f"👤 **اليوزر:** {user_profile_link}\n"
            f"🆔 **ID:** <code>{user_id}</code>\n"
            f"📛 **الاسم:** {user.first_name} {user.last_name or ''}\n"
            f"🌐 **اللغة:** {user.language_code or 'غير معروف'}\n"
            f"📅 **التسجيل:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📢 **تم الاشتراك في:** جميع القنوات الإجبارية ✅\n"
            f"💡 **النظام:** إذا غادر القنوات، سيحتاج إعادة الاشتراك عند العودة\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )
        
        try:
            await context.bot.send_message(
                ADMIN_ID,
                admin_msg,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال إشعار تسجيل جديد للآدمن: {e}")
    
    # رسالة الترحيب النهائية
    welcome_msg = (
        f"👋 أهلاً وسهلاً {user.first_name}!\n\n"
        f"🌟 **مرحباً بك في بوت خدمات القنوات** 🌟\n\n"
        f"✅ **تم تسجيلك رسمياً في البوت!**\n\n"
        f"📌 **كيفية عمل البوت:**\n"
        f"1️⃣ ادخل على المتجر واشترِ أعضاء لقناتك\n"
        f"2️⃣ شارك رابط دعوتك مع أصدقائك واحصل على نقاط\n"
        f"3️⃣ انضم للقنوات في قسم التجميع واحصل على نقاط\n"
        f"4️⃣ استخدم نقاطك لشراء أعضاء جدد\n\n"
        f"📢 **قناة البوت الرسمية:** {BOT_CHANNEL}\n"
        f"🎯 **لديك:** 0 نقطة\n"
        f"🔗 **رابط دعوتك:** https://t.me/{(await context.bot.get_me()).username}?start={user_id}\n\n"
        f"💡 **تنبيه:** إذا غادرت القنوات الإجبارية، ستحتاج إعادة الاشتراك عند العودة!\n\n"
        f"اختر من القائمة:"
    )
    
    keyboard = [
        [InlineKeyboardButton("🛒 المتجر", callback_data="store")],
        [InlineKeyboardButton("📊 جمع النقاط", callback_data="collect_points")],
        [InlineKeyboardButton("🎁 الهدية اليومية", callback_data="daily_gift")],
        [InlineKeyboardButton("🏆 التوب", callback_data="top")],
        [InlineKeyboardButton("🔗 رابط الدعوة", callback_data="invite_link")],
        [InlineKeyboardButton("🎟️ الأكواد", callback_data="codes")],
    ]
    
    if is_admin(user.id):
        keyboard.append([InlineKeyboardButton("👑 لوحة الإدمن", callback_data="admin_panel")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(welcome_msg, reply_markup=reply_markup, parse_mode="HTML")

def get_user_position(user_id):
    """الحصول على موضع المستخدم في القائمة"""
    try:
        users_data = load_users()
        user_ids = list(users_data.keys())
        
        # حاول ترتيب حسب تاريخ الانضمام
        try:
            # إنشاء قائمة مرتبة حسب تاريخ الانضمام
            users_with_dates = []
            for uid in user_ids:
                user_data = get_user_data(uid)
                join_date = user_data.get("first_join", "2099-12-31 00:00:00")
                users_with_dates.append((uid, join_date))
            
            # ترتيب من الأقدم إلى الأحدث
            users_with_dates.sort(key=lambda x: x[1])
            sorted_user_ids = [uid for uid, _ in users_with_dates]
            
            if str(user_id) in sorted_user_ids:
                return sorted_user_ids.index(str(user_id)) + 1
        except:
            # إذا فشل الترتيب، استخدم الترتيب البسيط
            if str(user_id) in user_ids:
                return user_ids.index(str(user_id)) + 1
        
        return 0
    except Exception as e:
        logger.error(f"❌ خطأ في get_user_position: {e}")
        return 0

async def get_user_rank(user_id):
    """الحصول على ترتيب المستخدم (رقمه) في البوت - نسخة مبسطة"""
    try:
        users_data = load_users()
        
        # التحقق من وجود المستخدم
        if str(user_id) not in users_data:
            return 0
        
        # عدّ المستخدمين حسب تاريخ الانضمام (نسخة مبسطة)
        user_ids = list(users_data.keys())
        
        # محاولة العثور على الترتيب
        try:
            return user_ids.index(str(user_id)) + 1
        except ValueError:
            return 0
            
    except Exception as e:
        logger.error(f"❌ خطأ في get_user_rank: {e}")
        return 0

def get_user_profile_link(user_id, username=None, first_name=None):
    """إنشاء رابط بروفايل المستخدم بلون أزرق"""
    try:
        # تنظيف البيانات
        if username is None:
            username = ""
        if first_name is None:
            first_name = "المستخدم"
        
        # إذا كان لديه يوزر حقيقي
        clean_username = str(username).replace("@", "").strip()
        if clean_username and clean_username not in ["بدون", "", "None", "none"]:
            # رابط بلون أزرق مع رموز HTML
            return f"<a href='https://t.me/{clean_username}' style='color: #0078ff;'>{first_name}</a>"
        else:
            # رابط عبر user_id بلون أزرق
            return f"<a href='tg://user?id={user_id}' style='color: #0078ff;'>{first_name}</a>"
    except Exception as e:
        logger.error(f"❌ خطأ في إنشاء رابط البروفايل: {e}")
        return f"<a href='tg://user?id={user_id}' style='color: #0078ff;'>{first_name or 'المستخدم'}</a>"


async def process_pending_invite(user_id, context, bot):
    """معالجة الإحالة المعلقة بعد اكتمال الاشتراك في القنوات الإجبارية"""
    try:
        if "invite_ref" not in context.user_data:
            return
        
        ref_id = context.user_data["invite_ref"]
        
        if not ref_id:
            return
        
        # التحقق من صحة رابط الإحالة
        users_data = load_users()
        
        if ref_id == user_id:
            logger.info(f"⚠️ المستخدم {user_id} حاول استخدام رابط دعوته الخاص")
            return
        
        if ref_id not in users_data:
            logger.info(f"⚠️ رابط إحالة غير صحيح: {ref_id}")
            return
        
        # التحقق إذا تم منح النقاط مسبقاً
        ref_data = get_user_data(ref_id, force_reload=True)
        invited_users = ref_data.get("invited_users", [])
        
        if str(user_id) in invited_users:
            logger.info(f"⚠️ النقاط ممنوحة مسبقاً للمُحيل {ref_id} للمستخدم {user_id}")
            return
        
        # ✅ منح 3 نقاط للمحيل
        old_points = ref_data.get("points", 0)
        old_invites = ref_data.get("invites", 0)
        
        success, message = safe_add_points(ref_id, 3, "add", "invite_reward_complete")
        
        if success:
            new_points = old_points + 3
            new_invites = old_invites + 1
            
            # تحديث قائمة المدعوين
            invited_users.append(str(user_id))
            
            updates = {
                "invites": new_invites,
                "invited_users": invited_users,
                "last_invite_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            update_user_data(ref_id, updates, "invite_update_complete")
            
            # ✅ تحديث إحصائيات النظام
            update_system_stats("total_invites", increment=1)
            update_system_stats("total_invite_points", increment=3)
            
            # الحصول على بيانات المستخدم الجديد
            user_data = get_user_data(user_id)
            user_username = user_data.get("username", "بدون")
            
            # 🔔 إرسال رسالة للمُحيل
            try:
                new_user_profile_link = get_user_profile_link(
                    user_id,
                    user_username,
                    user_data.get("first_name", "")
                )
                
                await bot.send_message(
                    int(ref_id),
                    f"🎊 **لقد دخل صديقك عبر رابطك!**\n\n"
                    f"👤 **صديقك:** {new_user_profile_link}\n"
                    f"✅ **حصلت على:** 3 نقاط إضافية 💎\n"
                    f"🎯 **نقاطك الآن:** {new_points}\n"
                    f"🔗 **دعواتك:** {new_invites} شخص\n"
                    f"⏰ **الوقت:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
                logger.info(f"✅ تم إرسال إشعار اكتمال التسجيل لـ {ref_id}")
            except Exception as e:
                logger.error(f"❌ خطأ في إرسال إشعار للمُحيل {ref_id}: {e}")
            
            # ✅ تنظيف بيانات الإحالة المؤقتة
            context.user_data.pop("invite_ref", None)
            context.user_data.pop("invite_pending", None)
            
            logger.info(f"✅ تم معالجة الإحالة المعلقة للمستخدم {user_id} للمُحيل {ref_id}")
            return True
        else:
            logger.error(f"❌ فشل إضافة نقاط الإحالة للمستخدم {ref_id}: {message}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في process_pending_invite: {e}")
        import traceback
        traceback.print_exc()
        return False

def update_user_registration_status(user_id, registered=True):
    """تحديث حالة تسجيل المستخدم"""
    updates = {
        "registered": registered,
        "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    if registered:
        updates["registration_completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    return update_user_data(user_id, updates, "registration_status_update")

def check_user_registration_complete(user_id):
    """التحقق من اكتمال تسجيل المستخدم"""
    user_data = get_user_data(user_id)
    return user_data.get("registered", False)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ معالجة ضغطات الأزرار مع الأزرار الجديدة"""
    query = update.callback_query
    user_id = str(query.from_user.id)
    
    # ✅ تعريف bot من context
    bot = context.bot
    
    if is_banned(query.from_user.id):
        await query.answer("❌ محظور", show_alert=True)
        return
    
    is_user_muted, mute_until = is_muted(user_id)
    if is_user_muted:
        await query.answer(f"🔇 مكتوم حتى: {mute_until}", show_alert=True)
        return
    
    # Cooldown
    allowed, remaining, reason = cooldown_manager.can_proceed(user_id, "general")
    if not allowed:
        await query.answer(f"⏳ انتظر {remaining:.1f}ث", show_alert=True)
        return
    
    try:
        await query.answer()
    except:
        pass
    
    # معالجة الأزرار
    try:
        if query.data == "store":
            await show_store(query)
        
        elif query.data == "collect_points":
            await show_collect_points(query, user_id, context, page=0)  # ✅ إضافة page=0
        
        elif query.data.startswith("page_"):
            # معالجة التنقل بين الصفحات
            try:
                page = int(query.data.replace("page_", ""))
                await show_collect_points(query, user_id, context, page=page)
            except ValueError:
                await show_collect_points(query, user_id, context, page=0)
            except Exception as e:
                logger.error(f"خطأ في معالجة الصفحة: {e}")
                await query.answer("❌ خطأ في التنقل", show_alert=True)
        
        elif query.data == "daily_gift":
            await show_daily_gift(query, user_id)
        
        elif query.data == "top":
            await show_top(query)
        
        elif query.data == "invite_link":
            await show_invite_link(query, user_id, bot)
        
        elif query.data == "codes":
            await show_codes_panel(query)
        
        elif query.data == "admin_panel":
            if is_admin(query.from_user.id):
                await show_admin_panel(query)
            else:
                await query.answer("❌ غير مسموح!", show_alert=True)
        
        elif query.data == "back_main":
            await back_to_main(query, user_id)
        
        elif query.data == "claim_daily_gift":
            await handle_claim_daily_gift(query, user_id, bot)
        
        elif query.data == "check_force_sub":
            can_use, missing = await check_force_subscription(bot, int(user_id), query.message.chat_id)
            if can_use:
                await query.answer("✅ مشترك!", show_alert=True)
                
                # ✅ إرسال إشعار اكتمال التسجيل للمالك
                if "pending_start" in context.user_data:
                    pending_data = context.user_data["pending_start"]
                    users_data = load_users()
                    
                    # التحقق إذا تم التسجيل مسبقاً
                    if user_id in users_data:
                        # مستخدم موجود بالفعل
                        user_data = get_user_data(user_id)
                        user_number = 0
                        
                        # حساب رقم المستخدم
                        user_ids = list(users_data.keys())
                        try:
                            user_number = user_ids.index(str(user_id)) + 1
                        except:
                            user_number = 0
                    else:
                        # مستخدم جديد تماماً
                        user_number = len(users_data) + 1
                    
                    user_profile_link = get_user_profile_link(
                        user_id,
                        pending_data["username"],
                        pending_data["first_name"]
                    )
                    
                    # تحديد نوع الدخول
                    if pending_data["invite_ref"]:
                        entry_type = "🔗 اكتمل تسجيل دخول عبر رابط إحالة"
                        ref_id = pending_data["invite_ref"]
                        ref_data = get_user_data(ref_id) if ref_id in load_users() else None
                        if ref_data:
                            ref_profile_link = get_user_profile_link(
                                ref_id,
                                ref_data.get("username", "بدون"),
                                ref_data.get("first_name", "")
                            )
                            entry_type += f"\n• المُحيل: {ref_profile_link}"
                    else:
                        entry_type = "🚪 اكتمل تسجيل دخول مباشر"
                    
                    completion_msg = (
                        f"✅ **اكتمل تسجيل مستخدم جديد!**\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"🎉 **الحالة:** 🟢 مسجل رسمياً الآن\n"
                        f"{entry_type}\n"
                        f"🔢 **رقم المستخدم:** #{user_number}\n"
                        f"👤 **اليوزر:** {user_profile_link}\n"
                        f"🆔 **ID:** <code>{user_id}</code>\n"
                        f"📛 **الاسم:** {pending_data['first_name']} {pending_data['last_name'] or ''}\n"
                        f"🌐 **اللغة:** {pending_data['language_code']}\n"
                        f"📅 **أول دخول:** {pending_data['timestamp']}\n"
                        f"📅 **اكتمل التسجيل:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"📢 **مشترك في:** جميع القنوات ✅\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"💡 **ملاحظة:**\n"
                        f"• تم إرسال إشعار أولي عند دخوله\n"
                        f"• هذا الإشعار يؤكد اكتمال التسجيل"
                    )
                    
                    try:
                        await bot.send_message(
                            ADMIN_ID,
                            completion_msg,
                            parse_mode="HTML",
                            disable_web_page_preview=True
                        )
                        logger.info(f"✅ تم إرسال إشعار اكتمال التسجيل للآدمن: {user_id}")
                    except Exception as e:
                        logger.error(f"❌ خطأ في إرسال إشعار اكتمال للآدمن: {e}")
                    
                    # تنظيف البيانات المؤقتة
                    context.user_data.pop("pending_start", None)
                
                # ✅ معالجة الإحالة المعلقة إذا وجدت
                if "invite_pending" in context.user_data and context.user_data["invite_pending"]:
                    await process_pending_invite(user_id, context, bot)
                
                # ✅ الانتقال إلى القائمة الرئيسية
                await back_to_main(query, user_id)
            else:
                await query.answer("❌ غير مشترك!", show_alert=True)
        
        elif query.data.startswith("buy_"):
            await handle_buy(query, context)
        
        elif query.data.startswith("join_"):
            await handle_join_channel(query, user_id, bot)
        
        elif query.data.startswith("verify_channel_"):
            await handle_verify_channel(query, user_id, bot, context)
        
        elif query.data.startswith("report_"):
            await handle_report_channel(query, user_id, bot)
        
        elif query.data.startswith("admin_"):
            if not is_admin(query.from_user.id):
                await query.answer("❌ غير مسموح!", show_alert=True)
                return
            
            action = query.data[6:]
            
            if action == "panel":
                await show_admin_panel(query)
            
            elif action == "stats":
                await show_admin_stats(query)
            
            elif action == "stats_detailed":
                await show_detailed_stats(query)
            
            elif action == "active_codes":
                await show_active_codes(query)
            
            elif action == "manage_admins":
                await manage_admins(query)
            
            elif action == "active_channels":
                await show_active_channels(query)
            
            elif action == "stats_graph":
                await show_stats_graph(query)
            
            elif action == "user_info":
                await query.edit_message_text("👤 أرسل يوزر المستخدم أو ID:", parse_mode="HTML")
                context.user_data["admin_action"] = "user_info"
            
            elif action == "broadcast":
                await query.edit_message_text("📢 أرسل الرسالة للبث:", parse_mode="HTML")
                context.user_data["admin_action"] = "broadcast"
            
            elif action == "give_points":
                await query.edit_message_text("💰 أرسل: يوزر/ID عدد_النقاط", parse_mode="HTML")
                context.user_data["admin_action"] = "give_points"
            
            elif action == "take_points":
                await query.edit_message_text("💸 أرسل: يوزر/ID عدد_النقاط", parse_mode="HTML")
                context.user_data["admin_action"] = "take_points"
            
            elif action == "ban":
                await query.edit_message_text("⚠️ أرسل يوزر المستخدم أو ID:", parse_mode="HTML")
                context.user_data["admin_action"] = "ban_user"
            
            elif action == "unban":
                await query.edit_message_text("✅ أرسل يوزر المستخدم أو ID:", parse_mode="HTML")
                context.user_data["admin_action"] = "unban_user"
            
            elif action == "mute":
                await query.edit_message_text("🔇 أرسل: يوزر/ID وقت_بالثواني [سبب]", parse_mode="HTML")
                context.user_data["admin_action"] = "mute_user"
            
            elif action == "unmute":
                await query.edit_message_text("🔊 أرسل يوزر المستخدم أو ID:", parse_mode="HTML")
                context.user_data["admin_action"] = "unmute_user"
            
            elif action == "add_channel":
                await query.edit_message_text("➕ أرسل: @channel عدد_الأعضاء", parse_mode="HTML")
                context.user_data["admin_action"] = "add_channel"
            
            elif action == "remove_channel":
                await query.edit_message_text("➖ أرسل: @channel", parse_mode="HTML")
                context.user_data["admin_action"] = "remove_channel"
            
            elif action == "force_add":
                await query.edit_message_text("🔒 أرسل: @channel", parse_mode="HTML")
                context.user_data["admin_action"] = "add_force"
            
            elif action == "force_remove":
                await query.edit_message_text("🔓 أرسل: @channel", parse_mode="HTML")
                context.user_data["admin_action"] = "remove_force"
            
            elif action == "add_code":
                await query.edit_message_text("🎟️ أرسل: اسم_الكود نقاط عدد_المستخدمين", parse_mode="HTML")
                context.user_data["admin_action"] = "add_code"
            
            elif action == "remove_code":
                await query.edit_message_text("🗑️ أرسل: اسم_الكود", parse_mode="HTML")
                context.user_data["admin_action"] = "remove_code"
            
            # ✅ الأزرار الجديدة
            elif action == "monitor_report":
                await generate_monitor_report(query, context)
            
            elif action == "run_monitor":
                await query.answer("🔄 جاري تشغيل المراقبة...", show_alert=True)
                await monitor_channel_activity(context)
                await query.answer("✅ تم تشغيل المراقبة!", show_alert=True)
                await generate_monitor_report(query, context)
        
        # ✅ زر الإحصائيات المفصلة (يمكن أن يكون خارج لوحة الأدمن)
        elif query.data == "detailed_stats":
            await show_detailed_stats(query)
        
        # ✅ زر القنوات النشطة
        elif query.data == "active_channels_list":
            await show_active_channels(query)
        
        # ✅ زر تقرير المراقبة
        elif query.data == "admin_monitor_report":
            await generate_monitor_report(query, context)
        
        # ✅ زر تشغيل المراقبة يدوياً
        elif query.data == "admin_run_monitor":
            await query.answer("🔄 جاري تشغيل المراقبة...", show_alert=True)
            await monitor_channel_activity(context)
            await query.answer("✅ تم تشغيل المراقبة!", show_alert=True)
            await generate_monitor_report(query, context)
        
        # ✅ زر معلومات النقاط السالبة
        elif query.data == "negative_info":
            user_data = get_user_data(user_id)
            points = user_data.get("points", 0)
            
            if points < 0:
                info_text = (
                    f"⚠️ **رصيدك بالسالب!**\n\n"
                    f"💰 **النقاط:** {points}\n\n"
                    f"📋 **الأسباب المحتملة:**\n"
                    f"• مغادرة قنوات نشطة (خصم 5 نقاط لكل قناة)\n"
                    f"• عمليات شراء\n"
                    f"• نقل نقاط\n\n"
                    f"💡 **الحلول:**\n"
                    f"1. انضم لقنوات جديدة من 'جمع النقاط'\n"
                    f"2. احصل على الهدية اليومية\n"
                    f"3. استخدم أكواد النقاط\n"
                    f"4. ادع أصدقاء عبر رابط الدعوة\n"
                    f"5. انتظر هدية الإدارة\n\n"
                    f"🎯 **هدفك:** الوصول إلى 0 نقطة أو أكثر"
                )
                
                keyboard = [
                    [InlineKeyboardButton("📊 جمع النقاط", callback_data="collect_points")],
                    [InlineKeyboardButton("🎁 الهدية اليومية", callback_data="daily_gift")],
                    [InlineKeyboardButton("🔗 رابط الدعوة", callback_data="invite_link")],
                    [InlineKeyboardButton("🎟️ الأكواد", callback_data="codes")],
                    [InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]
                ]
                
                await query.edit_message_text(
                    info_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="Markdown"
                )
            else:
                await back_to_main(query, user_id)
        
        else:
            await query.answer("❌ زر غير معروف", show_alert=True)
    
    except Exception as e:
        logger.error(f"خطأ في button_handler: {e}")
        import traceback
        traceback.print_exc()
        await query.answer("⚠️ حدث خطأ!", show_alert=True)

# ===================== الهدية اليومية =====================

async def send_channel_completion_notifications(bot, channel_username, owner_id, 
                                                 current_count, required_count, created_at):
    """
    إرسال إشعارات اكتمال القناة
    للمالك وللأدمن
    """
    from database import get_user_data, ADMIN_ID, logger
    
    try:
        # ✅ 1. إشعار المالك
        if str(owner_id) != str(ADMIN_ID):
            try:
                await bot.send_message(
                    int(owner_id),
                    f"🎉 **تهانينا! اكتملت قناتك!**\n\n"
                    f"📢 **القناة:** @{channel_username}\n"
                    f"👥 **العدد النهائي:** {current_count}/{required_count}\n"
                    f"📅 **بدأت في:** {created_at}\n"
                    f"⏰ **اكتملت في:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    f"✅ **ماذا الآن؟**\n"
                    f"• يمكنك إعادة استخدام القناة لجولة جديدة\n"
                    f"• اشتر أعضاء جدد من المتجر\n"
                    f"• يمكن إعادة الاستخدام حتى 3 مرات\n\n"
                    f"🎊 شكراً لاستخدامك البوت!",
                    parse_mode="Markdown"
                )
                logger.info(f"✅ تم إرسال إشعار الاكتمال لمالك القناة {owner_id}")
            except Exception as owner_error:
                logger.error(f"❌ خطأ في إرسال إشعار للمالك {owner_id}: {owner_error}")
        
        # ✅ 2. إشعار الأدمن
        try:
            owner_data = get_user_data(owner_id)
            owner_username = owner_data.get("username", "بدون")
            
            await bot.send_message(
                ADMIN_ID,
                f"✅ **قناة اكتملت!**\n\n"
                f"📢 **القناة:** @{channel_username}\n"
                f"👤 **المالك:** @{owner_username} (`{owner_id}`)\n"
                f"📊 **العداد النهائي:** {current_count}/{required_count}\n"
                f"📅 **بدأت:** {created_at}\n"
                f"⏰ **اكتملت:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"🎯 **الإحصائيات:**\n"
                f"• وقت الإكمال: {(datetime.now() - datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')).days} يوم\n"
                f"• المعدل: {current_count / max(1, (datetime.now() - datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')).days):.1f} عضو/يوم",
                parse_mode="Markdown"
            )
            logger.info(f"✅ تم إرسال إشعار الاكتمال للأدمن")
        except Exception as admin_error:
            logger.error(f"❌ خطأ في إرسال إشعار للأدمن: {admin_error}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في send_channel_completion_notifications: {e}")
        import traceback
        traceback.print_exc()
        return False

async def send_to_admin(bot, message):
    """إرسال رسالة للمالك مع دعم HTML"""
    try:
        await bot.send_message(
            ADMIN_ID, 
            message, 
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Error sending to admin: {e}")

async def send_join_notification_to_owner(bot, channel_username, owner_id, user_data, 
                                          is_returning_user, current_count, required_count, 
                                          current_round):
    """
    إشعار المالك بانضمام عضو جديد أو عائد
    """
    from database import ADMIN_ID, logger
    
    try:
        # لا ترسل إشعارات للأدمن (تجنب الإزعاج)
        if str(owner_id) == str(ADMIN_ID):
            return True
        
        user_username = user_data.get("username", "بدون")
        user_id = user_data.get("user_id", "غير معروف")
        
        # رمز مناسب
        emoji = "🔄" if is_returning_user else "✨"
        status = "عضو عائد" if is_returning_user else "عضو جديد"
        
        msg = (
            f"{emoji} **{status} في قناتك!**\n\n"
            f"📢 **القناة:** @{channel_username}\n"
            f"👤 **العضو:** @{user_username}\n"
            f"🆔 **ID:** `{user_id}`\n"
            f"📊 **التقدم:** {current_count}/{required_count}\n"
        )
        
        # إضافة معلومات الجولة إذا كانت هناك إعادة استخدام
        if current_round > 0:
            msg += f"🔄 **الجولة:** {current_round + 1}\n"
        
        # حساب المتبقي
        remaining = required_count - current_count
        if remaining > 0:
            percentage = (current_count / required_count) * 100
            msg += f"\n⏳ **متبقي:** {remaining} عضو ({percentage:.1f}%)\n"
        else:
            msg += f"\n🎉 **اكتملت القناة!**\n"
        
        # إضافة ملاحظة للعائدين
        if is_returning_user:
            msg += f"\n💡 هذا العضو عاد بعد مغادرته السابقة"
        
        await bot.send_message(int(owner_id), msg, parse_mode="Markdown")
        logger.info(f"✅ تم إرسال إشعار انضمام لمالك القناة {owner_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في send_join_notification_to_owner: {e}")
        import traceback
        traceback.print_exc()
        return False

# تعديل دالة handle_code_command لإرسال إشعار عند اكتمال الكود
async def handle_code_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /code مع الإشعارات"""
    if len(context.args) < 1:
        await update.message.reply_text("📝 استخدام: /code اسم_الكود")
        return
    
    code_name = context.args[0].upper()
    user_id = str(update.message.from_user.id)
    data = load_data()
    
    if code_name in data.get("codes", {}):
        code_data = data["codes"][code_name]
        
        if code_data.get("used_count", 0) >= code_data.get("max_uses", 0):
            await update.message.reply_text("❌ الكود مستخدم بالكامل!")
            return
        
        if user_id in code_data.get("used_by", []):
            await update.message.reply_text("❌ استخدمت هذا الكود مسبقاً!")
            return
        
        points = code_data.get("points", 0)
        
        success, message = safe_add_points(user_id, points, "add", "code_redeem")
        if not success:
            await update.message.reply_text(f"❌ {message}")
            return
        
        code_data["used_count"] = code_data.get("used_count", 0) + 1
        code_data.setdefault("used_by", []).append(user_id)
        
        # التحقق إذا اكتمل الكود
        if code_data["used_count"] >= code_data["max_uses"]:
            # إرسال إشعار للمالك
            await send_to_admin(
                context.bot,
                f"🏆 كود اكتمل!\n\n"
                f"🎟️ الكود: {code_name}\n"
                f"💰 القيمة: {points} نقطة\n"
                f"👥 العدد: {code_data['max_uses']} مستخدم\n"
                f"👤 آخر مستخدم: @{get_user_data(user_id).get('username', 'بدون')}\n"
                f"⏰ الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        
        save_data(data)
        
        user_data = get_user_data(user_id)
        
        await update.message.reply_text(
            f"🎉 تم استخدام الكود!\n\n"
            f"💰 النقاط: {points}\n"
            f"🎯 نقاطك: {user_data['points']}\n"
            f"📊 استخدامات الكود: {code_data['used_count']}/{code_data['max_uses']}",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text("❌ الكود غير صحيح!")


async def show_daily_gift(query, user_id):
    """عرض الهدية اليومية"""
    user_data = get_user_data(user_id)
    daily_gift = user_data.get("daily_gift", {})
    streak = daily_gift.get("streak", 0)
    total_claimed = daily_gift.get("total_claimed", 0)
    
    can_claim, time_remaining = await can_claim_daily_gift(user_id)
    
    text = "🎁 الهدية اليومية\n\n"
    
    if can_claim:
        text += f"🎉 يمكنك المطالبة!\n\n💰 الهدية: 3 نقاط\n📊 السلسلة: {streak} يوم\n"
        
        keyboard = [
            [InlineKeyboardButton("🎁 المطالبة (3 نقاط)", callback_data="claim_daily_gift")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]
        ]
    else:
        text += f"⏰ حصلت عليها!\n\n⏳ الوقت المتبقي: {time_remaining}\n📊 السلسلة: {streak} يوم\n"
        keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")

async def handle_claim_daily_gift(query, user_id, bot):
    """المطالبة بالهدية - مع تدقيق مضاعف ضد التكرار"""
    transaction_id = f"daily_{user_id}_{int(time.time() * 1000)}"
    
    lock_key = f"daily_{user_id}"
    _daily_locks.setdefault(lock_key, threading.Lock())
    
    with _daily_locks[lock_key]:
        # ✅ التحقق الأول: من نظام Cooldown
        can_proceed, remaining, reason = cooldown_manager.can_proceed(
            user_id, 
            "daily_gift", 
            transaction_id
        )
        
        if not can_proceed:
            await query.answer(f"⏳ {reason}. انتظر {remaining:.1f} ثواني", show_alert=True)
            return
        
        # ✅ التحقق الثاني: من الوقت الفعلي
        can_claim, time_remaining = await can_claim_daily_gift(user_id)
        
        if not can_claim:
            await query.answer(f"⏳ لقد حصلت على الهدية مسبقاً! انتظر {time_remaining}", show_alert=True)
            return
        
        # ✅ التحقق الثالث: من قاعدة البيانات مباشرة
        user_data = get_user_data(user_id, force_reload=True)
        daily_gift = user_data.get("daily_gift", {})
        last_claimed = daily_gift.get("last_claimed")
        
        if last_claimed:
            try:
                last_claimed_date = datetime.strptime(last_claimed, "%Y-%m-%d %H:%M:%S")
                now = datetime.now()
                
                # إذا مر أقل من 24 ساعة
                if now - last_claimed_date < timedelta(hours=24):
                    # حساب الوقت المتبقي
                    next_claim = last_claimed_date + timedelta(hours=24)
                    remaining = next_claim - now
                    hours = int(remaining.total_seconds() // 3600)
                    minutes = int((remaining.total_seconds() % 3600) // 60)
                    
                    await query.answer(
                        f"⏳ حصلت على الهدية اليوم! انتظر {hours}:{minutes:02d}", 
                        show_alert=True
                    )
                    return
            except Exception as e:
                logger.error(f"❌ خطأ في تحقق الهدية: {e}")
        
        # ✅ بعد كل الفحوصات: منح النقاط
        points_to_add = 2
        success, message = safe_add_points(user_id, points_to_add, "add", "daily_gift", transaction_id)
        
        if not success:
            await query.answer(f"❌ {message}", show_alert=True)
            return
        
        # ✅ تحديث سجل الهدية
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        daily_gift = user_data.get("daily_gift", {})
        
        # حساب streak
        last_claimed = daily_gift.get("last_claimed")
        if last_claimed:
            try:
                last_date = datetime.strptime(last_claimed, "%Y-%m-%d %H:%M:%S")
                now_date = datetime.now()
                
                # إذا كان الفرق أقل من 48 ساعة ولم يمر يوم كامل
                if (now_date - last_date).days <= 1 and (now_date - last_date).total_seconds() >= 86400:
                    streak = daily_gift.get("streak", 0) + 1
                else:
                    streak = 1
            except:
                streak = 1
        else:
            streak = 1
        
        # ✅ تحديث البيانات
        updates = {
            "daily_gift": {
                "last_claimed": now,
                "streak": streak,
                "total_claimed": daily_gift.get("total_claimed", 0) + 1,
                "last_transaction_id": transaction_id
            }
        }
        
        update_user_data(user_id, updates, "daily_gift_update", transaction_id)
        update_stat("total_daily_gifts", 1)
        
        # ✅ الحصول على النقاط الجديدة
        updated_user_data = get_user_data(user_id, force_reload=True)
        
        success_message = (
            f"✅ تمت المطالبة بالهدية اليومية!\n\n"
            f"💰 حصلت على: {points_to_add} نقاط\n"
            f"🎯 نقاطك الآن: {updated_user_data['points']}\n"
            f"📊 السلسلة: {streak} يوم\n"
            f"📅 آخر مطالبة: {now}\n"
            f"🆔 المعاملة: {transaction_id[:10]}..."
        )
        
        keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
        
        try:
            await query.edit_message_text(
                success_message, 
                reply_markup=InlineKeyboardMarkup(keyboard), 
                parse_mode="HTML"
            )
        except Exception as e:
            # إذا فشل التعديل، أرسل رسالة جديدة
            await query.message.reply_text(
                success_message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
        
        # ✅ وضع علامة على المعاملة كمكتملة
        cooldown_manager.mark_transaction_complete(transaction_id)
        
        logger.info(
            f"🎁 تم منح الهدية اليومية لـ {user_id}: "
            f"{points_to_add} نقطة | سلسلة: {streak} | معاملة: {transaction_id}"
        )

# ===================== المتجر =====================
async def my_channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض قنوات المستخدم"""
    user_id = str(update.message.from_user.id)
    
    data = load_data()
    user_channels = []
    
    for channel_id, channel_data in data.get("channels", {}).items():
        if channel_data.get("owner") == user_id:
            user_channels.append((channel_id, channel_data))
    
    if not user_channels:
        await update.message.reply_text("📭 ليس لديك قنوات مضافة")
        return
    
    text = f"📢 قنواتك ({len(user_channels)}):\n\n"
    
    for channel_id, channel_data in user_channels:
        username = channel_data.get("username", "")
        current = channel_data.get("current", 0)
        required = channel_data.get("required", 0)
        completed = channel_data.get("completed", False)
        status = "✅ مكتملة" if completed else "🔄 جارية"
        
        # حساب المغادرين
        channel_subs = get_channel_subscribers(channel_id)
        left_count = len(channel_subs.get("left", []))
        
        text += f"📢 @{username}\n"
        text += f"   📊 التقدم: {current}/{required}\n"
        text += f"   📌 الحالة: {status}\n"
        text += f"   🚶 المغادرين: {left_count}\n"
        text += f"   🆔 المعرف: {channel_id[:8]}...\n\n"
    
    await update.message.reply_text(text, parse_mode="HTML")


async def show_store(query):
    """عرض المتجر مع التحذير"""
    warning_text = (
        "⚠️ تنبيه هام قبل الشراء:\n\n"
        "1. يجب أن يكون البوت مشرفاً في القناة\n"
        "2. القناة يجب أن تكون عامة\n"
        "3. البوت يحتاج صلاحية مشاهدة الأعضاء\n"
        "4. إذا غادر أحد الأعضاء، يتم خصم 5 نقاط منه\n\n"
        "🛒 اختر العرض:"
    )
    
    keyboard = [
        [InlineKeyboardButton("10 أعضاء - 20 نقطة", callback_data="buy_10")],
        [InlineKeyboardButton("25 عضو - 50 نقطة", callback_data="buy_25")],
        [InlineKeyboardButton("50 عضو - 100 نقطة", callback_data="buy_50")],
        [InlineKeyboardButton("100 عضو - 200 نقطة", callback_data="buy_100")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(warning_text, reply_markup=reply_markup)

async def handle_buy(query, context):
    """معالجة الشراء (نسخة محمية ضد التكرار)"""
    user_id = str(query.from_user.id)

    # منع الضغط المكرر على نفس الزر
    if context.user_data.get("buy_lock"):
        await query.answer("⏳ انتظر، العملية قيد المعالجة", show_alert=True)
        return

    context.user_data["buy_lock"] = True

    try:
        user_data = get_user_data(user_id)

        num_members = int(query.data.split("_")[1])
        points_needed = num_members * 2

        # فحص الرصيد
        if user_data.get("points", 0) < points_needed:
            keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
            await query.edit_message_text(
                f"❌ نقاطك غير كافية!\n"
                f"💰 المطلوب: {points_needed} نقطة\n"
                f"📉 رصيدك: {user_data.get('points', 0)}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # تخزين بيانات الشراء (بدون خصم وبدون transaction_id)
        context.user_data["buying"] = {
            "members": num_members,
            "points": points_needed,
            "user_id": user_id
        }

        await query.edit_message_text(
            f"🛒 <b>طلب شراء أعضاء</b>\n\n"
            f"👥 العدد: {num_members}\n"
            f"💰 السعر: {points_needed} نقطة\n\n"
            f"✍️ أرسل يوزر القناة الآن (مثال: @channel)",
            parse_mode="HTML"
        )

    finally:
        # فك القفل بعد التنفيذ
        context.user_data["buy_lock"] = False

def activate_channel(channel_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE channels 
        SET status = 'active'
        WHERE channel_id = ?
    """, (channel_id,))

    conn.commit()
    conn.close()

def complete_channel(channel_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE channels 
        SET status = 'completed',
            completed = 1,
            completed_at = ?
        WHERE channel_id = ?
    """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), channel_id))

    conn.commit()
    conn.close()

async def show_collect_points(query, user_id, context, page=0):
    """عرض قنوات التجميع المتاحة للمستخدم مع زر تحقق بجانب القناة"""
    try:
        # تحديث فوري للذاكرة المؤقتة أولاً
        user_data = get_user_data(user_id, force_reload=True)
        
        # تحديث القنوات المكتملة
        auto_complete_channels()
        
        data = load_data()
        
        # جمع جميع القنوات المتاحة
        all_channels = []
        
        for channel_id, channel_data in data.get("channels", {}).items():
            channel_username = channel_data.get("username", "")
            if not channel_username:
                continue
            
            # تخطي القنوات التي المستخدم منضم لها حالياً
            if channel_id in user_data.get("active_subscriptions", []):
                continue
            if channel_data.get("completed", False):
                continue
            if not channel_data.get("is_active", True):
                continue
            
            # التحقق مما إذا كان يجب عرض القناة للمستخدم
            should_show = await should_channel_be_shown_to_user(
                context.bot,
                user_id, 
                channel_id
            )
            if not should_show:
                continue
            
            # التحقق من joined_channels
            joined_channels = user_data.get("joined_channels", {})
            if isinstance(joined_channels, str):
                try:
                    import json
                    joined_channels = json.loads(joined_channels) if joined_channels else {}
                except:
                    joined_channels = {}
            
            # إذا كان منضماً حالياً ولم يغادر - تخطي
            if channel_id in joined_channels:
                join_info = joined_channels[channel_id]
                if join_info.get("verified", False) and not join_info.get("left", False):
                    continue
            
            can_join, reason = can_user_join_channel(user_id, channel_id, channel_username, channel_data)
            if not can_join:
                continue
            
            current = channel_data.get("current", 0)
            required = channel_data.get("required", 0)
            
            all_channels.append({
                "id": channel_id,
                "username": channel_username,
                "current": current,
                "required": required,
            })
        
        # تنظيم القنوات في صفحات (10 قنوات لكل صفحة)
        CHANNELS_PER_PAGE = 10
        total_pages = (len(all_channels) + CHANNELS_PER_PAGE - 1) // CHANNELS_PER_PAGE
        
        # التأكد من أن الصفحة ضمن النطاق
        if page < 0:
            page = 0
        elif page >= total_pages and total_pages > 0:
            page = total_pages - 1
        
        # الحصول على القنوات للصفحة الحالية
        start_idx = page * CHANNELS_PER_PAGE
        end_idx = start_idx + CHANNELS_PER_PAGE
        current_channels = all_channels[start_idx:end_idx]
        
        keyboard = []
        
        if not current_channels:
            text = "📭 لا توجد قنوات متاحة لك حالياً.\n\n"
            text += "💡 الأسباب المحتملة:\n• جميع القنوات مكتملة\n• انضممت لجميع القنوات النشطة\n• لا توجد قنوات نشطة حالياً"
            
            keyboard.append([InlineKeyboardButton("🔄 تحديث", callback_data="collect_points")])
            keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="back_main")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
            return
        
        # عرض القنوات مع زر تحقق بجانب كل قناة
        for channel in current_channels:
            channel_link = f"https://t.me/{channel['username'].replace('@', '')}"
            
            # صف واحد: زر رابط القناة وزر التحقق
            keyboard.append([
                InlineKeyboardButton(
                    f"📲 @{channel['username']}", 
                    url=channel_link
                ),
                InlineKeyboardButton(
                    f"✅ تحقق من الانضمام", 
                    callback_data=f"verify_channel_{channel['id']}"
                )
            ])
        
        # إضافة أزرار التنقل بين الصفحات إذا كان هناك أكثر من صفحة
        if total_pages > 1:
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"page_{page-1}"))
            
            nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="current_page"))
            
            if page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"page_{page+1}"))
            
            keyboard.append(nav_buttons)
        
        # أزرار التحكم
        keyboard.append([InlineKeyboardButton("🔄 تحديث", callback_data="collect_points")])
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="back_main")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # إرسال/تعديل الرسالة
        text = f"📲 اختر قناة للتحقق من الانضمام:"
        
        try:
            await query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
        except Exception as e:
            error_msg = str(e).lower()
            if "not modified" in error_msg:
                await query.answer("🔄 القائمة محدثة بالفعل!", show_alert=False)
            elif "message is not modified" in error_msg:
                pass
            else:
                await query.message.reply_text(
                    text,
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
        
    except Exception as e:
        logger.error(f"❌ خطأ في show_collect_points: {e}")
        import traceback
        traceback.print_exc()
        await query.answer("❌ حدث خطأ في عرض القنوات", show_alert=True)

async def handle_report_channel(query, user_id, bot):
    """معالجة الإبلاغ مع طلب السبب"""
    try:
        channel_id = query.data.replace("report_", "")
        
        # التحقق من أن المستخدم لم يبلغ عن هذه القناة مسبقاً في هذه الجولة
        if not can_user_report_channel(user_id, channel_id):
            await query.answer("⚠️ أبلغت عن هذه القناة مسبقاً في هذه الجولة!", show_alert=True)
            return
        
        # حفظ حالة التبليغ للاستكمال
        context = query.message._bot._dispatcher.context_types.context()
        context.user_data["reporting_channel"] = {
            "channel_id": channel_id,
            "user_id": user_id,
            "message_id": query.message.message_id
        }
        
        # الحصول على معلومات القناة
        channel_data = get_channel_data(channel_id)
        channel_username = channel_data.get("username", "غير معروف") if channel_data else "غير معروف"
        
        # طلب السبب
        try:
            await query.edit_message_text(
                f"🚨 **الإبلاغ عن قناة**\n\n"
                f"📢 القناة: @{channel_username}\n\n"
                f"📝 الرجاء كتابة سبب الإبلاغ:\n"
                f"مثال:\n"
                f"• محتوى غير مناسب\n"
                f"• قناة مزيفة\n"
                f"• رابط لا يعمل\n"
                f"• مشكلة تقنية\n\n"
                f"⚠️ **ملاحظة:** يمكنك الإبلاغ مرة واحدة فقط لكل قناة في كل جولة",
                parse_mode="HTML"
            )
        except:
            await query.message.reply_text(
                f"🚨 **الإبلاغ عن قناة**\n\n"
                f"📢 القناة: @{channel_username}\n\n"
                f"📝 الرجاء كتابة سبب الإبلاغ:\n"
                f"مثال:\n"
                f"• محتوى غير مناسب\n"
                f"• قناة مزيفة\n"
                f"• رابط لا يعمل\n"
                f"• مشكلة تقنية\n\n"
                f"⚠️ **ملاحظة:** يمكنك الإبلاغ مرة واحدة فقط لكل قناة في كل جولة",
                parse_mode="HTML"
            )
        
    except Exception as e:
        logger.error(f"❌ خطأ في handle_report_channel: {e}")
        await query.answer("❌ حدث خطأ في معالجة التبليغ", show_alert=True)

async def handle_report_channel(query, user_id, bot):
    """معالجة الإبلاغ مع طلب السبب"""
    channel_id = query.data.replace("report_", "")
    
    # التحقق من أن المستخدم لم يبلغ عن هذه القناة مسبقاً في هذه الجولة
    if not can_user_report_channel(user_id, channel_id):
        await query.answer("⚠️ أبلغت عن هذه القناة مسبقاً في هذه الجولة!", show_alert=True)
        return
    
    # حفظ حالة التبليغ للاستكمال
    context.user_data["reporting_channel"] = {
        "channel_id": channel_id,
        "user_id": user_id,
        "message_id": query.message.message_id
    }
    
    # طلب السبب
    await query.edit_message_text(
        "🚨 **الإبلاغ عن قناة**\n\n"
        "📝 الرجاء كتابة سبب الإبلاغ:\n"
        "مثال:\n"
        "• محتوى غير مناسب\n"
        "• قناة مزيفة\n"
        "• رابط لا يعمل\n"
        "• مشكلة تقنية\n\n"
        "⚠️ **ملاحظة:** يمكنك الإبلاغ مرة واحدة فقط لكل قناة في كل جولة",
        parse_mode="Markdown"
    )

def can_user_report_channel(user_id, channel_id):
    """التحقق من إمكانية الإبلاغ عن القناة (مرة واحدة لكل جولة)"""
    from database import get_user_data, get_channel_data
    
    user_data = get_user_data(user_id)
    channel_data = get_channel_data(channel_id)
    
    if not channel_data:
        return False
    
    # جولة القناة الحالية
    current_round = channel_data.get("reuse_count", 0)
    
    # الحصول على التقارير السابقة للمستخدم
    reports = user_data.get("channel_reports", {})
    
    # إذا كان هناك تقرير سابق لنفس القناة في نفس الجولة
    if channel_id in reports:
        report_data = reports[channel_id]
        if report_data.get("round", -1) == current_round:
            return False
    
    return True

def add_channel_report(user_id, channel_id, reason):
    """إضافة تقرير عن قناة"""
    from database import get_user_data, get_channel_data, update_user_data
    
    user_data = get_user_data(user_id)
    channel_data = get_channel_data(channel_id)
    
    if not channel_data:
        return False
    
    current_round = channel_data.get("reuse_count", 0)
    
    # تحديث تقارير المستخدم
    reports = user_data.get("channel_reports", {})
    reports[channel_id] = {
        "reason": reason,
        "reported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "round": current_round,
        "channel_username": channel_data.get("username", "")
    }
    
    # تحديث إحصائيات التبليغ
    updates = {
        "channel_reports": reports,
        "total_reports": user_data.get("total_reports", 0) + 1
    }
    
    # إضافة القناة إلى القائمة المحظورة للمستخدم (لفترة الجولة الحالية)
    blocked_channels = user_data.get("blocked_channels_by_report", [])
    if channel_id not in blocked_channels:
        blocked_channels.append(channel_id)
        updates["blocked_channels_by_report"] = blocked_channels
    
    if update_user_data(user_id, updates, "channel_report"):
        # تحديث إحصائيات النظام
        update_system_stats("total_reports", increment=1)
        return True
    
    return False

def check_and_mark_completed_channels():
    """
    التحقق من القنوات المكتملة ووضع علامة عليها في SQLite
    """
    import sqlite3
    from database import logger, DB_NAME
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # جلب جميع القنوات غير المكتملة
        cursor.execute('''
            SELECT channel_id, current, required 
            FROM channels 
            WHERE completed = 0 AND current >= required
        ''')
        
        completed_channels = cursor.fetchall()
        completed_count = 0
        
        for channel_id, current, required in completed_channels:
            # وضع علامة الاكتمال
            cursor.execute('''
                UPDATE channels 
                SET completed = 1, 
                    completed_at = ?
                WHERE channel_id = ?
            ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), channel_id))
            
            completed_count += 1
            logger.info(f"✅ تم وضع علامة اكتمال على القناة {channel_id}")
        
        conn.commit()
        conn.close()
        
        if completed_count > 0:
            logger.info(f"🎯 تم وضع علامة اكتمال على {completed_count} قناة")
        
        return completed_count
        
    except Exception as e:
        logger.error(f"❌ خطأ في check_and_mark_completed_channels: {e}")
        return 0

async def is_user_member_of_channel(bot, channel_username, user_id):
    """
    التحقق المباشر من تيليغرام إذا كان المستخدم عضو في القناة
    يرجع:
        True: عضو
        False: غير عضو
        None: خطأ في التحقق
    """
    try:
        channel_username = channel_username.replace("@", "").strip()
        
        # الحصول على معلومات القناة
        try:
            chat = await bot.get_chat(chat_id=f"@{channel_username}")
        except Exception as chat_error:
            logger.error(f"❌ خطأ في جلب القناة @{channel_username}: {chat_error}")
            return None
        
        # الحصول على حالة العضوية
        try:
            member = await bot.get_chat_member(chat_id=chat.id, user_id=user_id)
            
            # الحالات الصالحة للعضوية
            valid_statuses = ["member", "administrator", "creator"]
            
            if member.status in valid_statuses:
                return True
            else:
                return False
                
        except Exception as member_error:
            error_text = str(member_error).lower()
            
            # حالات الخطأ
            if "user not found" in error_text or "user not participant" in error_text:
                return False
            elif "forbidden" in error_text or "kicked" in error_text:
                return None
            else:
                logger.error(f"❌ خطأ في التحقق من العضوية: {member_error}")
                return None
                
    except Exception as e:
        logger.error(f"❌ خطأ عام في is_user_member_of_channel: {e}")
        return None

# ==================== نظام التخزين المؤقت للتحقق من العضوية ====================

_user_member_cache = {}
_cache_expiry = 300  # 5 دقائق

async def cached_is_user_member(bot, channel_username, user_id):
    """
    نسخة مخبأة من التحقق من العضوية لتحسين الأداء
    يخزن النتائج لمدة 5 دقائق
    """
    cache_key = f"{user_id}_{channel_username}"
    current_time = time.time()
    
    # التحقق من التخزين المؤقت
    if cache_key in _user_member_cache:
        cache_time, is_member = _user_member_cache[cache_key]
        if current_time - cache_time < _cache_expiry:
            logger.debug(f"📦 استخدام التخزين المؤقت للتحقق: {cache_key}")
            return is_member
    
    # التحقق المباشر
    logger.debug(f"🔍 التحقق المباشر من العضوية: {cache_key}")
    is_member = await is_user_member_of_channel(bot, channel_username, user_id)
    
    if is_member is not None:
        _user_member_cache[cache_key] = (current_time, is_member)
        logger.debug(f"✅ تم تخزين نتيجة التحقق: {cache_key} = {is_member}")
    
    return is_member


def cleanup_member_cache():
    """
    تنظيف التخزين المؤقت القديم
    """
    try:
        current_time = time.time()
        expired_keys = []
        
        for key, (cache_time, _) in list(_user_member_cache.items()):
            if current_time - cache_time > _cache_expiry:
                expired_keys.append(key)
        
        for key in expired_keys:
            del _user_member_cache[key]
        
        if expired_keys:
            logger.debug(f"🧹 تم تنظيف {len(expired_keys)} مدخل من ذاكرة التحقق")
        
        return len(expired_keys)
        
    except Exception as e:
        logger.error(f"❌ خطأ في تنظيف ذاكرة التحقق: {e}")
        return 0

async def should_channel_be_shown_to_user(bot, user_id, channel_id):
    """
    التحقق مما إذا كان يجب عرض القناة للمستخدم مع التحقق المباشر من تيليغرام
    """
    import sqlite3
    import json
    from database import logger, DB_NAME
    
    user_id = str(user_id)
    channel_id = str(channel_id)
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # ✅ جلب بيانات القناة
        cursor.execute('''
            SELECT username, owner, completed, is_active, current, required, reuse_count
            FROM channels WHERE channel_id = ?
        ''', (channel_id,))
        
        result = cursor.fetchone()
        if not result:
            conn.close()
            return False
        
        username, owner, completed, is_active, current, required, reuse_count = result
        
        # 1. القناة مكتملة - لا تعرض
        if completed:
            conn.close()
            return False
        
        # 2. القناة غير نشطة - لا تعرض
        if not is_active:
            conn.close()
            return False
        
        # 3. المستخدم صاحب القناة - لا تعرض
        if str(user_id) == str(owner):
            conn.close()
            return False
        
        # 4. القناة وصلت للعدد المطلوب - تحقق واكتملها إذا لزم
        if current >= required and not completed:
            cursor.execute('''
                UPDATE channels SET 
                    completed = 1,
                    completed_at = ?,
                    is_active = 0
                WHERE channel_id = ?
            ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), channel_id))
            conn.commit()
            conn.close()
            return False
        
        # ✅ ✅ ✅ التحقق المباشر من تيليغرام (هذا هو الحل الحقيقي)
        try:
            is_member = await is_user_member_of_channel(bot, username, int(user_id))
            
            if is_member is None:
                # خطأ في التحقق - نستمر بالفحص العادي
                logger.warning(f"⚠️ فشل التحقق المباشر من @{username} للمستخدم {user_id}")
            elif is_member:
                # ✅ المستخدم منضم للقناة - لا تعرضها له
                logger.info(f"✅ اكتشاف: المستخدم {user_id} منضم لـ @{username} (خارج البوت)")
                conn.close()
                return False
                
        except Exception as e:
            logger.error(f"❌ خطأ في التحقق المباشر: {e}")
        
        # ✅ جلب بيانات المستخدم
        cursor.execute('''
            SELECT joined_channels, temp_left_channels, active_subscriptions
            FROM users WHERE user_id = ?
        ''', (user_id,))
        
        user_result = cursor.fetchone()
        if not user_result:
            conn.close()
            return False
        
        joined_channels_str, temp_left_str, active_subs_str = user_result
        
        # 5. التحقق من joined_channels أولاً
        joined_channels = {}
        if joined_channels_str:
            try:
                joined_channels = json.loads(joined_channels_str)
            except:
                joined_channels = {}
        
        if channel_id in joined_channels:
            join_info = joined_channels[channel_id]
            
            # ✅ منضم حالياً وموثق ولم يغادر
            if join_info.get("verified", False) and not join_info.get("left", False):
                conn.close()
                return False
            
            # ✅ غادر سابقاً - يتحقق من الجولة
            if join_info.get("left", False):
                user_round = join_info.get("round", 0)
                current_round = reuse_count or 0
                
                # إذا كانت هناك جولة جديدة، يسمح بالانضمام
                if current_round > user_round:
                    conn.close()
                    return True
                else:
                    # نفس الجولة، إذا كان في temp_left يسمح بالانضمام
                    temp_left = []
                    if temp_left_str:
                        try:
                            temp_left = json.loads(temp_left_str)
                        except:
                            temp_left = []
                    
                    if channel_id in temp_left:
                        conn.close()
                        return True
                    else:
                        conn.close()
                        return False  # غادر نفس الجولة ولكن ليس في temp_left
        
        # 6. التحقق من temp_left_channels
        temp_left = []
        if temp_left_str:
            try:
                temp_left = json.loads(temp_left_str)
            except:
                temp_left = []
        
        if channel_id in temp_left:
            conn.close()
            return True
        
        # 7. التحقق من active_subscriptions
        active_subscriptions = []
        if active_subs_str:
            try:
                active_subscriptions = json.loads(active_subs_str)
            except:
                pass
        
        if channel_id in active_subscriptions:
            conn.close()
            return False
        
        # 8. لم ينضم مطلقاً
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في should_channel_be_shown_to_user: {e}")
        if 'conn' in locals():
            conn.close()
        return False

async def handle_join_channel(query, user_id, bot):
    """
    معالجة طلب الانضمام للقناة
    عرض رابط القناة وزر التحقق
    """
    from database import load_data, logger
    
    try:
        await query.answer()
        
        channel_id = query.data.replace("join_channel_", "")
        
        data = load_data()
        
        # التحقق من وجود القناة
        if channel_id not in data.get("channels", {}):
            await query.answer("❌ القناة غير متاحة", show_alert=True)
            return
        
        channel = data["channels"][channel_id]
        channel_username = channel.get("username", "")
        
        if not channel_username:
            await query.answer("❌ القناة غير صالحة", show_alert=True)
            return
        
        # التحقق من إمكانية الانضمام
        can_join, reason = can_user_join_channel(user_id, channel_id, channel_username, channel)
        if not can_join:
            await query.answer(reason, show_alert=True)
            return
        
        # رابط القناة
        channel_link = f"https://t.me/{channel_username.replace('@', '')}"
        
        # رسالة الانضمام
        join_message = (
            f"📢 **@{channel_username}**\n\n"
            f"📋 **خطوات الحصول على النقاط:**\n"
            f"1️⃣ اضغط زر 'رابط القناة' أدناه\n"
            f"2️⃣ اشترك في القناة\n"
            f"3️⃣ انتظر 5-10 ثواني\n"
            f"4️⃣ ارجع واضغط زر 'تحقق'\n\n"
            f"💰 **المكافأة:** 2 نقاط ✨"
        )
        
        try:
            await query.edit_message_text(
                join_message,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📲 رابط القناة", url=channel_link)],
                    [InlineKeyboardButton("✅ تحقق والحصول على 2 نقاط", 
                                         callback_data=f"verify_channel_{channel_id}")]
                ])
            )
        except Exception:
            # إذا فشل التعديل، أرسل رسالة جديدة
            await query.message.reply_text(
                join_message,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📲 رابط القناة", url=channel_link)],
                    [InlineKeyboardButton("✅ تحقق والحصول على 2 نقاط", 
                                         callback_data=f"verify_channel_{channel_id}")]
                ])
            )
            
    except Exception as e:
        logger.error(f"❌ خطأ في handle_join_channel: {e}")
        import traceback
        traceback.print_exc()
        await query.answer("❌ حدث خطأ!", show_alert=True)

def get_user_data(user_id, force_reload=False):
    """جلب بيانات المستخدم مع دعم force_reload"""
    from database import get_user_data as db_get_user_data
    import json
    
    # إذا طلبنا إعادة التحميل، نظهر من ذاكرة التخزين المؤقت أولاً
    if not force_reload:
        with _cache_lock:
            if user_id in _data_cache:
                cache_time = _cache_last_update.get(user_id, 0)
                current_time = time.time()
                
                # التحقق من انتهاء صلاحية التخزين المؤقت
                if current_time - cache_time < CACHE_TTL:
                    return _data_cache[user_id].copy()
    
    # جلب البيانات من قاعدة البيانات
    try:
        user_data = db_get_user_data(user_id)
        
        # تأكد من تنسيق البيانات الصحيح
        if isinstance(user_data.get("joined_channels"), str):
            try:
                user_data["joined_channels"] = json.loads(user_data["joined_channels"]) if user_data["joined_channels"] else {}
            except:
                user_data["joined_channels"] = {}
        
        if isinstance(user_data.get("active_subscriptions"), str):
            try:
                user_data["active_subscriptions"] = json.loads(user_data["active_subscriptions"]) if user_data["active_subscriptions"] else []
            except:
                user_data["active_subscriptions"] = []
        
        # تخزين في التخزين المؤقت
        with _cache_lock:
            _data_cache[user_id] = user_data.copy()
            _cache_last_update[user_id] = time.time()
        
        return user_data
        
    except Exception as e:
        logger.error(f"❌ خطأ في get_user_data للمستخدم {user_id}: {e}")
        # إرجاع بيانات افتراضية
        return create_default_user_data(user_id)

async def handle_verify_channel(query, user_id, bot, context):
    """معالجة التحقق من الانضمام للقناة مع نظام العداد الذكي والتحديث الفوري"""
    import json
    
    try:
        await query.answer("⏳ جاري التحقق من اشتراكك...")
    except Exception:
        pass
    
    channel_id = query.data.replace("verify_channel_", "")
    
    # منع التضارب: قفل فريد لكل عملية
    lock_key = f"verify_lock_{user_id}_{channel_id}"
    if lock_key in _active_locks:
        await query.answer("⚠️ العملية قيد المعالجة، انتظر قليلاً", show_alert=True)
        return
    
    _active_locks[lock_key] = True
    
    try:
        # التحقق من Cooldown
        transaction_id = f"verify_{user_id}_{channel_id}_{int(time.time() * 1000)}"
        can_proceed, remaining, reason = cooldown_manager.can_proceed(
            user_id, 
            "verify_channel", 
            transaction_id
        )
        
        if not can_proceed:
            await query.answer(f"⏳ {reason}. انتظر {remaining:.1f} ثواني", show_alert=True)
            del _active_locks[lock_key]
            return
        
        # الاتصال بقاعدة البيانات
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # بدء المعاملة
        cursor.execute('BEGIN TRANSACTION')
        
        try:
            # جلب بيانات القناة
            cursor.execute('''
                SELECT username, owner, current, required, completed, is_active, reuse_count 
                FROM channels 
                WHERE channel_id = ?
            ''', (channel_id,))
            
            result = cursor.fetchone()
            if not result:
                await query.answer("❌ القناة غير متاحة", show_alert=True)
                conn.rollback()
                conn.close()
                del _active_locks[lock_key]
                return
            
            username, owner, current, required, completed, is_active, reuse_count = result
            
            # التحقق من صحة القناة
            if completed:
                await query.answer("❌ هذه القناة اكتملت بالفعل!", show_alert=True)
                conn.rollback()
                conn.close()
                del _active_locks[lock_key]
                return
            
            if not is_active:
                await query.answer("❌ القناة غير نشطة حالياً", show_alert=True)
                conn.rollback()
                conn.close()
                del _active_locks[lock_key]
                return
            
            # التحقق من أن المستخدم ليس صاحب القناة
            if str(user_id) == str(owner):
                await query.answer("❌ لا يمكنك الانضمام لقناتك الخاصة!", show_alert=True)
                conn.rollback()
                conn.close()
                del _active_locks[lock_key]
                return
            
            # جلب بيانات المستخدم
            cursor.execute('''
                SELECT points, joined_channels, active_subscriptions 
                FROM users 
                WHERE user_id = ?
            ''', (user_id,))
            
            user_result = cursor.fetchone()
            if not user_result:
                await query.answer("❌ خطأ في بيانات المستخدم", show_alert=True)
                conn.rollback()
                conn.close()
                del _active_locks[lock_key]
                return
            
            current_points, joined_channels_str, active_subs_str = user_result
            
            # تحليل joined_channels
            joined_channels = {}
            if joined_channels_str:
                try:
                    joined_channels = json.loads(joined_channels_str)
                except:
                    joined_channels = {}
            
            # التحقق من الانضمام السابق
            if channel_id in joined_channels:
                join_info = joined_channels[channel_id]
                
                # إذا كان منضماً حالياً ولم يغادر
                if join_info.get("verified", False) and not join_info.get("left", False):
                    await query.answer("❌ سبق أن انضممت لهذه القناة!", show_alert=True)
                    conn.rollback()
                    conn.close()
                    del _active_locks[lock_key]
                    return
                
                # إذا غادر سابقاً، يمكنه الانضمام مرة أخرى
                if join_info.get("left", False):
                    # تم السماح بالعودة
                    pass
            
            # التحقق من اشتراك المستخدم في القناة
            try:
                is_subscribed = await check_channel_subscription(bot, int(user_id), username)
                
                if is_subscribed is None:
                    await query.edit_message_text(
                        f"⚠️ حدث خطأ في التحقق\n\n"
                        f"📢 @{username}\n\n"
                        f"🔧 حاول مرة أخرى بعد قليل.",
                        parse_mode="HTML"
                    )
                    conn.rollback()
                    conn.close()
                    del _active_locks[lock_key]
                    return
                
                if not is_subscribed:
                    await query.edit_message_text(
                        f"❌ أنت غير مشترك بالقناة!\n\n"
                        f"📢 @{username}\n\n"
                        f"🔗 اشترك في القناة أولاً ثم اضغط تحقق مرة أخرى",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📲 اشترك في القناة", url=f"https://t.me/{username}")],
                            [InlineKeyboardButton("🔄 تحقق مرة أخرى", callback_data=f"verify_channel_{channel_id}")]
                        ])
                    )
                    conn.rollback()
                    conn.close()
                    del _active_locks[lock_key]
                    return
                    
            except Exception as e:
                logger.error(f"خطأ في التحقق من الاشتراك: {e}")
                await query.edit_message_text("⚠️ حدث خطأ في التحقق، حاول لاحقاً")
                conn.rollback()
                conn.close()
                del _active_locks[lock_key]
                return
            
            # ✅ المستخدم مشترك - منح النقاط وتحديث العداد
            
            # زيادة العداد
            new_current = current + 1
            cursor.execute('''
                UPDATE channels 
                SET current = ?, last_activity = ? 
                WHERE channel_id = ?
            ''', (new_current, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), channel_id))
            
            # منح 3 نقاط للمستخدم
            points_to_add = 2
            new_points = current_points + points_to_add
            
            cursor.execute('''
                UPDATE users 
                SET points = ?, last_active = ? 
                WHERE user_id = ?
            ''', (new_points, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id))
            
            # ✅ تحديث معلومات انضمام المستخدم - تحديث فوري
            join_info = {
                "channel_username": username,
                "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "verified": True,
                "points_earned": points_to_add,
                "left": False,
                "round": reuse_count or 0,
                "verified_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "immediate_update": True
            }
            
            joined_channels[channel_id] = join_info
            
            # تحديث active_subscriptions
            active_subscriptions = []
            if active_subs_str:
                try:
                    active_subscriptions = json.loads(active_subs_str)
                except:
                    active_subscriptions = []
            
            if channel_id not in active_subscriptions:
                active_subscriptions.append(channel_id)
            
            cursor.execute('''
                UPDATE users 
                SET joined_channels = ?, active_subscriptions = ? 
                WHERE user_id = ?
            ''', (json.dumps(joined_channels), json.dumps(active_subscriptions), user_id))
            
            # ✅ تحديث joined_users في القناة
            cursor.execute('SELECT joined_users FROM channels WHERE channel_id = ?', (channel_id,))
            joined_users_result = cursor.fetchone()
            
            joined_users = []
            if joined_users_result and joined_users_result[0]:
                try:
                    joined_users = json.loads(joined_users_result[0])
                except:
                    joined_users = []
            
            # إضافة المستخدم الجديد
            user_join_info = {
                "user_id": user_id,
                "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "left": False,
                "points_earned": points_to_add,
                "immediate": True
            }
            
            joined_users.append(user_join_info)
            cursor.execute('''
                UPDATE channels 
                SET joined_users = ? 
                WHERE channel_id = ?
            ''', (json.dumps(joined_users), channel_id))
            
            # ✅ إزالة القناة من temp_left_channels إذا كانت موجودة
            cursor.execute('SELECT temp_left_channels FROM users WHERE user_id = ?', (user_id,))
            temp_left_result = cursor.fetchone()
            
            if temp_left_result:
                temp_left_str = temp_left_result[0]
                temp_left = []
                if temp_left_str:
                    try:
                        temp_left = json.loads(temp_left_str)
                    except:
                        temp_left = []
                
                # إزالة القناة من temp_left
                if channel_id in temp_left:
                    temp_left.remove(channel_id)
                    cursor.execute('''
                        UPDATE users SET temp_left_channels = ? WHERE user_id = ?
                    ''', (json.dumps(temp_left), user_id))
            
            # تسجيل المعاملة
            cursor.execute('''
                INSERT INTO transactions 
                (user_id, channel_id, points, type, details, transaction_id, timestamp) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_id,
                channel_id,
                points_to_add,
                'channel_join',
                json.dumps({
                    "channel_username": username,
                    "previous_counter": current,
                    "new_counter": new_current,
                    "reuse_count": reuse_count
                }),
                transaction_id,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
            
            # التحقق إذا اكتملت القناة
            if new_current >= required:
                cursor.execute('''
                    UPDATE channels 
                    SET completed = 1, completed_at = ? 
                    WHERE channel_id = ?
                ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), channel_id))
                
                completion_notified = True
            else:
                completion_notified = False
            
            # حفظ التغييرات
            conn.commit()
            
            # ✅ تحديث الذاكرة المؤقتة فوراً
            with _cache_lock:
                if user_id in _data_cache:
                    # تحديث joined_channels
                    if "joined_channels" not in _data_cache[user_id]:
                        _data_cache[user_id]["joined_channels"] = {}
                    
                    _data_cache[user_id]["joined_channels"][channel_id] = join_info
                    
                    # تحديث active_subscriptions
                    if "active_subscriptions" not in _data_cache[user_id]:
                        _data_cache[user_id]["active_subscriptions"] = []
                    
                    if channel_id not in _data_cache[user_id]["active_subscriptions"]:
                        _data_cache[user_id]["active_subscriptions"].append(channel_id)
                    
                    # تحديث النقاط
                    _data_cache[user_id]["points"] = new_points
                    
                    # تحديث الوقت
                    _cache_last_update[user_id] = time.time()
            
            # ✅ مسح cache الرسالة القديمة لمنع عرض القناة مرة أخرى
            message_cache_key = f"collect_points_{user_id}"
            if message_cache_key in _data_cache:
                del _data_cache[message_cache_key]
            
            # ✅ إرسال إشعار للمالك
            if str(owner) != str(ADMIN_ID):
                try:
                    user_data = get_user_data(user_id)
                    user_username = user_data.get("username", "بدون")
                    
                    await bot.send_message(
                        int(owner),
                        f"✨ عضو جديد في قناتك!\n\n"
                        f"📢 القناة: @{username}\n"
                        f"👤 العضو: @{user_username}\n"
                        f"📊 العداد: {current} → {new_current}/{required}\n"
                        f"💰 حصل على: 2 نقاط\n"
                        f"⏰ الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        parse_mode="HTML"
                    )
                except Exception as notify_error:
                    logger.error(f"❌ خطأ في إرسال إشعار للمالك: {notify_error}")
            
            # ✅ إرسال إشعار الاكتمال إذا اكتملت القناة
            if completion_notified:
                try:
                    # إشعار المالك
                    if str(owner) != str(ADMIN_ID):
                        await bot.send_message(
                            int(owner),
                            f"🎉 اكتملت قناتك!\n\n"
                            f"📢 القناة: @{username}\n"
                            f"👥 العدد النهائي: {new_current}/{required}\n"
                            f"⏰ الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                            f"✅ يمكنك إعادة استخدام القناة لجولة جديدة",
                            parse_mode="HTML"
                        )
                    
                    # إشعار الأدمن
                    await bot.send_message(
                        ADMIN_ID,
                        f"✅ قناة اكتملت!\n\n"
                        f"📢 @{username}\n"
                        f"👤 المالك: {owner}\n"
                        f"📊 العدد: {new_current}/{required}\n"
                        f"🔄 جولة: {reuse_count + 1 if reuse_count else 1}\n"
                        f"⏰ الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        parse_mode="HTML"
                    )
                except Exception as complete_error:
                    logger.error(f"❌ خطأ في إرسال إشعار الاكتمال: {complete_error}")
            
            # ✅ إعداد رسالة النجاح
            success_message = (
                f"✅ تم التحقق بنجاح!\n\n"
                f"📢 القناة: @{username}\n"
                f"💰 حصلت على: {points_to_add} نقاط\n"
                f"🎯 نقاطك الآن: {new_points}\n"
                f"📊 العداد: {current} → {new_current}/{required}"
            )
            
            if new_current >= required:
                success_message += f"\n\n🎉 القناة اكتملت!"
            
            if channel_id in joined_channels and joined_channels[channel_id].get("left", False):
                success_message += f"\n\n🔄 مرحباً بعودتك!"
            
            success_message += f"\n\n🎉 استمر في جمع النقاط!"
            
            # ✅ إعادة تحميل بيانات المستخدم للتأكد من التحديث الفوري
            user_data = get_user_data(user_id, force_reload=True)
            
            await query.edit_message_text(
                success_message,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 المزيد من القنوات", callback_data="collect_points")],
                    [InlineKeyboardButton("🏠 الرئيسية", callback_data="back_main")]
                ])
            )
            
            # وضع علامة على المعاملة كمكتملة
            cooldown_manager.mark_transaction_complete(transaction_id)
            
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ خطأ في المعاملة: {e}")
            await query.answer("⚠️ حدث خطأ، حاول مرة أخرى", show_alert=True)
            raise
        finally:
            conn.close()
            
    except sqlite3.OperationalError as e:
        logger.error(f"❌ خطأ في قاعدة البيانات: {e}")
        await query.answer("⚠️ حدث خطأ في النظام، حاول لاحقاً", show_alert=True)
    except Exception as e:
        logger.error(f"❌ خطأ غير متوقع: {e}")
        await query.answer("⚠️ حدث خطأ، حاول مرة أخرى", show_alert=True)
    finally:
        # تحرير القفل
        if lock_key in _active_locks:
            del _active_locks[lock_key]
        
        # ✅ فرض تحديث للذاكرة المؤقتة
        with _cache_lock:
            # مسح cache المستخدم لضمان التحديث
            if user_id in _data_cache:
                del _data_cache[user_id]
            if user_id in _cache_last_update:
                del _cache_last_update[user_id]



# ===================== نظام التوب المطور (يستثني الأدمن) =====================

async def show_top(query):
    """عرض التوب المطور مع الأقسام والروابط - يستثني الأدمن"""
    users_data = load_users()
    data = load_data()
    admins = data.get("admins", [])
    
    # تصفية المستخدمين (استبعاد الأدمن)
    filtered_users = {}
    for uid, user_data in users_data.items():
        # استبعاد الأدمن من قائمة التوب
        if str(uid) not in admins or str(uid) == str(query.from_user.id):
            # السماح للمستخدم برؤية نفسه حتى لو كان أدمن
            filtered_users[uid] = user_data
    
    # 1. أعلى 10 نقاط (باستثناء الأدمن)
    users_points = []
    for uid, data in filtered_users.items():
        username = data.get("username", "")
        first_name = data.get("first_name", "مجهول")
        last_name = data.get("last_name", "")
        points = data.get("points", 0)
        invites = data.get("invites", 0)
        
        # تحديد الاسم المعروض
        if first_name and first_name != "مجهول":
            display_name = first_name
            # إضافة اللقب إذا موجود
            if last_name:
                display_name = f"{first_name} {last_name}"
        else:
            # إذا لم يكن هناك اسم، نستخدم الآيدي
            display_name = f"المستخدم {uid[:8]}"
        
        # التحقق إذا كان أدمن (للتلوين)
        is_admin_user = str(uid) in admins
        
        users_points.append({
            "id": uid,
            "username": username,
            "display_name": display_name,
            "points": points,
            "invites": invites,
            "is_admin": is_admin_user,
            "first_name": first_name
        })
    
    # ترتيب حسب النقاط (تنازلي)
    users_points.sort(key=lambda x: x["points"], reverse=True)
    
    # 2. أعلى 10 دعوات (باستثناء الأدمن)
    users_invites = sorted(users_points.copy(), key=lambda x: x["invites"], reverse=True)
    
    # إنشاء النص
    text = "🏆 **لوحة المتصدرين** 🏆\n\n"
    text += "👑 *ملاحظة:* الأدمن مستبعدون من التوب\n\n"
    
    # قسم النقاط
    text += "💰 **أعلى 10 نقاط:**\n"
    for i, user in enumerate(users_points[:10], 1):
        medal = ""
        if i == 1: medal = "🥇"
        elif i == 2: medal = "🥈"
        elif i == 3: medal = "🥉"
        else: medal = f"{i}."
        
        # تحديد طريقة الربط
        if user["username"] and user["username"] != "" and user["username"] != "بدون":
            # إذا لديه يوزر: رابط مباشر ليوزر التليجرام
            user_link = f"https://t.me/{user['username']}"
            name_part = f"[{user['display_name']}]({user_link})"
        else:
            # إذا ليس لديه يوزر: رابط عبر user_id
            user_link = f"tg://user?id={user['id']}"
            name_part = f"[{user['display_name']}]({user_link})"
        
        # إضافة تاج إذا كان أدمن (فقط إذا كان المستخدم نفسه)
        admin_badge = " 👑" if user["is_admin"] and str(user["id"]) == str(query.from_user.id) else ""
        
        text += f"{medal} {name_part}{admin_badge} - **{user['points']} نقطة**\n"
    
    text += "\n" + "─" * 30 + "\n\n"
    
    # قسم الدعوات
    text += "👥 **أعلى 10 دعوات:**\n"
    for i, user in enumerate(users_invites[:10], 1):
        medal = ""
        if i == 1: medal = "👑"
        elif i == 2: medal = "🥈"
        elif i == 3: medal = "🥉"
        else: medal = f"{i}."
        
        # تحديد طريقة الربط
        if user["username"] and user["username"] != "" and user["username"] != "بدون":
            # إذا لديه يوزر: رابط مباشر ليوزر التليجرام
            user_link = f"https://t.me/{user['username']}"
            name_part = f"[{user['display_name']}]({user_link})"
        else:
            # إذا ليس لديه يوزر: رابط عبر user_id
            user_link = f"tg://user?id={user['id']}"
            name_part = f"[{user['display_name']}]({user_link})"
        
        # إضافة تاج إذا كان أدمن (فقط إذا كان المستخدم نفسه)
        admin_badge = " 👑" if user["is_admin"] and str(user["id"]) == str(query.from_user.id) else ""
        
        text += f"{medal} {name_part}{admin_badge} - **{user['invites']} دعوة**\n"
    
    # إضافة زر تحديث
    keyboard = [
        [InlineKeyboardButton("🔄 تحديث التوب", callback_data="top")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]
    ]
    
    try:
        await query.edit_message_text(
            text, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"خطأ في عرض التوب: {e}")
        # إذا فشل التعديل، أرسل رسالة جديدة
        await query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
            disable_web_page_preview=True
        )

# ===================== نسخة بديلة مع أزرار =====================



async def show_invite_link(query, user_id, bot):
    """عرض رابط الدعوة"""
    bot_username = (await bot.get_me()).username
    invite_link = f"https://t.me/{bot_username}?start={user_id}"
    
    user_data = get_user_data(user_id)
    
    text = f"🔗 رابطك:\n\n{invite_link}\n\n👥 دعواتك: {user_data['invites']}\n💰 النقاط: {user_data['invites'] * 3}"
    
    keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def show_codes_panel(query):
    """عرض لوحة الأكواد"""
    text = "🎟️ نظام الأكواد\n\nاستخدام: /code اسم_الكود"
    keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_admin_panel(query):
    """✅ لوحة الإدمن المتقدمة مع تقرير المراقبة"""
    keyboard = [
        [InlineKeyboardButton("📊 إحصائيات مفصلة", callback_data="admin_stats_detailed"),
         InlineKeyboardButton("📋 تقرير المراقبة", callback_data="admin_monitor_report")],  # ✅ زر جديد
        [InlineKeyboardButton("🎟️ الأكواد النشطة", callback_data="admin_active_codes"),
         InlineKeyboardButton("📢 القنوات النشطة", callback_data="admin_active_channels")],
        [InlineKeyboardButton("👥 إدارة الأدمن", callback_data="admin_manage_admins")],
        [InlineKeyboardButton("⚠️ حظر", callback_data="admin_ban"),
         InlineKeyboardButton("✅ فك حظر", callback_data="admin_unban")],
        [InlineKeyboardButton("🔇 كتم", callback_data="admin_mute"),
         InlineKeyboardButton("🔊 فك كتم", callback_data="admin_unmute")],
        [InlineKeyboardButton("➕ إضافة قناة", callback_data="admin_add_channel"),
         InlineKeyboardButton("➖ حذف قناة", callback_data="admin_remove_channel")],
        [InlineKeyboardButton("🔒 قناة إجباري", callback_data="admin_force_add"),
         InlineKeyboardButton("🔓 حذف إجباري", callback_data="admin_force_remove")],
        [InlineKeyboardButton("🎟️ إضافة كود", callback_data="admin_add_code"),
         InlineKeyboardButton("🗑️ حذف كود", callback_data="admin_remove_code")],
        [InlineKeyboardButton("💰 إضافة نقاط", callback_data="admin_give_points"),
         InlineKeyboardButton("💸 خصم نقاط", callback_data="admin_take_points")],
        [InlineKeyboardButton("👤 معلومات مستخدم", callback_data="admin_user_info")],
        [InlineKeyboardButton("📢 بث رسالة", callback_data="admin_broadcast")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("👑 **لوحة الإدمن المتقدمة**\n\n⚡ مع تقرير مراقبة القنوات", reply_markup=reply_markup, parse_mode="Markdown")

def mark_channel_as_left(user_id, channel_id, channel_data=None):
    """
    تحديد القناة كمتروكة مع خصم 5 نقاط وتحديث العداد في SQLite
    مع ضمان تقليل العداد إلى الصفر إذا لزم الأمر
    """
    import sqlite3
    import json
    import time
    from datetime import datetime
    
    from database import get_channel_data, safe_add_points, logger, DB_NAME
    
    user_id = str(user_id)
    channel_id = str(channel_id)
    
    try:
        # الحصول على بيانات القناة
        if channel_data is None:
            channel_data = get_channel_data(channel_id)
        
        if not channel_data:
            logger.error(f"❌ القناة {channel_id} غير موجودة")
            return False
        
        # التحقق من اكتمال القناة
        if channel_data.get("completed", False):
            logger.info(f"⏭️ قناة مكتملة - لا خصم: {channel_id}")
            return True
        
        # ❗❗❗ الحصول على العداد الحالي من قاعدة البيانات مباشرة
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('SELECT current, required FROM channels WHERE channel_id = ?', (channel_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            logger.error(f"❌ القناة {channel_id} غير موجودة في SQLite")
            return False
        
        current_count, required_count = result
        logger.info(f"📊 العداد قبل التحديث: {current_count}/{required_count}")
        
        # خصم 5 نقاط
        transaction_id = f"leave_penalty_{user_id}_{channel_id}_{int(time.time())}"
        success, msg = safe_add_points(
            user_id, 
            5, 
            "subtract", 
            "left_channel_penalty",
            transaction_id
        )
        
        if not success:
            logger.error(f"❌ فشل خصم النقاط: {msg}")
            conn.close()
            return False
        
        # ❗❗❗ تقليل العداد - مهم جداً
        if current_count > 0:
            new_count = current_count - 1
            logger.info(f"📉 تقليل العداد: {current_count} → {new_count}")
            
            # تحديث left_users
            left_users = []
            cursor.execute('SELECT left_users FROM channels WHERE channel_id = ?', (channel_id,))
            left_result = cursor.fetchone()
            
            if left_result and left_result[0]:
                try:
                    left_users = json.loads(left_result[0])
                except:
                    left_users = []
            
            # إضافة المستخدم إلى left_users
            left_users.append({
                "user_id": user_id,
                "left_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "points_deducted": 5,
                "previous_count": current_count,
                "new_count": new_count
            })
            
            # ❗❗❗ تحديث العداد في SQLite
            cursor.execute('''
                UPDATE channels SET 
                    current = ?,
                    last_activity = ?,
                    left_users = ?
                WHERE channel_id = ?
            ''', (
                new_count,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                json.dumps(left_users, ensure_ascii=False),
                channel_id
            ))
            
            logger.info(f"✅ تم تحديث العداد في SQLite: {new_count}/{required_count}")
        else:
            logger.warning(f"⚠️ العداد بالفعل 0، لا يمكن تقليله أكثر")
            new_count = 0
        
        # تحديث بيانات المستخدم
        cursor.execute('''
            SELECT joined_channels, active_subscriptions, temp_left_channels 
            FROM users WHERE user_id = ?
        ''', (user_id,))
        
        user_result = cursor.fetchone()
        
        if user_result:
            joined_channels_str, active_subs_str, temp_left_str = user_result
            
            # تحديث joined_channels
            joined_channels = {}
            if joined_channels_str:
                try:
                    joined_channels = json.loads(joined_channels_str)
                except:
                    joined_channels = {}
            
            if channel_id in joined_channels:
                joined_channels[channel_id]["left"] = True
                joined_channels[channel_id]["left_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                joined_channels[channel_id]["penalty_applied"] = True
            
            # تحديث active_subscriptions
            active_subscriptions = []
            if active_subs_str:
                try:
                    active_subscriptions = json.loads(active_subs_str)
                except:
                    active_subscriptions = []
            
            if channel_id in active_subscriptions:
                active_subscriptions = [c for c in active_subscriptions if c != channel_id]
            
            # تحديث temp_left_channels
            temp_left = []
            if temp_left_str:
                try:
                    temp_left = json.loads(temp_left_str)
                except:
                    temp_left = []
            
            if channel_id not in temp_left:
                temp_left.append(channel_id)
            
            # تحديث بيانات المستخدم
            cursor.execute('''
                UPDATE users SET 
                    joined_channels = ?,
                    active_subscriptions = ?,
                    temp_left_channels = ?
                WHERE user_id = ?
            ''', (
                json.dumps(joined_channels, ensure_ascii=False),
                json.dumps(active_subscriptions, ensure_ascii=False),
                json.dumps(temp_left, ensure_ascii=False),
                user_id
            ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ تم وضع علامة المغادرة للمستخدم {user_id} من القناة {channel_id}")
        logger.info(f"✅ العداد النهائي: {new_count}/{required_count}")
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في mark_channel_as_left للمستخدم {user_id}: {e}")
        import traceback
        traceback.print_exc()
        return False

async def show_admin_stats(query):
    """إحصائيات البوت"""
    stats = get_user_statistics()
    
    if not stats:
        await query.answer("❌ خطأ في جلب الإحصائيات", show_alert=True)
        return
    
    data = load_data()
    channels = data.get("channels", {})
    
    completed_channels = sum(1 for ch in channels.values() if ch.get("completed"))
    active_channels = len(channels) - completed_channels
    
    text = (
        f"📊 إحصائيات البوت:\n\n"
        f"👥 المستخدمين:\n"
        f"• الإجمالي: {stats['total_users']}\n"
        f"• النشطين اليوم: {stats['active_users']}\n"
        f"• الجدد اليوم: {stats['new_today']}\n"
        f"• الجدد الأسبوع: {stats['new_week']}\n"
        f"• الجدد الشهر: {stats['new_month']}\n"
        f"• باليوزر: {stats['with_username']}\n"
        f"• بالدعوات: {stats['with_invites']}\n"
        f"• المحظورين: {stats['banned_users']}\n"
        f"• المكتومين: {stats['muted_users']}\n\n"
        f"💰 النقاط:\n"
        f"• الإجمالي: {stats['total_points']}\n"
        f"• إجمالي الدعوات: {stats['total_invites']}\n\n"
        f"📢 القنوات:\n"
        f"• الإجمالي: {len(channels)}\n"
        f"• النشطة: {active_channels}\n"
        f"• المكتملة: {completed_channels}"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 رجوع للوحة", callback_data="admin_panel")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def back_to_main(query, user_id):
    """العودة للرئيسية مع إظهار النقاط السالبة"""
    user_data = get_user_data(user_id)
    points = user_data.get("points", 0)
    
    # تحضير رسالة النقاط
    if points < 0:
        points_text = f"⚠️ نقاطك: {points} (سالب)"
    else:
        points_text = f"🎯 نقاطك: {points}"
    
    welcome_msg = (
        f"👋 أهلاً!\n{points_text}\n🔗 دعواتك: {user_data['invites']}\n\n"
        f"اختر من القائمة:"
    )
    
    keyboard = [
        [InlineKeyboardButton("🛒 المتجر", callback_data="store")],
        [InlineKeyboardButton("📊 جمع النقاط", callback_data="collect_points")],
        [InlineKeyboardButton("🎁 الهدية اليومية", callback_data="daily_gift")],
        [InlineKeyboardButton("🏆 التوب", callback_data="top")],
        [InlineKeyboardButton("🔗 رابط الدعوة", callback_data="invite_link")],
        [InlineKeyboardButton("🎟️ الأكواد", callback_data="codes")],
    ]
    
    # إذا كان المستخدم بالنقاط السالبة، أضف زر خاص
    if points < 0:
        keyboard.insert(0, [
            InlineKeyboardButton("⚠️ رصيدك بالسالب!", callback_data="negative_info")
        ])
    
    if is_admin(query.from_user.id):
        keyboard.append([InlineKeyboardButton("👑 لوحة الإدمن", callback_data="admin_panel")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(welcome_msg, reply_markup=reply_markup)


# ===================== معالجة رسائل الأدمن =====================

async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة رسائل الأدمن"""
    user_id = str(update.message.from_user.id)

    if context.user_data.get("buying"):
        return

    if not is_admin(update.message.from_user.id):
        return
    
    text = update.message.text
    
    if "admin_action" in context.user_data:
        action = context.user_data["admin_action"]
        
        try:
            if action == "user_info":
                target = text.replace("@", "").strip()
                target_uid = target if target.isdigit() else find_user_by_username(target)
                
                if target_uid:
                    user_data = get_user_data(target_uid)
                    
                    info_text = (
                        f"👤 معلومات المستخدم:\n\n"
                        f"🆔 ID: {target_uid}\n"
                        f"👤 اليوزر: @{user_data.get('username', 'بدون')}\n"
                        f"📛 الاسم: {user_data.get('first_name', '')} {user_data.get('last_name', '')}\n"
                        f"🎯 النقاط: {user_data.get('points', 0)}\n"
                        f"💰 مجموع الربح: {user_data.get('total_earned', 0)}\n"
                        f"💸 مجموع الصرف: {user_data.get('total_spent', 0)}\n"
                        f"🔗 الدعوات: {user_data.get('invites', 0)}\n"
                        f"📅 الانضمام: {user_data.get('first_join', '')}\n"
                        f"🔄 آخر نشاط: {user_data.get('last_active', '')}"
                    )
                    
                    await update.message.reply_text(info_text, parse_mode="HTML")
                else:
                    await update.message.reply_text("❌ المستخدم غير موجود.")
                
                del context.user_data["admin_action"]
                
            elif action == "broadcast":
                broadcast_msg = text
                users_data = load_users()
                sent_count = 0
                failed_count = 0
                
                await update.message.reply_text("⏳ جاري الإرسال...")
                
                for i, uid in enumerate(users_data.keys(), 1):
                    try:
                        await context.bot.send_message(
                            int(uid), 
                            f"📢 إشعار من الإدارة:\n\n{broadcast_msg}", 
                            parse_mode="HTML"
                        )
                        sent_count += 1
                        
                        if i % 50 == 0:
                            await update.message.reply_text(f"📤 {i}/{len(users_data)}...")
                        
                        time.sleep(0.1)
                        
                    except:
                        failed_count += 1
                
                await update.message.reply_text(
                    f"✅ اكتمل!\n\n✅ تم: {sent_count}\n❌ فشل: {failed_count}",
                    parse_mode="HTML"
                )
                del context.user_data["admin_action"]
                
            elif action == "give_points":
                parts = text.split()
                if len(parts) < 2:
                    await update.message.reply_text("❌ تنسيق خاطئ. أرسل: يوزر/ID عدد")
                    return
                
                target_input = parts[0]
                amount = int(parts[1])
                target_uid = target_input if target_input.isdigit() else find_user_by_username(target_input)
                
                if target_uid:
                    success, message = safe_add_points(target_uid, amount, "add", "admin_give_points")
                    if not success:
                        await update.message.reply_text(f"❌ {message}")
                        return
                    
                    user_data = get_user_data(target_uid)
                    
                    await update.message.reply_text(
                        f"✅ تم!\n\n👤 @{user_data.get('username', target_uid)}\n💰 {amount} نقطة\n🎯 النقاط: {user_data['points']}",
                        parse_mode="HTML"
                    )
                    
                    try:
                        await context.bot.send_message(
                            int(target_uid),
                            f"🎉 مكافأة!\n\n💰 حصلت على: {amount} نقطة\n🎯 نقاطك: {user_data['points']}",
                            parse_mode="HTML"
                        )
                    except:
                        pass
                else:
                    await update.message.reply_text("❌ المستخدم غير موجود.")
                
                del context.user_data["admin_action"]
                
            elif action == "take_points":
                parts = text.split()
                if len(parts) < 2:
                    await update.message.reply_text("❌ تنسيق خاطئ.")
                    return
                
                target_input = parts[0]
                amount = int(parts[1])
                target_uid = target_input if target_input.isdigit() else find_user_by_username(target_input)
                
                if target_uid:
                    success, message = safe_add_points(target_uid, amount, "subtract", "admin_take_points")
                    if not success:
                        await update.message.reply_text(f"❌ {message}")
                        return
                    
                    user_data = get_user_data(target_uid)
                    
                    await update.message.reply_text(
                        f"✅ تم الخصم!\n\n👤 @{user_data.get('username', target_uid)}\n💸 {amount} نقطة\n🎯 النقاط: {user_data['points']}",
                        parse_mode="HTML"
                    )
                else:
                    await update.message.reply_text("❌ المستخدم غير موجود.")
                
                del context.user_data["admin_action"]
                
            elif action == "ban_user":
                target = text.replace("@", "").strip()
                target_uid = target if target.isdigit() else find_user_by_username(target)
                
                if target_uid:
                    data = load_data()
                    if target_uid not in data["banned_users"]:
                        data["banned_users"].append(target_uid)
                        save_data(data)
                        
                        await update.message.reply_text(f"✅ تم حظر المستخدم {target_uid}")
                        
                        try:
                            await context.bot.send_message(int(target_uid), "🚫 تم حظرك من البوت!")
                        except:
                            pass
                    else:
                        await update.message.reply_text("⚠️ محظور مسبقاً.")
                else:
                    await update.message.reply_text("❌ المستخدم غير موجود.")
                
                del context.user_data["admin_action"]
                
            elif action == "unban_user":
                target = text.replace("@", "").strip()
                target_uid = target if target.isdigit() else find_user_by_username(target)
                
                if target_uid:
                    data = load_data()
                    if target_uid in data["banned_users"]:
                        data["banned_users"].remove(target_uid)
                        save_data(data)
                        
                        await update.message.reply_text(f"✅ تم فك حظر {target_uid}")
                        
                        try:
                            await context.bot.send_message(int(target_uid), "✅ تم فك حظرك!")
                        except:
                            pass
                    else:
                        await update.message.reply_text("❌ غير محظور.")
                
                del context.user_data["admin_action"]
                
            elif action == "mute_user":
                parts = text.split()
                if len(parts) < 2:
                    await update.message.reply_text("❌ تنسيق خاطئ.")
                    return
                
                target_input = parts[0]
                mute_seconds = int(parts[1])
                reason = " ".join(parts[2:]) if len(parts) > 2 else "بدون سبب"
                
                target_uid = target_input if target_input.isdigit() else find_user_by_username(target_input)
                
                if target_uid:
                    if is_admin(int(target_uid)):
                        await update.message.reply_text("❌ لا يمكن كتم أدمن!")
                        return
                    
                    mute_info = add_muted_user(target_uid, mute_seconds, reason)
                    
                    duration_text = "دائم" if mute_seconds == 0 else format_time(mute_seconds)
                    
                    try:
                        await context.bot.send_message(
                            int(target_uid),
                            f"🔇 تم كتمك!\n\n⏰ المدة: {duration_text}\n📝 السبب: {reason}",
                            parse_mode="HTML"
                        )
                    except:
                        pass
                    
                    await update.message.reply_text(f"✅ تم كتم المستخدم {target_uid}\n⏰ {duration_text}")
                else:
                    await update.message.reply_text("❌ المستخدم غير موجود.")
                
                del context.user_data["admin_action"]
                
            elif action == "unmute_user":
                target = text.replace("@", "").strip()
                target_uid = target if target.isdigit() else find_user_by_username(target)
                
                if target_uid:
                    is_user_muted, mute_until = is_muted(target_uid)
                    
                    if not is_user_muted:
                        await update.message.reply_text("❌ غير مكتوم!")
                        return
                    
                    if remove_muted_user(target_uid):
                        await update.message.reply_text(f"✅ تم فك كتم {target_uid}")
                        
                        try:
                            await context.bot.send_message(int(target_uid), "🔊 تم فك كتمك!")
                        except:
                            pass
                
                del context.user_data["admin_action"]
                
            elif action == "add_channel":
                parts = text.split()
                if len(parts) < 2:
                    await update.message.reply_text("❌ تنسيق خاطئ.")
                    return
                
                channel_username = parts[0].replace("@", "").strip()
                members_count = int(parts[1])
                
                bot_is_admin = await check_bot_is_admin(context.bot, channel_username)
                
                if not bot_is_admin:
                    await update.message.reply_text(f"❌ البوت ليس مشرفاً في @{channel_username}")
                    del context.user_data["admin_action"]
                    return
                
                channel_id = f"admin_channel_{int(time.time())}_{abs(hash(channel_username)) % 10000}"
                
                channel_data = {
                    "username": channel_username,
                    "owner": str(ADMIN_ID),
                    "required": members_count,
                    "current": 0,
                    "completed": False,
                    "joined_users": [],
                    "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "admin_added": True,
                    "reuse_count": 0
                }
                
                save_channel_data(channel_id, channel_data)
                
                await update.message.reply_text(
                    f"✅ تم إضافة قناة!\n\n📢 @{channel_username}\n👥 {members_count} عضو\n🆔 {channel_id}",
                    parse_mode="HTML"
                )
                del context.user_data["admin_action"]
                
            elif action == "remove_channel":
                channel_input = text.strip()
                
                data = load_data()
                channels = data.get("channels", {})
                removed = []
                
                for cid, channel_data in channels.items():
                    if channel_input.startswith("@"):
                        channel_username = channel_input.replace("@", "").strip()
                        if channel_data.get("username") == channel_username:
                            removed.append(cid)
                    else:
                        if cid == channel_input:
                            removed.append(cid)
                
                if removed:
                    for cid in removed:
                        delete_channel(cid)
                    
                    await update.message.reply_text(f"✅ تم حذف {len(removed)} قناة")
                else:
                    await update.message.reply_text("❌ القناة غير موجودة!")
                
                del context.user_data["admin_action"]
                
            elif action == "add_force":
                channel_username = text.replace("@", "").strip()
                
                bot_is_admin = await check_bot_is_admin(context.bot, channel_username)
                
                if not bot_is_admin:
                    await update.message.reply_text(f"❌ البوت ليس مشرفاً في @{channel_username}")
                    del context.user_data["admin_action"]
                    return
                
                data = load_data()
                if channel_username not in data.get("force_sub_channels", []):
                    data["force_sub_channels"].append(channel_username)
                    save_data(data)
                    
                    await update.message.reply_text(f"✅ تم إضافة قناة إجباري: @{channel_username}")
                else:
                    await update.message.reply_text("⚠️ مضافة مسبقاً.")
                
                del context.user_data["admin_action"]
                
            elif action == "remove_force":
                channel_username = text.replace("@", "").strip()
                
                data = load_data()
                if channel_username in data.get("force_sub_channels", []):
                    data["force_sub_channels"].remove(channel_username)
                    save_data(data)
                    await update.message.reply_text(f"✅ تم حذف @{channel_username} من الإجباري")
                else:
                    await update.message.reply_text("❌ غير موجودة.")
                
                del context.user_data["admin_action"]
                
            elif action == "add_code":
                parts = text.split()
                if len(parts) < 3:
                    await update.message.reply_text("❌ تنسيق خاطئ.")
                    return
                
                code_name = parts[0].upper()
                points = int(parts[1])
                max_uses = int(parts[2])
                
                data = load_data()
                
                if code_name in data.get("codes", {}):
                    await update.message.reply_text("⚠️ الكود موجود!")
                    return
                
                data["codes"][code_name] = {
                    "points": points,
                    "max_uses": max_uses,
                    "used_count": 0,
                    "used_by": [],
                    "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "created_by": str(ADMIN_ID)
                }
                
                save_data(data)
                
                await update.message.reply_text(
                    f"✅ تم إضافة كود!\n\n🎟️ {code_name}\n💰 {points} نقطة\n👥 {max_uses} مستخدم",
                    parse_mode="HTML"
                )
                del context.user_data["admin_action"]
                
            elif action == "remove_code":
                code_name = text.upper().strip()
                
                data = load_data()
                if code_name in data.get("codes", {}):
                    del data["codes"][code_name]
                    save_data(data)
                    await update.message.reply_text(f"✅ تم حذف الكود: {code_name}")
                else:
                    await update.message.reply_text("❌ الكود غير موجود.")
                
                del context.user_data["admin_action"]
                
        except ValueError:
            await update.message.reply_text("❌ الرقم غير صحيح!")
            if "admin_action" in context.user_data:
                del context.user_data["admin_action"]
        except Exception as e:
            await update.message.reply_text(f"❌ خطأ: {str(e)}")
            if "admin_action" in context.user_data:
                del context.user_data["admin_action"]

# ===================== معالجة الشراء =====================

# ===================== نظام مشتركي القنوات =====================

def update_user_joined_channels_immediate(user_id, channel_id, verified=True):
    """
    تحديث فوري لـ joined_channels بعد الانضمام مباشرة
    """
    import json
    import sqlite3
    from database import logger, DB_NAME
    
    user_id = str(user_id)
    channel_id = str(channel_id)
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # جلب بيانات القناة أولاً
        cursor.execute('SELECT username, reuse_count FROM channels WHERE channel_id = ?', (channel_id,))
        channel_result = cursor.fetchone()
        
        if not channel_result:
            conn.close()
            return False
        
        channel_username, current_round = channel_result
        
        # جلب بيانات المستخدم
        cursor.execute('SELECT joined_channels, active_subscriptions FROM users WHERE user_id = ?', (user_id,))
        user_result = cursor.fetchone()
        
        if not user_result:
            conn.close()
            return False
        
        joined_channels_str, active_subs_str = user_result
        
        # تحديث joined_channels
        joined_channels = {}
        if joined_channels_str:
            try:
                joined_channels = json.loads(joined_channels_str)
            except:
                joined_channels = {}
        
        # معلومات الانضمام الجديدة
        join_info = {
            "channel_username": channel_username,
            "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "verified": verified,
            "points_earned": 2 if verified else 0,
            "left": False,
            "round": current_round or 0,
            "verified_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S") if verified else None,
            "immediate_update": True
        }
        
        joined_channels[channel_id] = join_info
        
        # تحديث active_subscriptions
        active_subscriptions = []
        if active_subs_str:
            try:
                active_subscriptions = json.loads(active_subs_str)
            except:
                active_subscriptions = []
        
        if channel_id not in active_subscriptions:
            active_subscriptions.append(channel_id)
        
        # حفظ التحديثات
        cursor.execute('''
            UPDATE users SET 
                joined_channels = ?,
                active_subscriptions = ?
            WHERE user_id = ?
        ''', (
            json.dumps(joined_channels, ensure_ascii=False),
            json.dumps(active_subscriptions, ensure_ascii=False),
            user_id
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ تم التحديث الفوري لـ joined_channels للمستخدم {user_id} في @{channel_username}")
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في update_user_joined_channels_immediate: {e}")
        if 'conn' in locals():
            conn.close()
        return False

def remove_channel_from_user_display_immediate(user_id, channel_id):
    """
    إزالة فورية للقناة من عرض المستخدم بعد الانضمام
    """
    import json
    import sqlite3
    from database import logger, DB_NAME
    
    user_id = str(user_id)
    channel_id = str(channel_id)
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # إزالة من temp_left_channels إذا كان موجوداً
        cursor.execute('SELECT temp_left_channels FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if result:
            temp_left_str = result[0]
            temp_left = []
            if temp_left_str:
                try:
                    temp_left = json.loads(temp_left_str)
                except:
                    temp_left = []
            
            # إزالة القناة من temp_left
            if channel_id in temp_left:
                temp_left.remove(channel_id)
                
                cursor.execute('''
                    UPDATE users SET temp_left_channels = ? WHERE user_id = ?
                ''', (json.dumps(temp_left, ensure_ascii=False), user_id))
                
                conn.commit()
                logger.info(f"✅ تم الإزالة الفورية للقناة {channel_id} من temp_left للمستخدم {user_id}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في remove_channel_from_user_display_immediate: {e}")
        if 'conn' in locals():
            conn.close()
        return False

def has_user_joined_channel_before(user_id, channel_id):
    """
    ✅ التحقق إذا كان المستخدم انضم لهذه القناة مسبقاً (ولم يغادر)
    """
    user_id = str(user_id)
    channel_id = str(channel_id)
    
    user_data = get_user_data(user_id)
    joined_channels = user_data.get("joined_channels", {})
    
    # التحقق من joined_channels
    if channel_id in joined_channels:
        join_info = joined_channels[channel_id]
        
        # ✅ إذا لم يغادر القناة أبداً
        if not join_info.get("left", False):
            return True  # منضم حالياً
    
    # التحقق من active_subscriptions
    if channel_id in user_data.get("active_subscriptions", []):
        return True
    
    return False  # يمكنه الانضمام مرة أخرى


def can_user_join_channel(user_id, channel_id, channel_username, channel_data=None):
    """
    التحقق من إمكانية الانضمام مع مراعاة المغادرة - نسخة SQLite محسنة
    """
    import sqlite3
    import json
    from database import get_channel_data, is_admin, ADMIN_ID, logger, DB_NAME
    
    user_id = str(user_id)
    channel_id = str(channel_id)
    
    try:
        # الحصول على بيانات القناة
        if channel_data is None:
            channel_data = get_channel_data(channel_id)
        
        if not channel_data:
            return False, "القناة غير موجودة!"
        
        # ❌ التحقق من اكتمال القناة
        if channel_data.get("completed", False):
            return False, "هذه القناة مكتملة حالياً!"
        
        # ❌ القناة غير نشطة
        if not channel_data.get("is_active", True):
            return False, "القناة غير نشطة حالياً!"
        
        # ❌ المستخدم صاحب القناة
        if user_id == str(channel_data.get("owner")):
            return False, "لا يمكنك الانضمام لقناتك الخاصة!"
        
        # ❌ منع الأدمن من الانضمام للقنوات الإدارية
        if str(channel_data.get("owner")) == str(ADMIN_ID) and is_admin(int(user_id)):
            return False, "لا يمكنك الانضمام لقناة الإدارة!"
        
        # ✅ التحقق من أن القناة لم تصل للعدد المطلوب (اكتساح)
        current = channel_data.get("current", 0)
        required = channel_data.get("required", 0)
        
        if current >= required and not channel_data.get("completed", False):
            # القناة وصلت للعدد المطلوب لكنها لم تكتمل بعد
            return False, "القناة وصلت للعدد المطلوب، جاري التجهيز..."
        
        # الاتصال بقاعدة البيانات
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # ✅ جلب بيانات المستخدم من قاعدة البيانات
        cursor.execute('''
            SELECT joined_channels, temp_left_channels, active_subscriptions, permanent_left_channels 
            FROM users WHERE user_id = ?
        ''', (user_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return False, "المستخدم غير موجود!"
        
        joined_channels_str, temp_left_str, active_subs_str, permanent_left_str = result
        
        # ✅ تحليل joined_channels
        joined_channels = {}
        if joined_channels_str:
            try:
                joined_channels = json.loads(joined_channels_str)
            except:
                joined_channels = {}
        
        # ✅ تحليل temp_left_channels
        temp_left = []
        if temp_left_str:
            try:
                temp_left = json.loads(temp_left_str)
            except:
                temp_left = []
        
        # ✅ تحليل active_subscriptions
        active_subscriptions = []
        if active_subs_str:
            try:
                active_subscriptions = json.loads(active_subs_str)
            except:
                active_subscriptions = []
        
        # ✅ تحليل permanent_left_channels
        permanent_left = []
        if permanent_left_str:
            try:
                permanent_left = json.loads(permanent_left_str)
            except:
                permanent_left = []
        
        # ❌ إذا كان في permanent_left (غادر نهائياً) لا يمكنه الانضمام
        if channel_id in permanent_left:
            return False, "غادرت هذه القناة نهائياً ولا يمكنك العودة!"
        
        # ✅ إذا كان في temp_left (غادر مؤقتاً) يمكنه الانضمام
        if channel_id in temp_left:
            # تحقق من أن القناة مازالت نشطة
            if current < required:
                return True, "يمكنك الانضمام مرة أخرى بعد مغادرتك السابقة!"
            else:
                # القناة مكتملة أو وصلت للعدد المطلوب
                return False, "القناة اكتملت بعد مغادرتك!"
        
        # إذا المستخدم انضم سابقاً
        if channel_id in joined_channels:
            join_info = joined_channels[channel_id]
            
            verified = join_info.get("verified", False)
            left = join_info.get("left", False)
            
            # ✅ إذا كان منضماً ولم يغادر - لا يمكنه الانضمام
            if verified and not left:
                return False, "لقد انضممت لهذه القناة مسبقاً!"
            
            # ✅ إذا غادر - يمكنه الانضمام مرة أخرى
            if left:
                # تحقق من أنه ليس في temp_left
                if channel_id not in temp_left:
                    # أضفه إلى temp_left للسماح بالعودة
                    temp_left.append(channel_id)
                    # تحديث قاعدة البيانات
                    conn = sqlite3.connect(DB_NAME)
                    cursor = conn.cursor()
                    cursor.execute('''
                        UPDATE users SET temp_left_channels = ? WHERE user_id = ?
                    ''', (json.dumps(temp_left), user_id))
                    conn.commit()
                    conn.close()
                
                # تحقق من أن القناة مازالت متاحة
                if current < required:
                    return True, "يمكنك الانضمام مرة أخرى بعد مغادرتك السابقة!"
                else:
                    return False, "القناة اكتملت بعد مغادرتك!"
        
        # ❌ إذا كان في active_subscriptions (ما زال منضماً)
        if channel_id in active_subscriptions:
            return False, "أنت منضم حالياً لهذه القناة!"
        
        # ✅ لم ينضم مطلقاً
        return True, "يمكنك الانضمام الآن!"
        
    except Exception as e:
        logger.error(f"❌ خطأ في can_user_join_channel للمستخدم {user_id}: {e}")
        import traceback
        traceback.print_exc()
        return False, "حدث خطأ في التحقق"

def get_channel_subscribers(channel_id):
    """جلب مشتركي القناة من قاعدة البيانات"""
    data = load_data()
    
    if "channels_subscribers" not in data:
        data["channels_subscribers"] = {}
    
    return data["channels_subscribers"].get(
        channel_id, 
        {"active": [], "left": [], "history": []}
    )

def update_user_channel_join_info(user_id, channel_id, channel_username, current_round, 
                                   reactivated_at, points_earned, transaction_id):
    """
    تحديث معلومات انضمام المستخدم للقناة في SQLite
    يحفظ: تاريخ الجولات، النقاط المكتسبة، حالة التوثيق
    """
    import json
    import sqlite3
    from database import get_user_data, update_user_data, logger, DB_NAME
    
    user_id = str(user_id)
    channel_id = str(channel_id)
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # ✅ 1. جلب بيانات المستخدم من قاعدة البيانات
        cursor.execute('SELECT joined_channels, active_subscriptions FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            logger.error(f"❌ المستخدم {user_id} غير موجود في قاعدة البيانات")
            return False, None
        
        joined_channels_str, active_subscriptions_str = result
        joined_channels = {}
        active_subscriptions = []
        
        # تحويل JSON إلى Python objects
        if joined_channels_str:
            try:
                joined_channels = json.loads(joined_channels_str)
            except Exception as e:
                logger.error(f"❌ خطأ في تحليل joined_channels للمستخدم {user_id}: {e}")
                joined_channels = {}
        
        if active_subscriptions_str:
            try:
                active_subscriptions = json.loads(active_subscriptions_str)
            except Exception as e:
                logger.error(f"❌ خطأ في تحليل active_subscriptions للمستخدم {user_id}: {e}")
                active_subscriptions = []
        
        # ✅ 2. معلومات الانضمام الجديدة
        join_info = {
            "channel_username": channel_username,
            "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "verified": True,
            "points_earned": points_earned,
            "left": False,
            "round": current_round,
            "reactivated_at": reactivated_at,
            "channel_reactivated": bool(reactivated_at),
            "join_round": current_round + 1,
            "verified_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "transaction_id": transaction_id,
            "last_verified": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "active",
            "join_type": "new" if not reactivated_at else "reactivated"
        }
        
        # ✅ 3. حفظ معلومات الجولات السابقة
        if channel_id in joined_channels:
            old_info = joined_channels[channel_id]
            
            if "previous_versions" not in join_info:
                join_info["previous_versions"] = []
            
            join_info["previous_versions"].append({
                "old_round": old_info.get("round", 0),
                "old_joined_at": old_info.get("joined_at"),
                "old_reactivated_at": old_info.get("reactivated_at"),
                "old_points_earned": old_info.get("points_earned", 0),
                "archived_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        # ✅ 4. تحديث joined_channels
        joined_channels[channel_id] = join_info
        
        # ✅ 5. تحديث active_subscriptions
        if channel_id not in active_subscriptions:
            active_subscriptions.append(channel_id)
        
        # ✅ 6. تحويل البيانات إلى JSON للتخزين
        joined_channels_json = json.dumps(joined_channels, ensure_ascii=False)
        active_subscriptions_json = json.dumps(active_subscriptions, ensure_ascii=False)
        
        # ✅ 7. تحديث قاعدة البيانات مباشرة
        cursor.execute('''
            UPDATE users 
            SET joined_channels = ?, 
                active_subscriptions = ?,
                last_active = ?,
                inactive = 0
            WHERE user_id = ?
        ''', (
            joined_channels_json,
            active_subscriptions_json,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            user_id
        ))
        
        # ✅ 8. تسجيل المعاملة
        cursor.execute('''
            INSERT INTO transactions (user_id, channel_id, points, type, details, transaction_id, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id,
            channel_id,
            points_earned,
            'channel_join_update',
            json.dumps({
                "channel_username": channel_username,
                "current_round": current_round,
                "reactivated_at": reactivated_at,
                "points_earned": points_earned
            }),
            transaction_id,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ تم تحديث معلومات انضمام {user_id} للقناة @{channel_username}")
        return True, join_info
            
    except Exception as e:
        logger.error(f"❌ خطأ في update_user_channel_join_info: {e}")
        import traceback
        traceback.print_exc()
        
        if 'conn' in locals():
            conn.close()
        return False, None

def update_system_stats(stat_name, increment=0, value=None):
    """
    تحديث إحصائيات النظام - نسخة مصححة
    """
    from database import update_stat, get_stat, logger
    
    try:
        if value is not None:
            # تعيين قيمة مطلقة
            update_stat(stat_name, value)
        elif increment != 0:
            # زيادة تراكمية - الحصول على القيمة الحالية أولاً
            current = get_stat(stat_name)  # ❗ تعديل هنا: إزالة القيمة الافتراضية
            if current is None:
                current = 0
            
            update_stat(stat_name, current + increment)
        
        logger.debug(f"✅ تم تحديث إحصائية {stat_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في update_system_stats: {e}")
        import traceback
        traceback.print_exc()
        return False


def update_channel_subscriber(channel_id, user_id, action="join"):
    """تحديث حالة مشترك في القناة"""
    data = load_data()
    
    if "channels_subscribers" not in data:
        data["channels_subscribers"] = {}
    
    if channel_id not in data["channels_subscribers"]:
        data["channels_subscribers"][channel_id] = {
            "active": [],
            "left": [],
            "history": []
        }
    
    channel_subs = data["channels_subscribers"][channel_id]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if action == "join":
        # إضافة للنشطين
        if user_id not in channel_subs["active"]:
            channel_subs["active"].append(user_id)
        # إزالة من المغادرين
        if user_id in channel_subs["left"]:
            channel_subs["left"].remove(user_id)
    
    elif action == "leave":
        # إزالة من النشطين
        if user_id in channel_subs["active"]:
            channel_subs["active"].remove(user_id)
        # إضافة للمغادرين
        if user_id not in channel_subs["left"]:
            channel_subs["left"].append(user_id)
    
    # تسجيل في التاريخ
    channel_subs["history"].append({
        "user_id": user_id,
        "action": action,
        "timestamp": timestamp
    })
    
    # حفظ آخر 100 حدث فقط
    if len(channel_subs["history"]) > 100:
        channel_subs["history"] = channel_subs["history"][-100:]
    
    save_data(data)
    return True




async def monitor_channel_activity(context: ContextTypes.DEFAULT_TYPE):
    """مراقبة نشاط القنوات وخصم 5 نقاط من المغادرين مع تحديث temp_left"""
    try:
        if "monitor_running" in _active_locks:
            return
        
        _active_locks["monitor_running"] = True
        
        logger.info("🔍 بدء مراقبة القنوات السريعة...")
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT channel_id, username, owner, current, required 
            FROM channels 
            WHERE completed = 0 AND is_active = 1
        ''')
        
        active_channels = cursor.fetchall()
        
        total_penalties = 0
        total_checked = 0
        
        for channel_id, username, owner, current, required in active_channels:
            try:
                cursor.execute('SELECT joined_users FROM channels WHERE channel_id = ?', (channel_id,))
                joined_result = cursor.fetchone()
                
                if not joined_result or not joined_result[0]:
                    continue
                
                try:
                    joined_users = json.loads(joined_result[0])
                except:
                    joined_users = []
                
                if not joined_users:
                    continue
                
                active_members = []
                for user_info in joined_users:
                    if isinstance(user_info, dict):
                        user_id = user_info.get("user_id")
                        if user_id and not user_info.get("left", False):
                            active_members.append(str(user_id))
                    elif isinstance(user_info, str):
                        active_members.append(user_info)
                
                total_checked += len(active_members)
                
                for member_id in list(active_members):
                    try:
                        is_member = await check_channel_subscription(
                            context.bot, 
                            int(member_id), 
                            username
                        )
                        
                        # ✅ المستخدم غادر - تطبيق العقوبة
                        if is_member is False:
                            logger.info(f"🚶 {member_id} غادر @{username}")
                            
                            penalty_amount = 5
                            cursor.execute('SELECT points FROM users WHERE user_id = ?', (member_id,))
                            user_points_result = cursor.fetchone()
                            
                            if user_points_result:
                                current_points = user_points_result[0]
                                new_points = current_points - penalty_amount  # ✅ السماح بالسالب
                                
                                # ✅ تحديث نقاط المستخدم (يسمح بالسالب)
                                cursor.execute(
                                    'UPDATE users SET points = ? WHERE user_id = ?',
                                    (new_points, member_id)
                                )
                                
                                # ✅ الحصول على النقاط الحقيقية بعد التحديث
                                points_after_deduction = new_points
                                
                                # ✅ تحديث joined_channels
                                cursor.execute('SELECT joined_channels FROM users WHERE user_id = ?', (member_id,))
                                user_joined_result = cursor.fetchone()
                                
                                if user_joined_result and user_joined_result[0]:
                                    try:
                                        user_joined = json.loads(user_joined_result[0])
                                        if channel_id in user_joined:
                                            user_joined[channel_id]["left"] = True
                                            user_joined[channel_id]["left_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                            user_joined[channel_id]["penalty_applied"] = True
                                            user_joined[channel_id]["points_after_penalty"] = points_after_deduction
                                            
                                            cursor.execute(
                                                'UPDATE users SET joined_channels = ? WHERE user_id = ?',
                                                (json.dumps(user_joined), member_id)
                                            )
                                    except:
                                        pass
                                
                                # ✅ إضافة القناة إلى temp_left_channels
                                cursor.execute('SELECT temp_left_channels FROM users WHERE user_id = ?', (member_id,))
                                temp_left_result = cursor.fetchone()
                                
                                if temp_left_result:
                                    temp_left_str = temp_left_result[0]
                                    temp_left = []
                                    if temp_left_str:
                                        try:
                                            temp_left = json.loads(temp_left_str)
                                        except:
                                            temp_left = []
                                    
                                    # ✅ إضافة القناة إلى temp_left إذا لم تكن موجودة
                                    if channel_id not in temp_left:
                                        temp_left.append(channel_id)
                                        cursor.execute(
                                            'UPDATE users SET temp_left_channels = ? WHERE user_id = ?',
                                            (json.dumps(temp_left), member_id)
                                        )
                                        logger.info(f"✅ تمت إضافة {channel_id} إلى temp_left للمستخدم {member_id}")
                                
                                # ✅ إزالة من active_subscriptions
                                cursor.execute('SELECT active_subscriptions FROM users WHERE user_id = ?', (member_id,))
                                active_subs_result = cursor.fetchone()
                                
                                if active_subs_result:
                                    active_subs_str = active_subs_result[0]
                                    active_subscriptions = []
                                    if active_subs_str:
                                        try:
                                            active_subscriptions = json.loads(active_subs_str)
                                        except:
                                            active_subscriptions = []
                                    
                                    # ✅ إزالة القناة من active_subscriptions
                                    if channel_id in active_subscriptions:
                                        active_subscriptions = [c for c in active_subscriptions if c != channel_id]
                                        cursor.execute(
                                            'UPDATE users SET active_subscriptions = ? WHERE user_id = ?',
                                            (json.dumps(active_subscriptions), member_id)
                                        )
                                        logger.info(f"✅ تمت إزالة {channel_id} من active_subscriptions للمستخدم {member_id}")
                                
                                # ✅ تقليل عداد القناة
                                if current > 0:
                                    new_current = current - 1
                                    cursor.execute(
                                        'UPDATE channels SET current = ? WHERE channel_id = ?',
                                        (new_current, channel_id)
                                    )
                                    current = new_current
                                    logger.info(f"📉 تم تقليل عداد @{username}: {current + 1} → {new_current}")
                                
                                # ✅ تحديث joined_users في القناة
                                updated_joined_users = []
                                for user_info in joined_users:
                                    if isinstance(user_info, dict):
                                        if str(user_info.get("user_id", "")) != str(member_id):
                                            updated_joined_users.append(user_info)
                                        else:
                                            user_info["left"] = True
                                            user_info["left_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                            user_info["points_after_penalty"] = points_after_deduction
                                            updated_joined_users.append(user_info)
                                    elif isinstance(user_info, str):
                                        if user_info != str(member_id):
                                            updated_joined_users.append(user_info)
                                
                                cursor.execute(
                                    'UPDATE channels SET joined_users = ? WHERE channel_id = ?',
                                    (json.dumps(updated_joined_users), channel_id)
                                )
                                
                                # ✅ تسجيل المعاملة
                                cursor.execute(
                                    'INSERT INTO transactions (user_id, channel_id, points, type, timestamp) VALUES (?, ?, ?, ?, ?)',
                                    (member_id, channel_id, -penalty_amount, 'leave_penalty', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                )
                                
                                total_penalties += 1
                                
                                # ✅ تحديث الذاكرة المؤقتة فوراً
                                with _cache_lock:
                                    if member_id in _data_cache:
                                        # تحديث joined_channels في الذاكرة المؤقتة
                                        if "joined_channels" not in _data_cache[member_id]:
                                            _data_cache[member_id]["joined_channels"] = {}
                                        
                                        if channel_id in _data_cache[member_id]["joined_channels"]:
                                            _data_cache[member_id]["joined_channels"][channel_id]["left"] = True
                                            _data_cache[member_id]["joined_channels"][channel_id]["left_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                        
                                        # تحديث temp_left_channels في الذاكرة المؤقتة
                                        if "temp_left_channels" not in _data_cache[member_id]:
                                            _data_cache[member_id]["temp_left_channels"] = []
                                        
                                        if channel_id not in _data_cache[member_id]["temp_left_channels"]:
                                            _data_cache[member_id]["temp_left_channels"].append(channel_id)
                                        
                                        # تحديث active_subscriptions في الذاكرة المؤقتة
                                        if "active_subscriptions" in _data_cache[member_id]:
                                            if channel_id in _data_cache[member_id]["active_subscriptions"]:
                                                _data_cache[member_id]["active_subscriptions"] = [
                                                    c for c in _data_cache[member_id]["active_subscriptions"] 
                                                    if c != channel_id
                                                ]
                                        
                                        # تحديث النقاط في الذاكرة المؤقتة
                                        _data_cache[member_id]["points"] = new_points
                                        
                                        # تحديث وقت التخزين المؤقت
                                        _cache_last_update[member_id] = time.time()
                                
                                # ✅ ✅ ✅ إرسال إشعار للمستخدم مع النقاط الحقيقية بعد الخصم
                                try:
                                    await context.bot.send_message(
                                        int(member_id),
                                        f"⚠️ تم خصم نقاط!\n\n"
                                        f"📢 القناة: @{username}\n"
                                        f"💸 السبب: مغادرة قناة نشطة\n"
                                        f"💰 المبلغ: {penalty_amount} نقاط\n"
                                        f"🎯 نقاطك الآن: {points_after_deduction} {'(سالب)' if points_after_deduction < 0 else ''}\n"
                                        f"📉 العداد الجديد: {current}/{required}\n\n"
                                        f"💡 يمكنك الانضمام مرة أخرى من قسم 'جمع النقاط'",
                                        parse_mode="HTML"
                                    )
                                    logger.info(f"📨 تم إرسال إشعار لـ {member_id}: نقاطه الآن {points_after_deduction}")
                                except Exception as msg_error:
                                    logger.error(f"❌ خطأ في إرسال رسالة للمستخدم {member_id}: {msg_error}")
                                
                                # ✅ إشعار مالك القناة
                                if str(owner) != str(ADMIN_ID):
                                    try:
                                        member_data = get_user_data(member_id)
                                        member_username = member_data.get("username", "بدون")
                                        
                                        await context.bot.send_message(
                                            int(owner),
                                            f"📉 مغادرة من قناتك!\n\n"
                                            f"📢 القناة: @{username}\n"
                                            f"👤 المغادر: @{member_username}\n"
                                            f"💸 خصم: {penalty_amount} نقطة منه\n"
                                            f"💰 نقاطه الآن: {points_after_deduction} {'(سالب)' if points_after_deduction < 0 else ''}\n"
                                            f"📊 العداد الجديد: {current}/{required}\n"
                                            f"⏰ الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                                            parse_mode="HTML"
                                        )
                                    except Exception as owner_error:
                                        logger.error(f"❌ خطأ في إرسال إشعار للمالك: {owner_error}")
                        
                        await asyncio.sleep(0.05)  # ✅ تقليل وقت الانتظار بين المستخدمين
                        
                    except Exception as member_error:
                        logger.error(f"❌ خطأ في معالجة المستخدم {member_id}: {member_error}")
                        continue
                
                await asyncio.sleep(0.1)  # ✅ تقليل وقت الانتظار بين القنوات
                
            except Exception as channel_error:
                logger.error(f"❌ خطأ في معالجة القناة {channel_id}: {channel_error}")
                continue
        
        conn.commit()
        conn.close()
        
        logger.info(f"📊 تمت مراقبة {len(active_channels)} قناة")
        logger.info(f"👥 تم فحص {total_checked} عضو")
        logger.info(f"💸 تم تطبيق {total_penalties} عقوبة")
        
        # ✅ إرسال تقرير للمالك إذا كان هناك عقوبات
        if total_penalties > 0:
            try:
                await context.bot.send_message(
                    ADMIN_ID,
                    f"📊 تقرير المراقبة السريعة\n\n"
                    f"⏰ الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"📢 القنوات: {len(active_channels)}\n"
                    f"👥 الأعضاء: {total_checked}\n"
                    f"💸 العقوبات: {total_penalties}\n"
                    f"💰 إجمالي النقاط المخصومة: {total_penalties * 5}\n"
                    f"⚡ تم تحديث temp_left للمستخدمين",
                    parse_mode="HTML"
                )
            except Exception as report_error:
                logger.error(f"❌ خطأ في إرسال التقرير: {report_error}")
        
    except Exception as e:
        logger.error(f"❌ خطأ كبير في المراقبة: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if "monitor_running" in _active_locks:
            del _active_locks["monitor_running"]

def fix_channel_counter(channel_id):
    """تصحيح عداد القناة يدوياً"""
    import sqlite3
    from database import logger, DB_NAME
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # جلب بيانات القناة
        cursor.execute('SELECT username, current, required FROM channels WHERE channel_id = ?', (channel_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return False
        
        username, current, required = result
        
        # حساب العدد الحقيقي من joined_users
        cursor.execute('SELECT joined_users FROM channels WHERE channel_id = ?', (channel_id,))
        joined_result = cursor.fetchone()
        
        actual_count = 0
        if joined_result and joined_result[0]:
            try:
                import json
                joined_users = json.loads(joined_result[0])
                # حساب المستخدمين الذين لم يغادروا
                for user in joined_users:
                    if isinstance(user, dict):
                        if not user.get("left", False):
                            actual_count += 1
                    elif isinstance(user, str):
                        actual_count += 1
            except:
                pass
        
        # إذا كان العداد مختلفاً عن العدد الفعلي
        if current != actual_count:
            logger.info(f"🔄 تصحيح عداد @{username}: {current} → {actual_count}")
            
            cursor.execute('UPDATE channels SET current = ? WHERE channel_id = ?', (actual_count, channel_id))
            conn.commit()
            conn.close()
            
            logger.info(f"✅ تم تصحيح العداد لـ @{username}")
            return True
        
        conn.close()
        return False
        
    except Exception as e:
        logger.error(f"❌ خطأ في fix_channel_counter: {e}")
        return False

def get_channel_data(channel_id):
    """جلب بيانات القناة من SQLite"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT channel_id, username, owner, required, current, completed, 
                   reuse_count, joined_users, left_users, last_activity,
                   created_at, completed_at, reactivated_at, last_reuse, is_active
            FROM channels WHERE channel_id = ?
        ''', (channel_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
        
        import json
        
        channel_data = {
            "channel_id": result[0],
            "username": result[1],
            "owner": result[2],
            "required": result[3] or 0,
            "current": result[4] or 0,  # ❗ هذا هو العداد المهم
            "completed": bool(result[5]),
            "reuse_count": result[6] or 0,
            "joined_users": [],
            "left_users": [],
            "last_activity": result[9],
            "created_at": result[10],
            "completed_at": result[11],
            "reactivated_at": result[12],
            "last_reuse": result[13],
            "is_active": bool(result[14])
        }
        
        # تحويل joined_users من JSON
        if result[7]:
            try:
                channel_data["joined_users"] = json.loads(result[7])
            except:
                channel_data["joined_users"] = []
        
        # تحويل left_users من JSON
        if result[8]:
            try:
                channel_data["left_users"] = json.loads(result[8])
            except:
                channel_data["left_users"] = []
        
        return channel_data
        
    except Exception as e:
        logger.error(f"❌ خطأ في get_channel_data: {e}")
        return None



def add_admin(user_id):
    """إضافة أدمن"""
    data = load_data()
    user_id = str(user_id)
    
    if user_id not in data.get("admins", []):
        data["admins"].append(user_id)
        save_data(data)
        return True
    return False

def remove_admin(user_id):
    """إزالة أدمن"""
    data = load_data()
    user_id = str(user_id)
    
    if user_id in data.get("admins", []):
        data["admins"].remove(user_id)
        save_data(data)
        return True
    return False

def get_admins():
    """جلب قائمة الأدمنز"""
    data = load_data()
    return data.get("admins", [])

def create_bar_chart(percentage, width=20):
    """إنشاء رسم بياني نصي"""
    filled = int(percentage * width / 100)
    empty = width - filled
    
    bar = "█" * filled + "░" * empty
    return bar

async def show_active_codes(query):
    """عرض الأكواد النشطة (غير المكتملة)"""
    data = load_data()
    codes = data.get("codes", {})
    
    if not codes:
        await query.edit_message_text("📭 لا توجد أكواد نشطة", parse_mode="HTML")
        return
    
    text = "🎟️ الأكواد النشطة:\n\n"
    
    for code_name, code_data in codes.items():
        used_count = code_data.get("used_count", 0)
        max_uses = code_data.get("max_uses", 0)
        
        if used_count >= max_uses:
            continue  # تخطي الأكواد المكتملة
        
        remaining = max_uses - used_count
        points = code_data.get("points", 0)
        created_at = code_data.get("created_at", "")
        
        text += f"🔹 {code_name}\n"
        text += f"   💰 النقاط: {points}\n"
        text += f"   👥 متبقي: {remaining}/{max_uses}\n"
        text += f"   📅 الإنشاء: {created_at}\n\n"
    
    if text == "🎟️ الأكواد النشطة:\n\n":
        text = "📭 لا توجد أكواد نشطة"
    
    keyboard = [
        [InlineKeyboardButton("🔙 رجوع للوحة", callback_data="admin_panel")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")



def auto_complete_channels(context=None):  # ✅ أضيف context=None
    """
    التحقق من القنوات التي وصلت للعدد المطلوب وتكملها تلقائياً
    """
    import sqlite3
    from database import logger, DB_NAME
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # جلب القنوات التي وصلت للعدد المطلوب ولكنها غير مكتملة
        cursor.execute('''
            SELECT channel_id, username, owner, current, required
            FROM channels 
            WHERE completed = 0 AND current >= required
        ''')
        
        channels_to_complete = cursor.fetchall()
        completed_count = 0
        
        for channel_id, username, owner, current, required in channels_to_complete:
            logger.info(f"✅ إكمال القناة @{username}: {current}/{required}")
            
            # تحديث حالة القناة إلى مكتملة
            cursor.execute('''
                UPDATE channels SET 
                    completed = 1,
                    completed_at = ?,
                    is_active = 0
                WHERE channel_id = ?
            ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), channel_id))
            
            completed_count += 1
            
            # إشعار مالك القناة
            if owner and str(owner) != str(ADMIN_ID):
                try:
                    # سأحتاج context.bot هنا، لذلك سأكتب رسالة في السجل فقط
                    logger.info(f"🎉 القناة @{username} اكتملت! يجب إشعار المالك {owner}")
                except:
                    pass
        
        conn.commit()
        conn.close()
        
        if completed_count > 0:
            logger.info(f"🎯 تم إكمال {completed_count} قناة تلقائياً")
        
        return completed_count
        
    except Exception as e:
        logger.error(f"❌ خطأ في auto_complete_channels: {e}")
        return 0
        


async def show_active_channels(query):
    """عرض القنوات النشطة (غير المكتملة)"""
    data = load_data()
    channels = data.get("channels", {})
    
    active_channels = []
    for channel_id, channel_data in channels.items():
        if not channel_data.get("completed", False):
            active_channels.append((channel_id, channel_data))
    
    if not active_channels:
        text = "📭 لا توجد قنوات نشطة حالياً"
        keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    text = f"📢 القنوات النشطة ({len(active_channels)}):\n\n"
    
    for channel_id, channel_data in active_channels:
        username = channel_data.get("username", "بدون")
        current = channel_data.get("current", 0)
        required = channel_data.get("required", 0)
        owner_id = channel_data.get("owner", "")
        created_at = channel_data.get("created_at", "")
        
        # حساب النسبة المئوية
        percentage = (current / required * 100) if required > 0 else 0
        
        # جلب معلومات المالك
        owner_data = get_user_data(owner_id)
        owner_username = owner_data.get("username", owner_id)
        
        text += f"📢 @{username}\n"
        text += f"   👤 المالك: @{owner_username}\n"
        text += f"   📊 التقدم: {current}/{required} ({percentage:.1f}%)\n"
        text += f"   🆔 المعرف: {channel_id[:8]}...\n"
        text += f"   📅 الإنشاء: {created_at}\n\n"
    
    keyboard = [
        [InlineKeyboardButton("🔄 تحديث", callback_data="admin_active_channels")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def show_stats_graph(query):
    """عرض رسم بياني للإحصائيات"""
    stats = get_user_statistics()
    
    if not stats:
        await query.answer("❌ خطأ في جلب الإحصائيات", show_alert=True)
        return
    
    # إنشاء رسم بياني نصي بسيط
    text = "📈 رسم بياني للإحصائيات:\n\n"
    
    # نسبة المستخدمين النشطين
    active_percentage = (stats['active_users'] / stats['total_users'] * 100) if stats['total_users'] > 0 else 0
    text += f"👥 المستخدمين النشطين:\n"
    text += f"   {create_bar_chart(active_percentage)} {active_percentage:.1f}%\n\n"
    
    # نسبة القنوات المكتملة
    data = load_data()
    channels = data.get("channels", {})
    completed_channels = sum(1 for ch in channels.values() if ch.get("completed"))
    total_channels = len(channels)
    
    if total_channels > 0:
        completed_percentage = (completed_channels / total_channels * 100)
        text += f"📢 القنوات المكتملة:\n"
        text += f"   {create_bar_chart(completed_percentage)} {completed_percentage:.1f}%\n\n"
    
    # نسبة النقاط المستخدمة
    total_points = stats['total_points']
    total_earned = sum(get_user_data(uid).get('total_earned', 0) for uid in load_users().keys())
    
    if total_earned > 0:
        used_percentage = (stats['total_points'] / total_earned * 100)
        text += f"💰 النقاط المستخدمة:\n"
        text += f"   {create_bar_chart(used_percentage)} {used_percentage:.1f}%\n\n"
    
    keyboard = [
        [InlineKeyboardButton("📊 إحصائيات مفصلة", callback_data="admin_stats_detailed")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


   
def can_add_channel(channel_username):
    """
    تمنع إضافة القناة إذا كانت موجودة ونشطة (غير مكتملة)
    ترجع:
    (True, None)  → مسموح
    (False, رسالة) → مرفوض
    """
    data = load_data()
    channels = data.get("channels", {})

    # توحيد صيغة اليوزر
    clean_username = channel_username.replace("@", "").strip().lower()

    for channel_id, channel_data in channels.items():
        existing_username = channel_data.get("username", "").lower()

        # نفس القناة + غير مكتملة = ممنوع
        if existing_username == clean_username and not channel_data.get("completed", False):
            return False, (
                "❌ لا يمكن إضافة هذه القناة حالياً\n\n"
                "📢 القناة نشطة الآن\n"
                "⏳ انتظر حتى تكتمل ثم أضفها من جديد"
            )

    return True, None

async def cleanup_penalties_task(context: ContextTypes.DEFAULT_TYPE = None):
    """تنظيف سجل الخصومات القديمة"""
    try:
        current_time = time.time()
        expired_keys = []
        
        for key, penalty_time in list(_recent_penalties.items()):
            if current_time - penalty_time > 3600:  # ساعة واحدة
                expired_keys.append(key)
        
        for key in expired_keys:
            del _recent_penalties[key]
        
        if expired_keys:
            logger.debug(f"🧹 تم تنظيف {len(expired_keys)} سجل خصم قديم")
            
    except Exception as e:
        logger.error(f"❌ خطأ في تنظيف سجل الخصومات: {e}")


        
def can_penalize(join_info, seconds=30):
    last_left = join_info.get("left_at")
    if not last_left:
        return True

    last_time = datetime.strptime(last_left, "%Y-%m-%d %H:%M:%S")
    return (datetime.now() - last_time).total_seconds() > seconds

async def admin_add_channel_handler(update, context):
    """
    ✅ إضافة قناة من قبل الأدمن مع منع التكرار للقنوات النشطة
    ✅ الآن: غير محدودة المرات (لا توقف عند 3 جولات)
    الصيغة: @channel عدد_الأعضاء
    """
    text = update.message.text.strip()
    parts = text.split()

    if len(parts) != 2:
        await update.message.reply_text(
            "❌ الصيغة غير صحيحة\n\n"
            "استخدم:\n"
            "@channel عدد_الأعضاء\n\n"
            "مثال:\n"
            "@TUX3T 50"
        )
        return

    channel_username = parts[0].replace("@", "").strip()
    try:
        required_members = int(parts[1])
    except ValueError:
        await update.message.reply_text("❌ عدد الأعضاء يجب أن يكون رقماً صحيحاً")
        return

    if required_members < 1:
        await update.message.reply_text("❌ عدد الأعضاء يجب أن يكون أكبر من 0")
        return

    data = load_data()
    channels = data.get("channels", {})

    clean_username = channel_username.lower()

    # ================== 🔒 التحقق من التكرار ==================
    existing_channel = None
    existing_channel_id = None
    
    for ch_id, ch_data in channels.items():
        existing_username = ch_data.get("username", "").replace("@", "").lower()

        if existing_username == clean_username:
            existing_channel = ch_data
            existing_channel_id = ch_id
            break
    
    if existing_channel:
        # ✅ إذا القناة موجودة ونشطة (غير مكتملة)
        if not existing_channel.get("completed", False):
            current = existing_channel.get("current", 0)
            required = existing_channel.get("required", 0)
            percentage = (current / required * 100) if required > 0 else 0
            owner_id = existing_channel.get("owner")
            owner_data = get_user_data(owner_id) if owner_id else {}
            owner_username = owner_data.get("username", owner_id if owner_id else "غير معروف")
            
            await update.message.reply_text(
                f"⚠️ **لا يمكن إضافة هذه القناة!**\n\n"
                f"📢 **القناة:** @{channel_username}\n"
                f"👤 **المالك:** @{owner_username}\n"
                f"📊 **التقدم:** {current}/{required} ({percentage:.1f}%)\n"
                f"🔄 **الحالة:** 🟡 جاري التجميع\n"
                f"⏳ **أضيفت في:** {existing_channel.get('created_at', '')}\n"
                f"🆔 **المعرف:** {existing_channel_id}\n\n"
                f"❌ **السبب:**\n"
                f"• القناة نشطة حالياً\n"
                f"• لا يمكن إضافة قناة نشطة مرة أخرى\n\n"
                f"💡 **الحلول:**\n"
                f"1️⃣ انتظر حتى تكتمل القناة ({required}/{required})\n"
                f"2️⃣ عندما تكتمل، يمكن إعادة استخدامها\n"
                f"3️⃣ أو اختر قناة أخرى بيوزر مختلف",
                parse_mode="Markdown"
            )
            return
        
        # ✅ إذا القناة مكتملة، يمكن إعادة استخدامها بدون حدود
        else:
            reuse_count = existing_channel.get("reuse_count", 0)
            
            # ✅ ✅ ✅ التعديل: إزالة الحد الأقصى تماماً
            # ✅ ✅ ✅ أصبح: دائماً يسمح بإعادة الاستخدام بدون حدود
            
            # إعادة تفعيل القناة بدون حدود
            existing_channel["reuse_count"] = reuse_count + 1
            existing_channel["completed"] = False
            existing_channel["current"] = 0
            existing_channel["required"] = required_members  # تحديث العدد المطلوب
            existing_channel["last_reuse"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            existing_channel["joined_users"] = []
            existing_channel["is_active"] = True
            
            save_channel_data(existing_channel_id, existing_channel)
            
            await update.message.reply_text(
                f"♻️ **تم إعادة استخدام القناة!**\n\n"
                f"📢 @{channel_username}\n"
                f"🔢 **إعادة الاستخدام رقم:** {existing_channel['reuse_count']} ♾️\n"  # ✅ تغيير
                f"👥 **العدد المطلوب:** {required_members} عضو\n"
                f"🔄 **الحالة:** نشطة من جديد\n"
                f"🆔 **المعرف:** {existing_channel_id}\n\n"
                f"✅ تم بدء التجميع مرة أخرى!\n"
                f"♾️ **غير محدود:** يمكن إعادة الاستخدام إلى ما لا نهاية",  # ✅ إضافة
                parse_mode="Markdown"
            )
            
            # تنظيف حالة الأدمن
            context.user_data.pop("admin_action", None)
            return
    
    # ====================================================
    # ✅ القناة جديدة تماماً - متابعة الإضافة

    # إنشاء ID فريد للقناة
    channel_id = f"admin_ch_{int(time.time() * 1000)}"

    # حفظ القناة
    data.setdefault("channels", {})[channel_id] = {
        "username": channel_username,
        "required": required_members,
        "current": 0,
        "owner": str(update.message.from_user.id),
        "completed": False,
        "reuse_count": 0,
        "joined_users": [],
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "admin_added": True,
        "is_active": True
    }

    save_data(data)

    await update.message.reply_text(
        f"✅ **تم إضافة القناة بنجاح!**\n\n"
        f"📢 @{channel_username}\n"
        f"🎯 **المطلوب:** {required_members} عضو\n"
        f"🔄 **الحالة:** 🟢 نشطة\n"
        f"⏰ **الوقت:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🆔 **المعرف:** `{channel_id}`\n"
        f"♾️ **غير محدود:** يمكن إعادة الاستخدام إلى ما لا نهاية\n\n"  # ✅ إضافة
        f"📊 يمكن للأعضاء الانضمام من قسم **'جمع النقاط'**",
        parse_mode="Markdown"
    )

    # تنظيف حالة الأدمن
    context.user_data.pop("admin_action", None)



async def handle_channel_purchase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة شراء قناة"""
    user = update.message.from_user
    user_id = str(user.id)
    
    # إذا كان أدمن وفي وضع admin_action، لا نتعامل معه هنا
    if is_admin(user.id) and "admin_action" in context.user_data:
        # الأدمن في وضع إداري (حذف، إضافة، إلخ)
        # لا نتعامل مع رسالته هنا، بل تذهب إلى handle_admin_message
        return

    if "buying" not in context.user_data:
        return

    text = update.message.text.strip()
    buying = context.user_data["buying"]

    if not text.startswith("@") or len(text) < 4:
        await update.message.reply_text(
            "❌ أرسل يوزر القناة بشكل صحيح\nمثال: @channel_username"
        )
        return

    channel_username = text.replace("@", "").strip()
    transaction_id = buying.get("transaction_id", f"purchase_{user_id}_{int(time.time() * 1000)}")
    
    # قفل للشراء
    lock_key = f"purchase_{user_id}_{channel_username}"
    _store_locks.setdefault(lock_key, threading.Lock())
    
    with _store_locks[lock_key]:
        # التحقق من إشراف البوت
        try:
            bot_member = await context.bot.get_chat_member(
                chat_id=f"@{channel_username}",
                user_id=context.bot.id
            )

            if bot_member.status not in ("administrator", "creator"):
                await update.message.reply_text(
                    f"❌ البوت ليس مشرفاً في القناة!\n\n"
                    f"📢 @{channel_username}\n\n"
                    f"➕ يجب عليك أولاً:\n"
                    f"1. أضف البوت كمشرف في القناة\n"
                    f"2. أعطه كل الصلاحيات\n"
                    f"3. أعد إرسال يوزر القناة",
                    parse_mode="HTML"
                )
                return

        except Exception as e:
            error_msg = str(e).lower()
            if "forbidden" in error_msg or "kicked" in error_msg:
                await update.message.reply_text(
                    f"❌ البوت ليس مشرفاً في القناة!\n\n"
                    f"📢 @{channel_username}\n\n"
                    f"➕ أضف البوت كمشرف في القناة أولاً",
                    parse_mode="HTML"
                )
            else:
                await update.message.reply_text(
                    f"❌ حدث خطأ!\n\n"
                    f"تأكد من:\n"
                    f"• القناة عامة\n"
                    f"• اليوزر صحيح\n"
                    f"• البوت مضاف كمشرف",
                    parse_mode="HTML"
                )
            return

        user_data = get_user_data(user_id, force_reload=True)

        if user_data["points"] < buying["points"]:
            await update.message.reply_text(
                f"❌ نقاطك غير كافية!\n"
                f"تحتاج {buying['points']} نقطة"
            )
            return

        data = load_data()
        
        # منع صاحب القناة النشطة من شراء أعضاء لها
        active_user_channels = []
        
        for cid, chan_data in data.get("channels", {}).items():
            if (chan_data.get("username") == channel_username and 
                chan_data.get("owner") == user_id and 
                not chan_data.get("completed", False)):  # قناة نشطة غير مكتملة
                active_user_channels.append(cid)
        
        if active_user_channels:
            # الحصول على معلومات القنوات النشطة
            active_channels_info = []
            for cid in active_user_channels:
                chan_data = data["channels"][cid]
                progress = f"{chan_data.get('current', 0)}/{chan_data.get('required', 0)}"
                created_at = chan_data.get('created_at', 'غير معروف')
                active_channels_info.append(f"• {progress} - {created_at}")
            
            await update.message.reply_text(
                f"❌ لا يمكنك شراء أعضاء لهذه القناة!\n\n"
                f"📢 القناة: @{channel_username}\n"
                f"📊 لديك {len(active_user_channels)} قناة نشطة لهذا اليوزر:\n"
                f"{chr(10).join(active_channels_info)}\n\n"
                f"💡 يجب عليك:\n"
                f"1. الانتظار حتى تكتمل القنوات الحالية\n"
                f"2. أو إعادة تفعيل قناة مكتملة (إذا كانت هناك قناة مكتملة)",
                parse_mode="HTML"
            )
            return

        channels = data.get("channels", {})
        
        # البحث عن قناة مكتملة من نفس المستخدم لنفس القناة
        existing_completed_channel = None
        for channel_id, channel_data in channels.items():
            if (channel_data.get("username") == channel_username and 
                channel_data.get("owner") == user_id and 
                channel_data.get("completed", False)):
                existing_completed_channel = (channel_id, channel_data)
                break
        
        if existing_completed_channel:
            # إعادة استخدام القناة المكتملة
            channel_id, channel_data = existing_completed_channel
            
            # خصم النقاط
            success, message = safe_add_points(
                user_id, 
                buying["points"], 
                "subtract", 
                "channel_reuse_purchase",
                transaction_id
            )
            
            if not success:
                await update.message.reply_text(f"❌ {message}")
                return
            
            # تنظيف بيانات جميع المستخدمين السابقين
            users_data = load_users()
            cleaned_users = 0
            
            for uid, user_info in users_data.items():
                try:
                    cleaned = False
                    
                    if "left_completed_channels" in user_info and channel_id in user_info["left_completed_channels"]:
                        user_info["left_completed_channels"].remove(channel_id)
                        cleaned = True
                    
                    if "permanent_left_channels" in user_info and channel_id in user_info["permanent_left_channels"]:
                        user_info["permanent_left_channels"].remove(channel_id)
                        cleaned = True
                    
                    if "temp_left_channels" in user_info and channel_id in user_info["temp_left_channels"]:
                        user_info["temp_left_channels"].remove(channel_id)
                        cleaned = True
                    
                    if "left_channels" in user_info and channel_id in user_info["left_channels"]:
                        user_info["left_channels"].remove(channel_id)
                        cleaned = True
                    
                    if "joined_channels" in user_info and channel_id in user_info["joined_channels"]:
                        del user_info["joined_channels"][channel_id]
                        cleaned = True
                    
                    if "active_subscriptions" in user_info and channel_id in user_info["active_subscriptions"]:
                        user_info["active_subscriptions"] = [c for c in user_info["active_subscriptions"] if c != channel_id]
                        cleaned = True
                    
                    if cleaned:
                        cleaned_users += 1
                        users_data[uid] = user_info
                        
                except Exception as e:
                    logger.error(f"خطأ في تنظيف بيانات المستخدم {uid}: {e}")
            
            if cleaned_users > 0:
                save_users(users_data, backup=False)
                logger.info(f"🧹 تم تنظيف بيانات {cleaned_users} مستخدم للقناة {channel_username}")
            
            # تحديث بيانات القناة
            channel_data.update({
                "required": buying["members"],
                "current": 0,
                "completed": False,
                "reuse_count": channel_data.get("reuse_count", 0) + 1,  # ✅ يزيد بدون حدود
                "joined_users": [],
                "reactivated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_activity": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "previous_completion": channel_data.get("completed_at"),
                "reactivated_by": user_id,
                "admin_added": channel_data.get("admin_added", False)
            })
            
            order_id = channel_id
            
            # حفظ الطلب
            user_data.setdefault("orders", []).append({
                "order_id": order_id,
                "channel": channel_username,
                "members": buying["members"],
                "points": buying["points"],
                "status": "إعادة تفعيل",
                "current": 0,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "reuse_number": channel_data.get("reuse_count", 1),
                "transaction_id": transaction_id,
                "reactivated_from_completed": True
            })
            
            update_user_data(user_id, {"orders": user_data["orders"]}, "channel_reuse_purchase", transaction_id)
            
            data["channels"][channel_id] = channel_data
            save_data(data)
            
            update_system_stats("total_purchases", increment=1)
            
            await update.message.reply_text(
                f"🔄 تم إعادة تفعيل القناة المكتملة!\n\n"
                f"📢 القناة: @{channel_username}\n"
                f"👥 العدد المطلوب: {buying['members']}\n"
                f"💰 المدفوع: {buying['points']} نقطة\n"
                f"⭐ رصيدك الآن: {user_data['points'] - buying['points']}\n"
                f"🆔 رقم الطلب: {order_id}\n"
                f"🔄 عدد المرات المستخدمة: {channel_data.get('reuse_count', 1)} (لا نهائي)\n"  # ✅ التعديل
                f"🧹 تم تنظيف بيانات {cleaned_users} مستخدم سابق\n\n"
                f"🚀 بدأ التجميع مرة أخرى للجميع!",
                parse_mode="HTML"
            )
            
        else:
            # شراء جديد
            # خصم النقاط
            success, message = safe_add_points(
                user_id, 
                buying["points"], 
                "subtract", 
                "channel_purchase",
                transaction_id
            )
            
            if not success:
                await update.message.reply_text(f"❌ {message}")
                return

            order_id = f"order_{user_id}_{int(time.time())}"

            # حفظ الطلب
            user_data.setdefault("orders", []).append({
                "order_id": order_id,
                "channel": channel_username,
                "members": buying["members"],
                "points": buying["points"],
                "status": "قيد التنفيذ",
                "current": 0,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "transaction_id": transaction_id
            })

            update_user_data(user_id, {"orders": user_data["orders"]}, "channel_purchase", transaction_id)

            # حفظ القناة
            data["channels"][order_id] = {
                "username": channel_username,
                "owner": user_id,
                "required": buying["members"],
                "current": 0,
                "completed": False,
                "reuse_count": 0,
                "joined_users": [],
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "bot_is_admin": True,
                "last_admin_check": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "transaction_id": transaction_id
            }

            save_data(data)
            
            update_system_stats("total_purchases", increment=1)

            await update.message.reply_text(
                f"✅ تم إنشاء الطلب بنجاح!\n\n"
                f"📢 القناة: @{channel_username}\n"
                f"👥 العدد المطلوب: {buying['members']}\n"
                f"💰 المدفوع: {buying['points']} نقطة\n"
                f"⭐ رصيدك الآن: {user_data['points'] - buying['points']}\n"
                f"🆔 رقم الطلب: {order_id}\n"
                f"♾️ يمكنك إعادة الشراء بعد اكتمال هذه الدفعة\n\n"  # ✅ التعديل
                f"🚀 بدأ التجميع!",
                parse_mode="HTML"
            )

        if "transaction_id" in buying:
            cooldown_manager.mark_transaction_complete(buying["transaction_id"])
    
    context.user_data.pop("buying", None)



# ===================== المهام المجدولة =====================


# ===================== مهام إرسال نسخ قاعدة البيانات =====================

async def send_database_backup_to_user(context: ContextTypes.DEFAULT_TYPE):
    """
    إرسال نسخة من قاعدة البيانات إلى المستخدم المحدد كل 30 ثانية
    يمكنك تعديل الوقت من الملف لاحقاً
    """
    try:
        # المستخدم الذي سيتم إرسال النسخة له (ضع آيدي المستخدم هنا)
        TARGET_USER_ID = 8401168362  # ضع آيدي المستخدم هنا
        
        # التحقق من وجود ملف قاعدة البيانات
        if not os.path.exists(DB_NAME):
            logger.error(f"❌ ملف قاعدة البيانات غير موجود: {DB_NAME}")
            return
        
        # إنشاء نسخة مؤقتة
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_filename = f"database_backup_{timestamp}.db"
        
        try:
            # نسخ الملف
            shutil.copy2(DB_NAME, temp_filename)
            
            # إرسال الملف للمستخدم
            with open(temp_filename, 'rb') as db_file:
                await context.bot.send_document(
                    chat_id=TARGET_USER_ID,
                    document=db_file,
                    filename=f"database_backup_{timestamp}.db",
                    caption=f"📦 نسخة احتياطية من قاعدة البيانات\n⏰ الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n🔄 تم إرسالها تلقائياً كل 30 ثانية"
                )
            
            logger.info(f"✅ تم إرسال نسخة قاعدة البيانات إلى المستخدم {TARGET_USER_ID}")
            
            # حذف الملف المؤقت
            try:
                os.remove(temp_filename)
            except:
                pass
                
        except Exception as send_error:
            logger.error(f"❌ خطأ في إرسال قاعدة البيانات: {send_error}")
            
    except Exception as e:
        logger.error(f"❌ خطأ في send_database_backup_to_user: {e}")
        import traceback
        traceback.print_exc()

async def send_database_info_to_user(context: ContextTypes.DEFAULT_TYPE):
    """
    إرسال معلومات عن قاعدة البيانات (بدل الملف الكامل) كل 30 ثانية
    أخف وزناً وسريع
    """
    try:
        TARGET_USER_ID = 8401168362  # ضع آيدي المستخدم هنا
        
        # جلب معلومات الملف
        if os.path.exists(DB_NAME):
            file_size = os.path.getsize(DB_NAME)
            file_size_mb = file_size / (1024 * 1024)
            modified_time = datetime.fromtimestamp(os.path.getmtime(DB_NAME)).strftime('%Y-%m-%d %H:%M:%S')
            
            # جلب إحصائيات من قاعدة البيانات
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            
            # عدد المستخدمين
            cursor.execute('SELECT COUNT(*) FROM users')
            user_count = cursor.fetchone()[0]
            
            # عدد القنوات
            cursor.execute('SELECT COUNT(*) FROM channels')
            channel_count = cursor.fetchone()[0]
            
            # عدد المعاملات
            cursor.execute('SELECT COUNT(*) FROM transactions')
            transaction_count = cursor.fetchone()[0]
            
            conn.close()
            
            # إعداد الرسالة
            message = (
                f"📊 **آخر تحديث لقاعدة البيانات**\n\n"
                f"⏰ **الوقت:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📁 **اسم الملف:** {DB_NAME}\n"
                f"📦 **الحجم:** {file_size_mb:.2f} ميجابايت\n"
                f"🕒 **آخر تعديل:** {modified_time}\n\n"
                f"📈 **الإحصائيات:**\n"
                f"• 👥 المستخدمين: {user_count}\n"
                f"• 📢 القنوات: {channel_count}\n"
                f"• 💰 المعاملات: {transaction_count}\n\n"
                f"🔄 **يتم الإرسال كل 30 ثانية**"
            )
            
            await context.bot.send_message(
                chat_id=TARGET_USER_ID,
                text=message,
                parse_mode="Markdown"
            )
            
            logger.info(f"📨 تم إرسال معلومات قاعدة البيانات إلى المستخدم {TARGET_USER_ID}")
            
        else:
            await context.bot.send_message(
                chat_id=TARGET_USER_ID,
                text="❌ ملف قاعدة البيانات غير موجود!",
                parse_mode="Markdown"
            )
            
    except Exception as e:
        logger.error(f"❌ خطأ في send_database_info_to_user: {e}")

async def send_database_status(context: ContextTypes.DEFAULT_TYPE):
    """
    إرسال تقرير حالة قاعدة البيانات كل 30 ثانية
    """
    try:
        TARGET_USER_ID = 8401168362  # ضع آيدي المستخدم هنا
        
        # جلب معلومات تفصيلية
        if os.path.exists(DB_NAME):
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            
            # التحقق من جميع الجداول
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            # جلب معلومات كل جدول
            table_info = []
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                table_info.append(f"• {table_name}: {row_count} سطر")
            
            conn.close()
            
            # إعداد الرسالة
            message = (
                f"📊 **تقرير حالة قاعدة البيانات**\n\n"
                f"⏰ **الوقت:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📁 **الملف:** `{DB_NAME}`\n"
                f"🔢 **عدد الجداول:** {len(tables)}\n\n"
                f"📋 **محتويات الجداول:**\n" + "\n".join(table_info) + "\n\n"
                f"🔄 **يتم الإرسال كل 30 ثانية**\n"
                f"🆔 **آيدي المستخدم:** {TARGET_USER_ID}"
            )
            
            await context.bot.send_message(
                chat_id=TARGET_USER_ID,
                text=message,
                parse_mode="Markdown"
            )
            
            logger.info(f"📨 تم إرسال تقرير حالة قاعدة البيانات إلى المستخدم {TARGET_USER_ID}")
            
    except Exception as e:
        logger.error(f"❌ خطأ في send_database_status: {e}")


def add_channel_to_temp_left(user_id, channel_id):
    """
    إضافة القناة إلى temp_left_channels عندما يغادر المستخدم
    """
    import json
    import sqlite3
    from database import logger, DB_NAME
    
    user_id = str(user_id)
    channel_id = str(channel_id)
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # جلب temp_left_channels الحالية
        cursor.execute('SELECT temp_left_channels FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if result:
            temp_left_str = result[0]
            temp_left = []
            if temp_left_str:
                try:
                    temp_left = json.loads(temp_left_str)
                except:
                    temp_left = []
            
            # إضافة القناة إذا لم تكن موجودة
            if channel_id not in temp_left:
                temp_left.append(channel_id)
                
                cursor.execute('''
                    UPDATE users SET temp_left_channels = ? WHERE user_id = ?
                ''', (json.dumps(temp_left, ensure_ascii=False), user_id))
                
                conn.commit()
                logger.info(f"✅ تمت إضافة القناة {channel_id} إلى temp_left للمستخدم {user_id}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في add_channel_to_temp_left: {e}")
        if 'conn' in locals():
            conn.close()
        return False


async def periodic_subscription_check(context: ContextTypes.DEFAULT_TYPE):
    """فحص دوري للاشتراكات مع منع التقارب"""
    # منع تشغيل أكثر من فحص في نفس الوقت
    if "subscription_check_running" in _active_locks:
        logger.debug("⏭️ الفحص الدوري يعمل حالياً، تم تخطي هذه الدورة")
        return
    
    _active_locks["subscription_check_running"] = True
    
    try:
        logger.info("🔍 بدء الفحص الدوري للاشتراكات...")
        
        if not context or not context.bot:
            logger.error("❌ context أو bot غير متوفرين")
            return
        
        bot = context.bot
        data = load_data()
        users_data = load_users()
        
        penalty_count = 0
        counter_decreased = 0
        checked_count = 0
        skipped_count = 0
        
        logger.info(f"📊 جاري فحص {len(users_data)} مستخدم...")
        
        for user_id, user_data in users_data.items():
            try:
                active_channels = user_data.get("active_subscriptions", [])
                
                if not active_channels:
                    continue
                
                for channel_id in list(active_channels):  # استخدام نسخة للكشف
                    try:
                        channel = data.get("channels", {}).get(channel_id)
                        if not channel:
                            # قناة غير موجودة - إزالتها من النشطة
                            active_subscriptions = user_data.get("active_subscriptions", [])
                            user_data["active_subscriptions"] = [c for c in active_subscriptions if c != channel_id]
                            update_user_data(user_id, {"active_subscriptions": user_data["active_subscriptions"]}, "remove_nonexistent_channel")
                            skipped_count += 1
                            continue
                        
                        channel_username = channel.get("username", "")
                        if not channel_username:
                            skipped_count += 1
                            continue
                        
                        checked_count += 1
                        
                        # التحقق من الاشتراك
                        try:
                            is_subscribed = await check_channel_subscription(bot, int(user_id), channel_username)
                            
                            if is_subscribed is None:
                                # خطأ في التحقق - تخطي
                                continue
                            
                            # 🔴 المستخدم غادر القناة النشطة
                            if is_subscribed is False and not channel.get("completed", False):
                                logger.info(f"🚨 {user_id} غادر @{channel_username} (قناة نشطة)")
                                
                                # خصم 5 نقاط
                                penalty_amount = 5
                                transaction_id = f"penalty_{user_id}_{channel_id}_{int(time.time() * 1000)}"
                                
                                # خصم النقاط
                                success, message = safe_add_points(
                                    user_id, 
                                    penalty_amount, 
                                    "subtract", 
                                    "periodic_check_penalty",
                                    transaction_id
                                )
                                
                                if success:
                                    penalty_count += 1
                                    logger.info(f"✅ تم خصم {penalty_amount} نقطة من {user_id}")
                                else:
                                    logger.error(f"❌ فشل خصم النقاط: {message}")
                                
                                # تقليل عداد القناة
                                current_counter = channel.get("current", 0)
                                if current_counter > 0:
                                    new_counter = current_counter - 1
                                    channel["current"] = new_counter
                                    data["channels"][channel_id] = channel
                                    save_data(data)
                                    counter_decreased += 1
                                    logger.info(f"📉 تم تقليل عداد @{channel_username}: {current_counter} → {new_counter}")
                                
                                # وضع علامة المغادرة
                                mark_channel_as_left(user_id, channel_id, channel)
                                
                                # إرسال إشعار للمستخدم
                                try:
                                    user_updated_data = get_user_data(user_id)
                                    current_points = user_updated_data.get("points", 0)
                                    
                                    await bot.send_message(
                                        int(user_id),
                                        f"⚠️ تم خصم نقاط!\n\n"
                                        f"📢 القناة: @{channel_username}\n"
                                        f"💸 السبب: مغادرة قناة نشطة\n"
                                        f"💰 المبلغ: 5 نقاط\n"
                                        f"🎯 نقاطك الآن: {current_points}\n\n"
                                        f"💡 يمكنك الانضمام مرة أخرى!",
                                        parse_mode="HTML"
                                    )
                                except Exception as notify_error:
                                    logger.error(f"❌ خطأ في إرسال إشعار للمستخدم {user_id}: {notify_error}")
                                
                        except Exception as check_error:
                            logger.error(f"❌ خطأ في التحقق للمستخدم {user_id}: {check_error}")
                            continue
                            
                    except Exception as channel_error:
                        logger.error(f"❌ خطأ في معالجة القناة {channel_id}: {channel_error}")
                        continue
                
                # تأخير بين المستخدمين
                await asyncio.sleep(0.1)
                
            except Exception as user_error:
                logger.error(f"❌ خطأ في معالجة المستخدم {user_id}: {user_error}")
                continue
        
        # تسجيل الإحصائيات
        logger.info(
            f"📊 نتائج الفحص الدوري:\n"
            f"  ✅ تم فحص: {checked_count} اشتراك\n"
            f"  💸 تم خصم نقاط من: {penalty_count} مستخدم\n"
            f"  📉 تم تقليل العداد لـ: {counter_decreased} قناة\n"
            f"  ⏭️ تم تجاهل: {skipped_count} اشتراك"
        )
        
        # إرسال تقرير للمالك إذا كان هناك عقوبات
        if penalty_count > 0:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"📊 تقرير الفحص الدوري\n\n"
                    f"⏰ الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"👥 المستخدمين المفحوصين: {len(users_data)}\n"
                    f"📢 الاشتراكات المفحوصة: {checked_count}\n"
                    f"💸 عقوبات مطبقة: {penalty_count}\n"
                    f"📉 عدادات مخفضة: {counter_decreased}\n"
                    f"💰 إجمالي النقاط المخصومة: {penalty_count * 5}",
                    parse_mode="HTML"
                )
            except Exception as report_error:
                logger.error(f"❌ خطأ في إرسال التقرير: {report_error}")
        
    except Exception as e:
        logger.error(f"❌ خطأ كبير في الفحص الدوري: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # تحرير القفل
        if "subscription_check_running" in _active_locks:
            del _active_locks["subscription_check_running"]
        logger.info("✅ انتهى الفحص الدوري للاشتراكات")

async def decrease_channel_counter(bot, user_id, channel_id, channel_data=None, penalty_amount=5):
    """
    تقليل عداد القناة عند المغادرة - نسخة مصححة
    """
    from database import get_channel_data, save_channel_data, logger
    
    user_id = str(user_id)
    channel_id = str(channel_id)
    
    try:
        if channel_data is None:
            channel_data = get_channel_data(channel_id)
        
        if not channel_data:
            return False, 0, "القناة غير موجودة", 0  # ✅ إرجاع 4 قيم
        
        current_count = channel_data.get("current", 0)
        new_count = max(0, current_count - 1)
        
        # ✅ الحصول على نقاط المستخدم الحالية بعد الخصم
        user_data = get_user_data(user_id, force_reload=True)
        points_after_penalty = user_data.get("points", 0)  # ✅ النقاط بعد الخصم
        
        channel_data["current"] = new_count
        channel_data["last_activity"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if "leave_history" not in channel_data:
            channel_data["leave_history"] = []
        
        channel_data["leave_history"].append({
            "user_id": user_id,
            "left_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "previous_count": current_count,
            "new_count": new_count,
            "penalty_applied": penalty_amount,
            "user_points_after": points_after_penalty,  # ✅ حفظ النقاط بعد الخصم
            "channel_username": channel_data.get("username", "unknown")
        })
        
        # ✅ تحديث joined_users
        if "joined_users" in channel_data:
            joined_users = channel_data.get("joined_users", [])
            if isinstance(joined_users, str):
                joined_users = json.loads(joined_users) if joined_users else []
            
            # إزالة المستخدم
            joined_users = [
                u for u in joined_users 
                if str(u.get("user_id", "") if isinstance(u, dict) else u) != str(user_id)
            ]
            
            channel_data["joined_users"] = json.dumps(joined_users) if joined_users else "[]"
        
        # ✅ إلغاء الاكتمال إذا لزم
        required = channel_data.get("required", 0)
        if channel_data.get("completed", False) and new_count < required:
            channel_data["completed"] = False
            channel_data["uncompleted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            channel_data["uncompleted_reason"] = f"user_left:{user_id}"
        
        # ✅ حفظ التحديثات
        if save_channel_data(channel_id, channel_data):
            logger.info(
                f"✅ تم تقليل عداد القناة {channel_data.get('username')}: "
                f"{current_count} → {new_count} | نقاط المستخدم: {points_after_penalty}"
            )
            return True, new_count, f"تم تقليل العداد من {current_count} إلى {new_count}", points_after_penalty
        else:
            logger.error(f"❌ فشل حفظ بيانات القناة {channel_id}")
            return False, current_count, "فشل حفظ البيانات", points_after_penalty
            
    except Exception as e:
        logger.error(f"❌ خطأ في decrease_channel_counter للقناة {channel_id}: {e}")
        import traceback
        traceback.print_exc()
        return False, 0, str(e), 0

async def periodic_cleanup(context: ContextTypes.DEFAULT_TYPE):
    """تنظيف دوري"""
    try:
        cleanup_expired_mutes()
        cooldown_manager.clear_old_transactions()
    except Exception as e:
        logger.error(f"خطأ في التنظيف: {e}")

async def send_backup_to_owner(context: ContextTypes.DEFAULT_TYPE):
    """إرسال نسخة احتياطية"""
    global _last_backup_time
    
    current_time = time.time()
    if current_time - _last_backup_time < BACKUP_INTERVAL:
        return
    
    _last_backup_time = current_time
    
    try:
        if backup_database(BACKUP_DIR):
            logger.info("✅ نسخة احتياطية تمت")
    except Exception as e:
        logger.error(f"خطأ في النسخ الاحتياطي: {e}")

def check_and_mark_completed_channels():
    """
    التحقق من القنوات المكتملة ووضع علامة عليها
    (لا يحذفها - فقط يضع علامة completed)
    """
    from database import load_channels, save_channel_data, logger
    
    try:
        channels = load_channels()
        completed_count = 0
        
        for channel_id, channel_data in list(channels.items()):
            current = channel_data.get("current", 0)
            required = channel_data.get("required", 0)
            
            # تحقق من اكتمال القناة
            if current >= required and not channel_data.get("completed", False):
                channel_username = channel_data.get("username", "unknown")
                
                # وضع علامة الاكتمال
                channel_data["completed"] = True
                channel_data["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # حفظ التحديث
                save_channel_data(channel_id, channel_data)
                
                completed_count += 1
                logger.info(f"✅ تم وضع علامة اكتمال على القناة @{channel_username}")
        
        if completed_count > 0:
            logger.info(f"🎯 تم وضع علامة اكتمال على {completed_count} قناة")
        
        return completed_count
        
    except Exception as e:
        logger.error(f"❌ خطأ في check_and_mark_completed_channels: {e}")
        import traceback
        traceback.print_exc()
        return 0

async def show_detailed_stats(query):
    """عرض إحصائيات مفصلة"""
    stats = get_user_statistics()
    
    if not stats:
        await query.answer("❌ خطأ في جلب الإحصائيات", show_alert=True)
        return
    
    data = load_data()
    channels = data.get("channels", {})
    
    completed_channels = sum(1 for ch in channels.values() if ch.get("completed"))
    active_channels = len(channels) - completed_channels
    
    # جلب إحصائيات إضافية
    total_points_in_system = 0
    total_invites_in_system = 0
    users_data = load_users()
    
    for uid, user_data in users_data.items():
        total_points_in_system += user_data.get("points", 0)
        total_invites_in_system += user_data.get("invites", 0)
    
    # إنشاء تقرير مفصل
    text = (
        f"📊 **الإحصائيات المفصلة**\n\n"
        f"👥 **المستخدمين:**\n"
        f"• الإجمالي: {stats['total_users']:,}\n"
        f"• النشطين اليوم: {stats['active_users']:,}\n"
        f"• الجدد اليوم: {stats['new_today']:,}\n"
        f"• الجدد الأسبوع: {stats['new_week']:,}\n"
        f"• الجدد الشهر: {stats['new_month']:,}\n"
        f"• باليوزر: {stats['with_username']:,}\n"
        f"• بالدعوات: {stats['with_invites']:,}\n"
        f"• المحظورين: {stats['banned_users']:,}\n"
        f"• المكتومين: {stats['muted_users']:,}\n\n"
        
        f"💰 **النقاط في النظام:**\n"
        f"• إجمالي النقاط: {stats['total_points']:,}\n"
        f"• إجمالي الدعوات: {stats['total_invites']:,}\n"
        f"• مجموع النقاط (كل المستخدمين): {total_points_in_system:,}\n"
        f"• مجموع الدعوات (كل المستخدمين): {total_invites_in_system:,}\n\n"
        
        f"📢 **القنوات:**\n"
        f"• الإجمالي: {len(channels):,}\n"
        f"• النشطة: {active_channels:,}\n"
        f"• المكتملة: {completed_channels:,}\n\n"
        
        f"📈 **النسب المئوية:**\n"
        f"• النشطين: {(stats['active_users']/max(stats['total_users'],1)*100):.1f}%\n"
        f"• باليوزر: {(stats['with_username']/max(stats['total_users'],1)*100):.1f}%\n"
        f"• القنوات المكتملة: {(completed_channels/max(len(channels),1)*100):.1f}%"
    )
    
    keyboard = [
        [InlineKeyboardButton("📈 رسم بياني", callback_data="admin_stats_graph")],
        [InlineKeyboardButton("📊 إحصائيات بسيطة", callback_data="admin_stats")],
        [InlineKeyboardButton("🔙 رجوع للوحة", callback_data="admin_panel")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            text, 
            reply_markup=reply_markup, 
            parse_mode="Markdown"
        )
    except Exception as e:
        await query.message.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

def check_and_fix_temp_left_channels():
    """
    فحص وتصحيح قنوات temp_left_channels
    """
    try:
        from database import load_users
        import json
        
        users_data = load_users()
        fixed_count = 0
        
        for user_id, user_data in users_data.items():
            joined_channels = user_data.get("joined_channels", {})
            if isinstance(joined_channels, str):
                try:
                    joined_channels = json.loads(joined_channels) if joined_channels else {}
                except:
                    joined_channels = {}
            
            temp_left = user_data.get("temp_left_channels", [])
            if isinstance(temp_left, str):
                try:
                    temp_left = json.loads(temp_left) if temp_left else []
                except:
                    temp_left = []
            
            active_subscriptions = user_data.get("active_subscriptions", [])
            if isinstance(active_subscriptions, str):
                try:
                    active_subscriptions = json.loads(active_subscriptions) if active_subscriptions else []
                except:
                    active_subscriptions = []
            
            # تحقق من القنوات في temp_left
            valid_temp_left = []
            
            for channel_id in temp_left:
                # إذا كان في joined_channels وليس left، أزله من temp_left
                if channel_id in joined_channels:
                    join_info = joined_channels[channel_id]
                    if join_info.get("verified", False) and not join_info.get("left", False):
                        continue  # لا تضف للـ valid_temp_left
                
                # إذا كان في active_subscriptions، أزله من temp_left
                if channel_id in active_subscriptions:
                    continue  # لا تضف للـ valid_temp_left
                
                # أضف القناة للـ valid_temp_left
                valid_temp_left.append(channel_id)
            
            # إذا كان هناك فرق، قم بالتحديث
            if len(valid_temp_left) != len(temp_left):
                updates = {"temp_left_channels": json.dumps(valid_temp_left)}
                update_user_data(user_id, updates, "fix_temp_left")
                fixed_count += abs(len(temp_left) - len(valid_temp_left))
        
        if fixed_count > 0:
            logger.info(f"🔧 تم إصلاح {fixed_count} قناة في temp_left_channels")
        
        return fixed_count
        
    except Exception as e:
        logger.error(f"❌ خطأ في check_and_fix_temp_left_channels: {e}")
        import traceback
        traceback.print_exc()
        return 0

def check_and_fix_columns():
    """التحقق من وجود جميع الأعمدة المطلوبة وإضافتها إذا كانت مفقودة"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # قائمة الأعمدة المطلوبة
        required_columns = [
            ("last_channel_join", "TEXT"),
            ("total_channel_points", "INTEGER DEFAULT 0"),
            ("reports_made", "INTEGER DEFAULT 0"),
            ("reported_channels", "TEXT DEFAULT '[]'"),
            ("joined_channels", "TEXT DEFAULT '{}'"),
            ("active_subscriptions", "TEXT DEFAULT '[]'"),
            ("daily_gift", "TEXT DEFAULT '{}'"),
            ("orders", "TEXT DEFAULT '[]'"),
            ("transactions", "TEXT DEFAULT '[]'")
        ]
        
        # جلب الأعمدة الحالية
        cursor.execute("PRAGMA table_info(users)")
        existing_columns = [col[1] for col in cursor.fetchall()]
        
        # إضافة الأعمدة المفقودة
        added_count = 0
        for column_name, column_type in required_columns:
            if column_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE users ADD COLUMN {column_name} {column_type}")
                    added_count += 1
                    logger.info(f"✅ تمت إضافة العمود: {column_name}")
                except Exception as e:
                    logger.error(f"❌ خطأ في إضافة العمود {column_name}: {e}")
        
        conn.commit()
        conn.close()
        
        if added_count > 0:
            logger.info(f"✅ تم إضافة {added_count} عمود جديد إلى جدول users")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في check_and_fix_columns: {e}")
        return False




async def manage_admins(query):
    """إدارة الأدمن"""
    data = load_data()
    admins = data.get("admins", [])
    
    text = "👥 قائمة الأدمن:\n\n"
    
    for i, admin_id in enumerate(admins, 1):
        admin_data = get_user_data(admin_id)
        username = admin_data.get("username", "بدون")
        text += f"{i}. @{username} ({admin_id})\n"
    
    text += "\n📝 الأوامر:\n"
    text += "➕ إضافة: /addadmin @username\n"
    text += "➖ إزالة: /removeadmin @username\n"
    text += "📋 القائمة: /listadmins"
    
    keyboard = [
        [InlineKeyboardButton("🔙 رجوع للوحة", callback_data="admin_panel")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def handle_admin_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أوامر الأدمن الجديدة"""
    if not is_admin(update.message.from_user.id):
        return
    
    text = update.message.text
    user_id = str(update.message.from_user.id)
    
    if text.startswith("/addadmin"):
        try:
            target_username = text.split()[1].replace("@", "")
            target_uid = find_user_by_username(target_username)
            
            if not target_uid:
                await update.message.reply_text("❌ المستخدم غير موجود!")
                return
            
            if add_admin(target_uid):
                await update.message.reply_text(f"✅ تم إضافة @{target_username} كأدمن")
                
                # إشعار للمستخدم الجديد
                try:
                    await context.bot.send_message(
                        int(target_uid),
                        f"🎉 تمت ترقيتك!\n\n"
                        f"👑 أنت الآن أدمن في البوت\n"
                        f"📊 يمكنك الوصول للوحة الإدارة\n"
                        f"⚙️ الأوامر: /admin",
                        parse_mode="HTML"
                    )
                except:
                    pass
            else:
                await update.message.reply_text("⚠️ المستخدم أدمن مسبقاً!")
                
        except:
            await update.message.reply_text("❌ استخدام: /addadmin @username")
    
    elif text.startswith("/removeadmin"):
        try:
            target_username = text.split()[1].replace("@", "")
            target_uid = find_user_by_username(target_username)
            
            if not target_uid:
                await update.message.reply_text("❌ المستخدم غير موجود!")
                return
            
            if target_uid == str(ADMIN_ID):
                await update.message.reply_text("❌ لا يمكن إزالة المالك!")
                return
            
            if remove_admin(target_uid):
                await update.message.reply_text(f"✅ تم إزالة @{target_username} من الأدمن")
            else:
                await update.message.reply_text("❌ المستخدم ليس أدمن!")
                
        except:
            await update.message.reply_text("❌ استخدام: /removeadmin @username")
    
    elif text.startswith("/listadmins"):
        admins = get_admins()
        
        if not admins:
            await update.message.reply_text("📭 لا توجد أدمنز!")
            return
        
        text_response = "👥 قائمة الأدمن:\n\n"
        
        for i, admin_id in enumerate(admins, 1):
            admin_data = get_user_data(admin_id)
            username = admin_data.get("username", "بدون")
            text_response += f"{i}. @{username} ({admin_id})\n"
        
        await update.message.reply_text(text_response, parse_mode="HTML")



async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة الأخطاء"""
    logger.error(f"❌ خطأ: {context.error}")

# ===================== الدالة الرئيسية =====================

def main():
    """الدالة الرئيسية لتشغيل البوت مع المهام المجدولة السريعة والتحكم الفوري"""
    logger.info("🚀 بدء تشغيل البوت مع نظام التحديث الفوري...")

    try:
        # ================== قاعدة البيانات ==================
        logger.info("📊 إنشاء قاعدة البيانات...")
        init_database()

        logger.info("🔍 التحقق من الأعمدة...")
        from database import add_missing_columns
        add_missing_columns()

        logger.info("🧹 تنظيف الأقفال القديمة...")
        clear_expired_locks()

        logger.info("🔧 فحص وتصحيح temp_left_channels...")
        check_and_fix_temp_left_channels()

        logger.info(f"✅ قاعدة البيانات: {DB_NAME}")

        # ================== إضافة المالك كأدمن ==================
        data = load_data()
        if str(ADMIN_ID) not in data.get("admins", []):
            data["admins"] = data.get("admins", []) + [str(ADMIN_ID)]
            save_data(data)
            logger.info(f"✅ تم إضافة المالك {ADMIN_ID} كأدمن")

        # ================== إنشاء التطبيق ==================
        logger.info("🤖 إنشاء تطبيق البوت...")
        application = Application.builder().token(TOKEN).build()

        # ================== الأوامر ==================
        logger.info("📝 إضافة معالجات الأوامر...")
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("code", handle_code_command))
        application.add_handler(CommandHandler("addadmin", handle_admin_commands))
        application.add_handler(CommandHandler("removeadmin", handle_admin_commands))
        application.add_handler(CommandHandler("listadmins", handle_admin_commands))
        application.add_handler(CommandHandler("my_channels", my_channels_command))
        application.add_handler(CallbackQueryHandler(check_force_sub_callback, pattern="^check_force_sub$"))

        # ================== الأزرار ==================
        logger.info("🔘 إضافة معالج الأزرار...")
        application.add_handler(CallbackQueryHandler(button_handler))

        # ================== مراقبة المغادرة ==================
        logger.info("📊 نظام المراقبة الذكية...")

        # ================== رسائل الأدمن ==================
        logger.info("👑 إضافة معالجات الأدمن...")
        admin_filter = filters.User(user_id=int(ADMIN_ID))
        application.add_handler(
            MessageHandler(filters.TEXT & admin_filter & ~filters.COMMAND, handle_admin_message),
            group=0
        )

        # ================== رسائل الشراء ==================
        logger.info("🛒 إضافة معالج الشراء...")
        application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_channel_purchase),
            group=1
        )

        # ================== أوامر الأدمن العامة ==================
        logger.info("⚙️ إضافة أوامر الأدمن...")
        application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_commands),
            group=2
        )
        

        # ================== معالج الأخطاء ==================
        logger.info("⚠️ إضافة معالج الأخطاء...")
        application.add_error_handler(error_handler)

        # ================== المهام المجدولة السريعة ==================
        logger.info("⏰ إضافة المهام المجدولة السريعة...")
        
        # 1. فحص الاشتراكات الدوري السريع - كل 10 ثواني
        
        
        # 2. التنظيف الدوري - كل 15 دقيقة
        application.job_queue.run_repeating(
            periodic_cleanup, 
            interval=900,   # 15 دقيقة
            first=30,       # بعد 30 ثانية
            name="periodic_cleanup"
        )
        
        # 3. النسخ الاحتياطي - كل 30 دقيقة
        application.job_queue.run_repeating(
            send_backup_to_owner, 
            interval=1800,  # 30 دقيقة
            first=120,      # بعد دقيقتين
            name="backup"
        )
        
        # 4. مراقبة القنوات الذكية - كل 30 ثانية
        application.job_queue.run_repeating(
            monitor_channel_activity, 
            interval=30,    # 30 ثانية
            first=10,       # بعد 10 ثواني
            name="smart_channel_monitor"
        )
        
        # 5. تنظيف الكتم المنتهي - كل دقيقتين
        application.job_queue.run_repeating(
            cleanup_expired_mutes, 
            interval=120,   # دقيقتين
            first=15,       # بعد 15 ثانية
            name="cleanup_mutes"
        )
        
        # 6. إكمال القنوات تلقائياً - كل دقيقة
        application.job_queue.run_repeating(
            auto_complete_channels, 
            interval=60,    # دقيقة
            first=20,       # بعد 20 ثانية
            name="auto_complete"
        )
        
        # 7. تنظيف الأقفال - كل 30 ثانية
        application.job_queue.run_repeating(
            cleanup_locks_task, 
            interval=30,    # 30 ثانية
            first=10,       # بعد 10 ثواني
            name="cleanup_locks"
        )
        
        # 8. تنظيف المعاملات القديمة - كل 5 دقائق
        application.job_queue.run_repeating(
            cooldown_manager.clear_old_transactions, 
            interval=300,   # 5 دقائق
            first=90,       # بعد دقيقة ونصف
            name="cleanup_transactions"
        )
        
        # 9. تنظيف سجل الخصومات - كل دقيقة
        application.job_queue.run_repeating(
            cleanup_penalties_task,
            interval=60,    # دقيقة
            first=30,
            name="cleanup_penalties"
        )
        
        # 10. فحص سلامة البيانات - كل 10 دقائق
        application.job_queue.run_repeating(
            lambda context: repair_corrupted_data(), 
            interval=600,   # 10 دقائق
            first=180,      # بعد 3 دقائق
            name="data_repair"
        )
        
        # 11. فحص وتصحيح temp_left_channels - كل دقيقتين
        application.job_queue.run_repeating(
            lambda context: check_and_fix_temp_left_channels(),
            interval=120,   # دقيقتين
            first=45,       # بعد 45 ثانية
            name="fix_temp_left"
        )
        
        # 12. فحص القنوات المتاحة - كل دقيقة
        application.job_queue.run_repeating(
            lambda context: None,
            interval=60,    # دقيقة
            first=30,       # بعد 30 ثانية
            name="channel_availability_check"
        )
        
        # 13. تحديث الذاكرة المؤقتة - كل 30 ثانية
        application.job_queue.run_repeating(
            lambda context: None,  
            interval=30,    # 30 ثانية
            first=15,       # بعد 15 ثانية
            name="cache_refresh"
        )
        
        # 14. فحص إحصائيات النظام - كل 5 دقائق
        application.job_queue.run_repeating(
            lambda context: update_system_stats("last_health_check", value=int(time.time())),
            interval=300,   # 5 دقائق
            first=60,       # بعد دقيقة
            name="system_health_check"
        )
        
        # 15. فحص وتنظيف بيانات القنوات - كل 10 دقائق
        application.job_queue.run_repeating(
            lambda context: cleanup_channel_data(),
            interval=600,   # 10 دقائق
            first=120,      # بعد دقيقتين
            name="channel_data_cleanup"
        )

        # ================== مهام إرسال قاعدة البيانات ==================
        logger.info("📦 إضافة مهام إرسال قاعدة البيانات...")

        # 16. إرسال معلومات قاعدة البيانات كل 30 ثانية
        application.job_queue.run_repeating(
            send_database_info_to_user, 
            interval=1800,    # 30 ثانية
            first=10,       # بعد 10 ثواني من التشغيل
            name="send_db_info"
        )

        # 17. إرسال تقرير حالة قاعدة البيانات كل 30 ثانية
        application.job_queue.run_repeating(
            send_database_status, 
            interval=1800,    # 30 ثانية
            first=15,       # بعد 15 ثانية
            name="send_db_status"
        )

        # 18. إرسال الملف الكامل كل 5 دقائق (اختياري)
        application.job_queue.run_repeating(
            send_database_backup_to_user, 
            interval=1800,   # 5 دقائق
            first=30,       # بعد 30 ثانية
            name="send_db_backup"
        )
        application.job_queue.run_repeating(
            lambda context: cleanup_member_cache(),
            interval=60,    # دقيقة
            first=20,       # بعد 20 ثانية
            name="cleanup_member_cache"
        )        

        # ================== معلومات التشغيل ==================
        logger.info("=" * 70)
        logger.info("🎉 البوت يعمل الآن مع نظام التحديث الفوري!")
        logger.info(f"🤖 المالك: {ADMIN_ID}")
        logger.info(f"📢 القناة: {BOT_CHANNEL}")
        logger.info(f"💾 قاعدة البيانات: {DB_NAME}")
        logger.info(f"📁 النسخ الاحتياطي: {BACKUP_DIR}")
        logger.info(f"⏰ المهام المجدولة: 18 مهمة نشطة")
        logger.info("=" * 70)
        logger.info("⚡ المهام السريعة:")
        logger.info(f"  • فحص الاشتراكات: 10 ثواني")
        logger.info(f"  • مراقبة القنوات: 30 ثانية")
        logger.info(f"  • إكمال القنوات: دقيقة")
        logger.info(f"  • فحص temp_left: دقيقتين")
        logger.info(f"  • تحديث الذاكرة: 30 ثانية")
        logger.info(f"  • إرسال معلومات DB: 30 ثانية ⭐")
        logger.info("=" * 70)
        logger.info("📦 مهام إرسال قاعدة البيانات:")
        logger.info(f"  • معلومات DB: كل 30 ثانية")
        logger.info(f"  • تقرير حالة DB: كل 30 ثانية")
        logger.info(f"  • ملف DB كامل: كل 5 دقائق")
        logger.info("=" * 70)
        logger.info("🔧 مهام الصيانة:")
        logger.info(f"  • تنظيف الكتم: دقيقتين")
        logger.info(f"  • تنظيف الأقفال: 30 ثانية")
        logger.info(f"  • النسخ الاحتياطي: 30 دقيقة")
        logger.info(f"  • فحص البيانات: 10 دقائق")
        logger.info("=" * 70)

        # ================== إشعار بدء التشغيل للمالك ==================
        try:
            from telegram import Bot
            bot_instance = Bot(token=TOKEN)
            startup_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # جلب إحصائيات البداية
            stats = get_user_statistics() or {}
            channels_data = load_data().get("channels", {})
            active_channels = len([c for c in channels_data.values() if not c.get("completed", False)])
            
            startup_message = (
                f"🚀 **تم تشغيل البوت بنجاح مع النظام الفوري!**\n\n"
                f"⏰ **الوقت:** {startup_time}\n"
                f"📊 **قاعدة البيانات:** {DB_NAME}\n\n"
                f"📈 **إحصائيات البداية:**\n"
                f"• المستخدمين: {stats.get('total_users', 0)}\n"
                f"• القنوات النشطة: {active_channels}\n"
                f"• القنوات المكتملة: {len(channels_data) - active_channels}\n\n"
                f"⚡ **سرعة الفحص:**\n"
                f"• فحص الاشتراكات: 10 ثواني ⚡\n"
                f"• مراقبة القنوات: 30 ثانية ⚡\n"
                f"• إكمال القنوات: دقيقة ⚡\n"
                f"• فحص temp_left: دقيقتين ⚡\n\n"
                f"📦 **نظام إرسال قاعدة البيانات:**\n"
                f"• إرسال معلومات DB: كل 30 ثانية 📨\n"
                f"• تقرير حالة DB: كل 30 ثانية 📊\n"
                f"• ملف DB كامل: كل 5 دقائق 📁\n\n"
                f"✅ **جاهز للعمل الفوري!**"
            )
            
            bot_instance.send_message(
                chat_id=ADMIN_ID,
                text=startup_message,
                parse_mode="Markdown"
            )
        except Exception as startup_error:
            logger.error(f"❌ خطأ في إرسال إشعار التشغيل: {startup_error}")

        # ================== تشغيل البوت ==================
        logger.info("🔄 بدء استقبال التحديثات...")
        
        # إضافة تأخير قصير للسماح بإكمال التهيئة
        time.sleep(1)
        
        application.run_polling(
            drop_pending_updates=True,      # حذف التحديثات القديمة
            allowed_updates=Update.ALL_TYPES,  # جميع أنواع التحديثات
            close_loop=False,               # عدم إغلاق الحلقة تلقائياً
            read_timeout=25,                # وقت انتظار القراءة
            write_timeout=25,               # وقت انتظار الكتابة
            connect_timeout=25,             # وقت الاتصال
            pool_timeout=25,                # وقت انتظار المجمع
            poll_interval=0.5,              # وقت الانتظار بين الفحوصات
            bootstrap_retries=3,            # محاولات إعادة الاتصال
            timeout=30                      # وقت المهلة العام
        )

    except Exception as e:
        logger.error(f"❌ خطأ في التشغيل: {e}")
        import traceback
        traceback.print_exc()

        # محاولة إرسال إشعار الخطأ للمالك
        try:
            error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_message = (
                f"❌ **خطأ في تشغيل البوت!**\n\n"
                f"⏰ **الوقت:** {error_time}\n"
                f"📝 **الخطأ:** {str(e)[:200]}\n\n"
                f"🔄 **جاري إعادة التشغيل بعد 5 ثواني...**"
            )
            
            from telegram import Bot
            bot = Bot(token=TOKEN)
            bot.send_message(
                chat_id=ADMIN_ID,
                text=error_message,
                parse_mode="Markdown"
            )
        except:
            pass

        logger.info("🔄 محاولة إعادة التشغيل بعد 5 ثواني...")
        time.sleep(5)
        main()  # إعادة التشغيل
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("👋 إيقاف البوت...")
        print("\n👋 وداعاً!")
