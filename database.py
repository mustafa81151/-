import sqlite3
import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

# إعداد المسارات
current_dir = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(current_dir, "bot_database.db")

logger = logging.getLogger(__name__)

# ===================== إنشاء قاعدة البيانات =====================

# إضافة ADMIN_ID لتكون متاحة للجميع
ADMIN_ID = 8117492678  # أو يمكنك جعله متغيراً يتم تمريره

def init_database():
    """إنشاء جميع الجداول في قاعدة البيانات"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        # جدول المستخدمين - التحديث مع العمود المفقود
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                points INTEGER DEFAULT 0,
                invites INTEGER DEFAULT 0,
                total_earned INTEGER DEFAULT 0,
                total_spent INTEGER DEFAULT 0,
                first_join TEXT,
                last_active TEXT,
                last_channel_join TEXT,  -- ✅ العمود الجديد
                total_channel_points INTEGER DEFAULT 0,  -- ✅ العمود الجديد
                channel_history TEXT DEFAULT '[]',  -- ✅ العمود الجديد
                inactive INTEGER DEFAULT 0,
                reports_made INTEGER DEFAULT 0,
                reports_received INTEGER DEFAULT 0,
                invited_users TEXT DEFAULT '[]',
                bought_channels TEXT DEFAULT '{}',
                joined_channels TEXT DEFAULT '{}',
                active_subscriptions TEXT DEFAULT '[]',
                orders TEXT DEFAULT '[]',
                daily_gift TEXT DEFAULT '{}',
                reported_channels TEXT DEFAULT '[]',
                left_channels TEXT DEFAULT '[]',
                temp_left_channels TEXT DEFAULT '[]',
                permanent_left_channels TEXT DEFAULT '[]',
                left_completed_channels TEXT DEFAULT '[]',
                transactions TEXT DEFAULT '[]',
                join_history TEXT DEFAULT '[]',
                permanent_registered BOOLEAN DEFAULT 0  -- ✅ إضافة عمود للتسجيل الدائم
            )
        ''')
        
        # جدول القنوات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                channel_id TEXT PRIMARY KEY,
                username TEXT,
                owner TEXT,
                required INTEGER,
                current INTEGER DEFAULT 0,
                completed INTEGER DEFAULT 0,
                reuse_count INTEGER DEFAULT 0,
                created_at TEXT,
                completed_at TEXT,
                reactivated_at TEXT,
                last_activity TEXT,
                admin_added INTEGER DEFAULT 0,
                bot_is_admin INTEGER DEFAULT 1,
                last_admin_check TEXT,
                transaction_id TEXT,
                joined_users TEXT DEFAULT '[]',
                leave_history TEXT DEFAULT '[]',
                return_history TEXT DEFAULT '[]',
                is_active INTEGER DEFAULT 1,
                left_users TEXT DEFAULT '[]',
                reports_count INTEGER DEFAULT 0
            )
        ''')
        
        # جدول الأكواد
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS codes (
                code_name TEXT PRIMARY KEY,
                points INTEGER,
                max_uses INTEGER,
                used_count INTEGER DEFAULT 0,
                created_at TEXT,
                created_by TEXT,
                used_by TEXT DEFAULT '[]'
            )
        ''')
        
        # جدول البلاغات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY,
                channel_id TEXT,
                channel_username TEXT,
                channel_type TEXT,
                reporter_id TEXT,
                reporter_username TEXT,
                reason TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT
            )
        ''')
        
        # جدول الإعدادات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        
        # جدول المسؤولين
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id TEXT PRIMARY KEY,
                added_at TEXT
            )
        ''')
        
        # جدول المحظورين
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id TEXT PRIMARY KEY,
                banned_at TEXT,
                reason TEXT
            )
        ''')
        
        # جدول المكتومين
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS muted_users (
                user_id TEXT PRIMARY KEY,
                muted_at TEXT,
                until TEXT,
                duration INTEGER,
                reason TEXT,
                muted_by TEXT
            )
        ''')
        
        # جدول قنوات الاشتراك الإجباري
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS force_sub_channels (
                channel_username TEXT PRIMARY KEY,
                added_at TEXT
            )
        ''')
        
        # جدول الإحصائيات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                stat_key TEXT PRIMARY KEY,
                stat_value INTEGER DEFAULT 0
            )
        ''')
        
        # جدول المعاملات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                user_id TEXT,
                channel_id TEXT,
                points INTEGER,
                type TEXT,
                details TEXT,
                timestamp TEXT,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # جدول سجل التحقق
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS verification_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                channel_id TEXT,
                status TEXT,
                details TEXT,
                timestamp TEXT
            )
        ''')
        
        # جدول سجل النشاط
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS activity_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                action TEXT,
                details TEXT,
                timestamp TEXT
            )
        ''')
        
        # جدول سجل المغادرة
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leave_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                channel_id TEXT,
                points_deducted INTEGER,
                timestamp TEXT
            )
        ''')
        
        # ⭐⭐⭐ جدول التسجيلات الدائمة ⭐⭐⭐
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS permanent_registrations (
                registration_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT UNIQUE,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                language_code TEXT,
                joined_at TIMESTAMP,
                completed_force_sub BOOLEAN DEFAULT 0,
                force_sub_completed_at TIMESTAMP,
                invite_ref TEXT,
                status TEXT DEFAULT 'pending', -- pending, completed, left, banned
                last_checked TIMESTAMP,
                archived BOOLEAN DEFAULT 0,
                metadata TEXT DEFAULT '{}',
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        
        # ⭐⭐⭐ جدول سجل التحقق من الاشتراك ⭐⭐⭐
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS subscription_checks (
                check_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                channel_username TEXT,
                subscribed BOOLEAN,
                checked_at TIMESTAMP,
                force_sub BOOLEAN DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        
        # إنشاء الفهارس
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_points ON users(points)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_channels_owner ON channels(owner)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_channels_completed ON channels(completed)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_channel ON transactions(channel_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permanent_reg_user ON permanent_registrations(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permanent_reg_status ON permanent_registrations(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscription_checks_user ON subscription_checks(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscription_checks_time ON subscription_checks(checked_at)')
        
        # إضافة إحصائيات افتراضية
        default_stats = [
            ('total_users', 0),
            ('total_points', 0),
            ('total_invites', 0),
            ('total_purchases', 0),
            ('total_joins', 0),
            ('total_reports', 0),
            ('total_daily_gifts', 0),
            ('total_mutes', 0),
            ('total_left_users', 0),
            ('total_points_deducted', 0),
            ('total_completed_channels', 0),
            ('total_channel_joins', 0),
            ('total_channel_points_earned', 0),
            ('total_channel_points_deducted', 0),
            ('total_transactions', 0),
            ('total_permanent_registrations', 0),  # ✅ إضافة إحصائية جديدة
            ('total_force_sub_completed', 0),  # ✅ إضافة إحصائية جديدة
            ('total_returning_users', 0)  # ✅ إضافة إحصائية جديدة
        ]
        
        for key, value in default_stats:
            cursor.execute('''
                INSERT OR IGNORE INTO stats (stat_key, stat_value) 
                VALUES (?, ?)
            ''', (key, value))
        
        conn.commit()
        logger.info("✅ تم إنشاء قاعدة البيانات بنجاح")
        
        # إضافة الأعمدة المفقودة للقواعد الحالية
        add_missing_columns()
        
        # التحقق من تحديث جدول users لإضافة العمود الجديد
        try:
            cursor.execute("SELECT permanent_registered FROM users LIMIT 1")
        except sqlite3.OperationalError:
            # العمود غير موجود، إضافته
            cursor.execute('ALTER TABLE users ADD COLUMN permanent_registered BOOLEAN DEFAULT 0')
            logger.info("✅ تم إضافة عمود permanent_registered إلى جدول users")
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"❌ خطأ في إنشاء قاعدة البيانات: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()

# أضف هذه الدوال إلى database.py



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



def update_user_data(user_id, updates, action_type="update", transaction_id=None):
    """تحديث بيانات المستخدم مع إصلاح الأخطاء"""
    user_id = str(user_id)
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # جلب البيانات الحالية
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        user_row = cursor.fetchone()
        
        if not user_row:
            # إنشاء بيانات افتراضية
            create_default_user_data(user_id)
            cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            user_row = cursor.fetchone()
        
        # الحصول على أسماء الأعمدة
        cursor.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in cursor.fetchall()]
        
        # إعداد البيانات للتحديث
        set_clauses = []
        values = []
        
        for key, value in updates.items():
            # التحقق من وجود العمود في قاعدة البيانات
            if key not in columns:
                logger.warning(f"⚠️ العمود {key} غير موجود في جدول users، سيتم تجاهله")
                continue
            
            # تحويل القيم المعقدة إلى JSON
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            
            set_clauses.append(f"{key} = ?")
            values.append(value)
        
        # تحديث last_active تلقائياً
        if "last_active" not in updates and "last_active" in columns:
            set_clauses.append("last_active = ?")
            values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # إضافة transaction_id إلى سجل المعاملات
        if transaction_id and "transactions" in columns:
            cursor.execute("SELECT transactions FROM users WHERE user_id = ?", (user_id,))
            transactions_json = cursor.fetchone()[0] or "[]"
            
            try:
                transactions = json.loads(transactions_json)
                transactions.append({
                    "id": transaction_id,
                    "action": action_type,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "updates": updates
                })
                
                set_clauses.append("transactions = ?")
                values.append(json.dumps(transactions, ensure_ascii=False))
            except Exception as e:
                logger.error(f"خطأ في تحديث transactions: {e}")
        
        # تنفيذ التحديث
        if set_clauses:
            sql = f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = ?"
            values.append(user_id)
            cursor.execute(sql, values)
        
        conn.commit()
        conn.close()
        
        # ✅ إزالة قسم التخزين المؤقت المشكلة
        # كان الكود يحاول الوصول إلى _data_cache غير الموجود
        
        logger.info(f"✅ تم تحديث بيانات {user_id} - {action_type}")
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في update_user_data: {e}")
        import traceback
        traceback.print_exc()
        return False

def load_users() -> Dict:
    """تحميل جميع المستخدمين"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT * FROM users')
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        
        users = {}
        for row in rows:
            user_data = dict(zip(columns, row))
            user_id = user_data['user_id']
            
            # تحويل JSON strings
            json_fields = [
                'invited_users', 'bought_channels', 'joined_channels',
                'active_subscriptions', 'orders', 'daily_gift',
                'reported_channels', 'left_channels', 'temp_left_channels',
                'permanent_left_channels', 'left_completed_channels',
                'transactions', 'join_history'
            ]
            
            for field in json_fields:
                if field in user_data and user_data[field]:
                    user_data[field] = json.loads(user_data[field])
            
            users[user_id] = user_data
        
        return users
        
    except Exception as e:
        logger.error(f"خطأ في load_users: {e}")
        return {}
    finally:
        conn.close()


        
        


def save_users(users_data: Dict, backup: bool = False) -> bool:
    """حفظ بيانات المستخدمين (للتوافق - غير مستخدم في SQLite)"""
    # في SQLite، البيانات محفوظة مباشرة
    return True

def create_default_user_data() -> Dict:
    """إنشاء بيانات مستخدم افتراضية"""
    return {
        "points": 0,
        "invites": 0,
        "invited_users": [],
        "bought_channels": {},
        "joined_channels": {},
        "username": "",
        "first_name": "",
        "last_name": "",
        "first_join": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_earned": 0,
        "total_spent": 0,
        "orders": [],
        "reports_made": 0,
        "reports_received": 0,
        "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "active_subscriptions": [],
        "daily_gift": {
            "last_claimed": None,
            "streak": 0,
            "total_claimed": 0
        },
        "reported_channels": [],
        "inactive": False,
        "left_channels": [],
        "transactions": [],
        "temp_left_channels": [],
        "permanent_left_channels": [],
        "left_completed_channels": [],
        "join_history": []
    }

# ===================== دوال القنوات =====================

def get_channel_data(channel_id: str) -> Optional[Dict]:
    """الحصول على بيانات قناة"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT * FROM channels WHERE channel_id = ?', (channel_id,))
        row = cursor.fetchone()
        
        if row:
            columns = [description[0] for description in cursor.description]
            channel_data = dict(zip(columns, row))
            
            # تحويل JSON strings
            json_fields = ['joined_users', 'leave_history', 'return_history']
            for field in json_fields:
                if field in channel_data and channel_data[field]:
                    channel_data[field] = json.loads(channel_data[field])
            
            return channel_data
        
        return None
        
    except Exception as e:
        logger.error(f"خطأ في get_channel_data: {e}")
        return None
    finally:
        conn.close()

def save_channel_data(channel_id: str, channel_data: Dict) -> bool:
    """حفظ بيانات قناة"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        # تحويل JSON fields
        json_fields = ['joined_users', 'leave_history', 'return_history']
        processed_data = channel_data.copy()
        
        for field in json_fields:
            if field in processed_data and isinstance(processed_data[field], (list, dict)):
                processed_data[field] = json.dumps(processed_data[field])
        
        # التحقق من وجود القناة
        cursor.execute('SELECT channel_id FROM channels WHERE channel_id = ?', (channel_id,))
        exists = cursor.fetchone()
        
        if exists:
            # تحديث
            set_clause = ', '.join([f'{k} = ?' for k in processed_data.keys()])
            values = list(processed_data.values())
            values.append(channel_id)
            cursor.execute(f'UPDATE channels SET {set_clause} WHERE channel_id = ?', values)
        else:
            # إدراج جديد
            columns = ', '.join(processed_data.keys())
            placeholders = ', '.join(['?' for _ in processed_data])
            cursor.execute(
                f'INSERT INTO channels (channel_id, {columns}) VALUES (?, {placeholders})',
                [channel_id] + list(processed_data.values())
            )
        
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"خطأ في save_channel_data: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def load_channels() -> Dict:
    """تحميل جميع القنوات"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT * FROM channels')
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        
        channels = {}
        for row in rows:
            channel_data = dict(zip(columns, row))
            channel_id = channel_data['channel_id']
            
            # تحويل JSON
            json_fields = ['joined_users', 'leave_history', 'return_history']
            for field in json_fields:
                if field in channel_data and channel_data[field]:
                    channel_data[field] = json.loads(channel_data[field])
            
            channels[channel_id] = channel_data
        
        return channels
        
    except Exception as e:
        logger.error(f"خطأ في load_channels: {e}")
        return {}
    finally:
        conn.close()

def delete_channel(channel_id: str) -> bool:
    """حذف قناة"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('DELETE FROM channels WHERE channel_id = ?', (channel_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"خطأ في delete_channel: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

# ===================== دوال البيانات العامة =====================

def load_data(force_reload: bool = False) -> Dict:
    """تحميل جميع البيانات"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        data = {
            "channels": load_channels(),
            "codes": {},
            "reports": {},
            "admins": [],
            "banned_users": [],
            "muted_users": {},
            "force_sub_channels": [],
            "stats": {}
        }
        
        # تحميل الأكواد
        cursor.execute('SELECT * FROM codes')
        for row in cursor.fetchall():
            code_name = row[0]
            data["codes"][code_name] = {
                "points": row[1],
                "max_uses": row[2],
                "used_count": row[3],
                "created_at": row[4],
                "created_by": row[5],
                "used_by": json.loads(row[6]) if row[6] else []
            }
        
        # تحميل البلاغات
        cursor.execute('SELECT * FROM reports')
        for row in cursor.fetchall():
            report_id = row[0]
            data["reports"][report_id] = {
                "channel_id": row[1],
                "channel_username": row[2],
                "channel_type": row[3],
                "reporter_id": row[4],
                "reporter_username": row[5],
                "reason": row[6],
                "status": row[7],
                "created_at": row[8]
            }
        
        # تحميل المسؤولين
        cursor.execute('SELECT user_id FROM admins')
        data["admins"] = [row[0] for row in cursor.fetchall()]
        
        # تحميل المحظورين
        cursor.execute('SELECT user_id FROM banned_users')
        data["banned_users"] = [row[0] for row in cursor.fetchall()]
        
        # تحميل المكتومين
        cursor.execute('SELECT * FROM muted_users')
        for row in cursor.fetchall():
            data["muted_users"][row[0]] = {
                "muted_at": row[1],
                "until": row[2],
                "duration": row[3],
                "reason": row[4],
                "muted_by": row[5]
            }
        
        # تحميل قنوات الإجباري
        cursor.execute('SELECT channel_username FROM force_sub_channels')
        data["force_sub_channels"] = [row[0] for row in cursor.fetchall()]
        
        # تحميل الإحصائيات
        cursor.execute('SELECT * FROM stats')
        for row in cursor.fetchall():
            data["stats"][row[0]] = row[1]
        
        return data
        
    except Exception as e:
        logger.error(f"خطأ في load_data: {e}")
        return create_initial_data()
    finally:
        conn.close()

def save_data(data: Dict, backup: bool = False) -> bool:
    """حفظ البيانات العامة"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        # حفظ القنوات
        if "channels" in data:
            for channel_id, channel_data in data["channels"].items():
                save_channel_data(channel_id, channel_data)
        
        # حفظ الأكواد
        if "codes" in data:
            cursor.execute('DELETE FROM codes')
            for code_name, code_data in data["codes"].items():
                cursor.execute('''
                    INSERT INTO codes (code_name, points, max_uses, used_count, created_at, created_by, used_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    code_name,
                    code_data.get('points', 0),
                    code_data.get('max_uses', 0),
                    code_data.get('used_count', 0),
                    code_data.get('created_at', ''),
                    code_data.get('created_by', ''),
                    json.dumps(code_data.get('used_by', []))
                ))
        
        # حفظ البلاغات
        if "reports" in data:
            cursor.execute('DELETE FROM reports')
            for report_id, report_data in data["reports"].items():
                cursor.execute('''
                    INSERT INTO reports (report_id, channel_id, channel_username, channel_type,
                                       reporter_id, reporter_username, reason, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    report_id,
                    report_data.get('channel_id', ''),
                    report_data.get('channel_username', ''),
                    report_data.get('channel_type', ''),
                    report_data.get('reporter_id', ''),
                    report_data.get('reporter_username', ''),
                    report_data.get('reason', ''),
                    report_data.get('status', 'pending'),
                    report_data.get('created_at', '')
                ))
        
        # حفظ المسؤولين
        if "admins" in data:
            cursor.execute('DELETE FROM admins')
            for admin_id in data["admins"]:
                cursor.execute('INSERT INTO admins (user_id, added_at) VALUES (?, ?)',
                             (admin_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        
        # حفظ المحظورين
        if "banned_users" in data:
            cursor.execute('DELETE FROM banned_users')
            for user_id in data["banned_users"]:
                cursor.execute('INSERT INTO banned_users (user_id, banned_at, reason) VALUES (?, ?, ?)',
                             (user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ''))
        
        # حفظ المكتومين
        if "muted_users" in data:
            cursor.execute('DELETE FROM muted_users')
            for user_id, mute_data in data["muted_users"].items():
                cursor.execute('''
                    INSERT INTO muted_users (user_id, muted_at, until, duration, reason, muted_by)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    user_id,
                    mute_data.get('muted_at', ''),
                    mute_data.get('until', ''),
                    mute_data.get('duration', 0),
                    mute_data.get('reason', ''),
                    mute_data.get('muted_by', '')
                ))
        
        # حفظ قنوات الإجباري
        if "force_sub_channels" in data:
            cursor.execute('DELETE FROM force_sub_channels')
            for channel_username in data["force_sub_channels"]:
                cursor.execute('INSERT INTO force_sub_channels (channel_username, added_at) VALUES (?, ?)',
                             (channel_username, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        
        # حفظ الإحصائيات
        if "stats" in data:
            for key, value in data["stats"].items():
                cursor.execute('''
                    INSERT OR REPLACE INTO stats (stat_key, stat_value)
                    VALUES (?, ?)
                ''', (key, value))
        
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"خطأ في save_data: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def create_initial_data() -> Dict:
    """إنشاء البيانات الأولية"""
    return {
        "channels": {},
        "codes": {},
        "reports": {},
        "admins": [],
        "banned_users": [],
        "muted_users": {},
        "force_sub_channels": [],
        "stats": {
            "total_users": 0,
            "total_points": 0,
            "total_invites": 0,
            "total_purchases": 0,
            "total_joins": 0,
            "total_reports": 0,
            "total_daily_gifts": 0,
            "total_mutes": 0
        }
    }

# ===================== دوال الإحصائيات =====================



def update_stat(stat_key: str, increment: int = 1) -> bool:
    """تحديث إحصائية"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO stats (stat_key, stat_value) VALUES (?, ?)
            ON CONFLICT(stat_key) DO UPDATE SET stat_value = stat_value + ?
        ''', (stat_key, increment, increment))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"خطأ في update_stat: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def is_muted(user_id: str) -> tuple:
    """التحقق من كتم المستخدم"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT * FROM muted_users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        
        if row:
            try:
                mute_until = row[2]  # until column
                
                if mute_until:
                    try:
                        mute_until_time = datetime.strptime(mute_until, "%Y-%m-%d %H:%M:%S")
                        if datetime.now() < mute_until_time:
                            return True, mute_until
                        else:
                            # انتهى الكتم - حذف
                            cursor.execute('DELETE FROM muted_users WHERE user_id = ?', (user_id,))
                            conn.commit()
                            return False, None
                    except Exception as time_error:
                        logger.warning(f"خطأ في تنسيق الوقت: {time_error}")
                        return False, None
                return True, "دائم"
            
            except Exception as inner_error:
                logger.error(f"خطأ في معالجة بيانات الكتم: {inner_error}")
                return False, None
        
        return False, None
        
    except Exception as e:
        logger.error(f"خطأ في is_muted: {e}")
        # ✅ إرجاع قيم افتراضية بدلاً من رفع استثناء
        return False, None
    finally:
        conn.close()

async def safe_cleanup_expired_mutes():
    """تنظيف الكتم المنتهي مع معالجة آمنة للأخطاء"""
    try:
        await cleanup_expired_mutes()
    except Exception as e:
        logger.error(f"❌ خطأ في safe_cleanup_expired_mutes: {e}")
        # عدم إعادة رفع الاستثناء لمنع توقف البوت
        
def get_stat(stat_key: str) -> int:
    """الحصول على قيمة إحصائية"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT stat_value FROM stats WHERE stat_key = ?', (stat_key,))
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"خطأ في get_stat: {e}")
        return 0
    finally:
        conn.close()

# ===================== دوال المسؤولين والحظر =====================

def add_missing_columns():
    """إضافة الأعمدة المفقودة إلى الجداول"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # قائمة الأعمدة المفقودة التي يحتاجها البوت
        missing_columns = [
            # جدول users
            ("users", "last_channel_join", "TEXT"),
            ("users", "total_channel_points", "INTEGER DEFAULT 0"),
            ("users", "channel_history", "TEXT DEFAULT '[]'"),
            
            # جدول channels
            ("channels", "is_active", "INTEGER DEFAULT 1"),
            ("channels", "left_users", "TEXT DEFAULT '[]'"),
            ("channels", "reports_count", "INTEGER DEFAULT 0"),
            
            # جدول stats - إضافة إحصائيات جديدة
            ("stats", "total_left_users", "INTEGER DEFAULT 0"),
            ("stats", "total_points_deducted", "INTEGER DEFAULT 0"),
            ("stats", "total_completed_channels", "INTEGER DEFAULT 0")
        ]
        
        for table, column, column_type in missing_columns:
            try:
                # التحقق إذا كان العمود موجوداً
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [col[1] for col in cursor.fetchall()]
                
                if column not in columns:
                    # إضافة العمود
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
                    logger.info(f"✅ تم إضافة العمود {column} إلى جدول {table}")
                    
                    # تعيين القيم الافتراضية للبيانات الحالية
                    if table == "users" and column == "last_channel_join":
                        cursor.execute(f"UPDATE {table} SET {column} = '' WHERE {column} IS NULL")
                    elif "DEFAULT" in column_type:
                        # استخراج القيمة الافتراضية
                        default_value = column_type.split("DEFAULT")[1].strip().strip("'")
                        cursor.execute(f"UPDATE {table} SET {column} = {default_value} WHERE {column} IS NULL")
                        
            except Exception as e:
                logger.error(f"⚠️ خطأ في إضافة العمود {column} لجدول {table}: {e}")
                continue
        
        # التحقق من جدول stats وإضافة الإحصائيات المفقودة
        cursor.execute("SELECT stat_key FROM stats")
        existing_stats = [row[0] for row in cursor.fetchall()]
        
        required_stats = [
            ("total_left_users", 0),
            ("total_points_deducted", 0),
            ("total_completed_channels", 0),
            ("total_channel_joins", 0),
            ("total_channel_points_earned", 0),
            ("total_channel_points_deducted", 0)
        ]
        
        for stat_key, default_value in required_stats:
            if stat_key not in existing_stats:
                cursor.execute('''
                    INSERT INTO stats (stat_key, stat_value) 
                    VALUES (?, ?)
                ''', (stat_key, default_value))
                logger.info(f"✅ تمت إضافة إحصائية: {stat_key}")
        
        conn.commit()
        conn.close()
        
        logger.info("✅ تمت إضافة جميع الأعمدة المفقودة بنجاح")
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في add_missing_columns: {e}")
        return False

def is_admin(user_id: int) -> bool:
    """التحقق من كون المستخدم مسؤول"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT user_id FROM admins WHERE user_id = ?', (str(user_id),))
        return cursor.fetchone() is not None
    except:
        return False
    finally:
        conn.close()

def is_banned(user_id: int) -> bool:
    """التحقق من حظر المستخدم"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT user_id FROM banned_users WHERE user_id = ?', (str(user_id),))
        return cursor.fetchone() is not None
    except:
        return False
    finally:
        conn.close()

def is_muted(user_id: str) -> tuple:
    """التحقق من كتم المستخدم"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT * FROM muted_users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        
        if row:
            mute_until = row[2]  # until column
            
            if mute_until:
                try:
                    mute_until_time = datetime.strptime(mute_until, "%Y-%m-%d %H:%M:%S")
                    if datetime.now() < mute_until_time:
                        return True, mute_until
                    else:
                        # انتهى الكتم - حذف
                        cursor.execute('DELETE FROM muted_users WHERE user_id = ?', (user_id,))
                        conn.commit()
                        return False, None
                except:
                    return False, None
            return True, "دائم"
        
        return False, None
        
    except Exception as e:
        logger.error(f"خطأ في is_muted: {e}")
        return False, None
    finally:
        conn.close()

# ===================== نسخ احتياطي من SQLite =====================

def backup_database(backup_dir: str) -> bool:
    """إنشاء نسخة احتياطية من قاعدة البيانات"""
    try:
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"bot_database_{timestamp}.db")
        
        import shutil
        shutil.copy2(DB_NAME, backup_file)
        
        logger.info(f"✅ نسخة احتياطية: {backup_file}")
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في النسخ الاحتياطي: {e}")
        return False
