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
ADMIN_ID = 8401168362  # أو يمكنك جعله متغيراً يتم تمريره

def init_database():
    """إنشاء جميع الجداول في قاعدة البيانات"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        # جدول المستخدمين - النسخة المحدثة مع جميع الأعمدة
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
                last_channel_join TEXT,
                total_channel_points INTEGER DEFAULT 0,
                channel_history TEXT DEFAULT '[]',
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
                permanent_registered BOOLEAN DEFAULT 0,
                
                -- ✅ الأعمدة الجديدة للمشكلة الحالية
                force_sub_left INTEGER DEFAULT 0,
                force_sub_left_at TEXT,
                force_sub_passed INTEGER DEFAULT 0,
                force_sub_passed_at TEXT,
                registered INTEGER DEFAULT 0,
                
                -- ✅ أعمدة إضافية قد تكون مفقودة
                channel_reports TEXT DEFAULT '{}',
                premium BOOLEAN DEFAULT 0,
                premium_until TEXT,
                last_daily_gift TEXT,
                total_daily_gifts INTEGER DEFAULT 0,
                streak INTEGER DEFAULT 0,
                wallet_balance INTEGER DEFAULT 0,
                total_reports INTEGER DEFAULT 0,
                total_orders INTEGER DEFAULT 0,
                total_channel_joins INTEGER DEFAULT 0,
                total_channel_leaves INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                language TEXT DEFAULT 'ar',
                notifications BOOLEAN DEFAULT 1,
                privacy_mode BOOLEAN DEFAULT 0,
                verification_level INTEGER DEFAULT 1,
                trust_score INTEGER DEFAULT 50,
                last_report_time TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # جدول القنوات - النسخة المحدثة
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
                reports_count INTEGER DEFAULT 0,
                
                -- ✅ أعمدة إضافية
                channel_type TEXT DEFAULT 'public',
                description TEXT,
                category TEXT,
                price_per_join INTEGER DEFAULT 0,
                min_points_to_join INTEGER DEFAULT 0,
                max_joins_per_user INTEGER DEFAULT 1,
                cooldown_hours INTEGER DEFAULT 24,
                verification_required BOOLEAN DEFAULT 1,
                auto_complete BOOLEAN DEFAULT 1,
                notify_owner BOOLEAN DEFAULT 1,
                quality_score INTEGER DEFAULT 100,
                total_penalties INTEGER DEFAULT 0,
                total_reported INTEGER DEFAULT 0,
                last_reported TEXT,
                featured BOOLEAN DEFAULT 0,
                verified BOOLEAN DEFAULT 0,
                tags TEXT DEFAULT '[]',
                metadata TEXT DEFAULT '{}'
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
                used_by TEXT DEFAULT '[]',
                
                -- ✅ أعمدة إضافية
                expires_at TEXT,
                minimum_level INTEGER DEFAULT 0,
                one_time_use BOOLEAN DEFAULT 0,
                category TEXT DEFAULT 'general',
                description TEXT,
                metadata TEXT DEFAULT '{}'
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
                created_at TEXT,
                
                -- ✅ أعمدة إضافية
                resolved_at TEXT,
                resolved_by TEXT,
                action_taken TEXT,
                severity TEXT DEFAULT 'medium',
                evidence TEXT,
                notes TEXT
            )
        ''')
        
        # جدول الإعدادات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                category TEXT DEFAULT 'general',
                description TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                requires_restart BOOLEAN DEFAULT 0
            )
        ''')
        
        # جدول المسؤولين
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id TEXT PRIMARY KEY,
                added_at TEXT,
                added_by TEXT,
                role TEXT DEFAULT 'moderator',
                permissions TEXT DEFAULT '{}',
                last_active TEXT,
                status TEXT DEFAULT 'active'
            )
        ''')
        
        # جدول المحظورين
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id TEXT PRIMARY KEY,
                banned_at TEXT,
                reason TEXT,
                banned_by TEXT,
                duration_days INTEGER DEFAULT 0,
                unbanned_at TEXT,
                unbanned_by TEXT,
                status TEXT DEFAULT 'active',
                notes TEXT
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
                muted_by TEXT,
                
                -- ✅ أعمدة إضافية
                mute_type TEXT DEFAULT 'text',
                remaining_duration INTEGER,
                auto_unmute BOOLEAN DEFAULT 1,
                warnings INTEGER DEFAULT 0,
                history TEXT DEFAULT '[]'
            )
        ''')
        
        # جدول قنوات الاشتراك الإجباري
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS force_sub_channels (
                channel_username TEXT PRIMARY KEY,
                added_at TEXT,
                added_by TEXT,
                priority INTEGER DEFAULT 1,
                is_active BOOLEAN DEFAULT 1,
                description TEXT,
                last_checked TEXT,
                member_count INTEGER DEFAULT 0
            )
        ''')
        
        # جدول الإحصائيات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                stat_key TEXT PRIMARY KEY,
                stat_value INTEGER DEFAULT 0,
                category TEXT DEFAULT 'general',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                description TEXT
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
                status TEXT DEFAULT 'completed',
                
                -- ✅ أعمدة إضافية
                reference_id TEXT,
                refunded BOOLEAN DEFAULT 0,
                refunded_at TEXT,
                refund_reason TEXT,
                metadata TEXT DEFAULT '{}',
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
                timestamp TEXT,
                
                -- ✅ أعمدة إضافية
                verification_method TEXT DEFAULT 'manual',
                attempt_number INTEGER DEFAULT 1,
                ip_address TEXT,
                user_agent TEXT,
                success BOOLEAN DEFAULT 0
            )
        ''')
        
        # جدول سجل النشاط
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS activity_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                action TEXT,
                details TEXT,
                timestamp TEXT,
                
                -- ✅ أعمدة إضافية
                ip_address TEXT,
                user_agent TEXT,
                location TEXT,
                duration_seconds INTEGER,
                result TEXT
            )
        ''')
        
        # جدول سجل المغادرة
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leave_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                channel_id TEXT,
                points_deducted INTEGER,
                timestamp TEXT,
                
                -- ✅ أعمدة إضافية
                auto_detected BOOLEAN DEFAULT 1,
                manual_action BOOLEAN DEFAULT 0,
                action_by TEXT,
                reason TEXT,
                refunded BOOLEAN DEFAULT 0,
                refunded_at TEXT
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
                status TEXT DEFAULT 'pending',
                last_checked TIMESTAMP,
                archived BOOLEAN DEFAULT 0,
                metadata TEXT DEFAULT '{}',
                
                -- ✅ أعمدة إضافية
                notification_sent BOOLEAN DEFAULT 0,
                welcome_points_given BOOLEAN DEFAULT 0,
                invited_users_count INTEGER DEFAULT 0,
                total_earned_points INTEGER DEFAULT 0,
                last_login TIMESTAMP,
                login_count INTEGER DEFAULT 0,
                
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
                
                -- ✅ أعمدة إضافية
                check_method TEXT DEFAULT 'api',
                response_time_ms INTEGER,
                error_message TEXT,
                cache_hit BOOLEAN DEFAULT 0,
                
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        
        # ⭐⭐⭐ جدول جديد: سجل دخول المستخدمين ⭐⭐⭐
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_login_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                login_time TIMESTAMP,
                login_type TEXT DEFAULT 'start_command',
                ip_address TEXT,
                user_agent TEXT,
                success BOOLEAN DEFAULT 1,
                details TEXT,
                
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        
        # ⭐⭐⭐ جدول جديد: نظام الحوافز والمكافآت ⭐⭐⭐
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rewards (
                reward_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                reward_type TEXT,
                points INTEGER,
                reason TEXT,
                given_at TIMESTAMP,
                given_by TEXT,
                status TEXT DEFAULT 'active',
                
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        
        # إنشاء الفهارس لتحسين الأداء
        logger.info("🔄 إنشاء الفهارس...")
        
        # فهارس جدول users
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_points ON users(points)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_registered ON users(registered)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_force_sub ON users(force_sub_passed)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_active ON users(last_active)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_invites ON users(invites)')
        # ⭐⭐⭐ تم تأجيل هذا الفهرس ⭐⭐⭐
        # cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)')
        
        # فهارس جدول channels
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_channels_owner ON channels(owner)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_channels_completed ON channels(completed)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_channels_active ON channels(is_active)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_channels_username ON channels(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_channels_required ON channels(required)')
        
        # فهارس جدول transactions
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_channel ON transactions(channel_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type)')
        
        # فهارس جدول permanent_registrations
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permanent_reg_user ON permanent_registrations(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permanent_reg_status ON permanent_registrations(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permanent_reg_force_sub ON permanent_registrations(completed_force_sub)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permanent_reg_joined ON permanent_registrations(joined_at)')
        
        # فهارس جدول subscription_checks
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscription_checks_user ON subscription_checks(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscription_checks_time ON subscription_checks(checked_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscription_checks_channel ON subscription_checks(channel_username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscription_checks_subscribed ON subscription_checks(subscribed)')
        
        # فهارس أخرى
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_activity_logs_user ON activity_logs(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_activity_logs_time ON activity_logs(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_leave_logs_user ON leave_logs(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_leave_logs_channel ON leave_logs(channel_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_login_logs_user ON user_login_logs(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_login_logs_time ON user_login_logs(login_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rewards_user ON rewards(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rewards_type ON rewards(reward_type)')
        
        # إضافة إحصائيات افتراضية
        logger.info("🔄 إضافة الإحصائيات الافتراضية...")
        
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
            ('total_permanent_registrations', 0),
            ('total_force_sub_completed', 0),
            ('total_returning_users', 0),
            ('total_registered_users', 0),
            ('total_unregistered_users', 0),
            ('total_banned_users', 0),
            ('total_muted_users', 0),
            ('total_premium_users', 0),
            ('total_active_channels', 0),
            ('total_inactive_channels', 0),
            ('total_verified_channels', 0),
            ('total_force_sub_channels', 0),
            ('total_codes_created', 0),
            ('total_codes_used', 0),
            ('total_verification_attempts', 0),
            ('total_successful_verifications', 0),
            ('total_failed_verifications', 0),
            ('total_login_attempts', 0),
            ('total_rewards_given', 0),
            ('system_uptime_days', 0),
            ('average_points_per_user', 0),
            ('average_invites_per_user', 0),
            ('conversion_rate', 0),
            ('retention_rate', 0),
            ('daily_active_users', 0),
            ('weekly_active_users', 0),
            ('monthly_active_users', 0)
        ]
        
        for key, value in default_stats:
            cursor.execute('''
                INSERT OR IGNORE INTO stats (stat_key, stat_value) 
                VALUES (?, ?)
            ''', (key, value))
        
        # إعدادات النظام الافتراضية
        default_settings = [
            ('bot_name', 'قنوات تيليجرام'),
            ('bot_language', 'ar'),
            ('welcome_message', 'مرحباً بك في البوت!'),
            ('minimum_points_for_store', 0),
            ('daily_gift_points', 2),
            ('invite_reward_points', 3),
            ('channel_join_points', 2),
            ('leave_penalty_points', 5),
            ('max_daily_gift_streak', 7),
            ('force_sub_enabled', 1),
            ('force_sub_required', 1),
            ('auto_monitor_interval', 300),
            ('auto_cleanup_interval', 3600),
            ('max_reuse_count', 3),
            ('admin_notifications', 1),
            ('user_notifications', 1),
            ('maintenance_mode', 0),
            ('debug_mode', 0),
            ('backup_interval', 1800),
            ('cache_ttl', 30)
        ]
        
        for key, value in default_settings:
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value) 
                VALUES (?, ?)
            ''', (key, str(value)))
        
        conn.commit()
        logger.info("✅ تم إنشاء قاعدة البيانات بنجاح")
        
        # 🔧 إصلاح مشكلة عمود status والمشاكل الأخرى
        logger.info("🔧 إصلاح الأعمدة المفقودة...")
        
        # 1. إضافة الأعمدة المفقودة
        add_missing_columns()
        
        # 2. التحقق من عمود registered
        check_and_add_registered_column()
        
        # 3. الآن إنشاء الفهرس لـ status بعد إضافة العمود
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)')
            logger.info("✅ تم إنشاء الفهرس idx_users_status بنجاح")
        except sqlite3.OperationalError as e:
            logger.warning(f"⚠️ لم يتمكن من إنشاء الفهرس idx_users_status: {e}")
            # حاول إضافة العمود أولاً
            try:
                cursor.execute('ALTER TABLE users ADD COLUMN status TEXT DEFAULT "active"')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)')
                logger.info("✅ تمت إضافة عمود status وإنشاء الفهرس")
            except Exception as e2:
                logger.error(f"❌ خطأ في معالجة عمود status: {e2}")
        
        # 4. تحديث حالة جميع المستخدمين إلى active
        try:
            cursor.execute('UPDATE users SET status = "active" WHERE status IS NULL OR status = ""')
            conn.commit()
            logger.info("✅ تم تحديث حالة جميع المستخدمين")
        except Exception as e:
            logger.warning(f"⚠️ خطأ في تحديث حالة المستخدمين: {e}")
        
        conn.commit()
        logger.info("🎉 تم إكمال تهيئة قاعدة البيانات بنجاح!")
        
    except Exception as e:
        logger.error(f"❌ خطأ في إنشاء قاعدة البيانات: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()


def add_missing_columns():
    """إضافة الأعمدة المفقودة للجداول الحالية"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        logger.info("🔍 البحث عن الأعمدة المفقودة...")
        
        # قائمة بالأعمدة التي يجب إضافتها لكل جدول
        tables_columns = {
            'users': [
                ('force_sub_left', 'INTEGER DEFAULT 0'),
                ('force_sub_left_at', 'TEXT'),
                ('force_sub_passed', 'INTEGER DEFAULT 0'),
                ('force_sub_passed_at', 'TEXT'),
                ('registered', 'INTEGER DEFAULT 0'),
                ('channel_reports', 'TEXT DEFAULT "{}"'),
                ('premium', 'BOOLEAN DEFAULT 0'),
                ('premium_until', 'TEXT'),
                ('last_daily_gift', 'TEXT'),
                ('total_daily_gifts', 'INTEGER DEFAULT 0'),
                ('streak', 'INTEGER DEFAULT 0'),
                ('wallet_balance', 'INTEGER DEFAULT 0'),
                ('total_reports', 'INTEGER DEFAULT 0'),
                ('total_orders', 'INTEGER DEFAULT 0'),
                ('total_channel_joins', 'INTEGER DEFAULT 0'),
                ('total_channel_leaves', 'INTEGER DEFAULT 0'),
                ('status', 'TEXT DEFAULT "active"'),
                ('language', 'TEXT DEFAULT "ar"'),
                ('notifications', 'BOOLEAN DEFAULT 1'),
                ('privacy_mode', 'BOOLEAN DEFAULT 0'),
                ('verification_level', 'INTEGER DEFAULT 1'),
                ('trust_score', 'INTEGER DEFAULT 50'),
                ('last_report_time', 'TEXT'),
                ('created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP')
            ]
        }
        
        for table, columns in tables_columns.items():
            try:
                # الحصول على الأعمدة الحالية
                cursor.execute(f"PRAGMA table_info({table})")
                existing_columns = [col[1] for col in cursor.fetchall()]
                
                # إضافة الأعمدة المفقودة
                for column_name, column_type in columns:
                    if column_name not in existing_columns:
                        try:
                            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}")
                            logger.info(f"✅ تم إضافة العمود {column_name} إلى جدول {table}")
                        except Exception as col_error:
                            logger.warning(f"⚠️ فشل إضافة العمود {column_name}: {col_error}")
                
            except Exception as table_error:
                logger.error(f"❌ خطأ في معالجة جدول {table}: {table_error}")
        
        conn.commit()
        conn.close()
        logger.info("✅ تم إصلاح الأعمدة المفقودة")
        
    except Exception as e:
        logger.error(f"❌ خطأ في add_missing_columns: {e}")

# أضف هذه الدوال إلى database.py

def add_missing_columns():
    """إضافة جميع الأعمدة المفقودة من تعريف init_database"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        logger.info("🔍 البحث عن جميع الأعمدة المفقودة...")
        
        # الأعمدة المذكورة في CREATE TABLE users
        users_columns_to_add = [
            ('force_sub_left', 'INTEGER DEFAULT 0'),
            ('force_sub_left_at', 'TEXT'),
            ('force_sub_passed', 'INTEGER DEFAULT 0'),
            ('force_sub_passed_at', 'TEXT'),
            ('registered', 'INTEGER DEFAULT 0'),
            ('channel_reports', 'TEXT DEFAULT "{}"'),
            ('premium', 'BOOLEAN DEFAULT 0'),
            ('premium_until', 'TEXT'),
            ('last_daily_gift', 'TEXT'),
            ('total_daily_gifts', 'INTEGER DEFAULT 0'),
            ('streak', 'INTEGER DEFAULT 0'),
            ('wallet_balance', 'INTEGER DEFAULT 0'),
            ('total_reports', 'INTEGER DEFAULT 0'),
            ('total_orders', 'INTEGER DEFAULT 0'),
            ('total_channel_joins', 'INTEGER DEFAULT 0'),
            ('total_channel_leaves', 'INTEGER DEFAULT 0'),
            ('status', 'TEXT DEFAULT "active"'),
            ('language', 'TEXT DEFAULT "ar"'),
            ('notifications', 'BOOLEAN DEFAULT 1'),
            ('privacy_mode', 'BOOLEAN DEFAULT 0'),
            ('verification_level', 'INTEGER DEFAULT 1'),
            ('trust_score', 'INTEGER DEFAULT 50'),
            ('last_report_time', 'TEXT'),
            ('created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP')
        ]
        
        # الحصول على الأعمدة الموجودة
        cursor.execute("PRAGMA table_info(users)")
        existing_columns = [col[1] for col in cursor.fetchall()]
        
        logger.info(f"📊 الأعمدة الموجودة: {len(existing_columns)}")
        
        # إضافة الأعمدة المفقودة
        added_count = 0
        for column_name, column_type in users_columns_to_add:
            if column_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE users ADD COLUMN {column_name} {column_type}")
                    logger.info(f"✅ تمت إضافة: {column_name}")
                    added_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ خطأ في {column_name}: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"✨ تم إضافة {added_count} عمود جديد")
        
        if added_count > 0:
            logger.info("🎉 تم تحديث قاعدة البيانات بنجاح!")
        else:
            logger.info("💡 جميع الأعمدة موجودة بالفعل")
        
        return added_count > 0
        
    except Exception as e:
        logger.error(f"❌ خطأ في add_all_missing_columns: {e}")
        return False

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

def get_user_data(user_id, force_reload=False):
    """دالة بديلة لجلب بيانات المستخدم"""
    try:
        from database import load_users
        
        users_data = load_users()
        user_id_str = str(user_id)
        
        if user_id_str in users_data:
            return users_data[user_id_str]
        else:
            # إنشاء بيانات افتراضية
            return {
                "user_id": user_id_str,
                "username": "",
                "first_name": "",
                "last_name": "",
                "points": 0,
                "invites": 0,
                "total_earned": 0,
                "total_spent": 0,
                "first_join": "",
                "last_active": "",
                "joined_channels": {},
                "active_subscriptions": [],
                "temp_left_channels": [],
                "permanent_left_channels": [],
                "invited_users": [],
                "total_reports": 0,
                "channel_reports": {},
                "blocked_channels_by_report": [],
                "orders": [],
                "daily_gift": {},
                "muted_until": "",
                "banned": False,
                "force_sub_passed": False,
                "force_sub_passed_at": "",
                "force_sub_left": False,
                "force_sub_left_at": "",
                "force_sub_returned_at": ""
            }
    except Exception as e:
        logger.error(f"خطأ في get_user_data البديلة: {e}")
        return {}

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
            # ✅ إنشاء مستخدم جديد في قاعدة البيانات
            default_data = create_default_user_data(user_id)
            
            # إعداد بيانات للإدراج
            columns = []
            values = []
            placeholders = []
            
            # جلب أسماء الأعمدة الصحيحة
            cursor.execute("PRAGMA table_info(users)")
            db_columns = [col[1] for col in cursor.fetchall()]
            
            for key in db_columns:
                if key in default_data:
                    columns.append(key)
                    value = default_data[key]
                    
                    # تحويل القيم المعقدة إلى JSON
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value, ensure_ascii=False)
                    
                    values.append(value)
                    placeholders.append("?")
            
            # إدراج المستخدم الجديد
            sql = f"INSERT INTO users ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            cursor.execute(sql, values)
            logger.info(f"✅ تم إنشاء مستخدم جديد: {user_id}")
            
            # جلب الصف بعد الإدراج
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
        
        # ✅ إصلاح المشكلة 1: جلب transactions بطريقة آمنة
        if transaction_id and "transactions" in columns:
            cursor.execute("SELECT transactions FROM users WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            
            transactions_json = "[]"  # القيمة الافتراضية
            
            if result and result[0]:
                try:
                    transactions_json = result[0] if result[0] else "[]"
                except:
                    transactions_json = "[]"
            
            try:
                transactions = json.loads(transactions_json)
                transactions.append({
                    "id": transaction_id,
                    "action": action_type,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "updates": updates
                })
                
                # تأكد أن المعاملات لا تتجاوز 100 معاملة (للحفاظ على الأداء)
                if len(transactions) > 100:
                    transactions = transactions[-100:]
                
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
        
        logger.info(f"✅ تم تحديث بيانات {user_id} - {action_type}")
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في update_user_data: {e}")
        import traceback
        traceback.print_exc()
        
        # ✅ إغلاق الاتصال في حالة الخطأ
        try:
            if 'conn' in locals():
                conn.close()
        except:
            pass
            
        return False

def check_and_add_registered_column():
    """التحقق من وجود عمود registered وإضافته إذا كان مفقوداً"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # التحقق من وجود العمود
        cursor.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'registered' not in columns:
            # إضافة العمود
            cursor.execute("ALTER TABLE users ADD COLUMN registered INTEGER DEFAULT 0")
            conn.commit()
            logger.info("✅ تم إضافة عمود registered إلى جدول users")
            
            # تحديث جميع المستخدمين الحاليين ليكونوا مسجلين
            cursor.execute("UPDATE users SET registered = 1 WHERE permanent_registered = 1")
            cursor.execute("UPDATE users SET registered = 1 WHERE first_join IS NOT NULL")
            conn.commit()
            logger.info("✅ تم تحديث حالة تسجيل المستخدمين الحاليين")
        
        conn.close()
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في إضافة عمود registered: {e}")
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

def create_default_user_data(user_id: str) -> Dict:
    """إنشاء بيانات مستخدم افتراضية"""
    return {
        "user_id": user_id,  # ⬅️ أضف هذا السطر المهم
        "points": 0,
        "invites": 0,
        "invited_users": [],
        "bought_channels": {},
        "joined_channels": {},
        "username": "",
        "first_name": "",
        "last_name": "",
        "first_join": None,
        "registered": False,
        "total_earned": 0,
        "total_spent": 0,
        "orders": [],
        "reports_made": 0,
        "reports_received": 0,
        "last_active": None,
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
