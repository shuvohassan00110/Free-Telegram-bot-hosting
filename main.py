# config.py - Configuration & Database
import os
import sqlite3
import json
from datetime import datetime
import hashlib

class Config:
    BOT_TOKEN = "8472500254:AAExwOsRRT37P6HHPwVT5cIrTCUh8p0J6tk"
    ADMIN_IDS = [7857957075]  # Add your admin Telegram IDs
    MAX_BOTS_PER_USER = 5
    MAX_FREE_BOTS = 3
    CHANNEL_USERNAME = "@gadgetpremiumzone"  # Your public channel
    PRIVATE_CHANNEL = "https://t.me/+HSqmdVuHFr84MzRl"
    
    # Resource Limits
    MAX_CPU_PERCENT = 50
    MAX_MEMORY_MB = 512
    MAX_STORAGE_MB = 100
    
    # Premium Features
    PREMIUM_MAX_BOTS = 15
    PREMIUM_MAX_MEMORY = 2048

class Database:
    def __init__(self):
        self.conn = sqlite3.connect('bot_hosting.db', check_same_thread=False)
        self.create_tables()
    
    def create_tables(self):
        cursor = self.conn.cursor()
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_premium INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                joined_date TEXT,
                total_bots INTEGER DEFAULT 0,
                storage_used INTEGER DEFAULT 0,
                last_active TEXT
            )
        ''')
        
        # Hosted bots table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hosted_bots (
                bot_id TEXT PRIMARY KEY,
                user_id INTEGER,
                bot_name TEXT,
                bot_type TEXT,
                file_path TEXT,
                status TEXT DEFAULT 'stopped',
                created_at TEXT,
                last_started TEXT,
                process_id INTEGER,
                logs TEXT,
                environment_vars TEXT,
                installed_modules TEXT,
                auto_restart INTEGER DEFAULT 1,
                cpu_usage REAL DEFAULT 0,
                memory_usage REAL DEFAULT 0,
                uptime INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0
            )
        ''')
        
        # Module installations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_modules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id TEXT,
                module_name TEXT,
                version TEXT,
                installed_at TEXT,
                FOREIGN KEY (bot_id) REFERENCES hosted_bots(bot_id)
            )
        ''')
        
        # Bot statistics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_stats (
                bot_id TEXT PRIMARY KEY,
                total_requests INTEGER DEFAULT 0,
                total_uptime INTEGER DEFAULT 0,
                total_restarts INTEGER DEFAULT 0,
                last_error TEXT,
                error_time TEXT
            )
        ''')
        
        # Admin logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                action TEXT,
                target_user_id INTEGER,
                target_bot_id TEXT,
                timestamp TEXT,
                details TEXT
            )
        ''')
        
        self.conn.commit()
    
    def add_user(self, user_id, username, first_name):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO users (user_id, username, first_name, joined_date, last_active)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, username, first_name, datetime.now().isoformat(), datetime.now().isoformat()))
        self.conn.commit()
    
    def get_user(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        return cursor.fetchone()
    
    def add_hosted_bot(self, bot_id, user_id, bot_name, bot_type, file_path):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO hosted_bots 
            (bot_id, user_id, bot_name, bot_type, file_path, created_at, logs, environment_vars, installed_modules)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (bot_id, user_id, bot_name, bot_type, file_path, datetime.now().isoformat(), '[]', '{}', '[]'))
        
        cursor.execute('''
            INSERT INTO bot_stats (bot_id)
            VALUES (?)
        ''', (bot_id,))
        
        self.conn.commit()
    
    def get_user_bots(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM hosted_bots WHERE user_id = ?', (user_id,))
        return cursor.fetchall()
    
    def get_bot(self, bot_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM hosted_bots WHERE bot_id = ?', (bot_id,))
        return cursor.fetchone()
    
    def update_bot_status(self, bot_id, status, process_id=None):
        cursor = self.conn.cursor()
        if process_id:
            cursor.execute('''
                UPDATE hosted_bots 
                SET status = ?, process_id = ?, last_started = ?
                WHERE bot_id = ?
            ''', (status, process_id, datetime.now().isoformat(), bot_id))
        else:
            cursor.execute('UPDATE hosted_bots SET status = ? WHERE bot_id = ?', (status, bot_id))
        self.conn.commit()
    
    def log_admin_action(self, admin_id, action, target_user_id=None, target_bot_id=None, details=None):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO admin_logs (admin_id, action, target_user_id, target_bot_id, timestamp, details)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (admin_id, action, target_user_id, target_bot_id, datetime.now().isoformat(), details))
        self.conn.commit()


# bot_validator.py - Advanced Code Validation
import ast
import re
import subprocess
import tempfile
import os

class CodeValidator:
    """Advanced Python/JS code validator with syntax checking"""
    
    @staticmethod
    def validate_python_code(code):
        """Validate Python code and return detailed error information [web:24][web:27]"""
        errors = []
        warnings = []
        
        try:
            # Parse the code into AST
            tree = ast.parse(code)
            
            # Check for dangerous imports
            dangerous_modules = ['os.system', 'subprocess', 'eval', 'exec', '__import__']
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if any(dm in alias.name for dm in dangerous_modules):
                            warnings.append(f"⚠️ Warning: Potentially dangerous import '{alias.name}' detected at line {node.lineno}")
                
                if isinstance(node, ast.ImportFrom):
                    if node.module and any(dm in node.module for dm in dangerous_modules):
                        warnings.append(f"⚠️ Warning: Potentially dangerous import from '{node.module}' at line {node.lineno}")
            
            # Syntax validation successful
            return {
                'valid': True,
                'errors': errors,
                'warnings': warnings,
                'message': '✅ Code validation successful!'
            }
            
        except SyntaxError as e:
            return {
                'valid': False,
                'errors': [{
                    'line': e.lineno,
                    'offset': e.offset,
                    'message': e.msg,
                    'text': e.text
                }],
                'warnings': warnings,
                'message': f'❌ Syntax Error at line {e.lineno}: {e.msg}'
            }
        except Exception as e:
            return {
                'valid': False,
                'errors': [{'message': str(e)}],
                'warnings': warnings,
                'message': f'❌ Validation Error: {str(e)}'
            }
    
    @staticmethod
    def validate_javascript_code(code):
        """Validate JavaScript code using Node.js"""
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False) as f:
                f.write(code)
                temp_file = f.name
            
            # Check syntax using node
            result = subprocess.run(
                ['node', '--check', temp_file],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            os.unlink(temp_file)
            
            if result.returncode == 0:
                return {
                    'valid': True,
                    'errors': [],
                    'warnings': [],
                    'message': '✅ JavaScript code validation successful!'
                }
            else:
                return {
                    'valid': False,
                    'errors': [{'message': result.stderr}],
                    'warnings': [],
                    'message': f'❌ JavaScript Syntax Error: {result.stderr}'
                }
        except FileNotFoundError:
            return {
                'valid': False,
                'errors': [{'message': 'Node.js not installed'}],
                'warnings': [],
                'message': '❌ Node.js is required for JavaScript validation'
            }
        except Exception as e:
            return {
                'valid': False,
                'errors': [{'message': str(e)}],
                'warnings': [],
                'message': f'❌ Validation Error: {str(e)}'
            }
    
    @staticmethod
    def extract_requirements(code):
        """Extract required modules from Python code"""
        imports = set()
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.add(alias.name.split('.')[0])
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        imports.add(node.module.split('.')[0])
        except:
            pass
        
        # Filter out standard library modules
        stdlib_modules = {'os', 'sys', 'json', 'time', 'datetime', 're', 'random', 'math'}
        return list(imports - stdlib_modules)


# bot_manager.py - Bot Process Management
import subprocess
import psutil
import signal
import threading
import time
from pathlib import Path

class BotManager:
    """Manage bot processes with resource monitoring [web:29][web:34]"""
    
    def __init__(self, db):
        self.db = db
        self.processes = {}
        self.monitoring_thread = threading.Thread(target=self._monitor_processes, daemon=True)
        self.monitoring_thread.start()
    
    def start_bot(self, bot_id, file_path, bot_type='python'):
        """Start a bot process in isolated environment"""
        try:
            # Prepare command based on bot type
            if bot_type == 'python':
                cmd = ['python3', file_path]
            elif bot_type == 'javascript':
                cmd = ['node', file_path]
            else:
                return {'success': False, 'message': 'Unsupported bot type'}
            
            # Start process with resource limits
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=os.path.dirname(file_path)
            )
            
            # Store process info
            self.processes[bot_id] = {
                'process': process,
                'start_time': time.time(),
                'file_path': file_path
            }
            
            # Update database
            self.db.update_bot_status(bot_id, 'running', process.pid)
            
            return {
                'success': True,
                'message': '✅ Bot started successfully!',
                'pid': process.pid
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f'❌ Failed to start bot: {str(e)}'
            }
    
    def stop_bot(self, bot_id):
        """Stop a running bot process"""
        try:
            if bot_id in self.processes:
                process_info = self.processes[bot_id]
                process = process_info['process']
                
                # Gracefully terminate
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()  # Force kill if not responding
                
                del self.processes[bot_id]
                self.db.update_bot_status(bot_id, 'stopped')
                
                return {'success': True, 'message': '✅ Bot stopped successfully!'}
            else:
                return {'success': False, 'message': '❌ Bot is not running'}
        except Exception as e:
            return {'success': False, 'message': f'❌ Error stopping bot: {str(e)}'}
    
    def restart_bot(self, bot_id):
        """Restart a bot"""
        bot = self.db.get_bot(bot_id)
        if bot:
            self.stop_bot(bot_id)
            time.sleep(1)
            return self.start_bot(bot_id, bot[4], bot[3])
        return {'success': False, 'message': '❌ Bot not found'}
    
    def get_bot_logs(self, bot_id, lines=50):
        """Get bot process logs"""
        if bot_id in self.processes:
            process = self.processes[bot_id]['process']
            try:
                stdout, stderr = process.communicate(timeout=0.1)
                logs = stdout.decode() + stderr.decode()
                return logs.split('\n')[-lines:]
            except:
                return ['No logs available']
        return ['Bot is not running']
    
    def get_bot_stats(self, bot_id):
        """Get resource usage statistics"""
        if bot_id in self.processes:
            process = self.processes[bot_id]['process']
            try:
                ps = psutil.Process(process.pid)
                uptime = time.time() - self.processes[bot_id]['start_time']
                
                return {
                    'cpu_percent': ps.cpu_percent(interval=0.1),
                    'memory_mb': ps.memory_info().rss / 1024 / 1024,
                    'uptime_seconds': int(uptime),
                    'status': ps.status()
                }
            except:
                return None
        return None
    
    def _monitor_processes(self):
        """Background thread to monitor all bot processes"""
        while True:
            try:
                for bot_id in list(self.processes.keys()):
                    process_info = self.processes[bot_id]
                    process = process_info['process']
                    
                    # Check if process is still alive
                    if process.poll() is not None:
                        # Process died, check auto-restart
                        bot = self.db.get_bot(bot_id)
                        if bot and bot[13]:  # auto_restart enabled
                            print(f"Auto-restarting bot {bot_id}")
                            time.sleep(2)
                            self.start_bot(bot_id, bot[4], bot[3])
                        else:
                            del self.processes[bot_id]
                            self.db.update_bot_status(bot_id, 'stopped')
                
                time.sleep(10)  # Check every 10 seconds
            except Exception as e:
                print(f"Monitor error: {e}")
                time.sleep(10)
    
    def install_module(self, bot_id, module_name):
        """Install a Python module for specific bot"""
        try:
            result = subprocess.run(
                ['pip3', 'install', module_name],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                # Log installation
                cursor = self.db.conn.cursor()
                cursor.execute('''
                    INSERT INTO bot_modules (bot_id, module_name, installed_at)
                    VALUES (?, ?, ?)
                ''', (bot_id, module_name, datetime.now().isoformat()))
                self.db.conn.commit()
                
                return {
                    'success': True,
                    'message': f'✅ Module "{module_name}" installed successfully!',
                    'output': result.stdout
                }
            else:
                return {
                    'success': False,
                    'message': f'❌ Failed to install "{module_name}"',
                    'error': result.stderr
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'❌ Installation error: {str(e)}'
            }


# main_bot.py - Main Telegram Bot with Advanced UI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
import zipfile
import shutil
import uuid

db = Database()
validator = CodeValidator()
bot_manager = BotManager(db)

# Beautiful UI Templates
class BotUI:
    @staticmethod
    def main_menu_keyboard(is_admin=False):
        keyboard = [
            [KeyboardButton("🤖 My Bots"), KeyboardButton("➕ Upload Bot")],
            [KeyboardButton("📊 Statistics"), KeyboardButton("⚙️ Settings")],
            [KeyboardButton("📚 Documentation"), KeyboardButton("💎 Premium")]
        ]
        if is_admin:
            keyboard.append([KeyboardButton("👑 Admin Panel")])
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    @staticmethod
    def bot_actions_keyboard(bot_id):
        keyboard = [
            [InlineKeyboardButton("▶️ Start", callback_data=f"start_{bot_id}"),
             InlineKeyboardButton("⏸ Stop", callback_data=f"stop_{bot_id}")],
            [InlineKeyboardButton("🔄 Restart", callback_data=f"restart_{bot_id}"),
             InlineKeyboardButton("📊 Stats", callback_data=f"stats_{bot_id}")],
            [InlineKeyboardButton("📝 Logs", callback_data=f"logs_{bot_id}"),
             InlineKeyboardButton("📦 Modules", callback_data=f"modules_{bot_id}")],
            [InlineKeyboardButton("⚙️ Settings", callback_data=f"settings_{bot_id}"),
             InlineKeyboardButton("🗑 Delete", callback_data=f"delete_{bot_id}")],
            [InlineKeyboardButton("🔙 Back to List", callback_data="my_bots")]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def admin_panel_keyboard():
        keyboard = [
            [InlineKeyboardButton("👥 User Management", callback_data="admin_users"),
             InlineKeyboardButton("🤖 All Bots", callback_data="admin_bots")],
            [InlineKeyboardButton("📊 System Stats", callback_data="admin_stats"),
             InlineKeyboardButton("📜 Logs", callback_data="admin_logs")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"),
             InlineKeyboardButton("⚙️ System Settings", callback_data="admin_settings")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ]
        return InlineKeyboardMarkup(keyboard)

# Bot Handlers
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.add_user(user.id, user.username, user.first_name)
    
    is_admin = user.id in Config.ADMIN_IDS
    
    welcome_message = f"""
╔═══════════════════════════════╗
║  🚀 **GADGET BOT HOSTING**  🚀  ║
╚═══════════════════════════════╝

👋 Welcome **{user.first_name}**!

🌟 **Most Advanced Bot Hosting Platform**

✨ **Premium Features:**
├ 🐍 Host Python & JavaScript Bots
├ 📦 Automated Module Installation  
├ 🔍 Real-time Syntax Validation
├ 📊 Advanced Resource Monitoring
├ 📝 Live Logs & Error Tracking
├ 🔄 Auto-Restart on Crash
├ ⚡ Lightning-Fast Performance
└ 💎 Premium Quality Design

📱 **Channels:**
🔸 Premium: {Config.CHANNEL_USERNAME}
🔸 MOD APK: {Config.PRIVATE_CHANNEL}

🎯 **Get Started:**
Tap "➕ Upload Bot" to host your first bot!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Powered by 𝙂𝘼𝘿𝙂𝙀𝙏 𝑷𝑹𝑬𝑴𝑰𝑼𝑴 𝒁𝑶𝑵𝑬
    """
    
    await update.message.reply_text(
        welcome_message,
        reply_markup=BotUI.main_menu_keyboard(is_admin),
        parse_mode='Markdown'
    )

async def handle_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle bot file uploads with validation [web:1][web:19]"""
    user_id = update.effective_user.id
    user_bots = db.get_user_bots(user_id)
    
    # Check bot limit
    user = db.get_user(user_id)
    max_bots = Config.PREMIUM_MAX_BOTS if user[2] else Config.MAX_FREE_BOTS
    
    if len(user_bots) >= max_bots:
        await update.message.reply_text(
            f"❌ You've reached your bot limit ({max_bots} bots).\n"
            f"💎 Upgrade to Premium for {Config.PREMIUM_MAX_BOTS} bots!"
        )
        return
    
    progress_msg = await update.message.reply_text("⏳ Processing your bot file...")
    
    try:
        file = await update.message.document.get_file()
        file_name = update.message.document.file_name
        bot_id = str(uuid.uuid4())[:8]
        
        # Create bot directory
        bot_dir = Path(f"bots/{user_id}/{bot_id}")
        bot_dir.mkdir(parents=True, exist_ok=True)
        
        # Download file
        file_path = bot_dir / file_name
        await file.download_to_drive(file_path)
        
        # Handle ZIP archives
        if file_name.endswith('.zip'):
            await progress_msg.edit_text("📦 Extracting ZIP archive...")
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(bot_dir)
            file_path.unlink()
            
            # Find main file
            py_files = list(bot_dir.glob('*.py'))
            js_files = list(bot_dir.glob('*.js'))
            
            if py_files:
                file_path = py_files[0]
                bot_type = 'python'
            elif js_files:
                file_path = js_files[0]
                bot_type = 'javascript'
            else:
                await progress_msg.edit_text("❌ No Python or JavaScript file found in ZIP!")
                shutil.rmtree(bot_dir)
                return
        else:
            if file_name.endswith('.py'):
                bot_type = 'python'
            elif file_name.endswith('.js'):
                bot_type = 'javascript'
            else:
                await progress_msg.edit_text("❌ Unsupported file type! Use .py, .js, or .zip")
                shutil.rmtree(bot_dir)
                return
        
        # Validate code
        await progress_msg.edit_text("🔍 Validating code syntax...")
        
        with open(file_path, 'r') as f:
            code = f.read()
        
        if bot_type == 'python':
            validation = validator.validate_python_code(code)
        else:
            validation = validator.validate_javascript_code(code)
        
        # Build response
        response = f"📄 **Bot: {file_name}**\n\n"
        
        if validation['valid']:
            response += "✅ **Validation: PASSED**\n\n"
            
            if validation['warnings']:
                response += "⚠️ **Warnings:**\n"
                for warn in validation['warnings']:
                    response += f"└ {warn}\n"
                response += "\n"
            
            # Extract and suggest modules
            if bot_type == 'python':
                required_modules = validator.extract_requirements(code)
                if required_modules:
                    response += "📦 **Detected Modules:**\n"
                    response += "".join([f"└ `{mod}`\n" for mod in required_modules])
                    response += "\n💡 Install modules using: `/install <bot_id> <module_name>`\n\n"
            
            # Save to database
            db.add_hosted_bot(bot_id, user_id, file_name, bot_type, str(file_path))
            
            response += f"🎉 **Bot ID:** `{bot_id}`\n"
            response += f"📂 **Type:** {bot_type.title()}\n"
            response += f"💾 **Size:** {file_path.stat().st_size / 1024:.2f} KB\n\n"
            response += "🚀 Use the button below to manage your bot!"
            
            await progress_msg.edit_text(
                response,
                reply_markup=BotUI.bot_actions_keyboard(bot_id),
                parse_mode='Markdown'
            )
        else:
            response += "❌ **Validation: FAILED**\n\n"
            response += "**Errors Found:**\n"
            
            for error in validation['errors']:
                if 'line' in error:
                    response += f"📍 Line {error['line']}, Column {error.get('offset', 'N/A')}\n"
                    response += f"└ {error['message']}\n"
                    if error.get('text'):
                        response += f"└ `{error['text'].strip()}`\n"
                else:
                    response += f"└ {error['message']}\n"
                response += "\n"
            
            response += "🔧 **Fix these errors and upload again!**"
            
            # Clean up failed upload
            shutil.rmtree(bot_dir)
            
            await progress_msg.edit_text(response, parse_mode='Markdown')
            
    except Exception as e:
        await progress_msg.edit_text(f"❌ Upload error: {str(e)}")

async def my_bots_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's hosted bots"""
    user_id = update.effective_user.id
    bots = db.get_user_bots(user_id)
    
    if not bots:
        await update.message.reply_text(
            "📭 You don't have any hosted bots yet!\n\n"
            "➕ Tap 'Upload Bot' to get started!"
        )
        return
    
    response = "🤖 **Your Hosted Bots:**\n\n"
    
    for bot in bots:
        bot_id, _, bot_name, bot_type, _, status, created_at, *_ = bot
        
        status_emoji = {
            'running': '🟢',
            'stopped': '🔴',
            'error': '⚠️'
        }.get(status, '⚪')
        
        response += f"{status_emoji} **{bot_name}**\n"
        response += f"├ ID: `{bot_id}`\n"
        response += f"├ Type: {bot_type.title()}\n"
        response += f"├ Status: {status.upper()}\n"
        response += f"└ Created: {created_at[:10]}\n\n"
    
    keyboard = [[InlineKeyboardButton(f"{bot[5]} {bot[2]}", callback_data=f"bot_{bot[0]}")] for bot in bots]
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_main")])
    
    await update.message.reply_text(
        response,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def install_module_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Install module for a bot: /install <bot_id> <module_name>"""
    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ **Usage:** `/install <bot_id> <module_name>`\n\n"
            "**Example:** `/install abc123 aiogram`",
            parse_mode='Markdown'
        )
        return
    
    bot_id = context.args[0]
    module_name = context.args[1]
    
    # Verify bot ownership
    bot = db.get_bot(bot_id)
    if not bot or bot[1] != update.effective_user.id:
        await update.message.reply_text("❌ Bot not found or you don't have permission!")
        return
    
    progress = await update.message.reply_text(f"📦 Installing `{module_name}`...")
    
    result = bot_manager.install_module(bot_id, module_name)
    
    await progress.edit_text(
        result['message'] + ("\n\n```\n" + result.get('output', result.get('error', ''))[:500] + "\n```" if result.get('output') or result.get('error') else ""),
        parse_mode='Markdown'
    )

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all inline keyboard callbacks"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data.startswith("start_"):
        bot_id = data.split("_")[1]
        bot = db.get_bot(bot_id)
        result = bot_manager.start_bot(bot_id, bot[4], bot[3])
        await query.edit_message_text(result['message'])
        
    elif data.startswith("stop_"):
        bot_id = data.split("_")[1]
        result = bot_manager.stop_bot(bot_id)
        await query.edit_message_text(result['message'])
        
    elif data.startswith("restart_"):
        bot_id = data.split("_")[1]
        result = bot_manager.restart_bot(bot_id)
        await query.edit_message_text(result['message'])
        
    elif data.startswith("stats_"):
        bot_id = data.split("_")[1]
        stats = bot_manager.get_bot_stats(bot_id)
        
        if stats:
            message = f"""
📊 **Bot Statistics**

🖥 **CPU Usage:** {stats['cpu_percent']:.1f}%
💾 **Memory:** {stats['memory_mb']:.2f} MB
⏱ **Uptime:** {stats['uptime_seconds'] // 3600}h {(stats['uptime_seconds'] % 3600) // 60}m
📍 **Status:** {stats['status']}
            """
        else:
            message = "❌ Bot is not running!"
        
        await query.edit_message_text(message, parse_mode='Markdown')
        
    elif data.startswith("logs_"):
        bot_id = data.split("_")[1]
        logs = bot_manager.get_bot_logs(bot_id, lines=20)
        
        log_text = "📝 **Recent Logs:**\n\n```\n" + "\n".join(logs[-20:]) + "\n```"
        await query.edit_message_text(log_text[:4000], parse_mode='Markdown')
        
    elif data.startswith("bot_"):
        bot_id = data.split("_")[1]
        bot = db.get_bot(bot_id)
        
        if bot:
            stats = bot_manager.get_bot_stats(bot_id)
            
            message = f"""
🤖 **{bot[2]}**

📋 **Bot ID:** `{bot[0]}`
📂 **Type:** {bot[3].title()}
📍 **Status:** {bot[5].upper()}

🕐 **Created:** {bot[6][:10]}
🔄 **Auto-Restart:** {'✅ Enabled' if bot[13] else '❌ Disabled'}
            """
            
            if stats:
                message += f"\n💻 **CPU:** {stats['cpu_percent']:.1f}%"
                message += f"\n💾 **RAM:** {stats['memory_mb']:.2f} MB"
            
            await query.edit_message_text(
                message,
                reply_markup=BotUI.bot_actions_keyboard(bot_id),
                parse_mode='Markdown'
            )

# Admin Commands
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Advanced admin panel [web:5]"""
    if update.effective_user.id not in Config.ADMIN_IDS:
        await update.message.reply_text("❌ You don't have admin access!")
        return
    
    cursor = db.conn.cursor()
    
    # Get statistics
    cursor.execute('SELECT COUNT(*) FROM users')
    total_users = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM hosted_bots')
    total_bots = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM hosted_bots WHERE status='running'")
    running_bots = cursor.fetchone()[0]
    
    message = f"""
👑 **ADMIN CONTROL PANEL**

━━━━━━━━━━━━━━━━━━━━━━━━
📊 **System Statistics:**

👥 Total Users: **{total_users}**
🤖 Total Bots: **{total_bots}**
🟢 Running Bots: **{running_bots}**
🔴 Stopped Bots: **{total_bots - running_bots}**

━━━━━━━━━━━━━━━━━━━━━━━━
⚡ System Status: **ONLINE**
    """
    
    await update.message.reply_text(
        message,
        reply_markup=BotUI.admin_panel_keyboard(),
        parse_mode='Markdown'
    )

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast message to all users"""
    if update.effective_user.id not in Config.ADMIN_IDS:
        return
    
    if len(context.args) == 0:
        await update.message.reply_text("❌ Usage: /broadcast <message>")
        return
    
    message = ' '.join(context.args)
    cursor = db.conn.cursor()
    cursor.execute('SELECT user_id FROM users')
    users = cursor.fetchall()
    
    success = 0
    failed = 0
    
    for user in users:
        try:
            await context.bot.send_message(chat_id=user[0], text=message, parse_mode='Markdown')
            success += 1
        except:
            failed += 1
    
    await update.message.reply_text(f"📢 Broadcast complete!\n✅ Success: {success}\n❌ Failed: {failed}")

# Main function
def main():
    """Start the bot"""
    print("🚀 Starting GADGET Bot Hosting Platform...")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    application = Application.builder().token(Config.BOT_TOKEN).build()
    
    # Command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("mybots", my_bots_command))
    application.add_handler(CommandHandler("install", install_module_command))
    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    
    # Message handlers
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file_upload))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u, c: u.message.reply_text("Please use the menu buttons!")))
    
    # Callback handler
    application.add_handler(CallbackQueryHandler(callback_handler))
    
    print("✅ Bot is running!")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    # Start bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
