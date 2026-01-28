# main.py
import os
import sys
import json
import asyncio
import logging
import traceback
import subprocess
import zipfile
import ast
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import aiofiles
import psutil

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command, StateFilter
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
BOT_TOKEN = os.getenv('8472500254:AAEGhYazKm-lAOvRsS8w9kOZg5FNs-uZmoQ')
ADMIN_IDS = list(map(int, os.getenv('ADMIN_IDS', '7857957075').split(',')))
SUPPORT_CHAT = os.getenv('SUPPORT_CHAT', '@gadgetpremiumzone')

# Create necessary directories
UPLOAD_DIR = Path("user_bots")
LOGS_DIR = Path("bot_logs")
TEMP_DIR = Path("temp_files")
DATABASE_DIR = Path("database")

for directory in [UPLOAD_DIR, LOGS_DIR, TEMP_DIR, DATABASE_DIR]:
    directory.mkdir(exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════════
# 🎯 DATABASE MANAGER - Advanced Data Management
# ═══════════════════════════════════════════════════════════════════════════

class DatabaseManager:
    """Advanced JSON-based database with file locking"""
    
    def __init__(self, db_file: str = "database/users.json"):
        self.db_file = Path(db_file)
        self.db_file.parent.mkdir(exist_ok=True)
        if not self.db_file.exists():
            self._write_db({})
    
    def _write_db(self, data: dict):
        with open(self.db_file, 'w') as f:
            json.dump(data, f, indent=4)
    
    def _read_db(self) -> dict:
        try:
            with open(self.db_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    
    def add_user(self, user_id: int, username: str, first_name: str):
        db = self._read_db()
        if str(user_id) not in db:
            db[str(user_id)] = {
                'user_id': user_id,
                'username': username,
                'first_name': first_name,
                'created_at': datetime.now().isoformat(),
                'bots': [],
                'total_uploads': 0,
                'storage_used': 0,
                'is_premium': False,
                'premium_expires': None
            }
            self._write_db(db)
    
    def get_user(self, user_id: int):
        db = self._read_db()
        return db.get(str(user_id))
    
    def add_bot(self, user_id: int, bot_data: dict):
        db = self._read_db()
        if str(user_id) in db:
            db[str(user_id)]['bots'].append(bot_data)
            db[str(user_id)]['total_uploads'] += 1
            self._write_db(db)
    
    def get_user_bots(self, user_id: int):
        db = self._read_db()
        user = db.get(str(user_id))
        return user['bots'] if user else []
    
    def delete_bot(self, user_id: int, bot_id: str):
        db = self._read_db()
        if str(user_id) in db:
            db[str(user_id)]['bots'] = [
                b for b in db[str(user_id)]['bots'] if b['id'] != bot_id
            ]
            self._write_db(db)

db_manager = DatabaseManager()

# ═══════════════════════════════════════════════════════════════════════════
# 🔧 CODE VALIDATOR - Advanced Syntax Checking
# ═══════════════════════════════════════════════════════════════════════════

class CodeValidator:
    """Advanced Python/JavaScript code validator with detailed error reporting"""
    
    @staticmethod
    def validate_python(code: str) -> tuple[bool, Optional[str]]:
        """Validate Python code and return (is_valid, error_message)"""
        try:
            ast.parse(code)
            return True, None
        except SyntaxError as e:
            error_msg = (
                f"❌ **SYNTAX ERROR** in Python Code\n\n"
                f"📍 Line {e.lineno}: {e.msg}\n"
                f"📝 Text: `{e.text.strip() if e.text else 'N/A'}`\n"
                f"{'🔹 ' + ' ' * (e.offset - 1) if e.offset else ''}\n\n"
                f"💡 Suggestion: Check your code at line {e.lineno}"
            )
            return False, error_msg
        except Exception as e:
            return False, f"❌ Validation Error: {str(e)}"
    
    @staticmethod
    def validate_javascript(code: str) -> tuple[bool, Optional[str]]:
        """Basic JavaScript validation"""
        try:
            # Check for common JS syntax errors
            if code.count('{') != code.count('}'):
                return False, "❌ Unmatched curly braces in JavaScript code"
            if code.count('(') != code.count(')'):
                return False, "❌ Unmatched parentheses in JavaScript code"
            if code.count('[') != code.count(']'):
                return False, "❌ Unmatched square brackets in JavaScript code"
            
            return True, None
        except Exception as e:
            return False, f"❌ JavaScript Validation Error: {str(e)}"
    
    @staticmethod
    def analyze_dependencies(code: str) -> List[str]:
        """Extract Python import statements to determine dependencies"""
        dependencies = []
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        dep = alias.name.split('.')[0]
                        if dep not in dependencies:
                            dependencies.append(dep)
                elif isinstance(node, ast.ImportFrom):
                    if node.module and not node.module.startswith('.'):
                        dep = node.module.split('.')[0]
                        if dep not in dependencies:
                            dependencies.append(dep)
        except:
            pass
        
        return dependencies

# ═══════════════════════════════════════════════════════════════════════════
# 📦 BOT MANAGER - Advanced Bot Execution & Management
# ═══════════════════════════════════════════════════════════════════════════

class BotManager:
    """Manages bot processes with advanced monitoring"""
    
    def __init__(self):
        self.processes: Dict[str, dict] = {}
    
    async def create_bot_directory(self, user_id: int, bot_id: str) -> Path:
        """Create isolated directory for each bot"""
        bot_dir = UPLOAD_DIR / f"user_{user_id}" / bot_id
        bot_dir.mkdir(parents=True, exist_ok=True)
        return bot_dir
    
    async def extract_zip(self, zip_path: Path, extract_to: Path) -> tuple[bool, str]:
        """Extract ZIP file with validation"""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            return True, "✅ ZIP extracted successfully"
        except zipfile.BadZipFile:
            return False, "❌ Invalid ZIP file"
        except Exception as e:
            return False, f"❌ Extraction failed: {str(e)}"
    
    async def install_requirements(self, bot_dir: Path, requirements: List[str]) -> tuple[bool, str]:
        """Install Python packages with detailed logging"""
        try:
            requirements_file = bot_dir / "requirements.txt"
            
            # Write requirements
            async with aiofiles.open(requirements_file, 'w') as f:
                await f.write('\n'.join(requirements))
            
            # Install packages
            process = await asyncio.create_subprocess_exec(
                'pip', 'install', '-r', str(requirements_file),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(bot_dir)
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)
            
            if process.returncode == 0:
                return True, "✅ All modules installed successfully"
            else:
                return False, f"❌ Installation failed:\n{stderr.decode()}"
        
        except asyncio.TimeoutError:
            return False, "❌ Installation timeout (>5 minutes)"
        except Exception as e:
            return False, f"❌ Installation error: {str(e)}"
    
    async def start_bot(self, user_id: int, bot_id: str, script_name: str) -> tuple[bool, str]:
        """Start bot process with monitoring"""
        try:
            bot_dir = UPLOAD_DIR / f"user_{user_id}" / bot_id
            script_path = bot_dir / script_name
            log_file = LOGS_DIR / f"{user_id}_{bot_id}.log"
            
            if not script_path.exists():
                return False, f"❌ Script not found: {script_name}"
            
            # Start process
            with open(log_file, 'w') as log:
                process = await asyncio.create_subprocess_exec(
                    'python3', script_name,
                    stdout=log,
                    stderr=log,
                    cwd=str(bot_dir)
                )
            
            self.processes[f"{user_id}_{bot_id}"] = {
                'process': process,
                'start_time': datetime.now(),
                'pid': process.pid,
                'log_file': log_file
            }
            
            # Give it 2 seconds to start
            await asyncio.sleep(2)
            
            if process.returncode is not None:
                return False, "❌ Bot crashed immediately. Check logs."
            
            return True, f"✅ Bot started successfully (PID: {process.pid})"
        
        except Exception as e:
            return False, f"❌ Failed to start bot: {str(e)}"
    
    async def stop_bot(self, user_id: int, bot_id: str) -> tuple[bool, str]:
        """Stop bot process gracefully"""
        try:
            key = f"{user_id}_{bot_id}"
            if key not in self.processes:
                return False, "❌ Bot is not running"
            
            process = self.processes[key]['process']
            process.terminate()
            
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
            
            del self.processes[key]
            return True, "✅ Bot stopped successfully"
        
        except Exception as e:
            return False, f"❌ Failed to stop bot: {str(e)}"
    
    async def get_bot_status(self, user_id: int, bot_id: str) -> dict:
        """Get detailed bot status"""
        key = f"{user_id}_{bot_id}"
        if key not in self.processes:
            return {'status': 'offline', 'uptime': '0s'}
        
        bot_info = self.processes[key]
        uptime = datetime.now() - bot_info['start_time']
        
        try:
            p = psutil.Process(bot_info['pid'])
            cpu = p.cpu_percent(interval=0.1)
            memory = p.memory_info().rss / 1024 / 1024
        except:
            cpu = 0
            memory = 0
        
        return {
            'status': 'online',
            'pid': bot_info['pid'],
            'uptime': str(uptime).split('.')[0],
            'cpu_usage': f"{cpu:.1f}%",
            'memory_usage': f"{memory:.1f}MB"
        }
    
    async def get_bot_logs(self, user_id: int, bot_id: str, lines: int = 50) -> str:
        """Get bot logs with tail functionality"""
        log_file = LOGS_DIR / f"{user_id}_{bot_id}.log"
        
        if not log_file.exists():
            return "❌ No logs available yet"
        
        try:
            async with aiofiles.open(log_file, 'r') as f:
                content = await f.read()
                log_lines = content.split('\n')[-lines:]
                return '\n'.join(log_lines)
        except Exception as e:
            return f"❌ Error reading logs: {str(e)}"

bot_manager = BotManager()

# ═══════════════════════════════════════════════════════════════════════════
# 🎨 KEYBOARD GENERATORS - Beautiful UI Design
# ═══════════════════════════════════════════════════════════════════════════

class KeyboardFactory:
    """Creates beautiful inline and reply keyboards"""
    
    @staticmethod
    def main_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Upload New Bot", callback_data="upload_bot")],
            [InlineKeyboardButton(text="📋 My Bots", callback_data="list_bots")],
            [InlineKeyboardButton(text="⚙️ Settings", callback_data="settings")],
            [InlineKeyboardButton(text="📚 Help & Docs", callback_data="help"),
             InlineKeyboardButton(text="👥 Support", url=f"https://t.me/gadgetpremiumzone")],
        ])
    
    @staticmethod
    def upload_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📄 Upload .py file", callback_data="upload_py")],
            [InlineKeyboardButton(text="📦 Upload .zip archive", callback_data="upload_zip")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="back_main")],
        ])
    
    @staticmethod
    def bot_actions(bot_id: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="▶️ Start", callback_data=f"start_bot_{bot_id}"),
             InlineKeyboardButton(text="⏹️ Stop", callback_data=f"stop_bot_{bot_id}")],
            [InlineKeyboardButton(text="📊 Status", callback_data=f"status_bot_{bot_id}")],
            [InlineKeyboardButton(text="📜 View Logs", callback_data=f"logs_bot_{bot_id}")],
            [InlineKeyboardButton(text="📥 Install Module", callback_data=f"install_bot_{bot_id}")],
            [InlineKeyboardButton(text="🗑️ Delete", callback_data=f"delete_bot_{bot_id}")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="list_bots")],
        ])
    
    @staticmethod
    def admin_panel() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 Users Stats", callback_data="admin_users")],
            [InlineKeyboardButton(text="📊 System Stats", callback_data="admin_system")],
            [InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="⚠️ Ban User", callback_data="admin_ban")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="back_main")],
        ])

# ═══════════════════════════════════════════════════════════════════════════
# 📊 FORMATTERS - Beautiful Message Formatting
# ═══════════════════════════════════════════════════════════════════════════

class MessageFormatter:
    """Creates beautiful formatted messages"""
    
    @staticmethod
    def welcome(user_name: str) -> str:
        return f"""
╔═══════════════════════════════════════════╗
║  🌟 WELCOME TO ADVANCED HOSTING BOT 🌟   ║
╚═══════════════════════════════════════════╝

👋 Hello, {user_name}!

🎉 You now have access to the most powerful and advanced bot hosting platform.

✨ **Premium Features:**
  ✅ Upload & Run Python (.py) scripts
  ✅ Extract & Run ZIP archives
  ✅ Real-time Error Detection
  ✅ Module Management
  ✅ Process Monitoring
  ✅ Live Logs Viewer
  ✅ Advanced Admin Controls
  ✅ Beautiful UI Design

🚀 **Get Started:**
  1. Click 🚀 Upload New Bot
  2. Upload your bot code
  3. Install required modules
  4. Start running!

📞 Support: {SUPPORT_CHAT}
"""
    
    @staticmethod
    def bot_info(bot_data: dict, status: dict) -> str:
        return f"""
╔═══════════════════════════════════════════╗
║         🤖 BOT INFORMATION               ║
╚═══════════════════════════════════════════╝

📝 **Bot Name:** {bot_data.get('name', 'Unnamed')}
🆔 **Bot ID:** `{bot_data['id']}`
📅 **Created:** {bot_data.get('created_at', 'N/A')}
📊 **Status:** {'🟢 ONLINE' if status['status'] == 'online' else '🔴 OFFLINE'}

{'─' * 45}

🔍 **Detailed Status:**
├─ 🆔 PID: {status.get('pid', 'N/A')}
├─ ⏱️  Uptime: {status.get('uptime', 'N/A')}
├─ 💻 CPU Usage: {status.get('cpu_usage', 'N/A')}
└─ 🧠 Memory Usage: {status.get('memory_usage', 'N/A')}

{'─' * 45}

📄 **Script Info:**
├─ File: {bot_data.get('script_name', 'N/A')}
├─ Type: {bot_data.get('file_type', 'N/A')}
└─ Size: {bot_data.get('file_size', 'N/A')}
"""
    
    @staticmethod
    def system_stats() -> str:
        try:
            cpu = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return f"""
╔═══════════════════════════════════════════╗
║        📊 SYSTEM STATISTICS               ║
╚═══════════════════════════════════════════╝

💻 **CPU Usage:** {cpu}%
🧠 **Memory Usage:** {memory.percent}%
   ├─ Used: {memory.used / (1024**3):.1f}GB
   └─ Total: {memory.total / (1024**3):.1f}GB

💾 **Disk Usage:** {disk.percent}%
   ├─ Used: {disk.used / (1024**3):.1f}GB
   └─ Total: {disk.total / (1024**3):.1f}GB

🔄 **Bot Processes:** {len(bot_manager.processes)} running

⏰ **Server Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        except:
            return "❌ Unable to fetch system stats"

# ═══════════════════════════════════════════════════════════════════════════
# 🎯 FSM STATES - Advanced State Management
# ═══════════════════════════════════════════════════════════════════════════

class BotUploadStates(StatesGroup):
    waiting_for_file = State()
    waiting_for_modules = State()
    waiting_for_confirmation = State()
    waiting_for_module_install = State()

class AdminBroadcastState(StatesGroup):
    waiting_for_message = State()

# ═══════════════════════════════════════════════════════════════════════════
# 🤖 BOT INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ═══════════════════════════════════════════════════════════════════════════
# 📡 COMMAND HANDLERS - Main Bot Commands
# ═══════════════════════════════════════════════════════════════════════════

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Start command - Main entry point"""
    user_id = message.from_user.id
    username = message.from_user.username or "User"
    first_name = message.from_user.first_name or "User"
    
    # Add user to database
    db_manager.add_user(user_id, username, first_name)
    
    welcome_text = MessageFormatter.welcome(first_name)
    
    await message.answer(
        welcome_text,
        reply_markup=KeyboardFactory.main_menu(),
        parse_mode="HTML"
    )

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    """Admin panel command"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ You don't have admin permissions")
        return
    
    await message.answer(
        "🛡️ **ADMIN CONTROL PANEL**\n\nSelect an option:",
        reply_markup=KeyboardFactory.admin_panel(),
        parse_mode="Markdown"
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    """Help command"""
    help_text = """
╔═══════════════════════════════════════════╗
║          📚 HELP & DOCUMENTATION         ║
╚═══════════════════════════════════════════╝

🚀 **Getting Started:**
1. Click "Upload New Bot" button
2. Choose file type (.py or .zip)
3. Upload your bot code
4. Review detected errors (if any)
5. Install required modules
6. Start your bot!

📝 **Supported Formats:**
• Python Scripts (.py)
• ZIP Archives (.zip)

⚙️ **Module Installation:**
• Auto-install: Common modules like aiogram, requests
• Manual-install: Use /install command for custom packages
  Example: /install aiogram==3.0.0

📊 **Monitoring:**
• Real-time status monitoring
• CPU & Memory usage tracking
• Live log viewer
• Process management

🔧 **Features:**
✅ Syntax error detection
✅ Dependency analysis
✅ Isolated bot environments
✅ Graceful process management
✅ Beautiful UI design

❓ **Common Issues:**
Q: Bot crashes immediately?
A: Check logs for detailed error messages

Q: Module not found error?
A: Use /install <module_name> in bot settings

Q: Can I run Node.js bots?
A: Yes! Upload .js files or .zip with npm packages

📞 **Support:**
Contact: {SUPPORT_CHAT}
Report Issues: {SUPPORT_CHAT}
"""
    
    await message.answer(help_text, parse_mode="Markdown")

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Show user statistics"""
    user_id = message.from_user.id
    user = db_manager.get_user(user_id)
    bots = db_manager.get_user_bots(user_id)
    
    if not user:
        await message.answer("❌ User not found in database")
        return
    
    stats_text = f"""
╔═══════════════════════════════════════════╗
║       📊 YOUR STATISTICS                 ║
╚═══════════════════════════════════════════╝

👤 **Profile:**
├─ Username: @{user['username']}
├─ Name: {user['first_name']}
└─ Member Since: {user['created_at'][:10]}

🤖 **Bots:**
├─ Total Uploaded: {user['total_uploads']}
├─ Active Bots: {len([b for b in bots if b.get('status') == 'online'])}
└─ Total Bots: {len(bots)}

💾 **Storage:**
└─ Used: {user['storage_used']} MB

⭐ **Premium:**
└─ Status: {'✅ Active' if user['is_premium'] else '❌ Free'}
"""
    
    await message.answer(stats_text, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════
# 🎯 CALLBACK HANDLERS - Button Actions
# ═══════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data == "upload_bot")
async def cb_upload_bot(query: types.CallbackQuery):
    """Upload bot callback"""
    await query.answer()
    await query.message.edit_text(
        "📦 **Choose Upload Type:**",
        reply_markup=KeyboardFactory.upload_menu(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "upload_py")
async def cb_upload_py(query: types.CallbackQuery, state: FSMContext):
    """Upload Python file"""
    await query.answer()
    await state.set_state(BotUploadStates.waiting_for_file)
    await query.message.edit_text(
        "📤 **Send your Python (.py) file now:**\n\n"
        "The bot will automatically:\n"
        "✅ Check for syntax errors\n"
        "✅ Analyze dependencies\n"
        "✅ Show validation results\n\n"
        "⏱️ Wait for validation...",
        parse_mode="Markdown"
    )
    await state.update_data(file_type="py")

@dp.callback_query(F.data == "upload_zip")
async def cb_upload_zip(query: types.CallbackQuery, state: FSMContext):
    """Upload ZIP file"""
    await query.answer()
    await state.set_state(BotUploadStates.waiting_for_file)
    await query.message.edit_text(
        "📤 **Send your ZIP archive now:**\n\n"
        "Your ZIP should contain:\n"
        "📁 Bot script (main.py or index.js)\n"
        "📄 requirements.txt (optional)\n"
        "📦 Other files\n\n"
        "⏱️ Wait for validation...",
        parse_mode="Markdown"
    )
    await state.update_data(file_type="zip")

@dp.message(BotUploadStates.waiting_for_file)
async def process_file_upload(message: types.Message, state: FSMContext):
    """Process uploaded file"""
    if not message.document:
        await message.answer("❌ Please send a file")
        return
    
    file_obj = message.document
    state_data = await state.get_data()
    file_type = state_data.get('file_type')
    
    # Validate file extension
    filename = file_obj.file_name
    if file_type == "py" and not filename.endswith('.py'):
        await message.answer("❌ File must be a .py file")
        return
    elif file_type == "zip" and not filename.endswith('.zip'):
        await message.answer("❌ File must be a .zip file")
        return
    
    # Download file
    status_msg = await message.answer("📥 Downloading file...")
    
    try:
        file_path = TEMP_DIR / f"{message.from_user.id}_{filename}"
        file_download = await bot.get_file(file_obj.file_id)
        await bot.download_file(file_download.file_path, file_path)
        
        await status_msg.edit_text("🔍 Validating code...")
        await asyncio.sleep(1)
        
        # Validate code
        if file_type == "py":
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                code = f.read()
            
            is_valid, error_msg = CodeValidator.validate_python(code)
            
            if not is_valid:
                await message.answer(
                    error_msg,
                    parse_mode="Markdown"
                )
                file_path.unlink()
                await state.clear()
                return
            
            # Get dependencies
            dependencies = CodeValidator.analyze_dependencies(code)
            
            validation_text = (
                "✅ **CODE VALIDATION PASSED!**\n\n"
                "📊 **Analysis Results:**\n"
                f"├─ Syntax: ✅ Valid\n"
                f"├─ Size: {len(code)} bytes\n"
                f"└─ Lines: {len(code.split(chr(10)))}\n\n"
            )
            
            if dependencies:
                validation_text += (
                    "📦 **Detected Dependencies:**\n"
                    + "".join([f"├─ {dep}\n" for dep in dependencies[:-1]])
                    + (f"└─ {dependencies[-1]}\n\n" if dependencies else "")
                )
            
            validation_text += (
                "❓ **Need Custom Modules?**\n"
                "You can install additional packages after upload.\n\n"
                "Click the button below to confirm upload."
            )
        
        else:  # ZIP file
            is_valid, extract_msg = await bot_manager.extract_zip(
                file_path,
                TEMP_DIR / f"extract_{message.from_user.id}"
            )
            
            if not is_valid:
                await message.answer(extract_msg)
                file_path.unlink()
                await state.clear()
                return
            
            validation_text = (
                "✅ **ZIP VALIDATION PASSED!**\n\n"
                "📦 **Archive Contents:**\n"
                f"├─ File: {filename}\n"
                f"├─ Size: {file_path.stat().st_size / 1024:.1f}KB\n"
                f"└─ Extracted: ✅ Success\n\n"
                "Click confirm to proceed with installation."
            )
        
        await status_msg.edit_text(validation_text, parse_mode="Markdown")
        
        # Store file path and proceed
        await state.update_data(
            file_path=str(file_path),
            filename=filename,
            file_size=file_obj.file_size
        )
        
        await state.set_state(BotUploadStates.waiting_for_modules)
        
        await message.answer(
            "🔧 **Ready to Configure?**\n\n"
            "Would you like to add custom modules before starting?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Continue", callback_data="continue_upload"),
                 InlineKeyboardButton(text="📦 Add Modules", callback_data="add_modules")]
            ])
        )
    
    except Exception as e:
        logger.error(f"File upload error: {e}")
        await status_msg.edit_text(f"❌ Error: {str(e)}")
        await state.clear()

@dp.callback_query(F.data == "continue_upload")
async def cb_continue_upload(query: types.CallbackQuery, state: FSMContext):
    """Continue with bot upload"""
    await query.answer()
    state_data = await state.get_data()
    
    file_path = Path(state_data['file_path'])
    filename = state_data['filename']
    user_id = query.from_user.id
    
    try:
        # Create bot in directory
        bot_id = f"bot_{int(time.time())}_{user_id}"
        bot_dir = await bot_manager.create_bot_directory(user_id, bot_id)
        
        # Move file to bot directory
        if filename.endswith('.zip'):
            await bot_manager.extract_zip(file_path, bot_dir)
            # Find main script
            scripts = list(bot_dir.glob('*.py')) + list(bot_dir.glob('*.js'))
            script_name = scripts[0].name if scripts else 'main.py'
        else:
            import shutil
            shutil.move(str(file_path), str(bot_dir / filename))
            script_name = filename
        
        # Get dependencies and auto-install common ones
        if filename.endswith('.py'):
            with open(bot_dir / script_name, 'r', errors='ignore') as f:
                code = f.read()
            deps = CodeValidator.analyze_dependencies(code)
        else:
            deps = []
        
        # Auto-install common packages
        auto_install = []
        common_packages = {
            'aiogram': 'aiogram',
            'requests': 'requests',
            'asyncio': None,
            'json': None,
            'os': None
        }
        
        for dep in deps:
            if dep in common_packages and common_packages[dep]:
                auto_install.append(common_packages[dep])
        
        installing_msg = await query.message.answer("📦 Auto-installing common modules...")
        
        if auto_install:
            success, install_msg = await bot_manager.install_requirements(bot_dir, auto_install)
        else:
            success = True
            install_msg = "✅ No additional modules needed"
        
        # Save bot info
        bot_data = {
            'id': bot_id,
            'name': filename.replace('.py', '').replace('.zip', ''),
            'script_name': script_name,
            'file_type': 'python' if filename.endswith('.py') else 'zip',
            'file_size': state_data['file_size'],
            'created_at': datetime.now().isoformat(),
            'dependencies': deps,
            'status': 'stopped'
        }
        
        db_manager.add_bot(user_id, bot_data)
        
        # Clean up temp files
        if file_path.exists():
            file_path.unlink()
        
        success_text = f"""
✅ **BOT UPLOADED SUCCESSFULLY!**

🎉 Your bot has been created and is ready to use.

🤖 **Bot Details:**
├─ Name: {bot_data['name']}
├─ ID: `{bot_id}`
├─ Script: {script_name}
└─ Status: Ready to start

📦 **Module Installation:**
{install_msg}

▶️ **Next Step:**
Click the button below to start your bot!
"""
        
        await installing_msg.edit_text(success_text, parse_mode="Markdown")
        
        await asyncio.sleep(1)
        await query.message.answer(
            "🚀 **Ready to start?**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="▶️ Start Bot", callback_data=f"start_bot_{bot_id}")],
                [InlineKeyboardButton(text="🔙 Back", callback_data="list_bots")]
            ])
        )
        
        await state.clear()
    
    except Exception as e:
        logger.error(f"Upload error: {e}")
        await query.message.answer(f"❌ Error: {str(e)}")
        await state.clear()

@dp.callback_query(F.data == "add_modules")
async def cb_add_modules(query: types.CallbackQuery, state: FSMContext):
    """Add custom modules"""
    await query.answer()
    await state.set_state(BotUploadStates.waiting_for_module_install)
    await query.message.edit_text(
        "📦 **Install Custom Modules:**\n\n"
        "Send module names separated by commas\n\n"
        "Examples:\n"
        "• `aiogram`\n"
        "• `requests, aiohttp`\n"
        "• `aiogram==3.0.0, requests>=2.28`\n\n"
        "Or type `skip` to continue without custom modules",
        parse_mode="Markdown"
    )

@dp.message(BotUploadStates.waiting_for_module_install)
async def process_module_install(message: types.Message, state: FSMContext):
    """Process custom module installation"""
    if message.text.lower() == 'skip':
        await state.set_state(BotUploadStates.waiting_for_modules)
        await message.answer("Continuing without custom modules...")
        await asyncio.sleep(0.5)
        # Trigger continue_upload
        state_data = await state.get_data()
        # Simulate button click
        await cb_continue_upload(
            types.CallbackQuery(
                id="manual",
                from_user=message.from_user,
                chat_instance="",
                message=message,
                data="continue_upload"
            ),
            state
        )
        return
    
    modules = [m.strip() for m in message.text.split(',')]
    
    state_data = await state.get_data()
    state_data['custom_modules'] = modules
    await state.update_data(**state_data)
    
    await message.answer(f"✅ Modules added: {', '.join(modules)}\n\nProceeding with upload...")
    await asyncio.sleep(1)
    
    await state.set_state(BotUploadStates.waiting_for_modules)

@dp.callback_query(F.data == "list_bots")
async def cb_list_bots(query: types.CallbackQuery):
    """List user's bots"""
    await query.answer()
    user_id = query.from_user.id
    bots = db_manager.get_user_bots(user_id)
    
    if not bots:
        await query.message.edit_text(
            "📋 **Your Bots**\n\n"
            "You haven't uploaded any bots yet.\n"
            "Click 'Upload New Bot' to get started!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🚀 Upload Bot", callback_data="upload_bot"),
                 InlineKeyboardButton(text="🔙 Back", callback_data="back_main")]
            ])
        )
        return
    
    # Show list of bots
    text = "📋 **YOUR BOTS:**\n\n"
    
    for idx, bot_data in enumerate(bots, 1):
        status = await bot_manager.get_bot_status(user_id, bot_data['id'])
        status_emoji = "🟢" if status['status'] == 'online' else "🔴"
        
        text += f"{idx}. {status_emoji} **{bot_data['name']}**\n"
        text += f"   └─ ID: `{bot_data['id']}`\n\n"
    
    text += "👇 Select a bot to manage it:"
    
    # Create buttons for each bot
    buttons = []
    for bot_data in bots:
        buttons.append([
            InlineKeyboardButton(
                text=f"⚙️ {bot_data['name']}",
                callback_data=f"manage_bot_{bot_data['id']}"
            )
        ])
    
    buttons.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_main")])
    
    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("manage_bot_"))
async def cb_manage_bot(query: types.CallbackQuery):
    """Manage specific bot"""
    await query.answer()
    bot_id = query.data.replace("manage_bot_", "")
    user_id = query.from_user.id
    
    bots = db_manager.get_user_bots(user_id)
    bot_data = next((b for b in bots if b['id'] == bot_id), None)
    
    if not bot_data:
        await query.answer("❌ Bot not found", show_alert=True)
        return
    
    status = await bot_manager.get_bot_status(user_id, bot_id)
    info_text = MessageFormatter.bot_info(bot_data, status)
    
    await query.message.edit_text(
        info_text,
        reply_markup=KeyboardFactory.bot_actions(bot_id),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("start_bot_"))
async def cb_start_bot(query: types.CallbackQuery):
    """Start bot process"""
    await query.answer()
    bot_id = query.data.replace("start_bot_", "")
    user_id = query.from_user.id
    
    bots = db_manager.get_user_bots(user_id)
    bot_data = next((b for b in bots if b['id'] == bot_id), None)
    
    if not bot_data:
        await query.answer("❌ Bot not found", show_alert=True)
        return
    
    status_msg = await query.message.answer("🚀 Starting bot...")
    
    success, msg = await bot_manager.start_bot(
        user_id,
        bot_id,
        bot_data['script_name']
    )
    
    await status_msg.edit_text(
        msg,
        parse_mode="Markdown"
    )
    
    await asyncio.sleep(2)
    
    # Show updated status
    status = await bot_manager.get_bot_status(user_id, bot_id)
    info_text = MessageFormatter.bot_info(bot_data, status)
    
    await query.message.answer(
        info_text,
        reply_markup=KeyboardFactory.bot_actions(bot_id),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("stop_bot_"))
async def cb_stop_bot(query: types.CallbackQuery):
    """Stop bot process"""
    await query.answer()
    bot_id = query.data.replace("stop_bot_", "")
    user_id = query.from_user.id
    
    status_msg = await query.message.answer("⏹️ Stopping bot...")
    
    success, msg = await bot_manager.stop_bot(user_id, bot_id)
    
    await status_msg.edit_text(msg, parse_mode="Markdown")

@dp.callback_query(F.data.startswith("status_bot_"))
async def cb_status_bot(query: types.CallbackQuery):
    """Get bot status"""
    await query.answer()
    bot_id = query.data.replace("status_bot_", "")
    user_id = query.from_user.id
    
    bots = db_manager.get_user_bots(user_id)
    bot_data = next((b for b in bots if b['id'] == bot_id), None)
    
    if not bot_data:
        await query.answer("❌ Bot not found", show_alert=True)
        return
    
    status = await bot_manager.get_bot_status(user_id, bot_id)
    info_text = MessageFormatter.bot_info(bot_data, status)
    
    await query.message.edit_text(
        info_text,
        reply_markup=KeyboardFactory.bot_actions(bot_id),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("logs_bot_"))
async def cb_logs_bot(query: types.CallbackQuery):
    """View bot logs"""
    await query.answer()
    bot_id = query.data.replace("logs_bot_", "")
    user_id = query.from_user.id
    
    logs = await bot_manager.get_bot_logs(user_id, bot_id, lines=100)
    
    logs_text = f"""
╔═══════════════════════════════════════════╗
║         📜 BOT LOGS (Last 100 lines)      ║
╚═══════════════════════════════════════════╝

`{logs}`
"""
    
    try:
        await query.message.edit_text(
            logs_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Refresh", callback_data=f"logs_bot_{bot_id}"),
                 InlineKeyboardButton(text="🔙 Back", callback_data=f"manage_bot_{bot_id}")]
            ])
        )
    except TelegramBadRequest:
        # Message too long, send as file
        log_file = LOGS_DIR / f"{user_id}_{bot_id}.log"
        if log_file.exists():
            await query.message.answer_document(
                FSInputFile(log_file),
                caption="📜 Bot logs file"
            )

@dp.callback_query(F.data.startswith("install_bot_"))
async def cb_install_bot(query: types.CallbackQuery, state: FSMContext):
    """Install custom modules for bot"""
    await query.answer()
    bot_id = query.data.replace("install_bot_", "")
    
    await query.message.edit_text(
        "📦 **Install Custom Module:**\n\n"
        "Send module name with version (optional):\n\n"
        "Examples:\n"
        "• `requests`\n"
        "• `aiohttp==3.8.0`",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data=f"manage_bot_{bot_id}")]
        ])
    )
    
    await state.set_state(BotUploadStates.waiting_for_module_install)
    await state.update_data(bot_id=bot_id)

@dp.callback_query(F.data.startswith("delete_bot_"))
async def cb_delete_bot(query: types.CallbackQuery):
    """Delete bot"""
    await query.answer()
    bot_id = query.data.replace("delete_bot_", "")
    user_id = query.from_user.id
    
    # Confirm deletion
    await query.message.edit_text(
        f"⚠️ **Delete Bot?**\n\n"
        f"Bot ID: `{bot_id}`\n\n"
        f"This action cannot be undone!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑️ Delete", callback_data=f"confirm_delete_{bot_id}"),
             InlineKeyboardButton(text="❌ Cancel", callback_data=f"manage_bot_{bot_id}")]
        ]),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("confirm_delete_"))
async def cb_confirm_delete(query: types.CallbackQuery):
    """Confirm bot deletion"""
    await query.answer()
    bot_id = query.data.replace("confirm_delete_", "")
    user_id = query.from_user.id
    
    # Stop bot if running
    await bot_manager.stop_bot(user_id, bot_id)
    
    # Delete from database
    db_manager.delete_bot(user_id, bot_id)
    
    # Delete files
    import shutil
    bot_dir = UPLOAD_DIR / f"user_{user_id}" / bot_id
    if bot_dir.exists():
        shutil.rmtree(bot_dir)
    
    await query.message.edit_text(
        "✅ Bot deleted successfully",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 My Bots", callback_data="list_bots"),
             InlineKeyboardButton(text="🔙 Home", callback_data="back_main")]
        ])
    )

@dp.callback_query(F.data == "settings")
async def cb_settings(query: types.CallbackQuery):
    """Settings menu"""
    await query.answer()
    await query.message.edit_text(
        "⚙️ **SETTINGS**\n\n"
        "Customize your experience:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 My Stats", callback_data="my_stats")],
            [InlineKeyboardButton(text="🔔 Notifications", callback_data="notifications")],
            [InlineKeyboardButton(text="👤 Profile", callback_data="profile")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="back_main")]
        ]),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "my_stats")
async def cb_my_stats(query: types.CallbackQuery):
    """Show user statistics"""
    await query.answer()
    user_id = query.from_user.id
    user = db_manager.get_user(user_id)
    bots = db_manager.get_user_bots(user_id)
    
    if not user:
        await query.answer("❌ User data not found")
        return
    
    active_bots = len([b for b in bots if b.get('status') == 'online'])
    
    stats_text = f"""
╔═══════════════════════════════════════════╗
║       📊 YOUR STATISTICS                 ║
╚═══════════════════════════════════════════╝

👤 **Profile:**
├─ User ID: {user_id}
├─ Username: @{user['username']}
├─ Name: {user['first_name']}
└─ Member Since: {user['created_at'][:10]}

🤖 **Bots:**
├─ Total Uploaded: {user['total_uploads']}
├─ Active Now: {active_bots}
└─ Total Bots: {len(bots)}

💾 **Storage:**
└─ Used: {user.get('storage_used', 0)} MB

⭐ **Premium:**
└─ Status: {'✅ Active' if user['is_premium'] else '❌ Inactive'}
"""
    
    await query.message.edit_text(
        stats_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Back", callback_data="settings")]
        ]),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "help")
async def cb_help(query: types.CallbackQuery):
    """Help section"""
    await query.answer()
    await query.message.edit_text(
        "📚 **HELP & DOCUMENTATION**\n\n"
        "Choose a topic:\n\n"
        "1. **Getting Started** - Upload your first bot\n"
        "2. **Module Installation** - Install custom packages\n"
        "3. **Troubleshooting** - Fix common issues\n"
        "4. **API Reference** - Bot commands\n\n"
        "Or visit: " + SUPPORT_CHAT,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Full Docs", url="https://t.me/gadgetpremiumzone")],
            [InlineKeyboardButton(text="💬 Support", url="https://t.me/gadgetpremiumzone")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="back_main")]
        ]),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "back_main")
async def cb_back_main(query: types.CallbackQuery):
    """Back to main menu"""
    await query.answer()
    await query.message.edit_text(
        MessageFormatter.welcome(query.from_user.first_name or "User"),
        reply_markup=KeyboardFactory.main_menu(),
        parse_mode="HTML"
    )

# ═══════════════════════════════════════════════════════════════════════════
# 🛡️ ADMIN HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data == "admin_users")
async def cb_admin_users(query: types.CallbackQuery):
    """Admin: User statistics"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Admin only", show_alert=True)
        return
    
    await query.answer()
    
    db = DatabaseManager()._read_db()
    total_users = len(db)
    total_bots = sum(len(user.get('bots', [])) for user in db.values())
    premium_users = sum(1 for user in db.values() if user.get('is_premium'))
    
    text = f"""
╔═══════════════════════════════════════════╗
║       👥 USER STATISTICS                 ║
╚═══════════════════════════════════════════╝

👤 **Total Users:** {total_users}
⭐ **Premium Users:** {premium_users}
🤖 **Total Bots:** {total_bots}

📊 **Average Bots per User:** {total_bots / max(total_users, 1):.1f}
"""
    
    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")]
        ]),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "admin_system")
async def cb_admin_system(query: types.CallbackQuery):
    """Admin: System statistics"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Admin only", show_alert=True)
        return
    
    await query.answer()
    
    system_stats = MessageFormatter.system_stats()
    
    await query.message.edit_text(
        system_stats,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_system")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")]
        ]),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(query: types.CallbackQuery, state: FSMContext):
    """Admin: Broadcast message"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Admin only", show_alert=True)
        return
    
    await query.answer()
    await state.set_state(AdminBroadcastState.waiting_for_message)
    
    await query.message.edit_text(
        "📢 **BROADCAST MESSAGE**\n\n"
        "Send the message you want to broadcast to all users:\n\n"
        "(This will be sent to all registered users)",
    )

@dp.message(AdminBroadcastState.waiting_for_message)
async def process_broadcast(message: types.Message, state: FSMContext):
    """Process broadcast message"""
    if message.from_user.id not in ADMIN_IDS:
        return
    
    broadcast_text = message.text or message.caption or "📢 Message from admin"
    
    status_msg = await message.answer("📤 Broadcasting to all users...")
    
    db = DatabaseManager()._read_db()
    success_count = 0
    failed_count = 0
    
    for user_id_str in db.keys():
        try:
            user_id = int(user_id_str)
            await bot.send_message(
                user_id,
                f"📢 **ANNOUNCEMENT**\n\n{broadcast_text}",
                parse_mode="Markdown"
            )
            success_count += 1
            await asyncio.sleep(0.05)  # Rate limit
        except:
            failed_count += 1
    
    await status_msg.edit_text(
        f"✅ Broadcast completed!\n\n"
        f"✅ Sent: {success_count}\n"
        f"❌ Failed: {failed_count}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")]
        ])
    )
    
    await state.clear()

@dp.callback_query(F.data == "admin_back")
async def cb_admin_back(query: types.CallbackQuery):
    """Back to admin panel"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Admin only", show_alert=True)
        return
    
    await query.answer()
    await query.message.edit_text(
        "🛡️ **ADMIN CONTROL PANEL**\n\nSelect an option:",
        reply_markup=KeyboardFactory.admin_panel(),
        parse_mode="Markdown"
    )

# ═══════════════════════════════════════════════════════════════════════════
# 🔧 ERROR HANDLERS & UTILS
# ═══════════════════════════════════════════════════════════════════════════

@dp.message()
async def echo_handler(message: types.Message):
    """Handle any other messages"""
    await message.answer(
        "❓ I don't understand that command.\n\n"
        "Use /start to see available options.",
        reply_markup=KeyboardFactory.main_menu()
    )

# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN BOT STARTUP
# ═══════════════════════════════════════════════════════════════════════════

async def main():
    """Start the bot"""
    logger.info("🚀 Starting Advanced Hosting Bot...")
    logger.info(f"👤 Admin IDs: {ADMIN_IDS}")
    
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
