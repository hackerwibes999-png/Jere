import asyncio, os, re, shutil, signal, sqlite3, subprocess, sys, time, uuid, json, logging, stat, threading, hashlib, socket, urllib.request
from pathlib import Path
from datetime import datetime, timezone
import psutil
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web, ClientSession
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile, LabeledPrice, PreCheckoutQuery

load_dotenv()
OWNER_ID=8178629769
OWNER_USERNAME='@dexycpm'
ADMIN_ID=OWNER_ID
UPI_ID='adithyan0o7@fam'
FREE_SLOTS=1
# Separate subscriptions for Bot Hosting and Website Hosting.
# Diamond VIP purchased in either shop unlocks BOTH services.
BOT_PLANS={
    'FREE':     {'slots':1,  'inr':0,   'stars':0,   'token':True, 'admin':True, 'autofix':False,'autorestart':False,'source':False,'priority':0},
    'BRONZE':   {'slots':3,  'inr':29,  'stars':20,  'token':True, 'admin':True, 'autofix':False,'autorestart':False,'source':False,'priority':1},
    'SILVER':   {'slots':5,  'inr':60,  'stars':40,  'token':True, 'admin':True, 'autofix':True, 'autorestart':True, 'source':False,'priority':2},
    'GOLD':     {'slots':8,  'inr':120, 'stars':60,  'token':True, 'admin':True, 'autofix':True, 'autorestart':True, 'source':True, 'priority':3},
    'PLATINUM': {'slots':10, 'inr':170, 'stars':80,  'token':True, 'admin':True, 'autofix':True, 'autorestart':True, 'source':True, 'priority':4},
    'DIAMOND':  {'slots':50, 'inr':300, 'stars':160, 'token':True, 'admin':True, 'autofix':True, 'autorestart':True, 'source':True, 'priority':5},
}
# Website plans intentionally cheaper than Bot Hosting plans.
WEB_PLANS={
    'FREE':     {'slots':1,  'inr':0,   'stars':0,   'priority':0},
    'BRONZE':   {'slots':2,  'inr':15,  'stars':10,  'priority':1},
    'SILVER':   {'slots':3,  'inr':30,  'stars':20,  'priority':2},
    'GOLD':     {'slots':4,  'inr':50,  'stars':30,  'priority':3},
    'PLATINUM': {'slots':5,  'inr':75,  'stars':45,  'priority':4},
    'DIAMOND':  {'slots':10, 'inr':150, 'stars':90,  'priority':5},
}
PLAN_ORDER=['FREE','BRONZE','SILVER','GOLD','PLATINUM','DIAMOND']
PLAN_EMOJI={'FREE':'🆓','BRONZE':'🟤','SILVER':'⚪','GOLD':'🟡','PLATINUM':'🔵','DIAMOND':'💠'}
# Backward compatibility for older code paths. Bot plans are the default feature set.
PLANS=BOT_PLANS
PREMIUM_PLANS=[(v['slots'],v['inr'],v['stars'],k) for k,v in BOT_PLANS.items() if k!='FREE']
BOT_TOKEN=os.getenv('BOT_TOKEN','').strip() or '8808200620:AAH-hqR1e_vpdwQJ8MnKr4-e3LMA4KRi5WA'
BASE=Path(__file__).resolve().parent
QR_FILE=BASE/'assets'/'owner_qr.jpg'
LOGO_FILE=BASE/'assets'/'xenora_logo.png'
DATA=BASE/'data'; HOSTED=BASE/'hosted_bots'; PENDING=BASE/'pending'; LOGS=BASE/'logs'; WEBS=BASE/'hosted_websites'; DB=DATA/'xenora.db'
WEB_PORT=int(os.getenv('PORT',os.getenv('XENORA_WEB_PORT','8080')))
WEB_HOST=os.getenv('XENORA_WEB_HOST','0.0.0.0')
PUBLIC_BASE=os.getenv('XENORA_PUBLIC_BASE_URL','').rstrip('/')
WEBSITE_MAX_MB=int(os.getenv('XENORA_MAX_WEBSITE_MB','2048'))
for d in (DATA,HOSTED,PENDING,LOGS,WEBS): d.mkdir(parents=True,exist_ok=True)
if not BOT_TOKEN: raise RuntimeError('BOT_TOKEN is required for the XENORA hosting bot')
logging.basicConfig(level=logging.INFO,format='%(asctime)s | %(levelname)s | %(message)s')
log=logging.getLogger('xenora')
bot=Bot(BOT_TOKEN); dp=Dispatcher(storage=MemoryStorage()); rtr=Router(); dp.include_router(rtr)

async def safe_canswer(c, text=None, *, show_alert=False):
    """Best-effort callback acknowledgement; never let a stale/duplicate ACK break a handler."""
    try:
        if text is None:
            await c.answer()
        else:
            await c.answer(text, show_alert=show_alert)
    except Exception:
        pass

class ImmediateCallbackAckMiddleware:
    async def __call__(self, handler, event, data):
        # Telegram callback queries expire quickly. ACK before any DB, file,
        # subprocess, network, or permission check so the button never spins
        # while a heavy operation is running. Individual handlers may update
        # the callback text later through safe_canswer().
        await safe_canswer(event)
        return await handler(event, data)

rtr.callback_query.middleware(ImmediateCallbackAckMiddleware())
processes={}
deploy_tasks=set()
website_processes={}
website_server=None

# Central background-task supervisor. Every long-running operation is tracked and
# unexpected exceptions are logged instead of becoming silent 'no response' bugs.
def spawn_background(coro, label='background'):
    task=asyncio.create_task(coro)
    deploy_tasks.add(task)
    def _done(t):
        deploy_tasks.discard(t)
        if t.cancelled():
            return
        try:
            exc=t.exception()
        except Exception as e:
            log.error('Task %s exception retrieval failed: %s',label,e)
            return
        if exc:
            log.error('Background task %s failed: %s',label,exc,exc_info=(type(exc),exc,exc.__traceback__))
    task.add_done_callback(_done)
    return task

# Process operations are serialized so a fast double-tap cannot race stop/start.
PROCESS_LOCK=threading.RLock()

# ---------------- database ----------------
# SQLite is used by both the Telegram event loop and background deployment
# workers. Serialize database sections in-process, keep connections short-lived,
# enable WAL only once, and give
# SQLite enough time to wait for a transient writer instead of failing with
# 'database is locked'.
SQLITE_TIMEOUT=float(os.getenv('XENORA_SQLITE_TIMEOUT','60'))
SQLITE_BUSY_TIMEOUT_MS=int(os.getenv('XENORA_SQLITE_BUSY_TIMEOUT_MS','60000'))
DB_LOCK=threading.RLock()

def conn():
    c=sqlite3.connect(DB, timeout=SQLITE_TIMEOUT)
    c.row_factory=sqlite3.Row
    c.execute(f'PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}')
    c.execute('PRAGMA foreign_keys=ON')
    return c

def _sqlite_retry(fn, attempts=8):
    last=None
    for i in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            last=e
            if 'locked' not in str(e).lower() and 'busy' not in str(e).lower():
                raise
            time.sleep(min(0.25*(2**i),4.0))
    raise last

def now(): return datetime.now(timezone.utc).isoformat()
def init_db():
    # Only initialization changes the journal mode. Doing this on every
    # connection was the source of avoidable lock contention during deploys.
    with DB_LOCK, conn() as c:
        c.execute('PRAGMA journal_mode=WAL')
        c.execute('PRAGMA synchronous=NORMAL')
        c.execute('PRAGMA wal_autocheckpoint=1000')
        c.executescript('''
        CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY,username TEXT,full_name TEXT,premium INTEGER DEFAULT 0,created_at TEXT,last_seen TEXT,premium_slots INTEGER DEFAULT 0,plan TEXT DEFAULT 'FREE',bot_plan TEXT DEFAULT 'FREE',web_plan TEXT DEFAULT 'FREE',banned INTEGER DEFAULT 0,kicked INTEGER DEFAULT 0,started INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS admins(user_id INTEGER PRIMARY KEY,added_by INTEGER,created_at TEXT);
        CREATE TABLE IF NOT EXISTS bots(id TEXT PRIMARY KEY,owner_id INTEGER,name TEXT,username TEXT,token TEXT,admin_id INTEGER,source_name TEXT,runtime TEXT,entrypoint TEXT,status TEXT,pid INTEGER,last_error TEXT,created_at TEXT,updated_at TEXT,priority INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS pending(id TEXT PRIMARY KEY,owner_id INTEGER,token TEXT,source_name TEXT,file_path TEXT,bot_name TEXT,bot_username TEXT,status TEXT,created_at TEXT);
        CREATE TABLE IF NOT EXISTS websites(id TEXT PRIMARY KEY,owner_id INTEGER,name TEXT,source_name TEXT,runtime TEXT,entrypoint TEXT,status TEXT,pid INTEGER,port INTEGER,root TEXT,last_error TEXT,created_at TEXT,updated_at TEXT);
        CREATE TABLE IF NOT EXISTS payments(id TEXT PRIMARY KEY,user_id INTEGER,method TEXT,amount INTEGER,status TEXT,created_at TEXT);
        CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,action TEXT,detail TEXT,created_at TEXT);
        ''')
        cols={r[1] for r in c.execute('PRAGMA table_info(users)').fetchall()}
        if 'premium_slots' not in cols:
            c.execute('ALTER TABLE users ADD COLUMN premium_slots INTEGER DEFAULT 0')
        if 'plan' not in cols:
            c.execute("ALTER TABLE users ADD COLUMN plan TEXT DEFAULT 'FREE'")
        if 'bot_plan' not in cols:
            c.execute("ALTER TABLE users ADD COLUMN bot_plan TEXT DEFAULT 'FREE'")
        if 'web_plan' not in cols:
            c.execute("ALTER TABLE users ADD COLUMN web_plan TEXT DEFAULT 'FREE'")
        if 'banned' not in cols:
            c.execute("ALTER TABLE users ADD COLUMN banned INTEGER DEFAULT 0")
        if 'kicked' not in cols:
            c.execute("ALTER TABLE users ADD COLUMN kicked INTEGER DEFAULT 0")
        if 'started' not in cols:
            c.execute("ALTER TABLE users ADD COLUMN started INTEGER DEFAULT 0")
        # Indexes keep dashboards/admin lists responsive as the service grows.
        c.execute('CREATE INDEX IF NOT EXISTS idx_bots_owner ON bots(owner_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_bots_status ON bots(status)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_websites_owner ON websites(owner_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_pending_owner ON pending(owner_id)')
        bcols={r[1] for r in c.execute('PRAGMA table_info(bots)').fetchall()}
        if 'priority' not in bcols:
            c.execute("ALTER TABLE bots ADD COLUMN priority INTEGER DEFAULT 0")
        # Migrate the old single-subscription system into Bot Hosting only.
        # A legacy Diamond is special: it unlocks both services.
        for row in c.execute("SELECT user_id,premium,premium_slots,plan,bot_plan,web_plan FROM users").fetchall():
            bp=(row['bot_plan'] or 'FREE').upper()
            wp=(row['web_plan'] or 'FREE').upper()
            if bp=='FREE' and row['premium']:
                slots=int(row['premium_slots'] or 0)
                legacy='DIAMOND' if slots>=50 else 'PLATINUM' if slots>=10 else 'GOLD' if slots>=8 else 'SILVER' if slots>=5 else 'BRONZE' if slots>=3 else 'FREE'
                bp=legacy
                if legacy=='DIAMOND': wp='DIAMOND'
            c.execute("UPDATE users SET bot_plan=?,web_plan=? WHERE user_id=?",(bp if bp in BOT_PLANS else 'FREE',wp if wp in WEB_PLANS else 'FREE',row['user_id']))
def _db_exec(sql,args=()):
    with DB_LOCK, conn() as c:
        c.execute(sql,args)

def audit(uid,action,detail=''):
    def write():
        with DB_LOCK, conn() as c:
            c.execute('INSERT INTO audit(user_id,action,detail,created_at) VALUES(?,?,?,?)',(uid,action,detail[:1000],now()))
    # Audit logging must never hold up a Telegram handler when SQLite is busy.
    try:
        loop=asyncio.get_running_loop()
    except RuntimeError:
        return write()
    task=loop.create_task(asyncio.to_thread(write))
    if 'deploy_tasks' in globals():
        deploy_tasks.add(task)
        def _audit_done(t):
            deploy_tasks.discard(t)
            if not t.cancelled():
                try:
                    e=t.exception()
                    if e: log.error('audit write failed: %s',e)
                except Exception as e: log.error('audit task check failed: %s',e)
        task.add_done_callback(_audit_done)
def upsert_user(u):
    t=now()
    with DB_LOCK, conn() as c: c.execute('''INSERT INTO users(user_id,username,full_name,premium,created_at,last_seen,premium_slots)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,full_name=excluded.full_name,last_seen=excluded.last_seen,started=1''',
                  (u.id,u.username or '',u.full_name or '',0,t,t,0))
ADMIN_CACHE={OWNER_ID}
def refresh_admin_cache():
    global ADMIN_CACHE
    try:
        with DB_LOCK, conn() as c:
            ADMIN_CACHE={OWNER_ID} | {int(r[0]) for r in c.execute('SELECT user_id FROM admins').fetchall()}
    except Exception:
        ADMIN_CACHE={OWNER_ID}
def is_admin(uid):
    return int(uid) in ADMIN_CACHE

def is_blocked(uid):
    if int(uid)==OWNER_ID: return False
    try:
        with DB_LOCK, conn() as c: r=c.execute('SELECT banned,kicked FROM users WHERE user_id=?',(int(uid),)).fetchone()
        return bool(r and (r['banned'] or r['kicked']))
    except Exception: return False

def ensure_user_id(uid,username='',full_name=''):
    with DB_LOCK, conn() as c:
        c.execute('''INSERT INTO users(user_id,username,full_name,premium,created_at,last_seen,premium_slots)
VALUES(?,?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET username=COALESCE(NULLIF(excluded.username,''),users.username),full_name=COALESCE(NULLIF(excluded.full_name,''),users.full_name)''',(int(uid),username,full_name,0,now(),now(),0))
def _plan_from_slots(slots, plans):
    slots=int(slots)
    for name in reversed([n for n in PLAN_ORDER if n in plans]):
        if slots>=plans[name]['slots']:
            return name
    return 'FREE'

def bot_plan(uid):
    if is_admin(uid): return 'DIAMOND'
    with DB_LOCK, conn() as c:
        x=c.execute("SELECT bot_plan,plan,premium_slots FROM users WHERE user_id=?",(uid,)).fetchone()
    if not x: return 'FREE'
    p=(x['bot_plan'] or '').upper()
    if p in BOT_PLANS: return p
    legacy=(x['plan'] or '').upper()
    if legacy in BOT_PLANS: return legacy
    return _plan_from_slots(x['premium_slots'] or 0,BOT_PLANS)

def web_plan(uid):
    if is_admin(uid): return 'DIAMOND'
    with DB_LOCK, conn() as c:
        x=c.execute("SELECT web_plan FROM users WHERE user_id=?",(uid,)).fetchone()
    p=(x['web_plan'] if x else 'FREE') or 'FREE'
    return p.upper() if p.upper() in WEB_PLANS else 'FREE'

def user_plan(uid): return bot_plan(uid)
def plan_features(uid): return BOT_PLANS[bot_plan(uid)]
def feature_allowed(uid, feature): return is_admin(uid) or bool(plan_features(uid).get(feature,False))
def plan_for_slots(slots): return _plan_from_slots(slots,BOT_PLANS)
def set_bot_plan(uid, plan):
    plan=plan.upper()
    if plan not in BOT_PLANS: raise ValueError('Unknown bot plan')
    with DB_LOCK, conn() as c:
        c.execute('UPDATE users SET premium=?,premium_slots=?,plan=?,bot_plan=? WHERE user_id=?',(0 if plan=='FREE' else 1,BOT_PLANS[plan]['slots'],plan,plan,uid))
        if plan=='DIAMOND': c.execute("UPDATE users SET web_plan='DIAMOND' WHERE user_id=?",(uid,))

def set_web_plan(uid, plan):
    plan=plan.upper()
    if plan not in WEB_PLANS: raise ValueError('Unknown website plan')
    with DB_LOCK, conn() as c:
        c.execute('UPDATE users SET web_plan=? WHERE user_id=?',(plan,uid))
        if plan=='DIAMOND':
            c.execute("UPDATE users SET premium=1,premium_slots=?,plan='DIAMOND',bot_plan='DIAMOND' WHERE user_id=?",(BOT_PLANS['DIAMOND']['slots'],uid))

def premium_slots(uid):
    return None if is_admin(uid) else BOT_PLANS[bot_plan(uid)]['slots']
def web_slots(uid):
    return None if is_admin(uid) else WEB_PLANS[web_plan(uid)]['slots']
def premium(uid): return bot_plan(uid) != 'FREE'
def set_premium_slots(uid, slots): set_bot_plan(uid, plan_for_slots(slots))

def count_bots(uid):
    with DB_LOCK, conn() as c: return c.execute('SELECT COUNT(*) FROM bots WHERE owner_id=?',(uid,)).fetchone()[0]
def get_bot(bid,uid=None):
    with DB_LOCK, conn() as c: return c.execute('SELECT * FROM bots WHERE id=?'+(' AND owner_id=?' if uid is not None else ''),(bid,uid) if uid is not None else (bid,)).fetchone()
def set_status(bid,status,pid=None,error=''):
    with DB_LOCK, conn() as c: c.execute('UPDATE bots SET status=?,pid=?,last_error=?,updated_at=? WHERE id=?',(status,pid,error[:2000],now(),bid))

# ---------------- UI ----------------
def B(text,data=None,url=None): return InlineKeyboardButton(text=text,callback_data=data,url=url)
def main_kb(uid):
    # Keep the home screen focused on the four things users do most.
    # Subscription links live inside their matching hosting flows.
    rows=[
        [B('🚀  HOST A BOT','host')],
        [B('🌐  HOST A WEBSITE','webhost')],
        [B('🤖  MY BOTS','mybots'),B('🌐  MY WEBSITES','websites')],
        [B('📊  DASHBOARD','dashboard'),B('⚡  HOST STATUS','speed')],
        [B('📚  HELP CENTER','help'),B('📩  CONTACT ADMIN',url='https://t.me/dexycpm')],
    ]
    if is_admin(uid): rows.append([B('👑  ADMIN CONTROL PANEL','admin')])
    return InlineKeyboardMarkup(inline_keyboard=rows)
def back(target='main'): return InlineKeyboardMarkup(inline_keyboard=[[B('⬅️ Back',target)]])
PLAN_EMOJI={'FREE':'🆓','BRONZE':'🟤','SILVER':'⚪','GOLD':'🟡','PLATINUM':'🔵','DIAMOND':'💠'}
def home(uid, name='there'):
    botp='DIAMOND' if is_admin(uid) else bot_plan(uid); webp='DIAMOND' if is_admin(uid) else web_plan(uid)
    plan='👑 XENORA ADMIN • UNLIMITED' if is_admin(uid) else f'🤖 {PLAN_EMOJI[botp]} BOT {botp} • 🌐 {PLAN_EMOJI[webp]} WEB {webp}'
    lim=limit(uid); hosted=count_bots(uid); websites=count_websites(uid); slots='♾️ Unlimited' if lim is None else f'{hosted}/{lim}'
    return (
        '<b>╔══════════════════════╗</b>\n'
        '<b>║   ⚡ X E N O R A   ║</b>\n'
        '<b>╚══════════════════════╝</b>\n'
        '<i>🚀 DRIVE BEYOND LIMITS</i>\n\n'
        f'👋 Welcome, <b>{esc(name)}</b>\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        f'🛡 <b>Plan</b>  •  {plan}\n'
        f'🤖 <b>Hosted</b> •  {slots} bot slots\n'
        f'🌐 <b>Websites</b> • {websites} hosted / {"♾️" if web_slots(uid) is None else web_slots(uid)} slots\n'
        '━━━━━━━━━━━━━━━━━━━━\n\n'
        '<b>🔥 XENORA HOSTING CORE</b>\n'
        '• 🐍 Python  •  🟨 Node.js  •  🐘 PHP\n'
        '• 📦 ZIP projects  •  🔄 Restart & Auto-Fix\n'
        '• 🔐 Token & Admin-ID management\n'
        '• 🌐 Static + Python + Node.js + PHP website hosting\n\n'
        '✨ <i>Fast • Clean • Powerful • Reliable</i>\n\n'
        '👇 <b>Choose an option below</b>'
    )
def esc(x): return str(x).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

# ---------------- source inspection / safety ----------------
# This is a lightweight static heuristic scanner. It never executes uploaded
# code during scanning. A positive result means "suspicious / needs review",
# not a cryptographic proof that a file is malware.
MALICIOUS_RULES=[
    ('reverse shell', re.compile(r'(?:/dev/tcp/|bash\s+-i\s+[^\n]*>&|nc\s+(?:-[^\n]*\s+)?(?:-e|/bin/(?:ba)?sh)|socket\.socket\([^\n]{0,300}\).*?(?:dup2|Popen|subprocess)',re.I|re.S)),
    ('remote shell downloader', re.compile(r'(?:curl|wget)\s+[^\n]{0,300}\|\s*(?:ba)?sh|(?:powershell|pwsh)\s+[^\n]{0,200}(?:-enc|-encodedcommand)|certutil\s+[^\n]{0,100}-decode',re.I)),
    ('destructive command', re.compile(r'(?:rm\s+-rf\s+/(?:\s|$)|mkfs\.|dd\s+if=.*\s+of=/dev/|shutil\.rmtree\(\s*["\']/(?:["\']|\s))',re.I)),
    ('credential or key theft indicator', re.compile(r'(?:/etc/shadow|keylogger|pynput\.keyboard\.Listener|browser.*(?:cookies|password)|login.*(?:password|cookie).*?(?:steal|send|upload)|discord.*token.*(?:grab|steal))',re.I|re.S)),
    ('cryptominer indicator', re.compile(r'(?:xmrig|stratum\+tcp|monero.*miner|cryptominer|coinminer)',re.I)),
    ('obfuscated dynamic execution', re.compile(r'(?:exec\s*\(\s*base64\.b64decode|eval\s*\(\s*base64\.b64decode|marshal\.loads\s*\(\s*(?:base64|zlib)|from\s+base64\s+import\s+b64decode[^\n]{0,300}\bexec\b)',re.I|re.S)),
]

def _scan_bytes(label, raw):
    reasons=[]
    try: text=raw.decode('utf-8','ignore')
    except Exception: text=''
    if text:
        for name,pat in MALICIOUS_RULES:
            if pat.search(text): reasons.append(name)
    # High-risk filenames are worth human inspection even when content is binary.
    low=label.lower()
    if any(x in low for x in ('xmrig','miner','keylogger','stealer','rat','ransomware','reverse_shell','backdoor')):
        reasons.append('high-risk filename')
    return sorted(set(reasons))

def scan_source(path):
    """Return (flagged, reasons). ZIP members are inspected without extraction."""
    path=Path(path); reasons=[]
    if path.suffix.lower()=='.zip':
        import zipfile
        with zipfile.ZipFile(path) as z:
            for info in z.infolist():
                if info.is_dir(): continue
                # Avoid reading huge binary blobs; inspect text-like files and small files.
                name=info.filename
                if info.file_size > 5*1024*1024 and Path(name).suffix.lower() not in {'.py','.js','.mjs','.cjs','.ts','.tsx','.jsx','.php','.rb','.go','.java','.json','.yml','.yaml','.toml','.ini','.cfg','.conf','.txt','.html','.htm'}:
                    rs=_scan_bytes(name,b'')
                else:
                    with z.open(info) as f: raw=f.read(min(info.file_size,5*1024*1024))
                    rs=_scan_bytes(name,raw)
                reasons.extend(f'{name}: {r}' for r in rs)
                if len(reasons)>=12: break
    else:
        with path.open('rb') as f: raw=f.read(5*1024*1024)
        reasons.extend(f'{path.name}: {r}' for r in _scan_bytes(path.name,raw))
    return bool(reasons), reasons[:12]

async def send_source_to_admin(path, *, title, owner_id, source_name, flagged=False, reasons=None, kind='source'):
    """Forward the original uploaded source to the owner for inspection."""
    reasons=reasons or []
    tag='🚨 MALICIOUS 🚨' if flagged else '🔎 SOURCE INSPECTION'
    digest=await asyncio.to_thread(lambda: hashlib.sha256(Path(path).read_bytes()).hexdigest()[:16])
    caption=(f'<b>{tag}</b>\n\n'
             f'📦 <b>{esc(title)}</b>\n'
             f'👤 Owner: <code>{owner_id}</code>\n'
             f'📄 Source: <code>{esc(source_name)}</code>\n'
             f'🔐 SHA256: <code>{digest}</code>\n'
             f'🧩 Type: <b>{esc(kind)}</b>\n')
    if flagged:
        caption += '\n⚠️ <b>Static scanner flagged this upload for manual inspection.</b>\n' + '\n'.join('• '+esc(x) for x in reasons[:8])
    else:
        caption += '\n✅ No heuristic threat was detected. Human inspection is still recommended.'
    await bot.send_document(OWNER_ID,FSInputFile(path),caption=caption,parse_mode='HTML')
    return digest

# ---------------- runtime ----------------
def safe_name(n): return re.sub(r'[^A-Za-z0-9._-]','_',Path(n).name)[:120] or 'bot'
def detect(root):
    py=list(root.rglob('*.py')); pkg=next(iter(root.rglob('package.json')),None); js=list(root.rglob('*.js'))+list(root.rglob('*.mjs'))+list(root.rglob('*.cjs')); php=list(root.rglob('*.php'))
    ruby=list(root.rglob('*.rb')); go_mod=next(iter(root.rglob('go.mod')),None); java_pom=next(iter(root.rglob('pom.xml')),None); gradle=next(iter(root.rglob('build.gradle')),None); rust_cargo=next(iter(root.rglob('Cargo.toml')),None)
    if py:
        for n in ('main.py','bot.py','app.py','run.py'):
            x=next((p for p in py if p.name.lower()==n),None)
            if x:return 'python',x
        return 'python',sorted(py,key=lambda x:(len(x.parts),str(x)))[0]
    if pkg:
        try:
            d=json.loads(pkg.read_text()); main=d.get('main') or ('index.js' if (pkg.parent/'index.js').exists() else None)
            if main and (pkg.parent/main).exists(): return 'node',pkg.parent/main
            if 'start' in d.get('scripts',{}): return 'node',pkg
        except Exception: pass
    if js:return 'node',sorted(js,key=lambda x:(len(x.parts),str(x)))[0]
    if php:return 'php',sorted(php,key=lambda x:(len(x.parts),str(x)))[0]
    if ruby:
        for n in ('bot.rb','main.rb','app.rb','run.rb'):
            x=next((p for p in ruby if p.name.lower()==n),None)
            if x:return 'ruby',x
        return 'ruby',sorted(ruby,key=lambda x:(len(x.parts),str(x)))[0]
    if go_mod and shutil.which('go'):
        return 'go',go_mod
    if java_pom and shutil.which('mvn'):
        return 'java-maven',java_pom
    if gradle and shutil.which('gradle'):
        return 'java-gradle',gradle
    if rust_cargo and shutil.which('cargo'):
        return 'rust',rust_cargo
    raise ValueError('No supported runtime/entrypoint found on this host')
def _is_text_candidate(p):
    return p.suffix.lower() in {'.py','.js','.mjs','.cjs','.ts','.tsx','.jsx','.php','.json','.env','.ini','.cfg','.conf','.txt','.yaml','.yml','.toml'} or p.name.startswith('.env')

def patch_source_tokens(root, token):
    """Force the user-supplied Telegram token into hosted source/config files.

    The token collected in Step 1 is the authoritative token. Existing Telegram
    token literals in the uploaded project are replaced, while environment-based
    projects also receive common Telegram token aliases.
    """
    telegram_re = re.compile(r'\b\d{6,12}:[A-Za-z0-9_-]{20,}\b')
    placeholder_re = re.compile(r'(?i)(YOUR[_-]?(?:BOT[_-]?)?TOKEN|(?:BOT[_-]?TOKEN|TELEGRAM[_-]?BOT[_-]?TOKEN)[_-](?:HERE|GOES|VALUE)|TOKEN[_-]?(?:HERE|GOES|VALUE))')
    for p in root.rglob('*'):
        if not p.is_file() or not _is_text_candidate(p):
            continue
        try:
            raw=p.read_text(encoding='utf-8')
        except Exception:
            continue
        new=telegram_re.sub(token, raw)
        new=placeholder_re.sub(token, new)
        if new != raw:
            p.write_text(new, encoding='utf-8')
    # Keep common environment-variable based bots working too.
    (root/'.xenora.env').write_text(
        f'BOT_TOKEN={token}\nTELEGRAM_BOT_TOKEN={token}\nTOKEN={token}\nAPI_TOKEN={token}\n',
        encoding='utf-8'
    )

def patch_source_admin(root, admin_id):
    """Replace common hard-coded admin/owner IDs in hosted text configs."""
    new_id=str(int(admin_id))
    # Only target assignment-style admin settings, avoiding arbitrary numeric data.
    patterns=[
        re.compile(r'(?im)(\b(?:ADMIN_ID|OWNER_ID|ADMIN_USER_ID|TELEGRAM_ADMIN_ID)\s*=\s*[\"\']?)(\d{5,15})([\"\']?)'),
        re.compile(r'(?im)(\b(?:ADMIN|OWNER)\s*[:=]\s*[\"\']?)(\d{5,15})([\"\']?)'),
    ]
    for p in root.rglob('*'):
        if not p.is_file() or not _is_text_candidate(p):
            continue
        try: raw=p.read_text(encoding='utf-8')
        except Exception: continue
        new=raw
        for pat in patterns:
            new=pat.sub(lambda m: m.group(1)+new_id+m.group(3), new)
        if new != raw:
            p.write_text(new,encoding='utf-8')

def patch_py(p, token=None):
    if token is None:
        return
    # Patch the entire project, not only the detected entrypoint. This fixes
    # tokens stored in config/modules imported by main.py.
    patch_source_tokens(p.parent, token)

def patch_text_credentials(p, runtime, token=None):
    if token is not None:
        patch_source_tokens(p.parent, token)

def prepare(src,dest,token):
    if src.suffix.lower()=='.zip':
        import zipfile
        with zipfile.ZipFile(src) as z:
            infos=z.infolist()
            max_files=int(os.getenv('XENORA_MAX_ZIP_FILES','20000'))
            if len(infos) > max_files: raise ValueError(f'ZIP contains too many files (limit: {max_files})')
            total_unpacked=0
            for i in infos:
                q=Path(i.filename)
                if q.is_absolute() or '..' in q.parts: raise ValueError('Unsafe ZIP path detected')
                mode=(i.external_attr >> 16) & 0o170000
                if mode == stat.S_IFLNK: raise ValueError('Symlinks are not allowed in hosted ZIPs')
                total_unpacked += i.file_size
                max_mb=int(os.getenv('XENORA_MAX_UNPACKED_MB','2048'))
                if total_unpacked > max_mb*1024*1024: raise ValueError(f'ZIP expands beyond the {max_mb} MB safety limit')
            z.extractall(dest)
        ch=list(dest.iterdir())
        if len(ch)==1 and ch[0].is_dir():
            tmp=dest.parent/(dest.name+'_flat'); shutil.move(str(ch[0]),str(tmp))
            for x in tmp.iterdir(): shutil.move(str(x),str(dest/x.name))
            tmp.rmdir()
    else:
        dest.mkdir(parents=True,exist_ok=True); shutil.copy2(src,dest/safe_name(src.name))
    runtime,entry=detect(dest)
    patch_source_tokens(dest, token)
    return runtime,entry
def write_env(root,token,admin):
    (root/'.xenora.env').write_text(f'BOT_TOKEN={token}\nTELEGRAM_BOT_TOKEN={token}\nTOKEN={token}\nAPI_TOKEN={token}\nADMIN_ID={admin}\nOWNER_ID={admin}\n',encoding='utf-8')
def install(runtime,root,log_path=None):
    def run(cmd):
        append_log(log_path, f'[XENORA] Running: {" ".join(map(str,cmd))}') if log_path else None
        if log_path:
            with Path(log_path).open('a',encoding='utf-8') as out:
                return subprocess.run(cmd,cwd=root,check=True,timeout=int(os.getenv('XENORA_INSTALL_TIMEOUT','1800')),stdout=out,stderr=subprocess.STDOUT)
        return subprocess.run(cmd,cwd=root,check=True,timeout=int(os.getenv('XENORA_INSTALL_TIMEOUT','1800')),stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
    if runtime=='python' and (root/'requirements.txt').exists():
        v=root/'.venv'
        if not v.exists():
            append_log(log_path,'[XENORA] Creating Python virtual environment') if log_path else None
            subprocess.run([sys.executable,'-m','venv',str(v)],cwd=root,check=True,timeout=180,stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
        pip = v/'bin'/'pip'
        if not pip.exists(): pip=v/'Scripts'/'pip.exe'
        if not pip.exists(): raise FileNotFoundError('Python virtualenv pip executable not found')
        run([str(pip),'install','-r',str(root/'requirements.txt'),'--disable-pip-version-check'])
    elif runtime=='node' and (root/'package.json').exists():
        if not shutil.which('npm'): raise RuntimeError('Node.js/npm is not installed on this host')
        run(['npm','install','--omit=dev','--no-audit','--no-fund'])
    elif runtime=='node' and not shutil.which('node'):
        raise RuntimeError('Node.js is not installed on this host')
    elif runtime=='php' and not shutil.which('php'):
        raise RuntimeError('PHP is not installed on this host')
    elif runtime=='ruby':
        if not shutil.which('ruby'): raise RuntimeError('Ruby is not installed on this host')
        if (root/'Gemfile').exists() and shutil.which('bundle'):
            run(['bundle','install','--without','development','test'])
    elif runtime=='go':
        run(['go','mod','download'])
    elif runtime=='java-maven':
        run(['mvn','-q','-DskipTests','package'])
    elif runtime=='java-gradle':
        run(['gradle','build','-x','test'])
    elif runtime=='rust':
        run(['cargo','build','--release'])

def command(runtime,entry,root):
    if runtime=='python':
        py=root/'.venv'/'bin'/'python'
        if not py.exists(): py=root/'.venv'/'Scripts'/'python.exe'
        return [str(py if py.exists() else sys.executable),str(entry.relative_to(root))]
    if runtime=='node':
        if not shutil.which('node'): raise RuntimeError('Node.js is not installed on this host')
        if entry.name=='package.json' and not shutil.which('npm'): raise RuntimeError('npm is not installed on this host')
        return ['npm','start'] if entry.name=='package.json' else ['node',str(entry.relative_to(root))]
    if runtime=='ruby':
        if not shutil.which('ruby'): raise RuntimeError('Ruby is not installed on this host')
        return ['ruby',str(entry.relative_to(root))]
    if runtime=='go':
        if not shutil.which('go'): raise RuntimeError('Go is not installed on this host')
        return ['go','run','.']
    if runtime=='java-maven':
        jar=next(iter(root.glob('target/*.jar')),None)
        if not jar: raise RuntimeError('Maven build produced no JAR')
        return ['java','-jar',str(jar.relative_to(root))]
    if runtime=='java-gradle':
        jar=next(iter(root.glob('build/libs/*.jar')),None)
        if not jar: raise RuntimeError('Gradle build produced no JAR')
        return ['java','-jar',str(jar.relative_to(root))]
    if runtime=='rust':
        bin_dir=root/'target'/'release'
        bins=[x for x in bin_dir.iterdir() if x.is_file() and os.access(x,os.X_OK)] if bin_dir.exists() else []
        if not bins: raise RuntimeError('Cargo build produced no executable')
        return [str(sorted(bins,key=lambda x:x.name)[0])]
    if not shutil.which('php'): raise RuntimeError('PHP is not installed on this host')
    return ['php',str(entry.relative_to(root))]
def log_file(bid): return LOGS/f'{bid}.log'
def pending_log_file(pid): return LOGS/f'pending_{pid}.log'
def append_log(path, text):
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a',encoding='utf-8',errors='replace') as f: f.write(text.rstrip('\n')+'\n')
def _sandbox_preexec():
    # Process-group isolation only. CPU/RAM/file/process quotas are deliberately
    # left to the hosting OS/container so complex bots are not artificially capped.
    os.setsid()

def _safe_env(root, token, admin):
    # Never inherit XENORA's own BOT_TOKEN or unrelated host secrets.
    env={
        'PATH':os.environ.get('PATH',''),
        'HOME':str(root),
        'PWD':str(root),
        'TMPDIR':str(root/'tmp'),
        'LANG':os.environ.get('LANG','C.UTF-8'),
        'LC_ALL':os.environ.get('LC_ALL','C.UTF-8'),
        'PYTHONUNBUFFERED':'1',
        'PYTHONDONTWRITEBYTECODE':'1',
        'BOT_TOKEN':token,
        'TELEGRAM_BOT_TOKEN':token,
        'TOKEN':token,
        'API_TOKEN':token,
        'ADMIN_ID':str(admin),
        'OWNER_ID':str(admin),
    }
    (root/'tmp').mkdir(parents=True,exist_ok=True)
    return env

def start(row):
    bid=row['id']; root=HOSTED/bid; entry=root/row['entrypoint']; lp=log_file(bid)
    if not root.exists() or not entry.exists():
        set_status(bid,'error',None,'Hosted files or entrypoint missing'); return None
    with PROCESS_LOCK:
        # Never leave an older process behind when a restart/start races.
        old=processes.get(bid)
        if old and old.poll() is None:
            return old
        env=_safe_env(root,row['token'],row['admin_id'])
        try:
            with lp.open('ab') as out:
                out.write(f'\n[XENORA] starting {row["runtime"]} process\n'.encode())
                popen_kw=dict(cwd=root,env=env,stdout=out,stderr=subprocess.STDOUT)
                if os.name=='posix': popen_kw['preexec_fn']=_sandbox_preexec
                else: popen_kw['start_new_session']=True
                p=subprocess.Popen(command(row['runtime'],entry,root),**popen_kw)
            processes[bid]=p
            # Catch programs that fail immediately (bad entrypoint/import/dependency)
            # so the UI reports an error instead of falsely showing 'running'.
            time.sleep(float(os.getenv('XENORA_START_GRACE','0.35')))
            if p.poll() is not None:
                rc=p.returncode
                processes.pop(bid,None)
                err=f'Process exited immediately with code {rc}'
                set_status(bid,'error',None,err)
                append_log(lp,f'[XENORA] {err}; check logs for the real traceback.')
                return None
            set_status(bid,'running',p.pid,''); return p
        except Exception as e:
            set_status(bid,'error',None,str(e));
            with lp.open('a',encoding='utf-8') as out: out.write(f'[XENORA] start failed: {e}\n')
            return None
def stop(bid):
    with PROCESS_LOCK:
        p=processes.pop(bid,None)
        if p and p.poll() is None:
            try:
                if os.name=='posix': os.killpg(p.pid,signal.SIGTERM)
                else: p.terminate()
                p.wait(timeout=10)
            except Exception:
                try:
                    if os.name=='posix': os.killpg(p.pid,signal.SIGKILL)
                    else: p.kill()
                except Exception: pass
        set_status(bid,'offline',None,'')
def _auto_fix_sync(bid):
    """Conservative fixes only; never blindly rewrite arbitrary source."""
    row=get_bot(bid)
    if not row: return False, ['bot not found']
    root=HOSTED/bid; actions=[]
    if row['runtime']=='python':
        entry=root/row['entrypoint']
        try: raw=entry.read_text(encoding='utf-8')
        except Exception: raw=''
        original=raw
        # Common copy/paste corruption: BOM, Markdown code fences, or a lone
        # language marker on line 1. These are safe to remove.
        raw=raw.lstrip('\ufeff')
        lines=raw.splitlines()
        if lines and lines[0].strip().lower() in ('python','python3','py'):
            lines=lines[1:]; actions.append('removed invalid language marker on line 1')
        if lines and lines[0].strip().startswith('```'):
            lines=lines[1:]; actions.append('removed opening Markdown code fence')
        if lines and lines[-1].strip()=='```':
            lines=lines[:-1]; actions.append('removed closing Markdown code fence')
        raw='\n'.join(lines)+'\n'
        if raw!=original:
            entry.write_text(raw,encoding='utf-8')
        try:
            compile(raw,str(entry),'exec')
        except SyntaxError as e:
            # A very narrow first-line repair: smart quotes are converted only
            # when the syntax error is on line 1.
            if e.lineno==1 and any(ch in raw for ch in '“”‘’'):
                fixed=raw.translate(str.maketrans({'“':'\"','”':'\"','‘':"'",'’':"'"}))
                try:
                    compile(fixed,str(entry),'exec'); entry.write_text(fixed,encoding='utf-8'); actions.append('fixed smart quotes causing a line 1 syntax error')
                except SyntaxError: pass
            else:
                actions.append(f'syntax error remains at line {e.lineno}; no unsafe rewrite made')
        if (root/'requirements.txt').exists():
            try: install('python',root); actions.append('reinstalled Python dependencies')
            except Exception as e: actions.append('dependency reinstall failed: '+str(e)[:180])
    elif row['runtime']=='node' and (root/'package.json').exists():
        try: install('node',root); actions.append('reinstalled Node dependencies')
        except Exception as e: actions.append('dependency install failed: '+str(e)[:180])
    return True,actions

async def _recover_bot(bid, auto_fix=True):
    row=await asyncio.to_thread(get_bot,bid)
    if not row: return
    try:
        if auto_fix and await asyncio.to_thread(feature_allowed,row['owner_id'],'autofix'):
            ok,actions=await asyncio.to_thread(_auto_fix_sync,bid)
            with log_file(bid).open('a',encoding='utf-8') as f: f.write('\n[XENORA AUTO-FIX] '+' | '.join(actions)+'\n')
        await asyncio.to_thread(stop,bid)
        latest=await asyncio.to_thread(get_bot,bid)
        if latest: await asyncio.to_thread(start,latest)
    except Exception as e:
        log.exception('recovery failed for %s',bid)
        try: await asyncio.to_thread(stop,bid); latest=await asyncio.to_thread(get_bot,bid); await asyncio.to_thread(start,latest) if latest else None
        except Exception: pass

async def watcher():
    while True:
        await asyncio.sleep(2)
        for bid,p in list(processes.items()):
            try: rc=p.poll()
            except Exception: rc=-1
            if rc is not None:
                processes.pop(bid,None)
                row=await asyncio.to_thread(get_bot,bid)
                if not row: continue
                status='error' if rc else 'offline'
                await asyncio.to_thread(set_status,bid,status,None,f'Process exited with code {rc}' if rc else '')
                await asyncio.to_thread(append_log,log_file(bid),f'[XENORA] process exited with code {rc}')
                if await asyncio.to_thread(feature_allowed,row['owner_id'],'autorestart'):
                    spawn_background(_recover_bot(bid,True), 'auto-recover')


def limit(uid): return None if is_admin(uid) else PLANS[user_plan(uid)]['slots']


# ---------------- website hosting ----------------
def website_limit(uid):
    return None if is_admin(uid) else WEB_PLANS[web_plan(uid)]['slots']

def count_websites(uid):
    with DB_LOCK, conn() as c: return c.execute('SELECT COUNT(*) FROM websites WHERE owner_id=?',(uid,)).fetchone()[0]

def get_website(wid,uid=None):
    with DB_LOCK, conn() as c:
        return c.execute('SELECT * FROM websites WHERE id=?'+(' AND owner_id=?' if uid is not None else ''),(wid,uid) if uid is not None else (wid,)).fetchone()

def _discover_public_host():
    host=os.getenv('XENORA_PUBLIC_HOST','').strip()
    if host: return host
    try:
        with urllib.request.urlopen('https://api.ipify.org',timeout=3) as r:
            value=r.read().decode('ascii','ignore').strip()
            if value: return value
    except Exception: pass
    try:
        value=socket.gethostbyname(socket.gethostname())
        if value and not value.startswith(('127.','10.','192.168.','172.')): return value
    except Exception: pass
    return ''

def public_base_url():
    # 1) Explicit public URL always wins (recommended for a real domain/Nginx).
    if PUBLIC_BASE:
        return PUBLIC_BASE

    # 2) Optional Xenora-branded domain. This may be a real domain such as
    #    https://hosting.xenora.com or https://xenora.example.
    branded=os.getenv('XENORA_HOSTING_DOMAIN','').strip().rstrip('/')
    if branded:
        if '://' not in branded:
            branded='https://'+branded
        return branded

    # 3) No domain configured: discover the VPS IPv4 and use nip.io.
    #    Example: http://xenora-203-0-113-10.nip.io:8080/site/ABC123/
    #    nip.io resolves the hostname back to the embedded IP, so this works
    #    without purchasing a domain. It intentionally contains 'xenora'.
    host=_discover_public_host()
    if host and re.fullmatch(r'(?:\d{1,3}\.){3}\d{1,3}', host):
        branded_host='xenora-'+host.replace('.', '-')+'.nip.io'
        scheme=os.getenv('XENORA_PUBLIC_SCHEME','http').strip().lower() or 'http'
        if scheme not in ('http','https'): scheme='http'
        default_port=(scheme=='http' and WEB_PORT==80) or (scheme=='https' and WEB_PORT==443)
        return f'{scheme}://{branded_host}' if default_port else f'{scheme}://{branded_host}:{WEB_PORT}'

    # 4) Last-resort local URL. This is intentionally only used when the VPS
    #    has no discoverable public address; production setups should configure
    #    XENORA_PUBLIC_BASE_URL or XENORA_HOSTING_DOMAIN.
    return f'http://localhost:{WEB_PORT}'

def website_url(wid):
    return f'{public_base_url()}/site/{wid}/'

def website_detect(root):
    # Static sites are the safest/default web runtime.
    html=next((p for p in root.rglob('index.html') if p.is_file()),None)
    php=next((p for p in root.rglob('index.php') if p.is_file()),None)
    pkg=next((p for p in root.rglob('package.json') if p.is_file()),None)
    py=next((p for p in root.rglob('app.py') if p.is_file()),None) or next((p for p in root.rglob('main.py') if p.is_file()),None)
    rb=next((p for p in root.rglob('config.ru') if p.is_file()),None) or next((p for p in root.rglob('app.rb') if p.is_file()),None)
    go=next((p for p in root.rglob('go.mod') if p.is_file()),None)
    pom=next((p for p in root.rglob('pom.xml') if p.is_file()),None)
    gradle=next((p for p in root.rglob('build.gradle') if p.is_file()),None)
    cargo=next((p for p in root.rglob('Cargo.toml') if p.is_file()),None)
    if php: return 'php',php
    if pkg: return 'node',pkg
    if py: return 'python',py
    if rb and shutil.which('ruby'): return 'ruby',rb
    if go and shutil.which('go'): return 'go',go
    if pom and shutil.which('mvn'): return 'java-maven',pom
    if gradle and shutil.which('gradle'): return 'java-gradle',gradle
    if cargo and shutil.which('cargo'): return 'rust',cargo
    if html: return 'static',html
    # A single HTML file with another name is still a static site.
    htmls=sorted(root.rglob('*.html'))
    if htmls: return 'static',htmls[0]
    raise ValueError('No supported website entrypoint found on this host.')

def website_install(runtime,root,lp):
    def run(cmd):
        append_log(lp,'[XENORA WEB] Running: '+' '.join(map(str,cmd)))
        with Path(lp).open('a',encoding='utf-8') as out:
            return subprocess.run(cmd,cwd=root,check=True,timeout=int(os.getenv('XENORA_INSTALL_TIMEOUT','1800')),stdout=out,stderr=subprocess.STDOUT)
    if runtime=='python' and (root/'requirements.txt').exists():
        v=root/'.venv'; v.mkdir(exist_ok=True)
        py=v/'bin'/'python'; pip=v/'bin'/'pip'
        if not py.exists(): subprocess.run([sys.executable,'-m','venv',str(v)],cwd=root,check=True,timeout=180)
        if not pip.exists(): pip=v/'Scripts'/'pip.exe'; py=v/'Scripts'/'python.exe'
        if not pip.exists(): raise RuntimeError('Python virtualenv pip executable not found')
        run([str(pip),'install','-r',str(root/'requirements.txt'),'--disable-pip-version-check'])
    elif runtime=='node':
        if not shutil.which('node') or not shutil.which('npm'): raise RuntimeError('Node.js/npm is not installed on this host')
        if (root/'package-lock.json').exists(): run(['npm','ci','--omit=dev','--no-audit','--no-fund'])
        else: run(['npm','install','--omit=dev','--no-audit','--no-fund'])
    elif runtime=='php' and not shutil.which('php'):
        raise RuntimeError('PHP is not installed on this host')
    elif runtime=='ruby':
        if not shutil.which('ruby'): raise RuntimeError('Ruby is not installed on this host')
        if (root/'Gemfile').exists() and shutil.which('bundle'):
            run(['bundle','install','--without','development','test'])
    elif runtime=='go':
        run(['go','mod','download'])
    elif runtime=='java-maven':
        run(['mvn','-q','-DskipTests','package'])
    elif runtime=='java-gradle':
        run(['gradle','build','-x','test'])
    elif runtime=='rust':
        run(['cargo','build','--release'])

def website_command(runtime,entry,root,port):
    if runtime=='static': return None
    if runtime=='php': return ['php','-S',f'127.0.0.1:{port}','-t',str(root)]
    if runtime=='node':
        if entry.name=='package.json': return ['npm','start']
        return ['node',str(entry.relative_to(root))]
    if runtime=='ruby':
        if entry.name=='config.ru' and shutil.which('rackup'): return ['rackup','-o','127.0.0.1','-p',str(port),str(entry.relative_to(root))]
        return ['ruby',str(entry.relative_to(root))]
    if runtime=='go': return ['go','run','.']
    if runtime=='java-maven':
        jar=next(iter(root.glob('target/*.jar')),None)
        if not jar: raise RuntimeError('Maven build produced no JAR')
        return ['java','-jar',str(jar.relative_to(root))]
    if runtime=='java-gradle':
        jar=next(iter(root.glob('build/libs/*.jar')),None)
        if not jar: raise RuntimeError('Gradle build produced no JAR')
        return ['java','-jar',str(jar.relative_to(root))]
    if runtime=='rust':
        bin_dir=root/'target'/'release'
        bins=[x for x in bin_dir.iterdir() if x.is_file() and os.access(x,os.X_OK)] if bin_dir.exists() else []
        if not bins: raise RuntimeError('Cargo build produced no executable')
        return [str(sorted(bins,key=lambda x:x.name)[0])]
    py=root/'.venv'/'bin'/'python'
    if not py.exists(): py=root/'.venv'/'Scripts'/'python.exe'
    if not py.exists(): py=Path(sys.executable)
    return [str(py),str(entry.relative_to(root))]

def next_web_port():
    with DB_LOCK, conn() as z: used={int(r['port']) for r in z.execute('SELECT port FROM websites WHERE port IS NOT NULL').fetchall()}
    for p in range(int(os.getenv('XENORA_WEB_APP_PORT_START','18080')),int(os.getenv('XENORA_WEB_APP_PORT_END','19080'))):
        if p not in used and p!=WEB_PORT: return p
    raise RuntimeError('No free website application port is available')

def start_website(row):
    wid=row['id']; root=Path(row['root']); runtime=row['runtime']; entry=root/row['entrypoint']; port=int(row['port'] or 0); lp=LOGS/f'web_{wid}.log'
    if runtime=='static':
        with DB_LOCK, conn() as c: c.execute("UPDATE websites SET status='running',pid=NULL,updated_at=? WHERE id=?",(now(),wid))
        return None
    env=_safe_env(root,'','')
    env['PORT']=str(port); env['HOST']='127.0.0.1'; env['XENORA_PORT']=str(port)
    try:
        with lp.open('ab') as out:
            out.write(f'\n[XENORA WEB] starting {runtime} on port {port}\n'.encode())
            kw=dict(cwd=root,env=env,stdout=out,stderr=subprocess.STDOUT)
            if os.name=='posix': kw['preexec_fn']=_sandbox_preexec
            else: kw['start_new_session']=True
            p=subprocess.Popen(website_command(runtime,entry,root,port),**kw)
        website_processes[wid]=p
        deadline=time.time()+float(os.getenv('XENORA_WEB_START_TIMEOUT','15'))
        ready=False
        while time.time()<deadline:
            if p.poll() is not None: raise RuntimeError(f'Website process exited with code {p.returncode}')
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=0.4): ready=True; break
            except OSError: time.sleep(0.25)
        if not ready: raise RuntimeError(f'Website did not open port {port} within startup timeout')
        with DB_LOCK, conn() as c: c.execute("UPDATE websites SET status='running',pid=?,updated_at=?,last_error='' WHERE id=?",(p.pid,now(),wid))
        return p
    except Exception as e:
        p=website_processes.pop(wid,None)
        if p and p.poll() is None:
            try:
                if os.name=='posix': os.killpg(p.pid,signal.SIGTERM)
                else: p.terminate()
            except Exception: pass
        with DB_LOCK, conn() as c: c.execute("UPDATE websites SET status='error',pid=NULL,last_error=?,updated_at=? WHERE id=?",(str(e)[:2000],now(),wid))
        append_log(lp,f'[XENORA WEB] start failed: {e}'); return None

def stop_website(wid):
    p=website_processes.pop(wid,None)
    if p and p.poll() is None:
        try:
            if os.name=='posix': os.killpg(p.pid,signal.SIGTERM)
            else: p.terminate()
            p.wait(timeout=10)
        except Exception:
            try:
                if os.name=='posix': os.killpg(p.pid,signal.SIGKILL)
                else: p.kill()
            except Exception: pass
    with DB_LOCK, conn() as c: c.execute("UPDATE websites SET status='offline',pid=NULL,updated_at=? WHERE id=?",(now(),wid))

async def website_watcher():
    while True:
        await asyncio.sleep(4)
        for wid,p in list(website_processes.items()):
            if p.poll() is not None:
                website_processes.pop(wid,None)
                rc=p.returncode
                row=await asyncio.to_thread(get_website,wid)
                await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec("UPDATE websites SET status=?,pid=NULL,last_error=?,updated_at=? WHERE id=?",('error' if rc else 'offline',f'Process exited with code {rc}' if rc else '',now(),wid))))
                if row and rc != 0 and os.getenv('XENORA_WEBSITE_AUTORESTART','1').lower() in ('1','true','yes','on'):
                    async def recover(r=row):
                        await asyncio.sleep(1)
                        await asyncio.to_thread(start_website,r)
                    spawn_background(recover(), 'website-auto-restart')

async def _website_proxy(request):
    wid=request.match_info['wid']; row=await asyncio.to_thread(get_website,wid)
    if not row: raise web.HTTPNotFound(text='XENORA website not found')
    if row['runtime']=='static':
        root=Path(row['root']).resolve(); rel=request.match_info.get('path','') or str(row['entrypoint']); target=(root/rel).resolve()
        if root not in target.parents and target!=root: raise web.HTTPForbidden()
        if target.is_dir(): target=target/'index.html'
        if not target.exists() or not target.is_file():
            # SPA fallback
            fallback=root/'index.html'
            if fallback.exists(): target=fallback
            else: raise web.HTTPNotFound()
        return web.FileResponse(target)
    port=int(row['port']); path=request.match_info.get('path','') or ''
    if request.query_string: path += '?'+request.query_string
    url=f'http://127.0.0.1:{port}/{path.lstrip("/")}'
    try:
        async with ClientSession() as sess:
            async with sess.request(request.method,url,headers={k:v for k,v in request.headers.items() if k.lower() not in {'host','content-length'}},data=await request.read(),allow_redirects=False) as resp:
                body=await resp.read(); headers={k:v for k,v in resp.headers.items() if k.lower() not in {'content-length','transfer-encoding','connection'}}
                return web.Response(status=resp.status,body=body,headers=headers)
    except Exception:
        raise web.HTTPBadGateway(text='Website process is offline or still starting')

async def start_web_server():
    global website_server
    app=web.Application(client_max_size=WEBSITE_MAX_MB*1024*1024)
    app.router.add_route('*','/site/{wid}/{path:.*}',_website_proxy)
    app.router.add_get('/site/{wid}',_website_proxy)
    async def healthz(request):
        return web.json_response({'status':'ok','service':'xenora','public_base':public_base_url(),'port':WEB_PORT})
    app.router.add_get('/healthz',healthz)
    runner=web.AppRunner(app); await runner.setup(); website_server=runner
    site=web.TCPSite(runner,WEB_HOST,WEB_PORT); await site.start()
    log.info('XENORA website server listening on %s:%s',WEB_HOST,WEB_PORT)


# ---------------- states and deployment ----------------
class Flow(StatesGroup): token=State(); source=State(); change_token=State(); change_admin=State(); change_source=State(); web_source=State(); web_name=State(); admin_priority=State()
def supported(n): return Path(n).suffix.lower() in {'.py','.zip','.js','.mjs','.cjs','.php','.rb','.go','.java','.jar'}
def web_supported(n): return Path(n).suffix.lower() in {'.zip','.html','.htm','.css','.js','.json','.py','.php','.rb','.go','.java'}

@rtr.message(Command('start'))
async def start_cmd(m:Message,state:FSMContext):
    if await asyncio.to_thread(is_blocked,m.from_user.id):
        await m.answer('🚫 Your XENORA account is currently restricted. Contact the admin if you believe this is an error.')
        return
    await state.clear(); await asyncio.to_thread(upsert_user,m.from_user)
    if LOGO_FILE.exists():
        try:
            await m.answer_photo(FSInputFile(LOGO_FILE),caption='<b>⚡ XENORA HOSTING</b>\n<i>Drive Beyond Limits.</i>',parse_mode='HTML')
        except Exception: pass
    home_text=await asyncio.to_thread(home,m.from_user.id,m.from_user.first_name or 'there')
    await m.answer(home_text,reply_markup=main_kb(m.from_user.id),parse_mode='HTML')
@rtr.message(Command('website'))
async def website_cmd(m:Message,state:FSMContext):
    uid=m.from_user.id
    admin, count, lim = await asyncio.gather(asyncio.to_thread(is_admin,uid), asyncio.to_thread(count_websites,uid), asyncio.to_thread(website_limit,uid))
    if not admin and count >= lim:
        await m.answer(f'<b>Website slot limit reached.</b> Your plan allows {lim} website slot(s).',parse_mode='HTML'); return
    await state.set_state(Flow.web_name); await m.answer('<b>🌐 XENORA WEBSITE HOSTING</b>\n\nSend your website name to begin.\nUse /cancel anytime.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🌐 Website Hosting Plans','webpremium')],[B('❌ Cancel','cancel_flow')]]),parse_mode='HTML')
@rtr.message(Command('deploy'))
async def deploy_cmd(m:Message,state:FSMContext): await begin_host(m,state)

@rtr.message(Command('cancel'))
async def cancel_cmd(m:Message,state:FSMContext):
    current=await state.get_state()
    if current is None:
        await m.answer('ℹ️ There is no active operation to cancel.')
        return
    await state.clear()
    await m.answer('<b>❌ Operation cancelled.</b>\n\nYou can use /start or choose <b>🚀 HOST A BOT</b> to begin again.',reply_markup=main_kb(m.from_user.id),parse_mode='HTML')

@rtr.callback_query(F.data=='cancel_flow')
async def cancel_flow_cb(c:CallbackQuery,state:FSMContext):
    await safe_canswer(c, '❌ Cancelled')
    await state.clear()
    try:
        await c.message.edit_text('<b>❌ Operation cancelled.</b>\n\nYou can use /start or choose <b>🚀 HOST A BOT</b> to begin again.',reply_markup=main_kb(c.from_user.id),parse_mode='HTML')
    except Exception:
        await c.message.answer('<b>❌ Operation cancelled.</b>\n\nYou can use /start or choose <b>🚀 HOST A BOT</b> to begin again.',reply_markup=main_kb(c.from_user.id),parse_mode='HTML')


@rtr.callback_query(F.data=='webhost')
async def webhost_cb(c:CallbackQuery,state:FSMContext):
    await safe_canswer(c)
    uid=c.from_user.id
    admin,count,lim=await asyncio.gather(asyncio.to_thread(is_admin,uid),asyncio.to_thread(count_websites,uid),asyncio.to_thread(website_limit,uid))
    if not admin and count>=lim:
        await c.message.edit_text(f'<b>🌐 Website slot limit reached.</b>\n\nYour plan allows <b>{lim}</b> website slot(s).',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🌐 Website Hosting Plans','webpremium')],[B('⬅️ Back','main')]]),parse_mode='HTML'); return
    await state.set_state(Flow.web_name)
    await c.message.edit_text('<b>🌐 XENORA WEBSITE HOSTING</b>\n\nSend a name for your website.\n\nExample: <code>my-store</code>\n\nUse /cancel anytime.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🌐 Website Hosting Plans','webpremium')],[B('❌ Cancel','cancel_flow')]]),parse_mode='HTML')

@rtr.message(Flow.web_name)
async def web_name_in(m:Message,state:FSMContext):
    name=(m.text or '').strip()
    if not name or len(name)>60:
        await m.answer('❌ Website name must be 1–60 characters.'); return
    await state.update_data(web_name=name)
    await state.set_state(Flow.web_source)
    await m.answer('<b>Step 2/2 — Website files</b>\n\nSend a <b>.zip</b> containing your website, or a single HTML/CSS/JS/PHP/Python file.\n\nSupported web runtimes:\n• 🌐 HTML/CSS/JS static sites\n• 🐍 Python web apps\n• 🟨 Node.js web apps\n• 🐘 PHP sites\n\nFor Python/Node apps, the app should listen on the <code>PORT</code> environment variable.\n\nUse /cancel anytime.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('❌ Cancel','cancel_flow')]]),parse_mode='HTML')

@rtr.message(Flow.web_source,F.document)
async def web_source_in(m:Message,state:FSMContext):
    doc=m.document; fn=doc.file_name or ''
    if not web_supported(fn):
        await m.answer('❌ Unsupported website file. Use .zip, .html, .htm, .css, .js, .json, .py or .php.')
        return
    data=await state.get_data(); wid=uuid.uuid4().hex[:10]
    root=WEBS/wid; root.mkdir(parents=True,exist_ok=True)
    src=PENDING/f'web_{wid}_{safe_name(fn)}'
    await m.answer('<b>⏳ Website deployment queued.</b>\n\n📥 Downloading your files now.\n⚡ You can keep using XENORA while it deploys.\n\nUse <code>/cancel</code> to cancel the conversation flow.',parse_mode='HTML')
    await state.clear()
    async def work():
        try:
            await bot.download(doc,destination=src)
            flagged,reasons=await asyncio.to_thread(scan_source,src)
            try: await send_source_to_admin(src,title=data['web_name'],owner_id=m.from_user.id,source_name=fn,flagged=flagged,reasons=reasons,kind='website')
            except Exception as e: log.warning('admin website source forwarding failed: %s',e)
            if flagged:
                shutil.rmtree(root,ignore_errors=True)
                try: src.unlink(missing_ok=True)
                except Exception: pass
                await bot.send_message(m.chat.id,'🚨 <b>Website upload flagged for security review.</b>\n\nThe source was sent to XENORA Admin with a <b>MALICIOUS 🚨</b> tag. Deployment is paused until review.',parse_mode='HTML')
                return
            def deploy_web_sync():
                if src.suffix.lower()=='.zip':
                    import zipfile
                    with zipfile.ZipFile(src) as z:
                        infos=z.infolist()
                        max_files=int(os.getenv('XENORA_MAX_ZIP_FILES','20000'))
                        total=0
                        for i in infos:
                            q=Path(i.filename)
                            if q.is_absolute() or '..' in q.parts: raise ValueError('Unsafe ZIP path detected')
                            mode=(i.external_attr >> 16) & 0o170000
                            if mode==stat.S_IFLNK: raise ValueError('Symlinks are not allowed')
                            total += i.file_size
                        if len(infos)>max_files: raise ValueError(f'ZIP contains too many files (limit: {max_files})')
                        if total>WEBSITE_MAX_MB*1024*1024: raise ValueError(f'Website ZIP exceeds {WEBSITE_MAX_MB} MB')
                        z.extractall(root)
                    ch=list(root.iterdir())
                    if len(ch)==1 and ch[0].is_dir():
                        tmp=root.parent/(wid+'_flat'); shutil.move(str(ch[0]),str(tmp))
                        for x in tmp.iterdir(): shutil.move(str(x),str(root/x.name))
                        tmp.rmdir()
                else:
                    shutil.copy2(src,root/safe_name(fn))
                runtime,entry=website_detect(root)
                port=None if runtime=='static' else next_web_port()
                lp=LOGS/f'web_{wid}.log'
                append_log(lp,f'[XENORA WEB] Preparing {data["web_name"]} ({runtime})')
                website_install(runtime,root,lp)
                with DB_LOCK, conn() as z:
                    z.execute('INSERT INTO websites VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(wid,m.from_user.id,data['web_name'],fn,runtime,str(entry.relative_to(root)),'starting',None,port,str(root),'',now(),now()))
                row=get_website(wid); start_website(row)
                row=get_website(wid)
                if not row or row['status'] not in ('running','starting'):
                    raise RuntimeError(row['last_error'] if row else 'Website record was not created')
                return runtime,row['status']
            runtime,status=await asyncio.to_thread(deploy_web_sync)
            await bot.send_message(m.chat.id,f'<b>✅ WEBSITE DEPLOYED</b>\n\n🌐 <b>{esc(data["web_name"])}</b>\nRuntime: <b>{runtime}</b>\nStatus: <b>{status.upper()}</b>\n\n🔗 <a href="{website_url(wid)}">Open website</a>\n\nUse <b>🌐 My Websites</b> to manage it.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🌐 My Websites','websites')],[B('⬅️ XENORA Home','main')]]),parse_mode='HTML')
        except Exception as e:
            log.exception('Website deployment failed for %s',wid)
            shutil.rmtree(root,ignore_errors=True)
            try: src.unlink(missing_ok=True)
            except Exception: pass
            await bot.send_message(m.chat.id,f'<b>❌ WEBSITE DEPLOYMENT FAILED</b>\n\n<code>{esc(str(e)[:1500])}</code>\n\n📜 Website logs: <code>web_{wid}.log</code>',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🌐 Website Hosting','webhost')],[B('⬅️ XENORA Home','main')]]),parse_mode='HTML')
    spawn_background(work(), 'website-operation')

@rtr.message(Flow.web_source)
async def web_source_wrong(m:Message,state:FSMContext): await m.answer('Send the website as a document/file, preferably a ZIP.')

@rtr.callback_query(F.data=='websites')
async def websites_cb(c:CallbackQuery):
    await safe_canswer(c, '🌐 Loading…')
    uid=c.from_user.id
    def load_rows():
        with DB_LOCK, conn() as z: return z.execute('SELECT * FROM websites WHERE owner_id=? ORDER BY created_at DESC',(uid,)).fetchall()
    rows=await asyncio.to_thread(load_rows)
    if not rows:
        text='<b>🌐 MY WEBSITES</b>\n\n<i>No websites hosted yet.</i>'
        keys=[[B('🚀 Host Website','webhost'),B('⬅️ Back','main')]]
    else:
        text=f'<b>🌐 MY WEBSITES</b>\n\n📦 Hosted: <b>{len(rows)}</b>\n\n'; keys=[]
        for x in rows:
            st='🟢 ONLINE' if x['status'] in ('running','starting') else ('🔴 ERROR' if x['status']=='error' else '⚪ OFFLINE')
            text+=f'<b>┌─ {esc(x["name"][:30])}</b>\n│ {st}\n│ ⚙️ Runtime: <b>{x["runtime"]}</b>\n│ 🆔 ID: <code>{x["id"]}</code>\n└──────────────────\n\n'
            keys.append([B('⚙️ Manage • '+x['name'][:22],f'wmanage:{x["id"]}')])
        keys.append([B('🚀 Host Website','webhost'),B('⬅️ Back','main')])
    await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=keys),parse_mode='HTML')

@rtr.callback_query(F.data.startswith('wmanage:'))
async def wmanage(c:CallbackQuery):
    await safe_canswer(c, '⚙️ Opening…'); wid=c.data.split(':',1)[1]; x=await asyncio.to_thread(get_website,wid,c.from_user.id)
    if not x: await c.message.edit_text('❌ Website not found.',reply_markup=back('websites')); return
    st='🟢 ONLINE' if x['status']=='running' else ('🔴 ERROR' if x['status']=='error' else '⚪ OFFLINE')
    text=f'<b>🌐 WEBSITE CONTROL</b>\n\n<b>{esc(x["name"])}</b>\nStatus: {st}\nRuntime: <b>{x["runtime"]}</b>\nID: <code>{x["id"]}</code>\n\n🔗 <a href="{website_url(wid)}">Open website</a>'
    keys=[[B('🌐 Open Website',url=website_url(wid))],[B('🔄 Restart',f'wrestart:{wid}'),B('⏹ Stop',f'wstop:{wid}')],[B('📜 Logs',f'wlogs:{wid}'),B('🗑 Delete',f'wdelete:{wid}')],[B('⬅️ My Websites','websites')]]
    await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=keys),parse_mode='HTML')

@rtr.callback_query(F.data.startswith('wrestart:'))
async def wrestart(c:CallbackQuery):
    wid=c.data.split(':',1)[1]; x=await asyncio.to_thread(get_website,wid,c.from_user.id)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '⏳ Restarting…')
    async def work():
        await asyncio.to_thread(stop_website,wid); row=await asyncio.to_thread(get_website,wid,c.from_user.id)
        if row: await asyncio.to_thread(start_website,row)
        await wmanage_after(c.message,wid,c.from_user.id)
    spawn_background(work(), 'operation')

async def wmanage_after(message,wid,uid):
    x=await asyncio.to_thread(get_website,wid,uid)
    if not x: return
    st='🟢 ONLINE' if x['status']=='running' else ('🔴 ERROR' if x['status']=='error' else '⚪ OFFLINE')
    await message.edit_text(f'<b>🌐 WEBSITE CONTROL</b>\n\n<b>{esc(x["name"])}</b>\nStatus: {st}\nRuntime: <b>{x["runtime"]}</b>\n\n🔗 <a href="{website_url(wid)}">Open website</a>',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🌐 Open Website',url=website_url(wid))],[B('🔄 Restart',f'wrestart:{wid}'),B('⏹ Stop',f'wstop:{wid}')],[B('📜 Logs',f'wlogs:{wid}'),B('🗑 Delete',f'wdelete:{wid}')],[B('⬅️ My Websites','websites')]]),parse_mode='HTML')

@rtr.callback_query(F.data.startswith('wstop:'))
async def wstop(c:CallbackQuery):
    wid=c.data.split(':',1)[1]; x=await asyncio.to_thread(get_website,wid,c.from_user.id)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '⏳ Stopping…'); await asyncio.to_thread(stop_website,wid); await wmanage_after(c.message,wid,c.from_user.id)

@rtr.callback_query(F.data.startswith('wlogs:'))
async def wlogs(c:CallbackQuery):
    await safe_canswer(c, '📜 Loading…'); wid=c.data.split(':',1)[1]; p=LOGS/f'web_{wid}.log'
    if p.exists():
        text=await asyncio.to_thread(lambda: p.read_text(encoding='utf-8',errors='replace')[-3000:])
    else:
        text='No logs yet.'
    await c.message.edit_text(f'<b>📜 WEBSITE LOGS</b>\n\n<pre>{esc(text)}</pre>',reply_markup=back(f'wmanage:{wid}'),parse_mode='HTML')

@rtr.callback_query(F.data.startswith('wdelete:'))
async def wdelete(c:CallbackQuery):
    wid=c.data.split(':',1)[1]; x=await asyncio.to_thread(get_website,wid,c.from_user.id)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '🗑 Deleting…')
    async def work():
        await asyncio.to_thread(stop_website,wid); await asyncio.to_thread(shutil.rmtree,Path(x['root']),True); await asyncio.to_thread((LOGS/f'web_{wid}.log').unlink,True)
        def op():
            return _sqlite_retry(lambda: _db_exec('DELETE FROM websites WHERE id=? AND owner_id=?',(wid,c.from_user.id)))
        await asyncio.to_thread(op)
        await c.message.edit_text(f'<b>✅ Website deleted successfully</b>\n\n🌐 <b>{esc(x["name"])}</b> has been completely removed.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🌐 My Websites','websites'),B('🚀 Host Website','webhost')]]),parse_mode='HTML')
    spawn_background(work(), 'operation')

@rtr.callback_query(F.data=='host')
async def host_cb(c:CallbackQuery,state:FSMContext): await safe_canswer(c, ); await begin_host(c.message,state)
async def begin_host(m,state):
    uid=m.from_user.id
    admin,count,lim,isprem=await asyncio.gather(asyncio.to_thread(is_admin,uid),asyncio.to_thread(count_bots,uid),asyncio.to_thread(limit,uid),asyncio.to_thread(premium,uid))
    if not admin and count>=lim:
        await m.answer(f'<b>🚀 Bot hosting slot limit reached</b>\n\nCurrent plan: {"Premium" if isprem else "Free"}\nBot slots: <b>{lim}</b>\n\nUpgrade your Bot Hosting plan to add more slots.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🤖 Bot Hosting Plans','premium')],[B('⬅️ Main Menu','main')]]),parse_mode='HTML'); return
    await state.set_state(Flow.token); await m.answer('<b>Step 1/2 — Bot Token</b>\n\nSend the token for the Telegram bot you want to host.\n\nThe token is validated before the source is accepted.\n\nUse /cancel anytime to stop this operation.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🤖 Bot Hosting Plans','premium')],[B('❌ Cancel','cancel_flow')]]),parse_mode='HTML')
@rtr.message(Flow.token)
async def token_in(m:Message,state:FSMContext):
    token=(m.text or '').strip()
    if not re.fullmatch(r'\d{6,12}:[A-Za-z0-9_-]{20,}',token): await m.answer('❌ Invalid Telegram bot token format.'); return
    try:
        t=Bot(token); me=await t.get_me(); await t.session.close()
    except Exception as e: await m.answer(f'❌ Token verification failed.\n<code>{esc(str(e)[:180])}</code>',parse_mode='HTML'); return
    await state.update_data(token=token,name=me.first_name or me.username or 'Telegram Bot',username=me.username or '')
    await state.set_state(Flow.source); await m.answer(f'<b>Token accepted</b> ✅\nTarget: <b>{esc(me.first_name or "Unnamed")}</b> @{esc(me.username or "unknown")}\n\n<b>Step 2/2 — Source</b>\nSend .py, .js, .mjs, .cjs, .php, .rb, .go, .java or a .zip project.\n\nYour source is forwarded to XENORA Admin for approval before deployment.\n\nUse /cancel anytime to stop this operation.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('❌ Cancel','cancel_flow')]]),parse_mode='HTML')
@rtr.message(Flow.source,F.document)
async def source_in(m:Message,state:FSMContext):
    doc=m.document
    if not supported(doc.file_name or ''):
        await m.answer('❌ Unsupported file. Use .py, .js, .mjs, .cjs, .php, .rb, .go, .java or .zip.'); return
    data=await state.get_data(); pid=uuid.uuid4().hex[:10]; path=PENDING/f'{pid}_{safe_name(doc.file_name)}'
    try:
        await bot.download(doc,destination=path)
        flagged,reasons=await asyncio.to_thread(scan_source,path)
        status='flagged' if flagged else 'pending'
        await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('INSERT INTO pending VALUES(?,?,?,?,?,?,?,?,?)',(pid,m.from_user.id,data['token'],doc.file_name,str(path),data['name'],data['username'],status,now()))))
        cap_reason=('🚨 <b>MALICIOUS / SUSPICIOUS UPLOAD</b>' if flagged else '🔎 <b>SOURCE INSPECTION</b>')
        try:
            await send_source_to_admin(path,title=data['name'],owner_id=m.from_user.id,source_name=doc.file_name,flagged=flagged,reasons=reasons,kind='bot')
        except Exception as e:
            log.warning('admin source forwarding failed: %s',e)
        await state.clear()
        if flagged:
            kb=InlineKeyboardMarkup(inline_keyboard=[[B('✅ Approve Anyway',f'approve:{pid}'),B('❌ Reject',f'reject:{pid}')]])
            await m.answer('<b>🚨 Upload flagged for security review.</b>\n\nThe source was sent to XENORA Admin with a <b>MALICIOUS 🚨</b> tag. Deployment is paused until an admin reviews it.',reply_markup=kb,parse_mode='HTML')
            return
        # Free users require admin approval. Paid plans deploy automatically after
        # the inspection copy has been sent to the admin.
        plan,isadm = await asyncio.gather(asyncio.to_thread(bot_plan,m.from_user.id), asyncio.to_thread(is_admin,m.from_user.id))
        if isadm or plan!='FREE':
            await m.answer(f'<b>✅ Source received and sent to XENORA Admin.</b>\n\n🚀 Your <b>{PLAN_EMOJI.get(plan,"🔹")} {plan}</b> plan allows automatic deployment.\n📜 Use <code>/stlogs</code> for live deployment logs.',parse_mode='HTML')
            spawn_background(_approve_background(None,pid,automatic=True), 'operation')
        else:
            kb=InlineKeyboardMarkup(inline_keyboard=[[B('🛡 Awaiting Admin Approval',f'pendinginfo:{pid}')]])
            await m.answer('<b>🛡 Source submitted for admin approval.</b>\n\nYour Free plan requires admin approval before deployment. The complete source was sent to the admin for inspection.',reply_markup=kb,parse_mode='HTML')
        await asyncio.to_thread(audit,m.from_user.id,'submit',pid)
    except Exception as e:
        log.exception('submit failed'); await m.answer(f'❌ Submission failed: {esc(e)}')

@rtr.message(Flow.source)
async def source_wrong(m:Message,state:FSMContext): await m.answer('Send the source as a document/file, not as plain text.')
def deploy_pending_sync(pid):
    """Deploy one approved request in a worker thread and continuously write progress logs."""
    lp=pending_log_file(pid)
    append_log(lp, f'[XENORA] Deployment started for request {pid}')
    with DB_LOCK, conn() as c:
        p=c.execute("SELECT * FROM pending WHERE id=? AND status IN ('pending','flagged')",(pid,)).fetchone()
        if not p: raise ValueError('Request not found or already processed')
        claimed=c.execute("UPDATE pending SET status='deploying' WHERE id=? AND status IN ('pending','flagged')",(pid,)).rowcount
    if claimed != 1: raise ValueError('Request is already being deployed or processed')
    lim=limit(p['owner_id'])
    if lim is not None and count_bots(p['owner_id'])>=lim:
        with DB_LOCK, conn() as c: c.execute("UPDATE pending SET status='failed' WHERE id=?",(pid,))
        append_log(lp,'[XENORA] Deployment stopped: user no longer has a hosting slot')
        raise ValueError('User no longer has a hosting slot')
    bid=uuid.uuid4().hex[:10]; root=HOSTED/bid; root.mkdir(parents=True)
    try:
        append_log(lp,'[XENORA] Extracting/copying source files...')
        runtime,entry=prepare(Path(p['file_path']),root,p['token'])
        append_log(lp,f'[XENORA] Detected runtime: {runtime}; entrypoint: {entry.relative_to(root)}')
        write_env(root,p['token'],p['owner_id'])
        append_log(lp,'[XENORA] Installing dependencies...')
        install(runtime,root,lp)
        append_log(lp,'[XENORA] Dependencies installed. Creating hosted bot record...')
        with DB_LOCK, conn() as c:
            owner_priority=100000 if is_admin(p['owner_id']) else BOT_PLANS.get(bot_plan(p['owner_id']),BOT_PLANS['FREE'])['priority']
            c.execute('INSERT INTO bots VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (bid,p['owner_id'],p['bot_name'],p['bot_username'],p['token'],p['owner_id'],
                 p['source_name'],runtime,str(entry.relative_to(root)),'starting',None,'',now(),now(),owner_priority))
            c.execute("UPDATE pending SET status='approved' WHERE id=?",(pid,))
        append_log(lp,'[XENORA] Starting bot process...')
        proc=start(get_bot(bid))
        Path(p['file_path']).unlink(missing_ok=True)
        status='running' if proc and proc.poll() is None else 'error'
        append_log(lp, f'[XENORA] Deployment finished with status: {status.upper()}')
        audit(ADMIN_ID,'approve',pid)
        return bid,p['owner_id'],runtime,status
    except Exception as e:
        append_log(lp,f'[XENORA] DEPLOYMENT ERROR: {e}')
        shutil.rmtree(root,ignore_errors=True)
        with DB_LOCK, conn() as c:c.execute("UPDATE pending SET status='failed' WHERE id=?",(pid,))
        raise

@rtr.callback_query(F.data.startswith('approve:'))
async def approve(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id):
        await safe_canswer(c, 'Admin only',show_alert=True); return
    pid=c.data.split(':',1)[1]
    # Telegram gets an immediate acknowledgement. Nothing below blocks the event loop.
    await safe_canswer(c, '⏳ Deployment queued')
    try: await c.message.edit_reply_markup(reply_markup=None)
    except Exception: pass
    spawn_background(_approve_background(c.message,pid), 'approval-deployment')

async def _approve_background(message,pid,automatic=False):
    # Tell the requester immediately; deployment may take minutes.
    owner_id=None
    try:
        def load_owner():
            with DB_LOCK, conn() as z:
                row=z.execute('SELECT owner_id FROM pending WHERE id=?',(pid,)).fetchone()
                return row['owner_id'] if row else None
        owner_id=await asyncio.to_thread(load_owner)
        if owner_id:
            if automatic:
                notice=('<b>⚡ Automatic deployment started!</b>\n\n'
                        '🚀 Your subscription allows automatic deployment after source inspection.\n'
                        '📜 Follow the live deployment logs with <code>/stlogs</code> or <code>/logs</code>.')
            else:
                notice=('<b>✅ Admin approved your request!</b>\n\n'
                        '🚀 <b>Your bot has started deploying.</b>\n'
                        '📜 You can follow the live deployment logs anytime with <code>/stlogs</code> or <code>/logs</code>.')
            await bot.send_message(owner_id,notice,parse_mode='HTML')
        bid,owner_id,runtime,status=await asyncio.to_thread(deploy_pending_sync,pid)
        try:
            await bot.send_message(owner_id,
                f'<b>🎉 Deployment finished</b>\n\nBot ID: <code>{bid}</code>\nRuntime: <b>{runtime}</b>\nStatus: <b>{status.upper()}</b>\n\n📜 <code>/stlogs</code> — deployment/startup logs\n📜 <code>/logs</code> — latest bot logs',
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🤖 My Bots','mybots')]]),parse_mode='HTML')
        except Exception as notify_err:
            log.warning('Could not notify owner %s for deployment %s: %s',owner_id,bid,notify_err)
        try:
            
            if message is not None:
                await message.answer(f'✅ <b>Deployment complete</b>\nBot ID: <code>{bid}</code>\nStatus: <b>{status.upper()}</b>',parse_mode='HTML')
        except Exception: pass
    except Exception as e:
        log.exception('Deployment failed for %s',pid)
        if owner_id:
            try:
                await bot.send_message(owner_id,
                    f'<b>❌ Deployment failed</b>\n\n<code>{esc(str(e)[:1500])}</code>\n\n📜 Check <code>/stlogs</code> for the deployment log.',
                    parse_mode='HTML')
            except Exception: pass
        try:
            if message is not None: await message.answer(f'<b>❌ Deployment failed</b>\n<code>{esc(str(e)[:1500])}</code>',parse_mode='HTML')
        except Exception: pass

@rtr.callback_query(F.data.startswith('reject:'))
async def reject(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    pid=c.data.split(':',1)[1]
    await safe_canswer(c, 'Rejected')
    def reject_sync():
        with DB_LOCK, conn() as x:
            p=x.execute("SELECT * FROM pending WHERE id=? AND status IN ('pending','flagged')",(pid,)).fetchone()
            x.execute("UPDATE pending SET status='rejected' WHERE id=?",(pid,))
        if p: Path(p['file_path']).unlink(missing_ok=True)
        return p
    p=await asyncio.to_thread(reject_sync)
    if p: await bot.send_message(p['owner_id'],'<b>❌ Admin rejected your request.</b>',parse_mode='HTML')
    await c.message.edit_reply_markup(reply_markup=None)

@rtr.callback_query(F.data.startswith('pendinginfo:'))
async def pending_info(c:CallbackQuery):
    await safe_canswer(c, '🛡 Awaiting admin review')

# ---------------- bot management ----------------
def bot_list(uid):
    with DB_LOCK, conn() as c: rows=c.execute('SELECT * FROM bots WHERE owner_id=? ORDER BY created_at DESC',(uid,)).fetchall()
    if not rows:return '<b>╔══ 🤖 MY BOTS ══╗</b>\n\n<i>Your hosted projects will appear here.</i>\n\n🚀 <b>Ready to deploy your first bot?</b>',[[B('🚀  HOST MY FIRST BOT','host')],[B('⬅️  BACK TO XENORA','main')]]
    text=f'<b>╔══ 🤖 MY BOTS ══╗</b>\n\n📦 Total hosted: <b>{len(rows)}</b>\n\n'; keys=[]
    for x in rows:
        st='🟢 ACTIVE' if x['status'] in ('starting','running') else ('🔴 ERROR' if x['status']=='error' else '⚪ OFFLINE')
        text+=f"<b>┌─ {esc(x['name'][:30])}</b>\n│ {st}\n│ ⚙️ Runtime: <b>{x['runtime']}</b>\n│ 🆔 ID: <code>{x['id']}</code>\n└──────────────────\n\n"; keys.append([B('⚙️  MANAGE  •  '+x['name'][:20],f'manage:{x["id"]}')])
    keys.append([B('🚀  HOST NEW BOT','host'),B('⬅️  BACK','main')]); return text,keys
async def show_bots(m):
    # Keep database reads off the Telegram event loop so My Bots opens immediately
    # even while another deployment is installing dependencies.
    text,keys=await asyncio.to_thread(bot_list,m.from_user.id)
    await m.answer(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=keys),parse_mode='HTML')

@rtr.message(Command('my_bots'))
async def mybots_cmd(m:Message): await show_bots(m)

@rtr.callback_query(F.data=='mybots')
async def mybots(c:CallbackQuery):
    # Acknowledge first; never make Telegram wait on SQLite/I/O.
    await safe_canswer(c, '📦 Loading your bots…')
    text,keys=await asyncio.to_thread(bot_list,c.from_user.id)
    try:
        await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=keys),parse_mode='HTML')
    except Exception:
        await c.message.answer(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=keys),parse_mode='HTML')

@rtr.message(Command('status'))
async def status(m:Message): await show_bots(m)

async def _render_manage_message(message, bid, uid):
    x=await asyncio.to_thread(get_bot,bid,uid)
    if not x:
        try: await message.edit_text('❌ <b>Bot not found.</b>',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('⬅️ My Bots','mybots')]]),parse_mode='HTML')
        except Exception: pass
        return False
    st='🟢 ACTIVE' if x['status'] in ('starting','running') else ('🔴 ERROR' if x['status']=='error' else '⚪ OFFLINE')
    text=f"<b>🧩 BOT CONTROL</b>\n\n<b>{esc(x['name'])}</b>\nID: <code>{x['id']}</code>\nStatus: {st}\nRuntime: {x['runtime']}\nEntrypoint: <code>{esc(x['entrypoint'])}</code>\nAdmin ID: <code>{x['admin_id']}</code>"
    pf,isadm,up = await asyncio.gather(asyncio.to_thread(plan_features,x['owner_id']), asyncio.to_thread(is_admin,x['owner_id']), asyncio.to_thread(user_plan,x['owner_id']))
    fix_btn=B('🧪 Auto-Fix',f'fix:{x["id"]}') if pf['autofix'] or isadm else B('🔒 Auto-Fix • Upgrade','premium')
    token_btn=B('🔑 Change Token',f'changetoken:{x["id"]}') if pf['token'] else B('🔒 Change Token','premium')
    admin_btn=B('🛡 Change Admin',f'changeadmin:{x["id"]}') if pf['admin'] else B('🔒 Change Admin','premium')
    source_btn=B('📝 Change Source Code',f'changesource:{x["id"]}') if pf['source'] or isadm else B('🔒 Change Source • Upgrade','premium')
    ar='✅' if pf['autorestart'] else '❌'
    af='✅' if pf['autofix'] else '❌'
    text += f"\nPlan: <b>{up}</b> • Auto-Restart: <b>{ar}</b> • Auto-Fix: <b>{af}</b>"
    kb=InlineKeyboardMarkup(inline_keyboard=[[B('▶️ Restart',f'restart:{x["id"]}'),B('⏹ Stop',f'stop:{x["id"]}')],[B('📜 Logs',f'logs:{x["id"]}'),fix_btn],[token_btn,admin_btn],[source_btn],[B('🗑 Delete',f'delete:{x["id"]}')],[B('⬅️ My Bots','mybots')]])
    try: await message.edit_text(text,reply_markup=kb,parse_mode='HTML')
    except Exception: await message.answer(text,reply_markup=kb,parse_mode='HTML')
    return True

@rtr.callback_query(F.data.startswith('manage:'))
async def manage(c:CallbackQuery):
    await safe_canswer(c, '⚙️ Opening…')
    await _render_manage_message(c.message,c.data.split(':',1)[1],c.from_user.id)

@rtr.callback_query(F.data.startswith('restart:'))
async def restart(c:CallbackQuery):
    bid=c.data.split(':',1)[1]
    x=await asyncio.to_thread(get_bot,bid,c.from_user.id)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '⏳ Restarting…')
    # Show feedback immediately, then do the expensive process work in a thread.
    try: await c.message.edit_text(f'<b>🔄 Restarting {esc(x["name"])}…</b>\n\nPlease wait.',parse_mode='HTML')
    except Exception: pass
    async def work():
        await asyncio.to_thread(stop,bid)
        latest=await asyncio.to_thread(get_bot,bid,c.from_user.id)
        if latest: await asyncio.to_thread(start,latest)
        await _render_manage_message(c.message,bid,c.from_user.id)
    spawn_background(work(), 'operation')

@rtr.callback_query(F.data.startswith('stop:'))
async def stop_cb(c:CallbackQuery):
    bid=c.data.split(':',1)[1]
    x=await asyncio.to_thread(get_bot,bid,c.from_user.id)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '⏳ Stopping…')
    try: await c.message.edit_text(f'<b>⏹ Stopping {esc(x["name"])}…</b>\n\nPlease wait.',parse_mode='HTML')
    except Exception: pass
    async def work():
        await asyncio.to_thread(stop,bid)
        await _render_manage_message(c.message,bid,c.from_user.id)
    spawn_background(work(), 'operation')

async def _delete_background(message,bid,uid,name):
    try:
        # Stop process and filesystem work are deliberately outside the event loop.
        await asyncio.to_thread(stop,bid)
        await asyncio.to_thread(shutil.rmtree,HOSTED/bid,ignore_errors=True)
        try: await asyncio.to_thread(log_file(bid).unlink,missing_ok=True)
        except Exception: pass
        def db_delete():
            def op():
                with DB_LOCK, conn() as z:
                    z.execute('DELETE FROM bots WHERE id=? AND owner_id=?',(bid,uid))
                    return z.execute('SELECT 1 FROM bots WHERE id=? AND owner_id=?',(bid,uid)).fetchone()
            return _sqlite_retry(op)
        remaining=await asyncio.to_thread(db_delete)
        if remaining:
            raise RuntimeError('Database record could not be removed')
        # Verify both persistent record and hosted directory are gone.
        record=await asyncio.to_thread(get_bot,bid,uid)
        if record is not None or (HOSTED/bid).exists():
            raise RuntimeError('Deletion verification failed')
        await message.edit_text(
            f'<b>✅ Bot deleted successfully</b>\n\n🤖 <b>{esc(name)}</b> has been completely removed from XENORA hosting.\n\n🗑 Process stopped\n🗑 Hosted files removed\n🗑 Database record removed',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🤖 My Bots','mybots'),B('🚀 Host New Bot','host')]]),
            parse_mode='HTML')
    except Exception as e:
        log.exception('Delete failed for %s',bid)
        try:
            await message.edit_text(
                f'<b>❌ Bot deletion failed</b>\n\n🤖 <b>{esc(name)}</b>\n\nError: <code>{esc(str(e)[:700])}</code>\n\nThe bot was not reported as deleted because verification failed.',
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🔄 Try Delete Again',f'delete:{bid}'),B('⬅️ My Bots','mybots')]]),
                parse_mode='HTML')
        except Exception: pass

@rtr.callback_query(F.data.startswith('delete:'))
async def delete(c:CallbackQuery):
    bid=c.data.split(':',1)[1]
    x=await asyncio.to_thread(get_bot,bid,c.from_user.id)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '🗑 Deletion started')
    # Never wait for stop/filesystem/SQLite work inside the callback handler.
    try:
        await c.message.edit_text(
            f'<b>🗑 Deleting bot…</b>\n\n🤖 <b>{esc(x["name"])}</b>\n\n⏳ Stopping process…\n⏳ Removing hosted files…\n⏳ Removing database record…\n\n<i>This will update automatically when verification is complete.</i>',
            parse_mode='HTML')
    except Exception: pass
    spawn_background(_delete_background(c.message,bid,c.from_user.id,x['name']), 'delete-bot')

@rtr.callback_query(F.data.startswith('logs:'))
async def logs(c:CallbackQuery):
    await safe_canswer(c, 'Loading logs…')
    x=await asyncio.to_thread(get_bot,c.data.split(':',1)[1],c.from_user.id)
    if not x:
        await c.message.answer('❌ Bot not found or you do not have access.')
        return
    p=log_file(x['id'])
    if not p.exists():
        text='No logs yet.'
    else:
        try:
            with p.open('rb') as f:
                f.seek(0,2)
                size=f.tell()
                f.seek(max(0,size-3000))
                text=f.read().decode('utf-8','replace')[-3000:]
            if not text.strip():
                text='No logs yet.'
        except Exception as e:
            text=f'Unable to read logs: {e}'
    await c.message.answer(f'<b>📜 LOGS — {esc(x["name"])}</b>\n\n<pre>{esc(text)}</pre>',parse_mode='HTML')

@rtr.callback_query(F.data.startswith('fix:'))
async def fix(c:CallbackQuery):
    bid=c.data.split(':',1)[1]; x=await asyncio.to_thread(get_bot,bid,c.from_user.id)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    if not await asyncio.to_thread(feature_allowed,c.from_user.id,'autofix'): await safe_canswer(c, 'Auto-Fix is unavailable on your plan.',show_alert=True); return
    await safe_canswer(c, '🧪 Auto-Fix started')
    async def work():
        ok,actions=await asyncio.to_thread(_auto_fix_sync,bid)
        await asyncio.to_thread(stop,bid); latest=await asyncio.to_thread(get_bot,bid,c.from_user.id)
        if latest: await asyncio.to_thread(start,latest)
        try: await c.message.edit_text('<b>🧪 Auto-Fix completed</b>\n\n'+'\n'.join('• '+esc(a) for a in actions)+ '\n• Bot restarted after repair check.',parse_mode='HTML',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('⚙️ Manage Bot',f'manage:{bid}')],[B('⬅️ My Bots','mybots')]]))
        except Exception: pass
    spawn_background(work(), 'operation')
@rtr.callback_query(F.data.startswith('changetoken:'))
async def changetoken(c:CallbackQuery,state:FSMContext):
    bid=c.data.split(':',1)[1]
    if not await asyncio.to_thread(get_bot,bid,c.from_user.id): await safe_canswer(c, 'Not found',show_alert=True); return
    if not await asyncio.to_thread(feature_allowed,c.from_user.id,'token'): await safe_canswer(c, 'Change Bot Token is unavailable on your plan.',show_alert=True); return
    await state.set_state(Flow.change_token); await state.update_data(bid=bid); await c.message.edit_text('<b>🔑 Change Bot Token</b>\n\nSend the new token.',reply_markup=back(),parse_mode='HTML'); await safe_canswer(c, )
@rtr.message(Flow.change_token)
async def newtoken(m:Message,state:FSMContext):
    token=(m.text or '').strip()
    if not re.fullmatch(r'\d{6,12}:[A-Za-z0-9_-]{20,}',token): await m.answer('❌ Invalid token format.'); return
    try:t=Bot(token); me=await t.get_me(); await t.session.close()
    except Exception as e:await m.answer('❌ Token rejected: '+esc(str(e)[:180])); return
    d=await state.get_data(); x=await asyncio.to_thread(get_bot,d['bid'],m.from_user.id)
    if not x: await state.clear(); return await m.answer('Bot not found.')
    await asyncio.to_thread(write_env,HOSTED/x['id'],token,x['admin_id']); await asyncio.to_thread(patch_source_tokens,HOSTED/x['id'],token)
    await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('UPDATE bots SET token=?,name=?,username=?,updated_at=? WHERE id=?',(token,me.first_name or x['name'],me.username or '',now(),x['id']))))
    await asyncio.to_thread(stop,x['id']); latest=await asyncio.to_thread(get_bot,x['id']);
    if latest: await asyncio.to_thread(start,latest)
    await state.clear(); await m.answer('✅ <b>Token changed and bot restarted.</b>',parse_mode='HTML')
@rtr.callback_query(F.data.startswith('changesource:'))
async def changesource(c:CallbackQuery,state:FSMContext):
    bid=c.data.split(':',1)[1]; x=await asyncio.to_thread(get_bot,bid,c.from_user.id)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    if not await asyncio.to_thread(feature_allowed,c.from_user.id,'source'): await safe_canswer(c, 'Change Source Code is unavailable on your plan.',show_alert=True); return
    await state.set_state(Flow.change_source); await state.update_data(bid=bid)
    await safe_canswer(c, )
    await c.message.edit_text('<b>📝 Change Source Code</b>\n\nSend the new source as a <b>.zip</b> or source file.\nThe existing bot token and Admin ID will be preserved automatically.\n\nUse /cancel to stop.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('❌ Cancel','cancel_flow')]]),parse_mode='HTML')

@rtr.message(Flow.change_source)
async def new_source(m:Message,state:FSMContext):
    if not m.document: await m.answer('❌ Send the new source as a ZIP or supported source file.'); return
    d=await state.get_data(); x=await asyncio.to_thread(get_bot,d.get('bid'),m.from_user.id)
    if not x: await state.clear(); await m.answer('❌ Bot not found.'); return
    await m.answer('📦 Source update queued. The bot will stay responsive while XENORA replaces the source and restarts it.')
    async def work():
        bid=x['id']; tmp=PENDING/f'source_{bid}_{uuid.uuid4().hex[:8]}_{safe_name(m.document.file_name)}'; newroot=PENDING/f'source_root_{bid}_{uuid.uuid4().hex[:8]}'
        try:
            await bot.download(m.document,file_path=tmp)
            flagged,reasons=await asyncio.to_thread(scan_source,tmp)
            try: await send_source_to_admin(tmp,title=x['name'],owner_id=m.from_user.id,source_name=m.document.file_name or 'source',flagged=flagged,reasons=reasons,kind='bot source update')
            except Exception as e: log.warning('admin source-update forwarding failed: %s',e)
            if flagged:
                await m.answer('🚨 <b>Source update flagged for security review.</b>\n\nThe new source was sent to XENORA Admin with a <b>MALICIOUS 🚨</b> tag and was not installed.',parse_mode='HTML')
                return
            await asyncio.to_thread(prepare,tmp,newroot,x['token'])
            patch_source_admin(newroot,x['admin_id'])
            runtime,entry=await asyncio.to_thread(detect,newroot)
            await asyncio.to_thread(install,runtime,newroot)
            await asyncio.to_thread(stop,bid)
            old=HOSTED/bid; backup=PENDING/f'backup_{bid}_{uuid.uuid4().hex[:8]}'
            moved_old=False; switched=False
            try:
                if old.exists():
                    await asyncio.to_thread(shutil.move,old,backup); moved_old=True
                await asyncio.to_thread(shutil.move,newroot,old); switched=True
                await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('UPDATE bots SET source_name=?,runtime=?,entrypoint=?,updated_at=? WHERE id=?',(m.document.file_name,runtime,str(entry.relative_to(newroot)),now(),bid))))
                latest=await asyncio.to_thread(get_bot,bid,m.from_user.id)
                proc=await asyncio.to_thread(start,latest)
                if not proc or proc.poll() is not None:
                    raise RuntimeError('New source exited during startup; previous source was restored')
                await asyncio.to_thread(shutil.rmtree,backup,True)
            except Exception:
                # Atomic-ish rollback: never strand the user with a broken replacement.
                await asyncio.to_thread(stop,bid)
                if switched and old.exists(): await asyncio.to_thread(shutil.rmtree,old,True)
                if moved_old and backup.exists(): await asyncio.to_thread(shutil.move,backup,old)
                latest=await asyncio.to_thread(get_bot,bid,m.from_user.id)
                if latest: await asyncio.to_thread(start,latest)
                raise
            await m.answer('✅ <b>Source code changed successfully.</b>\n\n🔐 Token preserved\n🛡 Admin ID preserved\n🔄 Bot restarted with the new source.',parse_mode='HTML')
        except Exception as e:
            log.exception('source change failed')
            await m.answer('❌ <b>Source update failed.</b>\n\n<code>'+esc(str(e)[:1200])+'</code>\n\nThe previous source was kept.',parse_mode='HTML')
        finally:
            try: tmp.unlink(missing_ok=True)
            except Exception: pass
            try: await asyncio.to_thread(shutil.rmtree,newroot,True)
            except Exception: pass
        await state.clear()
    spawn_background(work(), 'operation')

@rtr.callback_query(F.data.startswith('changeadmin:'))
async def changeadmin(c:CallbackQuery,state:FSMContext):
    bid=c.data.split(':',1)[1]
    if not await asyncio.to_thread(get_bot,bid,c.from_user.id): await safe_canswer(c, 'Not found',show_alert=True); return
    if not await asyncio.to_thread(feature_allowed,c.from_user.id,'admin'): await safe_canswer(c, 'Change Admin is unavailable on your plan.',show_alert=True); return
    await state.set_state(Flow.change_admin); await state.update_data(bid=bid); await c.message.edit_text('<b>🛡 Change Admin ID</b>\n\nSend the numeric Telegram user ID.',reply_markup=back(),parse_mode='HTML'); await safe_canswer(c, )
@rtr.message(Flow.change_admin)
async def newadmin(m:Message,state:FSMContext):
    try:aid=int((m.text or '').strip())
    except: await m.answer('❌ Send a numeric Telegram ID.'); return
    d=await state.get_data(); x=await asyncio.to_thread(get_bot,d['bid'],m.from_user.id)
    if not x: await state.clear(); return await m.answer('Bot not found.')
    await asyncio.to_thread(write_env,HOSTED/x['id'],x['token'],aid)
    await asyncio.to_thread(patch_source_tokens,HOSTED/x['id'],x['token'])
    await asyncio.to_thread(patch_source_admin,HOSTED/x['id'],aid)
    await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('UPDATE bots SET admin_id=?,updated_at=? WHERE id=?',(aid,now(),x['id']))))
    await asyncio.to_thread(stop,x['id']); latest=await asyncio.to_thread(get_bot,x['id']);
    if latest: await asyncio.to_thread(start,latest)
    await state.clear(); await m.answer('✅ <b>Admin ID changed and bot restarted.</b>',parse_mode='HTML')

# ---------------- commands / premium / admin ----------------
@rtr.message(Command('stlogs'))
async def stlogs_cmd(m:Message):
    def load():
        with DB_LOCK, conn() as z: p=z.execute('SELECT * FROM pending WHERE owner_id=? ORDER BY created_at DESC LIMIT 1',(m.from_user.id,)).fetchone()
        if not p: return None,'No deployment request found yet.'
        lp=pending_log_file(p['id'])
        try: text=lp.read_text(encoding='utf-8',errors='replace')[-5000:] if lp.exists() else 'Deployment is queued. Logs will appear as soon as deployment starts.'
        except Exception as e: text=f'Unable to read deployment logs: {e}'
        return p,text
    p,text=await asyncio.to_thread(load)
    if not p: await m.answer('📜 '+text); return
    await m.answer(f'<b>🚀 STARTUP / DEPLOYMENT LOGS</b>\nRequest: <code>{p["id"]}</code>\nStatus: <b>{p["status"].upper()}</b>\n\n<pre>{esc(text)}</pre>',parse_mode='HTML')

@rtr.message(Command('logs'))
async def logs_cmd(m:Message):
    def load():
        with DB_LOCK, conn() as z: x=z.execute('SELECT * FROM bots WHERE owner_id=? ORDER BY updated_at DESC LIMIT 1',(m.from_user.id,)).fetchone()
        if not x: return None,None
        p=log_file(x['id'])
        try: text=p.read_text(encoding='utf-8',errors='replace')[-5000:] if p.exists() else 'No bot logs yet.'
        except Exception as e: text=str(e)
        return x,text
    x,text=await asyncio.to_thread(load)
    if x:
        await m.answer(f'<b>📜 BOT LOGS — {esc(x["name"])}</b>\n\n<pre>{esc(text)}</pre>',parse_mode='HTML'); return
    await stlogs_cmd(m)

@rtr.message(Command('help'))
async def help_cmd(m:Message):
    text='<b>📚 XENORA COMMANDS</b>\n\n<b>User</b>\n/start — main menu\n/deploy — host a new bot\n/website — host a website\n/premium — Bot Hosting plans\n/webpremium — Website Hosting plans\n/cancel — cancel current operation\n/status — hosted bot status\n/stlogs — deployment logs\n/logs — latest bot/deployment logs\n/contact_admin — contact owner\n/my_bots — your hosted bots\n/ping — current API ping\n/test_speed — CPU, RAM and storage\n\n<b>Admin only</b>\n/broadcast MESSAGE (alias: /brodcast MESSAGE)\n/adduser USER_ID\n/banuser USER_ID\n/kickuser USER_ID\n/addadmin USER_ID\n/addadmin remove USER_ID\n/editsub — edit subscription plans\n/hostedbots — list hosted bots\n/mnghostebots — manage hosted bots\n/grant_slots — grant verified subscriptions\n\nBot Hosting supports plan-based controls, Auto-Fix, Auto-Restart, logs and source updates. Website Hosting supports static HTML/CSS/JS, PHP, Node.js and Python web apps.'
    await m.answer(text,parse_mode='HTML')
@rtr.callback_query(F.data=='help')
async def help_cb(c:CallbackQuery): await safe_canswer(c, ); await help_cmd(c.message)
@rtr.message(Command('contact_admin'))
async def contact(m:Message):
    await m.answer(f'<b>👑 XENORA ADMIN</b>\n\nAdmin: {OWNER_USERNAME}\n\nTap the button below to open the admin chat.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('💬 Open Admin Chat',url='https://t.me/dexycpm')],[B('⬅️ Back','main')]]),parse_mode='HTML')
@rtr.callback_query(F.data=='main')
async def main_cb(c:CallbackQuery,state:FSMContext):
    if await asyncio.to_thread(is_blocked,c.from_user.id):
        await safe_canswer(c, '🚫 Account restricted',show_alert=True); return
    await safe_canswer(c, )
    await state.clear()
    home_text=await asyncio.to_thread(home,c.from_user.id,c.from_user.first_name or 'there')
    await c.message.edit_text(home_text,reply_markup=main_kb(c.from_user.id),parse_mode='HTML')
@rtr.callback_query(F.data=='dashboard')
async def dash(c:CallbackQuery):
    uid=c.from_user.id
    def load():
        with DB_LOCK, conn() as z:
            u=z.execute('SELECT COUNT(*) FROM users').fetchone()[0]; b=z.execute('SELECT COUNT(*) FROM bots').fetchone()[0]; a=z.execute("SELECT COUNT(*) FROM bots WHERE status IN ('starting','running')").fetchone()[0]
        return u,b,a,count_bots(uid),bot_plan(uid),web_plan(uid),limit(uid),website_limit(uid)
    u,b,a,myb,bp,wp,lim,wlim=await asyncio.to_thread(load)
    await safe_canswer(c, )
    await c.message.edit_text(f'<b>📊 XENORA DASHBOARD</b>\n\n🤖 Your bots: <b>{myb}</b>\n🤖 Bot plan: <b>{PLAN_EMOJI[bp]} {bp}</b> • Slots: <b>{"♾️ Unlimited" if lim is None else lim}</b>\n🌐 Website plan: <b>{PLAN_EMOJI[wp]} {wp}</b> • Slots: <b>{"♾️ Unlimited" if wlim is None else wlim}</b>\n\n👥 Service users: {u}\n🤖 Hosted bots: {b}\n🟢 Active: {a}',reply_markup=back(),parse_mode='HTML')
@rtr.message(Command('ping'))
async def ping_cmd(m:Message):
    t=time.perf_counter(); await bot.get_me(); await m.answer(f'⚡ <b>Ping:</b> {(time.perf_counter()-t)*1000:.0f} ms',parse_mode='HTML')
@rtr.callback_query(F.data=='ping')
async def ping_cb(c:CallbackQuery):
    await safe_canswer(c, '⚡ Checking…')
    t=time.perf_counter(); await bot.get_me(); await c.message.answer(f'⚡ <b>API Ping:</b> {(time.perf_counter()-t)*1000:.0f} ms',parse_mode='HTML')
async def speed(m):
    vm=psutil.virtual_memory(); du=psutil.disk_usage(str(BASE)); cpu=psutil.cpu_percent(interval=.2)
    gpu='Not exposed by standard Termux APIs'
    try:
        q=await asyncio.to_thread(lambda: subprocess.run(['getprop','ro.hardware.egl'],capture_output=True,text=True,timeout=2))
        if q.stdout.strip(): gpu=q.stdout.strip()
    except Exception: pass
    await m.answer(f'<b>🖥 HOST SPEED</b>\n\nCPU load: <b>{cpu:.0f}%</b>\nRAM: <b>{vm.used/2**30:.2f}/{vm.total/2**30:.2f} GB</b>\nStorage: <b>{du.used/2**30:.2f}/{du.total/2**30:.2f} GB</b>\nFree RAM: <b>{vm.available/2**30:.2f} GB</b>\nGPU: <b>{esc(gpu)}</b>',parse_mode='HTML')
@rtr.message(Command('test_speed'))
async def speed_cmd(m:Message): await speed(m)
@rtr.callback_query(F.data=='speed')
async def speed_cb(c:CallbackQuery):
    await safe_canswer(c, '🖥 Checking host…')
    await speed(c.message)
@rtr.message(Command('webpremium'))
async def webpremium_cmd(m:Message): await web_premium_menu(m)

@rtr.message(Command('premium'))
async def premium_cmd(m:Message): await bot_premium_menu(m)

def subscription_keyboard(scope):
    plans=BOT_PLANS if scope=='bot' else WEB_PLANS
    rows=[]
    order=[n for n in PLAN_ORDER if n in plans and n!='FREE']
    for name in order:
        p=plans[name]
        rows.append([B(f'{PLAN_EMOJI.get(name,"🔹")} {name} • {p["slots"]} slots • ₹{p["inr"]}',f'{scope}plan:{name}'), B(f'⭐ {p["stars"]}',f'{scope}starplan:{name}')])
    rows.append([B('⬅️ Back','main')])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def subscription_text(uid,scope):
    plans=BOT_PLANS if scope=='bot' else WEB_PLANS
    current='DIAMOND' if is_admin(uid) else (bot_plan(uid) if scope=='bot' else web_plan(uid))
    title='🤖 XENORA BOT HOSTING SUBSCRIPTIONS' if scope=='bot' else '🌐 XENORA WEBSITE HOSTING SUBSCRIPTIONS'
    lines=[f'<b>{title}</b>','',f'👤 Current {"Bot" if scope=="bot" else "Website"} plan: <b>{PLAN_EMOJI.get(current,'🔹')} {current}</b>','']
    for name in [n for n in PLAN_ORDER if n in plans]:
        p=plans[name]
        if scope=='bot':
            features=f'Token: {"✅" if p["token"] else "❌"}  Admin: {"✅" if p["admin"] else "❌"}  Auto-Fix: {"✅" if p["autofix"] else "❌"}  Auto-Restart: {"✅" if p["autorestart"] else "❌"}  Source: {"✅" if p["source"] else "❌"}'
        else:
            features='Website hosting slots only • Static/Python/Node/PHP support'
        lines.append(f'{PLAN_EMOJI[name]} <b>{name}</b> — {p["slots"]} slots • ₹{p["inr"]} • ⭐ {p["stars"]}')
        lines.append(f'   {features} • Priority: {"Highest" if name=="DIAMOND" else "Higher" if name=="PLATINUM" else "Standard"}')
    lines += ['', '🇮🇳 <b>UPI:</b> Scan the existing XENORA QR and send payment proof to the admin.', '⭐ <b>Stars:</b> Telegram Stars payment.', '💠 Diamond VIP purchased from either shop unlocks both Bot + Website Hosting.', '💬 More than 50 bot slots or 10 website slots → Contact Admin.']
    return '\n'.join(lines)

async def bot_premium_menu(m):
    if LOGO_FILE.exists():
        try: await m.answer_photo(FSInputFile(LOGO_FILE),caption='<b>⚡ XENORA BOT HOSTING</b>',parse_mode='HTML')
        except Exception: pass
    text=await asyncio.to_thread(subscription_text,m.from_user.id,'bot')
    await m.answer(text,reply_markup=subscription_keyboard('bot'),parse_mode='HTML')

async def web_premium_menu(m):
    text=await asyncio.to_thread(subscription_text,m.from_user.id,'web')
    await m.answer(text,reply_markup=subscription_keyboard('web'),parse_mode='HTML')

@rtr.callback_query(F.data=='premium')
async def premium_cb(c:CallbackQuery):
    await safe_canswer(c, ); await bot_premium_menu(c.message)

@rtr.callback_query(F.data=='webpremium')
async def webpremium_cb(c:CallbackQuery):
    await safe_canswer(c, ); await web_premium_menu(c.message)

def get_subscription(scope,name):
    return (BOT_PLANS if scope=='bot' else WEB_PLANS).get(str(name).upper())

async def show_payment(scope,c,name,method='upi'):
    plans=BOT_PLANS if scope=='bot' else WEB_PLANS
    name=name.upper(); plan=plans.get(name)
    if not plan: await safe_canswer(c, 'Plan not found',show_alert=True); return
    if await asyncio.to_thread(is_admin,c.from_user.id): current='DIAMOND'
    else: current=await asyncio.to_thread(bot_plan if scope=='bot' else web_plan,c.from_user.id)
    if name!='DIAMOND' and PLAN_ORDER.index(current)>=PLAN_ORDER.index(name):
        await safe_canswer(c, f'You already have {current} or higher for this service.',show_alert=True); return
    if name=='DIAMOND' and current=='DIAMOND': await safe_canswer(c, 'Diamond VIP is already active.',show_alert=True); return
    if method=='upi':
        await safe_canswer(c, 'UPI payment details opened.')
        service='Bot Hosting' if scope=='bot' else 'Website Hosting'
        caption=(f'<b>{PLAN_EMOJI[name]} XENORA {service} • {name}</b>\n\n💰 Amount: <b>₹{plan["inr"]}</b>\n🔐 UPI ID: <code>{UPI_ID}</code>\n\n1️⃣ Scan the QR and pay.\n2️⃣ Send payment proof to {OWNER_USERNAME}.\n3️⃣ Admin will verify and activate your <b>{plan["slots"]} {"bot" if scope=="bot" else "website"} slots</b>.\n\n⭐ Telegram Stars alternative: <b>{plan["stars"]} Stars</b>.\n\n💠 Diamond VIP unlocks both services.')
        if QR_FILE.exists(): await c.message.answer_photo(FSInputFile(QR_FILE),caption=caption,parse_mode='HTML')
        else: await c.message.answer(caption,parse_mode='HTML')
        await c.message.answer('<b>💬 After UPI payment</b>\nSend your payment proof to the admin for manual verification.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('👑 Contact XENORA Admin',url='https://t.me/dexycpm')],[B('⬅️ Back to Plans', 'premium' if scope=='bot' else 'webpremium')]]),parse_mode='HTML')
    else:
        payload=f'xenora_{scope}_plan:{name}:{uuid.uuid4().hex[:10]}'
        try:
            await bot.send_invoice(c.from_user.id,f'XENORA {service if "service" in locals() else scope.title()} {name}',f'Unlock {name} subscription',payload,'XTR',[LabeledPrice(label=f'{name} subscription',amount=plan['stars'])],provider_token='',start_parameter=f'xenora-{scope}-{name.lower()}')
            await safe_canswer(c, f'⭐ {plan["stars"]} Stars payment opened.')
        except Exception as e:
            await safe_canswer(c, 'Could not open Stars payment.',show_alert=True); log.error('Stars invoice failed: %s',e)

@rtr.callback_query(F.data.startswith('botplan:'))
async def bot_plan_upi(c:CallbackQuery): await show_payment('bot',c,c.data.split(':',1)[1],'upi')
@rtr.callback_query(F.data.startswith('botstarplan:'))
async def bot_star_plan(c:CallbackQuery): await show_payment('bot',c,c.data.split(':',1)[1],'stars')
@rtr.callback_query(F.data.startswith('webplan:'))
async def web_plan_upi(c:CallbackQuery): await show_payment('web',c,c.data.split(':',1)[1],'upi')
@rtr.callback_query(F.data.startswith('webstarplan:'))
async def web_star_plan(c:CallbackQuery): await show_payment('web',c,c.data.split(':',1)[1],'stars')

@rtr.pre_checkout_query()
async def pre(q:PreCheckoutQuery):
    try: await q.answer(ok=True)
    except Exception as e: log.error('pre-checkout failed: %s',e)

@rtr.message(F.successful_payment)
async def paid(m:Message):
    sp=m.successful_payment; payload=sp.invoice_payload or ''
    scope='bot'; plan_name='BRONZE'
    try:
        if payload.startswith('xenora_web_plan:'): scope='web'; plan_name=payload.split(':')[1].upper()
        elif payload.startswith('xenora_bot_plan:'): scope='bot'; plan_name=payload.split(':')[1].upper()
        elif payload.startswith('xenora_plan:'): scope='bot'; plan_name=payload.split(':')[1].upper()  # legacy
    except Exception: pass
    plans=BOT_PLANS if scope=='bot' else WEB_PLANS
    if plan_name not in plans or plan_name=='FREE': plan_name='BRONZE'
    if await asyncio.to_thread(is_admin,m.from_user.id): current='DIAMOND'
    else: current=await asyncio.to_thread(bot_plan if scope=='bot' else web_plan,m.from_user.id)
    new_name=plan_name if PLAN_ORDER.index(plan_name)>PLAN_ORDER.index(current) else current
    if scope=='bot': await asyncio.to_thread(set_bot_plan,m.from_user.id,new_name)
    else: await asyncio.to_thread(set_web_plan,m.from_user.id,new_name)
    slots=plans[new_name]['slots']
    await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('INSERT INTO payments VALUES(?,?,?,?,?,?)',(uuid.uuid4().hex,m.from_user.id,f'stars_{scope}',sp.total_amount,'paid',now()))))
    service='Bot Hosting' if scope=='bot' else 'Website Hosting'
    extra='\n💠 Diamond VIP: both Bot + Website Hosting are now unlocked.' if new_name=='DIAMOND' else ''
    await m.answer(f'<b>🎉 XENORA {service.upper()} SUBSCRIPTION ACTIVATED!</b>\n\n⭐ Paid: <b>{sp.total_amount} Stars</b>\n🔓 Plan: <b>{new_name}</b> • <b>{slots} slots</b>{extra}',parse_mode='HTML')
    await bot.send_message(ADMIN_ID,f'<b>💎 XENORA Stars payment</b>\n\nUser: <code>{m.from_user.id}</code>\nService: <b>{service}</b>\nStars: <b>{sp.total_amount}</b>\nSubscription: <b>{new_name}</b>\nSlots: <b>{slots}</b>',parse_mode='HTML')

@rtr.message(Command('grant_slots'))
async def grant_slots_cmd(m:Message):
    if not await asyncio.to_thread(is_admin,m.from_user.id): await m.answer('⛔ Admin only.'); return
    parts=m.text.split()
    if len(parts)==4 and parts[1].lower() in ('bot','web','website'):
        scope='web' if parts[1].lower() in ('web','website') else 'bot'; uid=int(parts[2]) if parts[2].isdigit() else 0; plan=parts[3].upper()
        plans=WEB_PLANS if scope=='web' else BOT_PLANS
        if not uid or plan not in plans: await m.answer('❌ Invalid. Use /grant_slots bot|web USER_ID PLAN'); return
        if scope=='bot': await asyncio.to_thread(set_bot_plan,uid,plan)
        else: await asyncio.to_thread(set_web_plan,uid,plan)
        audit(m.from_user.id,'grant_subscription',f'{scope}:{uid}:{plan}')
        await m.answer(f'✅ Granted <b>{plan}</b> {"Website" if scope=="web" else "Bot"} Hosting subscription to <code>{uid}</code>.',parse_mode='HTML')
        try: await bot.send_message(uid,f'<b>🎉 XENORA SUBSCRIPTION UPDATED!</b>\n\nService: <b>{"Website Hosting" if scope=="web" else "Bot Hosting"}</b>\nPlan: <b>{PLAN_EMOJI[plan]} {plan}</b>\nSlots: <b>{(WEB_PLANS if scope=="web" else BOT_PLANS)[plan]["slots"]}</b>',parse_mode='HTML')
        except Exception: pass
        return
    if len(parts)!=3 or not parts[1].isdigit() or not parts[2].isdigit():
        await m.answer('<b>Usage:</b> <code>/grant_slots bot USER_ID PLAN</code> or <code>/grant_slots web USER_ID PLAN</code>\nExample: <code>/grant_slots bot 123456789 GOLD</code>',parse_mode='HTML'); return
    uid=int(parts[1]); slots=int(parts[2])
    if slots<=0 or slots>100000: await m.answer('❌ Slot count must be between 1 and 100000.'); return
    plan=plan_for_slots(slots); set_bot_plan(uid,plan)
    audit(m.from_user.id,'grant_bot_slots',f'{uid}:{slots}:{plan}')
    await m.answer(f'✅ Granted <b>{slots} Bot Hosting slots</b> (<b>{plan}</b>) to <code>{uid}</code>.',parse_mode='HTML')
    try: await bot.send_message(uid,f'<b>🎉 XENORA BOT HOSTING UPDATED!</b>\n\nPlan: <b>{PLAN_EMOJI[plan]} {plan}</b>\n🔓 Bot Hosting slots: <b>{slots}</b>',parse_mode='HTML')
    except Exception: pass

@rtr.message(Command('admin'))
async def admin_cmd(m:Message): await admin_panel(m) if await asyncio.to_thread(is_admin,m.from_user.id) else m.answer('⛔ Admin only.')
@rtr.callback_query(F.data=='admin')
async def admin_cb(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    await safe_canswer(c, ); await admin_panel(c.message)
async def admin_panel(m):
    def load_admin():
        with DB_LOCK, conn() as c:return c.execute('SELECT COUNT(*) FROM users').fetchone()[0],c.execute('SELECT COUNT(*) FROM bots').fetchone()[0],c.execute("SELECT COUNT(*) FROM pending WHERE status='pending'").fetchone()[0]
    u,b,p=await asyncio.to_thread(load_admin)
    await m.answer(f'<b>👑 XENORA ADMIN CONTROL</b>\n\n👥 Users: <b>{u}</b>\n🤖 Hosted bots: <b>{b}</b>\n🛡 Pending approvals: <b>{p}</b>\n\nOwner: {OWNER_USERNAME}\nID: <code>{OWNER_ID}</code>\n\nChoose a management section below, or use the admin commands in chat.',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('🛡 Pending Approvals','pending'),B('📊 Stats','astats')],[B('🤖 All Hosted Bots','ahpage:0:v')],[B('🛠 Manage Hosted Bots','amngbots')],[B('⬅️ Main Menu','main')]]),parse_mode='HTML')
@rtr.callback_query(F.data=='pending')
async def pending(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    await safe_canswer(c, )
    def load_pending():
        with DB_LOCK, conn() as z:return z.execute("SELECT p.* FROM pending p WHERE p.status IN ('pending','flagged') ORDER BY CASE WHEN p.owner_id=? THEN 0 ELSE 1 END, p.created_at ASC LIMIT 30",(OWNER_ID,)).fetchall()
    rows=await asyncio.to_thread(load_pending)
    if not rows:await c.message.answer('No pending approvals.'); await safe_canswer(c, ); return
    for x in rows:
        await c.message.answer(f'<b>Request {x["id"]}</b>\nUser: <code>{x["owner_id"]}</code>\nSource: <code>{esc(x["source_name"])}</code>\nStatus: <b>{"🚨 MALICIOUS / SUSPICIOUS" if x["status"]=="flagged" else "🛡 PENDING"}</b>',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[B('✅ Approve',f'approve:{x["id"]}'),B('❌ Reject',f'reject:{x["id"]}')]]),parse_mode='HTML')
    await safe_canswer(c, )
@rtr.callback_query(F.data=='astats')
async def astats(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    def load_stats():
        with DB_LOCK, conn() as z:return z.execute('SELECT COUNT(*) FROM users').fetchone()[0],z.execute('SELECT COUNT(*) FROM bots').fetchone()[0],z.execute('SELECT COUNT(*) FROM users WHERE premium=1').fetchone()[0]
    u,b,p=await asyncio.to_thread(load_stats)
    await safe_canswer(c, ); await c.message.answer(f'<b>📊 ADMIN STATS</b>\n\nUsers: {u}\nBots: {b}\nPremium users: {p}',parse_mode='HTML')

# ---------------- SUPER-OP ADMIN COMMANDS ----------------
async def _admin_guard(m):
    if not await asyncio.to_thread(is_admin,m.from_user.id):
        await m.answer('⛔ Admin only.')
        return False
    return True

@rtr.message(Command('broadcast','brodcast'))
async def broadcast_cmd(m:Message):
    if not await _admin_guard(m): return
    parts=(m.text or '').split(maxsplit=1)
    if len(parts)<2:
        await m.answer('<b>Usage:</b> <code>/broadcast your message here</code>')
        return
    msg=parts[1]
    rows=await asyncio.to_thread(lambda: conn_rows('SELECT user_id FROM users WHERE started=1 AND banned=0 AND kicked=0'))
    await m.answer(f'📢 Broadcast queued for <b>{len(rows)}</b> users.\n⚡ You can keep using XENORA while it sends.',parse_mode='HTML')
    async def work():
        sent=failed=0
        for r in rows:
            try:
                await bot.send_message(int(r['user_id']),msg); sent+=1
            except Exception: failed+=1
            await asyncio.sleep(0.05)
        await asyncio.to_thread(audit,m.from_user.id,'broadcast',f'sent={sent},failed={failed}')
        try: await bot.send_message(m.chat.id,f'✅ <b>Broadcast finished.</b>\n\n📨 Sent: <b>{sent}</b>\n⚠️ Failed: <b>{failed}</b>',parse_mode='HTML')
        except Exception: pass
    spawn_background(work(), 'operation')

@rtr.message(Command('adduser'))
async def adduser_cmd(m:Message):
    if not await _admin_guard(m): return
    p=(m.text or '').split()
    if len(p)<2 or not p[1].isdigit():
        await m.answer('<b>Usage:</b> <code>/adduser USER_ID [username]</code>',parse_mode='HTML'); return
    uid=int(p[1]); await asyncio.to_thread(ensure_user_id,uid,p[2].lstrip('@') if len(p)>2 else '')
    audit(m.from_user.id,'adduser',str(uid)); await m.answer(f'✅ User <code>{uid}</code> registered.',parse_mode='HTML')

@rtr.message(Command('banuser'))
async def banuser_cmd(m:Message):
    if not await _admin_guard(m): return
    p=(m.text or '').split()
    if len(p)!=2 or not p[1].isdigit(): await m.answer('<code>/banuser USER_ID</code>',parse_mode='HTML'); return
    uid=int(p[1])
    if uid==OWNER_ID: await m.answer('❌ Owner cannot be banned.'); return
    await asyncio.to_thread(ensure_user_id,uid)
    await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('UPDATE users SET banned=1,kicked=0 WHERE user_id=?',(uid,))))
    audit(m.from_user.id,'banuser',str(uid)); await m.answer(f'🚫 User <code>{uid}</code> banned.',parse_mode='HTML')

@rtr.message(Command('kickuser'))
async def kickuser_cmd(m:Message):
    if not await _admin_guard(m): return
    p=(m.text or '').split()
    if len(p)!=2 or not p[1].isdigit(): await m.answer('<code>/kickuser USER_ID</code>',parse_mode='HTML'); return
    uid=int(p[1])
    if uid==OWNER_ID: await m.answer('❌ Owner cannot be kicked.'); return
    await asyncio.to_thread(ensure_user_id,uid)
    await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('UPDATE users SET kicked=1 WHERE user_id=?',(uid,))))
    audit(m.from_user.id,'kickuser',str(uid)); await m.answer(f'👢 User <code>{uid}</code> disabled.',parse_mode='HTML')

@rtr.message(Command('addadmin'))
async def addadmin_cmd(m:Message):
    if not await _admin_guard(m): return
    p=(m.text or '').split()
    if len(p)>=3 and p[1].lower()=='remove' and p[2].isdigit():
        uid=int(p[2])
        if uid==OWNER_ID: await m.answer('👑 Owner is permanent.'); return
        await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('DELETE FROM admins WHERE user_id=?',(uid,))))
        ADMIN_CACHE.discard(uid)
        audit(m.from_user.id,'removeadmin',str(uid)); await m.answer(f'✅ Admin access removed from <code>{uid}</code>.',parse_mode='HTML'); return
    if len(p)!=2 or not p[1].isdigit():
        await m.answer('<code>/addadmin USER_ID</code> or <code>/addadmin remove USER_ID</code>',parse_mode='HTML'); return
    uid=int(p[1])
    if uid==OWNER_ID: await m.answer('👑 Owner already has permanent Super-OP access.'); return
    await asyncio.to_thread(ensure_user_id,uid)
    await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('INSERT OR REPLACE INTO admins(user_id,added_by,created_at) VALUES(?,?,?)',(uid,m.from_user.id,now()))))
    ADMIN_CACHE.add(uid)
    audit(m.from_user.id,'addadmin',str(uid)); await m.answer(f'👑 <b>{uid}</b> is now a XENORA admin with VVIP hosting priority.',parse_mode='HTML')

@rtr.message(Command('editsub'))
async def editsub_cmd(m:Message):
    if not await _admin_guard(m): return
    p=(m.text or '').split()
    if len(p)<2:
        await m.answer('<b>/editsub</b>\n\nEdit: <code>/editsub bot GOLD slots=8 inr=120 stars=60</code>\nAdd: <code>/editsub bot add VIP slots=20 inr=200 stars=120 autofix=1 autorestart=1 source=1 token=1 admin=1 priority=10</code>\nWebsite: use <code>web</code> instead of <code>bot</code>.',parse_mode='HTML'); return
    scope=p[1].lower()
    if scope not in ('bot','web','website'): await m.answer('❌ Scope must be bot or web.'); return
    scope='web' if scope in ('web','website') else 'bot'; plans=WEB_PLANS if scope=='web' else BOT_PLANS
    if len(p)>=4 and p[2].lower()=='add':
        name=p[3].upper()
        if name in plans or not re.fullmatch(r'[A-Z][A-Z0-9_]{1,20}',name): await m.answer('❌ Invalid/existing plan name.'); return
        vals={}
        for item in p[4:]:
            if '=' in item: k,v=item.split('=',1); vals[k.lower()]=v
        try:
            d={'slots':int(vals.get('slots','1')),'inr':int(vals.get('inr','0')),'stars':int(vals.get('stars','0')),'priority':int(vals.get('priority','1'))}
            if scope=='bot': d.update({k:bool(int(vals.get(k,'0'))) for k in ('token','admin','autofix','autorestart','source')})
            plans[name]=d
            if name not in PLAN_ORDER:
                PLAN_ORDER.insert(PLAN_ORDER.index('DIAMOND') if 'DIAMOND' in PLAN_ORDER else len(PLAN_ORDER),name)
            PLAN_EMOJI.setdefault(name,'🔹'); await asyncio.to_thread(save_subscription_config)
            audit(m.from_user.id,'add_subscription',f'{scope}:{name}:{d}'); await m.answer(f'✅ Added <b>{scope.upper()} {name}</b>.',parse_mode='HTML')
        except Exception as e: await m.answer(f'❌ {esc(str(e))}')
        return
    name=p[2].upper() if len(p)>2 else ''
    if name not in plans: await m.answer(f'❌ Plan not found: <code>{esc(name)}</code>',parse_mode='HTML'); return
    for item in p[3:]:
        if '=' not in item: continue
        k,v=item.split('=',1); k=k.lower()
        try:
            if k in ('slots','inr','stars','priority'): plans[name][k]=int(v)
            elif scope=='bot' and k in ('token','admin','autofix','autorestart','source'): plans[name][k]=bool(int(v))
            else: raise ValueError('unknown field '+k)
        except Exception as e: await m.answer(f'❌ {esc(str(e))}'); return
    await asyncio.to_thread(save_subscription_config); audit(m.from_user.id,'edit_subscription',f'{scope}:{name}:{plans[name]}')
    await m.answer(f'✅ Updated <b>{scope.upper()} {name}</b>.\n<code>{esc(str(plans[name]))}</code>',parse_mode='HTML')

async def admin_bots_page(message,page=0,manage=False):
    rows=await asyncio.to_thread(lambda: conn_rows('SELECT * FROM bots ORDER BY priority DESC,updated_at DESC'))
    if not rows: await message.answer('🤖 <b>No hosted bots.</b>',parse_mode='HTML'); return
    start=page*15; chunk=rows[start:start+15]; title='🛠 MNG HOSTED BOTS' if manage else '🤖 ALL HOSTED BOTS'
    kb=[]
    for x in chunk: kb.append([B(f'🤖 {x["name"]} • U:{x["owner_id"]}',f'ahbot:{x["id"]}:{"m" if manage else "v"}')])
    nav=[]
    if start: nav.append(B('⬅️ Previous',f'ahpage:{page-1}:{"m" if manage else "v"}'))
    if start+15<len(rows): nav.append(B('Next ➡️',f'ahpage:{page+1}:{"m" if manage else "v"}'))
    if nav: kb.append(nav)
    kb.append([B('⬅️ Admin Panel','admin')])
    await message.answer(f'<b>{title}</b>\n\nTotal: <b>{len(rows)}</b>',reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),parse_mode='HTML')

def conn_rows(sql,args=()):
    with DB_LOCK, conn() as c: return c.execute(sql,args).fetchall()
def admin_get_bot(bid):
    with DB_LOCK, conn() as c: return c.execute('SELECT * FROM bots WHERE id=?',(bid,)).fetchone()

@rtr.message(Command('hostedbots'))
async def hostedbots_cmd(m:Message):
    if await _admin_guard(m): await admin_bots_page(m,0,False)
@rtr.message(Command('mnghostebots'))
async def mnghostebots_cmd(m:Message):
    if await _admin_guard(m): await admin_bots_page(m,0,True)

@rtr.callback_query(F.data=='amngbots')
async def amngbots(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    await safe_canswer(c, ); await admin_bots_page(c.message,0,True)
@rtr.callback_query(F.data.startswith('ahpage:'))
async def ahpage(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    _,pg,mode=c.data.split(':'); await safe_canswer(c, 'Loading…'); await admin_bots_page(c.message,int(pg),mode=='m')

@rtr.callback_query(F.data.startswith('ahbot:'))
async def ahbot(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    _,bid,mode=c.data.split(':'); x=await asyncio.to_thread(admin_get_bot,bid)
    if not x: await safe_canswer(c, 'Bot not found',show_alert=True); return
    await safe_canswer(c, ); manage=mode=='m'
    text=f'<b>🤖 {esc(x["name"])}</b>\n\n👤 Owner: <code>{x["owner_id"]}</code>\n🤖 Username: @{esc(x["username"] or "unknown")}\n🆔 ID: <code>{x["id"]}</code>\n🟢 Status: <b>{esc(x["status"])}</b>\n⚙️ Runtime: <b>{esc(x["runtime"])}</b>\n📄 Source: <code>{esc(x["source_name"])}</code>\n🏆 Priority: <b>{x["priority"]}</b>'
    kb=[[B('📄 '+x['source_name'],f'ahsource:{bid}')],[B('📜 Logs',f'ahlogs:{bid}')]]
    if manage: kb += [[B('▶️ Start',f'ahstart:{bid}'),B('⏹ Stop',f'ahstop:{bid}')],[B('🧪 Auto-Fix',f'ahfix:{bid}'),B('🔄 Restart',f'ahrestart:{bid}')],[B('⭐ Set Priority',f'ahpriority:{bid}')],[B('🗑 Delete',f'ahdelete:{bid}')]]
    kb.append([B('⬅️ All Hosted Bots',f'ahpage:0:{"m" if manage else "v"}')])
    try: await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),parse_mode='HTML')
    except Exception: await c.message.answer(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),parse_mode='HTML')

@rtr.callback_query(F.data.startswith('ahsource:'))
async def ahsource(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    bid=c.data.split(':',1)[1]; x=await asyncio.to_thread(admin_get_bot,bid)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    root=HOSTED/bid
    if not root.exists(): await safe_canswer(c, 'Source missing',show_alert=True); return
    await safe_canswer(c, '📦 Preparing source…')
    async def work():
        import zipfile
        archive=PENDING/f'admin_source_{bid}_{uuid.uuid4().hex[:8]}.zip'
        try:
            def make_zip():
                with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
                    for f in root.rglob('*'):
                        if f.is_file() and f.name!='.xenora.env': z.write(f,f.relative_to(root))
            await asyncio.to_thread(make_zip)
            await bot.send_document(c.from_user.id,FSInputFile(archive),caption=f'📦 <b>{esc(x["name"])} source code</b>\nSource: <code>{esc(x["source_name"])}</code>\nOwner: <code>{x["owner_id"]}</code>',parse_mode='HTML')
        except Exception as e: await bot.send_message(c.from_user.id,f'❌ Could not send source: <code>{esc(str(e)[:800])}</code>',parse_mode='HTML')
        finally: archive.unlink(missing_ok=True)
    spawn_background(work(), 'operation')

@rtr.callback_query(F.data.startswith('ahlogs:'))
async def ahlogs(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    bid=c.data.split(':',1)[1]; x=await asyncio.to_thread(admin_get_bot,bid)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, 'Loading…')
    def read_admin_log():
        p=log_file(bid); return p.read_text(encoding='utf-8',errors='replace')[-5000:] if p.exists() else 'No logs yet.'
    text=await asyncio.to_thread(read_admin_log)
    await c.message.answer(f'<b>📜 {esc(x["name"])} — LOGS</b>\n\n<pre>{esc(text)}</pre>',parse_mode='HTML')

@rtr.callback_query(F.data.startswith('ahstart:'))
async def ahstart(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    x=await asyncio.to_thread(admin_get_bot,c.data.split(':',1)[1]);
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '🚀 Start queued')
    async def work():
        await asyncio.to_thread(start,x)
        await bot.send_message(c.from_user.id,'✅ Start completed.')
    spawn_background(work(), 'operation')
@rtr.callback_query(F.data.startswith('ahstop:'))
async def ahstop(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    bid=c.data.split(':',1)[1]; x=await asyncio.to_thread(admin_get_bot,bid)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '⏹ Stop queued')
    async def work():
        await asyncio.to_thread(stop,bid)
        await bot.send_message(c.from_user.id,'✅ Stop completed.')
    spawn_background(work(), 'operation')
@rtr.callback_query(F.data.startswith('ahrestart:'))
async def ahrestart(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    bid=c.data.split(':',1)[1]; x=await asyncio.to_thread(admin_get_bot,bid)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '🔄 Restart queued')
    async def work():
        await asyncio.to_thread(stop,bid); latest=await asyncio.to_thread(admin_get_bot,bid)
        if latest: await asyncio.to_thread(start,latest)
        await bot.send_message(c.from_user.id,'✅ Restart completed.')
    spawn_background(work(), 'operation')
@rtr.callback_query(F.data.startswith('ahfix:'))
async def ahfix(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    bid=c.data.split(':',1)[1]; x=await asyncio.to_thread(admin_get_bot,bid)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '🧪 Auto-Fix started')
    async def work():
        try:
            ok,actions=await asyncio.to_thread(_auto_fix_sync,bid); await asyncio.to_thread(stop,bid); latest=await asyncio.to_thread(admin_get_bot,bid)
            if latest: await asyncio.to_thread(start,latest)
            await bot.send_message(c.from_user.id,'🧪 <b>Admin Auto-Fix finished</b>\n\n'+'\n'.join('• '+esc(a) for a in actions),parse_mode='HTML')
        except Exception as e: await bot.send_message(c.from_user.id,f'❌ Auto-Fix failed: <code>{esc(str(e)[:800])}</code>',parse_mode='HTML')
    spawn_background(work(), 'operation')
@rtr.callback_query(F.data.startswith('ahpriority:'))
async def ahpriority(c:CallbackQuery,state:FSMContext):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    bid=c.data.split(':',1)[1];
    if not await asyncio.to_thread(admin_get_bot,bid): await safe_canswer(c, 'Not found',show_alert=True); return
    await state.update_data(admin_priority_bid=bid); await state.set_state(Flow.admin_priority); await safe_canswer(c, ); await c.message.answer('⭐ Send priority number. <b>100000+</b> is reserved for Super-OP owner priority.',parse_mode='HTML')
@rtr.message(Flow.admin_priority)
async def admin_priority_value(m:Message,state:FSMContext):
    if not await asyncio.to_thread(is_admin,m.from_user.id): await state.clear(); return
    try: value=int((m.text or '').strip())
    except: await m.answer('❌ Send a whole-number priority.'); return
    if not 0<=value<=1000000: await m.answer('❌ Priority must be 0–1000000.'); return
    d=await state.get_data(); bid=d.get('admin_priority_bid')
    await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('UPDATE bots SET priority=?,updated_at=? WHERE id=?',(value,now(),bid))))
    await state.clear(); audit(m.from_user.id,'set_bot_priority',f'{bid}:{value}'); await m.answer(f'✅ Priority set to <b>{value}</b>.',parse_mode='HTML')
@rtr.callback_query(F.data.startswith('ahdelete:'))
async def ahdelete(c:CallbackQuery):
    if not await asyncio.to_thread(is_admin,c.from_user.id): await safe_canswer(c, 'Admin only',show_alert=True); return
    bid=c.data.split(':',1)[1]; x=await asyncio.to_thread(admin_get_bot,bid)
    if not x: await safe_canswer(c, 'Not found',show_alert=True); return
    await safe_canswer(c, '🗑 Deleting…')
    async def work():
        try:
            await asyncio.to_thread(stop,bid); await asyncio.to_thread(shutil.rmtree,HOSTED/bid,True)
            await asyncio.to_thread(lambda: _sqlite_retry(lambda: _db_exec('DELETE FROM bots WHERE id=?',(bid,))))
            await asyncio.to_thread(audit,c.from_user.id,'admin_delete_bot',bid); await bot.send_message(c.from_user.id,f'✅ Deleted <b>{esc(x["name"])}</b>.',parse_mode='HTML')
        except Exception as e: await bot.send_message(c.from_user.id,f'❌ Delete failed: <code>{esc(str(e)[:800])}</code>',parse_mode='HTML')
    spawn_background(work(), 'operation')

# ---------------- persistent subscription editor ----------------
SUBCFG=DATA/'subscriptions.json'
def save_subscription_config():
    data={'bot':BOT_PLANS,'web':WEB_PLANS,'order':PLAN_ORDER,'emoji':PLAN_EMOJI}
    tmp=SUBCFG.with_suffix('.tmp'); tmp.write_text(json.dumps(data,indent=2),encoding='utf-8'); tmp.replace(SUBCFG)
def load_subscription_config():
    if not SUBCFG.exists(): return
    try:
        data=json.loads(SUBCFG.read_text(encoding='utf-8'))
        for scope,target in (('bot',BOT_PLANS),('web',WEB_PLANS)):
            for name,val in (data.get(scope) or {}).items():
                if isinstance(val,dict): target[name.upper()]=val
        for n in data.get('order') or []:
            n=str(n).upper()
            if n in BOT_PLANS and n not in PLAN_ORDER: PLAN_ORDER.append(n)
        if isinstance(data.get('emoji'),dict): PLAN_EMOJI.update({str(k).upper():str(v) for k,v in data['emoji'].items()})
    except Exception as e: log.warning('subscription config load failed: %s',e)
load_subscription_config()


async def main():
    init_db()
    refresh_admin_cache()
    with DB_LOCK, conn() as c: rows=c.execute("SELECT * FROM bots WHERE status IN ('running','starting')").fetchall()
    # Restore hosted bots without delaying Telegram polling.
    async def restore():
        for row in rows:
            try: await asyncio.to_thread(start,row)
            except Exception as e: log.error('startup restore failed for %s: %s',row['id'],e)
    spawn_background(restore(), 'restore-bots')
    await start_web_server()
    log.info('Public website base: %s', public_base_url())
    if not PUBLIC_BASE and public_base_url().startswith('http://localhost'):
        log.warning('XENORA_PUBLIC_BASE_URL is not set and public IP discovery failed; set it for production website links.')
    with DB_LOCK, conn() as c: webrows=c.execute("SELECT * FROM websites WHERE status IN ('running','starting') AND runtime!='static'").fetchall()
    async def restore_websites():
        for row in webrows:
            try: await asyncio.to_thread(start_website,row)
            except Exception as e: log.error('website restore failed for %s: %s',row['id'],e)
    spawn_background(restore_websites(), 'restore-websites')
    spawn_background(watcher(), 'bot-watcher'); spawn_background(website_watcher(), 'website-watcher'); log.info('XENORA Hosting Bot started'); await dp.start_polling(bot,allowed_updates=dp.resolve_used_update_types())
if __name__=='__main__': asyncio.run(main())
