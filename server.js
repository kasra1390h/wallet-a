const express=require('express'),crypto=require('crypto'),fs=require('fs'),zlib=require('zlib'),path=require('path');
const Database=require('better-sqlite3'),multer=require('multer');
const T=process.env.BOT_TOKEN,ADMINS=(process.env.ADMIN_IDS||'').split(',').filter(Boolean).map(Number);
if(!T){console.error('FATAL: BOT_TOKEN is not set');process.exit(1)}
if(!ADMINS.length){console.error('FATAL: ADMIN_IDS is not set — without it nobody can access the admin panel');process.exit(1)}
if(process.env.DEV==='1'){
 if(process.env.ALLOW_DEV!=='1'){console.error('FATAL: DEV=1 is set but ALLOW_DEV=1 is not. Refusing to start — DEV bypasses Telegram auth and must never run with real user money. Set ALLOW_DEV=1 only on your own local test machine.');process.exit(1)}
 console.warn('WARNING: DEV=1 + ALLOW_DEV=1 — Telegram signature checks are bypassed. NEVER set these on Render/Railway/production.')}
const DATA=process.env.DATA_DIR||'./data';
fs.mkdirSync(DATA+'/receipts',{recursive:true});fs.mkdirSync(DATA+'/tgs',{recursive:true});
const db=new Database(DATA+'/shop.db');db.pragma('journal_mode=WAL');db.pragma('foreign_keys=ON');
db.exec(`
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY,username TEXT,balance INTEGER NOT NULL DEFAULT 0 CHECK(balance>=0),blocked INTEGER NOT NULL DEFAULT 0,note TEXT);
CREATE TABLE IF NOT EXISTS ledger(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL REFERENCES users(id),type TEXT,amount INTEGER,bal_before INTEGER,bal_after INTEGER,ref TEXT,at INTEGER);
CREATE TRIGGER IF NOT EXISTS ledger_u BEFORE UPDATE ON ledger BEGIN SELECT RAISE(ABORT,'append-only'); END;
CREATE TRIGGER IF NOT EXISTS ledger_d BEFORE DELETE ON ledger BEGIN SELECT RAISE(ABORT,'append-only'); END;
CREATE TABLE IF NOT EXISTS topups(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL REFERENCES users(id),username TEXT,code TEXT,status TEXT,card TEXT,note TEXT,receipt TEXT,amount INTEGER,reason TEXT,admin_id INTEGER,created INTEGER,updated INTEGER);
CREATE TABLE IF NOT EXISTS cart(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL REFERENCES users(id),kind TEXT,gift_id TEXT,qty INTEGER,stars INTEGER,base INTEGER,fee INTEGER,total INTEGER,recipient TEXT,message TEXT,created INTEGER,expires INTEGER);
CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL REFERENCES users(id),idem TEXT UNIQUE,total INTEGER,fee INTEGER,status TEXT,created INTEGER,updated INTEGER);
CREATE TABLE IF NOT EXISTS order_items(id INTEGER PRIMARY KEY AUTOINCREMENT,order_id INTEGER NOT NULL REFERENCES orders(id),kind TEXT,gift_id TEXT,qty INTEGER,stars INTEGER,base INTEGER,fee INTEGER,total INTEGER,recipient TEXT,message TEXT);
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY,v TEXT);
CREATE TABLE IF NOT EXISTS admin_log(id INTEGER PRIMARY KEY AUTOINCREMENT,admin_id INTEGER,action TEXT,detail TEXT,ip TEXT,ua TEXT,at INTEGER);
CREATE TABLE IF NOT EXISTS notif_jobs(id INTEGER PRIMARY KEY AUTOINCREMENT,chat_id INTEGER,text TEXT,status TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,last_error TEXT,created INTEGER,updated INTEGER);
CREATE INDEX IF NOT EXISTS idx_ledger_user ON ledger(user_id,at);
CREATE INDEX IF NOT EXISTS idx_topups_user ON topups(user_id,status);
CREATE INDEX IF NOT EXISTS idx_topups_status ON topups(status);
CREATE INDEX IF NOT EXISTS idx_topups_created ON topups(created);
CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id,created);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_cart_user ON cart(user_id);
CREATE INDEX IF NOT EXISTS idx_notif_status ON notif_jobs(status);
`);
const S=(k,d)=>{const r=db.prepare('SELECT v FROM settings WHERE k=?').get(k);return r?r.v:d};
const setS=(k,v)=>db.prepare('INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v').run(k,String(v));
const log=(a,x,d,req)=>db.prepare('INSERT INTO admin_log(admin_id,action,detail,ip,ua,at) VALUES(?,?,?,?,?,?)')
 .run(a,x,JSON.stringify(d),req?(req.ip||''):'',req?(req.get('user-agent')||'').slice(0,300):'',Date.now());
const move=(uid,type,amt,ref)=>{const b=db.prepare('SELECT balance FROM users WHERE id=?').get(uid).balance,a=b+amt;
 if(a<0)throw new Error('LOW');db.prepare('UPDATE users SET balance=? WHERE id=?').run(a,uid);
 db.prepare('INSERT INTO ledger(user_id,type,amount,bal_before,bal_after,ref,at) VALUES(?,?,?,?,?,?,?)').run(uid,type,amt,b,a,ref,Date.now())};

// ---- order state machine ----
const ORDER_TRANS={paid:['fulfilling','failed','refunded','cancelled'],fulfilling:['delivered','failed','refunded'],
 delivered:['refunded'],failed:['fulfilling','refunded'],refunded:[],cancelled:[]};
function orderTransition(id,to,extra){const t=db.transaction(()=>{const o=db.prepare('SELECT * FROM orders WHERE id=?').get(id);
 if(!o)throw new Error('NONE');if(!(ORDER_TRANS[o.status]||[]).includes(to))throw new Error('STATE');
 db.prepare('UPDATE orders SET status=?,updated=? WHERE id=?').run(to,Date.now(),id);extra&&extra(o);return o})();return t}

// ---- notification queue (persisted; retried with backoff instead of fire-and-forget) ----
function enqueueNotify(chatId,text){db.prepare('INSERT INTO notif_jobs(chat_id,text,created,updated) VALUES(?,?,?,?)').run(chatId,text,Date.now(),Date.now())}
const notify=(id,text)=>enqueueNotify(id,text); // kept name for call-site compatibility
async function processNotifJobs(){const jobs=db.prepare(`SELECT * FROM notif_jobs WHERE status IN ('pending','retry') AND updated<? ORDER BY id LIMIT 20`).all(Date.now()-1000);
 for(const j of jobs){try{await tgc('sendMessage',{chat_id:j.chat_id,text:j.text});
   db.prepare(`UPDATE notif_jobs SET status='sent',updated=? WHERE id=?`).run(Date.now(),j.id)}
  catch(e){const attempts=j.attempts+1,failed=attempts>=5;
   db.prepare(`UPDATE notif_jobs SET status=?,attempts=?,last_error=?,updated=? WHERE id=?`)
    .run(failed?'failed':'retry',attempts,String(e.message).slice(0,300),Date.now()+Math.min(60000,1000*2**attempts),j.id);
   if(failed)ADMINS.forEach(a=>a!==j.chat_id&&tgc('sendMessage',{chat_id:a,text:'Notification permanently failed for '+j.chat_id}).catch(()=>{}))}}}
setInterval(()=>processNotifJobs().catch(e=>console.error('notif worker',e.message)),5000);

// ---- Telegram Bot API call with retry/backoff + 429 handling ----
async function tgc(m,b,attempt=0){let r;try{r=await fetch(`https://api.telegram.org/bot${T}/${m}`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b||{})})}
 catch(e){if(attempt<3){await new Promise(r=>setTimeout(r,500*2**attempt));return tgc(m,b,attempt+1)}throw new Error('TG_NETWORK:'+e.message)}
 let j;try{j=await r.json()}catch(e){throw new Error('TG_BADJSON')}
 if(!j.ok){
  if(j.error_code===429&&j.parameters&&j.parameters.retry_after&&attempt<3){
   await new Promise(res=>setTimeout(res,(j.parameters.retry_after+1)*1000));return tgc(m,b,attempt+1)}
  if([500,502,503].includes(j.error_code)&&attempt<3){await new Promise(r=>setTimeout(r,500*2**attempt));return tgc(m,b,attempt+1)}
  throw new Error('TG_API:'+(j.description||'unknown'))}
 return j.result}

// ---- simple per-user rate limiter for sensitive endpoints (in-memory; resets on restart) ----
const hits=new Map();
function limit(key,ms){return(q,s,n)=>{const k=key+':'+q.u.id,last=hits.get(k);
 if(last&&Date.now()-last<ms)return s.status(429).json({e:'RATE_LIMIT'});hits.set(k,Date.now());n()}}

// ---- rate & price, with provider/error metadata ----
async function loadRate(){if(!process.env.RATE_URL)return;try{const r=await fetch(process.env.RATE_URL);
 if(!r.ok)throw new Error('http '+r.status);
 const j=await r.json();let v=j;for(const k of (process.env.RATE_PATH||'').split('.').filter(Boolean))v=v?.[k];
 v=Number(String(v).replace(/,/g,''));
 if(!(v>0&&Number.isFinite(v)))throw new Error('bad value '+v);
 setS('rate',v);setS('rate_at',Date.now());setS('rate_provider',new URL(process.env.RATE_URL).host);setS('rate_errors',0)}
 catch(e){setS('rate_errors',(+S('rate_errors',0))+1);console.error('rate',e.message)}}
const getRate=()=>{const r=+S('rate',0);return r>0?{rate:r,stale:Date.now()-(+S('rate_at',0))>120000}:null};
const cfg=()=>({starUsd:+S('star_usd',0.014),fee:+S('fee',5)});

// ---- official Telegram stars->USD rate (stars_usd_sell_rate_x1000), fetched via MTProto (help.getConfig) ----
let tgStarClient=null;
async function updateStarsRateFromTelegram(){
 if(!process.env.TG_API_ID||!process.env.TG_API_HASH)return;
 try{
  if(!tgStarClient){const {TelegramClient}=require('telegram'),{StringSession}=require('telegram/sessions');
   tgStarClient=new TelegramClient(new StringSession(process.env.TG_SESSION||''),+process.env.TG_API_ID,process.env.TG_API_HASH,{connectionRetries:3});
   await tgStarClient.connect()}
  const {Api}=require('telegram'),conf=await tgStarClient.invoke(new Api.help.GetConfig());
  const raw=conf.starsUsdSellRateX1000;
  if(raw==null)throw new Error('field missing from Telegram config');
  const usd=raw/1000;
  if(!(usd>0.002&&usd<0.05))throw new Error('value out of expected range: '+raw+' -> '+usd);
  setS('star_usd',usd);setS('star_usd_source','telegram');setS('star_usd_at',Date.now());setS('star_usd_errors',0);
  console.log('stars_usd updated from Telegram config:',usd,'(raw',raw,')')}
 catch(e){setS('star_usd_errors',(+S('star_usd_errors',0))+1);console.error('stars rate fetch failed, keeping manual value:',e.message)}}
updateStarsRateFromTelegram();setInterval(updateStarsRateFromTelegram,10*60*1000);

function price(stars,qty){const R=getRate();if(!R)throw new Error('NORATE');if(R.stale)throw new Error('STALE_RATE');
 const c=cfg(),base=Math.ceil(stars*qty*c.starUsd*R.rate),fee=Math.ceil(base*c.fee/100);
 if(!Number.isSafeInteger(base+fee)||base+fee<=0)throw new Error('QTY');return{base,fee,total:base+fee}}
const PRICE_LOCK_MS=10*60*1000;

// ---- gifts ----
let gifts=[];
async function loadGifts(){try{const r=await tgc('getAvailableGifts'),out=[];
 for(const g of (r.gifts||[])){try{const f=`${DATA}/tgs/${g.id}.json`;
   if(!fs.existsSync(f)){const fi=await tgc('getFile',{file_id:g.sticker.file_id});
    const fr=await fetch(`https://api.telegram.org/file/bot${T}/${fi.file_path}`);
    if(!fr.ok)throw new Error('file http '+fr.status);
    const buf=Buffer.from(await fr.arrayBuffer());
    let json;try{json=zlib.gunzipSync(buf)}catch(e){throw new Error('bad tgs gzip')}
    fs.writeFileSync(f,json)}
   out.push({id:g.id,stars:g.star_count,left:g.remaining_count??null})}
  catch(e){console.error('gift',g.id,'skipped:',e.message)}}
 gifts=out.sort((a,b)=>a.stars-b.stars)}catch(e){console.error('gifts load failed:',e.message)}}
loadRate();loadGifts();setInterval(loadRate,(+process.env.RATE_SEC||10)*1000);setInterval(loadGifts,30*60*1000);

// ---- app ----
const app=express();app.use(express.json());
app.use((q,s,n)=>{q.rid=crypto.randomBytes(4).toString('hex');s.setHeader('X-Request-ID',q.rid);
 s.setHeader('X-Content-Type-Options','nosniff');
 s.setHeader('Referrer-Policy','no-referrer');
 s.setHeader('Permissions-Policy','geolocation=(),camera=(),microphone=()');
 s.setHeader('X-Frame-Options','ALLOWALL'); // Telegram loads this in an iframe by design
 s.setHeader('Content-Security-Policy',
  "default-src 'self'; script-src 'self' 'unsafe-inline' https://telegram.org https://cdnjs.cloudflare.com; "+
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; "+
  "img-src 'self' data: blob:; connect-src 'self'; frame-ancestors https://web.telegram.org https://*.telegram.org;");
 const t0=Date.now();s.on('finish',()=>console.log(JSON.stringify({rid:q.rid,method:q.method,path:q.path,status:s.statusCode,ms:Date.now()-t0})));
 n()});
app.use(express.static('public'));
app.get('/live',(q,s)=>s.json({ok:1}));
app.get('/ready',(q,s)=>{try{db.prepare('SELECT 1').get()}catch(e){return s.status(503).json({ok:0,db:0})}
 s.json({ok:1,db:1,rate:!!getRate(),gifts:gifts.length,telegram_stars_source:S('star_usd_source','manual')})});
app.get('/health',(q,s)=>s.json({ok:1,rate:!!getRate(),gifts:gifts.length}));
app.get('/tgs/:id',(req,res)=>{const f=`${DATA}/tgs/${req.params.id.replace(/\W/g,'')}.json`;fs.existsSync(f)?res.type('json').sendFile(path.resolve(f)):res.status(404).end()});

function safeEq(a,b){const A=Buffer.from(a,'hex'),B=Buffer.from(b,'hex');
 if(A.length!==B.length)return false;return crypto.timingSafeEqual(A,B)}
function auth(req,res,next){const d=req.get('x-init')||'';let u;
 if(d){try{const p=new URLSearchParams(d),h=p.get('hash')||'';p.delete('hash');
   const s=[...p.entries()].sort(([a],[b])=>a<b?-1:1).map(([k,v])=>k+'='+v).join('\n');
   const k=crypto.createHmac('sha256','WebAppData').update(T).digest();
   const expect=crypto.createHmac('sha256',k).update(s).digest('hex');
   const ad=+p.get('auth_date');
   if(!/^[0-9a-f]{64}$/.test(h)||!safeEq(expect,h))return res.status(401).json({e:'AUTH'});
   if(!ad||Date.now()/1000-ad>86400||ad>Date.now()/1000+60)return res.status(401).json({e:'AUTH'});
   u=JSON.parse(p.get('user'))}catch(e){return res.status(401).json({e:'AUTH'})}}
 else if(process.env.DEV==='1')u={id:+req.get('x-dev')||1,username:'dev'};
 else return res.status(401).json({e:'AUTH'});
 if(!u||!Number.isFinite(+u.id))return res.status(401).json({e:'AUTH'});
 db.prepare('INSERT INTO users(id,username) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET username=excluded.username').run(u.id,u.username||'');
 const row=db.prepare('SELECT blocked FROM users WHERE id=?').get(u.id);
 if(row&&row.blocked)return res.status(403).json({e:'BLOCKED'});
 req.u=u;req.admin=ADMINS.includes(+u.id);next()}
const adm=(q,s,n)=>q.admin?n():s.status(403).json({e:'FORBIDDEN'});
const KNOWN={AUTH:401,FORBIDDEN:403,LOW:409,EMPTY:400,OPEN:409,NORATE:503,STALE_RATE:503,QTY:400,MIN50:400,
 RECIPIENT:400,STATE:409,FILE:400,AMOUNT:400,REASON:400,CARD:400,GIFT_GONE:409,RATE_LIMIT:429,KEY:400,
 GIFT:404,KIND:400,NONE:404,VALUE:400,BLOCKED:403,MAINTENANCE:503,FEATURE_OFF:403,CART_EXPIRED:409,LIMIT:400};
const wrap=f=>(q,s)=>{try{f(q,s)}catch(e){const code=e&&e.message,known=KNOWN[code];
 if(known){s.status(known).json({e:code})}
 else{console.error('rid='+q.rid,e);s.status(500).json({e:'ERR-'+q.rid})}}};
const RECIPIENT_RE=/^@?[a-zA-Z0-9_]{5,32}$/;
const maintOn=()=>S('maintenance','0')==='1';
const featOn=k=>S('feature_'+k,'1')!=='0';
const clampLimit=n=>Math.max(1,Math.min(100,Math.floor(+n)||30));

app.get('/api/me',auth,wrap((q,s)=>{const u=db.prepare('SELECT balance FROM users WHERE id=?').get(q.u.id);
 s.json({id:q.u.id,balance:u.balance,admin:q.admin,maintenance:maintOn(),features:{gifts:featOn('gifts'),stars:featOn('stars'),topup:featOn('topup')}})}));
app.get('/api/prices',auth,wrap((q,s)=>{const R=getRate();s.json({rate:R&&R.rate,stale:R?R.stale:true,...cfg(),
 rate_provider:S('rate_provider',''),rate_errors:+S('rate_errors',0),star_source:S('star_usd_source','manual'),star_errors:+S('star_usd_errors',0)})}));
app.get('/api/gifts',auth,wrap((q,s)=>{if(!featOn('gifts'))throw new Error('FEATURE_OFF');s.json(gifts.filter(g=>g.left!==0))}));
app.get('/api/cart',auth,wrap((q,s)=>{db.prepare('DELETE FROM cart WHERE user_id=? AND expires<?').run(q.u.id,Date.now());
 s.json(db.prepare('SELECT * FROM cart WHERE user_id=?').all(q.u.id))}));
app.get('/api/orders',auth,wrap((q,s)=>{const o=db.prepare('SELECT id,total,fee,status,created FROM orders WHERE user_id=? ORDER BY id DESC LIMIT 30').all(q.u.id);
 o.forEach(x=>x.items=db.prepare('SELECT kind,gift_id,qty,stars,total,recipient,message FROM order_items WHERE order_id=?').all(x.id));s.json(o)}));
app.delete('/api/cart/:id',auth,wrap((q,s)=>{db.prepare('DELETE FROM cart WHERE id=? AND user_id=?').run(q.params.id,q.u.id);s.json({ok:1})}));
app.post('/api/cart',auth,limit('cart',500),wrap((q,s)=>{if(maintOn())throw new Error('MAINTENANCE');
 const{kind,gift_id,message}=q.body,qty=Math.floor(+q.body.qty),recipient=String(q.body.recipient||'').trim();
 if(!(qty>=1&&qty<=100000)||!Number.isSafeInteger(qty))throw new Error('QTY');
 if(!RECIPIENT_RE.test(recipient))throw new Error('RECIPIENT');
 let unit;
 if(kind==='gift'){if(!featOn('gifts'))throw new Error('FEATURE_OFF');const g=gifts.find(x=>x.id===gift_id&&x.left!==0);if(!g)throw new Error('GIFT');unit=g.stars;if(qty>50)throw new Error('QTY')}
 else if(kind==='stars'){if(!featOn('stars'))throw new Error('FEATURE_OFF');if(qty<50||qty>1000000)throw new Error('MIN50');unit=1}else throw new Error('KIND');
 const p=price(unit,qty);
 db.prepare('INSERT INTO cart(user_id,kind,gift_id,qty,stars,base,fee,total,recipient,message,created,expires) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)')
  .run(q.u.id,kind,gift_id||null,qty,unit*qty,p.base,p.fee,p.total,recipient,String(message||'').slice(0,200),Date.now(),Date.now()+PRICE_LOCK_MS);s.json({ok:1})}));
app.post('/api/checkout',auth,limit('checkout',3000),wrap((q,s)=>{if(maintOn())throw new Error('MAINTENANCE');
 const idem=String(q.body.key||'');if(idem.length<8)throw new Error('KEY');
 const r=db.transaction(()=>{const ex=db.prepare('SELECT id FROM orders WHERE idem=?').get(idem);if(ex)return{id:ex.id,dup:1};
  const now=Date.now(),expired=db.prepare('SELECT COUNT(*) c FROM cart WHERE user_id=? AND expires<?').get(q.u.id,now).c;
  if(expired){db.prepare('DELETE FROM cart WHERE user_id=? AND expires<?').run(q.u.id,now);throw new Error('CART_EXPIRED')}
  const it=db.prepare('SELECT * FROM cart WHERE user_id=?').all(q.u.id);if(!it.length)throw new Error('EMPTY');
  for(const i of it)if(i.kind==='gift'){const g=gifts.find(x=>x.id===i.gift_id);if(!g||g.left===0)throw new Error('GIFT_GONE')}
  const tot=it.reduce((a,i)=>a+i.total,0),fee=it.reduce((a,i)=>a+i.fee,0);
  const o=db.prepare('INSERT INTO orders(user_id,idem,total,fee,status,created,updated) VALUES(?,?,?,?,?,?,?)').run(q.u.id,idem,tot,fee,'paid',now,now);
  for(const i of it)db.prepare('INSERT INTO order_items(order_id,kind,gift_id,qty,stars,base,fee,total,recipient,message) VALUES(?,?,?,?,?,?,?,?,?,?)').run(o.lastInsertRowid,i.kind,i.gift_id,i.qty,i.stars,i.base,i.fee,i.total,i.recipient,i.message);
  move(q.u.id,'purchase',-tot,'order:'+o.lastInsertRowid);db.prepare('DELETE FROM cart WHERE user_id=?').run(q.u.id);return{id:o.lastInsertRowid}})();
 if(!r.dup)ADMINS.forEach(a=>notify(a,'New order #'+r.id));s.json(r)}));

// ---- topup (user) ----
const OPEN=`('requested','card_sent','receipt_uploaded')`;
app.post('/api/topup',auth,limit('topup',60000),wrap((q,s)=>{if(!featOn('topup'))throw new Error('FEATURE_OFF');
 if(db.prepare(`SELECT 1 FROM topups WHERE user_id=? AND status IN ${OPEN}`).get(q.u.id))throw new Error('OPEN');
 const cnt=db.prepare('SELECT COUNT(*) c FROM topups WHERE user_id=? AND created>?').get(q.u.id,Date.now()-86400000).c;
 if(cnt>=10)ADMINS.forEach(a=>notify(a,'Anomaly: user '+q.u.id+' made '+cnt+' topup requests in 24h'));
 const code=crypto.randomBytes(3).toString('hex').toUpperCase(),n=Date.now();
 db.prepare('INSERT INTO topups(user_id,username,code,status,created,updated) VALUES(?,?,?,?,?,?)').run(q.u.id,q.u.username||'',code,'requested',n,n);
 ADMINS.forEach(a=>notify(a,'Card request '+code));s.json({ok:1})}));
app.post('/api/topup/:id/cancel',auth,wrap((q,s)=>{const r=db.prepare(`UPDATE topups SET status='cancelled',updated=? WHERE id=? AND user_id=? AND status IN ${OPEN}`).run(Date.now(),q.params.id,q.u.id);
 if(!r.changes)throw new Error('STATE');s.json({ok:1})}));
app.get('/api/topups',auth,wrap((q,s)=>s.json(db.prepare('SELECT id,code,status,card,note,amount,reason,created FROM topups WHERE user_id=? ORDER BY id DESC LIMIT 20').all(q.u.id))));
function magicOk(buf,mime){if(mime==='image/png')return buf.length>8&&buf[0]===0x89&&buf[1]===0x50&&buf[2]===0x4e&&buf[3]===0x47;
 if(mime==='image/jpeg')return buf.length>3&&buf[0]===0xff&&buf[1]===0xd8;return false}
const up=multer({dest:DATA+'/receipts',limits:{fileSize:5e6},fileFilter:(r,f,cb)=>cb(null,['image/jpeg','image/png'].includes(f.mimetype))});
app.post('/api/topup/:id/receipt',auth,limit('receipt',10000),up.single('f'),wrap((q,s)=>{if(!q.file)throw new Error('FILE');
 const head=fs.readFileSync(q.file.path,{start:0,end:16});
 if(!magicOk(head,q.file.mimetype)){fs.unlink(q.file.path,()=>{});throw new Error('FILE')}
 const r=db.prepare(`UPDATE topups SET status='receipt_uploaded',receipt=?,updated=? WHERE id=? AND user_id=? AND status='card_sent'`).run(q.file.filename,Date.now(),q.params.id,q.u.id);
 if(!r.changes){fs.unlink(q.file.path,()=>{});throw new Error('STATE')}ADMINS.forEach(a=>notify(a,'Receipt uploaded #'+q.params.id));s.json({ok:1})}));

// ---- admin: topups ----
app.get('/api/admin/topups',auth,adm,wrap((q,s)=>{const {status,q:term}=q.query,limit=clampLimit(q.query.limit),page=Math.max(1,+q.query.page||1);
 let sql='SELECT * FROM topups WHERE 1=1',p=[];
 if(status){sql+=' AND status=?';p.push(String(status))}else sql+=` AND status IN ${OPEN}`;
 if(term){sql+=' AND (CAST(id AS TEXT)=? OR CAST(user_id AS TEXT)=? OR username LIKE ? OR code LIKE ?)';p.push(term,term,'%'+term+'%','%'+term+'%')}
 sql+=' ORDER BY id DESC LIMIT ? OFFSET ?';p.push(limit,(page-1)*limit);
 s.json(db.prepare(sql).all(...p))}));
app.get('/api/admin/receipt/:id',auth,adm,wrap((q,s)=>{const t=db.prepare('SELECT receipt FROM topups WHERE id=?').get(q.params.id);
 if(!t||!t.receipt)throw new Error('NONE');s.sendFile(path.resolve(DATA,'receipts',t.receipt))}));
function luhnOk(n){let s=0,alt=false;for(let i=n.length-1;i>=0;i--){let d=+n[i];if(alt){d*=2;if(d>9)d-=9}s+=d;alt=!alt}return s%10===0}
app.post('/api/admin/topup/:id/card',auth,adm,limit('a_card',1000),wrap((q,s)=>{const card=String(q.body.card||'').replace(/\D/g,'');
 if(card.length!==16||!luhnOk(card))throw new Error('CARD');
 const t=db.prepare('SELECT * FROM topups WHERE id=?').get(q.params.id);if(!t)throw new Error('NONE');
 const pretty=card.match(/.{4}/g).join('-');
 const r=db.prepare(`UPDATE topups SET status='card_sent',card=?,note=?,admin_id=?,updated=? WHERE id=? AND status='requested'`).run(pretty,String(q.body.note||'').slice(0,300),q.u.id,Date.now(),t.id);
 if(!r.changes)throw new Error('STATE');log(q.u.id,'card',{id:t.id},q);notify(t.user_id,'Card details ready. Tracking code: '+t.code);s.json({ok:1})}));
app.post('/api/admin/topup/:id/approve',auth,adm,limit('a_approve',1000),wrap((q,s)=>{const amt=Math.floor(+q.body.amount);
 if(!(amt>0)||!Number.isSafeInteger(amt)||amt>1e12)throw new Error('AMOUNT');
 const t=db.transaction(()=>{const t=db.prepare('SELECT * FROM topups WHERE id=?').get(q.params.id);if(!t)throw new Error('NONE');
  const r=db.prepare(`UPDATE topups SET status='approved',amount=?,admin_id=?,updated=? WHERE id=? AND status='receipt_uploaded'`).run(amt,q.u.id,Date.now(),t.id);
  if(!r.changes)throw new Error('STATE');move(t.user_id,'topup',amt,'topup:'+t.id);log(q.u.id,'approve',{id:t.id,amt},q);return t})();
 notify(t.user_id,'Wallet charged: '+amt+' Toman');s.json({ok:1})}));
app.post('/api/admin/topup/:id/reject',auth,adm,limit('a_reject',1000),wrap((q,s)=>{const reason=String(q.body.reason||'').trim();if(!reason)throw new Error('REASON');
 const t=db.prepare('SELECT * FROM topups WHERE id=?').get(q.params.id);if(!t)throw new Error('NONE');
 const r=db.prepare(`UPDATE topups SET status='rejected',reason=?,admin_id=?,updated=? WHERE id=? AND status='receipt_uploaded'`).run(reason.slice(0,300),q.u.id,Date.now(),t.id);
 if(!r.changes)throw new Error('STATE');log(q.u.id,'reject',{id:t.id},q);notify(t.user_id,'Receipt rejected: '+reason);s.json({ok:1})}));

// ---- admin: settings / feature flags / maintenance ----
app.get('/api/admin/settings',auth,adm,wrap((q,s)=>s.json({...cfg(),rate:S('rate',0),star_usd_source:S('star_usd_source','manual'),
 rate_provider:S('rate_provider',''),rate_errors:+S('rate_errors',0),star_errors:+S('star_usd_errors',0),
 maintenance:maintOn(),features:{gifts:featOn('gifts'),stars:featOn('stars'),topup:featOn('topup')}})));
app.post('/api/admin/settings',auth,adm,wrap((q,s)=>{for(const k of['fee','star_usd','rate']){const v=q.body[k];
 if(v!==undefined&&v!==''){if(!(+v>=0)||!Number.isFinite(+v))throw new Error('VALUE');setS(k,+v);if(k==='rate')setS('rate_at',Date.now());if(k==='star_usd')setS('star_usd_source','manual');log(q.u.id,'set_'+k,{v},q)}}
 if(q.body.maintenance!==undefined){setS('maintenance',q.body.maintenance?'1':'0');log(q.u.id,'maintenance',{v:q.body.maintenance},q)}
 for(const k of['gifts','stars','topup'])if(q.body['feature_'+k]!==undefined){setS('feature_'+k,q.body['feature_'+k]?'1':'0');log(q.u.id,'feature_'+k,{v:q.body['feature_'+k]},q)}
 s.json({ok:1})}));

// ---- admin: orders (state machine + pagination/search) ----
app.get('/api/admin/orders',auth,adm,wrap((q,s)=>{const {status,q:term}=q.query,limit=clampLimit(q.query.limit),page=Math.max(1,+q.query.page||1);
 let sql='SELECT * FROM orders WHERE 1=1',p=[];
 if(status){sql+=' AND status=?';p.push(String(status))}
 if(term){sql+=' AND (CAST(id AS TEXT)=? OR CAST(user_id AS TEXT)=?)';p.push(term,term)}
 sql+=' ORDER BY id DESC LIMIT ? OFFSET ?';p.push(limit,(page-1)*limit);
 const o=db.prepare(sql).all(...p);o.forEach(x=>x.items=db.prepare('SELECT * FROM order_items WHERE order_id=?').all(x.id));s.json(o)}));
app.post('/api/admin/order/:id/fulfilling',auth,adm,wrap((q,s)=>{orderTransition(q.params.id,'fulfilling');log(q.u.id,'fulfilling',{id:q.params.id},q);s.json({ok:1})}));
app.post('/api/admin/order/:id/done',auth,adm,wrap((q,s)=>{orderTransition(q.params.id,'delivered');log(q.u.id,'delivered',{id:q.params.id},q);s.json({ok:1})}));
app.post('/api/admin/order/:id/fail',auth,adm,wrap((q,s)=>{orderTransition(q.params.id,'failed');log(q.u.id,'failed',{id:q.params.id},q);
 const o=db.prepare('SELECT * FROM orders WHERE id=?').get(q.params.id);notify(o.user_id,'Order #'+o.id+' failed and will be reviewed for refund.');s.json({ok:1})}));
app.post('/api/admin/order/:id/refund',auth,adm,limit('a_refund',1000),wrap((q,s)=>{
 const o=orderTransition(q.params.id,'refunded',o=>move(o.user_id,'refund',o.total,'order:'+o.id));
 log(q.u.id,'refund',{id:o.id,amt:o.total},q);notify(o.user_id,'Order #'+o.id+' refunded: '+o.total+' Toman');s.json({ok:1})}));

// ---- admin: dashboard, export, reconcile, audit ----
app.get('/api/admin/dashboard',auth,adm,wrap((q,s)=>{const day=86400000,now=Date.now();
 const rev=since=>db.prepare("SELECT COALESCE(SUM(total),0) t,COUNT(*) c FROM orders WHERE status!='refunded' AND created>?").get(since);
 s.json({today:rev(now-day),week:rev(now-7*day),month:rev(now-30*day),
  pending_topups:db.prepare(`SELECT COUNT(*) c FROM topups WHERE status IN ${OPEN}`).get().c,
  pending_orders:db.prepare("SELECT COUNT(*) c FROM orders WHERE status IN ('paid','fulfilling')").get().c,
  refunds_month:db.prepare("SELECT COALESCE(SUM(total),0) t,COUNT(*) c FROM orders WHERE status='refunded' AND updated>?").get(now-30*day),
  gifts_available:gifts.length,rate_ok:!!getRate(),notif_failed:db.prepare("SELECT COUNT(*) c FROM notif_jobs WHERE status='failed'").get().c})}));
function toCsv(rows){if(!rows.length)return'';const cols=Object.keys(rows[0]);
 const esc=v=>{v=v==null?'':String(v);return /[",\n]/.test(v)?'"'+v.replace(/"/g,'""')+'"':v};
 return cols.join(',')+'\n'+rows.map(r=>cols.map(c=>esc(r[c])).join(',')).join('\n')}
app.get('/api/admin/export/:what',auth,adm,wrap((q,s)=>{const w=q.params.what,map={orders:'SELECT * FROM orders ORDER BY id DESC LIMIT 5000',
 topups:'SELECT id,user_id,username,code,status,amount,reason,created,updated FROM topups ORDER BY id DESC LIMIT 5000',
 ledger:'SELECT * FROM ledger ORDER BY id DESC LIMIT 5000'};
 if(!map[w])throw new Error('NONE');const rows=db.prepare(map[w]).all();
 s.setHeader('Content-Type','text/csv');s.setHeader('Content-Disposition',`attachment; filename="${w}.csv"`);s.send(toCsv(rows))}));
app.get('/api/admin/reconcile',auth,adm,wrap((q,s)=>{
 const bad=db.prepare(`SELECT * FROM (SELECT u.id,u.balance,
  (SELECT bal_after FROM ledger WHERE user_id=u.id ORDER BY id DESC LIMIT 1) AS ledger_bal FROM users u)
  WHERE ledger_bal IS NOT NULL AND ledger_bal<>balance`).all();
 s.json({ok:bad.length===0,mismatches:bad})}));
app.get('/api/admin/audit',auth,adm,wrap((q,s)=>{const limit=clampLimit(q.query.limit);
 s.json(db.prepare('SELECT * FROM admin_log ORDER BY id DESC LIMIT ?').all(limit))}));
app.get('/api/admin/users/:id',auth,adm,wrap((q,s)=>{const u=db.prepare('SELECT * FROM users WHERE id=?').get(q.params.id);if(!u)throw new Error('NONE');
 const orders=db.prepare('SELECT id,total,status,created FROM orders WHERE user_id=? ORDER BY id DESC LIMIT 20').all(u.id);
 const topups=db.prepare('SELECT id,status,amount,created FROM topups WHERE user_id=? ORDER BY id DESC LIMIT 20').all(u.id);
 s.json({...u,orders,topups})}));
app.post('/api/admin/users/:id/block',auth,adm,wrap((q,s)=>{db.prepare('UPDATE users SET blocked=? WHERE id=?').run(q.body.blocked?1:0,q.params.id);
 log(q.u.id,'block',{id:q.params.id,v:q.body.blocked},q);s.json({ok:1})}));
app.post('/api/admin/users/:id/note',auth,adm,wrap((q,s)=>{db.prepare('UPDATE users SET note=? WHERE id=?').run(String(q.body.note||'').slice(0,500),q.params.id);
 log(q.u.id,'note',{id:q.params.id},q);s.json({ok:1})}));

app.get('/api/wallet/statement',auth,wrap((q,s)=>{const l=db.prepare('SELECT type,amount,bal_before,bal_after,ref,at FROM ledger WHERE user_id=? ORDER BY id DESC LIMIT 100').all(q.u.id);s.json(l)}));

process.on('uncaughtException',e=>console.error('uncaught',e));
process.on('unhandledRejection',e=>console.error('unhandled',e));
app.use((err,q,s,n)=>{const rid=(q&&q.rid)||'?';console.error('rid='+rid,err);s.status(400).json({e:'ERR-'+rid})});
const server=app.listen(process.env.PORT||3000);
function shutdown(){console.log('shutting down...');server.close(()=>{try{db.close()}catch(e){}process.exit(0)});
 setTimeout(()=>process.exit(1),10000).unref()}
process.on('SIGTERM',shutdown);process.on('SIGINT',shutdown);
