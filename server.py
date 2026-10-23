#!/usr/bin/env python3
"""Smart Commodity Stock Allocation & FIFO Management. Stdlib only: python3 server.py"""
import sqlite3, json, hashlib, secrets, os, threading, csv, io, statistics
from datetime import datetime, timedelta, timezone, date
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

DB = os.environ.get("FIFO_DB", "fifo.db"); LOCK = threading.Lock()
now = lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
today = lambda: date.today().isoformat()
ROLES = {  # server-side permission matrix
 "users": {"admin"}, "settings": {"admin"}, "audit": {"admin", "manager"},
 "commodity": {"admin", "manager"}, "receive": {"admin", "manager"}, "adjust": {"admin", "manager"},
 "allocate": {"admin", "manager"}, "request": {"admin", "manager", "requester"},
 "read": {"admin", "manager", "requester", "viewer"}, "reports": {"admin", "manager", "viewer"}}
UNITS = {"box", "kg", "litre", "vial", "pack", "unit"}

def conn():
    c = sqlite3.connect(DB, isolation_level=None, timeout=30); c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON"); return c
def hp(pw, salt): return hashlib.pbkdf2_hmac("sha256", pw.encode(), bytes.fromhex(salt), 200_000).hex()
def rows(c, q, a=()): return [dict(r) for r in c.execute(q, a)]
def audit(c, user, action, detail): c.execute("INSERT INTO audit(ts,user,action,detail) VALUES(?,?,?,?)", (now(), user, action, detail))

SCHEMA = """
CREATE TABLE users(id INTEGER PRIMARY KEY,username TEXT UNIQUE,pw TEXT,salt TEXT,role TEXT);
CREATE TABLE sessions(token TEXT PRIMARY KEY,uid INT,created TEXT);
CREATE TABLE settings(k TEXT PRIMARY KEY,v TEXT);
CREATE TABLE locations(id INTEGER PRIMARY KEY,name TEXT,type TEXT,x REAL,y REAL,parent_id INT);
CREATE TABLE commodities(id INTEGER PRIMARY KEY,name TEXT UNIQUE,unit TEXT,category TEXT,reorder REAL);
CREATE TABLE batches(id INTEGER PRIMARY KEY,commodity_id INT,batch_no TEXT,supplier TEXT,received TEXT,expiry TEXT,unit_cost REAL,
  blocked INT DEFAULT 0,location_id INT,demo INT DEFAULT 0,UNIQUE(commodity_id,batch_no));
CREATE TABLE ledger(id INTEGER PRIMARY KEY,batch_id INT,type TEXT,qty REAL,reason TEXT,ref TEXT,user TEXT,ts TEXT);
CREATE TABLE requests(id INTEGER PRIMARY KEY,commodity_id INT,qty REAL,requester TEXT,location_id INT,status TEXT,created TEXT,note TEXT,decided_by TEXT,demo INT DEFAULT 0,urgency TEXT DEFAULT 'normal',purpose TEXT);
CREATE TABLE allocations(id INTEGER PRIMARY KEY,request_id INT,batch_id INT,qty REAL,user TEXT,ts TEXT);
CREATE TABLE audit(id INTEGER PRIMARY KEY,ts TEXT,user TEXT,action TEXT,detail TEXT);
"""
def seed():
    c = conn(); c.executescript(SCHEMA); c.execute("BEGIN")
    for u, r in [("admin", "admin"), ("manager", "manager"), ("requester", "requester"), ("viewer", "viewer")]:
        s = secrets.token_hex(8); c.execute("INSERT INTO users(username,pw,salt,role) VALUES(?,?,?,?)", (u, hp(u + "-demo", s), s, r))
    c.execute("INSERT INTO settings VALUES('org_name','IPDs (demo organization)')")
    locs = [("Central Warehouse", "warehouse", 50, 20, None), ("North Store", "store", 18, 62, 1), ("East Clinic Depot", "distribution", 82, 58, 1), ("South Store", "store", 50, 86, 1)]
    for i, l in enumerate(locs, 1): c.execute("INSERT INTO locations VALUES(?,?,?,?,?,?)", (i, *l))
    com = [("Oral Rehydration Salts", "box", "Medicine", 120), ("Paracetamol 500mg", "pack", "Medicine", 200), ("Rice (25kg)", "kg", "Food", 500), ("Measles Vaccine", "vial", "Vaccine", 350)]
    for i, m in enumerate(com, 1): c.execute("INSERT INTO commodities VALUES(?,?,?,?,?)", (i, *m))
    d = date.today(); iso = lambda n: (d + timedelta(days=n)).isoformat()
    bt = [(1,"ORS-101",-120,40,1),(1,"ORS-104",-60,200,1),(1,"ORS-109",-20,300,2),(2,"PCM-201",-90,15,1),(2,"PCM-205",-30,400,3),
          (3,"RIC-301",-100,-5,1),(3,"RIC-305",-45,25,4),(4,"VAC-401",-50,20,1),(4,"VAC-405",-10,150,3)]
    for i, (cm, bn, rd, ex, loc) in enumerate(bt, 1):
        c.execute("INSERT INTO batches VALUES(?,?,?,?,?,?,?,?,?,1)", (i, cm, bn, "Demo Supplier " + "AB"[i % 2], iso(rd), iso(ex), 2.5 + cm, 1 if bn == "PCM-205" and False else 0, loc))
        qty = {1:800,2:400,3:500,4:700,5:600,6:2500,7:3000,8:300,9:200}[i]
        c.execute("INSERT INTO ledger(batch_id,type,qty,reason,ref,user,ts) VALUES(?,?,?,?,?,?,?)", (i, "receipt", qty, "Initial receipt (demo)", "demo", "manager", iso(rd) + "T08:00:00+00:00"))
    # 8 weeks of historical issues (demo): oldest eligible batch first
    base = {1: 45, 2: 70, 3: 260, 4: 25}; first = {1: 1, 2: 4, 3: 6, 4: 8}
    for w in range(8, 0, -1):
        for cm, b in base.items():
            q = round(b * (1 + 0.15 * ((w * 7 + cm) % 5 - 2) / 2))
            c.execute("INSERT INTO ledger(batch_id,type,qty,reason,ref,user,ts) VALUES(?,?,?,?,?,?,?)", (first[cm], "issue", -q, "Historical issue (demo)", "demo", "manager", (d - timedelta(days=w * 7 + 2)).isoformat() + "T10:00:00+00:00"))
    c.execute("INSERT INTO ledger(batch_id,type,qty,reason,ref,user,ts) VALUES(2,'adjustment',-12,'Damaged in transit (demo)','demo','manager',?)", (now(),))
    for cm, q, loc, st in [(1, 150, 2, "pending"), (2, 90, 3, "pending"), (4, 100, 4, "pending"), (3, 400, 2, "rejected")]:
        c.execute("INSERT INTO requests(commodity_id,qty,requester,location_id,status,created,note,demo) VALUES(?,?,?,?,?,?,?,1)", (cm, q, "requester", loc, st, now(), "Demo request"))
    c.execute("UPDATE requests SET purpose='Demo purpose: clinic resupply',urgency=CASE id WHEN 1 THEN 'high' WHEN 3 THEN 'critical' ELSE 'normal' END"); audit(c, "system", "seed", "Demo data loaded"); c.execute("COMMIT"); c.close()

def balances(c, extra="", a=()):
    return rows(c, f"""SELECT b.*,c.name commodity,c.unit,l.name location,COALESCE(SUM(g.qty),0) balance FROM batches b JOIN commodities c ON c.id=b.commodity_id
      JOIN locations l ON l.id=b.location_id LEFT JOIN ledger g ON g.batch_id=b.id {extra} GROUP BY b.id ORDER BY b.received,b.batch_no""", a)
def status(b):
    if b["expiry"] < today(): return "expired"
    if b["blocked"]: return "blocked"
    if b["expiry"] <= (date.today() + timedelta(days=30)).isoformat(): return "expiring"
    return "ok"
def eligible(c, cid):
    return [b for b in balances(c, "WHERE b.commodity_id=?", (cid,)) if b["balance"] > 0 and not b["blocked"] and b["expiry"] >= today()]
def fifo_plan(c, cid, qty):
    plan, need = [], qty
    for b in eligible(c, cid):  # already ordered by received, batch_no
        if need <= 0: break
        t = min(b["balance"], need); plan.append({"batch_id": b["id"], "batch_no": b["batch_no"], "received": b["received"], "expiry": b["expiry"], "qty": t, "location": b["location"]}); need -= t
    avail = sum(b["balance"] for b in eligible(c, cid))
    expl = ", then ".join(f"{p['qty']:g} from batch {p['batch_no']}" for p in plan) or "No eligible stock"
    return {"plan": plan, "available": avail, "shortfall": max(need, 0), "explanation": expl}

def forecast(c, cid):
    wk = rows(c, "SELECT CAST((julianday('now')-julianday(substr(g.ts,1,10)))/7 AS INT) w,-SUM(g.qty) q FROM ledger g JOIN batches b ON b.id=g.batch_id WHERE b.commodity_id=? AND g.type='issue' GROUP BY w HAVING w>=1", (cid,))
    hist = {r["w"]: r["q"] for r in wk}; n = len(hist); series = [hist.get(w, 0) for w in range(max(hist, default=0), 0, -1)]
    stock = sum(b["balance"] for b in eligible(c, cid)); last4 = series[-4:]
    avg = statistics.mean(last4) if last4 else 0; cv = (statistics.pstdev(last4) / avg) if avg and len(last4) > 1 else 1
    conf = "insufficient" if n < 4 else ("high" if cv < .15 else "medium" if cv < .35 else "low")
    proj = [round(avg)] * 4
    return {"history": series, "projection": proj, "weeks_of_data": n, "method": "Mean of the last 4 weekly issues, held flat for 4 weeks", "confidence": conf,
            "sample": n < 4, "stock": stock, "projected_total": sum(proj), "at_risk": sum(proj) > stock, "cv": round(cv, 2)}

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, code, obj, ctype="application/json", extra=None):
        body = obj if isinstance(obj, bytes) else json.dumps(obj).encode()
        self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        for k, v in (extra or {}).items(): self.send_header(k, v)
        self.end_headers(); self.wfile.write(body)
    def user(self, c):
        ck = self.headers.get("Cookie") or ""; t = ck.split("sid=")[-1].split(";")[0] if "sid=" in ck else ""
        if self.headers.get("Authorization", "").startswith("Bearer "): t = self.headers["Authorization"][7:]
        r = c.execute("SELECT u.username,u.role FROM sessions s JOIN users u ON u.id=s.uid WHERE s.token=?", (t,)).fetchone(); return dict(r) if r else None
    def do_GET(self): self.route("GET")
    def do_POST(self): self.route("POST")
    def route(self, m):
        u = urlparse(self.path); q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if m == "GET" and not u.path.startswith("/api/"):
            f = os.path.join("static", "index.html"); return self.send(200, open(f, "rb").read(), "text/html; charset=utf-8")
        body = {}
        if m == "POST":
            try: body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
            except Exception: return self.send(400, {"error": "Invalid JSON"})
        c = conn()
        try:
            if u.path == "/api/login":
                r = c.execute("SELECT * FROM users WHERE username=?", (str(body.get("username", "")),)).fetchone()
                if not r or not secrets.compare_digest(r["pw"], hp(str(body.get("password", "")), r["salt"])):
                    audit(c, "anonymous", "login_failed", "Invalid credentials"); return self.send(401, {"error": "Wrong username or password."})
                t = secrets.token_urlsafe(32); c.execute("INSERT INTO sessions VALUES(?,?,?)", (t, r["id"], now())); audit(c, r["username"], "login", "")
                return self.send(200, {"username": r["username"], "role": r["role"], "token": t}, extra={"Set-Cookie": f"sid={t}; HttpOnly; SameSite=Strict; Path=/"})
            me = self.user(c)
            if not me: return self.send(401, {"error": "Sign in required."})
            def need(p):
                if me["role"] not in ROLES[p]: audit(c, me["username"], "denied", f"{m} {u.path}"); raise PermissionError(f"Your role ({me['role']}) cannot do this.")
            if u.path == "/api/me": return self.send(200, {**me, "org": c.execute("SELECT v FROM settings WHERE k='org_name'").fetchone()[0], "perms": [p for p, r in ROLES.items() if me["role"] in r]})
            if u.path == "/api/logout": c.execute("DELETE FROM sessions WHERE token IN (?,?)", (self.headers.get("Authorization", "")[7:], (self.headers.get("Cookie") or "").split("sid=")[-1].split(";")[0])); return self.send(200, {"ok": 1}, extra={"Set-Cookie": "sid=; Max-Age=0; Path=/"})
            with LOCK:
                res = self.api(c, m, u.path, q, body, me, need)
            if isinstance(res, tuple): return self.send(*res)
            return self.send(200, res)
        except PermissionError as e: self.send(403, {"error": str(e)})
        except ValueError as e: self.send(422, {"errors": e.args[0] if isinstance(e.args[0], dict) else {"_": str(e.args[0])}})
        except Exception as e: self.send(500, {"error": "Server error: " + type(e).__name__})
        finally: c.close()

    def api(self, c, m, p, q, b, me, need):
        U = me["username"]
        def num(k, errs, allow_neg=False):
            try: v = float(b.get(k))
            except (TypeError, ValueError): errs[k] = "Enter a number."; return 0
            if not allow_neg and v <= 0: errs[k] = "Must be greater than zero."
            if allow_neg and v == 0: errs[k] = "Cannot be zero."
            return v
        def fail(errs):
            if errs: raise ValueError(errs)
        if p == "/api/commodities" and m == "GET":
            need("read"); out = rows(c, "SELECT * FROM commodities")
            for r in out: r["stock"] = sum(x["balance"] for x in eligible(c, r["id"])); r["low"] = r["stock"] <= r["reorder"]
            return out
        if p == "/api/commodities" and m == "POST":
            need("commodity"); e = {}; name = str(b.get("name", "")).strip()
            if not name: e["name"] = "Name is required."
            if b.get("unit") not in UNITS: e["unit"] = "Choose a valid unit: " + ", ".join(sorted(UNITS))
            ro = num("reorder", e)
            if name and c.execute("SELECT 1 FROM commodities WHERE lower(name)=lower(?) AND id IS NOT ?", (name, b.get("id"))).fetchone(): e["name"] = "A commodity with this name exists."
            fail(e)
            if b.get("id"): c.execute("UPDATE commodities SET name=?,unit=?,category=?,reorder=? WHERE id=?", (name, b["unit"], b.get("category") or "General", ro, b["id"])); audit(c, U, "commodity_edit", name)
            else: c.execute("INSERT INTO commodities(name,unit,category,reorder) VALUES(?,?,?,?)", (name, b["unit"], b.get("category") or "General", ro)); audit(c, U, "commodity_add", name)
            return {"ok": 1}
        if p == "/api/locations" and m == "POST":
            need("commodity"); e = {}; name = str(b.get("name", "")).strip()
            if not name: e["name"] = "Name is required."
            if b.get("type") not in {"warehouse", "store", "distribution"}: e["type"] = "Choose warehouse, store or distribution."
            xy = []
            for k in ("x", "y"):
                try: v = float(b.get(k)); assert 0 <= v <= 100; xy.append(v)
                except Exception: e[k] = "Enter a number from 0 to 100."
            par = b.get("parent_id") or None
            if par and not c.execute("SELECT 1 FROM locations WHERE id=?", (par,)).fetchone(): e["parent_id"] = "Unknown parent location."
            if par and str(par) == str(b.get("id")): e["parent_id"] = "A location cannot be its own parent."
            fail(e)
            if b.get("id"): c.execute("UPDATE locations SET name=?,type=?,x=?,y=?,parent_id=? WHERE id=?", (name, b["type"], *xy, par, b["id"])); audit(c, U, "location_edit", name)
            else: c.execute("INSERT INTO locations(name,type,x,y,parent_id) VALUES(?,?,?,?,?)", (name, b["type"], *xy, par)); audit(c, U, "location_add", name)
            return {"ok": 1}
        if p == "/api/locations": need("read"); return rows(c, "SELECT * FROM locations")
        if p == "/api/batches": need("read"); r = balances(c, "WHERE (?='' OR b.commodity_id=?)", (q.get("commodity", ""), q.get("commodity", ""))); [x.update(status=status(x)) for x in r]; return r
        if p == "/api/ledger": need("read"); return rows(c, "SELECT * FROM ledger WHERE batch_id=? ORDER BY id DESC", (q.get("batch"),))
        if p == "/api/receive" and m == "POST":
            need("receive"); e = {}; qty = num("qty", e); bn = str(b.get("batch_no", "")).strip()
            if not bn: e["batch_no"] = "Batch number is required."
            if not b.get("supplier"): e["supplier"] = "Supplier is required."
            if not c.execute("SELECT 1 FROM commodities WHERE id=?", (b.get("commodity_id"),)).fetchone(): e["commodity_id"] = "Choose a commodity."
            elif c.execute("SELECT 1 FROM batches WHERE commodity_id=? AND batch_no=?", (b["commodity_id"], bn)).fetchone(): e["batch_no"] = "This batch number already exists for the commodity."
            rd, ex = b.get("received", ""), b.get("expiry", "")
            if not rd: e["received"] = "Received date is required."
            elif rd > today(): e["received"] = "Received date cannot be in the future."
            if not ex: e["expiry"] = "Expiry date is required."
            elif rd and ex <= rd: e["expiry"] = "Expiry must be after the received date."
            cost = num("unit_cost", e)
            if not c.execute("SELECT 1 FROM locations WHERE id=?", (b.get("location_id"),)).fetchone(): e["location_id"] = "Choose a storage location."
            fail(e); c.execute("BEGIN IMMEDIATE")
            cur = c.execute("INSERT INTO batches(commodity_id,batch_no,supplier,received,expiry,unit_cost,location_id) VALUES(?,?,?,?,?,?,?)", (b["commodity_id"], bn, b["supplier"], rd, ex, cost, b["location_id"]))
            c.execute("INSERT INTO ledger(batch_id,type,qty,reason,ref,user,ts) VALUES(?,?,?,?,?,?,?)", (cur.lastrowid, "receipt", qty, "Stock received", "", U, now())); audit(c, U, "receipt", f"{bn} +{qty:g}"); c.execute("COMMIT"); return {"ok": 1}
        if p == "/api/adjust" and m == "POST":
            need("adjust"); e = {}; d = num("qty", e, True); reason = str(b.get("reason", "")).strip()
            if len(reason) < 5: e["reason"] = "Give a reason (at least 5 characters)."
            bt = balances(c, "WHERE b.id=?", (b.get("batch_id"),))
            if not bt: e["batch_id"] = "Choose a batch."
            elif not e.get("qty") and bt[0]["balance"] + d < 0: e["qty"] = f"Would make the balance negative (current {bt[0]['balance']:g})."
            fail(e); c.execute("BEGIN IMMEDIATE"); c.execute("INSERT INTO ledger(batch_id,type,qty,reason,ref,user,ts) VALUES(?,?,?,?,?,?,?)", (b["batch_id"], "adjustment", d, reason, "", U, now()))
            audit(c, U, "adjustment", f"{bt[0]['batch_no']} {d:+g}: {reason}"); c.execute("COMMIT"); return {"ok": 1}
        if p == "/api/block" and m == "POST":
            need("adjust"); c.execute("UPDATE batches SET blocked=? WHERE id=?", (1 if b.get("blocked") else 0, b.get("batch_id"))); audit(c, U, "batch_block", f"batch {b.get('batch_id')} blocked={bool(b.get('blocked'))}"); return {"ok": 1}
        if p == "/api/requests" and m == "GET":
            need("read"); w = "WHERE r.requester=?" if me["role"] == "requester" else ""
            r = rows(c, f"SELECT r.*,c.name commodity,c.unit,l.name location FROM requests r JOIN commodities c ON c.id=r.commodity_id JOIN locations l ON l.id=r.location_id {w} ORDER BY r.id DESC", (U,) if w else ())
            for x in r: x["allocations"] = rows(c, "SELECT a.qty,b.batch_no,a.ts FROM allocations a JOIN batches b ON b.id=a.batch_id WHERE request_id=?", (x["id"],))
            return r
        if p == "/api/requests" and m == "POST":
            need("request"); e = {}; qty = num("qty", e)
            if not c.execute("SELECT 1 FROM commodities WHERE id=?", (b.get("commodity_id"),)).fetchone(): e["commodity_id"] = "Choose a commodity."
            if not c.execute("SELECT 1 FROM locations WHERE id=?", (b.get("location_id"),)).fetchone(): e["location_id"] = "Choose a destination."
            if b.get("urgency") not in {"low", "normal", "high", "critical"}: e["urgency"] = "Choose low, normal, high or critical."
            if len(str(b.get("purpose", "")).strip()) < 3: e["purpose"] = "Describe the purpose (at least 3 characters)."
            fail(e); c.execute("INSERT INTO requests(commodity_id,qty,requester,location_id,status,created,note,urgency,purpose) VALUES(?,?,?,?,?,?,?,?,?)", (b["commodity_id"], qty, U, b["location_id"], "pending", now(), "", b["urgency"], str(b["purpose"]).strip())); audit(c, U, "request_submit", f"{qty:g} of commodity {b['commodity_id']}"); return {"ok": 1}
        if p == "/api/preview": need("allocate"); r = c.execute("SELECT * FROM requests WHERE id=?", (q.get("request"),)).fetchone(); return fifo_plan(c, r["commodity_id"], r["qty"])
        if p == "/api/allocate" and m == "POST":
            need("allocate"); c.execute("BEGIN IMMEDIATE")  # write lock + process lock: no two allocations see the same stock
            r = c.execute("SELECT * FROM requests WHERE id=?", (b.get("request_id"),)).fetchone()
            if not r or r["status"] != "pending": c.execute("ROLLBACK"); raise ValueError("Request is not pending, so it cannot be changed.")
            pl = fifo_plan(c, r["commodity_id"], r["qty"])
            if b.get("action") == "reject":
                c.execute("UPDATE requests SET status='rejected',decided_by=? WHERE id=?", (U, r["id"])); audit(c, U, "request_reject", f"#{r['id']}"); c.execute("COMMIT"); return {"ok": 1}
            if pl["shortfall"] > 0 and b.get("action") != "partial": c.execute("ROLLBACK"); raise ValueError(f"Only {pl['available']:g} eligible units available; {pl['shortfall']:g} short. Choose partial fulfilment or reject.")
            if not pl["plan"]: c.execute("ROLLBACK"); raise ValueError("No eligible stock. Reject the request instead.")
            for x in pl["plan"]:
                c.execute("INSERT INTO ledger(batch_id,type,qty,reason,ref,user,ts) VALUES(?,?,?,?,?,?,?)", (x["batch_id"], "issue", -x["qty"], "FIFO allocation", f"request {r['id']}", U, now()))
                c.execute("INSERT INTO allocations(request_id,batch_id,qty,user,ts) VALUES(?,?,?,?,?)", (r["id"], x["batch_id"], x["qty"], U, now()))
            c.execute("UPDATE requests SET status=?,decided_by=? WHERE id=?", ("partial" if pl["shortfall"] > 0 else "fulfilled", U, r["id"])); audit(c, U, "allocation", f"#{r['id']}: {pl['explanation']}"); c.execute("COMMIT"); return pl
        if p == "/api/dashboard":
            need("read"); d = int(q.get("days", 30)); since = (datetime.now(timezone.utc) - timedelta(days=d)).isoformat()
            sm = lambda t: rows(c, "SELECT COALESCE(SUM(ABS(qty)),0) s FROM ledger WHERE type=? AND ts>=?", (t, since))[0]["s"]
            bs = balances(c); [x.update(status=status(x)) for x in bs]; com = rows(c, "SELECT * FROM commodities")
            for x in com: x["stock"] = sum(y["balance"] for y in eligible(c, x["id"]))
            fc = [dict(forecast(c, x["id"]), commodity=x["name"]) for x in com]
            ser = rows(c, "SELECT substr(ts,1,10) day,type,SUM(ABS(qty)) q FROM ledger WHERE ts>=? AND type IN('receipt','issue') GROUP BY day,type ORDER BY day", (since,))
            return {"days": d, "available": sum(x["stock"] for x in com), "commodities": len(com), "received": sm("receipt"), "issued": sm("issue"), "allocated": rows(c, "SELECT COALESCE(SUM(qty),0) s FROM allocations WHERE ts>=?", (since,))[0]["s"],
                    "low": [x["name"] for x in com if x["stock"] <= x["reorder"]], "expiring": [x for x in bs if x["status"] == "expiring" and x["balance"] > 0], "expired": [x for x in bs if x["status"] == "expired" and x["balance"] > 0],
                    "requests": rows(c, "SELECT status,COUNT(*) n FROM requests GROUP BY status"), "forecast": fc, "series": ser, "activity": rows(c, "SELECT * FROM audit ORDER BY id DESC LIMIT 8"), "generated": now()}
        if p == "/api/map":
            need("read"); cid = q.get("commodity", ""); ls = rows(c, "SELECT * FROM locations")
            for l in ls:
                bs = [x for x in balances(c, "WHERE b.location_id=? AND (?='' OR b.commodity_id=?)", (l["id"], cid, cid)) if x["balance"] > 0]
                l["stock"] = sum(x["balance"] for x in bs); l["batches"] = [{"batch_no": x["batch_no"], "commodity": x["commodity"], "balance": x["balance"], "unit": x["unit"], "status": status(x)} for x in bs]
                l["open_requests"] = c.execute("SELECT COUNT(*) FROM requests WHERE location_id=? AND status='pending'", (l["id"],)).fetchone()[0]
                l["low"] = bool(cid) and l["stock"] <= 0
            return ls
        if p == "/api/forecast": need("read"); r = forecast(c, int(q["commodity"])); return r
        if p.startswith("/api/report/"):
            need("reports"); nm = p.split("/")[-1]; f = q
            qs = {"inventory": ("SELECT c.name commodity,b.batch_no,l.name location,b.received,b.expiry,SUM(g.qty) balance FROM batches b JOIN commodities c ON c.id=b.commodity_id JOIN locations l ON l.id=b.location_id JOIN ledger g ON g.batch_id=b.id WHERE (?='' OR b.commodity_id=?) GROUP BY b.id ORDER BY b.received,b.batch_no", (f.get("commodity", ""),) * 2),
                  "allocations": ("SELECT r.id request,c.name commodity,b.batch_no,a.qty,r.status,a.user,a.ts FROM allocations a JOIN requests r ON r.id=a.request_id JOIN batches b ON b.id=a.batch_id JOIN commodities c ON c.id=r.commodity_id WHERE (?='' OR c.id=?) AND a.ts>=? ORDER BY a.id DESC", (f.get("commodity", ""),) * 2 + (f.get("from", "") or "0",)),
                  "transactions": ("SELECT g.ts,g.type,c.name commodity,b.batch_no,g.qty,g.reason,g.user FROM ledger g JOIN batches b ON b.id=g.batch_id JOIN commodities c ON c.id=b.commodity_id WHERE (?='' OR g.type=?) AND (?='' OR c.id=?) AND g.ts>=? ORDER BY g.id DESC", (f.get("type", ""),) * 2 + (f.get("commodity", ""),) * 2 + (f.get("from", "") or "0",)),
                  "expiry": ("SELECT c.name commodity,b.batch_no,b.expiry,SUM(g.qty) balance,CASE WHEN b.expiry<date('now') THEN 'expired' ELSE 'expiring in 30 days' END state FROM batches b JOIN commodities c ON c.id=b.commodity_id JOIN ledger g ON g.batch_id=b.id WHERE b.expiry<=date('now','+30 day') GROUP BY b.id HAVING balance>0 ORDER BY b.expiry", ()),
                  "forecast_vs_actual": None}
            if nm == "forecast_vs_actual":
                data = [{"commodity": x["name"], "avg_weekly_actual_last4": (lambda s: round(sum(s[-4:]) / max(len(s[-4:]), 1), 1))(forecast(c, x["id"])["history"]), "projected_weekly": forecast(c, x["id"])["projection"][0], "confidence": forecast(c, x["id"])["confidence"]} for x in rows(c, "SELECT * FROM commodities")]
            else: data = rows(c, *qs[nm])
            if q.get("format") == "csv":
                s = io.StringIO(); w = csv.writer(s); w.writerow(["# generated", now(), "filters", json.dumps({k: v for k, v in q.items() if k != "format"})])
                if data: w.writerow(data[0].keys()); [w.writerow(r.values()) for r in data]
                return (200, s.getvalue().encode(), "text/csv", {"Content-Disposition": f"attachment; filename={nm}.csv"})
            return {"rows": data, "generated": now(), "filters": {k: v for k, v in q.items() if k != "format"}}
        if p == "/api/audit": need("audit"); return rows(c, "SELECT * FROM audit ORDER BY id DESC LIMIT 100")
        if p == "/api/users": need("users"); return rows(c, "SELECT id,username,role FROM users")
        if p == "/api/user_role" and m == "POST":
            need("users")
            if b.get("role") not in ROLES["read"] and b.get("role") != "admin": raise ValueError({"role": "Invalid role."})
            if b.get("username") == U: raise ValueError({"role": "You cannot change your own role."})
            c.execute("UPDATE users SET role=? WHERE username=?", (b["role"], b["username"])); audit(c, U, "role_change", f"{b['username']} -> {b['role']}"); return {"ok": 1}
        if p == "/api/settings" and m == "POST": need("settings"); c.execute("UPDATE settings SET v=? WHERE k='org_name'", (str(b.get("org_name", ""))[:80] or "IPDs",)); audit(c, U, "settings", "org name changed"); return {"ok": 1}
        return (404, {"error": "Not found"})

if __name__ == "__main__":
    if not os.path.exists(DB): seed(); print("Seeded demo data into", DB)
    port = int(os.environ.get("PORT", 8000)); print(f"http://localhost:{port}"); ThreadingHTTPServer((os.environ.get("HOST", "0.0.0.0"), port), H).serve_forever()
