#!/usr/bin/env python3
"""
Login Server v1.0 — port 8768
Zarządza kontami graczy. Inne serwery mogą weryfikować
token gracza przez Hub Server (port 1000).
"""
import asyncio, json, uuid, logging, time
from logging.handlers import RotatingFileHandler
from pathlib import Path

try:
    import websockets
except ImportError:
    print("pip install websockets"); raise SystemExit(1)

_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
_con = logging.StreamHandler(); _con.setFormatter(_fmt)
_fh  = RotatingFileHandler("login.log", maxBytes=2*1024*1024, backupCount=2, encoding="utf-8")
_fh.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_con, _fh])
log = logging.getLogger("login")

HOST="0.0.0.0"; PORT=8768; HUB_URL="ws://localhost:1000"
ACCOUNTS_FILE="accounts.json"; START=time.time()

# Aktywne sesje: token → { username, role, login_time }
sessions = {}
# ws → token
ws_tokens = {}
hub_ws = None

def load_accounts():
    p = Path(ACCOUNTS_FILE)
    if not p.exists():
        default = {"accounts":[
            {"username":"admin",  "password":"admin123","role":"admin"},
            {"username":"player1","password":"pass1",   "role":"player"},
            {"username":"player2","password":"pass2",   "role":"player"},
        ]}
        p.write_text(json.dumps(default, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info(f"  Utworzono {ACCOUNTS_FILE}")
        return default
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        log.info(f"  Konta: {len(data.get('accounts',[]))}")
        return data
    except Exception as e:
        log.error(f"  Blad accounts: {e}"); return {"accounts":[]}

def save_accounts():
    try:
        Path(ACCOUNTS_FILE).write_text(
            json.dumps(ACCOUNTS, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log.error(f"  Blad zapisu: {e}")

ACCOUNTS = load_accounts()

def get_account(username):
    for a in ACCOUNTS.get("accounts",[]):
        if a.get("username")==username: return a
    return None

async def send_to(ws, payload):
    try: await ws.send(json.dumps(payload))
    except: pass

async def hub_send(payload):
    if hub_ws:
        try: await hub_ws.send(json.dumps(payload))
        except: pass

async def hub_connect():
    global hub_ws
    while True:
        try:
            async with websockets.connect(HUB_URL) as ws:
                hub_ws = ws
                await ws.send(json.dumps({
                    "type":"register","name":"login","port":PORT,"version":"1.0",
                    "meta":{"description":"Login Server","accounts":len(ACCOUNTS.get("accounts",[]))}
                }))
                log.info("  Hub: połączono")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        # Weryfikacja tokenu przez inne serwery
                        if msg.get("type")=="verify_token":
                            token = msg.get("token","")
                            sess  = sessions.get(token)
                            resp  = {
                                "type":  "token_result",
                                "token": token,
                                "valid": sess is not None,
                            }
                            if sess:
                                resp["username"] = sess["username"]
                                resp["role"]     = sess["role"]
                            # Odpowiedź relay do pytającego serwera
                            await hub_ws.send(json.dumps({
                                "type": "relay",
                                "to":   msg.get("from","mp"),
                                **resp
                            }))
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"  Hub offline: {e} — retry 5s")
        finally:
            hub_ws = None
        await asyncio.sleep(5)

async def handle_client(ws):
    my_token = None
    log.info(f"+ {ws.remote_address}")
    try:
        async for raw in ws:
            try: msg = json.loads(raw)
            except: continue
            t = msg.get("type","")

            # ── Logowanie ──────────────────────────────────
            if t == "login":
                uname = str(msg.get("username","")).strip()
                pw    = str(msg.get("password",""))
                acc   = get_account(uname)
                if acc and acc.get("password") == pw:
                    token    = str(uuid.uuid4())
                    my_token = token
                    sessions[token] = {
                        "username":   uname,
                        "role":       acc.get("role","player"),
                        "login_time": time.strftime("%H:%M:%S"),
                    }
                    ws_tokens[ws] = token
                    log.info(f"  LOGIN OK: '{uname}'")
                    await send_to(ws,{
                        "type":       "login_success",
                        "username":   uname,
                        "role":       acc.get("role","player"),
                        "token":      token,
                        "login_time": sessions[token]["login_time"],
                    })
                    await hub_send({"type":"update_stats","clients":len(sessions),
                                    "meta":{"sessions":len(sessions)}})
                else:
                    log.warning(f"  LOGIN FAIL: '{uname}'")
                    await send_to(ws,{"type":"login_failed","reason":"Błędne dane"})

            # ── Gość ───────────────────────────────────────
            elif t == "guest_login":
                name  = str(msg.get("name","Gość")).strip() or "Gość"
                token = str(uuid.uuid4())
                my_token = token
                sessions[token] = {
                    "username":   name,
                    "role":       "guest",
                    "login_time": time.strftime("%H:%M:%S"),
                }
                ws_tokens[ws] = token
                log.info(f"  GOŚĆ: '{name}'")
                await send_to(ws,{
                    "type":"login_success","username":name,
                    "role":"guest","token":token,
                })

            # ── Wylogowanie ────────────────────────────────
            elif t == "logout":
                token = str(msg.get("token",""))
                sess  = sessions.pop(token, None)
                ws_tokens.pop(ws, None)
                my_token = None
                log.info(f"  LOGOUT: '{sess['username'] if sess else '?'}'")
                await send_to(ws,{"type":"ok","message":"Wylogowano"})

            # ── Weryfikacja tokenu ─────────────────────────
            elif t == "verify_token":
                token = str(msg.get("token",""))
                sess  = sessions.get(token)
                if sess:
                    await send_to(ws,{"type":"token_valid","username":sess["username"],"role":sess["role"]})
                else:
                    await send_to(ws,{"type":"token_invalid"})

            # ── Rejestracja ────────────────────────────────
            elif t == "register_account":
                uname = str(msg.get("username","")).strip()
                pw    = str(msg.get("password",""))
                role  = str(msg.get("role","player"))
                if not uname or not pw:
                    await send_to(ws,{"type":"error","message":"Brak danych"}); continue
                if get_account(uname):
                    await send_to(ws,{"type":"error","message":"Użytkownik istnieje"}); continue
                ACCOUNTS["accounts"].append({"username":uname,"password":pw,"role":role})
                save_accounts()
                log.info(f"  REGISTER: '{uname}' ({role})")
                await send_to(ws,{"type":"ok","message":f"Konto '{uname}' utworzone"})

            # ── Lista kont (tylko admin) ───────────────────
            elif t == "list_accounts":
                token = str(msg.get("token",""))
                sess  = sessions.get(token)
                if not sess or sess.get("role") != "admin":
                    await send_to(ws,{"type":"error","message":"Tylko admin"}); continue
                accounts = [{"username":a["username"],"role":a.get("role","player")}
                            for a in ACCOUNTS.get("accounts",[])]
                await send_to(ws,{"type":"accounts_list","accounts":accounts})

    except websockets.exceptions.ConnectionClosed: pass
    except Exception as e: log.error(f"  Blad: {e}",exc_info=True)
    finally:
        if my_token:
            sessions.pop(my_token, None)
        ws_tokens.pop(ws, None)
        log.info(f"- {ws.remote_address}")

async def stats_loop():
    while True:
        await asyncio.sleep(30)
        log.info(f"  Sesje: {len(sessions)}  Konta: {len(ACCOUNTS.get('accounts',[]))}")

async def main():
    log.info("="*46)
    log.info("  Login Server  v1.0")
    log.info(f"  ws://{HOST}:{PORT}")
    log.info(f"  Konta: {ACCOUNTS_FILE}")
    log.info(f"  Hub: {HUB_URL}")
    log.info("="*46)
    async with websockets.serve(handle_client, HOST, PORT):
        await asyncio.gather(asyncio.Future(), stats_loop(), hub_connect())

if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("Login zatrzymany.")
