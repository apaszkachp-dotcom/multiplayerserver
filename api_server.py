#!/usr/bin/env python3
"""
API Server v2.0 — port 8766
Globalny magazyn statów graczy.

ZMIANA v2.0:
  Dane API są GLOBALNE — nie per pokój.
  Klucze jak kills_1, deaths_2, score_1 są widoczne
  dla wszystkich podłączonych klientów niezależnie
  od pokoju. Dane nie znikają gdy pokój się opróżni.

  Jedyny reset to jawne api_reset lub restart serwera.

TYPY WIADOMOŚCI (klient → serwer):
  join_api      { room }   dołącz (room tylko dla info)
  api_set       { key, value }
  api_get       { key }    → api_value
  api_get_all   {}         → api_all
  api_reset     { key? }   reset klucza lub wszystkich

TYPY WIADOMOŚCI (serwer → klient):
  api_ready     { }
  api_update    { key, value }   broadcast do wszystkich
  api_value     { key, value }   odpowiedź na api_get
  api_all       { data }         cały słownik
  api_reset_done { keys }
"""
import asyncio, json, logging, time
from logging.handlers import RotatingFileHandler

try:
    import websockets
except ImportError:
    print("pip install websockets"); raise SystemExit(1)

_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
_con = logging.StreamHandler(); _con.setFormatter(_fmt)
_fh  = RotatingFileHandler("api.log", maxBytes=2*1024*1024, backupCount=2, encoding="utf-8")
_fh.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_con, _fh])
log = logging.getLogger("api")

HOST="0.0.0.0"; PORT=8766; HUB_URL="ws://localhost:1000"; START=time.time()

# ── GLOBALNE dane — jeden słownik dla wszystkich ───────────
global_data = {}   # { key: value }

# Wszyscy podłączeni klienci
all_clients = set()   # set of ws
hub_ws = None

async def send_to(ws, payload):
    try: await ws.send(json.dumps(payload))
    except: pass

async def broadcast_all(payload, exclude=None):
    """Rozsyła do WSZYSTKICH podłączonych klientów."""
    dead = set()
    for ws in list(all_clients):
        if ws is exclude: continue
        try: await ws.send(json.dumps(payload))
        except: dead.add(ws)
    all_clients.difference_update(dead)

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
                    "type":"register","name":"api","port":PORT,"version":"2.0",
                    "meta":{"description":"API Server (global data)","keys":len(global_data)}
                }))
                log.info("  Hub: połączono")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("type")=="hub_command" and msg.get("command")=="reset_all":
                            global_data.clear()
                            await broadcast_all({"type":"api_all","data":{}})
                            log.info("  HUB CMD: reset_all")
                    except: pass
        except Exception as e:
            log.warning(f"  Hub offline: {e} — retry 5s")
        finally:
            hub_ws = None
        await asyncio.sleep(5)

async def handle_client(ws):
    room_id = "global"   # tylko do logowania
    log.info(f"+ {ws.remote_address}")
    all_clients.add(ws)

    try:
        async for raw in ws:
            try: msg = json.loads(raw)
            except: continue
            t = msg.get("type","")

            # ── Dołącz ────────────────────────────────────
            if t == "join_api":
                room_id = str(msg.get("room","global")).strip()[:64]
                log.info(f"  JOIN_API [{room_id}]  clients={len(all_clients)}")
                await send_to(ws, {"type":"api_ready"})
                # Wyślij CAŁY globalny stan od razu
                await send_to(ws, {"type":"api_all","data":dict(global_data)})
                await hub_send({"type":"update_stats","clients":len(all_clients),
                                "meta":{"keys":len(global_data)}})

            # ── Zapisz wartość ─────────────────────────────
            elif t == "api_set":
                key   = str(msg.get("key","")).strip()[:64]
                value = msg.get("value", 0)
                if not key: continue
                global_data[key] = value
                log.info(f"  SET {key}={value}  (total keys: {len(global_data)})")
                # Broadcast do WSZYSTKICH — łącznie z nadawcą
                await broadcast_all({"type":"api_update","key":key,"value":value})

            # ── Pobierz jedną wartość ──────────────────────
            elif t == "api_get":
                key   = str(msg.get("key","")).strip()
                value = global_data.get(key, 0)
                await send_to(ws, {"type":"api_value","key":key,"value":value})

            # ── Pobierz wszystko ───────────────────────────
            elif t == "api_get_all":
                await send_to(ws, {"type":"api_all","data":dict(global_data)})

            # ── Reset ──────────────────────────────────────
            elif t == "api_reset":
                key = str(msg.get("key","")).strip()
                if key:
                    # Reset jednego klucza
                    global_data.pop(key, None)
                    log.info(f"  RESET key={key}")
                    await broadcast_all({"type":"api_update","key":key,"value":0})
                    await send_to(ws, {"type":"api_reset_done","keys":[key]})
                else:
                    # Reset wszystkich
                    keys = list(global_data.keys())
                    global_data.clear()
                    log.info(f"  RESET all ({len(keys)} kluczy)")
                    for k in keys:
                        await broadcast_all({"type":"api_update","key":k,"value":0})
                    await send_to(ws, {"type":"api_reset_done","keys":keys})
                await hub_send({"type":"update_stats","clients":len(all_clients),
                                "meta":{"keys":len(global_data)}})

    except websockets.exceptions.ConnectionClosed: pass
    except Exception as e: log.error(f"  Blad: {e}", exc_info=True)
    finally:
        all_clients.discard(ws)
        await hub_send({"type":"update_stats","clients":len(all_clients)})
        log.info(f"- {ws.remote_address}  (pozostało klientów: {len(all_clients)})")

async def stats_loop():
    while True:
        await asyncio.sleep(30)
        log.info(f"  Klienci:{len(all_clients)}  Kluczy:{len(global_data)}  Uptime:{int(time.time()-START)}s")

async def main():
    log.info("="*50)
    log.info("  API Server  v2.0  (dane globalne)")
    log.info(f"  ws://{HOST}:{PORT}  Hub: {HUB_URL}")
    log.info("  Dane nie sa per-pokoj — jeden slownik dla wszystkich")
    log.info("="*50)
    async with websockets.serve(handle_client, HOST, PORT):
        await asyncio.gather(asyncio.Future(), stats_loop(), hub_connect())

if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("API zatrzymany.")
