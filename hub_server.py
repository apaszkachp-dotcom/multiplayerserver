#!/usr/bin/env python3
"""
Hub Server v1.0 — port 1000
Centralny rejestr wszystkich serwerów.
Każdy serwer rejestruje się tutaj podając nazwę i port.
Strona www i konfigurator pobierają stąd statusy.
"""
import asyncio, json, logging, time
from logging.handlers import RotatingFileHandler

try:
    import websockets
except ImportError:
    print("pip install websockets"); raise SystemExit(1)

_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
_con = logging.StreamHandler(); _con.setFormatter(_fmt)
_fh  = RotatingFileHandler("hub.log", maxBytes=2*1024*1024, backupCount=2, encoding="utf-8")
_fh.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_con, _fh])
log = logging.getLogger("hub")

HOST  = "0.0.0.0"
PORT  = 1000
START = time.time()

registry      = {}  # name → { ws, port, version, clients, uptime_start, meta }
ws_to_name    = {}  # ws → name
panel_clients = set()

async def send_to(ws, payload):
    try: await ws.send(json.dumps(payload))
    except: pass

async def push_status_to_panels():
    status = build_status()
    dead = set()
    for ws in list(panel_clients):
        try: await ws.send(json.dumps({"type":"hub_status",**status}))
        except: dead.add(ws)
    panel_clients.difference_update(dead)

def build_status():
    now = time.time()
    servers = {}
    for name, info in registry.items():
        servers[name] = {
            "port":    info.get("port",0),
            "version": info.get("version","?"),
            "clients": info.get("clients",0),
            "uptime":  int(now - info.get("uptime_start",now)),
            "online":  True,
            "meta":    info.get("meta",{}),
        }
    return {
        "servers":      servers,
        "hub_uptime":   int(now - START),
        "hub_port":     PORT,
        "server_count": len(servers),
    }

async def handle(ws):
    name = None; is_panel = False
    log.info(f"+ {ws.remote_address}")
    try:
        async for raw in ws:
            try: msg = json.loads(raw)
            except: continue
            t = msg.get("type","")

            if t == "register":
                name    = str(msg.get("name","?"))[:32].strip()
                port    = int(msg.get("port",0))
                version = str(msg.get("version","1.0"))
                meta    = msg.get("meta",{})
                registry[name] = {
                    "ws":ws,"port":port,"version":version,
                    "clients":0,"uptime_start":time.time(),"meta":meta,
                }
                ws_to_name[ws] = name
                log.info(f"  REGISTER: {name} :{port}")
                await send_to(ws, {
                    "type":"registered","name":name,"hub_port":PORT,
                    "connected_servers":list(registry.keys()),
                })
                await push_status_to_panels()
                await broadcast_servers({"type":"server_up","name":name,"port":port}, exclude=ws)

            elif t == "update_stats" and name:
                registry[name]["clients"] = int(msg.get("clients",0))
                registry[name]["meta"]    = msg.get("meta", registry[name].get("meta",{}))
                await push_status_to_panels()

            elif t == "panel_connect":
                is_panel = True; panel_clients.add(ws)
                await send_to(ws, {"type":"hub_status",**build_status()})

            elif t == "status":
                await send_to(ws, {"type":"hub_status",**build_status()})

            elif t == "relay" and name:
                to = str(msg.get("to",""))
                payload = {k:v for k,v in msg.items() if k!="to"}
                payload["from"] = name
                if to in registry: await send_to(registry[to]["ws"], payload)
                else: await send_to(ws, {"type":"error","message":f"'{to}' offline"})

            elif t == "broadcast" and name:
                payload = dict(msg); payload["from"] = name
                await broadcast_servers(payload, exclude=ws)

            elif t == "command":
                target  = str(msg.get("target",""))
                command = str(msg.get("command",""))
                if target == "all":
                    for info in registry.values():
                        await send_to(info["ws"],{"type":"hub_command","command":command})
                elif target in registry:
                    await send_to(registry[target]["ws"],{"type":"hub_command","command":command})
                    log.info(f"  CMD → {target}: {command}")

            # ── Admin komendy przekazywane do MP ──────────
            elif t in ("admin_kick","admin_ban","admin_map","admin_status",
                        "admin_broadcast","admin_create_room","admin_delete_room",
                        "admin_reset_walls","room_list"):
                if "mp" in registry:
                    payload = dict(msg)
                    payload["from"] = "panel"
                    await send_to(registry["mp"]["ws"], payload)
                    log.info(f"  ADMIN→MP: {t}  room={msg.get('room','?')}")
                else:
                    await send_to(ws, {"type":"error","message":"Multiplayer Server offline"})

            # ── Log admina — roześlij do paneli ───────────
            elif t == "admin_log":
                import time as _t
                entry = {
                    "type":   "admin_log",
                    "action": msg.get("action","?"),
                    "room":   msg.get("room",""),
                    "time":   msg.get("time", _t.strftime("%H:%M:%S")),
                    "data":   {k:v for k,v in msg.items() if k not in ("type","from")},
                }
                dead = set()
                for pws in list(panel_clients):
                    try: await pws.send(__import__("json").dumps(entry))
                    except: dead.add(pws)
                panel_clients.difference_update(dead)
                log.info(f"  ADMIN LOG: {entry['action']} [{entry['room']}]")

    except websockets.exceptions.ConnectionClosed: pass
    except Exception as e: log.error(f"  Blad: {e}", exc_info=True)
    finally:
        if is_panel: panel_clients.discard(ws)
        if name and registry.get(name,{}).get("ws") is ws:
            del registry[name]
            log.info(f"- DOWN: {name}")
            await broadcast_servers({"type":"server_down","name":name})
            await push_status_to_panels()
        ws_to_name.pop(ws,None)
        log.info(f"- {ws.remote_address}")

async def broadcast_servers(payload, exclude=None):
    for info in list(registry.values()):
        if info["ws"] is not exclude:
            await send_to(info["ws"], payload)

async def stats_loop():
    while True:
        await asyncio.sleep(30)
        log.info(f"  Online: {list(registry.keys())}  Panele: {len(panel_clients)}")

async def main():
    log.info("="*46)
    log.info("  Hub Server  v1.0")
    log.info(f"  ws://{HOST}:{PORT}")
    log.info("="*46)
    async with websockets.serve(handle, HOST, PORT):
        await asyncio.gather(asyncio.Future(), stats_loop())

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("Hub zatrzymany.")
