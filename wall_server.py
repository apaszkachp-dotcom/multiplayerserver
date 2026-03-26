#!/usr/bin/env python3
"""
Wall Server v1.0 — port 8767
Synchronizacja stanu ścian (HP, wizualizacja, zniszczenie).
Łączy się z Hub Server (port 1000).
"""
import asyncio, json, logging, time
from logging.handlers import RotatingFileHandler

try:
    import websockets
except ImportError:
    print("pip install websockets"); raise SystemExit(1)

_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
_con = logging.StreamHandler(); _con.setFormatter(_fmt)
_fh  = RotatingFileHandler("wall.log", maxBytes=2*1024*1024, backupCount=2, encoding="utf-8")
_fh.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_con, _fh])
log = logging.getLogger("wall")

HOST="0.0.0.0"; PORT=8767; HUB_URL="ws://localhost:1000"
WALL_HP_ONLY_DECREASING=True
VIS1_THRESHOLD=60; VIS2_THRESHOLD=20
START=time.time()

# walls[room_id] = { wall_id: { hp, vis, destroyed } }
walls = {}
# rooms[room_id] = set of ws
room_clients = {}
# ws → room_id
ws_room = {}
hub_ws = None

def hp_to_vis(hp):
    if hp > VIS1_THRESHOLD: return 0
    if hp > VIS2_THRESHOLD: return 1
    return 2

async def send_to(ws, payload):
    try: await ws.send(json.dumps(payload))
    except: pass

async def broadcast_room(rid, payload, exclude=None):
    for ws in list(room_clients.get(rid, set())):
        if ws is not exclude:
            await send_to(ws, payload)

async def hub_send(payload):
    if hub_ws:
        try: await hub_ws.send(json.dumps(payload))
        except: pass

def total_clients():
    return sum(len(v) for v in room_clients.values())

async def hub_connect():
    global hub_ws
    while True:
        try:
            async with websockets.connect(HUB_URL) as ws:
                hub_ws = ws
                await ws.send(json.dumps({
                    "type":"register","name":"wall","port":PORT,"version":"1.0",
                    "meta":{"description":"Wall Server","rooms":len(walls)}
                }))
                log.info("  Hub: połączono")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("type")=="hub_command":
                            cmd=msg.get("command","")
                            if cmd=="reset_all_walls":
                                walls.clear()
                                log.info("  HUB CMD: reset_all_walls")
                    except: pass
        except Exception as e:
            log.warning(f"  Hub offline: {e} — retry 5s")
        finally:
            hub_ws = None
        await asyncio.sleep(5)

async def handle_client(ws):
    room_id = None
    log.info(f"+ {ws.remote_address}")
    try:
        async for raw in ws:
            try: msg = json.loads(raw)
            except: continue
            t = msg.get("type","")

            # ── Dołącz do pokoju ────────────────────────────
            if t == "join_walls":
                new_room = str(msg.get("room","default")).strip()[:64]
                if room_id:
                    room_clients.get(room_id, set()).discard(ws)
                room_id = new_room
                ws_room[ws] = room_id
                if room_id not in room_clients: room_clients[room_id] = set()
                if room_id not in walls: walls[room_id] = {}
                room_clients[room_id].add(ws)
                log.info(f"  JOIN_WALLS [{room_id}]")
                await send_to(ws, {"type":"walls_ready","room":room_id})
                # Wyślij aktualny stan wszystkich ścian
                for wid, wdata in walls[room_id].items():
                    await send_to(ws, {"type":"wall","id":wid,**wdata})
                await hub_send({"type":"update_stats","clients":total_clients(),"meta":{"rooms":len(walls)}})

            # ── Aktualizacja ściany ──────────────────────────
            elif t == "wall" and room_id:
                wall_id   = str(msg.get("id","")).strip()[:32]
                if not wall_id: continue
                try:
                    new_hp    = max(0, min(100, int(msg.get("hp", 100))))
                    destroyed = bool(msg.get("destroyed", False))
                except (TypeError, ValueError): continue

                prev = walls.get(room_id,{}).get(wall_id,{})
                prev_hp = prev.get("hp", 100)

                if WALL_HP_ONLY_DECREASING and new_hp > prev_hp:
                    continue

                # Przelicz vis na podstawie HP
                if new_hp <= 0:
                    destroyed = True; vis = 2
                elif new_hp > VIS1_THRESHOLD: vis = 0
                elif new_hp > VIS2_THRESHOLD: vis = 1
                else: vis = 2

                walls[room_id][wall_id] = {"hp":new_hp,"vis":vis,"destroyed":destroyed}
                log.info(f"  WALL [{room_id}] {wall_id} hp={new_hp} vis={vis}")

                await broadcast_room(room_id, {
                    "type":"wall","id":wall_id,
                    "hp":new_hp,"vis":vis,"destroyed":destroyed
                })

            # ── Reset ścian ──────────────────────────────────
            elif t == "reset_walls" and room_id:
                wall_id = str(msg.get("id","")).strip()
                if wall_id and wall_id in walls.get(room_id,{}):
                    del walls[room_id][wall_id]
                    await broadcast_room(room_id,{"type":"wall","id":wall_id,"hp":100,"vis":0,"destroyed":False})
                elif not wall_id:
                    ids = list(walls.get(room_id,{}).keys())
                    walls[room_id] = {}
                    for wid in ids:
                        await broadcast_room(room_id,{"type":"wall","id":wid,"hp":100,"vis":0,"destroyed":False})
                    log.info(f"  RESET_WALLS [{room_id}] ({len(ids)} scian)")

            # ── Status ──────────────────────────────────────
            elif t == "wall_status":
                rid = room_id or str(msg.get("room",""))
                w   = walls.get(rid,{})
                await send_to(ws,{"type":"walls_status","room":rid,"count":len(w),"walls":w})

    except websockets.exceptions.ConnectionClosed: pass
    except Exception as e: log.error(f"  Blad: {e}",exc_info=True)
    finally:
        if room_id:
            room_clients.get(room_id, set()).discard(ws)
            if not room_clients.get(room_id):
                room_clients.pop(room_id, None)
                walls.pop(room_id, None)
                log.info(f"  Pokoj '{room_id}' usuniety")
        ws_room.pop(ws, None)
        await hub_send({"type":"update_stats","clients":total_clients()})
        log.info(f"- {ws.remote_address}")

async def stats_loop():
    while True:
        await asyncio.sleep(30)
        log.info(f"  Pokoje:{len(walls)}  Klienci:{total_clients()}  Uptime:{int(time.time()-START)}s")

async def main():
    log.info("="*46)
    log.info("  Wall Server  v1.0")
    log.info(f"  ws://{HOST}:{PORT}")
    log.info(f"  Hub: {HUB_URL}")
    log.info(f"  hp_only_decreasing={WALL_HP_ONLY_DECREASING}")
    log.info("="*46)
    async with websockets.serve(handle_client, HOST, PORT):
        await asyncio.gather(asyncio.Future(), stats_loop(), hub_connect())

if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("Wall zatrzymany.")
