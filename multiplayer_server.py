#!/usr/bin/env python3
"""
Multiplayer Server v7.1 — port 8765
Synchronizacja pozycji, danych, czatu graczy.
Łączy się z Hub Server (port 1000) tylko do raportowania statusu.
"""
import asyncio, json, uuid, logging, time
from collections import deque
from logging.handlers import RotatingFileHandler

try:
    import websockets
except ImportError:
    print("pip install websockets"); raise SystemExit(1)

_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
_con = logging.StreamHandler(); _con.setFormatter(_fmt)
_fh  = RotatingFileHandler("multiplayer.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
_fh.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_con, _fh])
log = logging.getLogger("mp")

HOST="0.0.0.0"; PORT=8765; HUB_URL="ws://localhost:1000"
CHAT_RATE_LIMIT=5; CHAT_RATE_WINDOW=3.0; CHAT_MAX_LEN=256
LOG_POSITIONS=True; POS_LOG_INTERVAL=5.0
MAX_PLAYERS_PER_ROOM=0; MAX_ROOMS=0
START=time.time()

rooms={}; clients={}
hub_ws=None

async def hub_connect():
    global hub_ws
    while True:
        try:
            async with websockets.connect(HUB_URL) as ws:
                hub_ws = ws
                await ws.send(json.dumps({
                    "type":"register","name":"mp","port":PORT,"version":"7.1",
                    "meta":{"description":"Multiplayer Server"}
                }))
                log.info("  Hub: połączono")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("type")=="hub_command":
                            if msg.get("command")=="status":
                                await hub_send({"type":"update_stats","clients":total_clients()})
                    except: pass
        except Exception as e:
            log.warning(f"  Hub offline: {e} — retry 5s")
        finally:
            hub_ws = None
        await asyncio.sleep(5)

async def hub_send(payload):
    if hub_ws:
        try: await hub_ws.send(json.dumps(payload))
        except: pass

def total_clients():
    return sum(len(get_players(r)) for r in rooms)

def make_meta(pw=""):
    return {
        "host_uuid":None,"map_id":"","status":"waiting",
        "pw_hash":pw,"chat_history":deque(maxlen=30),
        "banned":set(),"chat_last":"","chat_from":"","walls":{}
    }

def get_players(rid):
    return {k:v for k,v in rooms[rid].items() if k!="_meta" and k!="_api"}

def remap_for(rid,viewer):
    pl=get_players(rid)
    if not pl: return {}
    ordered=sorted(pl.keys(),key=lambda u:pl[u]["joined_at"])
    others=[u for u in ordered if u!=viewer]
    return {u:str(i+1) for i,u in enumerate([viewer]+others)}

def get_nick(rid,uid):
    return rooms.get(rid,{}).get(uid,{}).get("data",{}).get("nick","Gracz")

def build_server_keys(rid,viewer):
    meta=rooms[rid]["_meta"]; pl=get_players(rid); mapping=remap_for(rid,viewer)
    parts=[f"{get_nick(rid,u)}|{lid}" for u,lid in mapping.items()]
    host_lid=mapping.get(meta.get("host_uuid",""),"")
    vping=rooms[rid].get(viewer,{}).get("last_ping_ms","")
    return {
        "player_count":len(pl),"player_list":"|".join(parts),
        "map_id":meta.get("map_id",""),"room_status":meta.get("status","waiting"),
        "host_id":host_lid,"has_password":"1" if meta.get("pw_hash") else "0",
        "chat_last":meta.get("chat_last",""),"chat_from":meta.get("chat_from",""),
        "ping_ms":vping if vping is not None else ""
    }

def local_snapshot(rid,viewer):
    mapping=remap_for(rid,viewer); pl=get_players(rid); snap={}
    for uid,lid in mapping.items():
        p=pl[uid]
        snap[lid]={"x":p.get("x",0),"y":p.get("y",0),"rotation":p.get("rotation",90),
                   "delete":p.get("delete",0),**p.get("data",{})}
    snap["server"]=build_server_keys(rid,viewer)
    snap["0"]=dict(rooms[rid].get("_api",{}))
    return snap

async def send_to(ws,payload):
    try: await ws.send(json.dumps(payload))
    except: pass

async def broadcast_delta(rid,sender,base):
    for viewer,info in list(get_players(rid).items()):
        if viewer==sender: continue
        lid=remap_for(rid,viewer).get(sender)
        if lid: await send_to(info["ws"],{**base,"id":lid})

async def push_server_keys(rid):
    for viewer,info in list(get_players(rid).items()):
        for k,v in build_server_keys(rid,viewer).items():
            await send_to(info["ws"],{"type":"data","id":"server","key":k,"value":v})

async def push_api_key(rid,key,value):
    for info in list(get_players(rid).values()):
        await send_to(info["ws"],{"type":"data","id":"0","key":key,"value":value})

def is_host(rid,uid): return rooms[rid]["_meta"].get("host_uuid")==uid

def lid_to_uuid(rid,viewer,lid):
    for uid,l in remap_for(rid,viewer).items():
        if l==str(lid): return uid
    return None

async def handle_client(ws):
    my_uuid=str(uuid.uuid4()); room_id=None
    log.info(f"+ {ws.remote_address}  {my_uuid[:8]}")
    clients[ws]={"uuid":my_uuid,"room":None}
    try:
        async for raw in ws:
            try: msg=json.loads(raw)
            except: continue
            t=msg.get("type","")
            if room_id and my_uuid in rooms.get(room_id,{}):
                rooms[room_id][my_uuid]["last_seen"]=time.time()

            if t=="join":
                nr=str(msg.get("room","default")); pw=str(msg.get("pw",""))
                if MAX_ROOMS>0 and nr not in rooms and len(rooms)>=MAX_ROOMS:
                    await send_to(ws,{"type":"error","code":"MAX_ROOMS","message":"Limit pokojow."}); continue
                if room_id: await _leave(room_id,my_uuid)
                if nr not in rooms: rooms[nr]={"_meta":make_meta(pw),"_api":{}}
                meta=rooms[nr]["_meta"]
                if my_uuid in meta.get("banned",set()):
                    await send_to(ws,{"type":"error","code":"BANNED","message":"Zbanowany."}); continue
                if meta["pw_hash"] and pw!=meta["pw_hash"]:
                    await send_to(ws,{"type":"error","code":"WRONG_PASSWORD","message":"Zle haslo."}); continue
                rm=meta.get("max_players",0) or MAX_PLAYERS_PER_ROOM
                if rm>0 and len(get_players(nr))>=rm:
                    await send_to(ws,{"type":"error","code":"ROOM_FULL","message":"Pelny."}); continue
                room_id=nr; clients[ws]["room"]=room_id
                rooms[room_id][my_uuid]={
                    "ws":ws,"x":0,"y":0,"rotation":90,"delete":0,"data":{},
                    "joined_at":asyncio.get_event_loop().time(),
                    "chat_ts":[],"last_seen":time.time(),"last_ping_ms":None,"last_pos_log":0.0
                }
                if meta["host_uuid"] is None or meta["host_uuid"] not in get_players(room_id):
                    meta["host_uuid"]=my_uuid
                count=len(get_players(room_id))
                await send_to(ws,{"type":"welcome","id":"1","room":room_id,"count":count})
                await send_to(ws,{"type":"room_state","players":local_snapshot(room_id,my_uuid)})
                for vu,info in list(get_players(room_id).items()):
                    if vu==my_uuid: continue
                    nl=remap_for(room_id,vu).get(my_uuid)
                    await send_to(info["ws"],{"type":"player_joined","id":nl,"data":{"x":0,"y":0,"rotation":90,"delete":0}})
                await push_server_keys(room_id)
                await hub_send({"type":"update_stats","clients":total_clients()})
                log.info(f"  JOIN [{room_id}] {my_uuid[:8]} ({count})")

            elif t=="position" and room_id and my_uuid in rooms.get(room_id,{}):
                x=msg.get("x",0); y=msg.get("y",0)
                rooms[room_id][my_uuid]["x"]=x; rooms[room_id][my_uuid]["y"]=y
                await broadcast_delta(room_id,my_uuid,{"type":"position","x":x,"y":y})
                if LOG_POSITIONS:
                    now=time.time(); p=rooms[room_id][my_uuid]
                    if now-p["last_pos_log"]>=POS_LOG_INTERVAL:
                        log.info(f"  pos [{room_id}] {get_nick(room_id,my_uuid)}: x={x} y={y}"); p["last_pos_log"]=now

            elif t=="state" and room_id and my_uuid in rooms.get(room_id,{}):
                x=msg.get("x",0); y=msg.get("y",0); rot=msg.get("rotation",90)
                rooms[room_id][my_uuid].update({"x":x,"y":y,"rotation":rot})
                await broadcast_delta(room_id,my_uuid,{"type":"state","x":x,"y":y,"rotation":rot})

            elif t=="data" and room_id and my_uuid in rooms.get(room_id,{}):
                key=str(msg.get("key","")); value=msg.get("value",0)
                if key in ("rotation","delete","x","y"): rooms[room_id][my_uuid][key]=value
                else: rooms[room_id][my_uuid]["data"][key]=value
                await broadcast_delta(room_id,my_uuid,{"type":"data","key":key,"value":value})
                if key=="nick": await push_server_keys(room_id)

            elif t=="api" and room_id and my_uuid in rooms.get(room_id,{}):
                key=str(msg.get("key","")).strip()[:64]; value=msg.get("value",0)
                if not key: continue
                rooms[room_id]["_api"][key]=value
                await push_api_key(room_id,key,value)

            elif t=="api_reset" and room_id and my_uuid in rooms.get(room_id,{}):
                key=str(msg.get("key","")).strip()
                if key:
                    rooms[room_id]["_api"].pop(key,None); await push_api_key(room_id,key,0)
                else:
                    keys=list(rooms[room_id]["_api"].keys()); rooms[room_id]["_api"].clear()
                    for k in keys: await push_api_key(room_id,k,0)

            elif t=="chat" and room_id and my_uuid in rooms.get(room_id,{}):
                pl=rooms[room_id][my_uuid]; now=asyncio.get_event_loop().time()
                ts=pl["chat_ts"]; ts[:]=[x for x in ts if now-x<CHAT_RATE_WINDOW]
                if len(ts)>=CHAT_RATE_LIMIT:
                    await send_to(ws,{"type":"error","code":"RATE_LIMIT","message":"Za szybko."}); continue
                ts.append(now)
                text=str(msg.get("text",""))[:CHAT_MAX_LEN]; nick=get_nick(room_id,my_uuid)
                my_lid=remap_for(room_id,my_uuid).get(my_uuid,"?")
                rooms[room_id]["_meta"]["chat_last"]=f"[{nick}]: {text}"
                rooms[room_id]["_meta"]["chat_from"]=nick
                rooms[room_id]["_meta"]["chat_history"].append({"from":nick,"text":text})
                log.info(f"  CHAT [{room_id}] {nick}: {text}")
                for uid,p in list(get_players(room_id).items()):
                    await send_to(p["ws"],{"type":"message","from":my_lid,"nick":nick,"text":text})
                await push_server_keys(room_id)

            elif t=="ping" and room_id and my_uuid in rooms.get(room_id,{}):
                cts=msg.get("ts",0)
                if cts:
                    em=round(time.time()*1000-cts)
                    if 0<em<30000:
                        rooms[room_id][my_uuid]["last_ping_ms"]=em
                        await send_to(ws,{"type":"data","id":"server","key":"ping_ms","value":em})
                await send_to(ws,{"type":"pong","ts":msg.get("ts",0)})

            elif t=="map" and room_id and my_uuid in rooms.get(room_id,{}):
                if not is_host(room_id,my_uuid):
                    await send_to(ws,{"type":"error","code":"FORBIDDEN","message":"Tylko host."}); continue
                rooms[room_id]["_meta"]["map_id"]=str(msg.get("id","")).strip()[:64]
                await push_server_keys(room_id)

            elif t=="status" and room_id and my_uuid in rooms.get(room_id,{}):
                if not is_host(room_id,my_uuid):
                    await send_to(ws,{"type":"error","code":"FORBIDDEN","message":"Tylko host."}); continue
                rooms[room_id]["_meta"]["status"]=str(msg.get("value","waiting"))[:32]
                await push_server_keys(room_id)

            elif t=="kick" and room_id and my_uuid in rooms.get(room_id,{}):
                if not is_host(room_id,my_uuid):
                    await send_to(ws,{"type":"error","code":"FORBIDDEN","message":"Tylko host."}); continue
                tu=lid_to_uuid(room_id,my_uuid,str(msg.get("target","")))
                if not tu or tu==my_uuid: continue
                if msg.get("ban"): rooms[room_id]["_meta"]["banned"].add(tu)
                tws=rooms[room_id][tu]["ws"]
                await send_to(tws,{"type":"error","code":"KICKED","message":"Wyrzucony."})
                await tws.close()

            # ═══════════════════════════════════════════
            # ADMIN COMMANDS [v7.3] — z Hub/panel
            # admin_kick, admin_ban, admin_map, admin_status,
            # admin_broadcast, admin_create_room, admin_delete_room,
            # admin_reset_walls, room_list
            # ═══════════════════════════════════════════
            elif t=="admin_kick":
                rid    = str(msg.get("room",""))
                target = str(msg.get("nick",""))
                ban    = bool(msg.get("ban",False))
                if rid in rooms:
                    for uid,p in list(get_players(rid).items()):
                        if get_nick(rid,uid)==target:
                            if ban: rooms[rid]["_meta"]["banned"].add(uid)
                            await send_to(p["ws"],{"type":"error","code":"KICKED" if not ban else "BANNED",
                                "message":"Wyrzucony przez admina." if not ban else "Zbanowany przez admina."})
                            await p["ws"].close()
                            action="BAN" if ban else "KICK"
                            log.info(f"  ADMIN {action} [{rid}] '{target}'")
                            await hub_send({"type":"admin_log","action":action,"room":rid,"target":target,
                                           "time":__import__("time").strftime("%H:%M:%S")})
                            break

            elif t=="admin_map":
                rid    = str(msg.get("room",""))
                map_id = str(msg.get("map_id","")).strip()[:64]
                if rid in rooms:
                    rooms[rid]["_meta"]["map_id"] = map_id
                    await push_server_keys(rid)
                    log.info(f"  ADMIN MAP [{rid}] map_id='{map_id}'")
                    await hub_send({"type":"admin_log","action":"MAP","room":rid,"map_id":map_id,
                                   "time":__import__("time").strftime("%H:%M:%S")})

            elif t=="admin_status":
                rid    = str(msg.get("room",""))
                status = str(msg.get("status","waiting"))[:32]
                if rid in rooms:
                    rooms[rid]["_meta"]["status"] = status
                    await push_server_keys(rid)
                    log.info(f"  ADMIN STATUS [{rid}] '{status}'")
                    await hub_send({"type":"admin_log","action":"STATUS","room":rid,"status":status,
                                   "time":__import__("time").strftime("%H:%M:%S")})

            elif t=="admin_broadcast":
                rid  = str(msg.get("room",""))
                text = str(msg.get("text",""))[:256]
                if rid in rooms:
                    for p in list(get_players(rid).values()):
                        await send_to(p["ws"],{"type":"message","from":"admin","nick":"[ADMIN]","text":text})
                    log.info(f"  ADMIN BROADCAST [{rid}] '{text}'")
                    await hub_send({"type":"admin_log","action":"BROADCAST","room":rid,"text":text,
                                   "time":__import__("time").strftime("%H:%M:%S")})

            elif t=="admin_create_room":
                rid = str(msg.get("room","")).strip()[:64]
                pw  = str(msg.get("pw",""))
                mid = str(msg.get("map_id",""))
                if rid and rid not in rooms:
                    rooms[rid]={"_meta":make_meta(pw),"_api":{}}
                    if mid: rooms[rid]["_meta"]["map_id"]=mid
                    log.info(f"  ADMIN CREATE_ROOM '{rid}'")
                    await hub_send({"type":"admin_log","action":"CREATE_ROOM","room":rid,
                                   "time":__import__("time").strftime("%H:%M:%S")})
                    await hub_send({"type":"update_stats","clients":total_clients(),
                                   "meta":{"rooms":list(rooms.keys())}})

            elif t=="admin_delete_room":
                rid = str(msg.get("room",""))
                if rid in rooms:
                    # Wyrzuć wszystkich graczy
                    for p in list(get_players(rid).values()):
                        await send_to(p["ws"],{"type":"error","code":"ROOM_DELETED",
                            "message":"Pokój został usunięty przez admina."})
                        try: await p["ws"].close()
                        except: pass
                    del rooms[rid]
                    log.info(f"  ADMIN DELETE_ROOM '{rid}'")
                    await hub_send({"type":"admin_log","action":"DELETE_ROOM","room":rid,
                                   "time":__import__("time").strftime("%H:%M:%S")})
                    await hub_send({"type":"update_stats","clients":total_clients(),
                                   "meta":{"rooms":list(rooms.keys())}})

            elif t=="admin_reset_walls":
                rid = str(msg.get("room",""))
                if rid in rooms:
                    rooms[rid]["_meta"]["walls"]={}
                    for p in list(get_players(rid).values()):
                        await send_to(p["ws"],{"type":"admin_walls_reset"})
                    log.info(f"  ADMIN RESET_WALLS [{rid}]")
                    await hub_send({"type":"admin_log","action":"RESET_WALLS","room":rid,
                                   "time":__import__("time").strftime("%H:%M:%S")})

            elif t=="room_list":
                # Pełna lista pokoi dla panelu
                room_data={}
                for rid,rdata in rooms.items():
                    meta=rdata["_meta"]; pl=get_players(rid)
                    room_data[rid]={
                        "player_count":len(pl),
                        "map_id":meta.get("map_id",""),
                        "status":meta.get("status","waiting"),
                        "has_password":bool(meta.get("pw_hash","")),
                        "players":[{"nick":get_nick(rid,u),"id":str(i+1),
                                    "x":p.get("x",0),"y":p.get("y",0),
                                    "ping_ms":p.get("last_ping_ms")}
                                   for i,(u,p) in enumerate(pl.items())],
                    }
                await send_to(ws,{"type":"room_list","rooms":room_data,
                                  "uptime_s":int(time.time()-START)})

            elif t=="stats":
                scope=msg.get("scope","room")
                if scope=="server":
                    await send_to(ws,{"type":"stats","scope":"server","version":"7.1",
                        "uptime_s":int(time.time()-START),"rooms":len(rooms),"players":total_clients()})
                elif scope=="room" and room_id and my_uuid in rooms.get(room_id,{}):
                    meta=rooms[room_id]["_meta"]; pl=get_players(room_id)
                    mapping=remap_for(room_id,my_uuid); now=time.time()
                    plist=[{"id":mapping.get(u,"?"),"nick":get_nick(room_id,u),
                            "x":p.get("x",0),"y":p.get("y",0),
                            "is_host":meta.get("host_uuid")==u,
                            "ping_ms":p.get("last_ping_ms"),
                            "idle_s":round(now-p.get("last_seen",now),1)} for u,p in pl.items()]
                    await send_to(ws,{"type":"stats","scope":"room","room":room_id,
                        "status":meta.get("status","waiting"),"map_id":meta.get("map_id",""),
                        "player_count":len(pl),"players":plist,"api":dict(rooms[room_id].get("_api",{}))})

    except websockets.exceptions.ConnectionClosed: pass
    except Exception as e: log.error(f"  Blad: {e}",exc_info=True)
    finally:
        if room_id: await _leave(room_id,my_uuid)
        clients.pop(ws,None)
        await hub_send({"type":"update_stats","clients":total_clients()})
        log.info(f"- {ws.remote_address}")

async def _leave(rid,my_uuid):
    if rid not in rooms or my_uuid not in rooms.get(rid,{}): return
    left_lids={vu:remap_for(rid,vu).get(my_uuid) for vu in list(get_players(rid).keys()) if vu!=my_uuid}
    nick=get_nick(rid,my_uuid); del rooms[rid][my_uuid]; count=len(get_players(rid))
    log.info(f"  LEAVE [{rid}] {nick} ({count})")
    if count==0: del rooms[rid]; log.info(f"  Pokoj '{rid}' usuniety"); return
    meta=rooms[rid]["_meta"]
    if meta.get("host_uuid")==my_uuid:
        pl=get_players(rid); nh=min(pl,key=lambda u:pl[u]["joined_at"]); meta["host_uuid"]=nh
        await send_to(pl[nh]["ws"],{"type":"data","id":"server","key":"you_are_host","value":"1"})
    for vu,info in list(get_players(rid).items()):
        lid=left_lids.get(vu)
        if lid: await send_to(info["ws"],{"type":"player_left","id":lid})
    await push_server_keys(rid)

async def stats_loop():
    while True:
        await asyncio.sleep(30)
        log.info(f"  Pokoje:{len(rooms)} Graczy:{total_clients()} Uptime:{int(time.time()-START)}s")
        await hub_send({"type":"update_stats","clients":total_clients()})

async def main():
    log.info("="*46)
    log.info("  Multiplayer Server  v7.1")
    log.info(f"  ws://{HOST}:{PORT}  Hub: {HUB_URL}")
    log.info("="*46)
    async with websockets.serve(handle_client,HOST,PORT):
        await asyncio.gather(asyncio.Future(), stats_loop(), hub_connect())

if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("MP zatrzymany.")
