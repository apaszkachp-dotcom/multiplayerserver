#!/usr/bin/env python3
"""
=============================================================
  Launcher — Scratch Multiplayer Servers
  Uruchomienie: python start.py
  Automatycznie wykrywa serwery w podfolderach.
=============================================================
"""
import subprocess, sys, os, time, signal
from pathlib import Path

try:
    import colorama; colorama.init(autoreset=True)
    R='\033[91m'; G='\033[92m'; Y='\033[93m'; B='\033[94m'
    M='\033[95m'; C='\033[96m'; W='\033[97m'; DIM='\033[2m'; RST='\033[0m'
except ImportError:
    R=G=Y=B=M=C=W=DIM=RST=''

SERVERS = [
    {"id":"hub",   "name":"Hub Server",        "file":"hub_server.py",         "port":1000, "color":M},
    {"id":"mp",    "name":"Multiplayer Server", "file":"multiplayer_server.py", "port":8765, "color":B},
    {"id":"api",   "name":"API Server",         "file":"api_server.py",         "port":8766, "color":C},
    {"id":"wall",  "name":"Wall Server",        "file":"wall_server.py",        "port":8767, "color":Y},
    {"id":"login", "name":"Login Server",       "file":"login_server.py",       "port":8768, "color":G},
]

BASE_DIR   = Path(__file__).resolve().parent
procs      = {}
# Tryb konsoli: "window" = osobne okno cmd, "file" = log do pliku
CONSOLE_MODE = "window"

# ── Auto-wykrywanie ────────────────────────────────────────
def find_all(filename):
    """Zwraca listę WSZYSTKICH miejsc gdzie znaleziono plik."""
    return list(BASE_DIR.rglob(filename))

# Wybrane ścieżki przez użytkownika: filename → Path
_chosen = {}

def find_file(filename):
    """Zwraca wybraną ścieżkę (lub pierwszą znalezioną)."""
    if filename in _chosen:
        return _chosen[filename]
    matches = find_all(filename)
    if not matches:
        return BASE_DIR / filename
    return matches[0]

def pick_file(filename):
    """Pokazuje menu wyboru gdy znaleziono wiele kopii pliku."""
    matches = find_all(filename)
    if not matches:
        print(f"  {R}Nie znaleziono: {filename}{RST}")
        return None
    if len(matches) == 1:
        _chosen[filename] = matches[0]
        return matches[0]
    cls()
    print(f"\n{W}  Znaleziono {len(matches)} kopii: {filename}{RST}\n")
    for i, p in enumerate(matches, 1):
        try:    rel = p.relative_to(BASE_DIR)
        except: rel = p
        print(f"  [{i}] {rel}")
    try:
        choice = input(f"\n  Wybierz [1-{len(matches)}] (Enter = pierwsza): ").strip()
        if choice:
            idx = int(choice) - 1
            if 0 <= idx < len(matches):
                _chosen[filename] = matches[idx]
                return matches[idx]
    except (ValueError, IndexError):
        pass
    _chosen[filename] = matches[0]
    return matches[0]

# ── Start/Stop ─────────────────────────────────────────────
def is_running(sid):
    p = procs.get(sid)
    return p is not None and p.poll() is None

def status_icon(sid):
    return f"{G}●{RST}" if is_running(sid) else f"{R}○{RST}"

def start_server(srv):
    sid  = srv["id"]
    # Jeśli jeszcze nie wybrano — sprawdź czy jest wiele kopii
    if srv["file"] not in _chosen:
        pick_file(srv["file"])
    path = find_file(srv["file"])
    if not path.exists():
        print(f"  {R}✗ Nie znaleziono: {srv['file']}{RST}")
        return False
    if is_running(sid):
        print(f"  {Y}⚠ {srv['name']} już działa.{RST}")
        return False

    try:
        if CONSOLE_MODE == "window":
            # Osobne okno terminala (Windows: cmd, Linux/Mac: nowe okno)
            if os.name == "nt":
                proc = subprocess.Popen(
                    f'start "{srv["name"]}" cmd /k python "{path}"',
                    shell=True, cwd=str(path.parent)
                )
            elif sys.platform == "darwin":
                script = f'tell application "Terminal" to do script "python3 \\"{path}\\""'
                proc = subprocess.Popen(["osascript", "-e", script])
            else:
                # Linux — próbuje różnych terminali
                term = None
                for t in ["gnome-terminal","xterm","konsole","xfce4-terminal"]:
                    if subprocess.run(["which",t], capture_output=True).returncode == 0:
                        term = t; break
                if term == "gnome-terminal":
                    proc = subprocess.Popen([term,"--",sys.executable,str(path)], cwd=str(path.parent))
                elif term:
                    proc = subprocess.Popen([term,"-e",f"{sys.executable} {path}"], cwd=str(path.parent))
                else:
                    print(f"  {Y}Brak terminala graficznego — przełączam na tryb plik.{RST}")
                    return _start_file_mode(srv, path, sid)
        else:
            return _start_file_mode(srv, path, sid)

        procs[sid] = proc
        time.sleep(0.5)
        print(f"  {G}▶ {srv['name']}{RST}  {DIM}port {srv['port']}  [{path.parent.name}]{RST}")
        return True

    except Exception as e:
        print(f"  {R}✗ Błąd: {e}{RST}")
        return False

def _start_file_mode(srv, path, sid):
    log_path = path.parent / f"{sid}.log"
    proc = subprocess.Popen(
        [sys.executable, str(path)],
        cwd=str(path.parent),
        stdout=open(log_path, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )
    procs[sid] = proc
    time.sleep(0.4)
    if proc.poll() is None:
        print(f"  {G}▶ {srv['name']}{RST}  {DIM}port {srv['port']}  log→{log_path.name}{RST}")
        return True
    else:
        print(f"  {R}✗ {srv['name']} zakończył się — sprawdź {log_path}{RST}")
        return False

def stop_server(srv):
    sid  = srv["id"]
    proc = procs.get(sid)
    if not proc or proc.poll() is not None:
        print(f"  {Y}⚠ {srv['name']} nie działa.{RST}")
        procs.pop(sid, None); return
    try:
        proc.terminate()
        try: proc.wait(timeout=4)
        except subprocess.TimeoutExpired: proc.kill()
        procs.pop(sid, None)
        print(f"  {R}■ {srv['name']} zatrzymany.{RST}")
    except Exception as e:
        print(f"  {R}✗ {e}{RST}")

def restart_server(srv):
    print(f"  {Y}↺ Restart: {srv['name']}...{RST}")
    stop_server(srv); time.sleep(0.5); start_server(srv)

def stop_all():
    print(f"\n  {R}Zatrzymuję wszystkie...{RST}")
    for srv in reversed(SERVERS):
        if is_running(srv["id"]): stop_server(srv)

def start_all():
    print(f"\n  {G}Uruchamiam serwery...{RST}")
    hub = next(s for s in SERVERS if s["id"]=="hub")
    if not is_running("hub"):
        start_server(hub); time.sleep(0.8)
    for srv in SERVERS:
        if srv["id"]!="hub" and not is_running(srv["id"]):
            start_server(srv); time.sleep(0.2)

def restart_all():
    stop_all(); time.sleep(1); start_all()

# ── Widok ──────────────────────────────────────────────────
def draw():
    cls()
    mode_str = f"{G}osobne okno{RST}" if CONSOLE_MODE=="window" else f"{C}log do pliku{RST}"
    print(f"\n{W}{'═'*62}{RST}")
    print(f"{W}  ⬡  Scratch Multiplayer — Launcher{RST}  {DIM}tryb: {RST}{mode_str}")
    print(f"{DIM}  {BASE_DIR}{RST}")
    print(f"{W}{'═'*62}{RST}")

    print(f"\n  {'ID':<8}{'Serwer':<24}{'Port':<7}{'Folder':<18}Status")
    print(f"  {DIM}{'─'*64}{RST}")
    for srv in SERVERS:
        path  = find_file(srv["file"])
        found = path.exists()
        state = f"{G}DZIAŁA{RST}" if is_running(srv["id"]) else (f"{DIM}offline{RST}" if found else f"{R}BRAK PLIKU{RST}")
        loc   = path.parent.name if found else "?"
        print(f"  {srv['color']}{srv['id']:<8}{RST}{srv['name']:<24}:{srv['port']:<6}{DIM}{loc:<18}{RST}{status_icon(srv['id'])} {state}")

    print(f"\n  {G}[1]{RST} Start wszystkich  {R}[2]{RST} Stop wszystkich  {Y}[3]{RST} Restart wszystkich")
    line = ""
    for i,srv in enumerate(SERVERS):
        icon = "▶" if is_running(srv["id"]) else "○"
        line += f"  {srv['color']}[{i+4}]{RST} {icon} {srv['name']:<22}"
        if (i+1)%3==0: line+="\n"
    print(line)

    # Pokaż ostrzeżenie jeśli jakiś plik ma wiele kopii
    dupes = [s for s in SERVERS if len(find_all(s["file"])) > 1]
    if dupes:
        names = ", ".join(s["id"] for s in dupes)
        print(f"  {Y}⚠ Wiele kopii: {names} — użyj [s] aby wybrać wersję{RST}")
    print(f"  {DIM}[l]{RST} Logi  {DIM}[s]{RST} Skanuj/wybierz  {DIM}[m]{RST} Tryb konsoli ({mode_str}{DIM}){RST}  {DIM}[q]{RST} Wyjście")
    print(f"\n  {DIM}{'─'*62}{RST}")

def cls(): os.system("cls" if os.name=="nt" else "clear")

def show_scan():
    cls()
    print(f"\n{W}  Skanowanie podfolderów:{RST}  {DIM}{BASE_DIR}{RST}\n")
    for srv in SERVERS:
        matches = find_all(srv["file"])
        chosen  = _chosen.get(srv["file"])
        if not matches:
            print(f"  {R}✗{RST}  {srv['file']:<36} {R}nie znaleziono{RST}")
        elif len(matches) == 1:
            try:    rel = matches[0].relative_to(BASE_DIR)
            except: rel = matches[0]
            print(f"  {G}✓{RST}  {srv['file']:<36} {DIM}{rel}{RST}")
        else:
            try:    rel = (chosen or matches[0]).relative_to(BASE_DIR)
            except: rel = chosen or matches[0]
            print(f"  {Y}✓{RST}  {srv['file']:<36} {Y}{len(matches)} kopii{RST}  używam: {DIM}{rel}{RST}")
    print(f"\n  {DIM}[p]{RST} Wybierz wersje plików  {DIM}[Enter]{RST} Powrót")
    ch = input("  Wybór: ").strip().lower()
    if ch == "p":
        show_pick_menu()

def show_pick_menu():
    """Pozwala wybrać wersję każdego pliku który ma wiele kopii."""
    for srv in SERVERS:
        matches = find_all(srv["file"])
        if len(matches) > 1:
            pick_file(srv["file"])
            chosen = _chosen.get(srv["file"])
            if chosen:
                try:    rel = chosen.relative_to(BASE_DIR)
                except: rel = chosen
                print(f"  {G}✓ {srv['file']} → {rel}{RST}")
    input(f"  {DIM}Enter aby kontynuować...{RST}")

def show_log_menu():
    cls()
    print(f"\n{W}  Wybierz serwer:{RST}\n")
    for i,srv in enumerate(SERVERS,1):
        path     = find_file(srv["file"])
        log_path = path.parent / f"{srv['id']}.log"
        ex = f"{G}✓{RST}" if log_path.exists() else f"{R}✗{RST}"
        print(f"  [{i}] {srv['name']:<24} {ex} {DIM}{srv['id']}.log{RST}")
    print(f"  [0] Powrót")
    choice = input(f"\n  Wybór: ").strip()
    if choice=="0": return
    try:
        srv      = SERVERS[int(choice)-1]
        path     = find_file(srv["file"])
        log_path = path.parent / f"{srv['id']}.log"
        if not log_path.exists():
            print(f"  {Y}Brak logu.{RST}"); input("  Enter..."); return
        print(f"\n  {W}─── {log_path} ───{RST}\n")
        lines = log_path.read_text(encoding="utf-8",errors="replace").splitlines()
        for line in lines[-40:]:
            col = R if ("ERROR" in line or "Blad" in line) else Y if "WARNING" in line else G if "✓" in line else RST
            print(f"  {col}{line}{RST}")
        print(); input(f"  {DIM}Enter...{RST}")
    except (ValueError,IndexError): pass

def handle_single(idx):
    srv = SERVERS[idx]
    print(f"\n  {srv['color']}── {srv['name']} ──{RST}")
    print(f"  [{G}s{RST}] Start  [{R}x{RST}] Stop  [{Y}r{RST}] Restart")
    ch = input(f"  Wybór: ").strip().lower()
    if   ch=="s": start_server(srv)
    elif ch=="x": stop_server(srv)
    elif ch=="r": restart_server(srv)

def toggle_mode():
    global CONSOLE_MODE
    CONSOLE_MODE = "file" if CONSOLE_MODE=="window" else "window"
    mode_str = "osobne okno cmd" if CONSOLE_MODE=="window" else "log do pliku"
    print(f"\n  {G}Tryb zmieniony na: {mode_str}{RST}")
    time.sleep(0.8)

# ── Main ───────────────────────────────────────────────────
def main():
    def on_exit(sig,frame):
        print(f"\n\n{Y}Zamykam serwery...{RST}")
        stop_all(); print(f"{G}Do widzenia!{RST}\n"); sys.exit(0)
    signal.signal(signal.SIGINT, on_exit)

    while True:
        draw()
        choice = input(f"  Wybór: ").strip().lower()
        if   choice=="1": start_all();      input(f"\n  {DIM}Enter...{RST}")
        elif choice=="2": stop_all();       input(f"\n  {DIM}Enter...{RST}")
        elif choice=="3": restart_all();    input(f"\n  {DIM}Enter...{RST}")
        elif choice=="l": show_log_menu()
        elif choice=="s": show_scan()
        elif choice=="m": toggle_mode()
        elif choice=="q":
            stop_all(); print(f"\n{G}Do widzenia!{RST}\n"); sys.exit(0)
        elif choice.isdigit():
            idx=int(choice)-4
            if 0<=idx<len(SERVERS):
                handle_single(idx); input(f"\n  {DIM}Enter...{RST}")

if __name__=="__main__":
    main()
