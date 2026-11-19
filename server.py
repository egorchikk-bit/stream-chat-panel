"""Панель стрима: общий чат и зрители со всех площадок (Twitch, YouTube, Kick, VK Видео Live).

Запуск: python server.py  ->  http://localhost:8778
Каналы настраиваются прямо в панели (шестерёнка), хранятся в config.json рядом с программой.
Twitch- и Kick-чат страница читает сама через websocket; сервер отдаёт зрителей,
YouTube-чат и чат VK Видео Live.
"""
import argparse
import json
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path

FROZEN = getattr(sys, "frozen", False)
ROOT = Path(sys.executable).parent if FROZEN else Path(__file__).parent
ASSETS = Path(getattr(sys, "_MEIPASS", ROOT))

_ap = argparse.ArgumentParser()
_ap.add_argument("--config", help="путь к config.json")
_ap.add_argument("--port", type=int)
_ap.add_argument("--open", action="store_true", help="открыть окно панели после запуска")
_ap.add_argument("--no-open", action="store_true")
_ap.add_argument("--idle-exit", type=int, help="выключиться через N секунд без открытого окна (у exe по умолчанию 180)")
_args = _ap.parse_args()

if _args.config:
    CFG_PATH = Path(_args.config)
elif FROZEN:                                    # exe: настройки в %APPDATA%\StreamChat
    import os
    _data = Path(os.environ.get("APPDATA") or ROOT) / "StreamChat"
    _data.mkdir(parents=True, exist_ok=True)
    CFG_PATH = _data / "config.json"
else:
    CFG_PATH = ROOT / "config.json"
LAST_HIT = [time.time()]
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")
HDR = {"User-Agent": UA, "Accept-Language": "ru-RU,ru;q=0.9",
       "Cookie": "SOCS=CAI; CONSENT=YES+"}
PLATFORMS = ("twitch", "youtube", "kick", "vk")


def log(*a):
    try:
        print(time.strftime("%H:%M:%S"), *a, flush=True)
    except Exception:  # noqa: BLE001  (в exe без консоли print может падать)
        pass


# ---------------------------------------------------------------- config
def load_cfg():
    try:
        cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        cfg = {}
    if "channels" not in cfg:                       # старый формат: twitch/youtube строками
        ch = []
        if cfg.get("twitch"):
            ch.append({"platform": "twitch", "id": cfg["twitch"]})
        if cfg.get("youtube_video_override"):
            ch.append({"platform": "youtube", "id": "video:" + cfg["youtube_video_override"]})
        elif cfg.get("youtube"):
            ch.append({"platform": "youtube", "id": cfg["youtube"]})
        cfg["channels"] = ch
    cfg["channels"] = [c for c in cfg["channels"]
                       if c.get("platform") in PLATFORMS and str(c.get("id", "")).strip()]
    return cfg


def save_cfg(cfg):
    out = {k: v for k, v in cfg.items() if k not in ("twitch", "youtube", "youtube_video_override")}
    CFG_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=4), encoding="utf-8")


CFG = load_cfg()
PORT = _args.port or CFG.get("port", 8778)


def http(url, data=None, headers=None, timeout=15):
    h = dict(HDR)
    h.update(headers or {})
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        h["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def http_json(url, **kw):
    kw.setdefault("headers", {})["Accept"] = "application/json"
    return json.loads(http(url, **kw))


# ---------------------------------------------------------------- общий поток сообщений
class Bus:
    def __init__(self):
        self.lock = threading.Lock()
        self.messages = []
        self.seq = 0

    def push(self, m):
        with self.lock:
            self.seq += 1
            m["seq"] = self.seq
            m["ts"] = time.time()
            self.messages.append(m)
            del self.messages[:-600]

    def since(self, seq):
        with self.lock:
            return [m for m in self.messages if m["seq"] > seq], self.seq


BUS = Bus()


class Seen:
    """Множество id уже показанных сообщений с ограничением размера."""
    def __init__(self):
        self.s = set()

    def add(self, x):
        if x in self.s:
            return False
        self.s.add(x)
        if len(self.s) > 20000:
            self.s = set(list(self.s)[-5000:])
        return True


# ---------------------------------------------------------------- базовый воркер
class Worker:
    platform = ""
    meta_every = 30

    def __init__(self, cid):
        self.id = cid
        self.key = f"{self.platform}:{cid}"
        self.live = False
        self.viewers = None
        self.title = ""
        self.error = None
        self.stop = threading.Event()
        self.wake = threading.Event()
        self._next_meta = 0
        self._next_chat = 0

    def url(self):
        return ""

    def name(self):
        return self.id

    def extra(self):
        return {}

    def refresh_meta(self):
        pass

    def poll_chat(self):
        """Вернуть паузу до следующего опроса чата в секундах, или None если чат не нужен."""
        return None

    def reset(self):
        self._next_meta = 0
        self._next_chat = 0
        self.error = None
        self.wake.set()

    def emit(self, **m):
        m.update(platform=self.platform, channel=self.key, channel_name=self.name())
        BUS.push(m)

    def run(self):
        while not self.stop.is_set():
            now = time.time()
            try:
                if now >= self._next_meta:
                    self._next_meta = now + self.meta_every
                    self.refresh_meta()
                if self.live and now >= self._next_chat:
                    wait = self.poll_chat()
                    self._next_chat = time.time() + (wait if wait is not None else 3600)
                self.error = None
            except Exception as e:  # noqa: BLE001
                self.error = str(e)[:200]
                log(self.key, "ошибка:", e)
                self.on_error()
                self._next_chat = time.time() + 10
                self._next_meta = min(self._next_meta, time.time() + 15)
            self.wake.wait(0.5)
            self.wake.clear()

    def on_error(self):
        pass

    def state(self):
        return {"key": self.key, "platform": self.platform, "id": self.id, "name": self.name(),
                "url": self.url(), "live": self.live, "viewers": self.viewers,
                "title": self.title, "error": self.error, **self.extra()}


# ---------------------------------------------------------------- Twitch
class Twitch(Worker):
    platform = "twitch"

    def __init__(self, cid):
        super().__init__(cid.lower().lstrip("#"))

    def url(self):
        return f"https://www.twitch.tv/{self.id}"

    def refresh_meta(self):
        # decapi.me — открытый сервис для оверлеев: число зрителей или "... is offline"
        r = http(f"https://decapi.me/twitch/viewercount/{self.id}").strip()
        if r.isdigit():
            self.live, self.viewers = True, int(r)
        elif "offline" in r.lower():
            self.live, self.viewers = False, None
        else:
            raise RuntimeError(r[:120])


# ---------------------------------------------------------------- YouTube
def runs_text(obj):
    if not obj:
        return ""
    if "simpleText" in obj:
        return obj["simpleText"]
    out = []
    for r in obj.get("runs", []):
        if "text" in r:
            out.append(r["text"])
        elif "emoji" in r:
            sc = r["emoji"].get("shortcuts") or []
            if r["emoji"].get("isCustomEmoji"):
                out.append(sc[0] if sc else "")
            else:
                out.append(r["emoji"].get("emojiId", ""))
    return "".join(out)


def find_initial_data(html, name):
    m = re.search(r"(?:var\s+|window\[\")" + name + r"(?:\"\])?\s*=\s*\{", html)
    if not m:
        return None
    obj, _ = json.JSONDecoder().raw_decode(html, m.end() - 1)
    return obj


class YouTube(Worker):
    """id: '@handle', 'channel/UC…', 'c/name' или 'video:ID' (конкретная трансляция)."""
    platform = "youtube"

    def __init__(self, cid):
        super().__init__(cid)
        self.video_id = None
        self.seen = Seen()
        self._chat_cont = None
        self._api_key = None
        self._client_ver = None

    def name(self):
        return self.id[6:] if self.id.startswith("video:") else self.id.replace("channel/", "")

    def url(self):
        if self.id.startswith("video:"):
            return f"https://www.youtube.com/watch?v={self.id[6:]}"
        return f"https://www.youtube.com/{self.id}"

    def extra(self):
        return {"video_id": self.video_id}

    def refresh_meta(self):
        url = self.url() if self.id.startswith("video:") else self.url() + "/live"
        html = http(url)
        player = find_initial_data(html, "ytInitialPlayerResponse") or {}
        vd = player.get("videoDetails") or {}
        vid = vd.get("videoId")
        live = bool(vd.get("isLive"))
        title = vd.get("title", "")
        m = re.search(r'"originalViewCount":"(\d+)"', html)
        viewers = int(m.group(1)) if m else None
        if not vid:
            # С IP VPN/дата-центров YouTube прячет данные плеера («подтвердите, что вы не бот»).
            # ID эфира всё равно есть в ytInitialData, а статус и зрителей отдаёт updated_metadata.
            data = find_initial_data(html, "ytInitialData") or {}
            vid = ((data.get("currentVideoEndpoint") or {}).get("watchEndpoint") or {}).get("videoId")
            if not vid and self.id.startswith("video:"):
                vid = self.id[6:]
            if vid:
                cv = (re.search(r'"clientVersion":"([^"]+)"', html) or [None, "2.20261118.01.00"])[1]
                live, viewers, title = self._updated_metadata(vid, cv)
        if vid != self.video_id:
            self._chat_cont = None
            log(self.key, "трансляция", vid, "live" if live else "не в эфире")
        self.video_id = vid if live else None
        self.live = live
        self.viewers = viewers if live else None
        self.title = title if live else ""

    @staticmethod
    def _updated_metadata(vid, client_ver):
        r = json.loads(http("https://www.youtube.com/youtubei/v1/updated_metadata?prettyPrint=false",
                            {"context": {"client": {"clientName": "WEB", "clientVersion": client_ver, "hl": "ru"}},
                             "videoId": vid}))
        live, viewers, title = False, None, ""
        for a in r.get("actions", []):
            vc = ((a.get("updateViewershipAction") or {}).get("viewCount") or {}).get("videoViewCountRenderer") or {}
            if vc:
                live = bool(vc.get("isLive"))
                n = vc.get("originalViewCount") or ""
                viewers = int(n) if str(n).isdigit() else None
            t = (a.get("updateTitleAction") or {}).get("title")
            if t:
                title = runs_text(t)
        return live, viewers, title

    def _init_chat(self):
        html = http(f"https://www.youtube.com/live_chat?is_popout=1&v={self.video_id}")
        self._api_key = (re.search(r'"INNERTUBE_API_KEY":"([^"]+)"', html) or [None, None])[1]
        self._client_ver = (re.search(r'"clientVersion":"([^"]+)"', html) or [None, "2.20241101.00.00"])[1]
        data = find_initial_data(html, "ytInitialData")
        if not data:
            raise RuntimeError("нет ytInitialData в live_chat")
        lcr = data["contents"]["liveChatRenderer"]
        self._chat_cont = self._cont_from(lcr.get("continuations", []))[0]
        for a in lcr.get("actions", []):           # история — только помечаем увиденной
            it = (a.get("addChatItemAction") or {}).get("item") or {}
            for v in it.values():
                if isinstance(v, dict) and v.get("id"):
                    self.seen.add(v["id"])

    @staticmethod
    def _cont_from(conts):
        for c in conts:
            for k in ("invalidationContinuationData", "timedContinuationData",
                      "reloadContinuationData"):
                if k in c:
                    return c[k]["continuation"], c[k].get("timeoutMs", 3000)
        return None, 5000

    def poll_chat(self):
        if not self.video_id:
            return 10
        if not self._chat_cont:
            self._init_chat()
        r = json.loads(http(
            f"https://www.youtube.com/youtubei/v1/live_chat/get_live_chat?key={self._api_key}&prettyPrint=false",
            {"context": {"client": {"clientName": "WEB", "clientVersion": self._client_ver, "hl": "ru"}},
             "continuation": self._chat_cont}))
        lcc = (r.get("continuationContents") or {}).get("liveChatContinuation")
        if not lcc:
            self._chat_cont = None
            return 5
        cont, timeout = self._cont_from(lcc.get("continuations", []))
        self._chat_cont = cont
        for a in lcc.get("actions", []):
            self._add_item((a.get("addChatItemAction") or {}).get("item") or {})
        return max(1.5, min(int(timeout or 3000), 8000) / 1000)

    KINDS = {
        "liveChatTextMessageRenderer": "msg",
        "liveChatPaidMessageRenderer": "superchat",
        "liveChatPaidStickerRenderer": "sticker",
        "liveChatMembershipItemRenderer": "member",
        "liveChatSponsorshipsGiftPurchaseAnnouncementRenderer": "gift",
    }
    SYS = {"superchat": "Суперчат", "sticker": "Суперстикер", "member": "Спонсорство",
           "gift": "Подарок спонсорств"}

    def _add_item(self, it):
        for key, kind in self.KINDS.items():
            v = it.get(key)
            if not v:
                continue
            mid = v.get("id")
            if not mid or not self.seen.add(mid):
                return
            if kind == "gift":
                hdr = (v.get("header") or {}).get("liveChatSponsorshipsHeaderRenderer") or {}
                author, text = runs_text(hdr.get("authorName")), runs_text(hdr.get("primaryText"))
            else:
                author = runs_text(v.get("authorName"))
                text = runs_text(v.get("message")) or runs_text(v.get("headerSubtext"))
            badges = [((b.get("liveChatAuthorBadgeRenderer") or {}).get("tooltip") or "")
                      for b in v.get("authorBadges", [])]
            self.emit(id=mid, author=author, parts=[{"t": "text", "v": text}],
                      kind=kind, sys=self.SYS.get(kind),
                      amount=runs_text(v.get("purchaseAmountText")) or None, badges=badges)
            return

    def on_error(self):
        self._chat_cont = None

    def reset(self):
        self._chat_cont = None
        super().reset()


# ---------------------------------------------------------------- Kick
KICK_EMOTE = re.compile(r"\[emote:(\d+):([^\]]*)\]")
KICK_BADGES = {"broadcaster": "стример", "moderator": "мод", "vip": "VIP", "subscriber": "саб",
               "founder": "основатель", "og": "OG"}


def kick_parts(text):
    out, pos = [], 0
    for m in KICK_EMOTE.finditer(text or ""):
        if m.start() > pos:
            out.append({"t": "text", "v": text[pos:m.start()]})
        out.append({"t": "img", "src": f"https://files.kick.com/emotes/{m.group(1)}/fullsize", "alt": m.group(2)})
        pos = m.end()
    if pos < len(text or ""):
        out.append({"t": "text", "v": text[pos:]})
    return out


class Kick(Worker):
    """Зрители — kick.com/api/v2/channels, чат — опрос истории web.kick.com (их websocket закрыт для чужих сайтов)."""
    platform = "kick"

    def __init__(self, cid):
        super().__init__(cid.lower())
        self.channel_id = None
        self.display = cid
        self.seen = Seen()
        self.primed = False

    def name(self):
        return self.display

    def url(self):
        return f"https://kick.com/{self.id}"

    def refresh_meta(self):
        d = http_json(f"https://kick.com/api/v2/channels/{urllib.parse.quote(self.id)}")
        self.channel_id = d.get("id")
        self.display = (d.get("user") or {}).get("username") or self.id
        ls = d.get("livestream") or {}
        was = self.live
        self.live = bool(ls and ls.get("is_live", True))
        self.viewers = ls.get("viewer_count") if self.live else None
        self.title = ls.get("session_title", "") if self.live else ""
        if self.live and not was:
            self.primed = False

    def poll_chat(self):
        if not self.channel_id:
            return 10
        d = http_json(f"https://web.kick.com/api/v1/chat/{self.channel_id}/history")
        msgs = list(reversed((d.get("data") or {}).get("messages") or []))
        msgs.sort(key=lambda m: m.get("created_at") or "")
        for m in msgs:
            mid = m.get("id")
            if not mid or not self.seen.add(mid) or not self.primed:
                continue
            s = m.get("sender") or {}
            idt = s.get("identity") or {}
            badges = [KICK_BADGES[b["type"]] for b in idt.get("badges") or []
                      if b.get("type") in KICK_BADGES]
            kind = "msg" if m.get("type") in ("message", "reply", None) else "event"
            self.emit(id=mid, author=s.get("username") or "", color=idt.get("color"),
                      parts=kick_parts(m.get("content") or ""), kind=kind, badges=badges)
        self.primed = True
        return 2

    def reset(self):
        self.primed = False
        super().reset()


# ---------------------------------------------------------------- VK Видео Live
VK_API = "https://api.live.vkvideo.ru/v1/blog/"


def vk_parts(blocks):
    out = []
    for b in blocks or []:
        t = b.get("type")
        if t == "text":
            c = b.get("content") or ""
            try:
                c = json.loads(c)[0] if c.startswith("[") else c
            except Exception:  # noqa: BLE001
                pass
            if c:
                out.append({"t": "text", "v": c})
        elif t == "smile":
            out.append({"t": "img", "src": b.get("mediumUrl") or b.get("smallUrl"), "alt": b.get("name", "")})
        elif t == "mention":
            out.append({"t": "text", "v": "@" + (b.get("displayName") or b.get("nick") or b.get("name") or "")})
        elif t == "link":
            c = b.get("content") or b.get("url") or ""
            try:
                c = json.loads(c)[0] if c.startswith("[") else c
            except Exception:  # noqa: BLE001
                pass
            out.append({"t": "text", "v": c or b.get("url", "")})
    return out


class VK(Worker):
    platform = "vk"
    meta_every = 20

    def __init__(self, cid):
        super().__init__(cid.lower())
        self.seen = Seen()
        self.primed = False

    def url(self):
        return f"https://live.vkvideo.ru/{self.id}"

    def refresh_meta(self):
        d = http_json(VK_API + urllib.parse.quote(self.id) + "/public_video_stream")
        was = self.live
        self.live = bool(d.get("isOnline"))
        self.viewers = (d.get("count") or {}).get("viewers") if self.live else None
        self.title = d.get("title", "") if self.live else ""
        if self.live and not was:
            self.primed = False

    def poll_chat(self):
        d = http_json(VK_API + urllib.parse.quote(self.id) + "/public_video_stream/chat?limit=30")
        items = sorted(d.get("data") or [], key=lambda m: m.get("id", 0))
        for m in items:
            mid = m.get("id")
            if mid is None or not self.seen.add(mid) or not self.primed:
                continue
            if m.get("isDeleted"):
                continue
            a = m.get("author") or {}
            badges = []
            if a.get("isOwner"):
                badges.append("стример")
            elif a.get("isChatModerator") or a.get("isChannelModerator"):
                badges.append("мод")
            self.emit(id=f"vk{mid}", author=a.get("displayName") or a.get("nick") or "",
                      parts=vk_parts(m.get("data")), kind="msg", badges=badges)
        self.primed = True
        return 2

    def reset(self):
        self.primed = False
        super().reset()


# ---------------------------------------------------------------- менеджер каналов
WORKER = {"twitch": Twitch, "youtube": YouTube, "kick": Kick, "vk": VK}


class Manager:
    def __init__(self):
        self.lock = threading.Lock()
        self.workers = []

    def apply(self, channels):
        with self.lock:
            old = {w.key: w for w in self.workers}
            new = []
            for c in channels:
                cls = WORKER[c["platform"]]
                probe = cls(c["id"])
                w = old.pop(probe.key, None)
                if w is None:
                    w = probe
                    threading.Thread(target=w.run, daemon=True).start()
                    log("канал добавлен:", w.key)
                new.append(w)
            for w in old.values():
                w.stop.set()
                w.wake.set()
                log("канал убран:", w.key)
            self.workers = new

    def refresh(self):
        with self.lock:
            for w in self.workers:
                w.reset()
        log("ручное обновление")

    def state(self):
        with self.lock:
            return [w.state() for w in self.workers]


MGR = Manager()


# ---------------------------------------------------------------- HTTP
def index_html():
    for p in (ROOT / "index.html", ASSETS / "index.html"):
        if p.exists():
            return p.read_bytes()
    return b"index.html not found"


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        b = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj, ensure_ascii=False))

    def do_GET(self):
        path, _, qs = self.path.partition("?")
        params = dict(urllib.parse.parse_qsl(qs))
        if path in ("/", "/index.html"):
            return self._send(200, index_html(), "text/html; charset=utf-8")
        if path == "/api/state":
            LAST_HIT[0] = time.time()
            msgs, seq = BUS.since(int(params.get("since") or 0))
            return self._json({"channels": MGR.state(), "messages": msgs, "seq": seq})
        if path == "/api/config":
            return self._json({"channels": CFG["channels"]})
        if path == "/api/refresh":
            MGR.refresh()
            return self._json({"ok": True})
        self._send(404, "not found", "text/plain")

    def do_POST(self):
        if self.path != "/api/config":
            return self._send(404, "not found", "text/plain")
        try:
            n = int(self.headers.get("Content-Length") or 0)
            data = json.loads(self.rfile.read(n) or b"{}")
            chans, keys = [], set()
            for c in data.get("channels", []):
                p, i = c.get("platform"), str(c.get("id", "")).strip()
                if p in PLATFORMS and i and (p, i.lower()) not in keys:
                    keys.add((p, i.lower()))
                    chans.append({"platform": p, "id": i})
            CFG["channels"] = chans
            save_cfg(CFG)
            MGR.apply(chans)
            self._json({"ok": True, "channels": chans})
        except Exception as e:  # noqa: BLE001
            self._json({"ok": False, "error": str(e)}, 400)


# ---------------------------------------------------------------- запуск
def open_window():
    url = f"http://localhost:{PORT}"
    edge = [r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"]
    for e in edge:
        if Path(e).exists():
            subprocess.Popen([e, f"--app={url}", "--new-window"])
            return
    webbrowser.open(url)


def port_busy():
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", PORT)) == 0


if __name__ == "__main__":
    want_open = (_args.open or FROZEN) and not _args.no_open
    if port_busy():                     # панель уже запущена — просто показать окно
        log(f"порт {PORT} уже занят — открываю окно панели")
        if want_open:
            open_window()
        sys.exit(0)
    MGR.apply(CFG["channels"])
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), H)
    log(f"Панель стрима: http://localhost:{PORT}  каналов: {len(CFG['channels'])}")
    if want_open:
        threading.Timer(0.8, open_window).start()
    idle = _args.idle_exit or (180 if FROZEN else 0)
    if idle:
        # у exe нет окна консоли: когда окно панели закрыто N секунд — выключаемся сами
        def idle_exit():
            while True:
                time.sleep(min(15, max(1, idle // 4)))
                if time.time() - LAST_HIT[0] > idle:
                    log("окно панели закрыто — выключаюсь")
                    srv.shutdown()
                    return
        threading.Thread(target=idle_exit, daemon=True).start()
    srv.serve_forever()
    log("выход")
