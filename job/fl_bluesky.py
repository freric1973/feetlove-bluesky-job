#!/usr/bin/env python3
"""Feetlove1973 – Bluesky Auto-Likes, Entfolger-Abgleich und Auto-Folgen als GitHub-Actions-Job.

Portierung von bluesky-autolike.php (Stand AUTOLIKE-500-20261113), dem Abgleich aus
bluesky-unfollowers.php und flb_af_run() aus bluesky-autofollow.php. Gleiche Regeln: echte Frage/Beleidigung -> Prüfung,
KI-Check (xAI, inkl. echte_frage), Like-Limit je Lauf (Setting bluesky_autolike_max, Standard 50), nie doppelt liken
(viewer.like wird vor jedem Like geprüft), geblockte/Zweitkonten/stummgeschaltete überspringen, Testmodus (dryrun) wird respektiert.
Auto-Folgen: Liker eigener Posts der letzten N Tage (Standard 7), max. 10 je Lauf / 100 am Tag, nicht schon gefolgt,
nicht blockiert/stumm, keine Spam-Labels, keine Entfolger-Sperre; bis einschließlich Setting autofollow_reunfollowed_until (Standard 2027-02-14, Europe/Berlin) auch Liker mit reason „von dir entfolgt“; Eintrag in flb_bluesky_autofollow (reason automatisch, followed_at UTC).
Nur-Lesen folgt nicht und schreibt nichts. Blockierte und kurze Störungen sind Hinweise, keine Fehler.
Datenbank-Zugriff nur über die geschützte Schnittstelle bluesky-gh-bridge.php (HMAC).
Nur Python-Standardbibliothek. Geheimnisse ausschließlich aus Umgebungsvariablen (GitHub Secrets).
"""
from __future__ import annotations
import hashlib, hmac, json, os, re, secrets, sys, threading, time, urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, wait as fwait
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ENV = os.environ.get
BRIDGE_URL = ENV("FL_BRIDGE_URL", "")
BRIDGE_SECRET = ENV("FL_BRIDGE_SECRET", "")
BSKY_HANDLE = ENV("BSKY_HANDLE", "")
BSKY_PW = ENV("BSKY_APP_PASSWORD", "")
BSKY_SERVICE = ENV("BSKY_SERVICE", "https://bsky.social").rstrip("/")
RELAY = ENV("BSKY_RELAY", "https://bsky.network").rstrip("/")
XAI_KEY = ENV("XAI_API_KEY", "")
XAI_BASE = ENV("XAI_BASE", "https://api.x.ai").rstrip("/")
READONLY = ENV("FL_READONLY", "0") == "1"          # nur lesen: keine Likes, kein Entfolgen, kein Folgen, nichts in die DB
UF_SCAN_MIN = int(ENV("FL_UF_SCAN_MIN", "240"))     # Follower-Abgleich höchstens alle X Minuten
MAX_RUNTIME = float(ENV("FL_MAX_RUNTIME", "420"))   # Sicherheitsgrenze je Lauf (Sekunden)
SLEEP_LIKE = float(ENV("FL_SLEEP_LIKE", "0.7"))
LIKES_BUDGET = float(ENV("FL_LIKES_BUDGET", "240"))  # Auto-Likes enden spätestens nach X s, wenn der Abgleich fällig ist
FINISH_RESERVE = float(ENV("FL_FINISH_RESERVE", "25"))  # Rest für Abschluss-Meldung an die Seite
AF_BUDGET = float(ENV("FL_AF_BUDGET", "90"))        # festes Zeitfenster für Auto-Folgen am Laufende
SLEEP_AF = float(ENV("FL_SLEEP_AF", "0.15"))        # Pause zwischen getLikes-/Profil-Seiten
SLEEP_FOLLOW = float(ENV("FL_SLEEP_FOLLOW", "1.5"))  # Pause zwischen Follows (wie PHP usleep 1.5s)
AI_WORKERS = max(1, int(ENV("FL_AI_WORKERS", "8")))   # gleichzeitige KI-Prüfungen / Thread-Abrufe
UF_STALE_MIN = int(ENV("FL_UF_STALE_MIN", "720"))   # Abgleich länger als X Min nicht fertig -> echter Fehler
RUN_ID = ENV("GITHUB_RUN_ID", "local") + "-" + ENV("GITHUB_RUN_ATTEMPT", "1") + "-" + secrets.token_hex(4)
T0 = time.monotonic()


_PHASE_END = [MAX_RUNTIME]   # Ende der aktuellen Phase (Sekunden ab Start)


def phase(end_s: float) -> None:
    _PHASE_END[0] = min(end_s, MAX_RUNTIME)


def left() -> float:
    """Restzeit der aktuellen Phase (nie mehr als die Gesamt-Restzeit)."""
    el = time.monotonic() - T0
    return min(MAX_RUNTIME, _PHASE_END[0]) - el


def total_left() -> float:
    return MAX_RUNTIME - (time.monotonic() - T0)


class Deferred(Exception):
    """Phase hatte nicht genug Zeit – kein Fehler, nächster Lauf macht weiter."""


_AI = {"calls": 0, "retries": 0, "secs": 0.0}
_AI_LOCK = threading.Lock()


class RateLimit(Exception):
    pass


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z] {msg}", flush=True)


# ---------------- HTTP ----------------

TRANSIENT_PREFIX = "Bluesky kurz gestört: "   # 5xx/Netz: nur Hinweis, kein Fehler (nächster Lauf holt es nach)


def http(url: str, headers: dict | None = None, body: bytes | None = None, timeout: float = 30, retries: int = 2) -> dict:
    last = None
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=body, headers=headers or {}, method="POST" if body is not None else "GET")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
            d = json.loads(raw or b"{}")
            return d if isinstance(d, dict) else {}
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                d = json.loads(raw or b"{}")
            except ValueError:
                d = {}
            if e.code == 429:
                raise RateLimit()
            msg = str(d.get("message") or d.get("error") or f"HTTP {e.code}") if isinstance(d, dict) else f"HTTP {e.code}"
            last = RuntimeError(f"{TRANSIENT_PREFIX}{msg} (HTTP {e.code})" if e.code >= 500 else msg)
            if e.code < 500 or body is not None:   # Schreibaufrufe nie automatisch wiederholen
                raise last
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = RuntimeError(TRANSIENT_PREFIX + "Bluesky nicht erreichbar.")
            if body is not None:
                raise last
        time.sleep(1.5 * (attempt + 1))
    raise last  # type: ignore[misc]


# ---------------- Schnittstelle (one.com) ----------------

class BridgeUnreachable(RuntimeError):
    pass


_IPV4_ONLY = False


def _force_ipv4() -> None:
    # Fallback: nur noch IPv4-Adressen verwenden (GitHub-Rechner haben teils kein IPv6-Netz)
    global _IPV4_ONLY
    if _IPV4_ONLY:
        return
    _IPV4_ONLY = True
    import socket
    orig = socket.getaddrinfo

    def v4(host, port, family=0, type=0, proto=0, flags=0):
        return orig(host, port, socket.AF_INET, type, proto, flags)
    socket.getaddrinfo = v4  # type: ignore[assignment]


def bridge(action: str, **data) -> dict:
    body = json.dumps({"action": action, "run": RUN_ID, **data}, ensure_ascii=False, separators=(",", ":")).encode()
    waits = [5, 15, 30]
    for attempt in range(len(waits) + 1):
        ts, nonce = str(int(time.time())), secrets.token_urlsafe(18)
        sig = hmac.new(BRIDGE_SECRET.encode(), ts.encode() + b"\n" + nonce.encode() + b"\n" + body, hashlib.sha256).hexdigest()
        hdr = {"Content-Type": "application/json", "X-FL-Time": ts, "X-FL-Nonce": nonce, "X-FL-Sig": sig, "User-Agent": "feetlove-gh-bluesky/1"}
        req = urllib.request.Request(BRIDGE_URL, data=body, headers=hdr, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                d = json.loads(r.read() or b"{}")
            break
        except urllib.error.HTTPError as e:
            try:
                d = json.loads(e.read() or b"{}")
            except ValueError:
                d = {}
            raise RuntimeError(f"Schnittstelle {action}: HTTP {e.code} {d.get('status', '')} {d.get('message', '')}".strip())
        except urllib.error.URLError as e:
            # Verbindung kam gar nicht zustande -> Anfrage hat die Seite nicht erreicht, Wiederholen ist sicher
            if attempt >= len(waits):
                raise BridgeUnreachable(f"Webseite nicht erreichbar ({action}): {e.reason}")
            log(f"Webseite nicht erreichbar ({e.reason}), neuer Versuch in {waits[attempt]} s" + ("" if _IPV4_ONLY else " über IPv4"))
            _force_ipv4()
            time.sleep(waits[attempt])
    if not d.get("ok"):
        raise RuntimeError(f"Schnittstelle {action}: {d.get('status')} {d.get('message', '')}".strip())
    return d


# ---------------- Bluesky ----------------

class Bsky:
    def __init__(self):
        d = http(BSKY_SERVICE + "/xrpc/com.atproto.server.createSession", {"Content-Type": "application/json"},
                 json.dumps({"identifier": BSKY_HANDLE, "password": BSKY_PW}).encode(), retries=0)
        if not d.get("accessJwt") or not d.get("did"):
            raise RuntimeError("Keine Bluesky-Sitzung.")
        self.jwt, self.did = d["accessJwt"], d["did"]

    def get(self, nsid: str, query) -> dict:
        qs = query if isinstance(query, str) else urllib.parse.urlencode(query, doseq=True)
        return http(f"{BSKY_SERVICE}/xrpc/{nsid}?{qs}", {"Authorization": "Bearer " + self.jwt})

    def post(self, nsid: str, payload: dict) -> dict:
        return http(f"{BSKY_SERVICE}/xrpc/{nsid}", {"Content-Type": "application/json", "Authorization": "Bearer " + self.jwt},
                    json.dumps(payload, separators=(",", ":")).encode(), retries=0)

    def like(self, uri: str, cid: str) -> str:
        if READONLY:
            raise RuntimeError("READONLY")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"
        d = self.post("com.atproto.repo.createRecord", {"repo": self.did, "collection": "app.bsky.feed.like",
                      "record": {"$type": "app.bsky.feed.like", "subject": {"uri": uri, "cid": cid}, "createdAt": now}})
        return str(d.get("uri", ""))

    def get_posts(self, uris: list[str]) -> dict:
        out, uris = {}, list(dict.fromkeys(u for u in uris if isinstance(u, str) and u))
        for i in range(0, len(uris), 25):
            d = self.get("app.bsky.feed.getPosts", "&".join("uris=" + urllib.parse.quote(u, safe="") for u in uris[i:i + 25]))
            for p in d.get("posts", []) or []:
                if p.get("uri"):
                    out[p["uri"]] = p
        return out

    def already_liked(self, uri: str) -> bool:
        p = self.get_posts([uri]).get(uri)
        return bool(p and (p.get("viewer") or {}).get("like"))


# ---------------- Regeln (1:1 aus bluesky-autolike.php) ----------------
_Q_PREFIX = re.compile(r"(?:^|[.!]\s+)(kannst du|könntest du|würdest du|hast du|hattest du|bist du|warst du|weißt du|weisst du|meinst du|machst du|gibst du|darf ich|kann ich|wie machst du|was meinst du|was denkst du|wo gibt es|can you|could you|would you|do you|did you|are you|have you|will you|what do you|how do you|where can i)\b")
_Q_MARK = re.compile(r"[^\W\d_]{2,}[^?？]*[?？]")  # PHP: \p{L}{2,}[^?？]*[?？]
_INS_WORDS = re.compile(r"\b(arschloch|wichser|hurensohn|missgeburt|spast|spasti|vollidiot|vollpfosten|drecksack|dreckskerl|penner|schwuchtel|perversling|kinderschänder|pädo|asozialer|abschaum|bastard|fotze|schlampe|motherfucker|asshole|dickhead|cunt|faggot|retard|scumbag|pervert)\b")
_INS_DIRECT = re.compile(r"(du\s+(bist|bist\s+echt|bist\s+so)\s+(ein[e]?\s+)?(dumm|blöd|krank|widerlich|ekelhaft|eklig|lächerlich|erbärmlich|peinlich|hässlich|psycho|gestört|behindert|pervers|abartig)|halt\s+(die|deine)\s+(fresse|klappe|maul)|verpiss\s+dich|fick\s+dich|geh\s+sterben|bring\s+dich\s+um|schäm\s+dich|du\s+ekel|you\s+are\s+(so\s+)?(stupid|disgusting|pathetic|sick|gross|creepy|ugly|an?\s+idiot)|you'?re\s+(so\s+)?(stupid|disgusting|pathetic|sick|gross|creepy|ugly|an?\s+idiot)|fuck\s+(you|off)|kill\s+yourself|\bkys\b|go\s+die)")


def is_question(t: str) -> bool:
    t = t.strip().lower()
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"(?:^|\s)[@#]\S+", " ", t).strip()
    if not t:
        return False
    return bool(_Q_MARK.search(t) or _Q_PREFIX.search(t))


def insult_hit(t: str) -> str:
    t = t.lower()
    m = _INS_WORDS.search(t)
    if m:
        return m.group(1)
    m = _INS_DIRECT.search(t)
    return m.group(1).strip() if m else ""


XAI_MODEL = "grok-4.3"
SYSTEM_PROMPT = ('Du prüfst Antworten auf Social-Media-Posts des Bloggers Dennis. negative=true NUR, wenn die Antwort Dennis PERSÖNLICH beleidigt, beschimpft, verhöhnt, bedroht oder belästigt (Beleidigungen, Schimpfwörter gegen ihn, Hass, abwertende Aussagen über seine Person, sexuelle Belästigung, Drohungen). negative=false für alles andere, insbesondere: Kritik an einem Thema, an Firmen (z. B. Google), Produkten, Politik oder Meinungen; Widerspruch in der Sache; Sarkasmus oder Spott über das Thema; Witze, Wortspiele, Insider, Emojis; kurze oder rätselhafte Antworten (z. B. "Hex, Hex"); Zustimmung, Flirt, Neutrales. Im Zweifel negative=false. echte_frage=true NUR, wenn der Antwortende von Dennis wirklich eine Antwort erwartet (eine echte, an ihn gerichtete Frage). echte_frage=false für Witze, Sprüche, rhetorische oder ausrufende Fragen und Scherze mit Fragezeichen (z. B. „… Bluna!?“, „Wieso Der? 🤭“, „Bin ich etwa schon drin oder was?“). Im Zweifel, ob wirklich eine Antwort erwartet wird: echte_frage=true. reason: max. 8 Wörter, deutsch.')


def ai_check(text: str, parent: str, deadline: float | None = None) -> tuple[bool, bool, str]:
    if not XAI_KEY:
        raise RuntimeError("xAI-Key fehlt")
    schema = {"type": "object", "properties": {"negative": {"type": "boolean"}, "echte_frage": {"type": "boolean"}, "reason": {"type": "string"}}, "required": ["negative", "echte_frage", "reason"], "additionalProperties": False}
    payload = {"model": XAI_MODEL, "temperature": 0, "max_tokens": 300, "messages": [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": "Ursprungspost (Kontext):\n" + parent[:500] + "\n\nAntwort:\n" + text[:1000]}],
        "response_format": {"type": "json_schema", "json_schema": {"name": "reply_check", "strict": True, "schema": schema}}}
    body = json.dumps(payload, ensure_ascii=False).encode()
    t_end = time.monotonic() + (deadline if deadline is not None else 90)
    d: dict = {}
    for attempt in range(4):
        req = urllib.request.Request(XAI_BASE + "/v1/chat/completions", data=body,
                                     headers={"Content-Type": "application/json", "Authorization": "Bearer " + XAI_KEY}, method="POST")
        t1 = time.monotonic(); wait_s = 0.0
        try:
            with urllib.request.urlopen(req, timeout=max(3.0, min(30.0, t_end - t1))) as r:
                d = json.loads(r.read() or b"{}")
            break
        except urllib.error.HTTPError as e:
            raw = e.read().decode(errors="replace")
            if e.code == 403 and "credits" in raw.lower():
                raise RuntimeError("xAI HTTP 403 (Guthaben/Ausgabelimit erreicht)")
            fail = RuntimeError(f"xAI HTTP {e.code}")
            if e.code != 429 and e.code < 500:
                raise fail
            try:
                wait_s = float(e.headers.get("Retry-After") or 0)
            except (TypeError, ValueError):
                wait_s = 0.0
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            fail = RuntimeError("xAI HTTP 0")
        finally:
            with _AI_LOCK:
                _AI["calls"] += 1; _AI["secs"] += time.monotonic() - t1
        wait_s = min(max(wait_s, 1.5 * (2 ** attempt)), 20.0)
        if attempt == 3 or time.monotonic() + wait_s + 5 > t_end:
            raise fail
        with _AI_LOCK:
            _AI["retries"] += 1
        time.sleep(wait_s)
    ch = (d.get("choices") or [{}])[0]
    try:
        r = json.loads((ch.get("message") or {}).get("content") or "")
    except ValueError:
        r = None
    if not isinstance(r, dict) or "negative" not in r or "echte_frage" not in r:
        raise RuntimeError(f"KI-Antwort unklar ({ch.get('finish_reason') or '-'})")
    return bool(r["negative"]), bool(r["echte_frage"]), str(r.get("reason", "")).strip()[:120]


def needs_ai(text: str) -> bool:
    # Fragen laufen jetzt durch die KI (echte_frage). Nur klare Beleidigungen brauchen keinen KI-Aufruf.
    return not insult_hit(text)


def classify(text: str, parent: str, deadline: float | None = None) -> tuple[str, str]:
    w = insult_hit(text)
    if w:
        return "review", f"Beleidigend: „{w}“"
    try:
        neg, echte_frage, why = ai_check(text, parent, deadline)
    except Exception as e:  # noqa: BLE001
        if is_question(text):
            return "review", "Frage"
        return "review", "KI-Prüfung fehlgeschlagen: " + str(e)
    if neg:
        return "review", "Persönlich beleidigend (KI): " + why
    if echte_frage:
        return "review", "Frage"
    return "like", why


# ---------------- Hilfen ----------------

def embed_thumb(e) -> str:
    if not isinstance(e, dict):
        return ""
    t = str(e.get("$type", ""))
    if t.startswith("app.bsky.embed.images"):
        return str(((e.get("images") or [{}])[0] or {}).get("thumb", ""))
    if t.startswith("app.bsky.embed.video"):
        return str(e.get("thumbnail", ""))
    if t.startswith("app.bsky.embed.external"):
        return str((e.get("external") or {}).get("thumb", ""))
    if t.startswith("app.bsky.embed.recordWithMedia"):
        return embed_thumb(e.get("media"))
    return ""


_TID = re.compile(r"/app\.bsky\.feed\.post/([2-7a-j][2-7a-z]{12})$")
_B32 = "234567abcdefghijklmnopqrstuvwxyz"


def tid_time(uri: str) -> int:
    m = _TID.search(uri)
    if not m:
        return 0
    v = 0
    for ch in m.group(1):
        v = (v << 5) | _B32.index(ch)
    t = (v >> 10) // 1000000
    return t if 1577836800 < t < time.time() + 86400 else 0


def ts(s: str) -> int:
    if not s:
        return 0
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def own_anchor(rec: dict, me: str, own_cutoff: int) -> str:
    pre = f"at://{me}/app.bsky.feed.post/"
    r = rec.get("reply") or {}
    for u in (str((r.get("root") or {}).get("uri", "")), str((r.get("parent") or {}).get("uri", ""))):
        if not u or not u.startswith(pre):
            continue
        t = tid_time(u)
        if t == 0 or t >= own_cutoff:
            return u
    return ""


def muted(a: dict) -> bool:
    v = a.get("viewer") or {}
    return bool(v.get("muted") or v.get("blocking") or v.get("blockedBy"))


# ---------------- Auto-Likes ----------------

class AutoLike:
    def __init__(self, bs: Bsky, state: dict):
        self.bs, self.me = bs, bs.did
        self.st = state["settings"]
        self.dry = bool(self.st["dryrun"])
        self.since = int(self.st["since_ts"])
        self.known: dict = dict(state.get("known") or {})
        self.flagged: dict = dict(state.get("flagged") or {})
        self.dry_rows, self.ki_rows = state.get("dryrun_rows") or [], state.get("ki_rows") or []
        self.notif_old = state.get("notif") or {"hwm": "", "back": "", "pending": []}
        self.cache_old: dict = dict(state.get("thread_cache") or {})
        self.notif_new, self.cache_new = None, None
        self.likes = 0
        self.ops: list = []
        self.pool = ThreadPoolExecutor(max_workers=AI_WORKERS, thread_name_prefix="fl")
        self.cls: dict = {}          # Schlüssel -> (Entscheidung, Grund), parallel vorab ermittelt
        self.t: dict = {}            # Zeitmessung je Schritt
        self.stats = {"ok": True, "dryrun": self.dry, "posts": 0, "seen": 0, "liked": 0, "review": 0, "skipped": 0, "would_like": 0,
                      "notif": 0, "notif_pages": 0, "notif_gone": 0, "errors": []}

    # Schreiben in die DB (gepuffert; nach jedem Like sofort)
    def op(self, o: dict, now: bool = False) -> None:
        self.ops.append(o)
        if now or len(self.ops) >= 20:
            self.flush()

    def flush(self) -> None:
        if self.ops and not READONLY:
            bridge("save", ops=self.ops)
        self.ops = []

    def err(self, msg: str) -> None:
        s = self.stats
        if msg.startswith(TRANSIENT_PREFIX):
            s["hint_count"] = s.get("hint_count", 0) + 1
            h = s.setdefault("hints", [])
            if len(h) < 5 and msg not in h:
                h.append(msg)
            return
        s["error_count"] = s.get("error_count", 0) + 1
        if len(s["errors"]) < 5 and msg not in s["errors"]:
            s["errors"].append(msg)

    def save(self, c: dict, decision: str, reason: str, like_uri: str = "", now: bool = False) -> None:
        self.op({"t": "save", "c": c, "decision": decision, "reason": reason[:250], "like_uri": like_uri}, now)
        self.known[c["uri"]] = decision

    def do_like(self, uri: str, cid: str) -> str:
        # Doppel-Like-Schutz: unmittelbar vorher frischen viewer.like-Status lesen
        if self.bs.already_liked(uri):
            return "ALREADY"
        return self.bs.like(uri, cid)

    def prefetch(self, items: list) -> None:
        """KI-Prüfungen parallel vorab ausführen. items: [(schlüssel, text, kontext)]. Ergebnis identisch zu classify()."""
        todo = [(k, t, p) for k, t, p in items if k not in self.cls and needs_ai(t)]
        if not todo or left() < 25:
            return
        dl = left() - 10
        futs = {self.pool.submit(classify, t, p, dl): k for k, t, p in todo}
        done, _ = fwait(futs, timeout=max(1.0, left() - 8))
        for f in done:
            self.cls[futs[f]] = f.result()

    def classify_c(self, key: str, text: str, parent: str):
        r = self.cls.pop(key, None)
        if r is not None:
            return r
        if needs_ai(text) and left() < 20:
            return None                      # keine Zeit mehr für die KI -> nächster Lauf
        return classify(text, parent, max(5.0, left() - 5))

    @staticmethod
    def needs_check(c: dict, prev, flagged: dict, dry: bool) -> bool:
        return not c["liked"] and not flagged.get(c["did"]) and not c["muted"] and not (prev == "dryrun" and not dry)

    def decide(self, c: dict, prev) -> str:
        s = self.stats
        # Von Hand in den Tageszahlen übersprungen: nie nachträglich liken.
        if prev == "dismissed":
            return "ok"
        if self.needs_check(c, prev, self.flagged, self.dry):
            r = self.classify_c(c["uri"], c["text"], c["root_text"])
            if r is None:
                s["time_limit"] = True; return "time"
        s["seen"] += 1
        if c["liked"]:
            self.save(c, "skipped", "bereits geliked", c["like_uri"]); s["skipped"] += 1; return "ok"
        if self.flagged.get(c["did"]):
            self.save(c, "skipped", self.flagged[c["did"]]); s["skipped"] += 1; return "ok"
        if c["muted"]:
            self.save(c, "skipped", "stummgeschaltet/blockiert"); s["skipped"] += 1; return "ok"
        if prev == "dryrun" and not self.dry:
            dec, why = "like", "aus Testmodus übernommen"
        else:
            dec, why = r
        if dec == "review":
            self.save(c, "review", why); s["review"] += 1; return "ok"
        if self.dry:
            self.save(c, "dryrun", "würde liken" + (f" ({why})" if why else "")); s["would_like"] += 1; return "ok"
        if self.likes >= self.st["max"]:
            s["limit_reached"] = True; return "limit"
        if READONLY:
            s["would_like"] += 1; return "ok"
        try:
            lu = self.do_like(c["uri"], c["cid"])
        except RateLimit:
            raise
        except Exception as e:  # noqa: BLE001
            self.err(str(e)); return "ok"
        if lu == "ALREADY":
            self.save(c, "skipped", "bereits geliked"); s["skipped"] += 1; return "ok"
        self.save(c, "liked", "automatisch (aus Testmodus)" if prev == "dryrun" else "automatisch", lu, now=True)
        s["liked"] += 1; self.likes += 1
        time.sleep(SLEEP_LIKE)
        return "ok"

    def step_dryrun_rows(self) -> None:
        if self.dry:
            return
        for row in self.dry_rows:
            if left() < 30:
                self.stats["time_limit"] = True; break
            uri, cid, rid = str(row["reply_uri"]), str(row["reply_cid"]), int(row["id"])
            if READONLY:
                self.stats["would_like"] += 1; continue
            try:
                lu = self.do_like(uri, cid)
            except RateLimit:
                raise
            except Exception as e:  # noqa: BLE001
                self.err(str(e)); continue
            if lu == "ALREADY":
                self.op({"t": "upd", "id": rid, "decision": "skipped", "reason": "bereits geliked"}); self.stats["skipped"] += 1; continue
            self.op({"t": "upd", "id": rid, "decision": "liked", "reason": "automatisch (aus Testmodus)", "like_uri": lu}, now=True)
            self.known[uri] = "liked"; self.stats["liked"] += 1; self.likes += 1; time.sleep(SLEEP_LIKE)

    def step_ki_rows(self) -> None:
        self.prefetch([("ki:" + str(r["id"]), str(r.get("text") or ""), str(r.get("parent_text") or "")) for r in self.ki_rows])
        for row in self.ki_rows:
            if left() < 30:
                self.stats["time_limit"] = True; break
            rid, uri, cid = int(row["id"]), str(row["reply_uri"]), str(row["reply_cid"])
            r = self.classify_c("ki:" + str(rid), str(row.get("text") or ""), str(row.get("parent_text") or ""))
            if r is None:
                self.stats["time_limit"] = True; break
            dec, why = r
            if dec == "review" and why.startswith("KI-Prüfung fehlgeschlagen"):
                self.err(why); break
            if dec == "review":
                self.op({"t": "upd", "id": rid, "decision": "review", "reason": why[:250]}); continue
            if self.dry:
                self.op({"t": "upd", "id": rid, "decision": "dryrun", "reason": ("neu geprüft: würde liken " + why)[:250]}); self.stats["would_like"] += 1; continue
            if self.likes >= self.st["max"]:
                break
            if READONLY:
                self.stats["would_like"] += 1; continue
            try:
                lu = self.do_like(uri, cid)
            except RateLimit:
                raise
            except Exception as e:  # noqa: BLE001
                self.err(str(e)); continue
            if lu == "ALREADY":
                self.op({"t": "upd", "id": rid, "decision": "skipped", "reason": "bereits geliked"}); continue
            self.op({"t": "upd", "id": rid, "decision": "liked", "reason": "automatisch (KI nachgeprüft)", "like_uri": lu}, now=True)
            self.known[uri] = "liked"; self.stats["liked"] += 1; self.likes += 1; time.sleep(SLEEP_LIKE)

    def want(self, u: str) -> bool:
        return u not in self.known or (self.known[u] == "dryrun" and not self.dry)

    def notif_page(self, cursor: str):
        d = self.bs.get("app.bsky.notification.listNotifications", "limit=100&reasons=reply" + ("&cursor=" + urllib.parse.quote(cursor, safe="") if cursor else ""))
        self.stats["notif_pages"] += 1
        return list(d.get("notifications") or []), str(d.get("cursor") or "")

    def step_notif(self) -> None:
        s, me = self.stats, self.me
        own_cutoff = int(time.time()) - self.st["days"] * 86400
        list_cutoff = max(self.since, own_cutoff)
        old = self.notif_old
        state = {"hwm": old.get("hwm", ""), "back": old.get("back", "")}
        hwm_t = ts(old.get("hwm", ""))
        fresh, back = [], []

        def add(n, into):
            if n.get("reason") != "reply" or (n.get("author") or {}).get("did") == me:
                return
            u = str(n.get("uri", ""))
            if u and u not in into:
                into.append(u); s["notif"] += 1
        queue = list(old.get("pending") or [])
        try:
            cursor, new_hwm, reached = "", "", False
            for _ in range(10):  # neue Mitteilungen bis zum letzten Stand
                lst, nxt = self.notif_page(cursor)
                for n in lst:
                    it = str(n.get("indexedAt", "")); t = ts(it)
                    if not new_hwm:
                        new_hwm = it
                    if t < hwm_t or t < list_cutoff:
                        reached = True; break
                    add(n, fresh)
                if reached or not nxt:
                    reached = True; break
                cursor = nxt
            if new_hwm:
                if reached:
                    state["hwm"] = new_hwm
                elif not state["back"]:
                    state["hwm"], state["back"] = new_hwm, cursor
            for _ in range(10):  # Nachholen älterer Mitteilungen
                if not state["back"] or left() < 60:
                    break
                lst, nxt = self.notif_page(state["back"]); end = False
                for n in lst:
                    if ts(str(n.get("indexedAt", ""))) < list_cutoff:
                        end = True; break
                    add(n, back)
                state["back"] = "" if (end or not nxt) else nxt
            queue = [u for u in dict.fromkeys(list(old.get("pending") or []) + fresh + back) if self.want(u)]
            s["notif_open"] = len(queue)
            while queue:
                if left() < 30:
                    s["time_limit"] = True; break
                chunk = queue[:25]
                try:
                    posts = self.bs.get_posts(chunk)
                    ctx_u = []
                    for p in posts.values():
                        r = (p.get("record") or {}).get("reply") or {}
                        for k in ("root", "parent"):
                            if (r.get(k) or {}).get("uri"):
                                ctx_u.append(r[k]["uri"])
                    ctx = self.bs.get_posts(ctx_u)
                except RateLimit:
                    raise
                except Exception as e:  # noqa: BLE001
                    self.err(str(e)); break
                built: dict = {}
                for u in chunk:
                    p = posts.get(u)
                    if p is None:
                        built[u] = None; continue
                    a, rec = p.get("author") or {}, p.get("record") or {}
                    anchor = own_anchor(rec, me, own_cutoff)
                    created = str(rec.get("createdAt") or p.get("indexedAt") or "")
                    if a.get("did") == me or not anchor or ts(created) < self.since or not self.want(u):
                        built[u] = False; continue
                    par = ctx.get(str(((rec.get("reply") or {}).get("parent") or {}).get("uri", "")), {})
                    anc = ctx.get(anchor, {})
                    built[u] = {"uri": u, "cid": str(p.get("cid", "")), "did": str(a.get("did", "")), "handle": str(a.get("handle", "")),
                         "text": str(rec.get("text", "")), "created": created,
                         "liked": bool((p.get("viewer") or {}).get("like")), "like_uri": str((p.get("viewer") or {}).get("like", "")),
                         "muted": muted(a), "root_uri": anchor, "root_text": str((anc.get("record") or {}).get("text", "")),
                         "avatar": str(a.get("avatar", "")),
                         "thumb": embed_thumb(p.get("embed")) or embed_thumb(par.get("embed")) or embed_thumb(anc.get("embed")),
                         "parent_text": str((par.get("record") or {}).get("text", ""))[:500]}
                if self.likes < self.st["max"] or self.dry:
                    self.prefetch([(c["uri"], c["text"], c["root_text"]) for u2, c in built.items()
                                   if c and self.needs_check(c, self.known.get(u2), self.flagged, self.dry)])
                for u in chunk:
                    c = built[u]
                    if c is None:
                        s["notif_gone"] += 1; queue.pop(0); continue
                    if c is False or not self.want(u):
                        queue.pop(0); continue
                    if self.decide(c, self.known.get(u)) in ("limit", "time"):
                        raise StopIteration
                    queue.pop(0)
        except StopIteration:
            pass
        finally:
            s["notif_pending"] = len(queue)
            self.notif_new = {"hwm": state["hwm"], "back": state["back"][:120], "pending": queue[:400]}

    def collect(self, node: dict, root_text: str, out: list, root_thumb: str = "") -> None:
        parent = node.get("post") or {}
        if not root_thumb:
            root_thumb = embed_thumb(parent.get("embed"))
        for r in node.get("replies") or []:
            if r.get("$type") != "app.bsky.feed.defs#threadViewPost":
                continue
            p = r.get("post") or {}; a = p.get("author") or {}; rec = p.get("record") or {}
            out.append({"uri": str(p.get("uri", "")), "cid": str(p.get("cid", "")), "did": str(a.get("did", "")), "handle": str(a.get("handle", "")),
                        "text": str(rec.get("text", "")), "created": str(rec.get("createdAt") or p.get("indexedAt") or ""),
                        "liked": bool((p.get("viewer") or {}).get("like")), "like_uri": str((p.get("viewer") or {}).get("like", "")),
                        "muted": muted(a), "root_uri": str((node.get("post") or {}).get("uri", "")), "root_text": root_text,
                        "avatar": str(a.get("avatar", "")),
                        "thumb": embed_thumb(p.get("embed")) or embed_thumb(parent.get("embed")) or root_thumb,
                        "parent_text": str((parent.get("record") or {}).get("text", ""))[:500]})
            self.collect(r, root_text, out, root_thumb)

    def own_posts(self) -> list:
        cutoff = int(time.time()) - self.st["days"] * 86400; out, cursor = [], None
        for _ in range(5):
            q = {"actor": self.me, "limit": 100, "filter": "posts_and_author_threads"}
            if cursor:
                q["cursor"] = cursor
            d = self.bs.get("app.bsky.feed.getAuthorFeed", q); old = False
            for item in d.get("feed") or []:
                p = item.get("post") or {}
                if (p.get("author") or {}).get("did") != self.me or "reason" in item:
                    continue
                if ts(str((p.get("record") or {}).get("createdAt", ""))) < cutoff:
                    if "reply" not in (p.get("record") or {}):
                        old = True
                    continue
                out.append(p)
            cursor = d.get("cursor")
            if not cursor or old:
                break
        return out

    def step_threads(self) -> None:
        s = self.stats
        if s.get("limit_reached") or left() < 60:
            s["fallback"] = "übersprungen"; return
        s["fallback"] = "gelaufen"
        posts = self.own_posts(); mine = {p.get("uri") for p in posts}
        cache, new_cache, cur = self.cache_old, {}, ""
        def fetch(u: str):
            try:
                return self.bs.get("app.bsky.feed.getPostThread", {"uri": u, "depth": 10, "parentHeight": 0})
            except Exception as e:  # noqa: BLE001  (RateLimit eingeschlossen; wird unten in Reihenfolge behandelt)
                return e
        need = []
        for p in posts:
            rc, uri = int(p.get("replyCount") or 0), str(p.get("uri", ""))
            root = str((((p.get("record") or {}).get("reply") or {}).get("root") or {}).get("uri", ""))
            cv = cache.get(uri)
            if rc and not (root and root in mine) and not (cv and int(cv[0]) == rc and time.time() - int(cv[1]) <= 10800):
                need.append(uri)
        futs = {u: self.pool.submit(fetch, u) for u in need} if left() >= 30 else {}
        try:
            for p in posts:
                rc, uri = int(p.get("replyCount") or 0), str(p.get("uri", ""))
                cur = uri
                if rc == 0:
                    continue
                root = str((((p.get("record") or {}).get("reply") or {}).get("root") or {}).get("uri", ""))
                if root and root in mine:
                    continue
                cv = cache.get(uri)
                if cv and int(cv[0]) == rc and time.time() - int(cv[1]) <= 10800:
                    new_cache[uri] = cv; continue
                if left() < 30 or uri not in futs:
                    s["time_limit"] = True; break
                s["posts"] += 1
                try:
                    th = futs[uri].result(timeout=max(1.0, left() - 20))
                except Exception:  # noqa: BLE001  (Zeitüberschreitung beim Warten)
                    s["time_limit"] = True; break
                if isinstance(th, RateLimit):
                    raise th
                if isinstance(th, Exception):
                    self.err(str(th)); continue
                new_cache[uri] = [rc, int(time.time())]
                lst: list = []
                self.collect(th.get("thread") or {}, str((p.get("record") or {}).get("text", "")), lst)
                if self.likes < self.st["max"] or self.dry:
                    self.prefetch([(c["uri"], c["text"], c["root_text"]) for c in lst
                                   if c["uri"] and c["did"] != self.me and ts(c["created"]) >= self.since
                                   and (self.known.get(c["uri"]) is None or (self.known.get(c["uri"]) == "dryrun" and not self.dry))
                                   and self.needs_check(c, self.known.get(c["uri"]), self.flagged, self.dry)])
                for c in lst:
                    if not c["uri"] or c["did"] == self.me:
                        continue
                    prev = self.known.get(c["uri"])
                    if prev is not None and not (prev == "dryrun" and not self.dry):
                        continue
                    if ts(c["created"]) < self.since:
                        continue
                    if left() < 20:
                        s["time_limit"] = True; new_cache.pop(uri, None); raise StopIteration
                    if self.decide(c, prev) in ("limit", "time"):
                        new_cache.pop(uri, None); raise StopIteration
        except StopIteration:
            pass
        if not s.get("time_limit") and not s.get("limit_reached"):
            self.cache_new = new_cache
        else:
            merged = {**cache, **new_cache}; merged.pop(cur, None); self.cache_new = merged

    def run(self) -> dict:
        try:
            for name, fn in (("testmodus", self.step_dryrun_rows), ("ki_nachpruefung", self.step_ki_rows),
                             ("mitteilungen", self.step_notif), ("threads", self.step_threads)):
                t1 = time.monotonic()
                try:
                    fn()
                finally:
                    self.t[name] = round(time.monotonic() - t1, 1)
        except RateLimit:
            self.stats["ok"] = False; self.stats["errors"].append("Bluesky-Rate-Limit erreicht – nächster Lauf macht weiter")
        except Exception as e:  # noqa: BLE001
            self.stats["ok"] = False; self.stats["errors"].append(str(e)[:300])
        finally:
            self.pool.shutdown(wait=False, cancel_futures=True)
            self.flush()
        return self.stats


# ---------------- Entfolger-Abgleich ----------------

def account_status(did: str) -> str:
    r = http(RELAY + "/xrpc/com.atproto.sync.getRepoStatus?did=" + urllib.parse.quote(did, safe=""), {"Accept": "application/json"})
    if r.get("did") != did or not isinstance(r.get("active"), bool):
        raise RuntimeError("Kontostatus nicht sicher prüfbar.")
    if r["active"]:
        if r.get("status"):
            raise RuntimeError("Kontostatus derzeit eingeschränkt.")
        return "active"
    return r.get("status") if r.get("status") in ("deactivated", "deleted", "suspended", "takendown") else "inactive"


_DID = re.compile(r"^did:[a-z0-9]+:[A-Za-z0-9._:%-]+$")


UF_INFO: dict = {}


def uf_scan(bs: Bsky) -> list:
    """Kompletter Follower-Abruf (100 je Seite, keine Einzelabrufe). Reicht die Zeit nicht: Deferred (kein Fehler)."""
    members, cursor, seen = {}, "", set()
    t1 = time.monotonic(); UF_INFO.update(pages=0, fetch_s=0.0)
    for _ in range(1000):
        UF_INFO["fetch_s"] = round(time.monotonic() - t1, 1); UF_INFO["fetched"] = len(members)
        if left() < 45:   # Rest wird für das Speichern der Liste (~2 MB) gebraucht
            raise Deferred("Zeit reichte nicht für den kompletten Follower-Abruf – nächster Lauf macht ihn")
        q = {"actor": bs.did, "limit": 100}
        if cursor:
            q["cursor"] = cursor
        d = bs.get("app.bsky.graph.getFollowers", q); UF_INFO["pages"] += 1
        if (d.get("subject") or {}).get("did") != bs.did or not isinstance(d.get("followers"), list):
            raise RuntimeError("Unvollständige Follower-Antwort; Vergleich nicht übernommen.")
        for p in d["followers"]:
            if not isinstance(p, dict) or not isinstance(p.get("did"), str) or not _DID.match(p["did"]):
                raise RuntimeError("Ungültiger Follower; Vergleich nicht übernommen.")
            members[p["did"]] = {"did": p["did"], "handle": str(p.get("handle", "")), "display_name": str(p.get("displayName", ""))}
        cursor = d.get("cursor") or ""
        if not isinstance(cursor, str):
            raise RuntimeError("Ungültiger Follower-Cursor.")
        if not cursor:
            UF_INFO["fetch_s"] = round(time.monotonic() - t1, 1); UF_INFO["fetched"] = len(members)
            return list(members.values())
        if cursor in seen:
            raise RuntimeError("Wiederholter Follower-Cursor; Vergleich abgebrochen.")
        seen.add(cursor)
    raise RuntimeError("Zu viele Follower-Seiten")


def uf_run(bs: Bsky, uf: dict) -> dict:
    if not uf.get("ready"):
        return {"status": "missing"}
    if not uf.get("enabled"):
        return {"status": "disabled"}
    res: dict = {}
    pending = uf.get("pending") or []
    last = uf.get("last_completed") or ""
    age = time.time() - ts(last.replace(" ", "T") + "+00:00") if last else 1e9
    if age >= UF_SCAN_MIN * 60:
        try:
            members = uf_scan(bs)
        except Deferred as e:
            res.update(status="deferred", message=str(e), **UF_INFO)
            if last and age >= UF_STALE_MIN * 60:   # zu lange nicht fertig geworden -> echter Fehler (alter Cron übernimmt)
                res.update(status="error", message=f"Follower-Abgleich seit {int(age // 3600)} h nicht fertig geworden")
            return res
        res["followers"] = len(members); res.update(UF_INFO)
        if READONLY:
            res["status"] = "readonly_scan"; return res
        t1 = time.monotonic()
        r = bridge("uf_commit", actor=bs.did, members=members)
        res["commit_s"] = round(time.monotonic() - t1, 1)
        res.update({k: r.get(k) for k in ("status", "new", "reason")})
        if r.get("status") not in ("baseline", "complete"):
            return res
        pending = r.get("pending") or []
    else:
        res["status"] = "not_due"; res["last_run_min"] = int(age // 60)
    if not pending or READONLY:
        res["pending"] = len(pending); return res
    count = errors = blocked = 0; stopped = ""
    for row in pending[:10]:
        if left() < 30:
            stopped = "Laufzeitgrenze"; break
        did, eid = row["did"], row["id"]
        base = {"id": eid, "did": did, "actor": bs.did, "handle": row.get("handle", ""), "display_name": row.get("display_name", "")}
        try:
            st = account_status(did)
            if st != "active":
                bridge("uf_event", type="inactive", account_status=st, **base); continue
            prof = (bs.get("app.bsky.actor.getProfiles", "actors=" + urllib.parse.quote(did, safe="")).get("profiles") or [])
            p = next((x for x in prof if x.get("did") == did), None)
            if not p or not isinstance(p.get("viewer"), dict):
                raise RuntimeError("Profilbeziehung nicht sicher prüfbar.")
            v = p["viewer"]
            if v.get("blocking") or v.get("blockedBy") or v.get("blockingByList"):
                # Blockierung ist kein Fehler: nur notieren und überspringen (bleibt auf der Website zur Ansicht markiert)
                blocked += 1; log("Entfolgen-Prüfung: blockierte Beziehung übersprungen (kein Fehler).")
                try:
                    bridge("uf_event", type="error", **base)
                except Exception:  # noqa: BLE001
                    pass
                continue
            base.update(handle=str(p.get("handle") or base["handle"]), display_name=str(p.get("displayName") or base["display_name"]))
            if v.get("followedBy"):
                bridge("uf_event", type="returned", **base); continue
            if not v.get("following"):
                bridge("uf_event", type="not_following", **base); continue
            g = bridge("uf_event", type="pre_delete", follow_uri=str(v["following"]), **base)   # Ausschluss wird VOR dem Löschen gespeichert
            if g.get("status") == "day_limit":
                stopped = "Tageslimit von 100 Entfolgen-Versuchen erreicht"; break
            if g.get("status") != "go":
                continue
            bs.post("com.atproto.repo.deleteRecord", {"repo": bs.did, "collection": "app.bsky.graph.follow", "rkey": g["rkey"]})
            bridge("uf_event", type="deleted", **base); count += 1
        except RateLimit:
            errors += 1
            try:
                bridge("uf_event", type="error", **base)
            except Exception:  # noqa: BLE001
                pass
            stopped = "Bluesky-Rate-Limit"; break
        except Exception as e:  # noqa: BLE001
            errors += 1; log(f"Entfolgen-Prüfung Fehler: {str(e)[:120]}")
            try:
                bridge("uf_event", type="error", **base)
            except Exception:  # noqa: BLE001
                pass
        time.sleep(0.15)
    res["automatic"] = {"unfollowed": count, "error_count": errors, "blocked_skipped": blocked, "stopped": stopped}
    if errors:
        res["status"] = "error"
    return res


# ---------------- Auto-Folgen (1:1 flb_af_run in bluesky-autofollow.php) ----------------

_DID_AF = re.compile(r"^did:[a-z0-9]+:[A-Za-z0-9._:%-]+$")
AF_BAD_LABELS = {"spam", "!hide", "!takedown", "!warn", "impersonation", "scam"}


def af_today_start_utc() -> str:
    start = datetime.now(ZoneInfo("Europe/Berlin")).replace(hour=0, minute=0, second=0, microsecond=0)
    return start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def af_berlin_now() -> str:
    return datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d %H:%M:%S")


def af_own_posts(bs: Bsky, days: int) -> list:
    cutoff = int(time.time()) - days * 86400
    out, cursor = [], None
    for _ in range(5):
        q = {"actor": bs.did, "limit": 100, "filter": "posts_and_author_threads"}
        if cursor:
            q["cursor"] = cursor
        d = bs.get("app.bsky.feed.getAuthorFeed", q)
        old = False
        for item in d.get("feed") or []:
            p = item.get("post") or {}
            if (p.get("author") or {}).get("did") != bs.did or item.get("reason"):
                continue
            rec = p.get("record") or {}
            pt = ts(str(rec.get("createdAt") or "")) or 0
            if pt < cutoff:
                if not rec.get("reply"):
                    old = True
                continue
            out.append(p)
        cursor = d.get("cursor")
        if not cursor or old:
            break
    return out


def af_cache_stale(cache: dict, uri: str, count: int, max_age: int = 21600) -> bool:
    row = cache.get(uri)
    if not isinstance(row, (list, tuple)) or len(row) < 2:
        return True
    return int(row[0]) != count or time.time() - int(row[1]) > max_age


def af_profiles(bs: Bsky, dids: list[str]) -> dict:
    out = {}
    uniq = [d for d in dict.fromkeys(dids) if d and _DID_AF.match(d)]
    for i in range(0, len(uniq), 25):
        chunk = uniq[i:i + 25]
        qs = "&".join("actors=" + urllib.parse.quote(d, safe="") for d in chunk)
        d = bs.get("app.bsky.actor.getProfiles", qs)
        for p in d.get("profiles") or []:
            if p.get("did"):
                out[str(p["did"])] = p
        if SLEEP_AF:
            time.sleep(SLEEP_AF)
    return out


def af_bad_label(p: dict) -> str:
    for lab in p.get("labels") or []:
        v = str((lab or {}).get("val") or "")
        if v in AF_BAD_LABELS:
            return v
    return ""


def af_hint(stats: dict, msg: str) -> None:
    h = stats.setdefault("hints", [])
    if msg not in h:
        h.append(msg[:300])


def af_err(stats: dict, msg: str) -> None:
    if msg.startswith(TRANSIENT_PREFIX):
        af_hint(stats, msg)
        stats["transient"] = True
        return
    stats["error_count"] = stats.get("error_count", 0) + 1
    errs = stats.setdefault("errors", [])
    if len(errs) < 5 and msg not in errs:
        errs.append(msg[:300])


def af_follow(bs: Bsky, did: str) -> str:
    if READONLY:
        raise RuntimeError("READONLY")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"
    d = bs.post("com.atproto.repo.createRecord", {
        "repo": bs.did, "collection": "app.bsky.graph.follow",
        "record": {"$type": "app.bsky.graph.follow", "subject": did, "createdAt": now},
    })
    return str(d.get("uri") or "")



AF_REUNFOLLOWED_UNTIL_DEFAULT = "2027-02-14"
AF_REASON_SELF_UNFOLLOWED = "von dir entfolgt"


def af_reunfollowed_until(raw=None) -> str:
    """Stichtag als YYYY-MM-DD; ungültige Werte fallen auf den Standard zurück."""
    s = str(raw or "").strip() or AF_REUNFOLLOWED_UNTIL_DEFAULT
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        return AF_REUNFOLLOWED_UNTIL_DEFAULT


def af_reunfollowed_active(until=None, now: datetime | None = None) -> bool:
    """True bis einschließlich Stichtag (Europe/Berlin). Ab dem Folgetag gilt wieder die alte Sperre."""
    tz = ZoneInfo("Europe/Berlin")
    if now is None:
        now = datetime.now(tz)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    else:
        now = now.astimezone(tz)
    deadline = datetime.strptime(af_reunfollowed_until(until), "%Y-%m-%d").date()
    return now.date() <= deadline


def af_is_reunfollowed_candidate(prev, did: str, reunfollowed: set[str], active: bool) -> bool:
    return bool(active and prev == "blocked" and did in reunfollowed)


class AutoFollow:
    def __init__(self, bs: Bsky, begin: dict):
        self.bs = bs
        self.af = begin.get("af") or {}
        self.ops: list[dict] = []
        st = self.af.get("settings") or {}
        self.enabled = bool(st.get("enabled"))
        self.dry = bool(st.get("dryrun"))
        self.max_day = max(1, min(500, int(st.get("max_day") or 100)))
        self.max_run = max(1, min(50, int(st.get("max_run") or 10)))
        self.days = max(1, min(30, int(st.get("days") or 7)))
        self.today = int(self.af.get("today") or 0)
        self.known = {str(k): str(v) for k, v in (self.af.get("known") or {}).items()}
        self.pending = {str(k): str(v) for k, v in (self.af.get("pending") or {}).items()}
        self.excluded = set(str(x) for x in (self.af.get("exclusions") or []))
        self.flagged = {str(k): str(v) for k, v in (self.af.get("flagged") or begin.get("flagged") or {}).items()}
        self.return_cursor = str(self.af.get("return_cursor") or "")
        self.cache = dict(self.af.get("cache") or {})
        self.recheck = [str(x) for x in (self.af.get("recheck") or [])]
        self.backcheck_rows = {str(k): v for k, v in (self.af.get("backcheck") or {}).items()}
        self.reunfollowed_until = af_reunfollowed_until((st.get("reunfollowed_until") if isinstance(st, dict) else None))
        self.reunfollowed_active = af_reunfollowed_active(self.reunfollowed_until)
        self.reunfollowed = set(str(x) for x in (self.af.get("reunfollowed") or []))
        self.cache_new: dict | None = None
        self.new_cursor: str | None = None
        self.stats: dict = {"ok": True, "dryrun": self.dry, "posts": 0, "likers": 0, "followed": 0,
                            "would_follow": 0, "skipped": 0, "blocked_new": 0, "today": self.today, "errors": []}

    def flush(self) -> None:
        if READONLY or not self.ops:
            self.ops = []
            return
        bridge("af_save", ops=self.ops)
        self.ops = []

    def add(self, op: dict) -> None:
        if READONLY:
            return
        self.ops.append(op)
        if len(self.ops) >= 40:
            self.flush()

    def save(self, did: str, p: dict, src: str, status: str, reason: str, follow_uri: str = "", followed_now: bool = False) -> None:
        self.add({"t": "save", "did": did, "handle": str(p.get("handle") or ""), "display_name": str(p.get("displayName") or ""),
                  "avatar_url": str(p.get("avatar") or ""), "source_post_uri": src, "status": status,
                  "reason": reason, "follow_uri": follow_uri, "followed_now": followed_now})
        self.known[did] = status

    def run(self) -> dict:
        st = self.stats
        if not self.af.get("ready"):
            st["status"] = "missing"
            return st
        if not self.enabled:
            st["status"] = "disabled"
            return st
        if READONLY:
            st["status"] = "readonly"
        budget = 10**9 if self.dry else max(0, min(self.max_run, self.max_day - self.today))
        try:
            # 1) Entfolgt-Erkennung
            if self.recheck and left() >= 8:
                pp = af_profiles(self.bs, self.recheck)
                for did in self.recheck:
                    p = pp.get(did)
                    if not p:
                        continue
                    if not (p.get("viewer") or {}).get("following"):
                        self.add({"t": "mark_blocked", "did": did, "reason": AF_REASON_SELF_UNFOLLOWED})
                        self.known[did] = "blocked"
                        self.reunfollowed.add(did)
                        st["blocked_new"] = st.get("blocked_new", 0) + 1
                    else:
                        self.add({"t": "checked", "did": did})
            # 1b) Zurückgefolgt
            if self.backcheck_rows and left() >= 8:
                pp = af_profiles(self.bs, list(self.backcheck_rows.keys()))
                n = 0
                for did in self.backcheck_rows:
                    p = pp.get(did)
                    if p is not None and (p.get("viewer") or {}).get("followedBy"):
                        self.add({"t": "backcheck_yes", "did": did}); n += 1
                    else:
                        self.add({"t": "backcheck_touch", "did": did})
                st["followed_back_new"] = n
            # 2) Liker eigener Posts
            likers: dict[str, str] = {}
            new_cache: dict = {}
            time_limit = False
            for p in af_own_posts(self.bs, self.days):
                lc = int(p.get("likeCount") or 0)
                uri = str(p.get("uri") or "")
                if lc == 0 or not uri:
                    continue
                if not af_cache_stale(self.cache, uri, lc):
                    new_cache[uri] = self.cache[uri]
                    continue
                if left() < 8:
                    time_limit = True
                    break
                st["posts"] += 1
                cursor, pages, ok_post = None, 0, True
                while True:
                    if left() < 8:
                        time_limit = True
                        ok_post = False
                        break
                    q = {"uri": uri, "limit": 100}
                    if cursor:
                        q["cursor"] = cursor
                    try:
                        d = self.bs.get("app.bsky.feed.getLikes", q)
                    except RateLimit:
                        raise
                    except Exception as e:  # noqa: BLE001
                        if "RATE_LIMIT" in str(e):
                            raise RateLimit() from e
                        af_err(st, str(e))
                        ok_post = False
                        break
                    for lk in d.get("likes") or []:
                        actor = (lk or {}).get("actor") or {}
                        did = str(actor.get("did") or "")
                        if did and did != self.bs.did and did not in likers:
                            likers[did] = uri
                    cursor = d.get("cursor")
                    pages += 1
                    if SLEEP_AF:
                        time.sleep(SLEEP_AF)
                    if not cursor or pages >= 20:
                        break
                if ok_post:
                    new_cache[uri] = [lc, int(time.time())]
            if time_limit:
                st["time_limit"] = True
                merged = {**self.cache, **new_cache}
                self.cache_new = merged
            else:
                self.cache_new = new_cache
            # 3) Todo: zuerst would_follow, dann neue Liker, dann frühere Entfolger
            todo: dict[str, str] = {}
            if not self.dry:
                for did, src in self.pending.items():
                    todo[did] = src
                st["pending"] = len(self.pending)
            for did, src in likers.items():
                if did in todo:
                    continue
                prev = self.known.get(did)
                if prev is None or (prev == "would_follow" and not self.dry) or af_is_reunfollowed_candidate(prev, did, self.reunfollowed, self.reunfollowed_active):
                    todo[did] = src
            return_dids = sorted(self.excluded)
            if return_dids:
                after = self.return_cursor
                batch = [d for d in return_dids if d > after]
                if not batch:
                    batch = return_dids
                batch = batch[:50]
                returns = {d: todo.get(d, "") for d in batch}
                todo = {**returns, **todo}
                if not self.dry:
                    self.new_cursor = batch[-1]
            st["likers"] = len(likers)
            profiles = af_profiles(self.bs, list(todo.keys())) if todo else {}
            leftover_reason = ""
            for did, src in todo.items():
                p = profiles.get(did)
                if p is None:
                    if did in self.pending:
                        self.save(did, {}, src, "skipped", "Profil nicht gefunden")
                        st["skipped"] += 1
                    continue
                v = p.get("viewer") or {}
                if did in self.excluded and not v.get("followedBy"):
                    self.add({"t": "mark_blocked", "did": did, "reason": "Früherer Entfolger: wartet auf erneutes Folgen"})
                    st["skipped"] += 1
                    continue
                if v.get("following"):
                    self.save(did, p, src, "skipped", "folgst bereits", str(v.get("following") or ""))
                    st["skipped"] += 1
                    continue
                if v.get("blocking") or v.get("blockedBy") or v.get("muted"):
                    self.save(did, p, src, "skipped", "blockiert/stummgeschaltet")
                    st["skipped"] += 1
                    af_hint(st, "Auto-Folgen: blockiertes oder stummgeschaltetes Konto übersprungen (kein Fehler).")
                    continue
                flag = self.flagged.get(did) or ""
                if flag:
                    self.save(did, p, src, "skipped", flag)
                    st["skipped"] += 1
                    continue
                lab = af_bad_label(p)
                if lab:
                    self.save(did, p, src, "skipped", "Label: " + lab)
                    st["skipped"] += 1
                    continue
                if self.dry or READONLY:
                    if not READONLY:
                        self.save(did, p, src, "would_follow", "würde folgen")
                    st["would_follow"] += 1
                    continue
                if budget <= 0:
                    st["limit_reached"] = True
                    leftover_reason = "Tageslimit erreicht"
                    self.save(did, p, src, "would_follow", leftover_reason)
                    st["would_follow"] += 1
                    # restliche ebenfalls merken, damit die Tageszahlen-Liste sie zeigt
                    leftover_reason = leftover_reason
                    idx = list(todo.keys()).index(did)
                    for rest_did in list(todo.keys())[idx + 1:]:
                        rp = profiles.get(rest_did) or {"handle": "", "displayName": ""}
                        self.save(rest_did, rp, todo[rest_did], "would_follow", leftover_reason)
                        st["would_follow"] += 1
                    break
                if left() < 8:
                    st["time_limit"] = True
                    leftover_reason = "Zeitbudget – nächster Lauf"
                    self.save(did, p, src, "would_follow", leftover_reason)
                    st["would_follow"] += 1
                    idx = list(todo.keys()).index(did)
                    for rest_did in list(todo.keys())[idx + 1:]:
                        rp = profiles.get(rest_did) or {"handle": "", "displayName": ""}
                        self.save(rest_did, rp, todo[rest_did], "would_follow", leftover_reason)
                        st["would_follow"] += 1
                    break
                try:
                    fu = af_follow(self.bs, did)
                except RateLimit:
                    raise
                except Exception as e:  # noqa: BLE001
                    if str(e) == "RATE_LIMIT":
                        raise RateLimit() from e
                    af_err(st, str(e))
                    continue
                if not fu:
                    af_err(st, "Bluesky hat das Folgen nicht bestätigt.")
                    continue
                self.save(did, p, src, "followed", "automatisch", fu, True)
                self.add({"t": "set_fb_before", "did": did, "before": 1 if v.get("followedBy") else 0})
                st["followed"] += 1
                budget -= 1
                if SLEEP_FOLLOW:
                    time.sleep(SLEEP_FOLLOW)
            st["today"] = self.today + st["followed"]
        except RateLimit:
            st["ok"] = False
            st["errors"].append("Bluesky-Rate-Limit erreicht – nächster Lauf macht weiter")
        except Exception as e:  # noqa: BLE001
            if str(e).startswith(TRANSIENT_PREFIX):
                af_hint(st, str(e))
                st["transient"] = True
                st["time_limit"] = True
            else:
                st["ok"] = False
                st["errors"].append(str(e)[:300])
        finally:
            self.flush()
        if not st.get("status"):
            st["status"] = "readonly" if READONLY else ("dryrun" if self.dry else "ok")
        return st


# ---------------- Ablauf ----------------

def main() -> int:
    global XAI_MODEL
    missing = [n for n, v in (("FL_BRIDGE_URL", BRIDGE_URL), ("FL_BRIDGE_SECRET", BRIDGE_SECRET), ("BSKY_HANDLE", BSKY_HANDLE), ("BSKY_APP_PASSWORD", BSKY_PW)) if not v]
    if missing:
        log("Fehlende Secrets: " + ", ".join(missing)); return 2
    log(f"Lauf {RUN_ID} startet (readonly={READONLY})")
    try:
        st = bridge("begin")
    except BridgeUnreachable as e:
        log(f"Hinweis (kein Fehler): {e}. Lauf ausgelassen, der nächste Anstoß holt alles nach.")
        return 0
    if st.get("status") != "ready":
        log(f"Kein Lauf: {st.get('status')} {st.get('reason', '')}"); return 0
    XAI_MODEL = st.get("xai_model") or XAI_MODEL
    al_stats: dict = {"ok": True, "status": "disabled", "errors": []}; uf_res: dict = {"status": "skipped"}
    af_stats: dict = {"ok": True, "status": "skipped", "errors": []}
    al = None; af = None; ran = False; af_ran = False
    timing: dict = {}
    uf = st.get("uf") or {}
    last = uf.get("last_completed") or ""
    uf_age = time.time() - ts(last.replace(" ", "T") + "+00:00") if last else 1e9
    uf_due = bool(uf.get("ready") and uf.get("enabled") and uf_age >= UF_SCAN_MIN * 60)
    uf_first = uf_due and uf_age >= 2 * UF_SCAN_MIN * 60     # deutlich überfällig -> Abgleich vor den Likes
    end_all = MAX_RUNTIME - FINISH_RESERVE
    af_on = bool((st.get("af") or {}).get("ready") and (st.get("af") or {}).get("settings", {}).get("enabled"))
    likes_uf_end = end_all - (AF_BUDGET if af_on else 0)

    def do_uf() -> dict:
        t1 = time.monotonic()
        try:
            return uf_run(bs, uf)
        except Exception as e:  # noqa: BLE001
            return {"status": "error", "message": str(e)[:200]}
        finally:
            timing["entfolger"] = round(time.monotonic() - t1, 1)
    try:
        bs = Bsky()
        if uf_first:
            phase(likes_uf_end - 90)          # mind. 90 s bleiben für die Likes
            uf_res = do_uf()
        if st["settings"].get("enabled"):
            phase(min(LIKES_BUDGET, likes_uf_end) if (uf_due and not uf_first) else likes_uf_end)
            t1 = time.monotonic()
            al = AutoLike(bs, st); al_stats = al.run(); ran = True
            timing["auto_likes"] = round(time.monotonic() - t1, 1); timing["auto_likes_schritte"] = al.t
        if not uf_first:
            phase(likes_uf_end)
            uf_res = do_uf()
        if (st.get("af") or {}).get("ready"):
            phase(end_all)
            t1 = time.monotonic()
            try:
                af = AutoFollow(bs, st); af_stats = af.run(); af_ran = True
            except Exception as e:  # noqa: BLE001
                af_stats = {"ok": False, "status": "error", "errors": [str(e)[:300]]}
            timing["auto_follow"] = round(time.monotonic() - t1, 1)
    except Exception as e:  # noqa: BLE001
        al_stats = {"ok": False, "status": "error", "errors": [str(e)[:300]]}
    finally:
        af_ok = bool(af_stats.get("ok", True)) and not af_stats.get("errors")
        clean = bool(al_stats.get("ok", True)) and not al_stats.get("errors") and uf_res.get("status") != "error" and af_ok
        # "deferred" meldet die Seite als in_progress -> alter Cron sieht den Abgleich weiter als fällig an
        uf_site = dict(uf_res, status="in_progress") if uf_res.get("status") == "deferred" else uf_res
        fin = {"stats": al_stats, "autolike_ran": ran and not READONLY, "uf": uf_site,
               "af": af_stats, "af_ran": af_ran and not READONLY, "clean": clean and not READONLY}
        if al and al.notif_new is not None and not READONLY:
            fin["notif"] = al.notif_new
        if al and al.cache_new is not None and not READONLY:
            fin["thread_cache"] = al.cache_new
        if af and af.cache_new is not None and not READONLY:
            fin["af_cache"] = af.cache_new
        if af and af.new_cursor is not None and not READONLY:
            fin["af_return_cursor"] = af.new_cursor
        bridge("finish", **fin)
    summary = {k: v for k, v in al_stats.items() if k not in ("errors", "hints")}
    log("Auto-Likes: " + json.dumps(summary, ensure_ascii=False))
    if al_stats.get("errors"):
        log("Fehler: " + "; ".join(al_stats["errors"]))
    if al_stats.get("hints"):
        log("Hinweise (kein Fehler): " + "; ".join(al_stats["hints"]))
    log("Entfolger: " + json.dumps({k: v for k, v in uf_res.items() if k != "pending"}, ensure_ascii=False))
    log("Auto-Folgen: " + json.dumps({k: v for k, v in af_stats.items() if k not in ("errors", "hints")}, ensure_ascii=False))
    if af_stats.get("errors"):
        log("Auto-Folgen Fehler: " + "; ".join(af_stats["errors"]))
    if af_stats.get("hints"):
        log("Hinweise (kein Fehler): " + "; ".join(af_stats["hints"]))
    if _AI["calls"]:
        log(f"KI: {_AI['calls']} Aufrufe, Ø {_AI['secs'] / _AI['calls']:.1f} s, {_AI['retries']} Wiederholungen, {AI_WORKERS} parallel")
    log("Zeiten (s): " + json.dumps(timing, ensure_ascii=False) + (" | Abgleich zuerst" if uf_first else ""))
    log(f"Dauer {time.monotonic() - T0:.1f} s")
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
