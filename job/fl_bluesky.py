#!/usr/bin/env python3
"""Feetlove1973 – Bluesky Auto-Likes, Entfolger-Abgleich und Auto-Folgen als GitHub-Actions-Job.

Portierung von bluesky-autolike.php (Stand AUTOLIKE-500-20261113), dem Abgleich aus
bluesky-unfollowers.php und flb_af_run() aus bluesky-autofollow.php. Gleiche Regeln: echte Frage/Beleidigung -> Prüfung,
KI-Check (nur Gemini, inkl. echte_frage; xAI entfernt 15.11.2026), Like-Limit je Lauf (Setting bluesky_autolike_max, Standard 50), nie doppelt liken
(viewer.like wird vor jedem Like geprüft), geblockte/Zweitkonten/stummgeschaltete überspringen, Testmodus (dryrun) wird respektiert.
Auto-Folgen: Liker eigener Posts der letzten N Tage (Standard 7), Limit je Lauf und Tag aus den Admin-Einstellungen, nicht schon gefolgt,
nicht blockiert/stumm, keine Spam-Labels, keine Entfolger-Sperre; bis einschließlich Setting autofollow_reunfollowed_until (Standard 2027-02-14, Europe/Berlin) auch Liker mit reason „von dir entfolgt“; Eintrag in flb_bluesky_autofollow (reason automatisch, followed_at UTC).
Ab 2026-11-14 22:00 Europe/Berlin neue Entfolger-Warteschlangen-Zeilen nur bei Like oder vorhandener Post-URI; bestehende Zeilen vor diesem Stichtag (created_at, UTC) behalten das bisherige Verhalten.
ENTFOLGER-LIKER-20261115: Frühere Entfolger bekommen den sichtbaren Grund AF_REASON_RETURN_LIKED nur, wenn ein Like
(getLikes createdAt, sonst indexedAt) ab 2026-11-15 00:00 Europe/Berlin belegt ist; sonst bleibt der alte Grund (im Admin ausgeblendet).
Nur-Lesen folgt nicht und schreibt nichts. Blockierte und kurze Störungen sind Hinweise, keine Fehler.
Datenbank-Zugriff nur über die geschützte Schnittstelle bluesky-gh-bridge.php (HMAC).
BDSMPOST-20261115: täglicher #BDSM-Textpost (Rubrik „Meine BDSM-Welt“, ab 23:30 Europe/Berlin, ohne Bild/Linkkarte, Standard AUS im Admin).
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
GEMINI_KEY = ENV("GEMINI_API_KEY", "")
GEMINI_BASE = ENV("GEMINI_BASE", "https://generativelanguage.googleapis.com").rstrip("/")
READONLY = ENV("FL_READONLY", "0") == "1"          # nur lesen: keine Likes, kein Entfolgen, kein Folgen, nichts in die DB
UF_SCAN_MIN = int(ENV("FL_UF_SCAN_MIN", "240"))     # Follower-Abgleich höchstens alle X Minuten
MAX_RUNTIME = float(ENV("FL_MAX_RUNTIME", "420"))   # Sicherheitsgrenze je Lauf (Sekunden)
SLEEP_LIKE = float(ENV("FL_SLEEP_LIKE", "0.7"))
LIKES_BUDGET = float(ENV("FL_LIKES_BUDGET", "240"))  # Auto-Likes enden spätestens nach X s, wenn der Abgleich fällig ist
FINISH_RESERVE = float(ENV("FL_FINISH_RESERVE", "25"))  # Rest für Abschluss-Meldung an die Seite
AF_BUDGET = float(ENV("FL_AF_BUDGET", "120"))       # Mindest-Zeitfenster für Auto-Folgen am Laufende (ALLELIKES: 90 -> 120 s)
# ALLELIKES-20261115: Vorgabe "Wer mir einen Like gegeben hat und dem ich nicht folge, wird gefolgt – bei jedem Beitrag".
AF_BACKFILL_POSTS = int(ENV("FL_AF_BACKFILL_POSTS", "150"))     # ältere Posts (außerhalb autofollow_days) je Lauf
AF_BACKFILL_RESERVE = float(ENV("FL_AF_BACKFILL_RESERVE", "50"))  # so viele Sekunden bleiben für Profile + Folgen
AF_VERIFY_MAX = int(ENV("FL_AF_VERIFY_MAX", "300"))             # bekannte Liker (followed/skipped) live nachprüfen je Lauf
AF_VERIFY_RECENT_H = int(ENV("FL_AF_VERIFY_RECENT_H", "48"))    # Likes der letzten X Stunden immer zuerst prüfen
AF_QUEUE_MAX = int(ENV("FL_AF_QUEUE_MAX", "300"))               # Rückstand je Lauf höchstens so viele Zeilen merken
SLEEP_AF = float(ENV("FL_SLEEP_AF", "0.15"))        # Pause zwischen getLikes-/Profil-Seiten
SLEEP_FOLLOW = float(ENV("FL_SLEEP_FOLLOW", "1.5"))  # Pause zwischen Follows (wie PHP usleep 1.5s)
AF_LOCK_RETRIES = max(1, int(ENV("FL_AF_LOCK_RETRIES", "4")))  # af_save-Versuche bei HTTP 409 af_locked
AF_LOCK_SLEEP = float(ENV("FL_AF_LOCK_SLEEP", "3"))            # Pause zwischen den Versuchen (3, 6, 9 s)
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


class BskyHttpError(RuntimeError):
    """HTTP-Fehler von Bluesky mit Status und Fehlercode (abwärtskompatibel zu RuntimeError)."""
    def __init__(self, msg: str, code: int = 0, error: str = "", body: dict | None = None):
        super().__init__(msg)
        self.code = int(code)
        self.error = str(error or "")
        self.body = body or {}




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
            err = str(d.get("error") or "") if isinstance(d, dict) else ""
            last = BskyHttpError(f"{TRANSIENT_PREFIX}{msg} (HTTP {e.code})" if e.code >= 500 else msg, e.code, err, d if isinstance(d, dict) else {})
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


GEMINI_MODEL = ENV("GEMINI_MODEL", "") or "gemini-3.5-flash-lite"
SYSTEM_PROMPT = ('Du prüfst Antworten auf Social-Media-Posts des Bloggers Dennis. negative=true NUR, wenn die Antwort Dennis PERSÖNLICH beleidigt, beschimpft, verhöhnt, bedroht oder belästigt (Beleidigungen, Schimpfwörter gegen ihn, Hass, abwertende Aussagen über seine Person, sexuelle Belästigung, Drohungen). negative=false für alles andere, insbesondere: Kritik an einem Thema, an Firmen (z. B. Google), Produkten, Politik oder Meinungen; Widerspruch in der Sache; Sarkasmus oder Spott über das Thema; Witze, Wortspiele, Insider, Emojis; kurze oder rätselhafte Antworten (z. B. "Hex, Hex"); Zustimmung, Flirt, Neutrales. Im Zweifel negative=false. echte_frage=true NUR, wenn der Antwortende von Dennis wirklich eine Antwort erwartet (eine echte, an ihn gerichtete Frage). echte_frage=false für Witze, Sprüche, rhetorische oder ausrufende Fragen und Scherze mit Fragezeichen (z. B. „… Bluna!?“, „Wieso Der? 🤭“, „Bin ich etwa schon drin oder was?“). Im Zweifel, ob wirklich eine Antwort erwartet wird: echte_frage=true. reason: max. 8 Wörter, deutsch.')


class AiBlocked(RuntimeError):
    """Gemini hat die Prüfung inhaltlich verweigert (Safety/PROHIBITED_CONTENT) – kein Lauf-Fehler, Antwort zur Handprüfung."""


GEMINI_SAFETY = [{"category": c, "threshold": "BLOCK_NONE"} for c in (
    "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH", "HARM_CATEGORY_DANGEROUS_CONTENT")]
GEMINI_SCHEMA = {"type": "OBJECT", "properties": {"negative": {"type": "BOOLEAN"}, "echte_frage": {"type": "BOOLEAN"}, "reason": {"type": "STRING"}},
                 "required": ["negative", "echte_frage", "reason"]}


def gemini_payload(text: str, parent: str) -> dict:
    return {"systemInstruction": {"parts": [{"text": SYSTEM_PROMPT + " Gib ausschließlich ein JSON-Objekt mit negative, echte_frage und reason zurück."}]},
            "contents": [{"role": "user", "parts": [{"text": "Ursprungspost (Kontext):\n" + parent[:500] + "\n\nAntwort:\n" + text[:1000]}]}],
            "generationConfig": {"temperature": 0, "maxOutputTokens": 2048, "responseMimeType": "application/json", "responseSchema": GEMINI_SCHEMA},
            "safetySettings": GEMINI_SAFETY}


def gemini_error_text(code: int, raw: str) -> str:
    try:
        err = (json.loads(raw or "{}") or {}).get("error") or {}
    except ValueError:
        err = {}
    status = str(err.get("status") or "") if isinstance(err, dict) else ""
    msg = (str(err.get("message") or "") if isinstance(err, dict) else "").lower()
    if code == 429 and ("quota" in msg or status == "RESOURCE_EXHAUSTED"):
        return f"Gemini HTTP {code} (Kontingent erreicht)"
    if code in (401, 403) or status in ("PERMISSION_DENIED", "UNAUTHENTICATED") or "api key" in msg:
        return f"Gemini HTTP {code} (Key ungültig oder ohne Freigabe)"
    if code == 404:
        return f"Gemini HTTP 404 (Modell {GEMINI_MODEL} nicht gefunden)"
    return f"Gemini HTTP {code}" + (f" ({status})" if status else "")


def gemini_parse(d: dict) -> tuple[bool, bool, str]:
    """Antwort von generateContent auswerten. AiBlocked bei Safety-Sperre, RuntimeError bei unklarer Antwort."""
    pf = (d.get("promptFeedback") or {}).get("blockReason")
    if pf:
        raise AiBlocked(f"Gemini blockiert ({pf})")
    cand = (d.get("candidates") or [{}])[0] or {}
    fr = str(cand.get("finishReason") or "")
    if fr in ("SAFETY", "PROHIBITED_CONTENT", "BLOCKLIST", "SPII", "RECITATION"):
        raise AiBlocked(f"Gemini blockiert ({fr})")
    txt = "".join(str(p.get("text") or "") for p in ((cand.get("content") or {}).get("parts") or []) if isinstance(p, dict) and not p.get("thought"))
    try:
        r = json.loads(txt.strip() or "")
    except ValueError:
        r = None
    if not isinstance(r, dict) or "negative" not in r or "echte_frage" not in r:
        raise RuntimeError(f"KI-Antwort unklar ({fr or '-'})")
    return bool(r["negative"]), bool(r["echte_frage"]), str(r.get("reason", "")).strip()[:120]


def ai_check(text: str, parent: str, deadline: float | None = None) -> tuple[bool, bool, str]:
    if not GEMINI_KEY:
        raise RuntimeError("Gemini-Key fehlt (Secret GEMINI_API_KEY)")
    body = json.dumps(gemini_payload(text, parent), ensure_ascii=False).encode()
    url = GEMINI_BASE + "/v1beta/models/" + urllib.parse.quote(GEMINI_MODEL, safe="") + ":generateContent"
    t_end = time.monotonic() + (deadline if deadline is not None else 90)
    d: dict = {}
    for attempt in range(4):
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_KEY}, method="POST")
        t1 = time.monotonic(); wait_s = 0.0
        try:
            with urllib.request.urlopen(req, timeout=max(3.0, min(30.0, t_end - t1))) as r:
                d = json.loads(r.read() or b"{}")
            break
        except urllib.error.HTTPError as e:
            raw = e.read().decode(errors="replace")
            fail = RuntimeError(gemini_error_text(e.code, raw))
            if "Kontingent" in str(fail) or (e.code != 429 and e.code < 500):
                raise fail
            try:
                wait_s = float(e.headers.get("Retry-After") or 0)
            except (TypeError, ValueError):
                wait_s = 0.0
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            fail = RuntimeError("Gemini HTTP 0")
        finally:
            with _AI_LOCK:
                _AI["calls"] += 1; _AI["secs"] += time.monotonic() - t1
        wait_s = min(max(wait_s, 1.5 * (2 ** attempt)), 20.0)
        if attempt == 3 or time.monotonic() + wait_s + 5 > t_end:
            raise fail
        with _AI_LOCK:
            _AI["retries"] += 1
        time.sleep(wait_s)
    return gemini_parse(d)


def needs_ai(text: str) -> bool:
    # Fragen laufen jetzt durch die KI (echte_frage). Nur klare Beleidigungen brauchen keinen KI-Aufruf.
    return not insult_hit(text)


def classify(text: str, parent: str, deadline: float | None = None) -> tuple[str, str]:
    w = insult_hit(text)
    if w:
        return "review", f"Beleidigend: „{w}“"
    try:
        neg, echte_frage, why = ai_check(text, parent, deadline)
    except AiBlocked as e:
        if is_question(text):
            return "review", "Frage"
        return "review", str(e) + " – bitte selbst ansehen"
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

UF_GONE = {"deleted", "takendown", "missing"}
UF_WEEKLY = {"deactivated", "suspended"}
UF_GONE_RETRY = 86400
UF_GONE_MSG = "Konto gelöscht – Follow entfernt"
UF_GONE_NONE = "Konto gelöscht – kein Follow mehr"


def account_status(did: str) -> str:
    try:
        r = http(RELAY + "/xrpc/com.atproto.sync.getRepoStatus?did=" + urllib.parse.quote(did, safe=""), {"Accept": "application/json"})
    except BskyHttpError as e:
        if e.code == 404 or e.error == "RepoNotFound":
            return "missing"
        raise
    if r.get("did") != did or not isinstance(r.get("active"), bool):
        raise RuntimeError("Kontostatus nicht sicher prüfbar.")
    if r["active"]:
        if r.get("status"):
            raise RuntimeError("Kontostatus derzeit eingeschränkt.")
        return "active"
    return r.get("status") if r.get("status") in ("deactivated", "deleted", "suspended", "takendown") else "inactive"


def profile_status(bs: "Bsky", did: str) -> tuple[str, dict | None]:
    """Aktives Profil oder gone/deactivated-Status. Fehlende DIDs in getProfiles gelten als missing."""
    try:
        prof = (bs.get("app.bsky.actor.getProfiles", "actors=" + urllib.parse.quote(did, safe="")).get("profiles") or [])
    except BskyHttpError as e:
        err, msg = e.error, str(e).lower()
        if err == "AccountTakedown" or "accounttakedown" in msg:
            return "takendown", None
        if err == "AccountDeactivated" or "accountdeactivated" in msg:
            return "deactivated", None
        if "profile not found" in msg:
            return "missing", None
        raise
    p = next((x for x in prof if isinstance(x, dict) and x.get("did") == did), None)
    if not p:
        return "missing", None
    return "active", p


def uf_gone_first(row: dict, status: str) -> int:
    """Unixzeit der ersten Gone-Bestätigung, 0 wenn noch keine vorliegt."""
    if status not in UF_GONE:
        return 0
    first = int(row.get("gone_first_seen") or 0)
    if first <= 0:
        prev = str(row.get("account_status") or "")
        checked = str(row.get("checked_at") or "")
        if prev in UF_GONE and checked:
            first = ts(checked.replace(" ", "T") + "+00:00")
    return first if first > 0 else 0


def uf_gone_ready(row: dict, status: str, now: float | None = None) -> bool:
    first = uf_gone_first(row, status)
    return first > 0 and ((time.time() if now is None else now) - first) >= UF_GONE_RETRY


def uf_bridge(typ: str, **kw) -> dict:
    """uf_event; unbekannte Typen einer älteren Brücke nicht als Fehler behandeln."""
    try:
        return bridge("uf_event", type=typ, **kw)
    except RuntimeError as e:
        if typ == "blocked" and "bad_type" in str(e):
            return {"status": "legacy_skip"}
        if typ in ("gone_cleared", "gone_wait") and "bad_type" in str(e):
            if typ == "gone_wait":
                kw = dict(kw)
                kw.pop("type", None)
                try:
                    return bridge("uf_event", type="inactive", **kw)
                except RuntimeError:
                    return {"status": "legacy_skip"}
            return {"status": "legacy_skip"}
        raise


class FollowIndex:
    """Did → rkey der eigenen Follow-Datensätze, einmal je Lauf per listRecords aufgebaut."""

    def __init__(self, bs: "Bsky"):
        self.bs = bs
        self.by_did: dict[str, str] = {}
        self.complete = False
        self.loaded = False

    def ensure(self) -> None:
        if self.loaded:
            return
        cursor, seen = "", set()
        prefix = f"at://{self.bs.did}/app.bsky.graph.follow/"
        for _ in range(1000):
            if left() < 25:
                break
            q = {"repo": self.bs.did, "collection": "app.bsky.graph.follow", "limit": 100}
            if cursor:
                q["cursor"] = cursor
            d = self.bs.get("com.atproto.repo.listRecords", q)
            for rec in d.get("records") or []:
                if not isinstance(rec, dict):
                    continue
                val = rec.get("value") or {}
                subj = val.get("subject") or ""
                if isinstance(subj, dict):
                    subj = str(subj.get("did") or "")
                uri = str(rec.get("uri") or "")
                subj = str(subj)
                if not subj or not uri.startswith(prefix):
                    continue
                key = uri[len(prefix):]
                if key and key not in (".", ".."):
                    self.by_did[subj] = key
            cursor = d.get("cursor") or ""
            if not cursor:
                self.complete = True
                break
            if not isinstance(cursor, str) or cursor in seen:
                break
            seen.add(cursor)
        self.loaded = True

    def rkey(self, did: str) -> str | None:
        self.ensure()
        return self.by_did.get(did)


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
    follows = FollowIndex(bs)
    for row in pending[:10]:
        if left() < 30:
            stopped = "Laufzeitgrenze"; break
        did, eid = row["did"], row["id"]
        base = {"id": eid, "did": did, "actor": bs.did, "handle": row.get("handle", ""), "display_name": row.get("display_name", "")}
        try:
            st = account_status(did)
            p = None
            if st == "active":
                st, p = profile_status(bs, did)
            if st in UF_GONE:
                if uf_gone_ready(row, st):
                    rkey = follows.rkey(did)
                    if rkey:
                        g = uf_bridge("pre_delete", follow_uri=f"at://{bs.did}/app.bsky.graph.follow/{rkey}", reason=UF_GONE_MSG, **base)
                        if g.get("status") == "day_limit":
                            stopped = "Tageslimit von 100 Entfolgen-Versuchen erreicht"; break
                        if g.get("status") != "go":
                            continue
                        bs.post("com.atproto.repo.deleteRecord", {"repo": bs.did, "collection": "app.bsky.graph.follow", "rkey": rkey})
                        uf_bridge("deleted", message=UF_GONE_MSG, reason=UF_GONE_MSG, **base); count += 1
                    elif follows.complete:
                        uf_bridge("gone_cleared", message=UF_GONE_NONE, **base)
                else:
                    uf_bridge("inactive", account_status=st, **base)
                continue
            if st != "active":
                uf_bridge("inactive", account_status=st, **base); continue
            if not p or not isinstance(p.get("viewer"), dict):
                raise RuntimeError("Profilbeziehung nicht sicher prüfbar.")
            v = p["viewer"]
            if v.get("blocking") or v.get("blockedBy") or v.get("blockingByList"):
                # Blockierung ist kein Fehler: eigener Status, keine stündliche Wiederholung, keine rote Ampel
                blocked += 1; log("Entfolgen-Prüfung: blockierte Beziehung übersprungen (kein Fehler).")
                uf_bridge("blocked", **base)
                continue
            base.update(handle=str(p.get("handle") or base["handle"]), display_name=str(p.get("displayName") or base["display_name"]))
            if v.get("followedBy"):
                uf_bridge("returned", **base); continue
            if not v.get("following"):
                uf_bridge("not_following", **base); continue
            g = uf_bridge("pre_delete", follow_uri=str(v["following"]), **base)   # Ausschluss wird VOR dem Löschen gespeichert
            if g.get("status") == "day_limit":
                stopped = "Tageslimit von 100 Entfolgen-Versuchen erreicht"; break
            if g.get("status") != "go":
                continue
            bs.post("com.atproto.repo.deleteRecord", {"repo": bs.did, "collection": "app.bsky.graph.follow", "rkey": g["rkey"]})
            uf_bridge("deleted", **base); count += 1
        except RateLimit:
            errors += 1
            try:
                uf_bridge("error", **base)
            except Exception:  # noqa: BLE001
                pass
            stopped = "Bluesky-Rate-Limit"; break
        except Exception as e:  # noqa: BLE001
            errors += 1; log(f"Entfolgen-Prüfung Fehler: {str(e)[:120]}")
            try:
                uf_bridge("error", **base)
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


AF_BF_KEY = "__backfill__"   # Fortschritt der Alt-Post-Runde im Auto-Folgen-Cache
AF_VF_KEY = "__verify__"     # Rotations-Cursor der Nachprüfung bekannter Liker
AF_VD_KEY = "__verified__"   # CACHELIKE: zuletzt als "dailyfun folgt" bestätigt (did -> Zeit), TTL AF_VERIFY_TTL
AF_VERIFY_TTL = int(ENV("FL_AF_VERIFY_TTL", "21600"))
AF_MS_KEY = "__manual_skip__"  # seit wann ein Konto "von Hand übersprungen" ist (erstmals in manual_skip gesehen)


def af_state(cache: dict, key: str) -> dict:
    v = cache.get(key)
    return dict(v) if isinstance(v, dict) else {}


def af_feed_post_ok(item: dict, me: str) -> dict | None:
    p = item.get("post") or {}
    if (p.get("author") or {}).get("did") != me or item.get("reason"):
        return None
    return p


def af_verify_order(cands: dict, like_ts: dict, after: str, recent_h: int, now: float | None = None) -> list[str]:
    """Erst Likes der letzten recent_h Stunden (neueste zuerst), dann alle übrigen rotierend ab Cursor 'after'."""
    now = time.time() if now is None else now
    lim = now - recent_h * 3600
    recent = [d for d in cands if float(like_ts.get(d) or 0) >= lim]
    recent.sort(key=lambda d: float(like_ts.get(d) or 0), reverse=True)
    rs = set(recent)
    rest = sorted(d for d in cands if d not in rs)
    rest = [d for d in rest if d > after] + [d for d in rest if d <= after]
    return recent + rest


def af_cache_likers(row) -> dict | None:
    """CACHELIKE: Post-Cache-Eintrag [Zähler, Zeit, {did: Like-Zeit ab Stichtag}] – ältere Einträge ohne Liker-Map -> None."""
    if isinstance(row, (list, tuple)) and len(row) >= 3 and isinstance(row[2], dict):
        return row[2]
    return None


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


# Like-Pflicht für neue Entfolger-Einträge; created_at in der Tabelle ist UTC.
AF_LIKE_CUTOFF = datetime(2026, 11, 14, 22, 0, tzinfo=ZoneInfo("Europe/Berlin"))
AF_POST_URI_RE = re.compile(r"^at://[^/]+/app\.bsky\.feed\.post/[A-Za-z0-9._~-]+$")


def af_like_cutoff_utc() -> datetime:
    return AF_LIKE_CUTOFF.astimezone(timezone.utc)


def af_has_post_uri(src: str) -> bool:
    return bool(src and AF_POST_URI_RE.match(src.strip()))


def af_created_before_like_cutoff(created: str) -> bool:
    """True bei fehlendem oder unlesbarem created_at (Bestand, alte Regel) oder Zeitpunkt vor dem Stichtag."""
    raw = (created or "").strip()
    if not raw:
        return True
    m = re.match(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", raw)
    if not m:
        return True
    try:
        dt = datetime.strptime(m.group(1) + " " + m.group(2), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    return dt < af_like_cutoff_utc()


# Frühere Entfolger: sichtbar nur mit Like ab diesem Zeitpunkt.
AF_RETURN_LIKE_SINCE = datetime(2026, 11, 15, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
AF_REASON_RETURN_WAIT = "Früherer Entfolger: wartet auf erneutes Folgen"
AF_REASON_RETURN_LIKED = "Früherer Entfolger: hat seit 15.11. geliked, wartet auf erneutes Folgen"


def af_parse_ts(raw: str) -> datetime | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        m = re.match(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", raw)
        if not m:
            return None
        try:
            dt = datetime.strptime(m.group(1) + " " + m.group(2), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def af_like_time(lk: dict) -> datetime | None:
    return af_parse_ts(str((lk or {}).get("createdAt") or "")) or af_parse_ts(str((lk or {}).get("indexedAt") or ""))


def af_liked_since_return_cutoff(ts: datetime | None) -> bool:
    return ts is not None and ts >= AF_RETURN_LIKE_SINCE.astimezone(timezone.utc)


def af_may_enqueue_return(did: str, likers: dict[str, str], created: dict[str, str], uris: dict[str, str], known: dict[str, str]) -> bool:
    if did in likers:
        return True
    src = uris.get(did) or ""
    if af_has_post_uri(src):
        return True
    if did not in known:
        return False
    return af_created_before_like_cutoff(created.get(did, ""))


AF_REASON_DAY_LIMIT = "Tageslimit erreicht"
AF_REASON_RUN_LIMIT = "Lauf-Limit – nächster Lauf"


def af_is_capacity_reason(reason: str) -> bool:
    return reason in (AF_REASON_DAY_LIMIT, AF_REASON_RUN_LIMIT) or reason.startswith("Zeitbudget")


def af_leftover_reason(max_day: int, today: int, followed_this_run: int) -> str:
    return AF_REASON_DAY_LIMIT if (today + followed_this_run) >= max_day else AF_REASON_RUN_LIMIT


def af_is_locked_error(exc: BaseException) -> bool:
    s = str(exc)
    return "HTTP 409" in s and "af_locked" in s


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
        self.created = {str(k): str(v or "") for k, v in (self.af.get("created_at") or {}).items()}
        self.uris = {str(k): str(v or "") for k, v in (self.af.get("source_uris") or {}).items()}
        # ALLELIKES: von Hand übersprungene Konten (Tageszahlen) bleiben ausgenommen. Ältere Bridge ohne Liste ->
        # skipped-Zeilen gar nicht nachprüfen (sicher), nur followed.
        ms = self.af.get("manual_skip")
        self.manual_skip: set[str] | None = set(str(x) for x in ms) if isinstance(ms, list) else None
        # Alte Bridge (ohne manual_skip): skipped nur nachprüfen, wenn die Zeile in recheck steht – die Bridge schickt dort
        # nur Gründe 'automatisch','folgst bereits','von Hand gefolgt','manuell', also nie "von Hand übersprungen".
        self.skip_ok_fallback = set(str(x) for x in (self.af.get("recheck") or []))
        # SKIPLIKE-20261115 (Dennis 10:04): "Ein neuer Like hebt das Überspringen auf." Die Bridge liefert keinen
        # Überspringen-Zeitpunkt; der Job merkt sich, seit wann ein Konto in manual_skip steht. Beim ersten Lauf gilt für
        # alle bereits übersprungenen der Stichtag 15.11. 00:00 Berlin, später übersprungene ab dem Lauf, der sie zuerst sieht.
        self.confirmed: set[str] = set()
        vd0 = af_state(self.cache, AF_VD_KEY)
        self.vd: dict[str, int] = {str(k): int(v) for k, v in vd0.items()
                                   if isinstance(v, (int, float)) and time.time() - float(v) <= AF_VERIFY_TTL}
        self.ms_since: dict[str, float] = {}
        self.ms_state: dict | None = None
        if self.manual_skip is not None:
            prev_ms = af_state(self.cache, AF_MS_KEY)
            seen = prev_ms.get("m") if isinstance(prev_ms.get("m"), dict) else {}
            init = bool(prev_ms.get("init"))
            base = AF_RETURN_LIKE_SINCE.timestamp()
            nowt = float(int(time.time()))
            for d in self.manual_skip:
                v = seen.get(d)
                self.ms_since[d] = float(v) if isinstance(v, (int, float)) else (nowt if init else base)
            self.ms_state = {"init": 1, "m": {d: int(t) for d, t in self.ms_since.items()}}
        self.like_ts: dict[str, float] = {}   # did -> jüngster Like (Unix-Zeit)
        self.cache_new: dict | None = None
        self.new_cursor: str | None = None
        self.stats: dict = {"ok": True, "dryrun": self.dry, "posts": 0, "likers": 0, "followed": 0,
                            "would_follow": 0, "skipped": 0, "blocked_new": 0, "today": self.today, "errors": []}

    def flush(self) -> None:
        if READONLY or not self.ops:
            self.ops = []
            return
        last = None
        for attempt in range(AF_LOCK_RETRIES):
            try:
                bridge("af_save", ops=self.ops)
                self.ops = []
                return
            except Exception as e:  # noqa: BLE001
                if not af_is_locked_error(e):
                    raise
                last = e
                if attempt < AF_LOCK_RETRIES - 1:
                    delay = AF_LOCK_SLEEP * (attempt + 1)
                    if delay > 0:
                        time.sleep(delay)
        has_follow = any(op.get("t") == "save" and op.get("followed_now") for op in self.ops)
        if has_follow:
            raise last  # type: ignore[misc]
        af_hint(self.stats, "Speichern kurz gesperrt (af_locked) – nächster Lauf holt den Rest nach.")
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

    def add_liker(self, did: str, uri: str, t: float, likers: dict, liked_recent: dict) -> None:
        if did not in likers:
            likers[did] = uri
        if t > self.like_ts.get(did, 0.0):
            self.like_ts[did] = t
        if did not in liked_recent:
            liked_recent[did] = uri

    def post_likers(self, uri: str, likers: dict, liked_recent: dict, per: dict | None = None) -> tuple[bool, bool]:
        """Alle Liker eines Posts (Pagination, max. 20 Seiten à 100). Rückgabe (vollständig, Zeitgrenze)."""
        st = self.stats
        cursor, pages = None, 0
        while True:
            if left() < 8:
                return False, True
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
                return False, False
            for lk in d.get("likes") or []:
                actor = (lk or {}).get("actor") or {}
                did = str(actor.get("did") or "")
                if not did or did == self.bs.did:
                    continue
                lt = af_like_time(lk)
                # ALLELIKES "Ab heute" (Dennis 15.11.2026): nur Likes ab AF_RETURN_LIKE_SINCE (15.11. 00:00 Berlin) zählen,
                # egal auf welchem Post. Ältere Likes (oder ohne Zeitstempel) lösen nichts aus.
                if not af_liked_since_return_cutoff(lt):
                    continue
                self.add_liker(did, uri, lt.timestamp(), likers, liked_recent)
                if per is not None and int(lt.timestamp()) > int(per.get(did) or 0):
                    per[did] = int(lt.timestamp())
            cursor = d.get("cursor")
            pages += 1
            if SLEEP_AF:
                time.sleep(SLEEP_AF)
            if not cursor or pages >= 20:
                return True, False

    def backfill(self, likers: dict, liked_recent: dict) -> None:
        """ALLELIKES: ältere Posts außerhalb des autofollow_days-Fensters in Scheiben je Lauf (Cursor + Position im Cache),
        damit über mehrere Läufe JEDER Beitrag erfasst wird. Danach beginnt die Runde von vorn (neue Likes auf alten Posts)."""
        st = self.stats
        bf = af_state(self.cache, AF_BF_KEY)
        cursor = str(bf.get("cursor") or "")
        skip = int(bf.get("skip") or 0)
        cutoff = time.time() - self.days * 86400
        calls = 0
        done_posts = 0
        while calls < AF_BACKFILL_POSTS and left() > AF_BACKFILL_RESERVE:
            q = {"actor": self.bs.did, "limit": 100, "filter": "posts_and_author_threads"}
            if cursor:
                q["cursor"] = cursor
            d = self.bs.get("app.bsky.feed.getAuthorFeed", q)
            feed = d.get("feed") or []
            stopped_at = -1
            for idx, item in enumerate(feed):
                if idx < skip:
                    continue
                p = af_feed_post_ok(item, self.bs.did)
                if p is None:
                    continue
                lc = int(p.get("likeCount") or 0)
                uri = str(p.get("uri") or "")
                pt = ts(str((p.get("record") or {}).get("createdAt") or "")) or 0
                if lc == 0 or not uri or pt >= cutoff:
                    continue   # ohne Likes oder schon im Fenster der letzten Tage erfasst
                if calls >= AF_BACKFILL_POSTS or left() <= AF_BACKFILL_RESERVE:
                    stopped_at = idx
                    break
                calls += 1
                ok, cut = self.post_likers(uri, likers, liked_recent)
                if not ok:
                    stopped_at = idx
                    if cut:
                        st["time_limit"] = True
                    break
                done_posts += 1
            if stopped_at >= 0:
                skip = stopped_at          # selbe Seite, ab diesem Post weiter
                break
            nxt = d.get("cursor")
            skip = 0
            if not nxt or not feed:
                cursor = ""
                bf["pass"] = int(bf.get("pass") or 0) + 1
                bf["pass_done_at"] = int(time.time())
                bf["pass_posts"] = int(bf.get("pass_posts_run") or 0) + done_posts
                bf["pass_posts_run"] = 0
                done_posts = 0
                break
            cursor = nxt
        bf["cursor"] = cursor
        bf["skip"] = skip
        bf["pass_posts_run"] = int(bf.get("pass_posts_run") or 0) + done_posts
        self.bf_state = bf
        st["backfill"] = {"posts": calls, "pass": int(bf.get("pass") or 0), "pass_posts": int(bf.get("pass_posts_run") or 0)}

    def verify_known(self, likers: dict, todo: dict, profiles: dict) -> None:
        """ALLELIKES: Liker mit Zeile followed/skipped live prüfen. Folgt dailyfun ihnen nicht (mehr), kommen sie in todo.
        Ausgenommen: von Hand übersprungen, blocked (Entfolger, von dir entfolgt), Kapazitätszeilen (schon in pending)."""
        st = self.stats
        cands = {}
        for did, src in likers.items():
            if did in todo:
                continue
            prev = self.known.get(did)
            if prev == "skipped":
                if self.manual_skip is None:
                    ok = did in self.skip_ok_fallback
                elif did in self.manual_skip:
                    # von Hand übersprungen: nur ein Like NACH dem Überspringen hebt es auf
                    ok = float(self.like_ts.get(did) or 0) > self.ms_since.get(did, AF_RETURN_LIKE_SINCE.timestamp())
                else:
                    ok = True
            else:
                ok = prev == "followed"
            if ok:
                cands[did] = src
        if not cands or AF_VERIFY_MAX <= 0 or left() < AF_BACKFILL_RESERVE / 2:
            return
        nowt = time.time()
        cands = {d: v for d, v in cands.items() if nowt - float(self.vd.get(d) or 0) > AF_VERIFY_TTL}
        if not cands:
            return
        vf = af_state(self.cache, AF_VF_KEY)
        order = af_verify_order(cands, self.like_ts, str(vf.get("after") or ""), AF_VERIFY_RECENT_H)
        # CACHELIKE: übersprungene zuerst (dort ist "dailyfun folgt nicht" wahrscheinlich), dann gefolgte; Rest per TTL im nächsten Lauf
        order = ([d for d in order if self.known.get(d) == "skipped"] + [d for d in order if self.known.get(d) != "skipped"])[:AF_VERIFY_MAX]
        pp = af_profiles(self.bs, order)
        n_new = 0
        for did in order:
            p = pp.get(did)
            if not p:
                continue
            profiles[did] = p
            if not (p.get("viewer") or {}).get("following"):
                todo[did] = cands[did]
                n_new += 1
            else:
                self.confirm(did)
        rest = [d for d in order if float(self.like_ts.get(d) or 0) < time.time() - AF_VERIFY_RECENT_H * 3600]
        if rest:
            vf["after"] = max(rest)
        self.vf_state = vf
        st["verified"] = len(order)
        st["verify_not_following"] = n_new

    def confirm(self, did: str) -> None:
        """dailyfun folgt dem Konto (bestätigt) -> nicht mehr im Liker-Cache halten, TTL für die Nachprüfung."""
        self.confirmed.add(did)
        self.vd[did] = int(time.time())

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
            liked_recent: dict[str, str] = {}  # did -> Post-URI eines Likes ab AF_RETURN_LIKE_SINCE
            new_cache: dict = {}
            time_limit = False
            for p in af_own_posts(self.bs, self.days):
                lc = int(p.get("likeCount") or 0)
                uri = str(p.get("uri") or "")
                if lc == 0 or not uri:
                    continue
                cached_map = af_cache_likers(self.cache.get(uri))
                if cached_map is not None and not af_cache_stale(self.cache, uri, lc):
                    # CACHELIKE: Liker ab Stichtag stehen im Cache -> zählen auch ohne frisches Einlesen
                    for cd, ct in cached_map.items():
                        if isinstance(ct, (int, float)) and cd != self.bs.did:
                            self.add_liker(str(cd), uri, float(ct), likers, liked_recent)
                    new_cache[uri] = [self.cache[uri][0], self.cache[uri][1], dict(cached_map)]
                    continue
                if left() < 8:
                    time_limit = True
                    break
                st["posts"] += 1
                per: dict = {}
                ok_post, cut = self.post_likers(uri, likers, liked_recent, per)
                if cut:
                    time_limit = True
                if ok_post:
                    new_cache[uri] = [lc, int(time.time()), per]
            recent_likers = dict(likers)
            bf_likers: dict[str, str] = {}
            self.bf_state = af_state(self.cache, AF_BF_KEY)
            self.vf_state = af_state(self.cache, AF_VF_KEY)
            if not time_limit:
                self.backfill(bf_likers, liked_recent)
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
            def take_new(src_map: dict) -> None:
                for did, src in src_map.items():
                    if did in todo:
                        continue
                    prev = self.known.get(did)
                    if prev is None or (prev == "would_follow" and not self.dry) or af_is_reunfollowed_candidate(prev, did, self.reunfollowed, self.reunfollowed_active):
                        todo[did] = src
            take_new(recent_likers)
            pre_profiles: dict = {}
            all_likers = {**bf_likers, **recent_likers}
            self.verify_known(all_likers, todo, pre_profiles)
            take_new(bf_likers)
            likers = all_likers
            # Frühere Entfolger in rotierenden Stapeln. Ab 2026-11-14 22:00 Europe/Berlin
            # neue Warteschlangen-Zeilen nur bei Like oder Post-URI; Bestand vor dem Stichtag bleibt.
            return_dids = sorted(self.excluded)
            if return_dids:
                after = self.return_cursor
                batch = [d for d in return_dids if d > after]
                if not batch:
                    batch = return_dids
                batch = batch[:50]
                returns: dict[str, str] = {}
                # Frühere Entfolger mit Like ab AF_RETURN_LIKE_SINCE unabhängig vom Stapel prüfen.
                for d in return_dids:
                    if d in liked_recent:
                        returns[d] = todo.get(d) or liked_recent[d]
                for d in batch:
                    if d in todo:
                        returns[d] = todo[d]
                        continue
                    if not af_may_enqueue_return(d, likers, self.created, self.uris, self.known):
                        continue
                    returns[d] = likers.get(d) or self.uris.get(d) or ""
                todo = {**returns, **todo}
                if not self.dry:
                    self.new_cursor = batch[-1]
            st["likers"] = len(likers)
            missing = [d for d in todo if d not in pre_profiles]
            profiles = {**pre_profiles, **(af_profiles(self.bs, missing) if missing else {})}
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
                    if did in liked_recent:
                        self.save(did, p, liked_recent[did], "blocked", AF_REASON_RETURN_LIKED)
                    else:
                        self.add({"t": "mark_blocked", "did": did, "reason": AF_REASON_RETURN_WAIT})
                    st["skipped"] += 1
                    continue
                if v.get("following"):
                    self.save(did, p, src, "skipped", "folgst bereits", str(v.get("following") or ""))
                    self.confirm(did)
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
                    leftover_reason = af_leftover_reason(self.max_day, self.today, int(st.get("followed") or 0))
                    if leftover_reason == AF_REASON_DAY_LIMIT:
                        st["limit_reached"] = True
                    else:
                        st["run_limit"] = True
                    self.save(did, p, src, "would_follow", leftover_reason)
                    st["would_follow"] += 1
                    # restliche ebenfalls merken, damit die Tageszahlen-Liste sie zeigt
                    idx = list(todo.keys()).index(did)
                    rest_all = list(todo.keys())[idx + 1:]
                    # ALLELIKES: Rückstand gedrosselt merken; Rest findet der nächste Lauf/die nächste Runde wieder
                    for rest_did in rest_all[:AF_QUEUE_MAX]:
                        rp = profiles.get(rest_did) or {"handle": "", "displayName": ""}
                        self.save(rest_did, rp, todo[rest_did], "would_follow", leftover_reason)
                        st["would_follow"] += 1
                    st["backlog"] = 1 + len(rest_all)
                    break
                if left() < 8:
                    st["time_limit"] = True
                    leftover_reason = "Zeitbudget – nächster Lauf"
                    self.save(did, p, src, "would_follow", leftover_reason)
                    st["would_follow"] += 1
                    idx = list(todo.keys()).index(did)
                    rest_all = list(todo.keys())[idx + 1:]
                    # ALLELIKES: Rückstand gedrosselt merken; Rest findet der nächste Lauf/die nächste Runde wieder
                    for rest_did in rest_all[:AF_QUEUE_MAX]:
                        rp = profiles.get(rest_did) or {"handle": "", "displayName": ""}
                        self.save(rest_did, rp, todo[rest_did], "would_follow", leftover_reason)
                        st["would_follow"] += 1
                    st["backlog"] = 1 + len(rest_all)
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
                self.confirm(did)
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
            if self.cache_new is not None:
                bfs = getattr(self, "bf_state", None)
                vfs = getattr(self, "vf_state", None)
                if isinstance(bfs, dict):
                    self.cache_new[AF_BF_KEY] = bfs
                if isinstance(vfs, dict):
                    self.cache_new[AF_VF_KEY] = vfs
                if isinstance(self.ms_state, dict):
                    self.cache_new[AF_MS_KEY] = self.ms_state
                self.cache_new[AF_VD_KEY] = dict(self.vd)
                if self.confirmed:
                    for k, row in list(self.cache_new.items()):
                        m = af_cache_likers(row)
                        if m:
                            self.cache_new[k] = [row[0], row[1], {d: t for d, t in m.items() if d not in self.confirmed}]
            try:
                self.flush()
            except Exception as e:  # noqa: BLE001
                # finally darf nicht werfen, sonst fehlen die Laufzahlen in der Auswertung
                if af_is_locked_error(e):
                    has_follow = any(op.get("t") == "save" and op.get("followed_now") for op in self.ops)
                    if has_follow:
                        st["ok"] = False
                        af_err(st, str(e))
                    else:
                        af_hint(st, "Speichern kurz gesperrt (af_locked) – nächster Lauf holt den Rest nach.")
                else:
                    st["ok"] = False
                    af_err(st, str(e))
                self.ops = []
        if not st.get("status"):
            st["status"] = "readonly" if READONLY else ("dryrun" if self.dry else "ok")
        return st


# ---------------- Ablauf ----------------

# ---------------- BDSM-Tagespost (BDSMPOST-20261115) ----------------
# Einmal täglich ein reiner Textpost aus der Rubrik „Meine BDSM-Welt“: erste Zeile „#BDSM“ (Stummschalten), Ü18-Hinweis,
# Überschrift, 1–2 Sätze, Link (Facet), 2–3 Hashtags. KEIN Bild, KEINE Linkkarte (kein embed). Auswahl, Text und Sperre
# kommen von der Seite (bp_pick); hier nur Prüfen, Facets bauen, Posten und Melden (bp_done/bp_fail).
# Frühestens ab 23:30 Europe/Berlin -> praktisch im Lauf um 23:41. Ein/Aus auf der Tageszahlen-Seite (Standard AUS).
BP_SLOT = ENV("FL_BP_SLOT", "23:30")
BP_ON = ENV("FL_BP", "1") != "0"                 # Notschalter im Job (Standard an; der eigentliche Schalter ist im Admin)
BP_PENDING_TTL = int(ENV("FL_BP_PENDING_TTL", "1200"))
BP_FIRST_LINE = "#BDSM"
_BP_TAG = re.compile(r"(?<![\w#])#([^\s#]+)")


def bp_in_slot(now: datetime | None = None) -> bool:
    n = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo("Europe/Berlin"))
    return n.strftime("%H:%M") >= BP_SLOT


def bp_check(pick: dict) -> str:
    """Leerer String = in Ordnung, sonst Grund, warum NICHT gepostet wird."""
    t = str(pick.get("text") or "")
    if not t.startswith(BP_FIRST_LINE + "\n"):
        return "erste Zeile ist nicht #BDSM"
    if len(t) > 300:                                   # Codepunkte >= Grapheme -> sichere Obergrenze
        return f"zu lang ({len(t)} Zeichen)"
    low = t.lower()
    if "hornhaut" in low:
        return "enthält „Hornhaut“"
    if len(re.findall(r"(?<![\w#])#bdsm(?![\w])", low)) != 1:
        return "#BDSM nicht genau einmal"
    if "\\n" in t:
        return "sichtbare Steuerzeichen"
    lt, url = str(pick.get("link_text") or ""), str(pick.get("url") or "")
    if not lt or lt not in t or not url.startswith("https://"):
        return "Link fehlt"
    if not str(pick.get("url", "")).split("?")[0].rstrip("/").split("/")[-1]:
        return "Link ohne Beitrag"
    return ""


def bp_facets(text: str, link_text: str, url: str, hashtags: list) -> list:
    def b(i: int) -> int:
        return len(text[:i].encode("utf-8"))
    out = []
    i = text.rfind(link_text)
    if i >= 0:
        out.append({"index": {"byteStart": b(i), "byteEnd": b(i + len(link_text))},
                    "features": [{"$type": "app.bsky.richtext.facet#link", "uri": url}]})
    want = {str(h).lstrip("#").lower() for h in hashtags or []}
    for m in _BP_TAG.finditer(text):
        tag = m.group(1).rstrip(".,;:!?…")
        if tag.lower() in want:
            out.append({"index": {"byteStart": b(m.start()), "byteEnd": b(m.start() + 1 + len(tag))},
                        "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": tag}]})
    return sorted(out, key=lambda f: f["index"]["byteStart"])


def bp_record(pick: dict, now: datetime | None = None) -> dict:
    n = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    t = str(pick["text"])
    return {"$type": "app.bsky.feed.post", "text": t, "langs": ["de"],
            "facets": bp_facets(t, str(pick["link_text"]), str(pick["url"]), list(pick.get("hashtags") or [])),
            "createdAt": n.strftime("%Y-%m-%dT%H:%M:%S.") + f"{n.microsecond // 1000:03d}Z"}   # bewusst KEIN embed


def bp_find_own(bs: "Bsky", url: str, hours: int = 72) -> str:
    """Eigener Post mit diesem Link in den letzten X Stunden? (Doppelpost-Schutz)"""
    base = url.split("?")[0].rstrip("/")
    if not base:
        return ""
    cutoff = time.time() - hours * 3600
    d = bs.get("app.bsky.feed.getAuthorFeed", {"actor": bs.did, "limit": 50, "filter": "posts_no_replies"})
    for it in d.get("feed", []) or []:
        p = it.get("post") or {}
        if (it.get("reason") or {}) or (p.get("author") or {}).get("did") != bs.did:
            continue
        rec = p.get("record") or {}
        if ts(str(rec.get("createdAt") or p.get("indexedAt") or "")) < cutoff:
            continue
        uris = [f2.get("uri", "") for f in rec.get("facets") or [] for f2 in f.get("features") or [] if isinstance(f2, dict)]
        ext = ((rec.get("embed") or {}).get("external") or {}).get("uri", "")
        if any(str(u).split("?")[0].rstrip("/") == base for u in uris + [ext]):
            return str(p.get("uri") or "")
    return ""


def bdsm_post(bs: "Bsky", now: datetime | None = None) -> dict:
    """Nie Ausnahme nach außen. Ergebnis nur fürs Log; Zustand steht auf der Seite."""
    if not BP_ON:
        return {"status": "aus (FL_BP=0)"}
    if not bp_in_slot(now):
        return {"status": "noch nicht dran"}
    try:
        for _ in range(2):
            r = bridge("bp_pick", dry=READONLY)
            st = r.get("status")
            if st == "pending":
                p = r.get("pending") or {}
                if READONLY:
                    return {"status": "pending (nur lesen)", "post_id": p.get("post_id")}
                found = bp_find_own(bs, str(p.get("url") or ""))
                if found:
                    bridge("bp_done", post_id=int(p.get("post_id") or 0), uri=found, recovered=True)
                    if p.get("today"):
                        return {"status": "posted", "uri": found, "recovered": True}
                    continue                                  # alter Tag aufgeräumt -> heutigen Post holen
                if p.get("today") and int(p.get("age") or 0) < BP_PENDING_TTL:
                    return {"status": "anderer Lauf postet gerade"}
                bridge("bp_fail", message="Abgebrochener Versuch, Post nicht im Feed gefunden")
                if p.get("today"):
                    return {"status": "abgebrochen", "post_id": p.get("post_id")}
                continue
            if st not in ("go", "preview"):
                return {"status": st}                          # disabled / not_yet / done / empty / missing
            pick = r.get("pick") or {}
            why = bp_check(pick)
            if why:
                if st == "go":
                    bridge("bp_fail", message="Nicht gepostet: " + why)
                return {"status": "abgelehnt", "grund": why, "post_id": pick.get("post_id")}
            if st == "preview" or READONLY:
                return {"status": "vorschau (nur lesen)", "post_id": pick.get("post_id"), "zeichen": len(pick["text"])}
            found = bp_find_own(bs, str(pick["url"]), hours=20)
            if found:                                         # schon von Hand gepostet o. ä. -> nicht doppelt
                bridge("bp_done", post_id=int(pick["post_id"]), uri=found, recovered=True, title=pick.get("title", ""),
                       url=pick["url"], text=pick["text"])
                return {"status": "posted", "uri": found, "recovered": True}
            try:
                d = bs.post("com.atproto.repo.createRecord", {"repo": bs.did, "collection": "app.bsky.feed.post", "record": bp_record(pick, now)})
                uri = str(d.get("uri") or "")
            except Exception as e:  # noqa: BLE001
                uri = ""
                if not (isinstance(e, BskyHttpError) and 0 < e.code < 500):
                    time.sleep(3)                             # unklar, ob angekommen -> im Feed nachsehen
                    try:
                        uri = bp_find_own(bs, str(pick["url"]), hours=2)
                    except Exception:  # noqa: BLE001
                        uri = ""
                if not uri:
                    try:
                        bridge("bp_fail", message=str(e)[:250])
                    except Exception:  # noqa: BLE001
                        pass
                    return {"status": "fehler", "grund": str(e)[:200]}
            bridge("bp_done", post_id=int(pick["post_id"]), uri=uri, title=pick.get("title", ""), url=pick["url"], text=pick["text"])
            return {"status": "posted", "uri": uri, "post_id": pick["post_id"], "zeichen": len(pick["text"])}
        return {"status": "nichts zu tun"}
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if "unknown_action" in msg:
            return {"status": "Seite kennt bp_pick noch nicht (Bridge-Update fehlt)"}
        return {"status": "fehler", "grund": msg[:200]}


def main() -> int:
    global GEMINI_MODEL
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
    GEMINI_MODEL = str(st.get("gemini_model") or "") or GEMINI_MODEL
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
    bp_res: dict = {"status": "nicht gelaufen"}
    try:
        bs = Bsky()
        t1 = time.monotonic()
        bp_res = bdsm_post(bs)                 # BDSMPOST-20261115: eigener Schritt, wirft nie, ändert nichts an den anderen
        timing["bdsm_post"] = round(time.monotonic() - t1, 1)
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
    log("BDSM-Tagespost: " + json.dumps(bp_res, ensure_ascii=False))
    log("Zeiten (s): " + json.dumps(timing, ensure_ascii=False) + (" | Abgleich zuerst" if uf_first else ""))
    log(f"Dauer {time.monotonic() - T0:.1f} s")
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
