#!/usr/bin/env python3
"""Feetlove1973 – Bluesky Auto-Likes + Entfolger-Abgleich als GitHub-Actions-Job.

Portierung von bluesky-autolike.php (Stand AUTOLIKE-500-20261113) und dem Abgleich aus
bluesky-unfollowers.php. Gleiche Regeln: Frage/Beleidigung -> Prüfung, KI-Check (xAI), Like-Limit je Lauf
(Setting bluesky_autolike_max, Standard 50), nie doppelt liken (viewer.like wird vor jedem Like geprüft),
geblockte/Zweitkonten/stummgeschaltete überspringen, Testmodus (dryrun) wird respektiert.
Datenbank-Zugriff nur über die geschützte Schnittstelle bluesky-gh-bridge.php (HMAC).
Nur Python-Standardbibliothek. Geheimnisse ausschließlich aus Umgebungsvariablen (GitHub Secrets).
"""
from __future__ import annotations
import hashlib, hmac, json, os, re, secrets, sys, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timezone

ENV = os.environ.get
BRIDGE_URL = ENV("FL_BRIDGE_URL", "")
BRIDGE_SECRET = ENV("FL_BRIDGE_SECRET", "")
BSKY_HANDLE = ENV("BSKY_HANDLE", "")
BSKY_PW = ENV("BSKY_APP_PASSWORD", "")
BSKY_SERVICE = ENV("BSKY_SERVICE", "https://bsky.social").rstrip("/")
RELAY = ENV("BSKY_RELAY", "https://bsky.network").rstrip("/")
XAI_KEY = ENV("XAI_API_KEY", "")
XAI_BASE = ENV("XAI_BASE", "https://api.x.ai").rstrip("/")
READONLY = ENV("FL_READONLY", "0") == "1"          # nur lesen: keine Likes, kein Entfolgen, nichts in die DB
UF_SCAN_MIN = int(ENV("FL_UF_SCAN_MIN", "240"))     # Follower-Abgleich höchstens alle X Minuten
MAX_RUNTIME = float(ENV("FL_MAX_RUNTIME", "420"))   # Sicherheitsgrenze je Lauf (Sekunden)
SLEEP_LIKE = float(ENV("FL_SLEEP_LIKE", "0.7"))
RUN_ID = ENV("GITHUB_RUN_ID", "local") + "-" + ENV("GITHUB_RUN_ATTEMPT", "1") + "-" + secrets.token_hex(4)
T0 = time.monotonic()


def left() -> float:
    return MAX_RUNTIME - (time.monotonic() - T0)


class RateLimit(Exception):
    pass


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z] {msg}", flush=True)


# ---------------- HTTP ----------------

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
            last = RuntimeError(msg)
            if e.code < 500 or body is not None:   # Schreibaufrufe nie automatisch wiederholen
                raise last
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = RuntimeError("Bluesky nicht erreichbar.")
            if body is not None:
                raise last
        time.sleep(1.5 * (attempt + 1))
    raise last  # type: ignore[misc]


# ---------------- Schnittstelle (one.com) ----------------

def bridge(action: str, **data) -> dict:
    body = json.dumps({"action": action, "run": RUN_ID, **data}, ensure_ascii=False, separators=(",", ":")).encode()
    ts, nonce = str(int(time.time())), secrets.token_urlsafe(18)
    sig = hmac.new(BRIDGE_SECRET.encode(), ts.encode() + b"\n" + nonce.encode() + b"\n" + body, hashlib.sha256).hexdigest()
    hdr = {"Content-Type": "application/json", "X-FL-Time": ts, "X-FL-Nonce": nonce, "X-FL-Sig": sig, "User-Agent": "feetlove-gh-bluesky/1"}
    req = urllib.request.Request(BRIDGE_URL, data=body, headers=hdr, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            d = json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            d = json.loads(e.read() or b"{}")
        except ValueError:
            d = {}
        raise RuntimeError(f"Schnittstelle {action}: HTTP {e.code} {d.get('status', '')} {d.get('message', '')}".strip())
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
SYSTEM_PROMPT = ('Du prüfst Antworten auf Social-Media-Posts des Bloggers Dennis. negative=true NUR, wenn die Antwort Dennis PERSÖNLICH beleidigt, beschimpft, verhöhnt, bedroht oder belästigt (Beleidigungen, Schimpfwörter gegen ihn, Hass, abwertende Aussagen über seine Person, sexuelle Belästigung, Drohungen). negative=false für alles andere, insbesondere: Kritik an einem Thema, an Firmen (z. B. Google), Produkten, Politik oder Meinungen; Widerspruch in der Sache; Sarkasmus oder Spott über das Thema; Witze, Wortspiele, Insider, Emojis; kurze oder rätselhafte Antworten (z. B. "Hex, Hex"); Zustimmung, Flirt, Neutrales. Im Zweifel negative=false. reason: max. 8 Wörter, deutsch.')


def ai_negative(text: str, parent: str) -> tuple[bool, str]:
    if not XAI_KEY:
        raise RuntimeError("xAI-Key fehlt")
    schema = {"type": "object", "properties": {"negative": {"type": "boolean"}, "reason": {"type": "string"}}, "required": ["negative", "reason"], "additionalProperties": False}
    payload = {"model": XAI_MODEL, "temperature": 0, "max_tokens": 300, "messages": [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": "Ursprungspost (Kontext):\n" + parent[:500] + "\n\nAntwort:\n" + text[:1000]}],
        "response_format": {"type": "json_schema", "json_schema": {"name": "reply_sentiment", "strict": True, "schema": schema}}}
    req = urllib.request.Request(XAI_BASE + "/v1/chat/completions", data=json.dumps(payload, ensure_ascii=False).encode(),
                                 headers={"Content-Type": "application/json", "Authorization": "Bearer " + XAI_KEY}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            d = json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        if e.code == 403 and "credits" in raw.lower():
            raise RuntimeError("xAI HTTP 403 (Guthaben/Ausgabelimit erreicht)")
        raise RuntimeError(f"xAI HTTP {e.code}")
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        raise RuntimeError("xAI HTTP 0")
    ch = (d.get("choices") or [{}])[0]
    try:
        r = json.loads((ch.get("message") or {}).get("content") or "")
    except ValueError:
        r = None
    if not isinstance(r, dict) or "negative" not in r:
        raise RuntimeError(f"KI-Antwort unklar ({ch.get('finish_reason') or '-'})")
    return bool(r["negative"]), str(r.get("reason", "")).strip()[:120]


def classify(text: str, parent: str) -> tuple[str, str]:
    if is_question(text):
        return "review", "Frage"
    w = insult_hit(text)
    if w:
        return "review", f"Beleidigend: „{w}“"
    try:
        neg, why = ai_negative(text, parent)
    except Exception as e:  # noqa: BLE001
        return "review", "KI-Prüfung fehlgeschlagen: " + str(e)
    return ("review", "Persönlich beleidigend (KI): " + why) if neg else ("like", why)


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

    def decide(self, c: dict, prev) -> str:
        s = self.stats
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
            dec, why = classify(c["text"], c["root_text"])
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
        for row in self.ki_rows:
            if left() < 30:
                self.stats["time_limit"] = True; break
            rid, uri, cid = int(row["id"]), str(row["reply_uri"]), str(row["reply_cid"])
            dec, why = classify(str(row.get("text") or ""), str(row.get("parent_text") or ""))
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
                for u in chunk:
                    p = posts.get(u)
                    if p is None:
                        s["notif_gone"] += 1; queue.pop(0); continue
                    a, rec = p.get("author") or {}, p.get("record") or {}
                    anchor = own_anchor(rec, me, own_cutoff)
                    created = str(rec.get("createdAt") or p.get("indexedAt") or "")
                    if a.get("did") == me or not anchor or ts(created) < self.since or not self.want(u):
                        queue.pop(0); continue
                    par = ctx.get(str(((rec.get("reply") or {}).get("parent") or {}).get("uri", "")), {})
                    anc = ctx.get(anchor, {})
                    c = {"uri": u, "cid": str(p.get("cid", "")), "did": str(a.get("did", "")), "handle": str(a.get("handle", "")),
                         "text": str(rec.get("text", "")), "created": created,
                         "liked": bool((p.get("viewer") or {}).get("like")), "like_uri": str((p.get("viewer") or {}).get("like", "")),
                         "muted": muted(a), "root_uri": anchor, "root_text": str((anc.get("record") or {}).get("text", "")),
                         "avatar": str(a.get("avatar", "")),
                         "thumb": embed_thumb(p.get("embed")) or embed_thumb(par.get("embed")) or embed_thumb(anc.get("embed")),
                         "parent_text": str((par.get("record") or {}).get("text", ""))[:500]}
                    if self.decide(c, self.known.get(u)) == "limit":
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
                if left() < 30:
                    s["time_limit"] = True; break
                s["posts"] += 1
                try:
                    th = self.bs.get("app.bsky.feed.getPostThread", {"uri": uri, "depth": 10, "parentHeight": 0})
                except RateLimit:
                    raise
                except Exception as e:  # noqa: BLE001
                    self.err(str(e)); continue
                new_cache[uri] = [rc, int(time.time())]
                lst: list = []
                self.collect(th.get("thread") or {}, str((p.get("record") or {}).get("text", "")), lst)
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
                    if self.decide(c, prev) == "limit":
                        new_cache.pop(uri, None); raise StopIteration
                time.sleep(0.2)
        except StopIteration:
            pass
        if not s.get("time_limit") and not s.get("limit_reached"):
            self.cache_new = new_cache
        else:
            merged = {**cache, **new_cache}; merged.pop(cur, None); self.cache_new = merged

    def run(self) -> dict:
        try:
            self.step_dryrun_rows()
            self.step_ki_rows()
            self.step_notif()
            self.step_threads()
        except RateLimit:
            self.stats["ok"] = False; self.stats["errors"].append("Bluesky-Rate-Limit erreicht – nächster Lauf macht weiter")
        except Exception as e:  # noqa: BLE001
            self.stats["ok"] = False; self.stats["errors"].append(str(e)[:300])
        finally:
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


def uf_scan(bs: Bsky) -> list:
    members, cursor, seen = {}, "", set()
    for _ in range(1000):
        if left() < 60:
            raise TimeoutError("Laufzeitgrenze vor Ende des Follower-Abrufs")
        q = {"actor": bs.did, "limit": 100}
        if cursor:
            q["cursor"] = cursor
        d = bs.get("app.bsky.graph.getFollowers", q)
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
        members = uf_scan(bs)
        res["followers"] = len(members)
        if READONLY:
            res["status"] = "readonly_scan"; return res
        r = bridge("uf_commit", actor=bs.did, members=members)
        res.update({k: r.get(k) for k in ("status", "new", "reason")})
        if r.get("status") not in ("baseline", "complete"):
            return res
        pending = r.get("pending") or []
    else:
        res["status"] = "not_due"; res["last_run_min"] = int(age // 60)
    if not pending or READONLY:
        res["pending"] = len(pending); return res
    count = errors = 0; stopped = ""
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
                raise RuntimeError("Blockierte Beziehung nicht als normales Entfolgen gewertet.")
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
    res["automatic"] = {"unfollowed": count, "error_count": errors, "stopped": stopped}
    if errors:
        res["status"] = "error"
    return res


# ---------------- Ablauf ----------------

def main() -> int:
    global XAI_MODEL
    missing = [n for n, v in (("FL_BRIDGE_URL", BRIDGE_URL), ("FL_BRIDGE_SECRET", BRIDGE_SECRET), ("BSKY_HANDLE", BSKY_HANDLE), ("BSKY_APP_PASSWORD", BSKY_PW)) if not v]
    if missing:
        log("Fehlende Secrets: " + ", ".join(missing)); return 2
    log(f"Lauf {RUN_ID} startet (readonly={READONLY})")
    st = bridge("begin")
    if st.get("status") != "ready":
        log(f"Kein Lauf: {st.get('status')} {st.get('reason', '')}"); return 0
    XAI_MODEL = st.get("xai_model") or XAI_MODEL
    al_stats: dict = {"ok": True, "status": "disabled", "errors": []}; uf_res: dict = {"status": "skipped"}
    al = None; ran = False
    try:
        bs = Bsky()
        if st["settings"].get("enabled"):
            al = AutoLike(bs, st); al_stats = al.run(); ran = True
        try:
            uf_res = uf_run(bs, st.get("uf") or {})
        except Exception as e:  # noqa: BLE001
            uf_res = {"status": "error", "message": str(e)[:200]}
    except Exception as e:  # noqa: BLE001
        al_stats = {"ok": False, "status": "error", "errors": [str(e)[:300]]}
    finally:
        clean = bool(al_stats.get("ok", True)) and not al_stats.get("errors") and uf_res.get("status") != "error"
        fin = {"stats": al_stats, "autolike_ran": ran and not READONLY, "uf": uf_res, "clean": clean and not READONLY}
        if al and al.notif_new is not None and not READONLY:
            fin["notif"] = al.notif_new
        if al and al.cache_new is not None and not READONLY:
            fin["thread_cache"] = al.cache_new
        bridge("finish", **fin)
    summary = {k: v for k, v in al_stats.items() if k != "errors"}
    log("Auto-Likes: " + json.dumps(summary, ensure_ascii=False))
    if al_stats.get("errors"):
        log("Hinweise: " + "; ".join(al_stats["errors"]))
    log("Entfolger: " + json.dumps({k: v for k, v in uf_res.items() if k != "pending"}, ensure_ascii=False))
    log(f"Dauer {time.monotonic() - T0:.1f} s")
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
