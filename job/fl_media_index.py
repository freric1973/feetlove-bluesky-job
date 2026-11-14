#!/usr/bin/env python3
"""BILDVORSCHLAG-20261114 – Medien-Indexer für feetlove1973.de

Rechnet für jedes vorschlagbare Medium (Medienpool + in Beiträgen benutzte Mediathek-Bilder) einen
Bildvektor mit open_clip xlm-roberta-base-ViT-B-32 (laion5b_s13b_b90k, mehrsprachig, CPU, ohne Inhaltsfilter)
und für die Wortliste die Textvektoren. Ergebnis geht über die signierte media-gh-bridge.php in die CMS-DB.

Modi
  bridge  – normaler Lauf (GitHub-Job oder einmalig auf der Box).
            Umgebung: FL_MEDIA_BRIDGE_URL, FL_MEDIA_BRIDGE_SECRET oder FL_MEDIA_BRIDGE_SECRET_FILE (wird nie ausgegeben).
  bench   – lokaler Test ohne Server: fl_media_index.py bench BILD_ODER_URL … (misst Laufzeit, zeigt Top-Wörter)

Zusätzlich (BILDVORSCHLAG-NSFW):
  * NSFW-Score je Medium mit Falconsai/nsfw_image_detection (ViT, CPU). Über der Schwelle der Webseite → Ü18 und
    kein automatisches Bluesky-Posting, bis Dennis im Medienpool freigibt.
  * Eigene Kurzbeschreibung ohne Inhaltsfilter: Florence-2-base (<CAPTION>) → Zitattext abschneiden → opus-mt-en-de → max. 6 Wörter.
    Abschalten mit FL_CAPTION=0 (dann nur Vektor + NSFW).

Optionen: --max-minutes N (Zeitbudget, danach sauber aufhören; Rest beim nächsten Lauf), --batch N, --limit N
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import argparse, base64, hashlib, hmac, io, json, os, re, secrets, subprocess, sys, tempfile, time, urllib.request, urllib.error

MODEL_ARCH = "xlm-roberta-base-ViT-B-32"
MODEL_PRETRAINED = "laion5b_s13b_b90k"
MODEL_ID = f"{MODEL_ARCH}/{MODEL_PRETRAINED}"   # muss FL_MS_MODEL in medien-suche-lib.php entsprechen
DIMS = 512
NSFW_MODEL = "Falconsai/nsfw_image_detection"                 # = FL_MS_NSFW_MODEL
CAP_MODEL = "florence-community/Florence-2-base"
MT_MODEL = "Helsinki-NLP/opus-mt-en-de"
CAP_MAX_WORDS = 6

def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)

# ---------------------------------------------------------------- Modell
def set_threads():
    import torch
    torch.set_num_threads(int(os.environ.get('FL_THREADS') or 0) or max(1, os.cpu_count() or 1))
    return torch

class Model:
    def __init__(self):
        import open_clip
        torch = set_threads()
        t = time.time()
        self.torch = torch
        self.model, _, self.preprocess = open_clip.create_model_and_transforms(MODEL_ARCH, pretrained=MODEL_PRETRAINED)
        self.model.eval()
        self.tokenizer = open_clip.get_tokenizer(MODEL_ARCH)
        self.load_s = time.time() - t

    def images(self, pil_list):
        with self.torch.no_grad():
            x = self.torch.stack([self.preprocess(im) for im in pil_list])
            f = self.model.encode_image(x)
            f = f / f.norm(dim=-1, keepdim=True)
        return f.float().numpy()

    def texts(self, labels):
        import numpy as np
        out = []
        with self.torch.no_grad():
            for i in range(0, len(labels), 64):
                chunk = labels[i:i + 64]
                vs = []
                for tpl in ("{}", "Ein Foto von {}."):
                    tok = self.tokenizer([tpl.format(l) for l in chunk])
                    f = self.model.encode_text(tok)
                    vs.append(f / f.norm(dim=-1, keepdim=True))
                f = sum(vs)
                f = f / f.norm(dim=-1, keepdim=True)
                out.append(f.float().numpy())
        return np.concatenate(out) if out else np.zeros((0, DIMS), dtype="float32")

def quantize(v):
    """float-Vektor (normiert) → (base64 int8×512, scale) – PHP: wert = int8 * scale"""
    import numpy as np
    m = float(np.abs(v).max()) or 1e-9
    scale = m / 127.0
    q = np.clip(np.rint(v / scale), -127, 127).astype("int8")
    return base64.b64encode(q.tobytes()).decode("ascii"), round(scale, 9)

def load_image(data: bytes, mime: str):
    from PIL import Image, ImageOps
    im = Image.open(io.BytesIO(data))
    try:
        im.seek(0)            # GIF/WebP: erstes Bild
    except Exception:
        pass
    im = ImageOps.exif_transpose(im) if hasattr(ImageOps, "exif_transpose") else im
    return im.convert("RGB")

def video_frame(data: bytes) -> bytes:
    """Standbild aus einem Video (ffmpeg, bei 1 s; bei sehr kurzen Clips das erste Bild)."""
    with tempfile.TemporaryDirectory() as d:
        src, dst = os.path.join(d, "v.mp4"), os.path.join(d, "f.jpg")
        with open(src, "wb") as fh:
            fh.write(data)
        for ss in ("1", "0"):
            r = subprocess.run(["ffmpeg", "-loglevel", "error", "-y", "-ss", ss, "-i", src, "-frames:v", "1", "-q:v", "3", dst], capture_output=True, timeout=120)
            if r.returncode == 0 and os.path.exists(dst) and os.path.getsize(dst) > 0:
                with open(dst, "rb") as fh:
                    return fh.read()
    raise RuntimeError("Kein Standbild aus dem Video")

# ---------------------------------------------------------------- Bridge
class Bridge:
    def __init__(self, url: str, secret: str):
        self.url, self._secret = url, secret.encode()

    def _req(self, payload: dict, raw: bool = False, timeout: int = 120):
        last = None
        for attempt in range(4):
            try:
                with urllib.request.urlopen(self._resign(payload), timeout=timeout) as r:
                    data = r.read()
                    if raw:
                        return data
                    j = json.loads(data.decode())
                    if not j.get("ok"):
                        raise RuntimeError(f"Bridge: {j.get('status')} {j.get('error', '')}")
                    return j
            except urllib.error.HTTPError as e:
                if e.code in (400, 403, 404, 413):
                    raise RuntimeError(f"Bridge HTTP {e.code}") from None
                last = e
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                last = e
            time.sleep(3 * (attempt + 1))
        raise RuntimeError(f"Bridge nicht erreichbar: {type(last).__name__}")

    def _resign(self, payload):
        body = json.dumps(payload, separators=(",", ":")).encode()
        ts, nonce = str(int(time.time())), secrets.token_urlsafe(24)[:32]
        sig = hmac.new(self._secret, ts.encode() + b"\n" + nonce.encode() + b"\n" + body, hashlib.sha256).hexdigest()
        return urllib.request.Request(self.url, data=body, method="POST", headers={
            "Content-Type": "application/json", "X-FL-Time": ts, "X-FL-Nonce": nonce, "X-FL-Sig": sig,
            "User-Agent": "feetlove-media-index/1"})

    def call(self, action: str, **kw):
        return self._req({"action": action, "model": MODEL_ID, **kw})

    def file(self, key: str, part: str) -> bytes:
        return self._req({"action": "file", "key": key, "part": part}, raw=True, timeout=300)

# ---------------------------------------------------------------- NSFW + Kurzbeschreibung
class Nsfw:
    def __init__(self):
        from transformers import pipeline
        set_threads()
        t = time.time()
        self.pipe = pipeline("image-classification", model=NSFW_MODEL, device=-1)
        self.load_s = time.time() - t

    def scores(self, pil_list):
        out = []
        for res in self.pipe(pil_list, batch_size=8):
            out.append(round(float(next((r["score"] for r in res if r["label"].lower() == "nsfw"), 0.0)), 4))
        return out

_CUT = re.compile(r"\s*(,|\bwith the (words|text|caption|quote)\b|\bthat (says|reads)\b|\bwhich (says|reads)\b|\bsaying\b|\bwritten\b|\bthe words\b|\bthe text\b|[\"\u201c\u201e:]).*$", re.I)

def clean_caption_en(en: str) -> str:
    """Zitierten Bildtext abschneiden (der steht schon in den Gemini-Feldern bzw. ist keine Stimmung)."""
    en = re.sub(r"\s+", " ", en or "").strip()
    en = re.sub(r"^(this is |there is |an image of |a picture of |a photo of )", "", en, flags=re.I)
    cut = _CUT.sub("", en).strip(" .,-")
    return cut if len(cut.split()) >= 2 else en.split(",")[0].strip(" .,-")

_DE_STOP = {"mit", "auf", "an", "in", "im", "vor", "von", "vom", "zu", "zum", "zur", "und", "oder", "ein", "eine", "einem", "einer", "einen", "eines",
            "der", "die", "das", "den", "dem", "des", "die", "sich", "neben", "über", "unter", "hinter", "bei", "für", "aus", "nach"}

def short_de(de: str) -> str:
    de = re.sub(r"\s+", " ", de or "").strip(" .,;:-\"'„“")
    de = re.sub(r"^(ein|eine|einer|einem|einen|der|die|das)\s+", "", de, flags=re.I)
    w = de.split(" ")
    cut = len(w) > CAP_MAX_WORDS
    w = w[:CAP_MAX_WORDS]
    while cut and len(w) > 2 and w[-1].lower().strip(",") in _DE_STOP:   # nicht auf „mit einem…“ enden
        w.pop()
    de = " ".join(w).rstrip(",") + ("…" if cut else "")
    return (de[:1].upper() + de[1:])[:120]

class Captioner:
    def __init__(self):
        torch = set_threads()
        from transformers import AutoProcessor, Florence2ForConditionalGeneration, MarianMTModel, MarianTokenizer
        t = time.time()
        self.torch = torch
        self.proc = AutoProcessor.from_pretrained(CAP_MODEL)
        self.model = Florence2ForConditionalGeneration.from_pretrained(CAP_MODEL).eval()
        self.tok = MarianTokenizer.from_pretrained(MT_MODEL)
        self.mt = MarianMTModel.from_pretrained(MT_MODEL).eval()
        self.load_s = time.time() - t

    def captions(self, pil_list):
        """→ (deutsch kurz, englisch roh) je Bild"""
        ens = []
        with self.torch.no_grad():
            for im in pil_list:   # einzeln: Florence-Batches bringen auf CPU kaum etwas und kosten RAM
                inp = self.proc(text="<CAPTION>", images=im, return_tensors="pt")
                out = self.model.generate(**inp, max_new_tokens=24, num_beams=1, do_sample=False)
                ens.append(self.proc.batch_decode(out, skip_special_tokens=True)[0])
            short = [clean_caption_en(e) for e in ens]
            enc = self.tok(short, return_tensors="pt", padding=True, truncation=True, max_length=64)
            de = self.tok.batch_decode(self.mt.generate(**enc, max_new_tokens=32), skip_special_tokens=True)
        return [short_de(d) for d in de], ens

# ---------------------------------------------------------------- Läufe
def run_bridge(a):
    import numpy as np
    url, secret = os.environ.get("FL_MEDIA_BRIDGE_URL", "").strip(), os.environ.get("FL_MEDIA_BRIDGE_SECRET", "").strip()
    sf = os.environ.get("FL_MEDIA_BRIDGE_SECRET_FILE", "").strip()   # Box: Geheimnis aus Datei (chmod 600), nie ausgeben
    if not secret and sf and os.path.isfile(sf):
        with open(sf, encoding="utf-8") as fh:
            secret = fh.read().strip()
    if not url or len(secret) < 32:
        log("FL_MEDIA_BRIDGE_URL / FL_MEDIA_BRIDGE_SECRET fehlen – Abbruch.")
        return 2
    br = Bridge(url, secret)
    t0 = time.time()
    try:
        lst = br.call("list")
    except RuntimeError as e:
        if "HTTP 403" in str(e):
            log("Signatur abgelehnt (HTTP 403) – FL_MEDIA_BRIDGE_SECRET passt nicht zur Webseite.")
            return 1
        log(f"Webseite nicht erreichbar ({e}) – Lauf übersprungen.")
        return 0
    if lst.get("status") == "disabled":
        log("Bridge ist ausgeschaltet (enabled=false) – nichts zu tun.")
        return 0
    if lst.get("model") != MODEL_ID:
        log(f"Modell passt nicht zur Webseite ({lst.get('model')}) – Abbruch.")
        return 2
    items = lst["items"]
    use_cap = os.environ.get("FL_CAPTION", "1") != "0"
    for it in items:                       # ältere Webseite ohne "need" → nur Vektor
        it["need"] = [n for n in it.get("need", [] if it.get("done") else ["vec"]) if use_cap or n != "cap"]
    todo = [i for i in items if i["need"]]
    if a.limit:
        todo = todo[: a.limit]
    voc = br.call("vocab", all=False)
    words = voc.get("todo", [])
    cnt = {n: sum(1 for i in todo if n in i["need"]) for n in ("vec", "nsfw", "cap")}
    log(f"Medien gesamt {len(items)}, offen {len(todo)} (Vektor {cnt['vec']}, NSFW {cnt['nsfw']}, Beschreibung {cnt['cap']}) · Wortliste {voc.get('total')}, offen {len(words)}")
    if not todo and not words:
        log("Nichts zu tun.")
        return 0
    model = Model() if (words or cnt["vec"]) else None
    if model:
        log(f"CLIP geladen in {model.load_s:.1f} s")
    nsfw = Nsfw() if cnt["nsfw"] else None
    if nsfw:
        log(f"NSFW-Modell geladen in {nsfw.load_s:.1f} s")
    cap = Captioner() if cnt["cap"] else None
    if cap:
        log(f"Florence-2 + Übersetzer geladen in {cap.load_s:.1f} s")
    if words:
        vecs = model.texts([w["label"] for w in words])
        rows = [{"key": w["key"], **dict(zip(("vec", "scale"), quantize(v)))} for w, v in zip(words, vecs)]
        saved = 0
        for i in range(0, len(rows), 300):
            saved += br.call("vocab_save", words=rows[i:i + 300]).get("saved", 0)
        log(f"Wortliste: {len(words)} neue Wörter berechnet, {saved} gespeichert (gesamt {voc.get('total')})")

    done = errs = flagged = 0
    deadline = t0 + a.max_minutes * 60
    tm = {"vec": 0.0, "nsfw": 0.0, "cap": 0.0}
    nm = {"vec": 0, "nsfw": 0, "cap": 0}
    for start in range(0, len(todo), a.batch):
        if time.time() > deadline:
            log(f"Zeitbudget {a.max_minutes} Min. erreicht – Rest beim nächsten Lauf.")
            break
        batch = todo[start:start + a.batch]
        pils, ok, out = [], [], []

        def fetch(it):
            if it["kind"] == "video":
                data = br.file(it["key"], "poster") if it.get("has_poster") else video_frame(br.file(it["key"], "main"))
            else:
                data = br.file(it["key"], "poster") if (it["kind"] == "gif" and it.get("has_poster")) else br.file(it["key"], "main")
            return load_image(data, it.get("mime", ""))

        with ThreadPoolExecutor(max_workers=a.fetchers) as ex:
            futs = [(it, ex.submit(fetch, it)) for it in batch]
            for it, f in futs:
                try:
                    pils.append(f.result())
                    ok.append(it)
                except Exception as e:  # einzelnes Medium kaputt → Fehler vermerken, weiter
                    errs += 1
                    out.append({"key": it["key"], "sig": it["sig"], "error": f"{type(e).__name__}: {str(e)[:150]}"})
        rows = {it["key"]: {"key": it["key"], "sig": it["sig"]} for it in ok}
        for part, fn in (("vec", None), ("nsfw", None), ("cap", None)):
            sel = [(it, im) for it, im in zip(ok, pils) if part in it["need"]]
            if not sel:
                continue
            t = time.time()
            try:
                if part == "vec":
                    for (it, _), v in zip(sel, model.images([im for _, im in sel])):
                        b64, scale = quantize(v)
                        rows[it["key"]].update(vec=b64, scale=scale)
                elif part == "nsfw":
                    for (it, _), sc in zip(sel, nsfw.scores([im for _, im in sel])):
                        rows[it["key"]]["nsfw"] = sc
                        flagged += sc >= 0.5
                else:
                    de, _ = cap.captions([im for _, im in sel])
                    for (it, _), d in zip(sel, de):
                        rows[it["key"]]["caption"] = d
            except Exception as e:      # Teil fehlgeschlagen → Rest trotzdem speichern
                for it, _ in sel:
                    rows[it["key"]]["error"] = f"{part}: {type(e).__name__}: {str(e)[:120]}"
            tm[part] += time.time() - t
            nm[part] += len(sel)
        out.extend(rows.values())
        done += len(ok)
        if out:
            r = br.call("save", items=out)
            if r.get("rejected"):
                log(f"Warnung: {r['rejected']} Einträge abgelehnt")
        log(f"{min(start + a.batch, len(todo))}/{len(todo)} verarbeitet")
    st = br.call("stats")
    per = " · ".join(f"{k} {tm[k]:.1f} s ({(tm[k] / nm[k] if nm[k] else 0):.3f} s/Bild)" for k in ("vec", "nsfw", "cap"))
    log(f"Fertig: {done} neu, {errs} Fehler, NSFW-Verdacht {flagged} · Modellzeit {per} · gesamt {time.time() - t0:.0f} s · "
        f"Webseite: {st.get('indexed')}/{st.get('items')} indexiert, NSFW geprüft {st.get('nsfw_checked')}, Verdacht {st.get('nsfw_flagged')}, Beschreibungen {st.get('captions')}")
    return 0

def run_bench(a):
    import numpy as np
    t0 = time.time()
    model = Model()
    log(f"Modell geladen in {model.load_s:.1f} s")
    labels = a.words.split(",") if a.words else ["Füße", "Strand", "Sand", "Katze", "Hund", "Kaffee", "Spruch", "Text", "Schuhe", "High Heels", "Schnee", "Blumen", "Essen", "Auto", "Frau", "Mann", "lustig", "Herbst", "Wasser", "Bett"]
    t = time.time(); tv = model.texts(labels); txt_s = time.time() - t
    log(f"{len(labels)} Wörter in {txt_s:.2f} s ({txt_s / len(labels) * 1000:.0f} ms/Wort)")
    def get(src):
        if src.startswith("http"):
            with urllib.request.urlopen(urllib.request.Request(src, headers={"User-Agent": "Mozilla/5.0 feetlove-media-index-test"}), timeout=60) as r:
                data = r.read()
        else:
            with open(src, "rb") as fh:
                data = fh.read()
        if src.lower().endswith((".mp4", ".mov", ".webm")):
            data = video_frame(data)
        return load_image(data, "")
    t = time.time()
    with ThreadPoolExecutor(max_workers=a.fetchers) as ex:
        pils = list(ex.map(get, a.inputs))
    dl = time.time() - t
    names = [s.rsplit("/", 1)[-1] for s in a.inputs]
    model.images(pils[:1])  # Aufwärmen
    t = time.time()
    vecs = np.concatenate([model.images(pils[i:i + a.batch]) for i in range(0, len(pils), a.batch)])
    img_s = time.time() - t
    q = [quantize(v) for v in vecs]
    deq = np.stack([np.frombuffer(base64.b64decode(b), dtype="int8").astype("float32") * s for b, s in q])
    err = float(np.abs((deq @ tv.T) - (vecs @ tv.T)).max())
    for n, v in zip(names, vecs):
        sims = tv @ v
        top = np.argsort(-sims)[:3]
        log(f"{n[:50]:50s} → " + ", ".join(f"{labels[i]} {sims[i]:.3f}" for i in top))
    res = {"bilder": len(pils), "modell_laden_s": round(model.load_s, 1), "download_s": round(dl, 2), "download_s_pro_bild": round(dl / len(pils), 3), "fetchers": a.fetchers, "modell_s": round(img_s, 2),
           "s_pro_bild": round(img_s / len(pils), 3), "int8_fehler_max": round(err, 4), "threads": model.torch.get_num_threads(),
           "gesamt_s": round(time.time() - t0, 1)}
    if os.environ.get("FL_BENCH_EXTRA", "1") != "0":
        nz = Nsfw(); nz.scores(pils[:1])
        t = time.time(); sc = nz.scores(pils); ns = time.time() - t
        cp = Captioner(); cp.captions(pils[:1])
        t = time.time(); de, en = cp.captions(pils); cs = time.time() - t
        for n, s_, d, e in zip(names, sc, de, en):
            log(f"{n[:40]:40s} nsfw {s_:.4f} | {d} | {e[:70]}")
        res.update({"nsfw_laden_s": round(nz.load_s, 1), "nsfw_s_pro_bild": round(ns / len(pils), 3),
                    "caption_laden_s": round(cp.load_s, 1), "caption_s_pro_bild": round(cs / len(pils), 3),
                    "gesamt_s": round(time.time() - t0, 1)})
    print(json.dumps(res, ensure_ascii=False))
    return 0

def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["bridge", "bench"])
    p.add_argument("inputs", nargs="*")
    p.add_argument("--max-minutes", type=float, default=float(os.environ.get("FL_MAX_MINUTES", "40")))
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--words", default="")
    p.add_argument("--fetchers", type=int, default=6, help="parallele Downloads")
    p.add_argument("--threads", type=int, default=0, help="CPU-Threads für das Modell (0 = alle)")
    a = p.parse_args()
    if a.threads:
        os.environ['FL_THREADS'] = str(a.threads)
    return run_bridge(a) if a.mode == "bridge" else run_bench(a)

if __name__ == "__main__":
    sys.exit(main())
