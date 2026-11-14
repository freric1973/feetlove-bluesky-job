#!/usr/bin/env python3
"""BILDVORSCHLAG-20261114 – Medien-Indexer für feetlove1973.de

Rechnet für jedes vorschlagbare Medium (Medienpool + in Beiträgen benutzte Mediathek-Bilder) einen
Bildvektor mit open_clip xlm-roberta-base-ViT-B-32 (laion5b_s13b_b90k, mehrsprachig, CPU, ohne Inhaltsfilter)
und für die Wortliste die Textvektoren. Ergebnis geht über die signierte media-gh-bridge.php in die CMS-DB.

Modi
  bridge  – normaler Lauf (GitHub-Job oder einmalig auf der Box).
            Umgebung: FL_MEDIA_BRIDGE_URL, FL_MEDIA_BRIDGE_SECRET oder FL_MEDIA_BRIDGE_SECRET_FILE (wird nie ausgegeben).
  bench   – lokaler Test ohne Server: fl_media_index.py bench BILD_ODER_URL … (misst Laufzeit, zeigt Top-Wörter)

Optionen: --max-minutes N (Zeitbudget, danach sauber aufhören; Rest beim nächsten Lauf), --batch N, --limit N
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import argparse, base64, hashlib, hmac, io, json, os, secrets, subprocess, sys, tempfile, time, urllib.request, urllib.error

MODEL_ARCH = "xlm-roberta-base-ViT-B-32"
MODEL_PRETRAINED = "laion5b_s13b_b90k"
MODEL_ID = f"{MODEL_ARCH}/{MODEL_PRETRAINED}"   # muss FL_MS_MODEL in medien-suche-lib.php entsprechen
DIMS = 512
TAGS_PER_ITEM = 6

def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)

# ---------------------------------------------------------------- Modell
class Model:
    def __init__(self):
        import torch, open_clip
        torch.set_num_threads(int(os.environ.get('FL_THREADS') or 0) or max(1, os.cpu_count() or 1))
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

# ---------------------------------------------------------------- Tags
class Tagger:
    def __init__(self, labels, vecs):
        import numpy as np
        self.labels, self.vecs = labels, vecs
        self.center, self.n = np.zeros(DIMS, dtype="float32"), 0

    def tags(self, img_vecs):
        import numpy as np
        if not len(self.labels):
            return [[] for _ in img_vecs]
        for v in img_vecs:                      # laufender Mittelwert der Bilder → typische Allerwelts-Wörter abwerten
            self.n += 1
            self.center += (v - self.center) / self.n
        bias = 0.5 * (self.vecs @ self.center)   # halb abziehen (Test: beste Mischung aus Treffern und Vielfalt)
        sims = img_vecs @ self.vecs.T - bias[None, :]
        return [[self.labels[i] for i in np.argsort(-row)[:TAGS_PER_ITEM]] for row in sims]

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
    todo = [i for i in items if not i.get("done")]
    if a.limit:
        todo = todo[: a.limit]
    voc = br.call("vocab", all=False)
    words = voc.get("todo", [])
    log(f"Medien gesamt {len(items)}, offen {len(todo)} · Wortliste {voc.get('total')}, offen {len(words)}")
    if not todo and not words:
        log("Nichts zu tun.")
        return 0
    model = Model()
    log(f"Modell geladen in {model.load_s:.1f} s")
    if words:
        vecs = model.texts([w["label"] for w in words])
        rows = [{"key": w["key"], **dict(zip(("vec", "scale"), quantize(v)))} for w, v in zip(words, vecs)]
        saved = 0
        for i in range(0, len(rows), 300):
            saved += br.call("vocab_save", words=rows[i:i + 300]).get("saved", 0)
        log(f"Wortliste: {len(words)} neue Wörter berechnet, {saved} gespeichert (gesamt {voc.get('total')})")
    allv = br.call("vocab", all=True).get("vectors", [])
    labels = [w["label"] for w in allv]
    mat = np.stack([np.frombuffer(base64.b64decode(w["vec"]), dtype="int8").astype("float32") * float(w["scale"]) for w in allv]) if allv else np.zeros((0, DIMS), dtype="float32")
    if len(mat):
        mat = mat / np.linalg.norm(mat, axis=1, keepdims=True)
    tagger = Tagger(labels, mat)

    done = errs = 0
    deadline = t0 + a.max_minutes * 60
    img_s = 0.0
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
        if pils:
            t = time.time()
            vecs = model.images(pils)
            img_s += time.time() - t
            for it, v, tg in zip(ok, vecs, tagger.tags(vecs)):
                b64, scale = quantize(v)
                out.append({"key": it["key"], "sig": it["sig"], "vec": b64, "scale": scale, "tags": tg})
            done += len(ok)
        if out:
            r = br.call("save", items=out)
            if r.get("rejected"):
                log(f"Warnung: {r['rejected']} Einträge abgelehnt")
        log(f"{min(start + a.batch, len(todo))}/{len(todo)} verarbeitet")
    st = br.call("stats")
    log(f"Fertig: {done} neu, {errs} Fehler, Modellzeit {img_s:.1f} s ({(img_s / done if done else 0):.3f} s/Bild), gesamt {time.time() - t0:.0f} s · Webseite: {st.get('indexed')}/{st.get('items')} indexiert")
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
