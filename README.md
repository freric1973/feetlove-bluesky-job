# feetlove-bluesky (privat)
GitHub-Actions-Job für Bluesky Auto-Likes + Entfolger-Abgleich von feetlove1973.de.
Secrets (Settings → Secrets and variables → Actions): FL_BRIDGE_URL, FL_BRIDGE_SECRET, BSKY_HANDLE, BSKY_APP_PASSWORD, GEMINI_API_KEY (KI-Prüfung nur noch Gemini; Modell kommt als gemini_model über die Bridge, sonst gemini-3.5-flash-lite).
Keine Geheimnisse ins Repo schreiben. Handstart über „Actions → Run workflow“ läuft standardmäßig nur lesend.

## Zeitplan je Lauf (Standard, FL_MAX_RUNTIME=420 s)
- Ist der Follower-Abgleich fällig, enden die Auto-Likes nach FL_LIKES_BUDGET (240 s); der Rest bis 395 s gehört dem Abgleich.
  Ist er deutlich überfällig (≥ 2× FL_UF_SCAN_MIN), läuft er zuerst. Sonst bekommen die Likes die ganze Zeit.
- KI-Prüfungen und Thread-Abrufe laufen parallel (FL_AI_WORKERS=8), Gemini mit Wiederholung bei 429/5xx (nicht bei Kontingent-Ende); Safety-Sperren gehen ohne Fehler zur Handprüfung. Likes bleiben nacheinander.
- Reicht die Zeit für den Follower-Abruf nicht: Status „deferred“ (Exit 0), nächster Lauf holt ihn nach.
  Erst wenn seit FL_UF_STALE_MIN (720 Min) kein Abgleich fertig wurde, gilt das als Fehler.
- Log zeigt Zeiten je Phase („Zeiten (s)“) und KI-Statistik.

## Auto-Folgen (14.11.2026)
Zusätzlicher Schritt nach Auto-Likes und Entfolger-Abgleich, Regeln wie flb_af_run() auf der Webseite.
Nur-Lesen (FL_READONLY=1 bzw. Workflow-Häkchen) folgt nicht und schreibt nichts.
Ergebnis nur über die Bridge (af_save / finish).
