# feetlove-bluesky (privat)
GitHub-Actions-Job für Bluesky Auto-Likes + Entfolger-Abgleich von feetlove1973.de.
Secrets (Settings → Secrets and variables → Actions): FL_BRIDGE_URL, FL_BRIDGE_SECRET, BSKY_HANDLE, BSKY_APP_PASSWORD, XAI_API_KEY.
Keine Geheimnisse ins Repo schreiben. Handstart über „Actions → Run workflow“ läuft standardmäßig nur lesend.

## Zeitplan je Lauf (Standard, FL_MAX_RUNTIME=420 s)
- Ist der Follower-Abgleich fällig, enden die Auto-Likes nach FL_LIKES_BUDGET (240 s); der Rest bis 395 s gehört dem Abgleich.
  Ist er deutlich überfällig (≥ 2× FL_UF_SCAN_MIN), läuft er zuerst. Sonst bekommen die Likes die ganze Zeit.
- KI-Prüfungen und Thread-Abrufe laufen parallel (FL_AI_WORKERS=8), xAI mit Wiederholung bei 429/5xx. Likes bleiben nacheinander.
- Reicht die Zeit für den Follower-Abruf nicht: Status „deferred“ (Exit 0), nächster Lauf holt ihn nach.
  Erst wenn seit FL_UF_STALE_MIN (720 Min) kein Abgleich fertig wurde, gilt das als Fehler.
- Log zeigt Zeiten je Phase („Zeiten (s)“) und KI-Statistik.
