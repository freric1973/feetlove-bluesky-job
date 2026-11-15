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

## Auto-Folgen: alle Liker aller Beiträge (ALLELIKES, 15.11.2026)

Vorgabe: Wer einen Beitrag von dailyfun geliked hat und dem dailyfun nicht folgt, wird automatisch gefolgt – bei jedem Beitrag.

**Ab heute (Dennis, 15.11.2026):** Es zählen nur Likes mit `createdAt` (ersatzweise `indexedAt`) ab 15.11.2026 00:00 Europe/Berlin – egal auf welchem (auch altem) Post. Ältere Likes lösen nichts aus.

- Posts der letzten `autofollow_days` werden wie bisher in jedem Lauf geprüft (Cache: nur bei geändertem Like-Zähler oder alle 6 h).
- Ältere Posts: Rundlauf über das ganze Profil, `FL_AF_BACKFILL_POSTS` (150) Posts je Lauf; Fortschritt steht im Auto-Folgen-Cache (`__backfill__`).
  Eine Runde über ~17.000 Posts dauert damit gut zwei Tage, danach beginnt sie von vorn (neue Likes auf alten Posts).
- Bekannte Liker (Like ab Stichtag) mit Status followed/skipped werden live nachgeprüft (max. `FL_AF_VERIFY_MAX` = 300 je Lauf, Likes der letzten 48 h zuerst).
  Folgt dailyfun ihnen nicht (mehr), werden sie gefolgt. Ausgenommen: von Hand übersprungen (`manual_skip` von der Bridge; ältere Bridge ohne Liste: skipped nur, wenn in `recheck`), blocked, Blockierungen/Stummschaltungen, Spam-Labels, frühere Entfolger ohne Rückfolge.
- Limits bleiben: `max_run` (20) je Lauf, `max_day` (400) je Tag. Rückstand wird gedrosselt als would_follow gemerkt (max. `FL_AF_QUEUE_MAX` = 300 je Lauf), `stats.backlog` zeigt die Restmenge des Laufs.
- Von Hand übersprungen (Tageszahlen): Ein Like NACH dem Überspringen hebt es auf (Dennis, 15.11. 10:04). Der Zeitpunkt kommt nicht von der Bridge; der Job merkt sich im Cache (`__manual_skip__`), seit wann ein Konto in `manual_skip` steht. Bereits beim ersten Lauf übersprungene gelten ab Stichtag 15.11. 00:00 Berlin.
- Liker-Cache (CACHELIKE): Post-Cache-Einträge enthalten die Likes ab Stichtag (`[Zähler, Zeit, {did: Like-Zeit}]`), damit Liker auch ohne frisches Einlesen zählen; bestätigte (dailyfun folgt) fliegen heraus. Nachprüfung: übersprungene zuerst, bestätigte 6 h nicht erneut (`__verified__`).

## BDSM-Tagespost (BDSMPOST-20261115)

Einmal täglich ein reiner Textpost aus der Rubrik „Meine BDSM-Welt“ (Kategorie-Slug `meine-bdsm-welt`), im Lauf um 23:41 Europe/Berlin (frühestens ab 23:30, `FL_BP_SLOT`).

- Erste Zeile immer `(zum Stummschalten): #BDSM` (#BDSM als Hashtag-Facet), dann Ü18-Hinweis, Überschrift, 1–2 Sätze aus der Meta-Beschreibung, Kurzlink `feetlove1973.de/b/<ID>` (Link-Facet; /b/ leitet per 301 auf den Slug und hängt `?shared=1&src=bluesky` an), 2–3 Hashtags aus den Schlagwörtern (nie „Hornhaut“, #BDSM nicht doppelt). Max. 300 Zeichen.
- **Kein Bild, keine Linkkarte**: der Post hat kein `embed`.
- Auswahl, Text und Doppelpost-Sperre kommen von der Seite (`bp_pick`, Lease nötig); Ergebnis über `bp_done` / `bp_fail`. Vor dem Posten prüft der Job den eigenen Feed auf denselben Link.
- Ein/Aus auf der Tageszahlen-Seite (Standard **aus**). Notschalter im Job: `FL_BP=0`. Nur-Lesen postet nie und setzt keine Sperre.
- Fehler beim Tagespost ändern den Exit-Code nicht; sie stehen im Log („BDSM-Tagespost: …“) und in der Liste auf der Tageszahlen-Seite.
