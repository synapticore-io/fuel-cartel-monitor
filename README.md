# fuel-cartel-monitor

**Die Gier-Lücke: Wie viel der Tankstellen-Preiserhöhung tatsächlich vom Ölpreis kommt.**

[![Python 3.12+](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Dashboard](https://img.shields.io/badge/Live_Dashboard-GitHub_Pages-green.svg)](https://synapticore-io.github.io/fuel-cartel-monitor/)

**[→ Live-Dashboard ansehen](https://synapticore-io.github.io/fuel-cartel-monitor/)**

---

## Kernbefund (März 2026)

**72% der Diesel-Preiserhöhung** im März 2026 sind nicht durch den gestiegenen Ölpreis erklärbar.

| Kennzahl | Diesel | Super E5 |
|----------|--------|----------|
| Zapfsäulen-Anstieg | +49,4 Ct/L | +26,5 Ct/L |
| davon Ölpreis (Brent) | +14,0 Ct/L | +14,0 Ct/L |
| **Gier-Marge** | **+35,4 Ct/L** | **+12,4 Ct/L** |
| **Kosten pro Tankfüllung (50L)** | **17,70 €** | **6,21 €** |

Basierend auf 13,6 Mio. Preisdatensätzen der Markttransparenzstelle für Kraftstoffe (MTS-K) des Bundeskartellamtes, bereitgestellt über [Tankerkönig](https://tankerkoenig.de/).

## Weitere Befunde

### Oligopol-Mechanismus: Leader-Follower

Shell ist der primäre Preisführer. Nach einer Shell-Erhöhung folgen die anderen Oligopol-Marken in **10–33 Minuten**. Das Bundeskartellamt dokumentierte 2011 noch einen 3-Stunden-Takt — die Koordination hat sich um Faktor 10–15 beschleunigt.

| Preisführer | Follower | Median-Lag | Events |
|------------|----------|-----------|--------|
| Shell | → Esso | 10 min | 752 |
| Shell | → JET | 17 min | 752 |
| Shell | → TotalEnergies | 20 min | 744 |
| ARAL | → Esso | 19 min | 544 |
| ARAL | → JET | 29 min | 544 |

*Region Hannover, 25 km Radius, E5, 30 Tage*

### Rockets & Feathers (Preisasymmetrie)

Preise steigen in großen Sprüngen, fallen aber in kleinen Schritten. Konsistentes **2:1 bis 2,5:1 Verhältnis** über alle Oligopol-Marken:

| Marke | Ø Erhöhung | Ø Senkung | Ratio |
|-------|-----------|-----------|-------|
| ARAL | +5,5 Ct | −2,4 Ct | 2,3:1 |
| Shell | +5,2 Ct | −2,2 Ct | 2,4:1 |
| Esso | +4,9 Ct | −2,3 Ct | 2,1:1 |

### Wann tanken?

| Bester Zeitpunkt | Schlechtester | Ersparnis |
|-----------------|---------------|-----------|
| **Abends (19–21 Uhr)** | Morgens (5–7 Uhr) | **~7 Ct/L** |

---

## Funktionsweise

Das Tool analysiert die offiziellen MTS-K-Preismeldungen aller ~17.750 deutschen Tankstellen:

1. **Daten laden** — tägliche CSVs mit allen Preisänderungen von [data.tankerkoenig.de](https://data.tankerkoenig.de/)
2. **Speichern** — in DuckDB (analytische Datenbank, keine Installation nötig)
3. **Analysieren** — SQL-Macros für Leader-Follower, Rockets-&-Feathers, Brent-Entkopplung
4. **Visualisieren** — GitHub Pages Dashboard oder interaktive Charts in Claude Desktop via MCP

Das Dashboard aktualisiert sich **täglich automatisch** über eine GitHub Action.

---

## Installation

```bash
git clone https://github.com/synapticore-io/fuel-cartel-monitor.git
cd fuel-cartel-monitor
uv sync
cp .env.example .env
```

Zugangsdaten in `.env` eintragen (kostenlose Registrierung bei [Tankerkönig](https://creativecommons.tankerkoenig.de/)):

```
TANKERKOENIG_DATA_USER=dein-benutzername
TANKERKOENIG_DATA_PASS=dein-api-key
```

## Benutzung

```bash
# Letzte 31 Tage Preisdaten laden
fuel-cartel-monitor ingest --days 31

# Brent-Ölpreise laden
fuel-cartel-monitor ingest --brent

# Leader-Follower-Analyse für Hannover
fuel-cartel-monitor analyze leader-follower --lat 52.37 --lng 9.73

# Rockets-and-Feathers für Diesel
fuel-cartel-monitor analyze rockets-feathers --fuel diesel

# Dashboard-Daten exportieren (für GitHub Pages)
fuel-cartel-monitor export --days 31

# Datenbankstatistiken
fuel-cartel-monitor stats

# MCP-Server starten
fuel-cartel-monitor serve
```

## MCP-Server (Claude Desktop)

Alle Analysen sind als MCP-Tools verfügbar und rendern interaktive Charts direkt in Claude Desktop via [MCP UI](https://mcpui.dev/).

Claude Desktop Konfiguration (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "fuel-cartel-monitor": {
      "command": "uv",
      "args": ["--directory", "/pfad/zu/fuel-cartel-monitor", "run", "fuel-cartel-monitor", "serve"],
      "env": {
        "TANKERKOENIG_DATA_USER": "dein-benutzername",
        "TANKERKOENIG_DATA_PASS": "dein-api-key"
      }
    }
  }
}
```

| Tool | Beschreibung |
|------|-------------|
| `analyze_leader_follower` | Wer erhöht zuerst, wer folgt wie schnell? |
| `analyze_rockets_feathers` | Preisasymmetrie: große Erhöhungen, kleine Senkungen |
| `analyze_brent_decoupling` | Tankstellenpreis vs. Brent-Rohöl — die Gier-Lücke |
| `analyze_price_sync` | Preissynchronisation zwischen Stationen |
| `compare_regions` | Regionale Preisunterschiede (nach PLZ) |
| `station_price_history` | Preisverlauf einzelner Tankstellen |
| `ingest_data` | Daten laden |
| `database_stats` | Datenbankstatistiken |

## Tech-Stack

- **DuckDB** — analytische Datenbank, SQL-Macros für die Kernanalysen
- **MCP SDK** — Model Context Protocol Server mit [MCP UI](https://mcpui.dev/) Charts
- **httpx** — HTTP-Client für Daten-Downloads
- **Chart.js** — Dashboard-Visualisierungen
- **GitHub Actions** — tägliche automatische Aktualisierung
- **uv** — Python-Paketmanager

## Datenquellen

- **Tankstellenpreise:** [Tankerkönig](https://tankerkoenig.de/) · [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) · Markttransparenzstelle für Kraftstoffe (MTS-K), Bundeskartellamt
- **Brent-Rohölpreise:** [EIA](https://www.eia.gov/) (U.S. Energy Information Administration)
- **EUR/USD-Kurse:** [EZB](https://data.ecb.europa.eu/) (Europäische Zentralbank)

## Entwicklung

```bash
uv sync --extra dev
uv run pytest           # 28 Tests
uv run ruff check src tests
```

## Lizenz

MIT — siehe [LICENSE](LICENSE).

© 2026 [synapticore.io](https://synapticore.io)
