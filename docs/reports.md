# Reporting & Visualizations

strobengine automatically persists execution reports as JSON artifacts
and optionally generates standalone HTML reports with interactive
Chart.js visualizations and historical baseline comparisons.

## Implicit Report Persistence

After every load test, strobengine writes a JSON report artifact
to disk by default.

| Property | Value |
|----------|-------|
| Default directory | `./.strobengine/reports/` |
| Filename pattern | `report_YYYYMMDD_HHMMSS_<host>.json` |
| Atomic writes | Yes — tmp file + rename prevents corruption |
| `latest.json` pointer | Yes — tracks most recent report |

Reports are written by both the Rust engine (`run_load_test`) and
the Python reporter (`save_report`).

## Configuration Flags

These flags appear on all three subcommands (`load`, `stress`, `spike`):

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--output-dir <DIR>` | string | `.strobengine/reports/` | Override report storage directory |
| `--no-save` | flag | false | Disable disk persistence entirely |
| `--html <PATH>` | string | none | Generate standalone HTML report at filepath |
| `--compare-to <PATH>` | string | none | Baseline JSON report for comparison |

## JSON Artifact Schema

### Structure

| Section | Key Fields |
|---------|------------|
| `metadata` | `timestamp`, `duration_secs`, `target_url`, `cli_options`, `system_info` |
| `cli_options` | `method`, `concurrency`, `timeout_secs`, `chaos`, `chaos_rate`, `body`, `headers` |
| `system_info` | `hostname`, `platform`, `version` |
| `summary` | `total_requests`, `successful_requests`, `failed_requests`, `rps`, `bytes_transferred` |
| `latency_percentiles` | `p50_us`, `p90_us`, `p95_us`, `p99_us`, `min_us`, `max_us`, `mean_us` |
| `error_breakdown` | Status code string → count (e.g., `"200": 950, "500": 50`) |

All latency values are stored in **microseconds**.

### Example

```json
{
  "metadata": {
    "timestamp": "2026-08-28T09:48:19Z",
    "duration_secs": 30.0,
    "target_url": "http://localhost:8080/api",
    "cli_options": { "method": "GET", "concurrency": 50, "chaos": false },
    "system_info": { "hostname": "ci-runner-01", "platform": "linux", "version": "0.5.0" }
  },
  "summary": { "total_requests": 1500, "successful_requests": 1485, "failed_requests": 15, "rps": 50.0, "bytes_transferred": 524288 },
  "latency_percentiles": { "p50_us": 1200.0, "p90_us": 3500.0, "p95_us": 5200.0, "p99_us": 9800.0, "min_us": 200.0, "max_us": 15000.0, "mean_us": 2800.0 },
  "error_breakdown": { "200": 1485, "500": 15 }
}
```

## HTML Report Generation

### Dependencies

- **Chart.js** — bundled locally at `src/strobengine/reporting/assets/chart.min.js`
  (186KB, embedded inline in generated HTML — 100% offline, no CDN required)
- **Jinja2** — used for template rendering (`jinja2>=3.1`)

### CLI Usage

```bash
strobengine load http://localhost:8080/api -c 10 -d 30 --html report.html
```

### Features

- **Self-contained single-file HTML** — no external dependencies
- **Embedded Chart.js** — 100% offline, no internet required
- **Dark theme** — slate/navy color palette
- **Bar chart**: Latency percentiles (P50, P90, P95, P99) in milliseconds
- **Doughnut chart**: Status code distribution (2xx, 4xx, 5xx, Other)
- **Metadata grid**: Target URL, method, concurrency, duration, RPS, error rate

### Python API

```python
from strobengine import StrobEngine
from strobengine.reporting.html_report import save_html_report

engine = StrobEngine(url="http://localhost:8080", concurrency=10, duration=30)
summary = engine.run()
filepath = save_html_report(summary, engine.get_config(), "report.html")
```

## Historical Baseline Comparison

### CLI Usage

```bash
# First run — generates baseline report
strobengine load http://localhost:8080/api -c 10 -d 30

# Second run — compare against baseline (terminal output)
strobengine load http://localhost:8080/api -c 10 -d 30 \
  --compare-to .strobengine/reports/report_baseline.json

# Second run — compare with HTML report
strobengine load http://localhost:8080/api -c 10 -d 30 \
  --html report.html --compare-to .strobengine/reports/report_baseline.json
```

### Terminal Output

`--compare-to` displays a Rich table comparing baseline vs current metrics,
even without `--html`. The table includes:

| Metric | Baseline | Current | Delta |
|--------|----------|---------|-------|
| RPS | 33.00 | 40.00 | +21.21% (green) |
| P95 Latency | 35.00 ms | 25.00 ms | -28.57% (green) |
| Error Rate | 10.00% | 5.00% | -5.00pp (green) |

Color coding:
- **Green**: Improvement (higher RPS, lower latency, fewer errors)
- **Red**: Regression (lower RPS, higher latency, more errors)
- **Gray**: No change (0% delta)

### HTML Output

When used with `--html`, the comparison section is embedded in the report
with the same color-coded deltas.

### Delta Calculation

| Metric | Formula | Unit |
|--------|---------|------|
| RPS | `((current - baseline) / baseline) * 100` | % (positive = improvement) |
| P95 Latency | `((current - baseline) / baseline) * 100` | % (negative = improvement) |
| Error Rate | `current_rate - baseline_rate` | pp (negative = improvement) |

## Limitations

- Baseline comparison assumes same target URL (cross-URL comparison not supported)
- Report schema version is not tracked (no migration support)
- `--compare-to` requires the baseline file to be a valid strobengine JSON artifact
