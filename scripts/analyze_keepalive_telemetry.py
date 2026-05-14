"""Deterministic analyzer for the OVE scraper's keepalive telemetry.

Parses logs/ove_scraper.log for the structured `KEEPALIVE_TICK` lines
(introduced 2026-05-01) and the `SAVED_SEARCH_TIMEOUT_STREAK` escalation
markers, then folds in current per-port lockout state to produce a
markdown report and a machine-readable JSON summary.

The script is intentionally LLM-free — counting and grouping are exact;
the LLM-driven recommendation step happens in the OpenClaw skill that
wraps this output.

Usage:
  python3 analyze_keepalive_telemetry.py [--since 3d] [--repo-root PATH] [--out-dir PATH]

Output:
  - artifacts/telemetry_reports/<UTC-ISO>.md  — full human-readable report
  - stdout (JSON) — machine summary the OpenClaw skill consumes

Designed to run from WSL OR Windows. Path resolution prefers the
explicit `--repo-root` flag, then `OVE_REPO_ROOT` env var, then walks up
from the script's own location to find the directory containing
`ove_scraper/`. Reads UTF-8; tolerates parse errors line-by-line.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


# ---- Log line patterns ------------------------------------------------------

# KEEPALIVE_TICK port=9223 outcome=goto_timeout duration_ms=60123
#                 url_at_failure=https://auth.manheim.com/... seed_url=about:blank
_KEEPALIVE_RE = re.compile(
    r"KEEPALIVE_TICK\s+"
    r"port=(?P<port>\d+)\s+"
    r"outcome=(?P<outcome>\S+)\s+"
    r"duration_ms=(?P<duration_ms>\d+)\s+"
    r"url_at_failure=(?P<url_at_failure>\S+)\s+"
    r"seed_url=(?P<seed_url>\S+)"
)

# SAVED_SEARCH_TIMEOUT_STREAK port=9223 streak=3 → escalating to recover_browser_session
_STREAK_RE = re.compile(
    r"SAVED_SEARCH_TIMEOUT_STREAK\s+"
    r"port=(?P<port>\d+)\s+"
    r"streak=(?P<streak>\d+)"
)

# Recovery outcome lines that immediately follow an escalation:
#   "recover_browser_session REFUSED by auth lockout (port 9223): ..."
#   "Recovery hit process-state failure ..."
# Plus generic success: a subsequent ok keepalive or successful sync within 5 min.
_RECOVER_REFUSED_RE = re.compile(
    r"recover_browser_session\s+REFUSED\s+by\s+auth\s+lockout\s+\(port\s+(?P<port>\d+)\)"
)

# Standard log line prefix: "2026-05-01 18:21:52,644 WARNING ..."
_LOG_TS_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})[,.]\d{3}\s+(?P<level>\w+)\s")


# ---- Path resolution --------------------------------------------------------


def _resolve_repo_root(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).resolve()
    env = os.environ.get("OVE_REPO_ROOT")
    if env:
        return Path(env).resolve()
    here = Path(__file__).resolve()
    for parent in (here.parent, *here.parents):
        if (parent / "ove_scraper").is_dir():
            return parent
    raise SystemExit(
        "Could not locate repo root (no `ove_scraper/` ancestor found). "
        "Pass --repo-root or set OVE_REPO_ROOT."
    )


def _parse_since(arg: str) -> timedelta | None:
    """Accepts '3d', '12h', '90m', or 'all' (None means no cutoff)."""
    if not arg or arg.lower() == "all":
        return None
    match = re.fullmatch(r"(\d+)([dhm])", arg.lower())
    if not match:
        raise SystemExit(f"--since must look like '3d', '12h', or '90m'; got {arg!r}")
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "d":
        return timedelta(days=value)
    if unit == "h":
        return timedelta(hours=value)
    return timedelta(minutes=value)


def _parse_log_timestamp(line: str) -> datetime | None:
    match = _LOG_TS_RE.match(line)
    if not match:
        return None
    try:
        # Logs use local time without tz; treat as UTC-naive and assume
        # Eastern. We don't have a local-tz lib guarantee here, so the
        # cutoff filter is approximate (within ~1 day either way is fine
        # for this use case).
        return datetime.fromisoformat(match.group("ts"))
    except ValueError:
        return None


# ---- Core parsing -----------------------------------------------------------


def parse_log(log_path: Path, cutoff: datetime | None) -> dict[str, Any]:
    """Stream the log, extracting KEEPALIVE_TICK + STREAK + REFUSED records.

    Returns a dict with raw records grouped for downstream analysis. Tolerant
    of malformed lines — anything we can't parse is skipped silently.
    """
    keepalive_records: list[dict[str, Any]] = []
    streak_records: list[dict[str, Any]] = []
    refused_records: list[dict[str, Any]] = []
    if not log_path.exists():
        return {
            "log_path": str(log_path),
            "log_exists": False,
            "keepalive": [],
            "streak": [],
            "refused": [],
            "lines_scanned": 0,
        }
    lines_scanned = 0
    with log_path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            lines_scanned += 1
            ts = _parse_log_timestamp(line)
            if cutoff and ts and ts < cutoff:
                continue
            ts_iso = ts.isoformat() if ts else None
            ka = _KEEPALIVE_RE.search(line)
            if ka:
                keepalive_records.append({
                    "ts": ts_iso,
                    "port": int(ka.group("port")),
                    "outcome": ka.group("outcome"),
                    "duration_ms": int(ka.group("duration_ms")),
                    "url_at_failure": ka.group("url_at_failure"),
                    "seed_url": ka.group("seed_url"),
                })
                continue
            sk = _STREAK_RE.search(line)
            if sk:
                streak_records.append({
                    "ts": ts_iso,
                    "port": int(sk.group("port")),
                    "streak": int(sk.group("streak")),
                })
                continue
            ref = _RECOVER_REFUSED_RE.search(line)
            if ref:
                refused_records.append({
                    "ts": ts_iso,
                    "port": int(ref.group("port")),
                })
    return {
        "log_path": str(log_path),
        "log_exists": True,
        "keepalive": keepalive_records,
        "streak": streak_records,
        "refused": refused_records,
        "lines_scanned": lines_scanned,
    }


def correlate_streak_outcomes(
    streak_records: list[dict[str, Any]],
    refused_records: list[dict[str, Any]],
    keepalive_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """For each STREAK escalation, decide whether recovery was refused
    by lockout, or appears to have succeeded (a subsequent ok keepalive
    on the same port within 5 min).

    Returns enriched streak records with `outcome` ∈
    {"refused_by_lockout", "appears_recovered", "unknown"}.
    """
    enriched: list[dict[str, Any]] = []
    for s in streak_records:
        port = s["port"]
        ts = s["ts"]
        s_dt = datetime.fromisoformat(ts) if ts else None
        outcome = "unknown"
        # Look for a refused line within 30s window after the streak.
        if s_dt is not None:
            for r in refused_records:
                if r["port"] != port or not r["ts"]:
                    continue
                r_dt = datetime.fromisoformat(r["ts"])
                if 0 <= (r_dt - s_dt).total_seconds() <= 30:
                    outcome = "refused_by_lockout"
                    break
            if outcome == "unknown":
                # Look for a subsequent ok keepalive on the same port
                # within 5 min — strong signal that recovery worked.
                for k in keepalive_records:
                    if k["port"] != port or k["outcome"] != "ok" or not k["ts"]:
                        continue
                    k_dt = datetime.fromisoformat(k["ts"])
                    delta = (k_dt - s_dt).total_seconds()
                    if 0 < delta <= 300:
                        outcome = "appears_recovered"
                        break
        enriched.append({**s, "outcome": outcome})
    return enriched


def summarize_keepalive(keepalive_records: list[dict[str, Any]]) -> dict[str, Any]:
    """Per-port outcome tallies + url_at_failure / seed_url distribution
    for the failure cases on each port."""
    by_port: dict[int, dict[str, Any]] = {}
    for k in keepalive_records:
        port = k["port"]
        bucket = by_port.setdefault(port, {
            "total": 0,
            "outcomes": Counter(),
            "url_at_failure_by_outcome": defaultdict(Counter),
            "seed_url_by_outcome": defaultdict(Counter),
            "duration_ms_max": 0,
            "duration_ms_sum": 0,
        })
        bucket["total"] += 1
        bucket["outcomes"][k["outcome"]] += 1
        bucket["duration_ms_sum"] += k["duration_ms"]
        bucket["duration_ms_max"] = max(bucket["duration_ms_max"], k["duration_ms"])
        if k["outcome"] != "ok":
            bucket["url_at_failure_by_outcome"][k["outcome"]][k["url_at_failure"]] += 1
            bucket["seed_url_by_outcome"][k["outcome"]][k["seed_url"]] += 1
    out: dict[str, Any] = {}
    for port, bucket in sorted(by_port.items()):
        out[str(port)] = {
            "total": bucket["total"],
            "outcomes": dict(bucket["outcomes"]),
            "duration_ms_avg": int(bucket["duration_ms_sum"] / bucket["total"]) if bucket["total"] else 0,
            "duration_ms_max": bucket["duration_ms_max"],
            "url_at_failure_by_outcome": {
                outcome: dict(counter) for outcome, counter in bucket["url_at_failure_by_outcome"].items()
            },
            "seed_url_by_outcome": {
                outcome: dict(counter) for outcome, counter in bucket["seed_url_by_outcome"].items()
            },
        }
    return out


def read_lockout_state(state_dir: Path) -> dict[str, Any]:
    """Read the three per-port + global lockout files. Missing files are
    reported as `null` rather than treated as errors."""
    files = {
        "global": state_dir / "auth_lockout_global.json",
        "9222": state_dir / "auth_lockout_9222.json",
        "9223": state_dir / "auth_lockout_9223.json",
    }
    out: dict[str, Any] = {}
    for label, path in files.items():
        if not path.exists():
            out[label] = None
            continue
        try:
            out[label] = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            out[label] = {"_read_error": str(exc)}
    return out


# ---- Reporting --------------------------------------------------------------


def _fmt_outcomes_block(outcomes: dict[str, int]) -> str:
    if not outcomes:
        return "  (no telemetry observed)\n"
    total = sum(outcomes.values())
    lines = []
    for outcome, count in sorted(outcomes.items(), key=lambda kv: (-kv[1], kv[0])):
        pct = (count / total) * 100
        lines.append(f"  - `{outcome}` — {count} ({pct:.1f}%)")
    return "\n".join(lines) + "\n"


def _fmt_url_distribution(by_outcome: dict[str, dict[str, int]]) -> str:
    if not by_outcome:
        return "  (no failures observed — nothing to attribute to a URL)\n"
    lines = []
    for outcome in sorted(by_outcome.keys()):
        urls = by_outcome[outcome]
        if not urls:
            continue
        lines.append(f"  Outcome `{outcome}`:")
        for url, count in sorted(urls.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"    - `{url}` × {count}")
    return ("\n".join(lines) + "\n") if lines else "  (no failures with URL captures)\n"


def render_markdown_report(
    summary: dict[str, Any],
    streak_enriched: list[dict[str, Any]],
    lockout_state: dict[str, Any],
    log_path: str,
    log_exists: bool,
    lines_scanned: int,
    since_label: str,
    generated_at: datetime,
) -> str:
    lines: list[str] = []
    lines.append(f"# Keepalive telemetry analysis — {generated_at.isoformat(timespec='seconds')}")
    lines.append("")
    lines.append(f"- Log scanned: `{log_path}` ({'OK' if log_exists else 'MISSING'}, {lines_scanned} lines)")
    lines.append(f"- Window: `{since_label}`")
    lines.append("")

    lines.append("## KEEPALIVE_TICK outcomes per port")
    lines.append("")
    if not summary:
        lines.append("_No KEEPALIVE_TICK lines found in the log window. Either the new code hasn't run yet, the keepalive interval hasn't elapsed since restart, or the scraper isn't running._")
        lines.append("")
    else:
        for port_str, stats in sorted(summary.items()):
            label_b = " (Login B / sync)" if port_str == "9223" else " (Login A / primary)" if port_str == "9222" else ""
            lines.append(f"### Port {port_str}{label_b}")
            lines.append("")
            lines.append(f"- Total ticks: **{stats['total']}**")
            lines.append(f"- Mean duration: {stats['duration_ms_avg']} ms (max {stats['duration_ms_max']} ms)")
            lines.append("- Outcomes:")
            lines.append(_fmt_outcomes_block(stats["outcomes"]))
            lines.append("- URL at failure (per outcome):")
            lines.append(_fmt_url_distribution(stats["url_at_failure_by_outcome"]))
            lines.append("- Seed URL at failure (per outcome):")
            lines.append(_fmt_url_distribution(stats["seed_url_by_outcome"]))
            lines.append("")

    lines.append("## SAVED_SEARCH_TIMEOUT_STREAK escalations")
    lines.append("")
    if not streak_enriched:
        lines.append("_No streak escalations observed. Either the saved-search goto hasn't repeatedly failed, or recovery was triggered by a different code path before the streak threshold._")
        lines.append("")
    else:
        per_port: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for s in streak_enriched:
            per_port[s["port"]].append(s)
        for port in sorted(per_port.keys()):
            entries = per_port[port]
            lines.append(f"### Port {port} — {len(entries)} escalation(s)")
            lines.append("")
            outcome_counter: Counter = Counter(s["outcome"] for s in entries)
            for outcome, count in outcome_counter.most_common():
                lines.append(f"- `{outcome}` × {count}")
            lines.append("")
            lines.append("Recent escalations (up to 10):")
            for s in entries[-10:]:
                lines.append(f"  - `{s['ts']}` streak={s['streak']} outcome=`{s['outcome']}`")
            lines.append("")

    lines.append("## Auth lockout state (current)")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(lockout_state, indent=2, sort_keys=True))
    lines.append("```")
    lines.append("")

    return "\n".join(lines)


# ---- Entry point ------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default="3d", help="How far back to scan: '3d', '12h', '90m', or 'all' (default: 3d)")
    parser.add_argument("--repo-root", default=None, help="Override repo root (default: auto-detect)")
    parser.add_argument("--out-dir", default=None, help="Override report output dir (default: artifacts/telemetry_reports)")
    parser.add_argument("--log-path", default=None, help="Override scraper log path (default: logs/ove_scraper.log)")
    parser.add_argument("--quiet", action="store_true", help="Suppress JSON-to-stdout summary (still writes report file)")
    args = parser.parse_args(argv)

    repo_root = _resolve_repo_root(args.repo_root)
    log_path = Path(args.log_path).resolve() if args.log_path else (repo_root / "logs" / "ove_scraper.log")
    out_dir = Path(args.out_dir).resolve() if args.out_dir else (repo_root / "artifacts" / "telemetry_reports")
    state_dir = repo_root / "artifacts" / "_state"

    delta = _parse_since(args.since)
    cutoff = datetime.now() - delta if delta else None

    parsed = parse_log(log_path, cutoff)
    summary = summarize_keepalive(parsed["keepalive"])
    streak_enriched = correlate_streak_outcomes(
        parsed["streak"], parsed["refused"], parsed["keepalive"]
    )
    lockout_state = read_lockout_state(state_dir)
    generated_at = datetime.now(timezone.utc)
    report_md = render_markdown_report(
        summary=summary,
        streak_enriched=streak_enriched,
        lockout_state=lockout_state,
        log_path=parsed["log_path"],
        log_exists=parsed["log_exists"],
        lines_scanned=parsed["lines_scanned"],
        since_label=args.since,
        generated_at=generated_at,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / f"{generated_at.strftime('%Y-%m-%dT%H-%M-%SZ')}.md"
    report_path.write_text(report_md, encoding="utf-8")

    machine_summary: dict[str, Any] = {
        "generated_at_utc": generated_at.isoformat(),
        "since": args.since,
        "report_path": str(report_path),
        "log_path": parsed["log_path"],
        "log_exists": parsed["log_exists"],
        "lines_scanned": parsed["lines_scanned"],
        "keepalive_summary": summary,
        "streak_escalations": streak_enriched,
        "lockout_state": lockout_state,
    }
    if not args.quiet:
        print(json.dumps(machine_summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
