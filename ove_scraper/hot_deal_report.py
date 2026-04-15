"""Formatting for Hot Deal pipeline run summaries."""
from __future__ import annotations

from typing import Any


def format_hot_deal_summary(run_summary: dict[str, Any], hot_deals: list[dict]) -> str:
    """Plain-text summary for logs and Telegram."""
    lines = [
        "=== Hot Deal Screening Summary ===",
        f"Run ID: {run_summary.get('run_id', 'N/A')}",
        f"Status: {run_summary.get('status', 'N/A')}",
        f"Started: {run_summary.get('started_at', 'N/A')}",
        f"Finished: {run_summary.get('finished_at', 'N/A')}",
        "",
        f"Total VINs screened: {run_summary.get('total_vins', 0)}",
        f"Hot Deals found: {run_summary.get('hot_deals', 0)}",
        f"Rejected at Step 1 (CR): {run_summary.get('step1_fail', 0)}",
        f"Rejected at Step 2 (AutoCheck): {run_summary.get('step2_fail', 0)}",
        f"Rejected at Step 3 (Web search): {run_summary.get('step3_fail', 0)}",
        f"Still pending: {run_summary.get('pending', 0)}",
    ]

    if hot_deals:
        lines.append("")
        lines.append("--- Hot Deal Vehicles ---")
        for v in hot_deals:
            price_str = f"${v['price']:,.0f}" if v.get("price") else "N/A"
            odo_str = f"{v['odometer']:,} mi" if v.get("odometer") else "N/A"
            lines.append(
                f"  {v['vin']}  {v.get('year', '')} {v.get('make', '')} {v.get('model', '')} "
                f"{v.get('trim', '') or ''}  |  {odo_str}  |  {price_str}  |  {v.get('location', '')}"
            )

    return "\n".join(lines)


def format_hot_deal_email_html(run_summary: dict[str, Any], hot_deals: list[dict]) -> str:
    """HTML email body for the daily screening notification."""
    total = run_summary.get("total_vins", 0)
    found = run_summary.get("hot_deals", 0)
    s1 = run_summary.get("step1_fail", 0)
    s2 = run_summary.get("step2_fail", 0)
    s3 = run_summary.get("step3_fail", 0)

    rows_html = ""
    for v in hot_deals:
        price_str = f"${v['price']:,.0f}" if v.get("price") else "N/A"
        odo_str = f"{v['odometer']:,}" if v.get("odometer") else "N/A"
        rows_html += (
            f"<tr>"
            f"<td>{v['vin']}</td>"
            f"<td>{v.get('year', '')}</td>"
            f"<td>{v.get('make', '')} {v.get('model', '')} {v.get('trim', '') or ''}</td>"
            f"<td>{odo_str}</td>"
            f"<td>{price_str}</td>"
            f"<td>{v.get('location', '')}</td>"
            f"</tr>"
        )

    return f"""<html>
<body style="font-family: Arial, sans-serif; max-width: 800px;">
<h2>Hot Deal Screening Complete</h2>
<p><strong>{found}</strong> vehicles passed all 3 screening steps out of <strong>{total}</strong> total.</p>
<table style="border-collapse: collapse; margin: 10px 0;">
<tr><td>Rejected at CR screen:</td><td><strong>{s1}</strong></td></tr>
<tr><td>Rejected at AutoCheck:</td><td><strong>{s2}</strong></td></tr>
<tr><td>Rejected at web search:</td><td><strong>{s3}</strong></td></tr>
</table>
{f'''
<h3>Hot Deal Vehicles</h3>
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">
<tr style="background: #f0f0f0;">
<th>VIN</th><th>Year</th><th>Vehicle</th><th>Miles</th><th>Price</th><th>State</th>
</tr>
{rows_html}
</table>
''' if hot_deals else '<p>No vehicles passed all screening steps in this run.</p>'}
<p style="color: #888; font-size: 12px;">Run ID: {run_summary.get("run_id", "N/A")}</p>
</body></html>"""
