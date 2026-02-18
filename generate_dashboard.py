import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import defaultdict

load_dotenv()

token = os.getenv("HUBSPOT_API_TOKEN")
headers = {"Authorization": f"Bearer {token}"}

SDR_PIPELINE_ID = "194381550"
EXCLUDE_OWNERS = ["Damian Jagusiak"]

STAGES = {
    "344689645": "New Lead",
    "346880461": "In Progress",
    "344689648": "SDR Call Scheduled",
    "344689652": "MQL",
    "344689650": "Kwalka (SQL)",
    "3981279427": "Sales Won",
    "3938055393": "Sales Lost",
    "344689651": "Lost Before MQL",
}

DATE_ENTERED_FIELDS = {
    "hs_v2_date_entered_344689645": "New Lead",
    "hs_v2_date_entered_346880461": "In Progress",
    "hs_v2_date_entered_344689648": "SDR Call Scheduled",
    "hs_v2_date_entered_344689652": "MQL",
    "hs_v2_date_entered_344689650": "Kwalka (SQL)",
    "hs_v2_date_entered_3981279427": "Sales Won",
    "hs_v2_date_entered_3938055393": "Sales Lost",
    "hs_v2_date_entered_344689651": "Lost Before MQL",
}

PROPERTIES = [
    "dealname", "dealstage", "hubspot_owner_id", "createdate",
    "closedate", "hs_lastmodifieddate", "amount",
    "lost_reason", "lost_description", "closed_lost_reason",
] + list(DATE_ENTERED_FIELDS.keys())


def get_report_date():
    """Zwraca datę poprzedniego dnia roboczego"""
    if os.getenv("REPORT_DATE"):
        return os.getenv("REPORT_DATE")
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def get_owners():
    r = requests.get("https://api.hubapi.com/crm/v3/owners?limit=200", headers=headers, timeout=30)
    owners = {}
    for o in r.json().get("results", []):
        owners[o["id"]] = f"{o.get('firstName', '')} {o.get('lastName', '')}".strip()
    return owners


def fetch_deals(report_date):
    all_deals = []
    after = None
    date_start = f"{report_date}T00:00:00.000Z"
    date_end = f"{report_date}T23:59:59.999Z"

    while True:
        payload = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": "pipeline", "operator": "EQ", "value": SDR_PIPELINE_ID},
                    {"propertyName": "hs_lastmodifieddate", "operator": "GTE", "value": date_start},
                    {"propertyName": "hs_lastmodifieddate", "operator": "LTE", "value": date_end},
                ]
            }],
            "properties": PROPERTIES,
            "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "DESCENDING"}],
            "limit": 100
        }
        if after:
            payload["after"] = after

        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/deals/search",
            headers=headers, json=payload, timeout=30
        )
        if r.status_code != 200:
            print(f"API error: {r.status_code}")
            break

        data = r.json()
        all_deals.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return all_deals


def is_date_match(date_str, target_date):
    if not date_str:
        return False
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d") == target_date
    except:
        return False


def process_deals(all_deals, owners, report_date):
    today_deals = []
    for deal in all_deals:
        props = deal["properties"]
        owner_name = owners.get(props.get("hubspot_owner_id"), "Nieznany")
        if owner_name in EXCLUDE_OWNERS:
            continue

        stage_changes = {}
        for field, stage_name in DATE_ENTERED_FIELDS.items():
            if is_date_match(props.get(field), report_date):
                stage_changes[stage_name] = props.get(field)

        if stage_changes:
            today_deals.append({
                "name": props.get("dealname", "?"),
                "current_stage": STAGES.get(props.get("dealstage"), props.get("dealstage")),
                "owner_name": owner_name,
                "stage_changes": stage_changes,
                "lost_reason": props.get("lost_reason") or props.get("closed_lost_reason") or "",
                "lost_description": props.get("lost_description") or "",
            })
    return today_deals


def calc_stats(deals):
    new_l = sum(1 for d in deals if "New Lead" in d["stage_changes"])
    mql = sum(1 for d in deals if "MQL" in d["stage_changes"])
    sql = sum(1 for d in deals if "Kwalka (SQL)" in d["stage_changes"])
    won = sum(1 for d in deals if "Sales Won" in d["stage_changes"])
    lost_bm = sum(1 for d in deals if "Lost Before MQL" in d["stage_changes"])
    lost_s = sum(1 for d in deals if "Sales Lost" in d["stage_changes"])

    lead_mql = sum(1 for d in deals if "New Lead" in d["stage_changes"] and "MQL" in d["stage_changes"])
    mql_sql = sum(1 for d in deals if "MQL" in d["stage_changes"] and "Kwalka (SQL)" in d["stage_changes"])
    lead_sql = sum(1 for d in deals if "New Lead" in d["stage_changes"] and "Kwalka (SQL)" in d["stage_changes"])

    def pct(a, b):
        return f"{a/b*100:.0f}%" if b > 0 else "-"

    return {
        "total": len(deals),
        "new_lead": new_l,
        "mql": mql,
        "sql": sql,
        "won": won,
        "lost_before_mql": lost_bm,
        "sales_lost": lost_s,
        "lost_total": lost_bm + lost_s,
        "lead_mql": f"{lead_mql}/{new_l} ({pct(lead_mql, new_l)})" if new_l > 0 else "-",
        "mql_sql": f"{mql_sql}/{mql} ({pct(mql_sql, mql)})" if mql > 0 else "-",
        "lead_sql": f"{lead_sql}/{new_l} ({pct(lead_sql, new_l)})" if new_l > 0 else "-",
    }


def generate_html(today_deals, report_date):
    by_owner = defaultdict(list)
    for d in today_deals:
        by_owner[d["owner_name"]].append(d)

    total_stats = calc_stats(today_deals)

    # Lost reasons
    all_lost = [d for d in today_deals if "Sales Lost" in d["stage_changes"] or "Lost Before MQL" in d["stage_changes"]]
    reason_counts = defaultdict(int)
    for d in all_lost:
        reason_counts[d["lost_reason"] or "Brak powodu"] += 1
    sorted_reasons = sorted(reason_counts.items(), key=lambda x: -x[1])

    # SDR rows
    sdr_rows = []
    for owner_name in sorted(by_owner.keys(), key=lambda x: -len(by_owner[x])):
        deals = by_owner[owner_name]
        stats = calc_stats(deals)
        lost_deals = [d for d in deals if "Sales Lost" in d["stage_changes"] or "Lost Before MQL" in d["stage_changes"]]
        sdr_rows.append({
            "name": owner_name,
            "stats": stats,
            "deals": deals,
            "lost_deals": lost_deals,
        })

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SDR Pipeline Dashboard - {report_date}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f172a;
            color: #e2e8f0;
            min-height: 100vh;
        }}
        .header {{
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border-bottom: 1px solid #334155;
            padding: 24px 40px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .header h1 {{
            font-size: 24px;
            font-weight: 700;
            color: #f1f5f9;
        }}
        .header .date {{
            font-size: 14px;
            color: #94a3b8;
        }}
        .header .badge {{
            background: #3b82f6;
            color: white;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 600;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 24px 40px; }}

        /* KPI Cards */
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
        }}
        .kpi-card {{
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 20px;
            text-align: center;
        }}
        .kpi-card .value {{
            font-size: 32px;
            font-weight: 700;
            margin-bottom: 4px;
        }}
        .kpi-card .label {{
            font-size: 12px;
            color: #94a3b8;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .kpi-card.blue .value {{ color: #3b82f6; }}
        .kpi-card.green .value {{ color: #22c55e; }}
        .kpi-card.red .value {{ color: #ef4444; }}
        .kpi-card.orange .value {{ color: #f59e0b; }}
        .kpi-card.purple .value {{ color: #a78bfa; }}

        /* Conversion Cards */
        .conv-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
        }}
        .conv-card {{
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 20px;
            text-align: center;
        }}
        .conv-card .conv-label {{
            font-size: 13px;
            color: #94a3b8;
            margin-bottom: 8px;
        }}
        .conv-card .conv-value {{
            font-size: 24px;
            font-weight: 700;
            color: #3b82f6;
        }}
        .conv-card .conv-arrow {{
            color: #64748b;
            font-size: 18px;
        }}

        /* Section */
        .section {{
            margin-bottom: 32px;
        }}
        .section h2 {{
            font-size: 18px;
            font-weight: 600;
            color: #f1f5f9;
            margin-bottom: 16px;
            padding-bottom: 8px;
            border-bottom: 2px solid #3b82f6;
            display: inline-block;
        }}

        /* Table */
        table {{
            width: 100%;
            border-collapse: collapse;
            background: #1e293b;
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid #334155;
        }}
        thead th {{
            background: #334155;
            color: #e2e8f0;
            padding: 12px 16px;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 600;
            text-align: center;
        }}
        thead th:first-child {{ text-align: left; }}
        tbody td {{
            padding: 10px 16px;
            font-size: 14px;
            text-align: center;
            border-bottom: 1px solid #1e293b;
        }}
        tbody td:first-child {{ text-align: left; font-weight: 500; }}
        tbody tr:nth-child(even) {{ background: #1a2332; }}
        tbody tr:hover {{ background: #263548; }}
        .text-green {{ color: #22c55e; font-weight: 600; }}
        .text-red {{ color: #ef4444; font-weight: 600; }}
        .text-blue {{ color: #3b82f6; font-weight: 600; }}
        .text-orange {{ color: #f59e0b; font-weight: 600; }}

        /* SDR Detail Cards */
        .sdr-cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(450px, 1fr)); gap: 20px; }}
        .sdr-card {{
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 12px;
            overflow: hidden;
        }}
        .sdr-card-header {{
            background: #334155;
            padding: 16px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .sdr-card-header h3 {{ font-size: 16px; font-weight: 600; }}
        .sdr-card-header .deal-count {{
            background: #3b82f6;
            color: white;
            padding: 2px 10px;
            border-radius: 12px;
            font-size: 13px;
        }}
        .sdr-card-body {{ padding: 16px 20px; }}
        .sdr-stat-row {{
            display: flex;
            justify-content: space-between;
            padding: 6px 0;
            border-bottom: 1px solid #263548;
            font-size: 14px;
        }}
        .sdr-stat-row:last-child {{ border-bottom: none; }}
        .sdr-stat-label {{ color: #94a3b8; }}

        /* Lost reason */
        .lost-item {{
            background: #1a1a2e;
            border-left: 3px solid #ef4444;
            padding: 10px 14px;
            margin-bottom: 8px;
            border-radius: 0 8px 8px 0;
        }}
        .lost-item .deal-name {{ font-weight: 600; font-size: 13px; color: #f1f5f9; }}
        .lost-item .lost-meta {{ font-size: 12px; color: #94a3b8; margin-top: 4px; }}

        /* Reason bar */
        .reason-bar {{
            display: flex;
            align-items: center;
            margin-bottom: 8px;
            gap: 12px;
        }}
        .reason-bar .bar-label {{ min-width: 180px; font-size: 14px; }}
        .reason-bar .bar-track {{
            flex: 1;
            background: #334155;
            height: 24px;
            border-radius: 6px;
            overflow: hidden;
        }}
        .reason-bar .bar-fill {{
            height: 100%;
            background: linear-gradient(90deg, #ef4444, #f87171);
            border-radius: 6px;
            display: flex;
            align-items: center;
            padding: 0 8px;
            font-size: 12px;
            font-weight: 600;
            min-width: 30px;
        }}
        .reason-bar .bar-count {{ min-width: 40px; text-align: right; font-weight: 600; }}

        .footer {{
            text-align: center;
            padding: 24px;
            color: #64748b;
            font-size: 12px;
            border-top: 1px solid #334155;
            margin-top: 40px;
        }}

        /* Collapsible */
        details {{ margin-top: 12px; }}
        summary {{
            cursor: pointer;
            font-size: 13px;
            color: #3b82f6;
            padding: 4px 0;
        }}
        summary:hover {{ color: #60a5fa; }}
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h1>SDR Pipeline Dashboard</h1>
            <div class="date">Dane z dnia: <strong>{report_date}</strong> | Wygenerowano: {generated_at}</div>
        </div>
        <div class="badge">{total_stats['total']} deali</div>
    </div>

    <div class="container">
        <!-- KPI -->
        <div class="kpi-grid">
            <div class="kpi-card blue">
                <div class="value">{total_stats['new_lead']}</div>
                <div class="label">Nowe Leady</div>
            </div>
            <div class="kpi-card purple">
                <div class="value">{total_stats['mql']}</div>
                <div class="label">MQL</div>
            </div>
            <div class="kpi-card green">
                <div class="value">{total_stats['sql']}</div>
                <div class="label">SQL (Kwalka)</div>
            </div>
            <div class="kpi-card green">
                <div class="value">{total_stats['won']}</div>
                <div class="label">Sales Won</div>
            </div>
            <div class="kpi-card orange">
                <div class="value">{total_stats['lost_before_mql']}</div>
                <div class="label">Lost Before MQL</div>
            </div>
            <div class="kpi-card red">
                <div class="value">{total_stats['sales_lost']}</div>
                <div class="label">Sales Lost</div>
            </div>
            <div class="kpi-card red">
                <div class="value">{total_stats['lost_total']}</div>
                <div class="label">Lost Total</div>
            </div>
            <div class="kpi-card">
                <div class="value" style="color:#f1f5f9">{len(by_owner)}</div>
                <div class="label">Aktywni SDR-owie</div>
            </div>
        </div>

        <!-- Konwersje ogolne -->
        <div class="conv-grid">
            <div class="conv-card">
                <div class="conv-label">Lead <span class="conv-arrow">\u2192</span> MQL</div>
                <div class="conv-value">{total_stats['lead_mql']}</div>
            </div>
            <div class="conv-card">
                <div class="conv-label">MQL <span class="conv-arrow">\u2192</span> SQL</div>
                <div class="conv-value">{total_stats['mql_sql']}</div>
            </div>
            <div class="conv-card">
                <div class="conv-label">Lead <span class="conv-arrow">\u2192</span> SQL</div>
                <div class="conv-value">{total_stats['lead_sql']}</div>
            </div>
        </div>

        <!-- Tabela konwersji per SDR -->
        <div class="section">
            <h2>Konwersje per SDR</h2>
            <table>
                <thead>
                    <tr>
                        <th>SDR</th>
                        <th>Deale</th>
                        <th>New Lead</th>
                        <th>MQL</th>
                        <th>SQL</th>
                        <th>Won</th>
                        <th>Lost</th>
                        <th>Lead\u2192MQL</th>
                        <th>MQL\u2192SQL</th>
                        <th>Lead\u2192SQL</th>
                    </tr>
                </thead>
                <tbody>"""

    for sdr in sdr_rows:
        s = sdr["stats"]
        html += f"""
                    <tr>
                        <td>{sdr['name']}</td>
                        <td>{s['total']}</td>
                        <td>{s['new_lead']}</td>
                        <td class="text-blue">{s['mql']}</td>
                        <td class="text-green">{s['sql']}</td>
                        <td class="text-green">{s['won']}</td>
                        <td class="text-red">{s['lost_total']}</td>
                        <td class="text-blue">{s['lead_mql']}</td>
                        <td class="text-blue">{s['mql_sql']}</td>
                        <td class="text-blue">{s['lead_sql']}</td>
                    </tr>"""

    html += """
                </tbody>
            </table>
        </div>

        <!-- Przyczyny lostow -->
        <div class="section">
            <h2>Przyczyny Lost\u00f3w</h2>"""

    if sorted_reasons:
        max_count = sorted_reasons[0][1] if sorted_reasons else 1
        for reason, count in sorted_reasons:
            pct = count / len(all_lost) * 100
            bar_w = count / max_count * 100
            html += f"""
            <div class="reason-bar">
                <div class="bar-label">{reason}</div>
                <div class="bar-track">
                    <div class="bar-fill" style="width:{bar_w}%">{pct:.0f}%</div>
                </div>
                <div class="bar-count">{count}</div>
            </div>"""
    else:
        html += '<p style="color:#94a3b8">Brak lost\u00f3w w tym dniu</p>'

    html += """
        </div>

        <!-- Szczegoly per SDR -->
        <div class="section">
            <h2>Szczeg\u00f3\u0142y per SDR</h2>
            <div class="sdr-cards">"""

    for sdr in sdr_rows:
        s = sdr["stats"]
        html += f"""
                <div class="sdr-card">
                    <div class="sdr-card-header">
                        <h3>{sdr['name']}</h3>
                        <span class="deal-count">{s['total']} deali</span>
                    </div>
                    <div class="sdr-card-body">
                        <div class="sdr-stat-row"><span class="sdr-stat-label">Nowe leady</span><span>{s['new_lead']}</span></div>
                        <div class="sdr-stat-row"><span class="sdr-stat-label">MQL</span><span class="text-blue">{s['mql']}</span></div>
                        <div class="sdr-stat-row"><span class="sdr-stat-label">SQL (Kwalka)</span><span class="text-green">{s['sql']}</span></div>
                        <div class="sdr-stat-row"><span class="sdr-stat-label">Sales Won</span><span class="text-green">{s['won']}</span></div>
                        <div class="sdr-stat-row"><span class="sdr-stat-label">Lost</span><span class="text-red">{s['lost_total']}</span></div>
                        <div class="sdr-stat-row"><span class="sdr-stat-label">Lead \u2192 MQL</span><span class="text-blue">{s['lead_mql']}</span></div>
                        <div class="sdr-stat-row"><span class="sdr-stat-label">MQL \u2192 SQL</span><span class="text-blue">{s['mql_sql']}</span></div>
                        <div class="sdr-stat-row"><span class="sdr-stat-label">Lead \u2192 SQL</span><span class="text-blue">{s['lead_sql']}</span></div>"""

        if sdr["lost_deals"]:
            html += """
                        <details>
                            <summary>Pokaż przyczyny lostów</summary>"""
            for d in sdr["lost_deals"]:
                reason = d["lost_reason"] or "Brak powodu"
                desc = d["lost_description"] or ""
                lost_type = "Sales Lost" if "Sales Lost" in d["stage_changes"] else "Lost Before MQL"
                html += f"""
                            <div class="lost-item">
                                <div class="deal-name">{d['name'][:60]}</div>
                                <div class="lost-meta">{lost_type} | {reason}</div>"""
                if desc:
                    html += f'<div class="lost-meta">{desc[:120]}</div>'
                html += "</div>"
            html += """
                        </details>"""

        # Deals list
        html += """
                        <details>
                            <summary>Pokaż listę deali</summary>
                            <div style="margin-top:8px">"""
        for d in sdr["deals"]:
            stages = ", ".join(d["stage_changes"].keys())
            html += f"""
                                <div style="padding:4px 0;border-bottom:1px solid #263548;font-size:13px">
                                    <span style="color:#f1f5f9">{d['name'][:50]}</span>
                                    <span style="color:#64748b;margin-left:8px">({d['current_stage']})</span>
                                    <div style="color:#94a3b8;font-size:11px">{stages}</div>
                                </div>"""
        html += """
                            </div>
                        </details>"""

        html += """
                    </div>
                </div>"""

    html += f"""
            </div>
        </div>
    </div>

    <div class="footer">
        SDR Pipeline Dashboard | Dane z HubSpot API | Wygenerowano: {generated_at}
    </div>
</body>
</html>"""

    return html


def main():
    report_date = get_report_date()
    print(f"Generowanie dashboardu dla daty: {report_date}")

    owners = get_owners()
    all_deals = fetch_deals(report_date)
    print(f"Pobrano {len(all_deals)} deali")

    today_deals = process_deals(all_deals, owners, report_date)
    print(f"Deale ze zmianą etapu: {len(today_deals)}")

    html = generate_html(today_deals, report_date)

    output_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(output_dir, "index.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Dashboard zapisany: {output_path}")


if __name__ == "__main__":
    main()
