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
        # Raw values for aggregation in JS
        "lead_mql_num": lead_mql,
        "mql_sql_num": mql_sql,
        "lead_sql_num": lead_sql,
    }


def build_json(today_deals, report_date):
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

    # SDR data
    sdr_data = []
    for owner_name in sorted(by_owner.keys(), key=lambda x: -len(by_owner[x])):
        deals = by_owner[owner_name]
        stats = calc_stats(deals)
        lost_deals = [d for d in deals if "Sales Lost" in d["stage_changes"] or "Lost Before MQL" in d["stage_changes"]]

        sdr_deals = []
        for d in deals:
            sdr_deals.append({
                "name": d["name"],
                "current_stage": d["current_stage"],
                "stage_changes": list(d["stage_changes"].keys()),
            })

        sdr_lost = []
        for d in lost_deals:
            lost_type = "Sales Lost" if "Sales Lost" in d["stage_changes"] else "Lost Before MQL"
            sdr_lost.append({
                "name": d["name"],
                "lost_type": lost_type,
                "lost_reason": d["lost_reason"] or "Brak powodu",
                "lost_description": d["lost_description"],
            })

        sdr_data.append({
            "name": owner_name,
            "stats": stats,
            "deals": sdr_deals,
            "lost_deals": sdr_lost,
        })

    return {
        "date": report_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "summary": total_stats,
        "active_sdrs": len(by_owner),
        "sdr_data": sdr_data,
        "lost_reasons": [{"reason": r, "count": c} for r, c in sorted_reasons],
    }


def update_index(data_dir, report_date):
    index_path = os.path.join(data_dir, "index.json")
    if os.path.exists(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            index = json.load(f)
    else:
        index = {"dates": []}

    if report_date not in index["dates"]:
        index["dates"].append(report_date)
        index["dates"].sort()

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print(f"Index zaktualizowany: {len(index['dates'])} dat")


def main():
    report_date = get_report_date()
    print(f"Generowanie danych dla daty: {report_date}")

    owners = get_owners()
    all_deals = fetch_deals(report_date)
    print(f"Pobrano {len(all_deals)} deali")

    today_deals = process_deals(all_deals, owners, report_date)
    print(f"Deale ze zmiana etapu: {len(today_deals)}")

    data = build_json(today_deals, report_date)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # Save daily JSON
    json_path = os.path.join(data_dir, f"{report_date}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"JSON zapisany: {json_path}")

    # Update index
    update_index(data_dir, report_date)


if __name__ == "__main__":
    main()
