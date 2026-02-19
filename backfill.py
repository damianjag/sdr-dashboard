"""
Backfill: pobiera WSZYSTKIE deale z pipeline SDR i generuje JSONy per dzien
z kumulatywnymi konwersjami (snapshot na dany dzien).
Użycie: python backfill.py
"""
import os
import sys
import json
import time
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from generate_data import (
    headers, SDR_PIPELINE_ID, PROPERTIES, STAGES, DATE_ENTERED_FIELDS,
    EXCLUDE_OWNERS, is_date_match, calc_stats, calc_conversions,
    build_json, update_index, get_owners
)
import requests


def api_request(method, url, **kwargs):
    """Wrapper z retry na 429 rate limit i timeout."""
    kwargs.setdefault("timeout", 60)
    for attempt in range(8):
        try:
            r = method(url, **kwargs)
        except requests.exceptions.ReadTimeout:
            print(f"    Timeout - czekam 10s (próba {attempt+1}/8)...")
            time.sleep(10)
            continue
        except requests.exceptions.ConnectionError:
            print(f"    Connection error - czekam 15s (próba {attempt+1}/8)...")
            time.sleep(15)
            continue
        if r.status_code == 429:
            wait = max(int(r.headers.get("Retry-After", 30)), 30)
            print(f"    Rate limit - czekam {wait}s (próba {attempt+1}/8)...")
            time.sleep(wait)
            continue
        return r
    return r


def fetch_all_pipeline_deals():
    """Pobiera WSZYSTKIE deale z pipeline SDR (bez filtra po dacie)."""
    all_deals = []
    after = None

    while True:
        payload = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": "pipeline", "operator": "EQ", "value": SDR_PIPELINE_ID},
                ]
            }],
            "properties": PROPERTIES,
            "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "DESCENDING"}],
            "limit": 100
        }
        if after:
            payload["after"] = after

        r = api_request(requests.post,
            "https://api.hubapi.com/crm/v3/objects/deals/search",
            headers=headers, json=payload, timeout=30
        )
        if r.status_code != 200:
            print(f"API error: {r.status_code}")
            break
        time.sleep(0.5)

        data = r.json()
        results = data.get("results", [])
        all_deals.extend(results)
        print(f"  Pobrano {len(all_deals)} deali...", end='\r')

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    print(f"  Pobrano {len(all_deals)} deali łącznie     ")
    return all_deals


def extract_deal_dates(deal):
    """Zwraca set dat (YYYY-MM-DD) w których deal zmienił etap."""
    props = deal["properties"]
    dates = set()
    for field in DATE_ENTERED_FIELDS:
        val = props.get(field)
        if val:
            try:
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                dates.add(dt.strftime("%Y-%m-%d"))
            except:
                pass
    return dates


def process_deals_for_date(all_deals, owners, report_date):
    """Filtruje deale które mialy zmiane etapu w danym dniu."""
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


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    start_date = datetime(2026, 2, 1)
    end_date = datetime(2026, 2, 19)

    print("=== SDR Dashboard Backfill ===\n")

    print("1. Pobieram ownerów...")
    owners = get_owners()
    print(f"   Znaleziono {len(owners)} ownerów\n")

    print("2. Pobieram WSZYSTKIE deale z pipeline SDR...")
    all_deals = fetch_all_pipeline_deals()
    print()

    # Znajdz wszystkie daty w których byly zmiany
    print("3. Analizuję daty zmian etapów...")
    all_active_dates = set()
    for deal in all_deals:
        all_active_dates.update(extract_deal_dates(deal))

    dates_in_range = sorted([d for d in all_active_dates
                             if start_date.strftime("%Y-%m-%d") <= d <= end_date.strftime("%Y-%m-%d")])
    print(f"   Znaleziono {len(dates_in_range)} dni z aktywnością\n")

    print("4. Generuję JSONy per dzień (z kumulatywnymi konwersjami)...")
    generated = 0
    for date_str in dates_in_range:
        today_deals = process_deals_for_date(all_deals, owners, date_str)

        # Konwersje kumulatywne na ten dzień (snapshot)
        conversions, sdr_conversions = calc_conversions(all_deals, owners, as_of_date=date_str)

        data = build_json(today_deals, date_str, conversions, sdr_conversions)

        json_path = os.path.join(data_dir, f"{date_str}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        update_index(data_dir, date_str)
        conv_str = conversions.get("lead_mql", "-")
        print(f"   {date_str}: {len(today_deals)} deali | Lead->MQL: {conv_str}")
        generated += 1

    print(f"\nGotowe! Wygenerowano {generated} plików JSON.")


if __name__ == "__main__":
    main()
