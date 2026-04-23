"""
================================================================================
README - Schedule Bulk Upload Generator
================================================================================

OVERVIEW:
    This script automates the generation of a bulk upload CSV for scheduling
    freight carrier pickups. It was built to eliminate manual data entry by
    reading a structured input file (high-cube/priority shipments) and
    cross-referencing several lookup tables to produce a fully populated,
    template-conformant CSV ready for upload into a transport management system.

PROBLEM IT SOLVES:
    Scheduling teams previously had to manually create pick up(s) for a lengthy list of 
    different shippers, calculate arrival times, cut-off times (CPT), and departure times 
    for each shipment row, then copy data into a rigid upload template. This script automates 
    that entire process, handling timezone-aware time calculations, vehicle lead times, 
    and sort center routing lookups — reducing a multi-hour manual task to a single script execution.

HOW IT WORKS:
    1. Loads a priority shipment input CSV (analogous to a "high priority" queue)
    2. Loads three lookup tables:
         - Sort Center (SC) routing: maps origin facility IDs to destination sort centers
         - Transit times: minutes of transit per origin, used to compute departure time
         - Timezone lookup: maps vendor/shipper IDs to their local IANA timezone
    3. For each shipment row:
         - Resolves the correct destination sort center (directly or via lookup)
         - Computes a timezone-aware proposed truck arrival time using configurable
           lead times per vehicle type
         - Validates the arrival falls within the facility's pickup window (PUW)
         - Calculates the Cut-off Pull Time (CPT) and departure time
         - Maps all values into the exact column structure of the upload template
    4. Writes the completed rows to a timestamped output CSV

KEY FEATURES:
    - Timezone-aware scheduling using pytz (per-facility or per-vendor timezone)
    - Configurable lead times per vehicle type (e.g. 53ft truck vs 26ft box truck)
    - Per-shipper pickup window support with global fallback defaults
    - Automatic sort center resolution with fallback and skip logic
    - Timestamped output file to prevent overwrites
    - Clear console output with per-row success/skip/error reporting

INPUT FILES REQUIRED:
    - priority_shipments.csv  : shipment rows with facility IDs, vehicle type, carrier
    - sc_routing.csv          : origin -> destination sort center + transit time (minutes)
    - upload_template.csv     : empty CSV whose columns define the output schema
    - timezone_lookup.csv     : vendor/shipper ID -> IANA timezone string

OUTPUT:
    - schedule_bulk_upload_<timestamp>.csv : completed bulk upload file

DEPENDENCIES:
    - Python 3.7+
    - pandas
    - pytz

AUTHOR NOTE:
    Company-specific identifiers (carrier codes, business type labels, internal
    node naming conventions) have been replaced with generic placeholders for
    portfolio purposes. The logic, architecture, and automation approach are
    entirely representative of the original implementation.
================================================================================
"""

import pandas as pd
from datetime import datetime, timedelta
import pytz

# --- Config ---
HIGH_CUBE_FILE   = 'priority_shipments.csv'
SC_LOOKUP_CSV    = 'sc_routing.csv'
TEMPLATE_CSV     = 'upload_template.csv'
TIMEZONE_PUW_CSV = 'timezone_lookup.csv'

VEHICLE_MAP = {
    '53': 'FIFTY_THREE_FOOT_TRUCK',
    '26': 'TWENTY_SIX_FOOT_BOX_TRUCK',
}

# Lead time in minutes from current local time to proposed arrival
LEAD_TIME = {
    'FIFTY_THREE_FOOT_TRUCK':    240,
    'TWENTY_SIX_FOOT_BOX_TRUCK': 90,
}

# Default PUW (used only as fallback if priority_shipments has no PickupWindow)
PUW_START_H, PUW_START_M = 14, 30
PUW_END_H,   PUW_END_M   = 17,  0

DEFAULT_CARRIER      = 'CARRIER_CODE'
DEFAULT_BUSINESS_TYPE = 'BUSINESS_TYPE'


def load_timezone_lookup(filepath):
    """Build a dict: vendor -> shipper_timezone from timezone_lookup.csv."""
    try:
        df = pd.read_csv(filepath)
        vendor_col = next((c for c in df.columns if c.lower() == 'vendor'), None)
        tz_col     = next((c for c in df.columns if c.lower() == 'shipper_timezone'), None)
        if not vendor_col or not tz_col:
            print(f"  ⚠️  timezone_lookup.csv missing vendor/shipper_timezone columns — timezone lookup disabled")
            return {}
        lookup = dict(zip(df[vendor_col].str.strip(), df[tz_col].str.strip()))
        print(f"  ✅ Loaded {len(lookup)} timezone mappings")
        return lookup
    except Exception as e:
        print(f"  ⚠️  Could not load timezone lookup: {e}")
        return {}


def load_sc_lookup(filepath):
    """Build dicts from sc_routing.csv: origin -> destination, and origin -> transit_time (minutes)."""
    try:
        df = pd.read_csv(filepath)
        origin_col  = next((c for c in df.columns if c.lower() == 'origin'), None)
        dest_col    = next((c for c in df.columns if c.lower() == 'destination'), None)
        transit_col = next((c for c in df.columns if 'transit' in c.lower()), None)
        if not origin_col or not dest_col:
            print(f"  ⚠️  sc_routing.csv missing origin/destination columns — SC auto-lookup disabled")
            return {}, {}
        sc_lookup = dict(zip(df[origin_col].str.strip(), df[dest_col].str.strip()))
        transit_lookup = {}
        if transit_col:
            transit_lookup = dict(zip(df[origin_col].str.strip(), pd.to_numeric(df[transit_col], errors='coerce')))
            print(f"  ✅ Loaded {len(sc_lookup)} SC mappings + transit times from {filepath}")
        else:
            print(f"  ✅ Loaded {len(sc_lookup)} SC mappings from {filepath} (no transit time column found)")
        return sc_lookup, transit_lookup
    except Exception as e:
        print(f"  ⚠️  Could not load SC lookup: {e}")
        return {}, {}


def resolve_sc(row, sc_lookup):
    """Return the correct sort center for this row.
    - If sc is filled, use it directly.
    - If sc is empty but destination_node is filled, look up the SC from sc_routing.csv using shipper_id.
    """
    sc_val  = str(row.get('sc', '')).strip()
    dst_val = str(row.get('destination_node', '')).strip()

    if sc_val and sc_val.lower() != 'nan':
        return sc_val

    if dst_val and dst_val.lower() != 'nan':
        shipper  = str(row.get('shipper_id', '')).strip()
        resolved = sc_lookup.get(shipper)
        if resolved:
            print(f"    ↳ {shipper}: destination '{dst_val}' → SC '{resolved}'")
            return resolved
        else:
            print(f"    ⚠️  {shipper}: no SC mapping found for destination '{dst_val}' — row skipped")
            return None

    raise ValueError(f"Row has no sc or destination_node value for shipper '{row.get('shipper_id', '')}'")


def load_high_cube(filepath, date_val):
    """Load priority shipments CSV and normalize rows to match upload template field names."""
    df = pd.read_csv(filepath)
    rows = []
    for _, r in df.iterrows():
        pickup_window = str(r.get('PickupWindow', '14:30-17:00')).strip()
        warehouse_tz  = str(r.get('WarehouseTimeZone', '')).strip()
        rows.append({
            'shipper_id':      str(r['OriginFacilityId']).strip(),
            'destination_node': str(r['DestinationNodeId']).strip(),
            'sc':              '',
            'date':            date_val,
            'vehicle_type':    '26',
            'carrier':         DEFAULT_CARRIER,
            'link':            'DESTINATION',
            'pickup_window':   pickup_window,
            'warehouse_tz':    warehouse_tz,
        })
    return rows


def parse_cpt(cpt_val, date_val):
    """Parse CPT string (HH:MM or H:MM) combined with date into a datetime."""
    date_str = str(date_val).strip()
    cpt_str  = str(cpt_val).strip()
    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y'):
        try:
            date_obj = datetime.strptime(date_str, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Cannot parse date: {date_str!r}")

    for fmt in ('%H:%M', '%I:%M %p', '%I:%M%p'):
        try:
            time_obj = datetime.strptime(cpt_str, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Cannot parse CPT time: {cpt_str!r}")

    return date_obj.replace(hour=time_obj.hour, minute=time_obj.minute, second=0)


def compute_arrival_cpt(equipment, shipper_id, tz_lookup, date_val,
                        transit_lookup=None, pickup_window=None, warehouse_tz=None):
    """Compute arrival and CPT datetimes based on current local time, PUW, and transit time.

    Returns (arrival_dt, cpt_dt) or raises ValueError if outside PUW.
    """
    # Resolve timezone: prefer warehouse_tz from input, fall back to tz_lookup
    tz_name = warehouse_tz if warehouse_tz and warehouse_tz.lower() != 'nan' else tz_lookup.get(shipper_id)
    if tz_name:
        try:
            tz        = pytz.timezone(tz_name)
            now_utc   = datetime.now(pytz.utc)
            now_local = now_utc.astimezone(tz).replace(tzinfo=None)
        except Exception:
            now_local = datetime.now()
    else:
        now_local = datetime.now()

    # Parse the scheduling date
    date_str = str(date_val).strip()
    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y'):
        try:
            date_obj = datetime.strptime(date_str, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Cannot parse date: {date_str!r}")

    # Parse pickup window — use per-shipper value, fall back to defaults
    if pickup_window and '-' in pickup_window and pickup_window.lower() != 'nan':
        try:
            pw_start_str, pw_end_str = pickup_window.split('-', 1)
            pw_start_parts = pw_start_str.strip().split(':')
            pw_end_parts   = pw_end_str.strip().split(':')
            pw_start_h, pw_start_m = int(pw_start_parts[0]), int(pw_start_parts[1])
            pw_end_h,   pw_end_m   = int(pw_end_parts[0]),   int(pw_end_parts[1])
        except (ValueError, IndexError):
            pw_start_h, pw_start_m = PUW_START_H, PUW_START_M
            pw_end_h,   pw_end_m   = PUW_END_H,   PUW_END_M
    else:
        pw_start_h, pw_start_m = PUW_START_H, PUW_START_M
        pw_end_h,   pw_end_m   = PUW_END_H,   PUW_END_M

    puw_start = date_obj.replace(hour=pw_start_h, minute=pw_start_m, second=0)
    puw_end   = date_obj.replace(hour=pw_end_h,   minute=pw_end_m,   second=0)

    lead_mins    = LEAD_TIME[equipment]
    proposed_arr = date_obj.replace(hour=now_local.hour, minute=now_local.minute, second=0) \
                   + timedelta(minutes=lead_mins)

    if proposed_arr < puw_start:
        arrival_dt = puw_start
        cpt_dt     = puw_start + timedelta(minutes=30)
    elif proposed_arr > puw_end:
        raise ValueError(
            f"{shipper_id}: proposed arrival {proposed_arr.strftime('%H:%M')} is after PUW end "
            f"{pw_end_h:02d}:{pw_end_m:02d} — row skipped"
        )
    else:
        arrival_dt = proposed_arr
        cpt_dt     = arrival_dt + timedelta(minutes=30)

    return arrival_dt, cpt_dt


def process_row(row, sc_lookup, tz_lookup, template_cols, transit_lookup=None):
    """Convert one input row into a dict matching the bulk-upload template."""
    out = {col: '' for col in template_cols}

    vehicle_key = str(row.get('vehicle_type', '')).strip()
    equipment   = VEHICLE_MAP.get(vehicle_key)
    if not equipment:
        raise ValueError(f"Unknown vehicle_type: {vehicle_key!r}. Expected '53' or '26'.")

    sc = resolve_sc(row, sc_lookup)
    if sc is None:
        return None

    shipper_id    = str(row.get('shipper_id', '')).strip()
    link          = str(row.get('link', '')).strip()
    carrier       = str(row.get('carrier', DEFAULT_CARRIER)).strip()
    date_val      = row.get('date', '')
    pickup_window = row.get('pickup_window')
    warehouse_tz  = row.get('warehouse_tz')

    arrival_dt, cpt_dt = compute_arrival_cpt(
        equipment, shipper_id, tz_lookup, date_val,
        transit_lookup=transit_lookup,
        pickup_window=pickup_window,
        warehouse_tz=warehouse_tz,
    )

    date_fmt = '%m/%d/%Y'
    time_fmt = '%H:%M'
    lane     = f"{shipper_id}->{sc}"

    # Use actual transit time from sc_routing for departure, fall back to 150 min
    transit_mins = 150
    if transit_lookup:
        tt = transit_lookup.get(shipper_id)
        if tt and tt > 0:
            transit_mins = tt

    departure_dt = cpt_dt + timedelta(minutes=transit_mins)

    out['New Tour']                         = ''
    out['Tour ID']                          = ''
    out['Load #']                           = ''
    out['Business Types']                   = DEFAULT_BUSINESS_TYPE
    out['Canceled Load']                    = 'FALSE'
    out['Carrier']                          = carrier
    out['Subcarrier']                       = carrier
    out['Lane']                             = lane
    out['Base Rate']                        = '1'
    out['Currency Unit']                    = 'USD'
    out['Equipment Type']                   = equipment
    out['Carrier Reference ID']             = link
    out['Freight Type']                     = 'TRUCKLOAD'
    out['Corresponding CPT']                = cpt_dt.strftime(f"{date_fmt} {time_fmt}")
    out['Shipper Accounts']                 = DEFAULT_BUSINESS_TYPE
    out['Pull time 1']                      = cpt_dt.strftime(time_fmt)
    out['Scheduled Truck Arrival - 1 date'] = arrival_dt.strftime(date_fmt)
    out['Scheduled Truck Arrival - 1 time'] = arrival_dt.strftime(time_fmt)
    out['Scheduled Truck Arrival - 2 date'] = departure_dt.strftime(date_fmt)
    out['Scheduled Truck Arrival - 2 time'] = departure_dt.strftime(time_fmt)
    out['Date Format']                      = 'MM/dd/yyyy'
    out['Time Format']                      = 'HH:mm'
    out['Transit Operator Type']            = 'SINGLE_DRIVER'
    out['Trailer Ready Time']               = cpt_dt.strftime(f"{date_fmt} {time_fmt}")
    out['Rate Type']                        = 'PER_LOAD'
    out['Requires Single Container']        = 'N'
    out['Load Type - 1']                    = 'LIVE'
    out['Unload Type - 2']                  = 'LIVE'

    return out


def main():
    print("\n[SCHEDULE BULK UPLOAD GENERATOR]")

    # Load SC + transit time lookups
    sc_lookup, transit_lookup = load_sc_lookup(SC_LOOKUP_CSV)

    # Load timezone lookup
    tz_lookup = load_timezone_lookup(TIMEZONE_PUW_CSV)

    # Load template columns
    try:
        template_df   = pd.read_csv(TEMPLATE_CSV, nrows=0)
        template_cols = list(template_df.columns)
        print(f"  ✅ Template loaded — {len(template_cols)} columns")
    except Exception as e:
        print(f"  ❌ Cannot load template: {e}")
        return

    # Load input rows
    try:
        date_input = datetime.today().strftime('%m/%d/%Y')
        input_rows = load_high_cube(HIGH_CUBE_FILE, date_input)
        print(f"  ✅ Priority shipments loaded — {len(input_rows)} rows")
    except Exception as e:
        print(f"  ❌ Cannot load input file: {e}")
        return

    # Process rows
    results = []
    errors  = []
    for idx, row in enumerate(input_rows):
        try:
            out = process_row(row, sc_lookup, tz_lookup, template_cols, transit_lookup=transit_lookup)
            if out is not None:
                results.append(out)
        except Exception as e:
            errors.append((idx + 2, str(e)))
            print(f"  ⚠️  Row {idx + 2} skipped: {e}")

    if not results:
        print("  ❌ No rows processed successfully.")
        return

    # Write output CSV
    out_df    = pd.DataFrame(results, columns=template_cols)
    timestamp = datetime.now().strftime('%m-%d-%Y-%H%M')
    out_path  = f'schedule_bulk_upload_{timestamp}.csv'
    out_df.to_csv(out_path, index=False)

    print(f"\n  ✅ Output written: {out_path}")
    print(f"     Rows processed : {len(results)}")
    if errors:
        print(f"     Rows skipped   : {len(errors)}")
        for row_num, msg in errors:
            print(f"       Row {row_num}: {msg}")


if __name__ == '__main__':
    main()
