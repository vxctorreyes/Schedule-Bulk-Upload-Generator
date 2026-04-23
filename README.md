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
    
