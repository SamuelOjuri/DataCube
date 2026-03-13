"""
Configuration module for DataCube project.
Contains all board IDs, column mappings, and API settings.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

# Load environment variables
load_dotenv()

# Monday.com API Configuration
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"

# OpenAI API Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")  # Default to GPT-4o

# Gemini API Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") 
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# Board IDs
PARENT_BOARD_ID = "1825117125"   # Tapered Enquiry Maintenance
SUBITEM_BOARD_ID = "1825117144"  # Subitems board
HIDDEN_ITEMS_BOARD_ID = "1825138260"  # Tapered Hidden Items (source of truth)

# Batch Processing Configuration
BATCH_SIZE = 100  # Default for simple boards
PARENT_BOARD_BATCH_SIZE = 100  # Reduced for complex main board
SUBITEM_BOARD_BATCH_SIZE = 100  # Moderate for subitems
HIDDEN_ITEMS_BATCH_SIZE = 100  # Keep high for simple hidden items board

RATE_LIMIT_DELAY = 0.5  # Seconds between batches
CACHE_EXPIRY_HOURS = 24  # Cache duration for label mappings
MAX_RETRIES = 5  # Maximum number of retry attempts for API calls

# Data Processing Settings
MIN_SAMPLE_SIZE = 15  # Minimum sample size for statistical analysis

# Numeric baseline weighting configuration
TIME_WEIGHTING_ENABLED = True  # Prioritise recent projects in baseline stats
TIME_WEIGHTING_HALF_LIFE_DAYS = 730  # Two-year half-life for exponential decay (730)

# =============================================================================
# Product Type Segmentation Taxonomy
# =============================================================================

# CANONICAL_PRODUCT_KEYS: the only allowed values for projects.product_key.
# Keep this set small (10–15 items) to maximise segment sample sizes.
# =============================================================================

CANONICAL_PRODUCT_KEYS = {
    # --- Major PIR Families (by Facing) ---
    "pir_tissue",           # Adhered systems
    "pir_foil",             # Mechanically fixed systems
    "pir_torchon",          # Torch-applied systems

    # --- High-Value Composites ---
    "pir_prebonded",        # PIR + Plywood/OSB (high unit price & margin)

    # --- EPS Variants (by Application) ---
    "eps_standard",         # Standard warm roof
    "eps_inverted",         # Inverted / ballasted roof (high compressive strength)
    "eps_pir_composite",    # EPS bonded to PIR

    # --- Other Material Families ---
    "xps",                  # Extruded Polystyrene
    "vips",                 # Vacuum Insulated Panels
    "hardrock",             # Stone Wool Insulation (base layer)
    "rockdeck",             # Stone Wool Overlay Board (top layer)

    # --- Proprietary Systems ---
    "t3_system",            # Modular / specialist system

    # --- Fallback ---
    "unknown",              # Unmapped non-empty strings
}

# =============================================================================
# PRODUCT_TYPE_ALIASES: maps raw (messy) product strings → canonical keys.
# Keys are matched after runtime normalisation (lowercase, slash/hyphen collapse).
# =============================================================================

PRODUCT_TYPE_ALIASES = {
    # --- PIR: Tissue Faced (Adhered) ---
    "tf pir": "pir_tissue",
    "tissue faced pir": "pir_tissue",

    # --- PIR: Foil Faced (Mechanically Fixed) ---
    "foil faced pir": "pir_foil",
    "enertherm": "pir_foil",

    # --- PIR: Torch-on (Bituminous) ---
    "torchon pir": "pir_torchon",
    "torch on pir": "pir_torchon",

    # --- PIR: Prebonded (High Value / Composite Deck) ---
    "tissue faced pir (prebonded)": "pir_prebonded",
    "torchon pir (prebonded)": "pir_prebonded",
    "torch on pir (prebonded)": "pir_prebonded",
    "Foil Faced PIR (Prebonded)": "pir_prebonded",

    # --- EPS: Standard Warm Roof ---
    "eps": "eps_standard",
    "eps 150 (spr)": "eps_standard",
    "eps 150 (spi)": "eps_standard",
    "eps 100 (spi)": "eps_standard",

    # --- EPS: Inverted / Heavy Duty ---
    "inverted eps": "eps_inverted",
    "eps 200e": "eps_inverted",
    "eps 300e": "eps_inverted",

    # --- EPS/PIR Composites ---
    "eps/pir tissue faced": "eps_pir_composite",
    "eps/pir tissue faced (prebonded)": "eps_pir_composite",
    "eps/pir foil faced": "eps_pir_composite",
    "eps/pir enertherm": "eps_pir_composite",
    "eps/pir torch on": "eps_pir_composite",
    "eps/pir torch on (prebonded)": "eps_pir_composite",

    # --- XPS & VIPs ---
    "xps": "xps",
    "vip": "vips",
    "vips": "vips",
    "inverted vips": "vips",

    # --- Stone Wool (Rockwool) ---
    "hardrock": "hardrock",
    "rockwool hardrock multi-fix (dd)": "hardrock",
    "hardrock multi-fix (dd)": "hardrock",
    "rockwool hardrock multifix (dd)": "hardrock",
    "hardrock multifix (dd)": "hardrock",
    "rockwool hardrock multi fix (dd)": "hardrock",
    "hardrock multi fix (dd)": "hardrock",

    "rockdeck": "rockdeck",
    "rockdeck coverboard": "rockdeck",
    "rockdeck coverboard (g-board12.5mm)": "rockdeck",
    "rockdeck coverboard (g-board 12.5mm)": "rockdeck",

    # --- Proprietary Systems (T3) ---
    "t3+": "t3_system",
    "ready t3+": "t3_system",
    "roofblock g1t3+": "t3_system",
}

# Project directories
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
CACHE_DIR = DATA_DIR / "cache"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
ANALYSIS_DIR = OUTPUTS_DIR / "analysis"

# Webhook Configuration
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "10000"))
WEBHOOK_RATE_LIMIT_MAX_REQUESTS = int(os.getenv("WEBHOOK_RATE_LIMIT_MAX_REQUESTS", "180"))
WEBHOOK_RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("WEBHOOK_RATE_LIMIT_WINDOW_SECONDS", "60"))
WEBHOOK_DUPLICATE_TTL_SECONDS = int(os.getenv("WEBHOOK_DUPLICATE_TTL_SECONDS", "300"))

# Create directories if they don't exist
for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, CACHE_DIR, ANALYSIS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Complete PARENT_COLUMNS mapping with all required fields
PARENT_COLUMNS = {
    'name': 'name',                          # Item name
    'project_name': 'text3__1',              # Project Name
    'pipeline_stage': 'status4__1',          # Pipeline Stage
    'type': 'dropdown7__1',                  # Type (New Build/Refurbishment)
    'category': 'dropdown__1',               # Category
    'gestation_period': 'formula_mkpp85yw',  # Gestation Period (DAYS)
    'account_mirror': 'lookup_mkq010zk',     # Account Mirror → subitems mirror_12__1
    'new_enq_value_mirror': 'lookup_mkqanpbe', # New Enq Value → subitems formula_mkqa31kh
    'product_mirror': 'lookup_mkt3xgz1',     # Product Mirror → subitems mirror875__1
    'won_vs_open_lost': 'formula_mkq8gyr',   # Helper: "Won" vs "Open or Lost"
    'date_created': 'date9__1',              # Date Created (for time filtering)
    'follow_up_date': 'date_mkpx4163',       # Follow Up Date
    'zip_code': 'dropdown_mknfpjbt',         # Zip Code
    'sales_representative': 'person',        # Sales Representative  
    'funding': 'dropdown_mkrb9mq5',          # Funding
    'feedback': 'dropdown_mkp0tjks',         # Feedback
    'lost_to_who_or_why': 'dropdown_mkp1rgp9', # Lost To Who or Why?
    'expected_start_date': 'date_mkp0heke',  # Expected Start Date
    'overall_project_value': 'mirror4__1',   # Overall Project Value
    'total_order_value': 'mirror5__1',       # Total Order Value
    'probability_percent': 'formula_mknkwpb5', # % Probability
    'project_value': 'formula_mkpfesjh',     # Project Value
    'weighted_pipeline': 'formula_mkpfqf6a', # Weighted Pipeline
    'first_date_designed': 'lookup_mkpptgsn', # First Date Designed
    'last_date_designed': 'lookup_mkqsqcck', # Last Date Designed
    'first_date_invoiced': 'lookup_mkppq0gc', # First Date Invoiced
}

# Monday analysts columns
PARENT_COLUMN_GESTATION_DAYS = "numeric_mkw8t5cw"
PARENT_COLUMN_EXPECTED_CONVERSION = "numeric_mkw86evn"
PARENT_COLUMN_PROJECT_RATING = "numeric_mkw8nx25"

# Complete SUBITEM_COLUMNS mapping
SUBITEM_COLUMNS = {
    'name': 'name',                          # Item name
    'account': 'mirror_12__1',               # Account (source for parent mirror)
    'product_type': 'mirror875__1',          # Product Type (source for parent mirror)
    'new_enquiry_value': 'formula_mkqa31kh', # New Enquiry Value (source for parent mirror)
    'date_design_completed': 'formula_mkpp1x74',  # DDC
    'first_date_invoiced': 'formula_mkppwkkq',    # FDI
    'reason_for_change': 'mirror03__1',             # Reason For Change
    'quote_amount': 'mirror77__1',                  # Quote Amount
    'area': 'mirror79__1',                          # Area
    'fall': 'mirror22__1',                          # Fall
    'deck_type': 'mirror75__1',                     # Deck Type
    'designer': 'mirror39__1',                      # Designer
    'surveyor': 'mirror92__1',                      # Surveyor
    'layering': 'lookup_mknfw2nr',                  # Layering
    'date_received': 'mirror95__1',                 # Date Received
    'date_design_completed_mirror': 'mirror98__1',  # Date Design Completed
    'invoice_date': 'mirror082__1',                 # Invoice Date
    'time_taken': 'mirror16__1',                # Time Taken
    'min_thickness': 'mirror62__1',             # Min Thickness
    'max_thickness': 'mirror74__1',             # Max Thickness
    'u_value': 'mirror0__1',                    # U-Value
    'm2_rate': 'mirror57__1',                   # M2 Rate
    'material_value': 'mirror73__1',            # Material Value
    'transport_cost': 'mirror43__1',            # Transport Cost
    'order_status': 'mirror713__1',             # Order Status
    'date_order_received': 'mirror226__1',      # Date Order Received
    'customer_po': 'mirror81__1',               # Customer PO (text)
    'supplier1': 'mirror878__1',                # Supplier1
    'supplier2': 'mirror44__1',                 # Supplier2
    'requested_delivery_date': 'mirror24__1',   # Requested Delivery Date
    'final_delivery_date': 'mirror20__1',       # Final Delivery Date
    'delivery_address': 'mirror64__1',          # Delivery Address (text)
    'invoice_number': 'mirror682__1',           # Invoice Number (text)
    'amount_invoiced': 'mirror5__1',            # Amount Invoiced
    'cust_order_value_material': 'mirror17__1', # Cust Order Value Material

    # Link to hidden items board (Note:board_relation is mostly broken 'name' matching is more reliable)
    'hidden_item_id': 'connect_boards8__1',
}
# Complete HIDDEN_ITEMS_COLUMNS mapping
HIDDEN_ITEMS_COLUMNS = {
    'name': 'name',
    'quote_amount': 'formula63__1',
    'reason_for_change': 'dropdown2__1',
    'date_design_completed': 'date__1',
    'invoice_date': 'date42__1',
    'date_received': 'date4__1',
    'status': 'status__1',
    'project_attachments': 'files__1',
    'prefix': 'text__1',
    'revision': 'text6__1',
    'project_name': 'dup__of_revision__1',
    'designer': 'people__1',
    'surveyor': 'people3__1',
    'account': 'connect_boards__1',
    'contact': 'connect_boards2__1',
    'product_type': 'dropdown9__1',
    'u_value': 'numbers__1',
    'min_u_value': 'numbers67__1',
    'area': 'numbers5__1',
    'approx_bonding': 'text_mknfmd9',
    'fall': 'text2__1',
    'min_thickness': 'numbers72__1',
    'max_thickness': 'numbers2__1',
    'gutter_sump': 'text62__1',
    'deck_type': 'deck_type__1',
    'volume_m3': 'numbers21__1',
    'wastage_percent': 'numbers70__1',
    'material_value': 'numbers22__1',
    'transport_cost': 'numbers9__1',
    'target_gpm': 'formula__1',
    'tp_margin': 'numbers4__1',
    'commission': 'numbers55__1',
    'distributor_margin': 'numbers79__1',
    'time_taken': 'numbers6__1',
    'account_id': 'dup__of_accounts_mkkd7bew',
    'contact_id': 'order_contact_email_mkktmg6g',
    'date_order_received': 'date7__1',
    'cust_order_value_material': 'numbers98__1',
}

# Mirror resolution mappings
# MIRROR_MAPPINGS = {
#     'lookup_mkq010zk': 'mirror_12__1',       # Account
#     'lookup_mkt3xgz1': 'mirror875__1',       # Product Type
#     'lookup_mkqanpbe': 'formula_mkqa31kh'    # New Enquiry Value
# }

# Pipeline stage labels mapping
PIPELINE_STAGE_LABELS = {
    "0": "Won - Closed (Invoiced)",
    "1": "Lost",
    "2": "Customer Confident Of Project Success",
    "3": "Customer Has Order",
    "4": "Customer Has Order - TP Preferred Supplier",
    "5": "Open Enquiry",
    "7": "Won Via Other Ref",
    "8": "Check with IS",
    "10": "Won - Open (Order Received)"
}

# Type labels mapping
TYPE_LABELS = {
    "1": "Refurbishment",
    "2": "New Build"
}

# Category labels mapping
CATEGORY_LABELS = {
    "1": "Apartments",
    "2": "Commercial",
    "3": "Education",
    "4": "House",
    "5": "Health",
    "6": "Commodity",
    "7": "Industrial",
    "8": "Leisure",
    "9": "Military",
    "10": "Mixed Use",
    "11": "Student Accommodation",
    "12": "Consultancy"
}



# Validation configuration
REQUIRED_ENV_VARS = ["MONDAY_API_KEY", "OPENAI_API_KEY"]
EXPECTED_PARENT_COLUMNS = list(PARENT_COLUMNS.values())
EXPECTED_SUBITEM_COLUMNS = list(SUBITEM_COLUMNS.values())


# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Webhook Configuration
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8000"))

# Sync Configuration
SYNC_BATCH_SIZE = 1000  # Records per batch for Supabase
SYNC_INTERVAL_MINUTES = 15  # Delta sync interval
RETENTION_DAYS = 730  # Keep 2 years of data

# Analysis Configuration
ANALYSIS_LOOKBACK_DAYS = 1825  # 5 years of historical data for analysis (5 * 365)
DEFAULT_ANALYSIS_LOOKBACK_DAYS = 1825  # Default fallback value

# Common lookback period presets
LOOKBACK_PRESETS = {
    '6_months': 180,
    '1_year': 365,
    '2_years': 730,
    '3_years': 1095,
    '5_years': 1825,
    '10_years': 3650,
    'all_time': None  # No filter
}

def get_lookback_days(preset: str = '5_years') -> Optional[int]:
    """
    Get lookback days for a given preset.
    
    Args:
        preset: One of '6_months', '1_year', '2_years', '3_years', '5_years', '10_years', 'all_time'
    
    Returns:
        Number of days to look back, or None for all_time
    """
    return LOOKBACK_PRESETS.get(preset, ANALYSIS_LOOKBACK_DAYS)


# ---------------------------------------------------------------------------
# Gestation bias correction tuning
# Support-aware, tier-aware shrinkage instead of flat additive uplift.
# ---------------------------------------------------------------------------

GESTATION_BIAS_CORRECTION_ENABLED = True

# Raw global weighted bias from historical diagnostics: (actual - expected).
# Keep this as the uncapped historical signal.
# GESTATION_BIAS_GLOBAL_DAYS is the primary shrinkage anchor for supported segment and value-band corrections
GESTATION_BIAS_GLOBAL_DAYS = 85

# Conservative fallback used only for global-only or truly zero-support cases.
# Supported low-support segments shrink toward GESTATION_BIAS_GLOBAL_DAYS; this fallback is reserved for global-only or zero-support cases.
GESTATION_BIAS_GLOBAL_FALLBACK_DAYS = 72

# Final adjustment formula still uses damping and a hard cap, but the raw bias
# is now blended/shrunk first in numeric_analyzer.py.
#
# Recommended starting values:
# - Slightly reduce damping from 0.50 -> 0.45
# - Slightly reduce cap from 60 -> 55
# These are a bit more conservative and should help median error while still
# correcting the strong underprediction bias.
GESTATION_BIAS_DAMPING_FACTOR = 0.62
GESTATION_BIAS_MAX_ABS_DAYS = 55

# Legacy threshold kept for compatibility / fallback defaults.
# The new logic uses continuous shrinkage, so this is no longer the main gate.
GESTATION_BIAS_MIN_SEGMENT_N = 20

# Continuous support shrinkage:
# segment_trust = support / (support + GESTATION_BIAS_SUPPORT_SHRINKAGE)
#
# Higher values:
# - trust segment bias less
# - pull more strongly toward the primary global bias anchor for supported rows
# - reduce overcorrection on sparse/noisy segments
#
# Recommended starting value: 30.0
GESTATION_BIAS_SUPPORT_SHRINKAGE = 14.0

# Tier weights applied after segment/global blending and before damping.
# These reduce broad type/category bias when the baseline came from a narrower
# hierarchical tier.
#
# Tier mapping in AnalysisService / NumericBaseline:
#   0: account + type + category + product_type
#   1: account + type + category
#   2: account + type
#   3: type + category
#   4: type
#   5: global
GESTATION_BIAS_TIER_WEIGHTS = {
    0: 0.50,
    1: 0.75,
    2: 0.90,
    3: 1.00,
    4: 0.90,
    5: 0.80,
}

# Segment mean bias (days): actual_days - expected_days
# These remain the historical raw segment signals before shrinkage.
GESTATION_BIAS_BY_SEGMENT = {
    ('New Build', 'Apartments'): 108,
    ('New Build', 'Commercial'): 76,
    ('New Build', 'Datacentre'): 65,
    ('New Build', 'Education'): 51,
    ('New Build', 'Health'): 69,
    ('New Build', 'House'): 43,
    ('New Build', 'Industrial'): 73,
    ('New Build', 'Leisure'): 115,
    ('New Build', 'Mixed Use'): 27,
    ('Refurbishment', 'Apartments'): 58,
    ('Refurbishment', 'Commercial'): 115,
    ('Refurbishment', 'Education'): 62,
    ('Refurbishment', 'Health'): 62,
    ('Refurbishment', 'House'): 25,
    ('Refurbishment', 'Leisure'): 80,
}

# Historical support counts for the segment-bias table above.
# The new code prefers actual effective support from the live segment when
# available, and falls back to these values when it is not.
GESTATION_BIAS_SEGMENT_SAMPLE_SIZES = {
    ('New Build', 'Apartments'): 190,
    ('New Build', 'Commercial'): 43,
    ('New Build', 'Datacentre'): 8,
    ('New Build', 'Education'): 41,
    ('New Build', 'Health'): 39,
    ('New Build', 'House'): 69,
    ('New Build', 'Industrial'): 10,
    ('New Build', 'Leisure'): 23,
    ('New Build', 'Mixed Use'): 8,
    ('Refurbishment', 'Apartments'): 79,
    ('Refurbishment', 'Commercial'): 52,
    ('Refurbishment', 'Education'): 110,
    ('Refurbishment', 'Health'): 28,
    ('Refurbishment', 'House'): 77,
    ('Refurbishment', 'Leisure'): 23,
}


# ---------------------------------------------------------------------------
# Per-segment outlier filtering (Tukey fences)
# ---------------------------------------------------------------------------
GESTATION_OUTLIER_FILTERING_ENABLED = True

# Tukey fence multiplier: values beyond p75 + k*IQR (or below p25 - k*IQR)
# are excluded before computing the baseline median.
# 1.5 is the standard Tukey fence; use 2.0 for a more permissive filter.
GESTATION_OUTLIER_IQR_MULTIPLIER = 3.0

# Minimum raw samples required before IQR-based filtering is applied.
# Below this threshold, the global (0, 1460) cap remains the only filter
# to avoid discarding too much data from tiny segments.
GESTATION_OUTLIER_MIN_SAMPLES = 12

# ---------------------------------------------------------------------------
# Variance-aware bias damping
# ---------------------------------------------------------------------------
GESTATION_VARIANCE_AWARE_DAMPING_ENABLED = True

# Floor for the variance damping multiplier.  Even the noisiest segments
# still receive at least this fraction of the configured damping factor.
GESTATION_VARIANCE_DAMPING_FLOOR = 0.45


# ---------------------------------------------------------------------------
# Value-band segment bias correction (additive refinement on top of type×category)
# ---------------------------------------------------------------------------
GESTATION_BIAS_VALUE_BAND_ENABLED = True

# Minimum closed-project events in a (type, category, value_band) cell before
# its segment calibrator is trusted.  Below this, falls back to (type, category).
GESTATION_BIAS_VALUE_BAND_MIN_N = 15

# Shrinkage constant for value-band segments (higher = more conservative).
# Works identically to GESTATION_BIAS_SUPPORT_SHRINKAGE but for the finer grain.
GESTATION_BIAS_VALUE_BAND_SHRINKAGE = 20.0

# Static value-band segment bias table: (type, category, value_band) -> days
# Populated from historical diagnostics, same convention as GESTATION_BIAS_BY_SEGMENT.
GESTATION_BIAS_BY_VALUE_BAND_SEGMENT = {
    ('New Build', 'Apartments', 'Large (40-100k)'): 163,
    ('New Build', 'Apartments', 'Medium (15-40k)'): 119,
    ('New Build', 'Apartments', 'Small (<15k)'): 54,
    ('New Build', 'Apartments', 'Zero'): 163,
    ('New Build', 'Commercial', 'Small (<15k)'): -1,
    ('New Build', 'Education', 'Small (<15k)'): -15,
    ('New Build', 'Health', 'Small (<15k)'): 0,
    ('New Build', 'House', 'Small (<15k)'): 42,
    ('Refurbishment', 'Apartments', 'Small (<15k)'): 13,
    ('Refurbishment', 'Commercial', 'Small (<15k)'): 122,
    ('Refurbishment', 'Education', 'Large (40-100k)'): 127,
    ('Refurbishment', 'Education', 'Medium (15-40k)'): 29,
    ('Refurbishment', 'Education', 'Small (<15k)'): 13,
    ('Refurbishment', 'Health', 'Small (<15k)'): 54,
    ('Refurbishment', 'House', 'Small (<15k)'): 25,
}

# Sample sizes for the above table (optional, used as fallback when live support unavailable).
GESTATION_BIAS_VALUE_BAND_SEGMENT_SAMPLE_SIZES = {
    ('New Build', 'Apartments', 'Large (40-100k)'): 17,
    ('New Build', 'Apartments', 'Medium (15-40k)'): 32,
    ('New Build', 'Apartments', 'Small (<15k)'): 102,
    ('New Build', 'Apartments', 'Zero'): 31,
    ('New Build', 'Commercial', 'Small (<15k)'): 23,
    ('New Build', 'Education', 'Small (<15k)'): 17,
    ('New Build', 'Health', 'Small (<15k)'): 18,
    ('New Build', 'House', 'Small (<15k)'): 56,
    ('Refurbishment', 'Apartments', 'Small (<15k)'): 54,
    ('Refurbishment', 'Commercial', 'Small (<15k)'): 38,
    ('Refurbishment', 'Education', 'Large (40-100k)'): 24,
    ('Refurbishment', 'Education', 'Medium (15-40k)'): 28,
    ('Refurbishment', 'Education', 'Small (<15k)'): 41,
    ('Refurbishment', 'Health', 'Small (<15k)'): 16,
    ('Refurbishment', 'House', 'Small (<15k)'): 68,
}