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
TIME_WEIGHTING_HALF_LIFE_DAYS = 730  # Two-year half-life for exponential decay

# Project directories
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
CACHE_DIR = DATA_DIR / "cache"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
ANALYSIS_DIR = OUTPUTS_DIR / "analysis"

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

    # Additional fields needed by app and DB
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

    # Newly added to populate missing DB columns
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

    # Link to hidden items board (Note:board_relation is mostly broken 'name' matching is more reliable)
    'hidden_item_id': 'connect_boards8__1',
}
# Complete HIDDEN_ITEMS_COLUMNS mapping
HIDDEN_ITEMS_COLUMNS = {
    'name': 'name',                          # Item name
    'quote_amount': 'formula63__1',          # Quote Amount
    'reason_for_change': 'dropdown2__1',     # Reason For Change
    'date_design_completed': 'date__1',      # Date Design Completed
    'invoice_date': 'date42__1',             # Invoice Date
    'date_received': 'date4__1',             # Date Received
    
    # Add these missing columns from BoardSchema
    'status': 'status__1',                   # Status
    'project_attachments': 'files__1',       # Project Attachments
    'prefix': 'text__1',                     # Prefix
    'revision': 'text6__1',                  # Revision
    'project_name': 'dup__of_revision__1',   # Project Name
    'designer': 'people__1',                 # Designer
    'surveyor': 'people3__1',                # Surveyor
    'account': 'connect_boards__1',          # Accounts
    'contact': 'connect_boards2__1',         # Contacts
    'product_type': 'dropdown9__1',          # Product Type
    'u_value': 'numbers__1',                 # U-Value
    'area': 'numbers5__1',                   # Area
    'fall': 'text2__1',                      # Fall
    'deck_type': 'deck_type__1',            # Deck Type
    'material_value': 'numbers22__1',        # Material value
    'transport_cost': 'numbers9__1',         # Transport Cost
    'target_gpm': 'formula__1',              # Target Gpm
    'tp_margin': 'numbers4__1',              # TP Margin
    'commission': 'numbers55__1',            # Commission
    'distributor_margin': 'numbers79__1',    # Distributor Margin
    'date_quoted': 'date8__1',               # Date Quoted
    'date_project_won': 'date64__1',         # Date Project Won
    'date_project_closed': 'date6__1',       # Date Project Closed
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
