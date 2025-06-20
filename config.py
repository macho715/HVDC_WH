# /config.py
"""
Configuration Module

This module contains all configuration settings for the warehouse analysis system.
It serves as the single source of truth for all system parameters and mappings.
"""

# =============================================================================
# 1. ONTOLOGY MAP: Maps messy real-world column names to clean, standard format
# =============================================================================
# This is the "brain" that allows the system to understand different file formats
# by mapping various column names to standardized internal names.

ONTOLOGY_MAP = {
    # Standard Name : [List of possible aliases in source files]
    'case_no':      ['Case No.', 'MR#', 'SCT SHIP NO.', 'Carton ID', 'Package No'],
    'quantity':     ["Q'TY", 'QTY', 'Quantity', 'QTY SHIPPED', 'Received', 'on hand', 'onhand'],
    'location':     ['Location', 'LOC', 'ActualLocation', 'Warehouse'],
    'description':  ['DESCRIPTION', 'Desc.'],
    'vendor':       ['VENDOR', 'Supplier', 'SHIPMENT ID'],
    'unit':         ['Unit', 'UOM'],
    'length':       ['L(m)', 'l(m)', 'L(cm)', 'Length'],
    'width':        ['W(m)', 'w(m)', 'W(cm)', 'Width'],
    'height':       ['H(m)', 'h(m)', 'H(cm)', 'Height'],
    'gross_weight': ['G.W(kg)', 'gw(kg)', 'Gross Weight', 'GW'],
    'hs_code':      ['HS CODE', 'Tariff Code'],
    'incoterm':     ['INCOTERM'],
    'oog_flag':     ['OOG'],
    'package_type': ['PKG TYPE', 'PackageType'],
}

# =============================================================================
# 2. FILE CONFIGURATION: Defines the location and type of each data file
# =============================================================================
# Each entry defines a data file with its path, sheet name, and type.

FILE_CONFIG = {
    'STOCK_ONHAND': {
        'path': 'data/HVDC Stock OnHand Report.xlsx',
        'sheet_name': 'Case List',
        'type': 'onhand'  # This file is the source of truth for current stock
    },
    'HITACHI': {
        'path': 'data/HVDC WAREHOUSE_HITACHI(HE).xlsx',
        'sheet_name': 'Case List',  # Auto-detect if this fails
        'type': 'movement'
    },
    'HITACHI_LOCAL': {
        'path': 'data/HVDC WAREHOUSE_HITACHI(HE_LOCAL).xlsx',
        'sheet_name': 'CASE LIST',
        'type': 'movement'
    },
    'HITACHI_LOT': {
        'path': 'data/HVDC WAREHOUSE_HITACHI(HE-0214,0252).xlsx',
        'sheet_name': 'CASE LIST',
        'type': 'movement'
    },
    'SIEMENS': {
        'path': 'data/HVDC WAREHOUSE_SIMENSE(SIM).xlsx',
        'sheet_name': 'CASE LIST',
        'type': 'movement'
    },
}

# =============================================================================
# 3. WAREHOUSE & SITE DEFINITIONS: Defines location columns by supplier
# =============================================================================
# Maps each supplier to their specific warehouse and site location columns.

WAREHOUSE_COLS_MAP = {
    'HITACHI': [
        'DSV Outdoor', 'DSV Indoor', 'DSV Al Markaz', 
        'Hauler Indoor', 'DSV MZP', 'MOSB', 'Shifting'
    ],
    'HITACHI_LOCAL': [
        'DSV Outdoor', 'DSV Al Markaz', 'DSV MZP', 'MOSB'
    ],
    'HITACHI_LOT': [
        'DSV Indoor', 'DHL WH', 'DSV Al Markaz', 'AAA Storage'
    ],
    'SIEMENS': [
        'DSV Outdoor', 'DSV Indoor', 'DSV Al Markaz', 
        'MOSB', 'AAA Storage', 'Shifting'
    ],
}

# Site locations (common across all suppliers)
SITE_COLS = ['DAS', 'MIR', 'SHU', 'AGI']

# =============================================================================
# 4. WAREHOUSE CLASSIFICATION: Categorizes warehouses by type
# =============================================================================
# Used for special handling and analysis of different warehouse types.

INDOOR_WAREHOUSES = {
    'DSV Indoor', 'Hauler Indoor', 'DSV Al Markaz', 
    'AAA Storage', 'DHL WH'
}

DANGEROUS_WAREHOUSES = {
    'AAA Storage'
}

# =============================================================================
# 5. ANALYSIS PARAMETERS: Configurable thresholds and settings
# =============================================================================
# These parameters control the behavior of various analyses.

DEADSTOCK_DAYS = 90  # Days threshold for deadstock analysis

# =============================================================================
# 6. OUTPUT CONFIGURATION: Report generation settings
# =============================================================================
# Settings for Excel report generation and formatting.

REPORT_CONFIG = {
    'output_directory': 'outputs',
    'filename_prefix': 'Inventory_Report',
    'auto_open': True,  # Whether to automatically open the report after generation
    'sheet_names': {
        'full_stock_list': 'Full_Stock_List',
        'verification': 'Stock_Verification',
        'deadstock': 'DeadStock_Analysis'
    }
}

# =============================================================================
# 7. VALIDATION RULES: Data quality and business rules
# =============================================================================
# Rules for validating data quality and business logic.

VALIDATION_RULES = {
    'required_columns': ['case_no', 'quantity'],
    'quantity_min': 0,
    'quantity_max': 1000000,
    'dimension_min': 0,
    'dimension_max': 100,  # meters
    'weight_min': 0,
    'weight_max': 100000  # kg
}
