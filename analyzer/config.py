# /config.py

# 1. ONTOLOGY MAP: Maps messy real-world column names to a clean, standard internal format.
# This is the "brain" that allows the system to understand different file formats.
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

# 2. FILE CONFIGURATION: Defines the location and type of each data file.
FILE_CONFIG = {
    'STOCK_ONHAND': {
        'path': 'data/HVDC Stock OnHand Report.xlsx',
        'sheet_name': 'Case List',
        'type': 'onhand'  # This file is the source of truth for current stock
    },
    'HITACHI': {
        'path': 'data/HVDC WAREHOUSE_HITACHI(HE).xlsx',
        'sheet_name': 'Case List', # Let the loader auto-detect if this fails
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

# 3. WAREHOUSE & SITE DEFINITIONS: Defines which columns represent locations.
WAREHOUSE_COLS_MAP = {
    'HITACHI': ['DSV Outdoor', 'DSV Indoor', 'DSV Al Markaz', 'Hauler Indoor', 'DSV MZP', 'MOSB', 'Shifting'],
    'HITACHI_LOCAL': ['DSV Outdoor', 'DSV Al Markaz', 'DSV MZP', 'MOSB'],
    'HITACHI_LOT': ['DSV Indoor', 'DHL WH', 'DSV Al Markaz', 'AAA Storage'],
    'SIEMENS': ['DSV Outdoor', 'DSV Indoor', 'DSV Al Markaz', 'MOSB', 'AAA Storage', 'Shifting'],
}
SITE_COLS = ['DAS', 'MIR', 'SHU', 'AGI']

# 4. WAREHOUSE CLASSIFICATION
INDOOR_WAREHOUSES = {'DSV Indoor', 'Hauler Indoor', 'DSV Al Markaz', 'AAA Storage', 'DHL WH'}
DANGEROUS_WAREHOUSES = {'AAA Storage'}

# 5. ANALYSIS PARAMETERS
DEADSTOCK_DAYS = 90
