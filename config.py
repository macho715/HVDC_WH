# /config.py

# 1. ONTOLOGY MAPPING: Standardizes column names across different data sources
# =============================================================================
# This mapping follows the inbound_mapping_schema_v1.2 for SAP FI → Knowledge Graph ETL

ONTOLOGY_MAP = {
    # === CORE FIELDS (Required) ===
    'case_no': {
        'patterns': ['case no', 'case_no', 'mr#', 'sct ship no', 'carton id', 'case number', 'case_id', 'No.', 'No,', 'Case No.', 'Case No', 'No'],
        'required': True,
        'ontology_tag': 'kg:InventoryUnit',
        'shacl_rule': 'sct:UniqueKey',
        'transform': 'unique_key_check'
    },
    'quantity': {
        'patterns': ["q'ty", 'qty', 'quantity', 'received', 'received qty', 'received quantity'],
        'required': True,
        'ontology_tag': 'kg:ReceivedQuantity',
        'shacl_rule': 'sct:PositiveNumber',
        'transform': 'to_numeric_with_ea_correction'
    },
    'arrival_date': {
        'patterns': ['inbound date', 'arrival date', 'last move', 'arrival', 'inbound'],
        'required': True,
        'ontology_tag': 'kg:InboundDate',
        'shacl_rule': 'sct:Date',
        'transform': 'to_period_month'
    },
    
    # === STORAGE & LOCATION FIELDS ===
    'storage_type': {
        'patterns': ['storage type', 'storage condition', 'storage'],
        'required': False,
        'ontology_tag': 'kg:StorageCondition',
        'shacl_rule': 'sct:EnumStorage',
        'transform': 'map_storage_type'
    },
    'warehouse': {
        'patterns': ['warehouse', 'wh', 'dsv', 'al markaz', 'zener', 'location'],
        'required': False,
        'ontology_tag': 'kg:WarehouseName',
        'shacl_rule': 'sct:WHRef',
        'transform': 'join_wh_master'
    },
    
    # === VENDOR & SUPPLIER FIELDS ===
    'vendor': {
        'patterns': ['vendor', 'supplier', 'supplier name', 'vendor name'],
        'required': False,
        'ontology_tag': 'kg:Supplier',
        'shacl_rule': 'sct:VendorRef',
        'transform': 'classify_vendor'
    },
    
    # === DIMENSIONS & WEIGHT FIELDS ===
    'sqm': {
        'patterns': ['sqm', 'area', 'square meter', 'square meters'],
        'required': False,
        'ontology_tag': 'kg:Area',
        'shacl_rule': 'sct:PositiveNumber',
        'transform': 'calculate_from_dimensions'
    },
    'cbm': {
        'patterns': ['cbm', 'volume', 'cubic meter', 'cubic meters'],
        'required': False,
        'ontology_tag': 'kg:Volume',
        'shacl_rule': 'sct:PositiveNumber',
        'transform': 'calculate_from_dimensions'
    },
    'gw': {
        'patterns': ['g.w', 'g.w(kg)', 'gross weight', 'weight', 'kg'],
        'required': False,
        'ontology_tag': 'kg:GrossWeight',
        'shacl_rule': 'sct:PositiveNumber',
        'transform': 'estimate_from_cbm_if_missing'
    },
    
    # === NEW FIELDS (v1.2) ===
    'hs_code': {
        'patterns': ['hs code', 'hs_code', 'tariff code', 'tariff', 'hs'],
        'required': False,
        'ontology_tag': 'kg:TariffCode',
        'shacl_rule': 'sct:HS6',
        'transform': 'validate_hs_code'
    },
    'incoterm': {
        'patterns': ['incoterm', 'incoterms', 'trade term', 'delivery term'],
        'required': False,
        'ontology_tag': 'kg:Incoterm',
        'shacl_rule': 'sct:Incoterm',
        'transform': 'normalize_incoterm'
    },
    'oog_flag': {
        'patterns': ['oog', 'oversize', 'over dimension', 'out of gauge'],
        'required': False,
        'ontology_tag': 'kg:OversizeFlag',
        'shacl_rule': 'sct:Boolean',
        'transform': 'calculate_oog_flag'
    },
    'package_type': {
        'patterns': ['pkg type', 'package type', 'packaging', 'package'],
        'required': False,
        'ontology_tag': 'kg:PackageType',
        'shacl_rule': 'sct:EnumPkg',
        'transform': 'classify_package_type'
    },
    
    # === DIMENSION FIELDS (for calculations) ===
    'length': {
        'patterns': ['l(cm)', 'length', 'l', 'length(cm)', 'l_cm'],
        'required': False,
        'ontology_tag': 'kg:Length',
        'shacl_rule': 'sct:PositiveNumber',
        'transform': 'cm_to_m'
    },
    'width': {
        'patterns': ['w(cm)', 'width', 'w', 'width(cm)', 'w_cm'],
        'required': False,
        'ontology_tag': 'kg:Width',
        'shacl_rule': 'sct:PositiveNumber',
        'transform': 'cm_to_m'
    },
    'height': {
        'patterns': ['h(cm)', 'height', 'h', 'height(cm)', 'h_cm'],
        'required': False,
        'ontology_tag': 'kg:Height',
        'shacl_rule': 'sct:PositiveNumber',
        'transform': 'cm_to_m'
    },
    
    # === LEGACY FIELDS (for backward compatibility) ===
    'description': {
        'patterns': ['description', 'desc', 'item description', 'product description'],
        'required': False,
        'ontology_tag': 'kg:Description',
        'shacl_rule': 'sct:Text',
        'transform': 'clean_text'
    },
    'unit': {
        'patterns': ['unit', 'uom', 'unit of measure'],
        'required': False,
        'ontology_tag': 'kg:UnitOfMeasure',
        'shacl_rule': 'sct:EnumUnit',
        'transform': 'normalize_unit'
    }
}

# 2. FILE CONFIGURATION: Defines the location and type of each data file
# =============================================================================
# Each entry defines a data file with its path, sheet name, and type.

FILE_CONFIG = {
    'DSV_ONHAND': {
        'path': 'analyzer/data/HVDC Stock OnHand Report.xlsx',
        'sheet_name': 'Case List',
        'type': 'onhand'  # 실제 재고 기준 파일
    },
    'STOCK_ONHAND': {
        'path': 'analyzer/data/HVDC Stock OnHand Report.xlsx',
        'sheet_name': 'Case List',
        'type': 'onhand'  # This file is the source of truth for current stock
    },
    'HITACHI': {
        'path': 'analyzer/data/HVDC WAREHOUSE_HITACHI(HE).xlsx',
        'sheet_name': 'Case List',  # Auto-detect if this fails
        'type': 'movement'
    },
    'HITACHI_LOCAL': {
        'path': 'analyzer/data/HVDC WAREHOUSE_HITACHI(HE_LOCAL).xlsx',
        'sheet_name': 'CASE LIST',
        'type': 'movement'
    },
    'HITACHI_LOT': {
        'path': 'analyzer/data/HVDC WAREHOUSE_HITACHI(HE-0214,0252).xlsx',
        'sheet_name': 'CASE LIST',
        'type': 'movement',
        'engine': 'openpyxl' # Specify engine for potentially problematic files
    },
    'SIEMENS': {
        'path': 'analyzer/data/HVDC WAREHOUSE_SIMENSE(SIM).xlsx',
        'sheet_name': 'CASE LIST',
        'type': 'movement'
    },
}

# 3. WAREHOUSE & SITE CONFIGURATION: Defines columns for specific logic
# =============================================================================
# Used to differentiate warehouse movements from final site deliveries.

WAREHOUSE_COLS_MAP = {
    'HITACHI': ['DSV Outdoor', 'DSV Indoor', 'DSV Al Markaz', 'Hauler Indoor', 'DSV MZP', 'MOSB'],
    'HITACHI_LOCAL': ['DSV Outdoor', 'DSV Al Markaz', 'DSV MZP', 'MOSB'],
    'HITACHI_LOT': ['DSV Indoor', 'DHL WH', 'DSV Al Markaz', 'AAA Storage'],
    'SIEMENS': ['DSV Outdoor', 'DSV Indoor', 'DSV Al Markaz', 'MOSB', 'AAA Storage'],
}

SITE_COLS = ['DAS', 'MIR', 'SHU', 'AGI']

# 4. ANALYSIS PARAMETERS
# =============================================================================
DEADSTOCK_DAYS = 90
TARGET_MONTH = "2025-06" # 분석 기준 월
