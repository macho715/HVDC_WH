# Warehouse Analysis System

A professional, modular warehouse analysis system designed for maintainability, scalability, and reliability.

## 🏗️ Architecture Overview

This system follows **Separation of Concerns** and **Single Responsibility Principle** to create a clean, maintainable codebase.

### Core Components

```
WAREHOUSE_A/
├── main.py              # 🎯 Main orchestration script
├── config.py            # ⚙️ Centralized configuration
├── requirements.txt     # 📦 Dependencies
├── README.md           # 📚 Documentation
└── analyzer/           # 🔧 Core analysis package
    ├── __init__.py     # Package initialization
    ├── normalizer.py   # Data standardization
    ├── calculator.py   # Business logic calculations
    └── reporter.py     # Excel report generation
```

## 🎯 Component Responsibilities

### 1. `main.py` - Orchestrator
- **Single Responsibility**: Workflow coordination only
- Coordinates the entire analysis pipeline
- Handles data loading, analysis execution, and report generation
- Clean, readable workflow that's easy to understand and modify

### 2. `config.py` - Configuration Hub
- **Single Responsibility**: All system configuration
- Centralized settings for file paths, ontology mapping, analysis parameters
- Easy to modify without touching code
- Single source of truth for all system parameters

### 3. `analyzer/normalizer.py` - Data Standardization
- **Single Responsibility**: Data cleaning and standardization
- Transforms messy real-world data into clean, standardized format
- Uses ontology mapping to handle different file formats
- No business logic, just data transformation

### 4. `analyzer/calculator.py` - Business Logic
- **Single Responsibility**: Calculations and analysis
- Performs all business logic calculations
- Works only with clean, standardized data
- No file handling or reporting logic

### 5. `analyzer/reporter.py` - Report Generation
- **Single Responsibility**: Excel report creation
- Handles all Excel formatting and styling
- Creates professional, formatted reports
- No data processing or calculations

## 🚀 Getting Started

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd WAREHOUSE_A
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Prepare data files**
   - Place your Excel files in the `data/` directory
   - Update `config.py` with correct file paths and sheet names

4. **Run the analysis**
   ```bash
   python main.py
   ```

## 📊 Analysis Features

### Current Capabilities
- **Full Stock List Generation**: Complete inventory with details
- **Stock Verification**: Compare calculated vs. actual stock levels
- **Discrepancy Detection**: Identify stock discrepancies automatically

### Future Enhancements
- **Deadstock Analysis**: Identify slow-moving inventory
- **Warehouse Classification**: Indoor vs. outdoor analysis
- **Trend Analysis**: Historical movement patterns

## ⚙️ Configuration

### File Configuration
Update `config.py` to match your data files:

```python
FILE_CONFIG = {
    'STOCK_ONHAND': {
        'path': 'data/your_onhand_file.xlsx',
        'sheet_name': 'Sheet1',
        'type': 'onhand'
    },
    # Add more files as needed
}
```

### Ontology Mapping
Map your column names to standard format:

```python
ONTOLOGY_MAP = {
    'case_no': ['Case No.', 'MR#', 'Your Column Name'],
    'quantity': ['QTY', 'Quantity', 'Your Qty Column'],
    # Add more mappings
}
```

## 🔧 Extending the System

### Adding New Analysis Types

1. **Add calculation method** in `analyzer/calculator.py`:
   ```python
   def run_new_analysis(self, data):
       # Your analysis logic here
       return results
   ```

2. **Add to main pipeline** in `main.py`:
   ```python
   new_report = analyzer.run_new_analysis(full_stock_list)
   reports_to_generate['New_Analysis'] = new_report
   ```

### Adding New Data Sources

1. **Update file configuration** in `config.py`
2. **Add ontology mappings** if needed
3. **The system will automatically handle the new data**

## 📈 Benefits of This Architecture

### 🛠️ Maintainability
- **Single Responsibility**: Each component has one clear purpose
- **Loose Coupling**: Components don't depend on each other's internals
- **Easy Testing**: Each component can be tested independently

### 🔄 Scalability
- **Modular Design**: Easy to add new features without breaking existing code
- **Configuration-Driven**: Most changes require only config updates
- **Extensible**: New analysis types can be added easily

### 🎯 Reliability
- **Error Isolation**: Problems in one component don't affect others
- **Clear Data Flow**: Easy to trace data through the system
- **Validation**: Built-in data validation and error handling

### 📚 Readability
- **Self-Documenting**: Code structure clearly shows system purpose
- **Consistent Patterns**: Similar operations follow the same patterns
- **Clear Interfaces**: Each component has well-defined inputs and outputs

## 🐛 Troubleshooting

### Common Issues

1. **File Not Found Errors**
   - Check file paths in `config.py`
   - Ensure data files exist in the specified locations

2. **Column Mapping Issues**
   - Verify ontology mappings in `config.py`
   - Check that column names match exactly (case-sensitive)

3. **Empty Reports**
   - Verify data files contain the expected data
   - Check sheet names in configuration

### Debug Mode

Add debug prints to any component for troubleshooting:

```python
print(f"Debug: Processing {len(df)} records from {file_key}")
```

## 📞 Support

For issues or questions:
1. Check the configuration settings
2. Verify data file formats
3. Review error messages for specific issues
4. Check the troubleshooting section above

---

**Built with ❤️ following software engineering best practices** 