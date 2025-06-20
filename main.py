# /main.py
"""
Warehouse Analysis Main Pipeline

This is the main orchestration script that coordinates the entire analysis pipeline.
It follows the Single Responsibility Principle by only handling workflow coordination.
"""

import pandas as pd
import os
from typing import Dict, Optional

from analyzer.normalizer import DataNormalizer
from analyzer.calculator import AnalysisCalculator
from analyzer.reporter import ExcelReporter
import config


class WarehouseAnalyzer:
    """
    Main orchestrator class that coordinates the entire analysis pipeline.
    
    This class follows the Single Responsibility Principle - it only handles
    workflow coordination, not data processing or calculations.
    """
    
    def __init__(self, file_config: Dict, ontology_map: Dict):
        """
        Initialize the analyzer with configuration.
        
        Args:
            file_config: Configuration for data files
            ontology_map: Ontology mapping for column standardization
        """
        self.file_config = file_config
        self.ontology_map = ontology_map
        
        # Initialize components
        self.normalizer = DataNormalizer(ontology_map)
        self.calculator = AnalysisCalculator(config.__dict__)
        
        # Data storage
        self.onhand_data = pd.DataFrame()
        self.movement_data = {}
    
    def load_data(self) -> None:
        """
        Load and normalize all data files.
        """
        print("🔄 Loading and normalizing data...")
        
        # Load onhand data (source of truth)
        self._load_onhand_data()
        
        # Load movement data
        self._load_movement_data()
        
        # Set data for calculator
        self.calculator.set_data(self.onhand_data, self.movement_data)
        
        print(f"✅ Data loading complete. OnHand: {len(self.onhand_data)} records, "
              f"Movement files: {len(self.movement_data)}")
    
    def generate_full_stock_list(self) -> pd.DataFrame:
        """
        Generate the complete stock list.
        
        Returns:
            DataFrame containing full stock information
        """
        return self.calculator.generate_full_stock_list()
    
    def run_stock_verification(self, full_stock_list: pd.DataFrame) -> pd.DataFrame:
        """
        Run stock verification analysis.
        
        Args:
            full_stock_list: Complete stock list to verify
            
        Returns:
            DataFrame containing verification results
        """
        return self.calculator.run_stock_verification(full_stock_list)
    
    def run_deadstock_analysis(self, full_stock_list: pd.DataFrame) -> pd.DataFrame:
        """
        Run deadstock analysis.
        
        Args:
            full_stock_list: Complete stock list to analyze
            
        Returns:
            DataFrame containing deadstock items
        """
        return self.calculator.run_deadstock_analysis(full_stock_list)
    
    def _load_onhand_data(self) -> None:
        """Load and normalize onhand data."""
        onhand_config = self.file_config.get('STOCK_ONHAND')
        if not onhand_config:
            print("⚠️ No STOCK_ONHAND configuration found")
            return
        
        file_path = onhand_config['path']
        sheet_name = onhand_config['sheet_name']
        
        if not os.path.exists(file_path):
            print(f"⚠️ OnHand file not found: {file_path}")
            return
        
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            normalized_df = self.normalizer.normalize(df, 'STOCK_ONHAND')
            if normalized_df is not None:
                self.onhand_data = normalized_df
        except Exception as e:
            print(f"❌ Error loading OnHand data: {e}")
    
    def _load_movement_data(self) -> None:
        """Load and normalize movement data from all suppliers."""
        for file_key, file_config in self.file_config.items():
            if file_key == 'STOCK_ONHAND':
                continue  # Skip onhand file as it's handled separately
            
            file_path = file_config['path']
            sheet_name = file_config['sheet_name']
            
            if not os.path.exists(file_path):
                print(f"⚠️ Movement file not found: {file_path}")
                continue
            
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                normalized_df = self.normalizer.normalize(df, file_key)
                if normalized_df is not None:
                    self.movement_data[file_key] = normalized_df
            except Exception as e:
                print(f"❌ Error loading {file_key} data: {e}")


def main():
    """
    Main pipeline to run the warehouse analysis.
    
    This function orchestrates the entire analysis workflow:
    1. Initialize components
    2. Load and normalize data
    3. Run analyses
    4. Generate reports
    """
    print("🚀 Starting Warehouse Analysis Pipeline")
    print("=" * 50)
    
    # 1. Initialize the analyzer with configurations
    analyzer = WarehouseAnalyzer(
        file_config=config.FILE_CONFIG,
        ontology_map=config.ONTOLOGY_MAP
    )
    
    # 2. Load and normalize all data
    analyzer.load_data()
    
    # 3. Run all analyses
    reports_to_generate = {}
    
    # Run core analyses based on the OnHand report
    if not analyzer.onhand_data.empty:
        # Generate the primary stock list
        full_stock_list = analyzer.generate_full_stock_list()
        reports_to_generate['Full_Stock_List'] = full_stock_list
        
        # Generate the verification report
        verification_report = analyzer.run_stock_verification(full_stock_list)
        reports_to_generate['Stock_Verification'] = verification_report
        
        # Future analyses can be added here
        # deadstock_report = analyzer.run_deadstock_analysis(full_stock_list)
        # reports_to_generate['DeadStock'] = deadstock_report
        
    else:
        print("\n⚠️ Skipping OnHand-based reports because OnHand data is missing.")
    
    # 4. Create the Excel report
    if reports_to_generate:
        reporter = ExcelReporter(reports_to_generate)
        output_path = reporter.create_report()
        
        # Print summary
        summary = reporter.get_report_summary()
        print("\n📊 Report Summary:")
        for sheet_name, row_count in summary.items():
            print(f"   - {sheet_name}: {row_count} rows")
    else:
        print("\n⚠️ No reports to generate.")
    
    print("\n✅ Analysis pipeline complete!")


if __name__ == '__main__':
    main()
