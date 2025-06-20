# /main.py

from analyzer.normalizer import DataNormalizer
from analyzer.calculator import AnalysisCalculator
from analyzer.reporter import ExcelReporter
import config  # Import the configuration file

def main():
    """
    Main pipeline to run the warehouse analysis.
    This script orchestrates the loading, analysis, and reporting steps.
    """
    # 1. Initialize the analyzer with configurations
    analyzer = WarehouseAnalyzer(
        file_config=config.FILE_CONFIG,
        ontology_map=config.ONTOLOGY_MAP
    )
    
    # 2. Load and normalize all data
    analyzer.load_data()

    # 3. Run all analyses
    # This dictionary will hold all the DataFrames for the final report.
    reports_to_generate = {}
    
    # Run core analyses based on the OnHand report
    if not analyzer.onhand_data.empty:
        # Generate the primary, most accurate stock list
        full_stock_list = analyzer.generate_full_stock_list()
        reports_to_generate['Full_Stock_List'] = full_stock_list
        
        # Generate the verification report
        verification_report = analyzer.run_stock_verification(full_stock_list)
        reports_to_generate['Stock_Verification'] = verification_report
        
        # Add other analyses here in the future
        # e.g., deadstock_report = analyzer.run_deadstock_analysis(full_stock_list)
        # reports_to_generate['DeadStock'] = deadstock_report

    else:
        print("\nSkipping OnHand-based reports because OnHand data is missing.")


    # 4. Create the Excel report from the generated DataFrames
    reporter = ExcelReporter(reports_to_generate)
    reporter.create_report()

if __name__ == '__main__':
    main()
