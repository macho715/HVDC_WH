# /analyzer/reporter.py
"""
Excel Reporter Module

This module is responsible for creating formatted Excel reports from analysis results.
It handles all Excel-specific formatting, styling, and file generation.
"""

import pandas as pd
from datetime import datetime
import os
import subprocess
from typing import Dict, Optional


class ExcelReporter:
    """
    Responsible for creating formatted Excel reports from analysis results.
    
    This class follows the Single Responsibility Principle - it only handles
    Excel report generation and formatting, not data analysis or calculations.
    """
    
    def __init__(self, reports_dict: Dict[str, pd.DataFrame]):
        """
        Initialize the reporter with analysis results.
        
        Args:
            reports_dict: Dictionary mapping sheet names to DataFrames
        """
        self.reports = reports_dict
        self.output_dir = "outputs"
        self._ensure_output_directory()
    
    def create_report(self, filename_prefix: str = "Inventory_Report") -> str:
        """
        Create a multi-sheet Excel report and save it.
        
        Args:
            filename_prefix: Prefix for the output filename
            
        Returns:
            Path to the created Excel file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{filename_prefix}_{timestamp}.xlsx"
        output_path = os.path.join(self.output_dir, output_filename)
        
        print("\n✍️ Creating Excel report...")
        
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            for sheet_name, df in self.reports.items():
                if not df.empty:
                    self._format_sheet(df, writer, sheet_name)
                    print(f"   ✅ '{sheet_name}' sheet created with {len(df)} rows")
                else:
                    print(f"   ⚠️ Skipping empty '{sheet_name}' sheet")
        
        print(f"\n📦 Report saved to: '{output_path}'")
        
        # Try to open the file automatically
        self._open_file(output_path)
        
        return output_path
    
    def _ensure_output_directory(self) -> None:
        """Ensure the output directory exists."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"   📁 Created output directory: {self.output_dir}")
    
    def _format_sheet(self, df: pd.DataFrame, writer: pd.ExcelWriter, sheet_name: str) -> None:
        """
        Format a single Excel sheet with proper styling.
        
        Args:
            df: DataFrame to write to the sheet
            writer: ExcelWriter instance
            sheet_name: Name of the sheet
        """
        # Write the DataFrame to Excel
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Get the workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # Create header format
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#D7E4BC',
            'border': 1,
            'align': 'center'
        })
        
        # Apply header formatting
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Auto-adjust column widths
        self._adjust_column_widths(df, worksheet)
        
        # Add conditional formatting for discrepancies if applicable
        self._add_conditional_formatting(df, worksheet, workbook)
    
    def _adjust_column_widths(self, df: pd.DataFrame, worksheet) -> None:
        """
        Automatically adjust column widths based on content.
        
        Args:
            df: DataFrame containing the data
            worksheet: Worksheet object to format
        """
        for i, col in enumerate(df.columns):
            # Calculate maximum width needed
            col_width = max(
                df[col].astype(str).map(len).max(),
                len(str(col))
            ) + 2
            
            # Set reasonable limits
            col_width = min(max(col_width, 10), 50)
            
            worksheet.set_column(i, i, col_width)
    
    def _add_conditional_formatting(self, df: pd.DataFrame, worksheet, workbook) -> None:
        """
        Add conditional formatting for special columns.
        
        Args:
            df: DataFrame containing the data
            worksheet: Worksheet object to format
            workbook: Workbook object for creating formats
        """
        # Add red formatting for negative discrepancies
        if 'Discrepancy' in df.columns:
            red_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
            green_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
            
            # Find the column index for Discrepancy
            discrepancy_col = df.columns.get_loc('Discrepancy')
            
            # Apply conditional formatting
            worksheet.conditional_format(
                1, discrepancy_col, len(df), discrepancy_col,
                {'type': 'cell', 'criteria': '<', 'value': 0, 'format': red_format}
            )
            worksheet.conditional_format(
                1, discrepancy_col, len(df), discrepancy_col,
                {'type': 'cell', 'criteria': '>', 'value': 0, 'format': green_format}
            )
    
    def _open_file(self, file_path: str) -> None:
        """
        Try to open the Excel file automatically.
        
        Args:
            file_path: Path to the Excel file
        """
        try:
            if os.name == 'nt':  # Windows
                subprocess.run(['start', file_path], shell=True, check=True)
            elif os.name == 'posix':  # macOS/Linux
                if os.system(f"open '{file_path}'") != 0:
                    os.system(f"xdg-open '{file_path}'")
        except Exception as e:
            print(f"⚠️ Could not open the file automatically: {e}")
            print(f"   Please open manually: {file_path}")
    
    def get_report_summary(self) -> Dict[str, int]:
        """
        Get a summary of the reports being generated.
        
        Returns:
            Dictionary mapping sheet names to row counts
        """
        summary = {}
        for sheet_name, df in self.reports.items():
            summary[sheet_name] = len(df) if not df.empty else 0
        return summary
