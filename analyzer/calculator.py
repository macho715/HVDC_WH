# /analyzer/calculator.py
"""
Analysis Calculator Module

This module is responsible for performing all business logic calculations
based on normalized data. It does not know about files or data loading,
only about clean, standardized DataFrames.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional


class AnalysisCalculator:
    """
    Performs all business logic calculations based on normalized data.
    
    This class follows the Single Responsibility Principle - it only handles
    calculations and analysis, not data loading or reporting.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the calculator with configuration.
        
        Args:
            config: Configuration dictionary containing analysis parameters
        """
        self.config = config
        self.onhand_data = pd.DataFrame()
        self.movement_data = {}
    
    def set_data(self, onhand_df: pd.DataFrame, movement_dfs: Dict[str, pd.DataFrame]) -> None:
        """
        Set the data for analysis. This separates data loading from calculation logic.
        
        Args:
            onhand_df: Normalized onhand data
            movement_dfs: Dictionary of normalized movement data by supplier
        """
        self.onhand_data = onhand_df
        self.movement_data = movement_dfs
    
    def generate_full_stock_list(self) -> pd.DataFrame:
        """
        Create the master list of all items currently in stock.
        
        Returns:
            DataFrame containing complete stock information
        """
        if self.onhand_data.empty:
            print("   - ⚠️ No onhand data available for stock list generation")
            return pd.DataFrame()
        
        print("   - Generating full stock list...")
        
        # Get master details from movement files
        master_details = self._get_master_details()
        
        if master_details.empty:
            print("   - ⚠️ No movement data available for details")
            return self.onhand_data
        
        # Merge onhand data with master details
        full_stock_list = pd.merge(
            self.onhand_data,
            master_details.drop(columns='supplier_key', errors='ignore'),
            on='case_no',
            how='left',
            suffixes=('_onhand', '_detail')
        )
        
        print(f"   ✅ Generated full stock list with {len(full_stock_list)} items")
        return full_stock_list
    
    def run_stock_verification(self, full_stock_list: pd.DataFrame) -> pd.DataFrame:
        """
        Compare calculated stock from movements against the OnHand report.
        
        Args:
            full_stock_list: The complete stock list to verify
            
        Returns:
            DataFrame containing discrepancies found
        """
        if self.onhand_data.empty:
            print("   - ⚠️ No onhand data available for verification")
            return pd.DataFrame()
        
        print("   - Running stock verification...")
        
        # Calculate stock from movement history
        calculated_stock = self._calculate_stock_from_movements()
        
        if calculated_stock.empty:
            print("   - ⚠️ Could not calculate stock from movement files. Skipping verification.")
            return pd.DataFrame()
        
        # Prepare actual stock data
        actual_stock = self.onhand_data[['case_no', 'quantity']].rename(
            columns={'quantity': 'ActualQty'}
        )
        
        # Merge and calculate discrepancies
        verification_df = pd.merge(
            calculated_stock, 
            actual_stock, 
            on='case_no', 
            how='outer'
        ).fillna(0)
        
        verification_df['Discrepancy'] = (
            verification_df['ActualQty'] - verification_df['CalculatedStock']
        )
        
        # Filter to only show discrepancies
        discrepancies = verification_df[verification_df['Discrepancy'] != 0].copy()
        
        print(f"   ✅ Stock verification complete. Found {len(discrepancies)} discrepancies.")
        return discrepancies
    
    def run_deadstock_analysis(self, full_stock_list: pd.DataFrame) -> pd.DataFrame:
        """
        Identify items that have not moved for a long time.
        
        Args:
            full_stock_list: The complete stock list to analyze
            
        Returns:
            DataFrame containing deadstock items
        """
        print("   - Running deadstock analysis...")
        
        if self.movement_data.empty:
            print("   - ⚠️ No movement data available for deadstock analysis")
            return pd.DataFrame()
        
        # This is a placeholder for deadstock analysis
        # Implementation would require tracking last move dates
        deadstock_days = self.config.get('DEADSTOCK_DAYS', 90)
        
        print(f"   - Deadstock analysis configured for {deadstock_days} days threshold")
        print("   - ⚠️ Deadstock analysis feature not yet implemented")
        
        return pd.DataFrame()
    
    def _get_master_details(self) -> pd.DataFrame:
        """
        Get master details from all movement files.
        
        Returns:
            DataFrame with master details for all items
        """
        if not self.movement_data:
            return pd.DataFrame()
        
        all_details = []
        for supplier, df in self.movement_data.items():
            if not df.empty:
                all_details.append(df)
        
        if not all_details:
            return pd.DataFrame()
        
        master_details = pd.concat(all_details, ignore_index=True)
        return master_details.drop_duplicates(subset=['case_no'], keep='first')
    
    def _calculate_stock_from_movements(self) -> pd.DataFrame:
        """
        Calculate final stock positions based on movement files.
        
        Returns:
            DataFrame with calculated stock quantities
        """
        all_statuses = []
        
        for supplier, df in self.movement_data.items():
            if df.empty:
                continue
                
            warehouse_cols = self.config.get('WAREHOUSE_COLS_MAP', {}).get(supplier, [])
            site_cols = self.config.get('SITE_COLS', [])
            all_locs = warehouse_cols + site_cols
            
            # Find location columns that exist in this dataframe
            value_vars = [col for col in all_locs if col in df.columns]
            if not value_vars:
                continue
            
            # Melt the dataframe to get location-date pairs
            id_vars = ['case_no', 'quantity']
            melted = df.melt(
                id_vars=id_vars, 
                value_vars=value_vars, 
                var_name='location', 
                value_name='date'
            ).dropna(subset=['date'])
            
            if melted.empty:
                continue
            
            # Get the last status for each case
            melted.sort_values(by=['case_no', 'date'], inplace=True)
            last_status = melted.drop_duplicates(subset=['case_no'], keep='last')
            
            # Filter to only warehouse locations (not sites)
            in_warehouse = last_status[last_status['location'].isin(warehouse_cols)]
            all_statuses.append(in_warehouse)
        
        if not all_statuses:
            return pd.DataFrame(columns=['case_no', 'CalculatedStock'])
        
        # Combine all statuses and calculate total stock
        calculated_df = pd.concat(all_statuses, ignore_index=True)
        stock_summary = calculated_df.groupby('case_no')['quantity'].sum().reset_index()
        stock_summary.rename(columns={'quantity': 'CalculatedStock'}, inplace=True)
        
        return stock_summary
