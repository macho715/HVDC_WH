# /analyzer/normalizer.py
"""
Data Normalization Module

This module is responsible for transforming raw, messy data into a clean,
standardized format based on the ontology mapping.
"""

import pandas as pd
import re
from typing import Dict, List, Optional


class DataNormalizer:
    """
    Responsible for taking raw DataFrames and transforming them into clean,
    standardized format based on the ontology mapping.
    
    This class follows the Single Responsibility Principle - it only handles
    data normalization and cleaning, nothing else.
    """
    
    def __init__(self, ontology_map: Dict[str, List[str]]):
        """
        Initialize the normalizer with ontology mapping.
        
        Args:
            ontology_map: Dictionary mapping standard column names to possible aliases
        """
        self.ontology_map = ontology_map
    
    def _find_column(self, df_columns: pd.Index, standard_name: str) -> Optional[str]:
        """
        Find the actual column name in a dataframe based on ontology aliases.
        
        Args:
            df_columns: DataFrame columns
            standard_name: Standard column name to find
            
        Returns:
            Actual column name if found, None otherwise
        """
        aliases = self.ontology_map.get(standard_name, [])
        df_cols_lower = {str(col).lower().strip(): col for col in df_columns}
        
        for alias in aliases:
            if alias.lower() in df_cols_lower:
                return df_cols_lower[alias.lower()]
        return None
    
    def normalize(self, df: pd.DataFrame, file_key: str) -> Optional[pd.DataFrame]:
        """
        Transform a raw DataFrame into a standardized format.
        
        Args:
            df: Raw DataFrame to normalize
            file_key: Identifier for the source file (for logging)
            
        Returns:
            Standardized DataFrame or None if critical columns are missing
        """
        if df.empty:
            print(f"   - ⚠️ WARNING: Empty DataFrame for {file_key}")
            return None
            
        standard_df = pd.DataFrame(index=df.index)
        
        # Normalize all fields defined in the ontology
        for standard_name in self.ontology_map.keys():
            found_col = self._find_column(df.columns, standard_name)
            if found_col:
                standard_df[standard_name] = df[found_col]
        
        # Critical validation: Case No. is required
        if 'case_no' not in standard_df.columns:
            print(f"   - ❌ CRITICAL: 'Case No.' column not found in {file_key}. File cannot be processed.")
            return None
        
        # Standardize Case No. to string to prevent merge errors
        standard_df['case_no'] = standard_df['case_no'].astype(str)
        
        # Standardize dimensions and units
        self._normalize_dimensions(df, standard_df)
        
        # Calculate derived metrics
        self._calculate_derived_metrics(standard_df)
        
        # Standardize quantity
        self._normalize_quantity(df, standard_df)
        
        # Add source tracking
        standard_df['supplier_key'] = file_key
        
        print(f"   ✅ Normalized {len(standard_df)} records from {file_key}")
        return standard_df
    
    def _normalize_dimensions(self, df: pd.DataFrame, standard_df: pd.DataFrame) -> None:
        """Normalize dimension columns (length, width, height, weight)."""
        dimension_fields = {
            'length': 'length',
            'width': 'width', 
            'height': 'height',
            'gross_weight': 'gross_weight'
        }
        
        for dim, field_name in dimension_fields.items():
            col_name = self._find_column(df.columns, field_name)
            if col_name:
                series = pd.to_numeric(df[col_name], errors='coerce').fillna(0)
                # Convert cm to meters if needed
                if '(cm)' in str(col_name).lower():
                    series /= 100
                standard_df[dim] = series
            else:
                standard_df[dim] = 0
    
    def _calculate_derived_metrics(self, standard_df: pd.DataFrame) -> None:
        """Calculate derived metrics like square meters and cubic meters."""
        standard_df['sqm'] = standard_df.get('length', 0) * standard_df.get('width', 0)
        standard_df['cbm'] = standard_df['sqm'] * standard_df.get('height', 0)
    
    def _normalize_quantity(self, df: pd.DataFrame, standard_df: pd.DataFrame) -> None:
        """Normalize quantity field with special handling for EA units."""
        if 'quantity' in standard_df.columns:
            unit_col_name = self._find_column(df.columns, 'unit')
            if unit_col_name and 'unit' in standard_df.columns:
                # Set quantity to 1 for EA units if quantity is missing
                ea_mask = (standard_df['unit'].str.upper() == 'EA') & (standard_df['quantity'].isna())
                standard_df.loc[ea_mask, 'quantity'] = 1
            
            standard_df['quantity'] = pd.to_numeric(standard_df['quantity'], errors='coerce').fillna(0)
        else:
            standard_df['quantity'] = 1
