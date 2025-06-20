# /analyzer/normalizer.py
"""
Data Normalization Module

This module is responsible for transforming raw, messy data into a clean,
standardized format based on the ontology mapping.
"""

import pandas as pd
import re
from typing import Dict, List, Optional, Any
import numpy as np
from datetime import datetime
import warnings


class DataNormalizer:
    """
    Responsible for taking raw DataFrames and transforming them into clean,
    standardized format based on the ontology mapping.
    
    This class follows the Single Responsibility Principle - it only handles
    data normalization and cleaning, nothing else.
    """
    
    def __init__(self, ontology_map: Dict[str, Any]):
        """
        Initialize the normalizer with ontology mapping.
        
        Args:
            ontology_map: Dictionary mapping standard column names to possible aliases
        """
        self.ontology_map = ontology_map
        self.validation_errors = []
    
    def _find_column(self, df_columns: pd.Index, standard_name: str) -> Optional[str]:
        """Find the best matching column for a standard name using patterns (부분 일치, 대소문자/공백 무시)."""
        patterns = self.ontology_map[standard_name]['patterns']
        for col in df_columns:
            col_norm = str(col).replace(' ', '').replace('_', '').lower()
            for pattern in patterns:
                pat_norm = pattern.replace(' ', '').replace('_', '').lower()
                if pat_norm in col_norm or col_norm in pat_norm:
                    return col
        return None
    
    def normalize(self, df: pd.DataFrame, source_name: str) -> Optional[pd.DataFrame]:
        """
        데이터프레임을 정규화하고 스키마 v1.2 규칙을 적용합니다.
        """
        if df.empty:
            print(f"   - ⚠️ WARNING: Empty DataFrame for {source_name}")
            return None
            
        print(f"   - Normalizing {source_name} data...")
        
        # 1. 기존 정규화 로직 (하위 호환성 유지)
        standard_df = pd.DataFrame(index=df.index)
        
        # 기존 온톨로지 매핑 적용
        for standard_name in self.ontology_map.keys():
            found_col = self._find_column(df.columns, standard_name)
            if found_col:
                standard_df[standard_name] = df[found_col]
        
        # Critical validation: Case No. is required
        if 'case_no' not in standard_df.columns:
            print(f"   - ❌ CRITICAL: 'Case No.' column not found in {source_name}. File cannot be processed.")
            return None
        
        # Standardize Case No. to string to prevent merge errors
        standard_df['case_no'] = standard_df['case_no'].astype(str)
        
        # Standardize dimensions and units
        self._normalize_dimensions(df, standard_df)
        
        # Calculate derived metrics
        self._calculate_derived_metrics(standard_df)
        
        # Standardize quantity
        self._normalize_quantity(df, standard_df)
        
        # Preserve location columns (warehouses and sites)
        self._preserve_location_columns(df, standard_df)
        
        # Add source tracking
        standard_df['supplier_key'] = source_name
        
        # 2. 스키마 v1.2 변환 규칙 추가 적용
        standard_df = self._apply_schema_v1_2_transformations(standard_df)
        
        # 3. SHACL 검증
        validation_results = self._validate_shacl_rules(standard_df)
        
        # 4. 검증 결과 보고
        self._report_validation_results(validation_results, source_name)
        
        print(f"   ✅ Normalized {len(standard_df)} records from {source_name}")
        return standard_df
    
    def _apply_schema_v1_2_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """스키마 v1.2 변환 규칙을 추가로 적용합니다."""
        transformed_df = df.copy()
        
        # HS 코드 검증
        if 'hs_code' in transformed_df.columns:
            transformed_df = self._transform_validate_hs_code(transformed_df, 'hs_code')
        
        # Incoterm 정규화
        if 'incoterm' in transformed_df.columns:
            transformed_df = self._transform_normalize_incoterm(transformed_df, 'incoterm')
        
        # OOG 플래그 계산
        if all(col in transformed_df.columns for col in ['length', 'width', 'height']):
            transformed_df['oog_flag'] = self._calculate_oog_flag(transformed_df)
        
        # 패키지 타입 분류
        if 'package_type' in transformed_df.columns:
            transformed_df = self._transform_classify_package_type(transformed_df, 'package_type')
        
        # 저장 타입 매핑
        if 'storage_type' in transformed_df.columns:
            transformed_df = self._transform_map_storage_type(transformed_df, 'storage_type')
        
        return transformed_df
    
    def _calculate_oog_flag(self, df: pd.DataFrame) -> pd.Series:
        """초과 규격 플래그를 계산합니다."""
        # 40'HC 기준 (12.192m x 2.438m x 2.896m)
        hc_length, hc_width, hc_height = 12.192, 2.438, 2.896
        
        oog_conditions = (
            (df['length'] > hc_length) | 
            (df['width'] > hc_width) | 
            (df['height'] > hc_height)
        )
        
        return oog_conditions
    
    def _normalize_dimensions(self, df: pd.DataFrame, standard_df: pd.DataFrame) -> None:
        """Normalize dimension columns (length, width, height, weight)."""
        dimension_fields = {
            'length': 'length',
            'width': 'width', 
            'height': 'height',
            'gross_weight': 'gw'
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
    
    def _preserve_location_columns(self, df: pd.DataFrame, standard_df: pd.DataFrame) -> None:
        """Preserve location columns (warehouses and sites)."""
        # Common warehouse and site column patterns
        warehouse_patterns = ['DSV', 'Hauler', 'MOSB', 'AAA', 'DHL', 'MZD', 'HALUER']
        site_patterns = ['MIR', 'SHU', 'DAS', 'AGI']
        
        # Find and preserve warehouse columns
        for col in df.columns:
            if any(pattern in str(col) for pattern in warehouse_patterns + site_patterns):
                standard_df[col] = df[col]
    
    def _transform_to_numeric_with_ea_correction(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """수량을 숫자로 변환하고 EA 단위 NaN 시 1로 보정합니다."""
        if field_name not in df.columns:
            return df
            
        # 숫자 변환
        df[field_name] = pd.to_numeric(df[field_name], errors='coerce')
        
        # EA 단위 NaN 처리
        if 'unit' in df.columns:
            ea_mask = (df['unit'].str.lower() == 'ea') & (df[field_name].isna())
            df.loc[ea_mask, field_name] = 1
        else:
            # unit 컬럼이 없으면 모든 NaN을 1로 처리
            df[field_name] = df[field_name].fillna(1)
            
        return df
    
    def _transform_to_period_month(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """날짜를 월 단위로 변환합니다."""
        if field_name not in df.columns:
            return df
            
        try:
            df[field_name] = pd.to_datetime(df[field_name], errors='coerce')
            df[field_name] = df[field_name].dt.to_period('M')
        except Exception as e:
            print(f"     - ⚠️ Date conversion error for {field_name}: {e}")
            
        return df
    
    def _transform_map_storage_type(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """저장 타입을 Indoor/Outdoor/Transitional로 매핑합니다."""
        if field_name not in df.columns:
            return df
            
        storage_mapping = {
            'indoor': 'Indoor',
            'outdoor': 'Outdoor',
            'transitional': 'Transitional',
            'temp': 'Transitional',
            'temporary': 'Transitional'
        }
        
        df[field_name] = df[field_name].astype(str).str.lower().map(storage_mapping).fillna('Unknown')
        return df
    
    def _transform_join_wh_master(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """웨어하우스 마스터 테이블과 JOIN합니다."""
        # 실제 구현에서는 WH 마스터 테이블을 로드하여 JOIN
        # 현재는 기본 정규화만 수행
        if field_name in df.columns:
            df[field_name] = df[field_name].astype(str).str.strip()
        return df
    
    def _transform_classify_vendor(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """공급사를 분류하고 ID를 생성합니다."""
        if field_name not in df.columns:
            return df
            
        # 공급사 분류 및 ID 생성
        df[field_name] = df[field_name].astype(str).str.strip()
        df[f'{field_name}_id'] = df[field_name].astype('category').cat.codes
        return df
    
    def _transform_calculate_from_dimensions(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """치수로부터 면적/부피를 계산합니다."""
        if field_name == 'sqm' and all(col in df.columns for col in ['length', 'width']):
            df['sqm'] = df['length'] * df['width']
        elif field_name == 'cbm' and all(col in df.columns for col in ['length', 'width', 'height']):
            df['cbm'] = df['length'] * df['width'] * df['height']
            
        return df
    
    def _transform_estimate_from_cbm_if_missing(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """CBM으로부터 GW를 추정합니다."""
        if field_name == 'gw' and 'cbm' in df.columns:
            # 밀도 테이블 (자재별)
            density_table = {
                'STEEL': 7.85,
                'TRANSFORMER_OIL': 0.88,
                'DEFAULT': 1.0
            }
            
            # GW가 결측이고 CBM이 있는 경우 추정
            missing_gw = df['gw'].isna() & df['cbm'].notna()
            if missing_gw.any():
                # 기본 밀도 사용 (실제로는 자재 정보 필요)
                df.loc[missing_gw, 'gw'] = df.loc[missing_gw, 'cbm'] * density_table['DEFAULT']
                
        return df
    
    def _transform_validate_hs_code(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """HS 코드를 검증합니다."""
        if field_name not in df.columns:
            return df
            
        # 6자리 HS 코드 패턴 검증
        hs_pattern = r'^\d{6}$'
        df[f'{field_name}_valid'] = df[field_name].astype(str).str.match(hs_pattern)
        
        return df
    
    def _transform_normalize_incoterm(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """Incoterm을 정규화합니다."""
        if field_name not in df.columns:
            return df
            
        incoterm_mapping = {
            'fob': 'FOB',
            'cif': 'CIF',
            'exw': 'EXW',
            'ddp': 'DDP',
            'dap': 'DAP'
        }
        
        df[field_name] = df[field_name].astype(str).str.lower().map(incoterm_mapping).fillna('Unknown')
        return df
    
    def _transform_classify_package_type(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """패키지 타입을 분류합니다."""
        if field_name not in df.columns:
            return df
            
        package_mapping = {
            'crate': 'Crate',
            'pallet': 'Pallet',
            'box': 'Box',
            'carton': 'Carton',
            'container': 'Container',
            'loose': 'Loose'
        }
        
        df[field_name] = df[field_name].astype(str).str.lower().map(package_mapping).fillna('Unknown')
        return df
    
    def _transform_cm_to_m(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """센티미터를 미터로 변환합니다."""
        if field_name in df.columns:
            df[field_name] = pd.to_numeric(df[field_name], errors='coerce') / 100
        return df
    
    def _transform_clean_text(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """텍스트를 정리합니다."""
        if field_name in df.columns:
            df[field_name] = df[field_name].astype(str).str.strip()
        return df
    
    def _transform_normalize_unit(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        """단위를 정규화합니다."""
        if field_name not in df.columns:
            return df
            
        unit_mapping = {
            'ea': 'EA',
            'each': 'EA',
            'pcs': 'EA',
            'pieces': 'EA',
            'kg': 'KG',
            'kgs': 'KG',
            'm': 'M',
            'meter': 'M',
            'meters': 'M'
        }
        
        df[field_name] = df[field_name].astype(str).str.lower().map(unit_mapping).fillna('Unknown')
        return df
    
    def _validate_shacl_rules(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """SHACL 규칙을 검증합니다."""
        validation_results = {}
        
        for field_name, field_config in self.ontology_map.items():
            if field_name not in df.columns:
                continue
                
            shacl_rule = field_config.get('shacl_rule')
            if not shacl_rule:
                continue
                
            errors = []
            
            if shacl_rule == 'sct:PositiveNumber':
                if field_name in df.columns:
                    negative_values = df[field_name] < 0
                    if negative_values.any():
                        errors.append(f"Negative values found: {negative_values.sum()} rows")
                        
            elif shacl_rule == 'sct:HS6':
                if field_name in df.columns:
                    invalid_hs = ~df[field_name].astype(str).str.match(r'^\d{6}$', na=False)
                    if invalid_hs.any():
                        errors.append(f"Invalid HS codes: {invalid_hs.sum()} rows")
                        
            elif shacl_rule == 'sct:Date':
                if field_name in df.columns:
                    invalid_dates = pd.to_datetime(df[field_name], errors='coerce').isna()
                    if invalid_dates.any():
                        errors.append(f"Invalid dates: {invalid_dates.sum()} rows")
            
            if errors:
                validation_results[field_name] = errors
                
        return validation_results
    
    def _report_validation_results(self, validation_results: Dict[str, List[str]], source_name: str):
        """검증 결과를 보고합니다."""
        if validation_results:
            print(f"     - ⚠️ SHACL validation issues in {source_name}:")
            for field, errors in validation_results.items():
                for error in errors:
                    print(f"       • {field}: {error}")
        else:
            print(f"     - ✅ SHACL validation passed for {source_name}")
