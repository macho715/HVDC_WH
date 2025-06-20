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


def main():
    """
    업로드된 엑셀 스타일과 동일한 Case 단위 월별 집계를 생성하는 메인 파이프라인.
    """
    # 1. 정규화 모듈 초기화
    normalizer = DataNormalizer(config.ONTOLOGY_MAP)
    
    # 2. 이동 데이터 로드 및 정규화
    print("🚀 Starting Data Loading and Normalization...")
    movement_data = {}
    for key, conf in config.FILE_CONFIG.items():
        if conf['type'] == 'movement':
            print(f"   - Loading: {key} ({conf['path']})")
            try:
                df = pd.read_excel(conf['path'], sheet_name=conf.get('sheet_name', 'CASE LIST'))
                normalized_df = normalizer.normalize(df, key)
                if normalized_df is not None:
                    movement_data[key] = normalized_df
            except Exception as e:
                print(f"   - ⚠️ ERROR reading {key}: {e}")
                
    # 3. 계산기 모듈 초기화 및 Case 단위 분석 실행
    calculator_config = {
        'WAREHOUSE_COLS_MAP': config.WAREHOUSE_COLS_MAP,
        'SITE_COLS': config.SITE_COLS,
        'TARGET_MONTH': config.TARGET_MONTH
    }
    calculator = AnalysisCalculator(calculator_config)
    calculator.set_data(movement_data)
    
    print("\n📈 Generating Supplier-Based Case Analysis Reports...")
    reports_to_generate = calculator.run_supplier_case_analysis()

    # 4. 생성된 데이터프레임들로 엑셀 리포트 생성
    if reports_to_generate:
        print(f"\n📊 Generating Excel Report with {len(reports_to_generate)} sheets...")
        reporter = ExcelReporter(reports_to_generate)
        reporter.create_report()
    else:
        print("\n- ⚠️ WARNING: No reports to generate.")


if __name__ == '__main__':
    main()
