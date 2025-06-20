# ==============================================================================
# 📝 파일: main.py
# ℹ️ 설명: 위에서 정의된 모든 분석 함수를 순서대로 호출하는 메인 파이프라인입니다.
# ==============================================================================
import pandas as pd
from analyzer.normalizer import DataNormalizer
from analyzer.calculator import AnalysisCalculator
from analyzer.reporter import ExcelReporter
import config

def main():
    # 1. 초기화 및 데이터 로드
    normalizer = DataNormalizer(config.ONTOLOGY_MAP)
    print("🚀 Starting Data Loading and Normalization...")
    movement_data = {}
    for key, conf in config.FILE_CONFIG.items():
        if conf['type'] == 'movement':
            try:
                df = pd.read_excel(conf['path'], sheet_name=conf.get('sheet_name'), engine=conf.get('engine'))
                normalized_df = normalizer.normalize(df, key)
                if normalized_df is not None:
                    movement_data[key] = normalized_df
            except Exception as e:
                print(f"   - ⚠️ ERROR reading {key}: {e}")

    # 2. 계산기 설정 및 분석 실행
    calculator_config = {
        'WAREHOUSE_COLS_MAP': config.WAREHOUSE_COLS_MAP,
        'SITE_COLS': config.SITE_COLS,
        'TARGET_MONTH': config.TARGET_MONTH
    }
    calculator = AnalysisCalculator(calculator_config)
    calculator.set_data(movement_data)
    
    print("\n📈 Generating All Analysis Reports...")
    
    # 분석 1: 공급사별 분석
    case_reports = calculator.run_supplier_case_analysis()
    
    # 분석 2 & 3: 요청하신 통합 시트들 생성
    consolidated_status = calculator.generate_consolidated_warehouse_status(case_reports)
    warehouse_to_site_flow = calculator.generate_warehouse_to_site_flow()
    
    # 3. 최종 리포트 딕셔너리 구성
    final_reports = {
        'Consolidated_WH_Status': consolidated_status,
        'Warehouse_to_Site_Flow': warehouse_to_site_flow,
    }
    final_reports.update(case_reports)

    # 4. 엑셀 리포트 생성
    if any(not df.empty for df in final_reports.values()):
        print(f"\n📊 Generating Excel Report with {len(final_reports)} sheets...")
        reporter = ExcelReporter(final_reports)
        reporter.create_report()
    else:
        print("\n- ⚠️ WARNING: No reports were generated.")

if __name__ == '__main__':
    main()
