# /main.py

# analyzer 패키지에서 각 클래스를 가져옴
from analyzer.normalizer import DataNormalizer
from analyzer.calculator import AnalysisCalculator
from analyzer.reporter import ExcelReporter
# 설정 파일에서 모든 설정을 가져옴
import config

def main():
    """
    웨어하우스 분석을 실행하는 메인 파이프라인.
    이 스크립트는 로딩, 분석, 리포팅 단계를 조율합니다.
    """
    # 1. 설정과 온톨로지를 사용하여 분석기 초기화
    analyzer = WarehouseAnalyzer(
        file_config=config.FILE_CONFIG,
        ontology_map=config.ONTOLOGY_MAP
    )
    
    # 2. 모든 데이터 파일을 로드하고 표준화
    analyzer.load_data()

    # 3. 모든 분석 실행
    # 이 딕셔너리에 최종 보고서에 포함될 모든 데이터프레임이 저장됩니다.
    reports_to_generate = {}
    
    # OnHand 리포트를 기준으로 핵심 분석 수행
    if not analyzer.onhand_data.empty:
        # 가장 정확한 최신 재고 리스트 생성
        full_stock_list = analyzer.generate_full_stock_list()
        reports_to_generate['Full_Stock_List'] = full_stock_list
        
        # 재고 검증 리포트 생성
        verification_report = analyzer.run_stock_verification(full_stock_list)
        reports_to_generate['Stock_Verification'] = verification_report
        
        # 향후 다른 분석 기능들을 이곳에 추가할 수 있습니다.
        # 예: deadstock_report = analyzer.run_deadstock_analysis(full_stock_list)
        # reports_to_generate['DeadStock'] = deadstock_report

    else:
        print("\nOnHand 데이터가 없으므로, OnHand 기반 리포트는 건너뜁니다.")


    # 4. 생성된 데이터프레임들로 엑셀 리포트 생성
    reporter = ExcelReporter(reports_to_generate)
    reporter.create_report()

if __name__ == '__main__':
    main()
