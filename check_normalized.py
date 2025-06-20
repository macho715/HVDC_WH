import pandas as pd
from analyzer.normalizer import DataNormalizer
import config

def check_normalized_columns():
    """정규화된 데이터의 컬럼명을 확인합니다."""
    print("🔍 Checking normalized column names...")
    
    normalizer = DataNormalizer(config.ONTOLOGY_MAP)
    
    for key, conf in config.FILE_CONFIG.items():
        if key == 'STOCK_ONHAND':
            continue  # OnHand는 movement 분석에서 제외
            
        print(f"\n📁 {key}:")
        try:
            df = pd.read_excel(conf['path'], sheet_name=conf.get('sheet_name'), engine='openpyxl')
            normalized_df = normalizer.normalize(df, key)
            
            if normalized_df is not None:
                print(f"   Original columns: {list(df.columns)}")
                print(f"   Normalized columns: {list(normalized_df.columns)}")
                
                # 위치 관련 컬럼 찾기
                location_cols = []
                for col in normalized_df.columns:
                    if any(loc in col for loc in ['DSV', 'Hauler', 'MOSB', 'MIR', 'SHU', 'DAS', 'AGI', 'AAA']):
                        location_cols.append(col)
                
                if location_cols:
                    print(f"   Location columns: {location_cols}")
                else:
                    print("   ⚠️ No location columns found in normalized data")
            else:
                print("   ❌ Normalization failed")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")

if __name__ == '__main__':
    check_normalized_columns() 