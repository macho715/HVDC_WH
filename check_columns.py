import pandas as pd
import config

def check_columns():
    """데이터 파일들의 실제 컬럼명을 확인합니다."""
    print("🔍 Checking actual column names in data files...")
    
    for key, conf in config.FILE_CONFIG.items():
        print(f"\n📁 {key}:")
        try:
            df = pd.read_excel(conf['path'], sheet_name=conf.get('sheet_name'), engine='openpyxl')
            print(f"   Path: {conf['path']}")
            print(f"   Shape: {df.shape}")
            print(f"   Columns: {list(df.columns)}")
            
            # 날짜 컬럼 찾기
            date_cols = []
            for col in df.columns:
                if df[col].dtype == 'datetime64[ns]' or (df[col].dtype == 'object' and df[col].str.contains(r'\d{4}-\d{2}-\d{2}', na=False).any()):
                    date_cols.append(col)
            
            if date_cols:
                print(f"   Date columns: {date_cols}")
            else:
                print("   ⚠️ No date columns found")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")

if __name__ == '__main__':
    check_columns() 