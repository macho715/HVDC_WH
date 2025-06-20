# ==============================================================================
# 📝 파일: analyzer/calculator.py
# ℹ️ 설명: 공급사별 분석, 통합 창고 현황, 창고-현장 이동 흐름 분석 기능을
#          모두 포함하는 통합 계산기 클래스입니다.
# ==============================================================================
import pandas as pd
from datetime import datetime
import re
from typing import Dict, Tuple

class AnalysisCalculator:
    """
    업로드된 엑셀 스타일(Case 단위, 월별 집계)에 맞춰 데이터 분석을 수행합니다.
    """
    def __init__(self, config: Dict):
        self.config = config
        self.movement_data = {}

    def set_data(self, movement_data: Dict[str, pd.DataFrame]):
        """분석에 사용할 이동 데이터를 설정합니다."""
        self.movement_data = movement_data

    # --- 기능 1: 공급사별 분석 (_창고, _현장 시트) ---
    def run_supplier_case_analysis(self) -> Dict[str, pd.DataFrame]:
        """
        모든 공급사에 대해 Case 단위 월별 분석을 실행하고,
        공급사별 "창고" 및 "현장" 시트 데이터를 생성합니다.
        """
        all_reports = {}
        for supplier, df in self.movement_data.items():
            if df is None or df.empty:
                print(f"   - ⚠️ No data for {supplier}, skipping.")
                continue
            
            print(f"   - Processing: {supplier}")
            warehouse_cols = self.config['WAREHOUSE_COLS_MAP'].get(supplier, [])
            
            warehouse_df, site_df = self._process_supplier(df, warehouse_cols)
            
            if not warehouse_df.empty:
                all_reports[f"{supplier}_창고"] = warehouse_df
            if not site_df.empty:
                all_reports[f"{supplier}_현장"] = site_df
        
        return all_reports

    # --- 기능 2: 통합 창고 현황 ---
    def generate_consolidated_warehouse_status(self, supplier_reports: dict) -> pd.DataFrame:
        """
        각 공급사별 창고 집계 리포트를 통합하여,
        전체 창고의 월별 입고/출고/재고 현황을 생성합니다.
        """
        print("   - Generating Consolidated Warehouse Status...")
        
        warehouse_reports = []
        for sheet_name, df in supplier_reports.items():
            if '_창고' in sheet_name and not df.empty:
                # TOTAL 행 제거 후 추가
                df_clean = df[df['월'] != 'TOTAL'].copy()
                warehouse_reports.append(df_clean)

        if not warehouse_reports:
            print("   - ⚠️ No supplier-specific warehouse reports found for consolidation.")
            return pd.DataFrame()
            
        consolidated_df = pd.concat(warehouse_reports, ignore_index=True)
        final_summary = consolidated_df.groupby('월').sum().reset_index().sort_values('월')

        if not final_summary.empty:
            total_row = {'월': 'TOTAL'}
            for col in final_summary.columns:
                if col == '월': continue
                total_row[col] = final_summary[col].iloc[-1] if '재고' in col else final_summary[col].sum()
            final_summary = pd.concat([final_summary, pd.DataFrame([total_row])], ignore_index=True)

        # 컬럼 순서 재정렬
        all_warehouse_names = sorted(list(set(col.split('_')[0] for col in final_summary.columns if '_' in col)))
        sorted_cols = ['월'] + [f"{wh_name}{suf}" for wh_name in all_warehouse_names for suf in ['_입고', '_출고', '_재고'] if f"{wh_name}{suf}" in final_summary.columns]
        
        print("   ✅ Consolidated Warehouse Status created.")
        return final_summary[sorted_cols]

    # --- 기능 3: 창고-현장 이동 흐름 분석 ---
    def generate_warehouse_to_site_flow(self) -> pd.DataFrame:
        """
        '어떤 창고'에서 '어떤 현장'으로 화물이 이동했는지 추적하여 매트릭스를 생성합니다.
        """
        print("   - Generating Warehouse-to-Site Flow Analysis...")
        all_transitions = []

        for supplier, df in self.movement_data.items():
            if df is None or df.empty: continue
            
            warehouse_cols = self.config['WAREHOUSE_COLS_MAP'].get(supplier, [])
            site_cols = self.config['SITE_COLS']
            
            for _, row in df.iterrows():
                events = []
                for loc in warehouse_cols + site_cols:
                    if loc in df.columns and pd.notna(row[loc]):
                        events.append({'date': row[loc], 'location': loc})
                
                events.sort(key=lambda x: x['date'])
                
                for i in range(1, len(events)):
                    if events[i-1]['location'] in warehouse_cols and events[i]['location'] in site_cols:
                        all_transitions.append({
                            'origin_warehouse': events[i-1]['location'], 
                            'destination_site': events[i]['location']
                        })
        
        if not all_transitions:
            print("   - ⚠️ No warehouse-to-site transitions found.")
            return pd.DataFrame()

        # 피벗 테이블 생성
        transitions_df = pd.DataFrame(all_transitions)
        pivot_df = transitions_df.groupby(['origin_warehouse', 'destination_site']).size().unstack(fill_value=0)
        
        print("   ✅ Warehouse-to-Site Flow sheet created.")
        return pivot_df.reset_index()

    # --- Helper Functions ---
    def _process_supplier(self, df: pd.DataFrame, warehouse_cols: list) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        단일 공급사 데이터에 대해 Case 단위 입출고/재고/누적입고를 계산합니다.
        """
        site_cols = self.config['SITE_COLS']
        target_month = self.config.get('TARGET_MONTH', '2025-06')

        # 날짜 컬럼 변환
        for col in warehouse_cols + site_cols:
            if col not in df.columns:
                df[col] = pd.NaT
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # 월 목록 생성
        all_months = set()
        for col in warehouse_cols + site_cols:
            all_months.update(df[col].dropna().dt.to_period('M'))
        month_strs = sorted([str(m) for m in all_months if str(m) <= target_month])
        if not month_strs: return pd.DataFrame(), pd.DataFrame()

        # Case별 이벤트 및 최종 상태 추적
        case_status, event_map = self._track_case_events(df, warehouse_cols, site_cols)

        # 월별/창고별 데이터 집계
        warehouse_df = self._aggregate_warehouse_data(case_status, event_map, month_strs, warehouse_cols)

        # 월별/현장별 데이터 집계
        site_df = self._aggregate_site_data(case_status, event_map, month_strs, site_cols)

        return warehouse_df, site_df

    def _track_case_events(self, df: pd.DataFrame, warehouse_cols: list, site_cols: list) -> Tuple[list, list]:
        """Case별 이동 이벤트를 추적합니다."""
        case_status, event_map = [], []
        for _, row in df.iterrows():
            case = row['case_no']
            events = []
            for w in warehouse_cols:
                if pd.notna(row[w]): events.append((row[w], w, 'warehouse_in'))
            for s in site_cols:
                if pd.notna(row[s]): events.append((row[s], s, 'site_out'))
            
            if not events: continue
            events.sort(key=lambda x: x[0])

            prev_loc, prev_type = None, None
            for date, loc, typ in events:
                mon = str(date.to_period('M'))
                if typ == 'warehouse_in':
                    event_map.append({'case': case, 'type': '입고', 'loc': loc, 'month': mon})
                    prev_loc, prev_type = loc, 'warehouse'
                elif typ == 'site_out':
                    if prev_type == 'warehouse' and prev_loc:
                        event_map.append({'case': case, 'type': '출고', 'loc': prev_loc, 'month': mon})
                    event_map.append({'case': case, 'type': 'site_in', 'loc': loc, 'month': mon})
                    prev_loc, prev_type = loc, 'site'
            
            last_date, last_loc, last_type = events[-1]
            case_status.append({'case': case, 'loc': last_loc, 'type': last_type.split('_')[0], 'month': str(last_date.to_period('M'))})
        
        return case_status, event_map

    def _aggregate_warehouse_data(self, case_status: list, event_map: list, months: list, warehouses: list) -> pd.DataFrame:
        """월별/창고별 입고, 출고, 재고를 집계합니다."""
        data = []
        for m in months:
            row_data = {'월': m}
            for w in warehouses:
                row_data[f'{w}_입고'] = sum(1 for e in event_map if e['type'] == '입고' and e['loc'] == w and e['month'] == m)
                row_data[f'{w}_출고'] = sum(1 for e in event_map if e['type'] == '출고' and e['loc'] == w and e['month'] == m)
                row_data[f'{w}_재고'] = sum(1 for s in case_status if s['loc'] == w and s['type'] == 'warehouse' and s['month'] <= m)
            data.append(row_data)
        
        if not data: return pd.DataFrame()
        df = pd.DataFrame(data)
        return self._add_total_row(df)

    def _aggregate_site_data(self, case_status: list, event_map: list, months: list, sites: list) -> pd.DataFrame:
        """월별/현장별 입고, 누적입고를 집계합니다."""
        data = []
        for m in months:
            row_data = {'월': m}
            for s in sites:
                row_data[f'{s}_입고'] = sum(1 for e in event_map if e['type'] == 'site_in' and e['loc'] == s and e['month'] == m)
                row_data[f'{s}_누적입고'] = sum(1 for st in case_status if st['loc'] == s and st['type'] == 'site' and st['month'] <= m)
            data.append(row_data)

        if not data: return pd.DataFrame()
        df = pd.DataFrame(data)
        return self._add_total_row(df)

    def _add_total_row(self, df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame에 TOTAL 행을 추가합니다."""
        if df.empty: return df
        total_row = {'월': 'TOTAL'}
        for col in df.columns:
            if col == '월': continue
            if '재고' in col or '누적입고' in col:
                total_row[col] = df[col].iloc[-1] if not df.empty else 0
            else:
                total_row[col] = df[col].sum()
        return pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
