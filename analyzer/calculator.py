# ==============================================================================
# 📝 파일: analyzer/calculator.py
# ℹ️ 설명: MOSB를 주요 리포트에서 제외하는 로직이 적용된 통합 계산기 클래스.
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
                continue
            print(f"   - Processing for supplier-specific sheets: {supplier}")
            warehouse_cols = self.config['WAREHOUSE_COLS_MAP'].get(supplier, [])
            warehouse_df, site_df = self._process_supplier(df, warehouse_cols)
            if not warehouse_df.empty:
                all_reports[f"{supplier}_창고"] = warehouse_df
            if not site_df.empty:
                all_reports[f"{supplier}_현장"] = site_df
        return all_reports

    # --- 기능 2: 통합 창고 현황 (MOSB 제외) ---
    def generate_consolidated_warehouse_status(self, supplier_reports: dict) -> pd.DataFrame:
        """
        각 공급사별 창고 집계 리포트를 통합하여,
        전체 창고의 월별 입고/출고/재고 현황을 생성합니다.
        (MOSB는 집계에서 제외)
        """
        print("   - Generating Consolidated Warehouse Status (excluding MOSB)...")
        warehouse_dfs = [report for name, report in supplier_reports.items() if name.endswith('_창고') and not report.empty]
        if not warehouse_dfs:
            return pd.DataFrame()
            
        consolidated = pd.concat([df[df['월'] != 'TOTAL'] for df in warehouse_dfs], ignore_index=True)
        summary = consolidated.groupby('월').sum().reset_index().sort_values('월')

        if summary.empty:
            return pd.DataFrame()

        total_row = {'월': 'TOTAL'}
        for col in summary.columns[1:]:
            total_row[col] = summary[col].iloc[-1] if '재고' in col else summary[col].sum()
        summary = pd.concat([summary, pd.DataFrame([total_row])], ignore_index=True)
        
        # 컬럼 순서 재정렬 및 MOSB 제외
        all_wh_names = sorted(list(set(c.split('_')[0] for c in summary.columns if '_' in c)))
        
        # ✨ MOSB를 최종 집계에서 제외하는 로직
        if 'MOSB' in all_wh_names:
            all_wh_names.remove('MOSB')
            
        sorted_cols = ['월'] + [f"{wh}{suf}" for wh in all_wh_names for suf in ['_입고', '_출고', '_재고'] if f"{wh}{suf}" in summary.columns]
        
        print("   ✅ Consolidated Warehouse Status created.")
        return summary[sorted_cols]

    # --- 기능 3: 창고-현장 이동 흐름 분석 (MOSB 제외) ---
    def generate_warehouse_to_site_flow(self) -> pd.DataFrame:
        """
        '어떤 창고'에서 '어떤 현장'으로 화물이 이동했는지 추적하여
        입고/출고/재고 형식의 매트릭스를 생성합니다.
        (MOSB는 출발지에서 제외)
        """
        print("   - Generating Warehouse-to-Site Flow Analysis (excluding MOSB as origin)...")
        all_transitions = []
        for supplier, df in self.movement_data.items():
            if df is None or df.empty: continue
            
            warehouse_cols = self.config['WAREHOUSE_COLS_MAP'].get(supplier, [])
            site_cols = self.config['SITE_COLS']
            
            for _, row in df.iterrows():
                events = [{'date': row[loc], 'location': loc} for loc in warehouse_cols + site_cols if loc in df.columns and pd.notna(row[loc])]
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

        pivot_df = pd.DataFrame(all_transitions).groupby(['origin_warehouse', 'destination_site']).size().unstack(fill_value=0)
        
        # ✨ MOSB를 출발지(index)에서 제외하는 로직
        if 'MOSB' in pivot_df.index:
            pivot_df = pivot_df.drop('MOSB')
            
        sites = self.config.get('SITE_COLS', [])
        final_cols = pd.MultiIndex.from_product([sites, ['입고', '출고', '재고']], names=['SITE', '구분'])
        result_df = pd.DataFrame(index=pivot_df.index, columns=final_cols).fillna(0).astype(int)

        for site in sites:
            if site in pivot_df.columns:
                result_df[(site, '입고')] = pivot_df[site]
        
        result_df.index.name = '구분'
        print("   ✅ Warehouse-to-Site Flow sheet created.")
        return result_df.reset_index()

    # --- Helper Functions ---
    def _process_supplier(self, df: pd.DataFrame, warehouse_cols: list) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        단일 공급사 데이터에 대해 Case 단위 입출고/재고/누적입고를 계산합니다.
        """
        site_cols = self.config['SITE_COLS']
        target_month = self.config.get('TARGET_MONTH', pd.Timestamp.now().strftime('%Y-%m'))

        # 날짜 컬럼 변환
        for col in warehouse_cols + site_cols:
            if col not in df.columns:
                df[col] = pd.NaT
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # 월 목록 생성
        all_months = set(pd.to_datetime(df[warehouse_cols + site_cols].stack(), errors='coerce').dropna().dt.to_period('M'))
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
