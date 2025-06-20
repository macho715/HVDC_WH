# /analyzer/calculator.py
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

    def _process_supplier(self, df: pd.DataFrame, warehouse_cols: list) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        단일 공급사 데이터에 대해 Case 단위 입출고/재고/누적입고를 계산합니다.
        """
        site_cols = self.config['SITE_COLS']
        target_month = self.config['TARGET_MONTH']

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

    def generate_full_stock_list(self):
        """실제 재고(OnHand)를 기준으로 전체 재고 상세 리스트를 생성합니다."""
        if self.onhand_data.empty: return pd.DataFrame()

        # 각 이동 기록 파일에서 Case No.별로 가장 최신 정보만 남겨 마스터 상세 정보 테이블 생성
        master_details_list = []
        for df in self.movement_data.values():
            # 날짜 관련 컬럼들만 선택하여 마지막 이동 날짜 계산
            date_cols = [col for col in df.columns if isinstance(df[col].iloc[0], pd.Timestamp)]
            if date_cols:
                df['last_move_date'] = df[date_cols].max(axis=1)
                master_details_list.append(df)
        
        if not master_details_list: return self.onhand_data # 이동 기록이 없을 경우 OnHand 데이터만 반환

        master_details = pd.concat(master_details_list, ignore_index=True)
        master_details.sort_values('last_move_date', ascending=False, inplace=True)
        master_details.drop_duplicates(subset=['case_no'], keep='first', inplace=True)
        
        # 실제 재고 리스트와 상세 정보를 병합
        full_stock_list = pd.merge(
            self.onhand_data, 
            master_details, 
            on='case_no', 
            how='left',
            suffixes=('_onhand', '_detail')
        )
        return full_stock_list

    def run_stock_verification(self, full_stock_list=None):
        """이동 기록 기반의 계산 재고와 실제 재고를 비교하여 차이를 분석합니다."""
        if self.onhand_data.empty: return pd.DataFrame()
        
        print("   - 이동 기록 기반 재고 계산 중...")
        calculated_stock = self._calculate_stock_from_movements()
        
        actual_stock = self.onhand_data[['case_no', 'quantity']].rename(columns={'quantity': 'ActualQty'})
        
        if calculated_stock.empty:
            print("   - ⚠️ WARNING: 이동 기록 파일에서 재고를 계산할 수 없습니다. 검증을 건너뜁니다.")
            return pd.DataFrame()
            
        verification_df = pd.merge(calculated_stock, actual_stock, on='case_no', how='outer').fillna(0)
        verification_df['Discrepancy'] = verification_df['ActualQty'] - verification_df['CalculatedStock']
        
        discrepancies = verification_df[verification_df['Discrepancy'] != 0].copy()
        print(f"   ✅ 재고 검증 완료. {len(discrepancies)}건의 차이 발견.")
        return discrepancies

    def run_monthly_warehouse_analysis(self):
        """월별 창고별 입고/출고/재고 분석을 수행합니다."""
        print("   - 월별 창고별 입고/출고/재고 분석 중...")
        
        all_monthly_data = []
        
        for supplier, df in self.movement_data.items():
            if df.empty:
                continue
                
            print(f"     - Processing {supplier} data...")
            monthly_df, event_summary = self._process_supplier_file(df, supplier)
            if not monthly_df.empty:
                all_monthly_data.append(monthly_df)
        
        if not all_monthly_data:
            print("   - ⚠️ WARNING: 월별 분석 데이터를 생성할 수 없습니다.")
            return pd.DataFrame()
        
        # 모든 공급사의 월별 데이터를 통합
        consolidated_monthly = pd.concat(all_monthly_data, ignore_index=True)
        consolidated_monthly.sort_values(['Month', 'Supplier'], inplace=True)
        
        print(f"   ✅ 월별 창고별 분석 완료. {len(consolidated_monthly)}개 월별 레코드 생성.")
        return consolidated_monthly

    def generate_overall_supplier_summary(self, monthly_data):
        """공급사별 전체 집계를 생성합니다."""
        if monthly_data.empty:
            return pd.DataFrame()
            
        print("   - 공급사별 전체 집계 생성 중...")
        
        summary_data = []
        for supplier in monthly_data['Supplier'].unique():
            supplier_data = monthly_data[monthly_data['Supplier'] == supplier]
            
            # 웨어하우스 컬럼 찾기
            warehouse_cols = [col for col in supplier_data.columns if any(w in col for w in ['DSV', 'Hauler', 'MOSB', 'AAA', 'MZD', 'HALUER']) and '_In' in col]
            
            row_data = {'Supplier': supplier}
            
            # 총 입고량
            total_in = sum(supplier_data[col].sum() for col in warehouse_cols)
            row_data['Total_Warehouse_In'] = total_in
            
            # 총 출고량
            out_cols = [col.replace('_In', '_Out') for col in warehouse_cols]
            total_out = sum(supplier_data[col].sum() for col in out_cols if col in supplier_data.columns)
            row_data['Total_Warehouse_Out'] = total_out
            
            # 최종 재고량
            stock_cols = [col.replace('_In', '_Stock') for col in warehouse_cols]
            final_stock = sum(supplier_data[col].iloc[-1] for col in stock_cols if col in supplier_data.columns)
            row_data['Final_Warehouse_Stock'] = final_stock
            
            # 사이트 입고량
            site_cols = [col for col in supplier_data.columns if any(s in col for s in ['MIR', 'SHU', 'DAS', 'AGI']) and '_Site_In' in col]
            total_site_in = sum(supplier_data[col].sum() for col in site_cols)
            row_data['Total_Site_In'] = total_site_in
            
            summary_data.append(row_data)
        
        summary_df = pd.DataFrame(summary_data)
        print(f"   ✅ 공급사별 집계 완료. {len(summary_df)}개 공급사.")
        return summary_df

    def generate_warehouse_stock_summary(self, monthly_data):
        """창고별 재고 요약을 생성합니다."""
        if monthly_data.empty:
            return pd.DataFrame()
            
        print("   - 창고별 재고 요약 생성 중...")
        
        summary_data = []
        for supplier in monthly_data['Supplier'].unique():
            supplier_data = monthly_data[monthly_data['Supplier'] == supplier]
            
            # 웨어하우스 컬럼 찾기
            warehouse_cols = [col for col in supplier_data.columns if any(w in col for w in ['DSV', 'Hauler', 'MOSB', 'AAA', 'MZD', 'HALUER']) and '_Stock' in col]
            
            for warehouse_col in warehouse_cols:
                warehouse_name = warehouse_col.replace('_Stock', '')
                
                # 창고 분류
                classification = 'Indoor' if any(indoor in warehouse_name for indoor in ['Indoor', 'Al Markaz', 'AAA', 'DHL']) else 'Outdoor'
                
                # 최신 재고량
                current_stock = supplier_data[warehouse_col].iloc[-1] if not supplier_data.empty else 0
                
                # 총 입고량
                in_col = warehouse_col.replace('_Stock', '_In')
                total_in = supplier_data[in_col].sum() if in_col in supplier_data.columns else 0
                
                # 총 출고량
                out_col = warehouse_col.replace('_Stock', '_Out')
                total_out = supplier_data[out_col].sum() if out_col in supplier_data.columns else 0
                
                summary_data.append({
                    'Supplier': supplier,
                    'Warehouse': warehouse_name,
                    'Classification': classification,
                    'Total_In': total_in,
                    'Total_Out': total_out,
                    'Current_Stock': current_stock
                })
        
        summary_df = pd.DataFrame(summary_data)
        print(f"   ✅ 창고별 요약 완료. {len(summary_df)}개 창고.")
        return summary_df

    def generate_pivoted_monthly_summary(self, monthly_data):
        """분류별·월별 피벗 요약을 생성합니다."""
        if monthly_data.empty:
            return pd.DataFrame()
            
        print("   - 분류별·월별 피벗 요약 생성 중...")
        
        # 웨어하우스 분류 매핑
        warehouse_classification = {}
        for col in monthly_data.columns:
            if any(w in col for w in ['DSV', 'Hauler', 'MOSB', 'AAA', 'MZD', 'HALUER']) and '_Stock' in col:
                warehouse_name = col.replace('_Stock', '')
                if any(indoor in warehouse_name for indoor in ['Indoor', 'Al Markaz', 'AAA', 'DHL']):
                    warehouse_classification[warehouse_name] = 'Indoor'
                else:
                    warehouse_classification[warehouse_name] = 'Outdoor'
        
        pivoted_data = []
        for month in monthly_data['Month'].unique():
            month_data = monthly_data[monthly_data['Month'] == month]
            
            for classification in ['Indoor', 'Outdoor']:
                # 해당 분류의 웨어하우스들
                classified_warehouses = [w for w, c in warehouse_classification.items() if c == classification]
                
                # 각 공급사별 집계
                for supplier in month_data['Supplier'].unique():
                    supplier_month_data = month_data[month_data['Supplier'] == supplier]
                    
                    total_stock = 0
                    for warehouse in classified_warehouses:
                        stock_col = f'{warehouse}_Stock'
                        if stock_col in supplier_month_data.columns:
                            total_stock += supplier_month_data[stock_col].iloc[0]
                    
                    pivoted_data.append({
                        'Month': month,
                        'Classification': classification,
                        'Supplier': supplier,
                        'Total_Stock': total_stock
                    })
        
        pivoted_df = pd.DataFrame(pivoted_data)
        print(f"   ✅ 피벗 요약 완료. {len(pivoted_df)}개 레코드.")
        return pivoted_df

    def run_deadstock_analysis(self, days_threshold=90):
        """90일 이상 장기재고 분석을 수행합니다."""
        if not self.movement_data:
            return pd.DataFrame()
            
        print(f"   - {days_threshold}일 이상 장기재고 분석 중...")
        
        deadstock_data = []
        current_date = pd.Timestamp.now()
        
        for supplier, df in self.movement_data.items():
            if df.empty:
                continue
                
            # 날짜 컬럼 찾기
            date_cols = [col for col in df.columns if any(w in col for w in ['DSV', 'Hauler', 'MOSB', 'AAA', 'MZD', 'HALUER']) and col not in ['case_no', 'quantity', 'supplier_key']]
            
            for idx, row in df.iterrows():
                case_no = row['case_no']
                quantity = row.get('quantity', 1)
                
                # 각 위치별 마지막 이동 날짜 확인
                for date_col in date_cols:
                    if pd.notna(row[date_col]) and isinstance(row[date_col], pd.Timestamp):
                        days_passed = (current_date - row[date_col]).days
                        
                        if days_passed >= days_threshold:
                            deadstock_data.append({
                                'Supplier': supplier,
                                'Case_No': case_no,
                                'Warehouse': date_col,
                                'Last_Arrival_Date': row[date_col],
                                'Days_Passed': days_passed,
                                'Quantity': quantity,
                                'Description': row.get('description', ''),
                                'SQM': row.get('sqm', 0),
                                'CBM': row.get('cbm', 0),
                                'HS_Code': row.get('hs_code', ''),
                                'Incoterm': row.get('incoterm', ''),
                                'OOG_Flag': row.get('oog_flag', False),
                                'Package_Type': row.get('package_type', '')
                            })
        
        if not deadstock_data:
            print(f"   - ⚠️ {days_threshold}일 이상 장기재고가 없습니다.")
            return pd.DataFrame()
        
        deadstock_df = pd.DataFrame(deadstock_data)
        deadstock_df.sort_values('Days_Passed', ascending=False, inplace=True)
        
        print(f"   ✅ 장기재고 분석 완료. {len(deadstock_df)}개 아이템.")
        return deadstock_df

    def generate_hs_code_analysis(self):
        """HS 코드별 분석을 생성합니다."""
        if not self.movement_data:
            return pd.DataFrame()
            
        print("   - HS 코드별 분석 생성 중...")
        
        hs_analysis_data = []
        
        for supplier, df in self.movement_data.items():
            if df.empty or 'hs_code' not in df.columns:
                continue
                
            # HS 코드별 집계
            hs_groups = df.groupby('hs_code').agg({
                'quantity': 'sum',
                'sqm': 'sum',
                'cbm': 'sum',
                'gw': 'sum',
                'case_no': 'count'
            }).reset_index()
            
            hs_groups['Supplier'] = supplier
            hs_analysis_data.append(hs_groups)
        
        if not hs_analysis_data:
            print("   - ⚠️ HS 코드 데이터가 없습니다.")
            return pd.DataFrame()
        
        hs_analysis_df = pd.concat(hs_analysis_data, ignore_index=True)
        hs_analysis_df = hs_analysis_df.sort_values(['hs_code', 'Supplier'])
        
        print(f"   ✅ HS 코드별 분석 완료. {len(hs_analysis_df)}개 HS 코드.")
        return hs_analysis_df

    def generate_incoterm_analysis(self):
        """Incoterm별 분석을 생성합니다."""
        if not self.movement_data:
            return pd.DataFrame()
            
        print("   - Incoterm별 분석 생성 중...")
        
        incoterm_analysis_data = []
        
        for supplier, df in self.movement_data.items():
            if df.empty or 'incoterm' not in df.columns:
                continue
                
            # Incoterm별 집계
            incoterm_groups = df.groupby('incoterm').agg({
                'quantity': 'sum',
                'sqm': 'sum',
                'cbm': 'sum',
                'gw': 'sum',
                'case_no': 'count'
            }).reset_index()
            
            incoterm_groups['Supplier'] = supplier
            incoterm_analysis_data.append(incoterm_groups)
        
        if not incoterm_analysis_data:
            print("   - ⚠️ Incoterm 데이터가 없습니다.")
            return pd.DataFrame()
        
        incoterm_analysis_df = pd.concat(incoterm_analysis_data, ignore_index=True)
        incoterm_analysis_df = incoterm_analysis_df.sort_values(['incoterm', 'Supplier'])
        
        print(f"   ✅ Incoterm별 분석 완료. {len(incoterm_analysis_df)}개 Incoterm.")
        return incoterm_analysis_df

    def generate_oog_analysis(self):
        """초과 규격(OOG) 분석을 생성합니다."""
        if not self.movement_data:
            return pd.DataFrame()
            
        print("   - 초과 규격(OOG) 분석 생성 중...")
        
        oog_analysis_data = []
        
        for supplier, df in self.movement_data.items():
            if df.empty or 'oog_flag' not in df.columns:
                continue
                
            # OOG 플래그별 집계
            oog_groups = df.groupby('oog_flag').agg({
                'quantity': 'sum',
                'sqm': 'sum',
                'cbm': 'sum',
                'gw': 'sum',
                'case_no': 'count'
            }).reset_index()
            
            oog_groups['Supplier'] = supplier
            oog_analysis_data.append(oog_groups)
        
        if not oog_analysis_data:
            print("   - ⚠️ OOG 플래그 데이터가 없습니다.")
            return pd.DataFrame()
        
        oog_analysis_df = pd.concat(oog_analysis_data, ignore_index=True)
        oog_analysis_df = oog_analysis_df.sort_values(['oog_flag', 'Supplier'])
        
        print(f"   ✅ OOG 분석 완료. {len(oog_analysis_df)}개 OOG 그룹.")
        return oog_analysis_df

    def generate_package_type_analysis(self):
        """패키지 타입별 분석을 생성합니다."""
        if not self.movement_data:
            return pd.DataFrame()
            
        print("   - 패키지 타입별 분석 생성 중...")
        
        package_analysis_data = []
        
        for supplier, df in self.movement_data.items():
            if df.empty or 'package_type' not in df.columns:
                continue
                
            # 패키지 타입별 집계
            package_groups = df.groupby('package_type').agg({
                'quantity': 'sum',
                'sqm': 'sum',
                'cbm': 'sum',
                'gw': 'sum',
                'case_no': 'count'
            }).reset_index()
            
            package_groups['Supplier'] = supplier
            package_analysis_data.append(package_groups)
        
        if not package_analysis_data:
            print("   - ⚠️ 패키지 타입 데이터가 없습니다.")
            return pd.DataFrame()
        
        package_analysis_df = pd.concat(package_analysis_data, ignore_index=True)
        package_analysis_df = package_analysis_df.sort_values(['package_type', 'Supplier'])
        
        print(f"   ✅ 패키지 타입별 분석 완료. {len(package_analysis_df)}개 패키지 타입.")
        return package_analysis_df

    def generate_storage_type_analysis(self):
        """저장 타입별 분석을 생성합니다."""
        if not self.movement_data:
            return pd.DataFrame()
            
        print("   - 저장 타입별 분석 생성 중...")
        
        storage_analysis_data = []
        
        for supplier, df in self.movement_data.items():
            if df.empty or 'storage_type' not in df.columns:
                continue
                
            # 저장 타입별 집계
            storage_groups = df.groupby('storage_type').agg({
                'quantity': 'sum',
                'sqm': 'sum',
                'cbm': 'sum',
                'gw': 'sum',
                'case_no': 'count'
            }).reset_index()
            
            storage_groups['Supplier'] = supplier
            storage_analysis_data.append(storage_groups)
        
        if not storage_analysis_data:
            print("   - ⚠️ 저장 타입 데이터가 없습니다.")
            return pd.DataFrame()
        
        storage_analysis_df = pd.concat(storage_analysis_data, ignore_index=True)
        storage_analysis_df = storage_analysis_df.sort_values(['storage_type', 'Supplier'])
        
        print(f"   ✅ 저장 타입별 분석 완료. {len(storage_analysis_df)}개 저장 타입.")
        return storage_analysis_df

    def _process_supplier_file(self, df: pd.DataFrame, supplier: str):
        """
        단일 공급사의 이동 데이터를 월별 요약으로 처리합니다.
        (전월재고 + 당월입고 - 당월출고 = 당월재고) 로직을 따릅니다.
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # 날짜 컬럼 식별
        location_cols = [col for col in df.columns if any(w in str(col) for w in list(self.config['WAREHOUSE_COLS_MAP'].keys()) + self.config['SITE_COLS'])]
        if not location_cols:
            return pd.DataFrame(), pd.DataFrame()

        # 데이터프레임 melt
        id_vars = ['case_no', 'quantity', 'sqm', 'cbm']
        id_vars_present = [v for v in id_vars if v in df.columns]
        
        events = df.melt(id_vars=id_vars_present, value_vars=location_cols, var_name='location', value_name='date')
        events.dropna(subset=['date'], inplace=True)
        if events.empty: return pd.DataFrame(), pd.DataFrame()
        
        events['date'] = pd.to_datetime(events['date'], errors='coerce')
        events.dropna(subset=['date'], inplace=True) # NaT 값 제거
        
        events['month'] = events['date'].dt.to_period('M')

        # 입고/출고 이벤트 분류
        events['in_qty'] = events.apply(lambda row: row['quantity'] if self._is_in_event(row['location']) else 0, axis=1)
        events['out_qty'] = events.apply(lambda row: row['quantity'] if not self._is_in_event(row['location']) else 0, axis=1)

        # 피벗 테이블 생성
        pivot = events.pivot_table(
            index='month',
            columns='location',
            values=['in_qty', 'out_qty'],
            aggfunc='sum',
            fill_value=0
        )
        
        if pivot.empty: return pd.DataFrame(), pd.DataFrame()

        # 컬럼명 정리
        pivot.columns = [f"{loc.replace(' ', '_')}_{val.replace('_qty', '').capitalize()}" for val, loc in pivot.columns]

        # 재고 계산
        stock_cols = {}
        for col in pivot.columns:
            if '_In' in col:
                base_name = col.replace('_In', '')
                out_col = f"{base_name}_Out"
                stock_col = f"{base_name}_Stock"
                
                in_series = pivot.get(col, 0)
                out_series = pivot.get(out_col, 0)
                
                # 전월 재고 + 당월 입고 - 당월 출고
                stock_cols[stock_col] = (in_series - out_series).cumsum()

        stock_df = pd.DataFrame(stock_cols, index=pivot.index)
        
        # In, Out, Stock 병합
        final_df = pd.concat([pivot, stock_df], axis=1)
        final_df['Supplier'] = supplier
        final_df.reset_index(inplace=True)
        final_df.rename(columns={'month': 'Month'}, inplace=True)
        
        # 최종 컬럼 순서 정리
        final_df = self._organize_columns(final_df)

        return final_df, events

    def _is_in_event(self, location_name: str) -> bool:
        """이벤트가 '입고'인지 '출고'인지 판단합니다."""
        # 사이트로 이동하면 '출고'로 간주
        site_patterns = self.config.get('SITE_COLS', [])
        return not any(site in location_name for site in site_patterns)

    def _organize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """데이터프레임 컬럼을 Month, Supplier, In/Out/Stock 순으로 정리합니다."""
        if df.empty: return df
        
        month_supplier = ['Month', 'Supplier']
        
        # 위치별 In/Out/Stock 그룹핑
        locations = sorted(list(set([c.replace('_In', '').replace('_Out', '').replace('_Stock', '') for c in df.columns if '_' in c])))
        
        organized_cols = month_supplier[:]
        for loc in locations:
            for suffix in ['_In', '_Out', '_Stock']:
                col = f"{loc}{suffix}"
                if col in df.columns:
                    organized_cols.append(col)
        
        # 기타 컬럼 추가
        other_cols = [c for c in df.columns if c not in organized_cols]
        organized_cols.extend(other_cols)
        
        return df[organized_cols]

    def _calculate_stock_from_movements(self):
        """(검증용) 이동 기록 파일들로부터 현재고를 계산하는 내부 함수."""
        all_statuses = []
        for supplier, df in self.movement_data.items():
            warehouse_cols = self.config['WAREHOUSE_COLS_MAP'].get(supplier, [])
            all_locs = warehouse_cols + self.config['SITE_COLS']
            
            id_vars = ['case_no', 'quantity']
            value_vars = [c for c in all_locs if c in df.columns]
            if not value_vars: continue

            melted = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='location', value_name='date').dropna(subset=['date'])
            if melted.empty: continue
            
            melted.sort_values(by=['case_no', 'date'], inplace=True)
            last_status = melted.drop_duplicates(subset=['case_no'], keep='last')
            
            in_warehouse = last_status[last_status['location'].isin(warehouse_cols)]
            all_statuses.append(in_warehouse)

        if not all_statuses: return pd.DataFrame(columns=['case_no', 'CalculatedStock'])
        
        calculated_df = pd.concat(all_statuses, ignore_index=True)
        return calculated_df.groupby('case_no')['quantity'].sum().reset_index().rename(columns={'quantity': 'CalculatedStock'})
