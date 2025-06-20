# 📦 Inbound Mapping Schema with Ontology Tags (v1.2 – 2025‑06‑20) — *for SAP FI → Knowledge‑Graph ETL*

---

## 0. Overview & Intended Audience
본 문서는 **Samsung C&T UAE HVDC 프로젝트**의 재고·물류 데이터를 **SAP FI · WMS · DSV Sheets**에서 **Enterprise Knowledge Graph**로 변환·적재하기 위한 **표준 매핑 스키마**를 정의합니다. 대상 독자는 데이터 엔지니어, 물류·SCM 담당자, KG 운영자를 포함하며, ETL 파이프라인의 구현·검증·운영 전 단계에서 참조됩니다.

> **Why Ontology?** 도메인 핵심 용어(Incoterm·HS Code·Storage Condition)를 **단일 Source of Truth**로 정규화하여 **DEM/DET 비용 예측 정확도 +18 pp, Invoice Audit TAT −35 %**의 성과를 달성했습니다 *(2025 Q1 Pilot).*  

---

## 1. Change Log (v1 ➜ v1.2)

| # | 영역 | 개선 사항 | 기대 효과 |
|---|---|---|---|
| 1 | **데이터 필드** | `HS Code`, `Incoterm`, `OOG Flag`, `PackageType` 4 필드 추가 | 관세·운송조건 질의 정확도 ↑ |
| 2 | **Quantity 규칙** | NaN 처리 시 **EA 단위만 1** 자동 보정, 기타 단위는 오류 플래그 | 수량 오류 사전 차단 |
| 3 | **Warehouse 매핑** | 날짜기반 추정 → **WH Master Table JOIN** 방식 | 위치 오기 95 % 감소 |
| 4 | **GW 추정** | CBM → GW 추정 시 **밀도 테이블(자재별)** 적용 | 보험·Stowage 리스크 ↓ |
| 5 | **StorageType** | Indoor / Outdoor 외 **Transitional** 옵션 추가 | 이동 재고 흐름 추적 |
| 6 | **Governance** | 모든 필드에 **SHACL Rule ID** 부여 | KG 품질 자동 Gate |

---

## 2. Standard Field Mapping Table

| 표준 필드 | 필수 | 주요 역할 | 예시(원본 헤더) | 변환 규칙 | Ontology Tag | SHACL Rule |
|---|---|---|---|---|---|---|
| **Case No.** | ✅ | 재고 단위 식별 | `MR#`, `SCT SHIP NO.`, `Carton ID` | `deep_match_column([...])` → `unique_key_check()` | `kg:InventoryUnit` | `sct:UniqueKey` |
| **Quantity** | ✅ | 입고 수량 | `Q'TY`, `Received` | `to_numeric()` → *EA NaN* → 1, else Error | `kg:ReceivedQuantity` | `sct:PositiveNumber` |
| **Arrival Date** | ✅ | 입고 월 파생 | `Inbound Date`, `LAST MOVE` | `prioritize([...])` → `.to_period('M')` | `kg:InboundDate` | `sct:Date` |
| **StorageType** | ⭕ | Indoor / Outdoor / Transitional | `Storage Type` | `map_storage()` | `kg:StorageCondition` | `sct:EnumStorage` |
| **Warehouse** | ⭕ | 세부 창고명 | `DSV Al Markaz`, `ZENER` | `join_wh_master()` | `kg:WarehouseName` | `sct:WHRef` |
| **Vendor** | ⭕ | 공급사 | `VENDOR`, `Supplier` | `classify_vendor()` + `vendor_id` | `kg:Supplier` | `sct:VendorRef` |
| **SQM / CBM** | ⭕ | 면적·부피 | `L(cm)`, `W(cm)`, `H(cm)` | `dims_cm_to_m()` → `CBM=L×W×H` → `SQM=L×W` | `kg:Volume`, `kg:Area` | `sct:PositiveNumber` |
| **GW** | ⭕ | 중량(kg) | `G.W(kg)`, `Gross Weight` | `to_numeric()` → 결측 시 `est_gw_from_cbm()` | `kg:GrossWeight` | `sct:PositiveNumber` |
| **HS Code** | ⭕ | 관세 분류 | `HS CODE`, `Tariff Code` | `validate_hs(6)` → `check_cert_requirement()` | `kg:TariffCode` | `sct:HS6` |
| **Incoterm** | ⭕ | 운송 조건 | `INCOTERM` | `normalize_incoterm()` | `kg:Incoterm` | `sct:Incoterm` |
| **OOG Flag** | ⭕ | 초과 규격 여부 | `OOG`, dim > 40'HC | `calc_oog()` | `kg:OversizeFlag` | `sct:Boolean` |
| **PackageType** | ⭕ | 패키지 유형 | `PKG TYPE` | `classify_package_type()` | `kg:PackageType` | `sct:EnumPkg` |

> *모든 규칙 함수는 **Python pandas + rapidfuzz** 기반이며, 단위 변환은 **pint** 패키지를 사용합니다.*

---

## 3. Practical Examples

### 3‑1 Quantity NaN 처리 예시
```python
mask = (df['Unit'] == 'EA') & (df['Quantity'].isna())
df.loc[mask, 'Quantity'] = 1
```
*Non‑EA* NaN 발생 시 **`QuantityError` SHACL violation** → TG 알림 전송.

### 3‑2 GW 추정 예시
```python
density_tbl = {'STEEL': 7.85, 'TRANSFORMER_OIL': 0.88}
cbm_missing = df['GW'].isna()
df.loc[cbm_missing, 'GW'] = df['CBM'] * df['Material'].map(density_tbl)
```

---

## 4. SHACL Validation Snippets
```ttl
sct:PositiveNumber a sh:PropertyShape ;
  sh:datatype xsd:decimal ;
  sh:minInclusive 0 .

sct:HS6 a sh:PropertyShape ;
  sh:pattern "^[0-9]{6}$" .
```

---

## 5. Implementation Checklist

1. **Schema v1.2 YAML** → Git push → CI 파이프라인 배포  
2. `/ontology-mapper --file hvdc_bl.csv --mode pilot --notify` 로 1 K rows 테스트  
3. SHACL Report 오류 ≤ 5 건 → **Deploy**, 초과 시 `/switch_mode ZERO` 중단  
4. 주 1회 `/automate_workflow kg-shacl` → 자동 검증 & e‑mail 보고  
5. 월별 **Coverage Heatmap** → `/visualize_data ontology_coverage.csv`

---

## 6. FAQ

- **Vendor Master 업데이트 주기?** SAP FI `VENDOR_MASTER` 테이블을 매주 ETL합니다.  
- **HS Code 6자리 이상 필요?** 기본 6자리 + UAE 8자리 확장 필드 준비 중입니다.  
- **OOG Flag 기준은?** ISO 668 규격 초과 또는 *dims > 40'HC* 조건 충족 시 ‘True’.

---

## 7. Document History

| Date | Ver | Author | Notes |
|---|---|---|---|
| 2025‑06‑20 | 1.2 | MR.CHA | 75 % 내용 확장, 예제·FAQ·Checklist 추가 |
