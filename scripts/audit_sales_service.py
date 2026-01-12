import pandas as pd
from pathlib import Path

BASE = r'c:\Dashboard_DRE_2025\02_dados_tratados\powerbi_model'
EXCEL = r'c:\Dashboard_DRE_2025\01_dados_originais\dados_case_pbi.xlsx'

# Read CSVs with correct parsing
fDRE_area = pd.read_csv(f'{BASE}/fDRE_por_Area.csv', sep=';', decimal=',')
fDRE = pd.read_csv(f'{BASE}/fDRE.csv', sep=';', decimal=',')
try:
    fDRE_extra = pd.read_csv(f'{BASE}/fDRE_extraido_direto.csv', sep=';', decimal=',')
except Exception:
    fDRE_extra = None

# Filter Realizado
area_real = fDRE_area[fDRE_area['Cenario'] == 'Realizado']
dre_real = fDRE[fDRE['Cenario'] == 'Realizado']

# Normalize Linha_DRE (canonical names) and Group by canonical names x Area
import re

def normalize_name(s):
    s = '' if pd.isna(s) else str(s)
    # remove leading symbols like '(=) ', '(-) ', '+' etc. and extra whitespace
    s = re.sub(r'^\W+\s*', '', s)
    s = s.strip()
    return s

area_real['Linha_DRE_raw'] = area_real['Linha_DRE']
area_real['Linha_DRE'] = area_real['Linha_DRE'].astype(str).apply(normalize_name)
area_real['Area'] = area_real['Area'].astype(str).str.strip()

dre_real['Linha_DRE'] = dre_real['Linha_DRE'].astype(str).apply(normalize_name)

# Re-group using canonical names
pivot = area_real.groupby(['Linha_DRE', 'Area'])['Valor'].sum().unstack(fill_value=0)
# Add Total column
pivot['Total'] = pivot.sum(axis=1)

print('\n📋 Breakdown: fDRE_por_Area (Realizado) - Linha_DRE x Area')
print('-' * 80)
print('   Available canonical Linha_DRE in fDRE_por_Area:', list(pivot.index))

# Safe print for lines of interest
lines_of_interest = ['Receita Bruta', 'EBITDA', 'Lucro Líquido']
lines_present = [l for l in lines_of_interest if l in pivot.index]
if lines_present:
    print(pivot.loc[lines_present].to_string(float_format='R$ {0:,.2f}'.format))
else:
    print('   None of the target lines were found in fDRE_por_Area.')

# Show any lines that look like EBITDA / Lucro to diagnose duplication
lines_ebitda = [i for i in pivot.index if 'EBITDA' in i.upper()]
lines_lucro = [i for i in pivot.index if 'LUCRO' in i.upper()]
if lines_ebitda:
    print('\n🔬 Lines matching "EBITDA":', lines_ebitda)
    print(pivot.loc[lines_ebitda].to_string(float_format='R$ {0:,.2f}'.format))

if lines_lucro:
    print('\n🔬 Lines matching "Lucro":', lines_lucro)
    print(pivot.loc[lines_lucro].to_string(float_format='R$ {0:,.2f}'.format))

# Also show raw rows from fDRE_area that contain EBITDA or Lucro for detail
print('\n🔎 Raw rows in fDRE_por_Area where Linha_DRE contains EBITDA or Lucro (sample):')
print(area_real[area_real['Linha_DRE'].str.contains('EBITDA|LUCRO', case=False)][['Linha_DRE','Area','Mes_Num','Valor']].head(20).to_string(index=False))

# Map Linha_DRE to dLinhaDRE to get categories and compute per-area calculated EBITDA (Receita Líquida + sum(Custos) + PLR)
dLinha = pd.read_csv(f'{BASE}/dLinhaDRE.csv', sep=';')
dLinha['Linha_DRE'] = dLinha['Linha_DRE'].astype(str).apply(lambda s: re.sub(r'^\W+\s*','',s).strip())

merged = area_real.merge(dLinha[['Linha_DRE','Categoria']], left_on='Linha_DRE', right_on='Linha_DRE', how='left')

# Sum categories per area
cat_area = merged.groupby(['Categoria','Area'])['Valor'].sum().unstack(fill_value=0)
print('\n📊 Sum by Categoria x Area (sample rows):')
print(cat_area.head(10).to_string(float_format='R$ {0:,.2f}'.format))

# Compute calculated EBITDA per area
rec_liq_sales = pivot.loc['Receita Líquida','Sales'] if 'Receita Líquida' in pivot.index else 0
rec_liq_service = pivot.loc['Receita Líquida','Service'] if 'Receita Líquida' in pivot.index else 0
custos_sales = cat_area.loc['Custo','Sales'] if 'Custo' in cat_area.index else 0
custos_service = cat_area.loc['Custo','Service'] if 'Custo' in cat_area.index else 0
plr_sales = pivot.loc['Plr','Sales'] if 'Plr' in pivot.index else 0
plr_service = pivot.loc['Plr','Service'] if 'Plr' in pivot.index else 0

calc_ebitda_sales = rec_liq_sales + custos_sales + plr_sales
calc_ebitda_service = rec_liq_service + custos_service + plr_service
print('\n🧾 Calculated EBITDA by area (using Categoria mapping):')
print(f"  Sales calc EBITDA = R$ {calc_ebitda_sales:,.2f} | reported EBITDA = R$ {pivot.loc['EBITDA','Sales']:,.2f}")
print(f"  Service calc EBITDA = R$ {calc_ebitda_service:,.2f} | reported EBITDA = R$ {pivot.loc['EBITDA','Service']:,.2f}")
print(f"  Sum calc = R$ {calc_ebitda_sales + calc_ebitda_service:,.2f} | Sum reported = R$ {pivot.loc['EBITDA','Total']:,.2f}")

# Totals from fDRE
tot_fdre = dre_real.groupby('Linha_DRE')['Valor'].sum()
print('\n📌 Totals (fDRE - Realizado):')
for k in ['Receita Bruta','EBITDA','Lucro Líquido']:
    print(f"   {k}: R$ {tot_fdre.get(k,0):,.2f}")

# Totals from fDRE_extraido_direto (if exists)
if fDRE_extra is not None:
    extra_tot = fDRE_extra.groupby('Linha_DRE')['Valor'].sum()
    print('\n📌 Totals (fDRE_extraido_direto - Realizado):')
    for k in ['Receita Bruta','EBITDA','Lucro Líquido']:
        print(f"   {k}: R$ {extra_tot.get(k,0):,.2f}")

# Totals from original Excel (Modelo DRE) - try to extract total column (col 13 index)
print('\n📥 Extracting totals from Excel sheet: Modelo DRE')
if Path(EXCEL).exists():
    df_raw = pd.read_excel(EXCEL, sheet_name='Modelo DRE', header=None)
    def get_excel_total(line_name):
        # find row index where cell contains the line_name
        mask = df_raw.iloc[:,0].astype(str).str.contains(line_name, na=False, case=False)
        if mask.any():
            idx = mask.idxmax()
            # column 13 (index 13) is the annual total per extracao_robusta_dre assumption
            # but some files may have totals in different columns - try last column if 13 missing
            col_idx = 13 if df_raw.shape[1] > 13 else df_raw.shape[1]-1
            val = df_raw.iloc[idx, col_idx]
            return float(val) if pd.notna(val) else 0.0
        return None

    for k in ['Receita Bruta','EBITDA','Lucro Líquido']:
        v = get_excel_total(k)
        print(f"   {k}: R$ {v:,.2f}")
        # If EBITDA, show monthly values and verify total column
        if k == 'EBITDA' and v is not None:
            mask = df_raw.iloc[:,0].astype(str).str.contains('EBITDA', na=False, case=False)
            if mask.any():
                idx = mask.idxmax()
                months = df_raw.iloc[idx, 1:13].fillna(0).astype(float)
                months_sum = months.sum()
                total_col = df_raw.iloc[idx, 13] if df_raw.shape[1] > 13 else None
                print(f"      Excel EBITDA - sum(months 1-12)=R$ {months_sum:,.2f} | total_col(R13)=R$ {total_col:,.2f} | diff=R$ {total_col-months_sum:,.2f}")

else:
    print('   Excel source not found at expected path.')

# Compare computed area totals to fDRE totals
print('\n🔎 Compare: fDRE_por_Area (Sales+Service) vs fDRE totals (Realizado)')
for k in ['Receita Bruta','EBITDA','Lucro Líquido']:
    area_sum = pivot.loc[k]['Total'] if k in pivot.index else 0
    fdre_sum = tot_fdre.get(k, 0)
    diff = fdre_sum - area_sum
    pct = (diff / fdre_sum*100) if fdre_sum!=0 else 0
    status = 'OK' if abs(diff) < 1 else 'MISMATCH'
    print(f"  {k}: AreaTotal=R$ {area_sum:,.2f} | fDRE=R$ {fdre_sum:,.2f} | Diff=R$ {diff:,.2f} ({pct:.2f}%) -> {status}")

# If user-provided numbers (from screenshot) are known, compare them
screenshot = {
    'Receita Bruta': {'Sales':15382065.76,'Service':51129834.19},
    'EBITDA': {'Sales':7474197.96,'Service':24844159.97},
    'Lucro Líquido': {'Sales':7907867.80,'Service':26285674.22}
}
print('\n🧾 Compare with screenshot numbers (if provided):')
for line in ['Receita Bruta','EBITDA','Lucro Líquido']:
    if line in pivot.index:
        for area in ['Sales','Service']:
            csv_val = pivot.loc[line].get(area,0)
            ss_val = screenshot[line][area]
            diff = csv_val - ss_val
            print(f"  {line} - {area}: CSV=R$ {csv_val:,.2f} | Screenshot=R$ {ss_val:,.2f} | Diff=R$ {diff:,.2f}")

print('\n✅ Audit complete.')
