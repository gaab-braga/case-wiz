import pandas as pd

BASE = r'c:\Dashboard_DRE_2025\02_dados_tratados\powerbi_model'

fDRE = pd.read_csv(f'{BASE}/fDRE.csv', sep=';', decimal=',')
fDRE_area = pd.read_csv(f'{BASE}/fDRE_por_Area.csv', sep=';', decimal=',')
fReceita = pd.read_csv(f'{BASE}/fReceita.csv', sep=';', decimal=',')

# Totals
rec_sales = fDRE_area[(fDRE_area['Area'] == 'Sales') & (fDRE_area['Cenario'] == 'Realizado') & (fDRE_area['Linha_DRE'] == 'Receita Bruta')]['Valor'].sum()
rec_service = fDRE_area[(fDRE_area['Area'] == 'Service') & (fDRE_area['Cenario'] == 'Realizado') & (fDRE_area['Linha_DRE'] == 'Receita Bruta')]['Valor'].sum()
rec_fdre = fDRE[(fDRE['Linha_DRE'] == 'Receita Bruta') & (fDRE['Cenario'] == 'Realizado')]['Valor'].sum()
rec_freceita = fReceita[fReceita['Cenario'] == 'Realizado']['Valor'].sum()

print('Receita Sales (fDRE_por_Area):', f'R$ {rec_sales:,.2f}')
print('Receita Service (fDRE_por_Area):', f'R$ {rec_service:,.2f}')
print('Total por Area (Sales+Service):', f'R$ {rec_sales + rec_service:,.2f}')
print('Receita Bruta (fDRE):', f'R$ {rec_fdre:,.2f}')
print('Receita Bruta (fReceita):', f'R$ {rec_freceita:,.2f}')

print('\nDIFERENÇAS:')
print('  fDRE vs fReceita:', f'R$ {rec_fdre - rec_freceita:,.2f}')
print('  fDRE vs Areas total:', f'R$ {rec_fdre - (rec_sales + rec_service):,.2f}')
