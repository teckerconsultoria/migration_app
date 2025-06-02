import pandas as pd
import re

# Carregar a planilha terciária
tertiary_df = pd.read_excel('uploads/tertiary.xlsx')
print(f'Total de registros na planilha terciária: {len(tertiary_df)}')
print(f'Colunas disponíveis: {tertiary_df.columns.tolist()}')

# Analisar a coluna GCPJ
gcpj_values = tertiary_df['gcpj'].dropna().head(20).tolist()
print('\nExemplos de valores GCPJ:')
for v in gcpj_values:
    print(f'  {v}')

# Função para extrair o GCPJ corretamente
def extract_gcpj(value):
    if pd.isna(value):
        return None
    
    value_str = str(value).strip()
    
    # Caso 1: Se for um número sem prefixo
    if value_str.isdigit():
        return value_str
    
    # Caso 2: Se tiver o prefixo "GCPJ"
    match = re.search(r'GCPJ\s*(\d+)', value_str)
    if match:
        return match.group(1)
    
    return None

# Aplicar extração
tertiary_df['gcpj_clean'] = tertiary_df['gcpj'].apply(extract_gcpj)

# Mostrar exemplos
sample_data = pd.DataFrame({
    'original': gcpj_values,
    'clean': [extract_gcpj(v) for v in gcpj_values]
})
print('\nExemplos de extração de GCPJ:')
print(sample_data.to_string())

# Contar valores válidos
valid_gcpj_count = tertiary_df['gcpj_clean'].notna().sum()
print(f'\nTotal de GCPJs válidos extraídos: {valid_gcpj_count}')

# Verificar valores únicos de CPF/CNPJ
cpf_values = tertiary_df['cpf.cnpj'].dropna().head(10).tolist()
print('\nExemplos de valores CPF/CNPJ:')
for v in cpf_values:
    print(f'  {v}')

# Contagem de valores CPF/CNPJ não nulos
valid_cpf_count = tertiary_df['cpf.cnpj'].notna().sum()
print(f'\nTotal de CPF/CNPJ válidos: {valid_cpf_count}')

# Registros com ambos GCPJ e CPF/CNPJ
both_valid = tertiary_df.dropna(subset=['gcpj_clean', 'cpf.cnpj'])
print(f'\nRegistros com GCPJ e CPF/CNPJ preenchidos: {len(both_valid)}')

# Exemplos completos
print('\nExemplos de registros completos:')
if len(both_valid) > 0:
    print(both_valid[['gcpj', 'gcpj_clean', 'cpf.cnpj']].head(5).to_string())
else:
    print('Nenhum registro encontrado com ambos os valores preenchidos.')

# Verificar duplicatas na coluna GCPJ limpa
duplicates = tertiary_df['gcpj_clean'].dropna().duplicated()
duplicate_count = duplicates.sum()
if duplicate_count > 0:
    print(f'\nNúmero de GCPJs duplicados: {duplicate_count}')
    # Mostrar exemplos de duplicatas
    duplicate_gcpjs = tertiary_df[tertiary_df['gcpj_clean'].duplicated(keep=False)].sort_values('gcpj_clean')
    print('\nExemplos de GCPJs duplicados:')
    print(duplicate_gcpjs[['gcpj', 'gcpj_clean', 'cpf.cnpj']].head(10).to_string())
else:
    print('\nNão há GCPJs duplicados.')

# Comparar com a planilha primária
primary_df = pd.read_excel('uploads/primary.xlsx')
print(f'\nTotal de registros na planilha primária: {len(primary_df)}')

# Verificar se a coluna GCPJ existe na planilha primária
if 'GCPJ' in primary_df.columns:
    # Contar quantos GCPJs da planilha primária estão presentes na terciária
    primary_gcpjs = set(primary_df['GCPJ'].dropna().astype(str))
    tertiary_gcpjs = set(tertiary_df['gcpj_clean'].dropna())
    
    intersection = primary_gcpjs.intersection(tertiary_gcpjs)
    print(f'\nGCPJs em comum entre planilha primária e terciária: {len(intersection)}')
    print(f'Percentual de correspondência: {len(intersection)/len(primary_gcpjs)*100:.2f}%')
    
    # Exemplos de valores em comum
    if len(intersection) > 0:
        print('\nExemplos de GCPJs em comum:')
        for gcpj in list(intersection)[:5]:
            try:
                primary_row = primary_df[primary_df['GCPJ'].astype(str) == gcpj].iloc[0]
                tertiary_row = tertiary_df[tertiary_df['gcpj_clean'] == gcpj].iloc[0]
                print(f'  GCPJ: {gcpj}')
                if 'CPF' in primary_df.columns:
                    print(f'  CPF no primário: {primary_row["CPF"]}')
                print(f'  CPF/CNPJ no terciário: {tertiary_row["cpf.cnpj"]}')
                print()
            except:
                continue
else:
    print('\nA coluna GCPJ não foi encontrada na planilha primária.')
