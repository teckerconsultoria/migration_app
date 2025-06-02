
import pandas as pd
import numpy as np

# Carregar o arquivo Excel
print("Carregando o arquivo Excel...")
df = pd.read_excel('4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA _ABRIL_disp_24_04_2025.xlsx')

# Converter colunas de valores para numérico para garantir
df['META_VLR'] = pd.to_numeric(df['META_VLR'], errors='coerce')
df['ESTOQUE'] = pd.to_numeric(df['ESTOQUE'], errors='coerce')

# Verificar os valores nulos (se houver)
print("\nVerificando valores nulos:")
print(f"Valores nulos em META_VLR: {df['META_VLR'].isna().sum()}")
print(f"Valores nulos em ESTOQUE: {df['ESTOQUE'].isna().sum()}")

# Excluir as gerências Leste e Santo Amaro conforme solicitado
print("\nExcluindo as gerências 'G.R. LESTE' e 'G.R.SANTO AMARO'...")
df_filtrado = df[~df['GER_REG_NEW'].isin(['G.R. LESTE', 'G.R.SANTO AMARO'])]

print(f"Total de registros originais: {len(df)}")
print(f"Total de registros após filtro: {len(df_filtrado)}")
print(f"Registros removidos: {len(df) - len(df_filtrado)}")

# Análise por Diretoria Regional
print("\n============== ANÁLISE POR DIRETORIA REGIONAL ==============")
dir_analise = df_filtrado.groupby('DIR_REG_NEW').agg({
    'PROCADV_CONTRATO': 'count',
    'ESTOQUE': 'sum',
    'META_VLR': 'sum'
}).reset_index()

# Renomear colunas para melhor compreensão
dir_analise.columns = ['Diretoria Regional', 'Quantidade de Contratos', 'Estoque Total', 'Meta Total']

# Adicionar colunas de média por contrato
dir_analise['Estoque Médio por Contrato'] = dir_analise['Estoque Total'] / dir_analise['Quantidade de Contratos']
dir_analise['Meta Média por Contrato'] = dir_analise['Meta Total'] / dir_analise['Quantidade de Contratos']

# Calcular a relação entre meta e estoque (percentual)
dir_analise['% Meta/Estoque'] = (dir_analise['Meta Total'] / dir_analise['Estoque Total'] * 100).round(2)

# Ordenar por quantidade de contratos (decrescente)
dir_analise = dir_analise.sort_values('Quantidade de Contratos', ascending=False)

# Formatar colunas monetárias
for coluna in ['Estoque Total', 'Meta Total', 'Estoque Médio por Contrato', 'Meta Média por Contrato']:
    dir_analise[coluna] = dir_analise[coluna].round(2)

print(dir_analise.to_string(index=False))

# Análise por Gerência Regional (excluindo as mencionadas)
print("\n============== ANÁLISE POR GERÊNCIA REGIONAL ==============")
ger_analise = df_filtrado.groupby('GER_REG_NEW').agg({
    'PROCADV_CONTRATO': 'count',
    'ESTOQUE': 'sum',
    'META_VLR': 'sum'
}).reset_index()

# Renomear colunas para melhor compreensão
ger_analise.columns = ['Gerência Regional', 'Quantidade de Contratos', 'Estoque Total', 'Meta Total']

# Adicionar colunas de média por contrato
ger_analise['Estoque Médio por Contrato'] = ger_analise['Estoque Total'] / ger_analise['Quantidade de Contratos']
ger_analise['Meta Média por Contrato'] = ger_analise['Meta Total'] / ger_analise['Quantidade de Contratos']

# Calcular a relação entre meta e estoque (percentual)
ger_analise['% Meta/Estoque'] = (ger_analise['Meta Total'] / ger_analise['Estoque Total'] * 100).round(2)

# Ordenar por quantidade de contratos (decrescente)
ger_analise = ger_analise.sort_values('Quantidade de Contratos', ascending=False)

# Formatar colunas monetárias
for coluna in ['Estoque Total', 'Meta Total', 'Estoque Médio por Contrato', 'Meta Média por Contrato']:
    ger_analise[coluna] = ger_analise[coluna].round(2)

print(ger_analise.to_string(index=False))

# Análise cruzada por Diretoria x Gerência
print("\n============== ANÁLISE CRUZADA: DIRETORIA x GERÊNCIA ==============")

# Para cada diretoria, analisar suas gerências
for diretoria in df_filtrado['DIR_REG_NEW'].unique():
    subset = df_filtrado[df_filtrado['DIR_REG_NEW'] == diretoria]
    
    # Pular se tiver poucas gerências (<3)
    if len(subset['GER_REG_NEW'].unique()) < 3:
        continue
    
    print(f"\n=== Diretoria: {diretoria} ===")
    print(f"Total de contratos: {len(subset)}")
    print(f"Estoque total: {subset['ESTOQUE'].sum():.2f}")
    print(f"Meta total: {subset['META_VLR'].sum():.2f}")
    print(f"% Meta/Estoque: {(subset['META_VLR'].sum() / subset['ESTOQUE'].sum() * 100):.2f}%")
    
    # Análise por gerência dentro da diretoria
    ger_dir_analise = subset.groupby('GER_REG_NEW').agg({
        'PROCADV_CONTRATO': 'count',
        'ESTOQUE': 'sum',
        'META_VLR': 'sum'
    }).reset_index()
    
    # Renomear colunas
    ger_dir_analise.columns = ['Gerência Regional', 'Quantidade de Contratos', 'Estoque Total', 'Meta Total']
    
    # Adicionar colunas calculadas
    ger_dir_analise['Estoque Médio por Contrato'] = ger_dir_analise['Estoque Total'] / ger_dir_analise['Quantidade de Contratos']
    ger_dir_analise['Meta Média por Contrato'] = ger_dir_analise['Meta Total'] / ger_dir_analise['Quantidade de Contratos']
    ger_dir_analise['% Meta/Estoque'] = (ger_dir_analise['Meta Total'] / ger_dir_analise['Estoque Total'] * 100).round(2)
    
    # Ordenar por quantidade de contratos
    ger_dir_analise = ger_dir_analise.sort_values('Quantidade de Contratos', ascending=False)
    
    # Formatar valores numéricos
    for coluna in ['Estoque Total', 'Meta Total', 'Estoque Médio por Contrato', 'Meta Média por Contrato']:
        ger_dir_analise[coluna] = ger_dir_analise[coluna].round(2)
    
    print("\nDistribuição por Gerência:")
    print(ger_dir_analise.to_string(index=False))
    
    # Análise de variação dentro da diretoria
    if len(ger_dir_analise) >= 3:
        print("\nVariação entre gerências nesta diretoria:")
        print(f"- Variação em Estoque Médio: {ger_dir_analise['Estoque Médio por Contrato'].max() / ger_dir_analise['Estoque Médio por Contrato'].min():.2f}x")
        print(f"- Variação em Meta Média: {ger_dir_analise['Meta Média por Contrato'].max() / ger_dir_analise['Meta Média por Contrato'].min():.2f}x")
        print(f"- Variação em % Meta/Estoque: {ger_dir_analise['% Meta/Estoque'].max() / ger_dir_analise['% Meta/Estoque'].min():.2f}x")

# Análise de correlação entre as variáveis
print("\n============== ANÁLISE DE CORRELAÇÃO ==============")

# Correlação entre variáveis por gerência
correlacao_ger = ger_analise[['Quantidade de Contratos', 'Estoque Total', 'Meta Total', 
                             'Estoque Médio por Contrato', 'Meta Média por Contrato', '% Meta/Estoque']].corr()

print("\nCorrelação entre variáveis (Pearson):")
print(correlacao_ger.round(3))

# Identificar as gerências com melhor e pior relação meta/estoque
print("\n--- Gerências com MELHOR relação Meta/Estoque ---")
melhores_ger = ger_analise.sort_values('% Meta/Estoque', ascending=False).head(5)
print(melhores_ger[['Gerência Regional', 'Quantidade de Contratos', 'Estoque Total', 'Meta Total', '% Meta/Estoque']].to_string(index=False))

print("\n--- Gerências com PIOR relação Meta/Estoque ---")
piores_ger = ger_analise.sort_values('% Meta/Estoque').head(5)
print(piores_ger[['Gerência Regional', 'Quantidade de Contratos', 'Estoque Total', 'Meta Total', '% Meta/Estoque']].to_string(index=False))

# Análise adicional por porte da conta (Estoque)
print("\n============== ANÁLISE POR PORTE DA CONTA ==============")

# Definir categorias de porte baseadas no estoque
df_filtrado['Porte'] = pd.cut(
    df_filtrado['ESTOQUE'], 
    bins=[0, 50000, 100000, 500000, float('inf')],
    labels=['Pequeno (até 50k)', 'Médio (50k-100k)', 'Grande (100k-500k)', 'Muito Grande (>500k)']
)

# Analisar quantidade e valores por porte
porte_analise = df_filtrado.groupby('Porte').agg({
    'PROCADV_CONTRATO': 'count',
    'ESTOQUE': 'sum',
    'META_VLR': 'sum'
}).reset_index()

# Renomear colunas
porte_analise.columns = ['Porte da Conta', 'Quantidade de Contratos', 'Estoque Total', 'Meta Total']

# Adicionar métricas calculadas
porte_analise['% do Total de Contratos'] = (porte_analise['Quantidade de Contratos'] / porte_analise['Quantidade de Contratos'].sum() * 100).round(2)
porte_analise['% do Estoque Total'] = (porte_analise['Estoque Total'] / porte_analise['Estoque Total'].sum() * 100).round(2)
porte_analise['Estoque Médio'] = (porte_analise['Estoque Total'] / porte_analise['Quantidade de Contratos']).round(2)
porte_analise['% Meta/Estoque'] = (porte_analise['Meta Total'] / porte_analise['Estoque Total'] * 100).round(2)

print(porte_analise.to_string(index=False))

# Análise adicional por tipo de pessoa (PF vs PJ)
print("\n============== ANÁLISE POR TIPO DE PESSOA ==============")

# Analisar quantidade e valores por tipo de pessoa
pessoa_analise = df_filtrado.groupby('TP_PESSOA').agg({
    'PROCADV_CONTRATO': 'count',
    'ESTOQUE': 'sum',
    'META_VLR': 'sum'
}).reset_index()

# Renomear colunas
pessoa_analise.columns = ['Tipo de Pessoa', 'Quantidade de Contratos', 'Estoque Total', 'Meta Total']

# Adicionar métricas calculadas
pessoa_analise['% do Total de Contratos'] = (pessoa_analise['Quantidade de Contratos'] / pessoa_analise['Quantidade de Contratos'].sum() * 100).round(2)
pessoa_analise['% do Estoque Total'] = (pessoa_analise['Estoque Total'] / pessoa_analise['Estoque Total'].sum() * 100).round(2)
pessoa_analise['Estoque Médio'] = (pessoa_analise['Estoque Total'] / pessoa_analise['Quantidade de Contratos']).round(2)
pessoa_analise['Meta Média'] = (pessoa_analise['Meta Total'] / pessoa_analise['Quantidade de Contratos']).round(2)
pessoa_analise['% Meta/Estoque'] = (pessoa_analise['Meta Total'] / pessoa_analise['Estoque Total'] * 100).round(2)

print(pessoa_analise.to_string(index=False))
