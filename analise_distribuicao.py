
import openpyxl
import pandas as pd
from collections import Counter

# Carregar o arquivo Excel usando pandas (mais eficiente para análise estatística)
print("Carregando o arquivo Excel...")
df = pd.read_excel('4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA _ABRIL_disp_24_04_2025.xlsx')

# Campos para análise
campos = ['CLASSIFICACAO', 'TIPO', 'Desc_Modalidade', 'TP_PESSOA', 'GER_REG_NEW', 'DIR_REG_NEW']

# Analisar a distribuição de cada campo
print("\n============ ANÁLISE DE DISTRIBUIÇÃO DOS CAMPOS ============")

for campo in campos:
    print(f"\n--- Distribuição por {campo} ---")
    
    # Contar a frequência de cada valor único
    contagem = df[campo].value_counts()
    
    # Calcular a porcentagem
    porcentagem = df[campo].value_counts(normalize=True) * 100
    
    # Combinar contagem e porcentagem
    resultado = pd.DataFrame({
        'Contagem': contagem,
        'Porcentagem (%)': porcentagem
    })
    
    # Ordenar por contagem (decrescente)
    resultado = resultado.sort_values('Contagem', ascending=False)
    
    # Exibir resultados
    print(resultado)
    
    # Exibir estatísticas adicionais
    total_valores_unicos = len(contagem)
    print(f"\nTotal de valores únicos em {campo}: {total_valores_unicos}")
    
    # Se tiver muitos valores únicos, mostrar apenas os 10 principais
    if total_valores_unicos > 10:
        print("\nOs 10 valores mais frequentes:")
        print(resultado.head(10))
    
    # Encontrar o valor mais frequente
    valor_mais_frequente = contagem.idxmax()
    frequencia_maior = contagem.max()
    porcentagem_maior = porcentagem.max()
    print(f"\nValor mais frequente: {valor_mais_frequente} ({frequencia_maior} ocorrências, {porcentagem_maior:.2f}%)")
    
    # Valores menos frequentes (menor que 1% do total)
    valores_raros = porcentagem[porcentagem < 1]
    print(f"Quantidade de valores raros (<1%): {len(valores_raros)}")
    if len(valores_raros) > 0 and len(valores_raros) <= 5:
        print("Valores raros (<1%):")
        for val, pct in valores_raros.items():
            print(f"  - {val}: {pct:.2f}%")

# Análise de correlações entre os campos
print("\n============ ANÁLISE DE CORRELAÇÕES ============")

# Verificar a correlação entre CLASSIFICACAO e TIPO
print("\n--- Correlação entre CLASSIFICACAO e TIPO ---")
correlacao_class_tipo = pd.crosstab(df['CLASSIFICACAO'], df['TIPO'])
print(correlacao_class_tipo)

# Verificar a correlação entre TP_PESSOA e Desc_Modalidade
print("\n--- Correlação entre TP_PESSOA e Desc_Modalidade ---")
correlacao_pessoa_modalidade = pd.crosstab(df['TP_PESSOA'], df['Desc_Modalidade'])
print(correlacao_pessoa_modalidade)

# Verificar a correlação entre DIR_REG_NEW e GER_REG_NEW
print("\n--- Correlação entre DIR_REG_NEW e GER_REG_NEW ---")
correlacao_dir_ger = pd.crosstab(df['DIR_REG_NEW'], df['GER_REG_NEW'])
print(correlacao_dir_ger)

# Adicionando análise condicional
print("\n============ ANÁLISE CONDICIONAL ============")

# Analisar a distribuição de Desc_Modalidade para cada tipo de pessoa
print("\n--- Distribuição de Desc_Modalidade por TP_PESSOA ---")
for tipo_pessoa in df['TP_PESSOA'].unique():
    subset = df[df['TP_PESSOA'] == tipo_pessoa]
    print(f"\nPara {tipo_pessoa}:")
    modalidades = subset['Desc_Modalidade'].value_counts()
    porcentagem = subset['Desc_Modalidade'].value_counts(normalize=True) * 100
    resultado = pd.DataFrame({
        'Contagem': modalidades,
        'Porcentagem (%)': porcentagem
    })
    print(resultado.sort_values('Contagem', ascending=False))

# Analisar a distribuição de TIPO para cada CLASSIFICACAO
print("\n--- Distribuição de TIPO por CLASSIFICACAO ---")
for classificacao in df['CLASSIFICACAO'].unique():
    subset = df[df['CLASSIFICACAO'] == classificacao]
    print(f"\nPara {classificacao}:")
    tipos = subset['TIPO'].value_counts()
    porcentagem = subset['TIPO'].value_counts(normalize=True) * 100
    resultado = pd.DataFrame({
        'Contagem': tipos,
        'Porcentagem (%)': porcentagem
    })
    print(resultado.sort_values('Contagem', ascending=False))

# Estatísticas de número de registros por região
print("\n--- Estatísticas por Região (DIR_REG_NEW) ---")
regioes = df['DIR_REG_NEW'].value_counts()
print(regioes)
print(f"\nTotal de regiões distintas: {len(regioes)}")
