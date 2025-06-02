import os
import pandas as pd
import numpy as np
from collections import Counter

def analyze_gcpj_patterns():
    """
    Analisa os padrões básicos de GCPJ nos arquivos primário e secundário.
    """
    print(f"Analisando padrões de GCPJ nos arquivos...")
    
    # Definir caminhos dos arquivos
    base_dir = os.path.dirname(os.path.abspath(__file__))
    primary_path = os.path.join(base_dir, 'uploads', 'primary.xlsx')
    secondary_path = os.path.join(base_dir, 'uploads', 'secondary.xlsx')
    
    # Carregar os arquivos
    primary_df = pd.read_excel(primary_path)
    secondary_df = pd.read_excel(secondary_path)
    
    # Estatísticas básicas
    print(f"\n=== Estatísticas Básicas ===")
    print(f"Registros no arquivo primário: {len(primary_df)}")
    print(f"Registros no arquivo secundário: {len(secondary_df)}")
    
    # Verificar valores nulos
    primary_nulls = primary_df['GCPJ'].isna().sum()
    secondary_nulls = secondary_df['GCPJ'].isna().sum()
    print(f"Valores nulos no arquivo primário: {primary_nulls} ({primary_nulls/len(primary_df)*100:.2f}%)")
    print(f"Valores nulos no arquivo secundário: {secondary_nulls} ({secondary_nulls/len(secondary_df)*100:.2f}%)")
    
    # Tipos de dados
    print(f"\n=== Tipos de Dados ===")
    print(f"Tipo de dados da coluna GCPJ no arquivo primário: {primary_df['GCPJ'].dtype}")
    print(f"Tipo de dados da coluna GCPJ no arquivo secundário: {secondary_df['GCPJ'].dtype}")
    
    # Exemplos de valores
    print(f"\n=== Exemplos de Valores ===")
    print(f"Exemplos de GCPJ no arquivo primário: {primary_df['GCPJ'].head(5).tolist()}")
    print(f"Exemplos de GCPJ no arquivo secundário: {secondary_df['GCPJ'].head(5).tolist()}")
    
    # Analisar prefixos (primeiros 2, 3 e 4 caracteres)
    primary_gcpjs = primary_df['GCPJ'].astype(str).tolist()
    secondary_gcpjs = secondary_df['GCPJ'].astype(str).tolist()
    
    def get_prefix_stats(gcpjs, length):
        return Counter([str(gcpj)[:length] for gcpj in gcpjs if len(str(gcpj)) >= length])
    
    print(f"\n=== Análise de Prefixos ===")
    for prefix_len in [2, 3, 4]:
        print(f"\nPrefixos de {prefix_len} caracteres no arquivo primário:")
        primary_prefixes = get_prefix_stats(primary_gcpjs, prefix_len)
        for prefix, count in primary_prefixes.most_common(10):
            print(f"  {prefix}: {count} ({count/len(primary_gcpjs)*100:.2f}%)")
            
        print(f"\nPrefixos de {prefix_len} caracteres no arquivo secundário:")
        secondary_prefixes = get_prefix_stats(secondary_gcpjs, prefix_len)
        for prefix, count in secondary_prefixes.most_common(10):
            print(f"  {prefix}: {count} ({count/len(secondary_gcpjs)*100:.2f}%)")
    
    # Análise básica de correspondência
    # Normalizar para comparação
    primary_set = set([str(gcpj).split('.')[0] for gcpj in primary_df['GCPJ'].dropna()])
    secondary_set = set([str(gcpj).split('.')[0] for gcpj in secondary_df['GCPJ'].dropna()])
    
    # Transformações que podem melhorar a correspondência
    primary_prefix16_to_22 = set(['22' + str(gcpj)[2:] if str(gcpj).startswith('16') else str(gcpj) for gcpj in primary_set])
    secondary_prefix22_to_16 = set(['16' + str(gcpj)[2:] if str(gcpj).startswith('22') else str(gcpj) for gcpj in secondary_set])
    
    intersection = primary_set.intersection(secondary_set)
    intersection_transformed1 = primary_prefix16_to_22.intersection(secondary_set)
    intersection_transformed2 = primary_set.intersection(secondary_prefix22_to_16)
    
    print(f"\n=== Análise de Correspondência ===")
    print(f"GCPJs únicos no arquivo primário: {len(primary_set)}")
    print(f"GCPJs únicos no arquivo secundário: {len(secondary_set)}")
    print(f"GCPJs em ambos arquivos (sem transformação): {len(intersection)} ({len(intersection)/len(primary_set)*100:.2f}%)")
    print(f"GCPJs após transformar '16' para '22' no primário: {len(intersection_transformed1)} ({len(intersection_transformed1)/len(primary_set)*100:.2f}%)")
    print(f"GCPJs após transformar '22' para '16' no secundário: {len(intersection_transformed2)} ({len(intersection_transformed2)/len(primary_set)*100:.2f}%)")
    
    # Verificar padrões de substituição específicos
    def test_transformation(name, transform_primary, transform_secondary=None):
        """Testa uma transformação específica e retorna a taxa de correspondência"""
        if transform_secondary is None:
            # Aplicar transformação apenas no primário
            primary_transformed = set([transform_primary(gcpj) for gcpj in primary_set])
            match = primary_transformed.intersection(secondary_set)
        else:
            # Aplicar transformações em ambos
            primary_transformed = set([transform_primary(gcpj) for gcpj in primary_set])
            secondary_transformed = set([transform_secondary(gcpj) for gcpj in secondary_set])
            match = primary_transformed.intersection(secondary_transformed)
            
        rate = len(match) / len(primary_set) * 100
        print(f"Transformação '{name}': {len(match)} correspondências ({rate:.2f}%)")
        return len(match), rate
    
    print(f"\n=== Testes de Transformações ===")
    
    # Teste 1: Substituir prefixo 16 por 22 no primário
    test_transformation(
        "Substituir '16' por '22' no primário", 
        lambda x: '22' + str(x)[2:] if str(x).startswith('16') else str(x)
    )
    
    # Teste 2: Substituir prefixo 22 por 16 no secundário
    test_transformation(
        "Substituir '22' por '16' no secundário", 
        lambda x: str(x),
        lambda x: '16' + str(x)[2:] if str(x).startswith('22') else str(x)
    )
    
    # Teste 3: Correspondência por prefixos específicos (mais complexa)
    def complex_transform(gcpj):
        gcpj_str = str(gcpj)
        if gcpj_str.startswith('16'):
            return '22' + gcpj_str[2:]
        elif gcpj_str.startswith('24'):
            return '16' + gcpj_str[2:]
        else:
            return gcpj_str
    
    test_transformation(
        "Mapeamento complexo de prefixos (16→22, 24→16)", 
        complex_transform
    )
    
    # Teste 4: Verificar a correspondência sem os dois primeiros dígitos
    test_transformation(
        "Ignorar os dois primeiros dígitos", 
        lambda x: str(x)[2:] if len(str(x)) > 2 else str(x),
        lambda x: str(x)[2:] if len(str(x)) > 2 else str(x)
    )

if __name__ == "__main__":
    # Executar análise
    analyze_gcpj_patterns()
