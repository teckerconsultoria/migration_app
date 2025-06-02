import os
import pandas as pd
import numpy as np
import re
from collections import Counter

def analyze_gcpj_patterns(primary_path, secondary_path):
    """
    Analisa os padrões de GCPJ nos arquivos primário e secundário.
    """
    print(f"Analisando padrões de GCPJ nos arquivos...")
    
    # Carregar os arquivos
    primary_df = pd.read_excel(primary_path)
    secondary_df = pd.read_excel(secondary_path)
    
    # Estatísticas básicas
    print(f"\n=== Estatísticas Básicas ===")
    print(f"Registros no arquivo primário: {len(primary_df)}")
    print(f"Registros no arquivo secundário: {len(secondary_df)}")
    
    # Verificar se a coluna GCPJ existe nos dois arquivos
    if 'GCPJ' not in primary_df.columns:
        print(f"ERRO: Coluna GCPJ não encontrada no arquivo primário!")
        return
    if 'GCPJ' not in secondary_df.columns:
        print(f"ERRO: Coluna GCPJ não encontrada no arquivo secundário!")
        return
    
    # Verificar valores nulos
    primary_nulls = primary_df['GCPJ'].isna().sum()
    secondary_nulls = secondary_df['GCPJ'].isna().sum()
    print(f"Valores nulos no arquivo primário: {primary_nulls} ({primary_nulls/len(primary_df)*100:.2f}%)")
    print(f"Valores nulos no arquivo secundário: {secondary_nulls} ({secondary_nulls/len(secondary_df)*100:.2f}%)")
    
    # Remover valores nulos para análise
    primary_df = primary_df.dropna(subset=['GCPJ'])
    secondary_df = secondary_df.dropna(subset=['GCPJ'])
    
    # Tipos de dados
    print(f"\n=== Tipos de Dados ===")
    print(f"Tipo de dados da coluna GCPJ no arquivo primário: {primary_df['GCPJ'].dtype}")
    print(f"Tipo de dados da coluna GCPJ no arquivo secundário: {secondary_df['GCPJ'].dtype}")
    
    # Exemplos de valores
    print(f"\n=== Exemplos de Valores ===")
    print(f"Exemplos de GCPJ no arquivo primário: {primary_df['GCPJ'].head(5).tolist()}")
    print(f"Exemplos de GCPJ no arquivo secundário: {secondary_df['GCPJ'].head(5).tolist()}")
    
    # Analisar padrões
    print(f"\n=== Análise de Padrões ===")
    
    # Converter para string para analisar padrões
    primary_gcpjs = primary_df['GCPJ'].astype(str).tolist()
    secondary_gcpjs = secondary_df['GCPJ'].astype(str).tolist()
    
    # Analisar comprimento
    primary_lengths = Counter([len(str(gcpj).strip()) for gcpj in primary_gcpjs])
    secondary_lengths = Counter([len(str(gcpj).strip()) for gcpj in secondary_gcpjs])
    
    print(f"Distribuição de comprimento dos GCPJs no arquivo primário: {primary_lengths}")
    print(f"Distribuição de comprimento dos GCPJs no arquivo secundário: {secondary_lengths}")
    
    # Analisar prefixos (primeiros 2, 3 e 4 caracteres)
    def get_prefix_stats(gcpjs, length):
        return Counter([str(gcpj)[:length] for gcpj in gcpjs if len(str(gcpj)) >= length])
    
    for prefix_len in [2, 3, 4]:
        print(f"\nPrefixos de {prefix_len} caracteres no arquivo primário:")
        primary_prefixes = get_prefix_stats(primary_gcpjs, prefix_len)
        for prefix, count in primary_prefixes.most_common(10):
            print(f"  {prefix}: {count} ({count/len(primary_gcpjs)*100:.2f}%)")
            
        print(f"\nPrefixos de {prefix_len} caracteres no arquivo secundário:")
        secondary_prefixes = get_prefix_stats(secondary_gcpjs, prefix_len)
        for prefix, count in secondary_prefixes.most_common(10):
            print(f"  {prefix}: {count} ({count/len(secondary_gcpjs)*100:.2f}%)")
            
    # Interseção e diferença
    primary_set = set([str(int(float(gcpj))) if re.match(r'^\d+\.0+$', str(gcpj)) else str(gcpj) for gcpj in primary_gcpjs])
    secondary_set = set([str(int(float(gcpj))) if re.match(r'^\d+\.0+$', str(gcpj)) else str(gcpj) for gcpj in secondary_gcpjs])
    
    intersection = primary_set.intersection(secondary_set)
    only_in_primary = primary_set - secondary_set
    only_in_secondary = secondary_set - primary_set
    
    print(f"\n=== Comparação de Conjuntos ===")
    print(f"GCPJs únicos no arquivo primário: {len(primary_set)}")
    print(f"GCPJs únicos no arquivo secundário: {len(secondary_set)}")
    print(f"GCPJs em ambos os arquivos: {len(intersection)} ({len(intersection)/len(primary_set)*100:.2f}% do primário)")
    print(f"GCPJs apenas no arquivo primário: {len(only_in_primary)}")
    print(f"GCPJs apenas no arquivo secundário: {len(only_in_secondary)}")
    
    # Verificar se existem padrões de correspondência parcial
    print(f"\n=== Análise de Correspondência Parcial ===")
    
    # Procurar por correspondências após remover zeros à esquerda
    primary_nozero = set([str(gcpj).lstrip('0') for gcpj in primary_set])
    secondary_nozero = set([str(gcpj).lstrip('0') for gcpj in secondary_set])
    nozero_intersection = primary_nozero.intersection(secondary_nozero)
    print(f"Correspondências após remover zeros à esquerda: {len(nozero_intersection)}")
    
    # Procurar por correspondências após remover casas decimais
    primary_nofloat = set([str(gcpj).split('.')[0] for gcpj in primary_set])
    secondary_nofloat = set([str(gcpj).split('.')[0] for gcpj in secondary_set])
    nofloat_intersection = primary_nofloat.intersection(secondary_nofloat)
    print(f"Correspondências após remover casas decimais: {len(nofloat_intersection)}")
    
    # Análise de estrutura específica (padrões com hífen ou outros separadores)
    def get_pattern(gcpj):
        gcpj = str(gcpj)
        if '-' in gcpj:
            return "Com hífen"
        elif '.' in gcpj and not gcpj.endswith('.0'):
            return "Com ponto (não decimal)"
        elif gcpj.endswith('.0'):
            return "Decimal .0"
        elif gcpj.isdigit():
            return "Apenas dígitos"
        else:
            return "Outro formato"
    
    primary_patterns = Counter([get_pattern(gcpj) for gcpj in primary_gcpjs])
    secondary_patterns = Counter([get_pattern(gcpj) for gcpj in secondary_gcpjs])
    
    print(f"\nPadrões de formato no arquivo primário:")
    for pattern, count in primary_patterns.items():
        print(f"  {pattern}: {count} ({count/len(primary_gcpjs)*100:.2f}%)")
    
    print(f"\nPadrões de formato no arquivo secundário:")
    for pattern, count in secondary_patterns.items():
        print(f"  {pattern}: {count} ({count/len(secondary_gcpjs)*100:.2f}%)")
        
    # Análise de prefixos comuns
    print(f"\n=== Análise de Transformações ===")
    
    # Tentar identificar transformações que aumentariam a correspondência
    transformations = [
        ("Sem transformação", lambda x: str(x)),
        ("Remover .0", lambda x: str(x).replace('.0', '')),
        ("Int para float", lambda x: str(float(x)) if str(x).isdigit() else str(x)),
        ("Float para int", lambda x: str(int(float(x))) if re.match(r'^\d+\.\d+$', str(x)) else str(x)),
        ("Adicionar 0s à esquerda (10 dígitos)", lambda x: str(x).zfill(10)),
        ("Adicionar prefixo 0 (se não tiver)", lambda x: '0' + str(x) if not str(x).startswith('0') else str(x)),
        ("Adicionar prefixo '16' (se não tiver)", lambda x: '16' + str(x) if not str(x).startswith('16') else str(x)),
        ("Remover prefixo '16' (se tiver)", lambda x: str(x)[2:] if str(x).startswith('16') else str(x)),
        ("Converter 22/24 para 16", lambda x: '16' + str(x)[2:] if str(x).startswith(('22', '24')) else str(x)),
        ("Converter 16 para 22", lambda x: '22' + str(x)[2:] if str(x).startswith('16') else str(x)),
    ]
    
    results = []
    for name, transform_func in transformations:
        try:
            primary_transformed = set([transform_func(gcpj) for gcpj in primary_set])
            secondary_transformed = set([transform_func(gcpj) for gcpj in secondary_set])
            intersection = primary_transformed.intersection(secondary_transformed)
            match_rate = len(intersection) / len(primary_set) * 100
            results.append((name, len(intersection), match_rate))
            print(f"Transformação '{name}': {len(intersection)} correspondências ({match_rate:.2f}%)")
        except Exception as e:
            print(f"Erro na transformação '{name}': {str(e)}")
    
    # Encontrar a melhor transformação
    if results:
        best_transform = max(results, key=lambda x: x[1])
        print(f"\nMelhor transformação: '{best_transform[0]}' com {best_transform[1]} correspondências ({best_transform[2]:.2f}%)")
    
    return {
        "primary_count": len(primary_df),
        "secondary_count": len(secondary_df),
        "primary_unique": len(primary_set),
        "secondary_unique": len(secondary_set),
        "intersection": len(intersection),
        "best_transformation": best_transform if results else None
    }

if __name__ == "__main__":
    # Definir caminhos dos arquivos
    base_dir = os.path.dirname(os.path.abspath(__file__))
    primary_path = os.path.join(base_dir, 'uploads', 'primary.xlsx')
    secondary_path = os.path.join(base_dir, 'uploads', 'secondary.xlsx')
    
    # Executar análise
    results = analyze_gcpj_patterns(primary_path, secondary_path)
    
    print("\nAnálise concluída. Execute o processador com as novas transformações.")
