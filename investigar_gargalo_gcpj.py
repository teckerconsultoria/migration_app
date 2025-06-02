import pandas as pd
import numpy as np
from config import Config
import os
import re

def investigar_correspondencia_gcpj():
    """
    Investigação detalhada do gargalo de correspondência GCPJ
    entre as fontes primária e secundária
    """
    print("🔍 INVESTIGAÇÃO DO GARGALO GCPJ")
    print("=" * 50)
    
    # Carregar dados
    print("📊 Carregando dados...")
    
    base_path = r"C:\desenvolvimento\migration_app"
    primary_df = pd.read_excel(os.path.join(base_path, "cópia-MOYA E LARA_BASE GCPJ ATIVOS - 07_04_2025.xlsx"))
    secondary_df = pd.read_excel(os.path.join(base_path, "4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA _ABRIL_disp_24_04_2025.xlsx"))
    
    print(f"✅ Primária: {len(primary_df)} registros")
    print(f"✅ Secundária: {len(secondary_df)} registros")
    
    # Analisar GCPJs únicos
    print("\n📈 ANÁLISE DE GCPJs ÚNICOS")
    print("-" * 30)
    
    # GCPJs primários
    gcpj_primary = primary_df['GCPJ'].dropna()
    gcpj_primary_unique = gcpj_primary.nunique()
    gcpj_primary_total = len(gcpj_primary)
    
    print(f"Primária - Total GCPJs: {gcpj_primary_total}")
    print(f"Primária - GCPJs únicos: {gcpj_primary_unique}")
    print(f"Primária - Taxa duplicação: {((gcpj_primary_total - gcpj_primary_unique) / gcpj_primary_total * 100):.1f}%")
    
    # GCPJs secundários
    gcpj_secondary = secondary_df['GCPJ'].dropna()
    gcpj_secondary_unique = gcpj_secondary.nunique()
    gcpj_secondary_total = len(gcpj_secondary)
    
    print(f"Secundária - Total GCPJs: {gcpj_secondary_total}")
    print(f"Secundária - GCPJs únicos: {gcpj_secondary_unique}")
    print(f"Secundária - Taxa duplicação: {((gcpj_secondary_total - gcpj_secondary_unique) / gcpj_secondary_total * 100):.1f}%")
    
    # Correspondência direta
    print("\n🔗 ANÁLISE DE CORRESPONDÊNCIA DIRETA")
    print("-" * 40)
    
    # Normalizar GCPJs para comparação
    def normalizar_gcpj(gcpj):
        if pd.isna(gcpj):
            return None
        return str(int(gcpj)) if isinstance(gcpj, (int, float)) else str(gcpj).strip()
    
    primary_gcpj_norm = set(primary_df['GCPJ'].dropna().apply(normalizar_gcpj))
    secondary_gcpj_norm = set(secondary_df['GCPJ'].dropna().apply(normalizar_gcpj))
    
    # Correspondência exata
    correspondencia_exata = primary_gcpj_norm.intersection(secondary_gcpj_norm)
    
    print(f"GCPJs únicos na Primária: {len(primary_gcpj_norm)}")
    print(f"GCPJs únicos na Secundária: {len(secondary_gcpj_norm)}")
    print(f"Correspondência EXATA: {len(correspondencia_exata)}")
    print(f"Taxa de correspondência exata: {(len(correspondencia_exata) / len(primary_gcpj_norm) * 100):.2f}%")
    
    # Análise de padrões
    print("\n🔬 ANÁLISE DE PADRÕES DE GCPJ")
    print("-" * 35)
    
    def analisar_padroes(gcpj_set, nome):
        print(f"\n📋 {nome}:")
        
        # Agrupar por prefixo (2 primeiros dígitos)
        prefixos = {}
        tamanhos = {}
        
        for gcpj in gcpj_set:
            if gcpj and len(str(gcpj)) >= 2:
                prefixo = str(gcpj)[:2]
                prefixos[prefixo] = prefixos.get(prefixo, 0) + 1
                
                tamanho = len(str(gcpj))
                tamanhos[tamanho] = tamanhos.get(tamanho, 0) + 1
        
        # Top 10 prefixos
        top_prefixos = sorted(prefixos.items(), key=lambda x: x[1], reverse=True)[:10]
        print(f"  Top prefixos: {top_prefixos}")
        
        # Distribuição de tamanhos
        print(f"  Tamanhos: {dict(sorted(tamanhos.items()))}")
        
        # Exemplos
        exemplos = list(gcpj_set)[:5]
        print(f"  Exemplos: {exemplos}")
        
        return prefixos, tamanhos
    
    prefixos_prim, tamanhos_prim = analisar_padroes(primary_gcpj_norm, "PRIMÁRIA")
    prefixos_sec, tamanhos_sec = analisar_padroes(secondary_gcpj_norm, "SECUNDÁRIA")
    
    # Análise de correspondência por prefixo
    print("\n🎯 CORRESPONDÊNCIA POR PREFIXO")
    print("-" * 32)
    
    # Verificar correspondência flexível por prefixo
    def gerar_chaves_alternativas(gcpj):
        keys = [gcpj]  # Chave original
        
        if len(gcpj) > 2:
            # Sem os 2 primeiros dígitos
            keys.append(gcpj[2:])
            
            # Variações de prefixo
            if gcpj.startswith('22'):
                keys.append('16' + gcpj[2:])
                keys.append('24' + gcpj[2:])
            elif gcpj.startswith('24'):
                keys.append('16' + gcpj[2:])
                keys.append('22' + gcpj[2:])
            elif gcpj.startswith('16'):
                keys.append('22' + gcpj[2:])
                keys.append('24' + gcpj[2:])
        
        return keys
    
    # Criar mapeamento flexível
    secondary_keys_map = {}
    for gcpj in secondary_gcpj_norm:
        keys = gerar_chaves_alternativas(gcpj)
        for key in keys:
            if key not in secondary_keys_map:
                secondary_keys_map[key] = []
            secondary_keys_map[key].append(gcpj)
    
    # Testar correspondência flexível
    correspondencia_flexivel = set()
    correspondencia_detalhes = {}
    
    for gcpj_prim in primary_gcpj_norm:
        keys_alt = gerar_chaves_alternativas(gcpj_prim)
        
        for key in keys_alt:
            if key in secondary_keys_map:
                correspondencia_flexivel.add(gcpj_prim)
                correspondencia_detalhes[gcpj_prim] = {
                    'chave_match': key,
                    'gcpj_secundarios': secondary_keys_map[key]
                }
                break
    
    print(f"Correspondência FLEXÍVEL: {len(correspondencia_flexivel)}")
    print(f"Taxa de correspondência flexível: {(len(correspondencia_flexivel) / len(primary_gcpj_norm) * 100):.2f}%")
    print(f"Melhoria vs exata: +{(len(correspondencia_flexivel) - len(correspondencia_exata))}")
    
    # Análise dos não correspondentes
    print("\n❌ ANÁLISE DOS NÃO CORRESPONDENTES")
    print("-" * 38)
    
    nao_correspondentes = primary_gcpj_norm - correspondencia_flexivel
    print(f"GCPJs sem correspondência: {len(nao_correspondentes)}")
    print(f"Taxa de falha: {(len(nao_correspondentes) / len(primary_gcpj_norm) * 100):.2f}%")
    
    # Analisar padrões dos não correspondentes
    if nao_correspondentes:
        print("\n🔍 Padrões dos não correspondentes:")
        prefixos_nao_corresp = {}
        for gcpj in list(nao_correspondentes)[:100]:  # Amostra
            if len(str(gcpj)) >= 2:
                prefixo = str(gcpj)[:2]
                prefixos_nao_corresp[prefixo] = prefixos_nao_corresp.get(prefixo, 0) + 1
        
        top_nao_corresp = sorted(prefixos_nao_corresp.items(), key=lambda x: x[1], reverse=True)[:10]
        print(f"  Top prefixos não correspondentes: {top_nao_corresp}")
        
        # Exemplos de não correspondentes
        exemplos_nao_corresp = list(nao_correspondentes)[:10]
        print(f"  Exemplos: {exemplos_nao_corresp}")
    
    # Análise da qualidade dos dados secundários
    print("\n📊 QUALIDADE DOS DADOS SECUNDÁRIOS")
    print("-" * 37)
    
    # Verificar completude das colunas alvo
    colunas_alvo = ['TIPO', 'PROCADV_CONTRATO']
    
    for coluna in colunas_alvo:
        if coluna in secondary_df.columns:
            total = len(secondary_df)
            nao_nulos = secondary_df[coluna].notna().sum()
            nao_vazios = secondary_df[secondary_df[coluna].notna()][coluna].astype(str).str.strip().ne('').sum()
            
            print(f"\n📋 Coluna: {coluna}")
            print(f"  Total registros: {total}")
            print(f"  Não nulos: {nao_nulos} ({(nao_nulos/total*100):.1f}%)")
            print(f"  Não vazios: {nao_vazios} ({(nao_vazios/total*100):.1f}%)")
            
            # Exemplos de valores
            valores_exemplo = secondary_df[coluna].dropna().head(5).tolist()
            print(f"  Exemplos: {valores_exemplo}")
            
            # Valores únicos
            valores_unicos = secondary_df[coluna].nunique()
            print(f"  Valores únicos: {valores_unicos}")
    
    # Recomendações específicas
    print("\n💡 RECOMENDAÇÕES ESPECÍFICAS")
    print("-" * 32)
    
    melhoria_potencial = len(correspondencia_flexivel) - len(correspondencia_exata)
    taxa_atual = len(correspondencia_exata) / len(primary_gcpj_norm) * 100
    taxa_potencial = len(correspondencia_flexivel) / len(primary_gcpj_norm) * 100
    
    print(f"1. 🎯 IMPLEMENTAR MATCHING FLEXÍVEL")
    print(f"   - Taxa atual: {taxa_atual:.2f}%")
    print(f"   - Taxa potencial: {taxa_potencial:.2f}%")
    print(f"   - Melhoria: +{melhoria_potencial} registros (+{(taxa_potencial-taxa_atual):.2f}%)")
    
    print(f"\n2. 🔍 INVESTIGAR PREFIXOS PROBLEMÁTICOS")
    if nao_correspondentes:
        problematicos = list(prefixos_nao_corresp.keys())[:3]
        print(f"   - Prefixos críticos: {problematicos}")
        print(f"   - Pode representar filiais/regiões diferentes")
    
    print(f"\n3. 📈 AMPLIAR BASE SECUNDÁRIA")
    cobertura_atual = len(secondary_gcpj_norm) / len(primary_gcpj_norm) * 100
    print(f"   - Cobertura atual: {cobertura_atual:.1f}%")
    print(f"   - Buscar dados históricos complementares")
    
    print(f"\n4. 🔧 OTIMIZAR ALGORITMO")
    print(f"   - Implementar fuzzy matching por AGÊNCIA/CONTA")
    print(f"   - Considerar dados auxiliares para matching")
    print(f"   - Validar correspondências múltiplas")
    
    # Salvar relatório detalhado
    print(f"\n💾 SALVANDO RELATÓRIO DETALHADO...")
    
    relatorio_detalhado = {
        'correspondencia_exata': len(correspondencia_exata),
        'correspondencia_flexivel': len(correspondencia_flexivel),
        'taxa_exata': taxa_atual,
        'taxa_flexivel': taxa_potencial,
        'gcpj_primarios_unicos': len(primary_gcpj_norm),
        'gcpj_secundarios_unicos': len(secondary_gcpj_norm),
        'prefixos_primarios': prefixos_prim,
        'prefixos_secundarios': prefixos_sec,
        'nao_correspondentes': len(nao_correspondentes)
    }
    
    # Criar DataFrame com análise detalhada
    analise_df = pd.DataFrame([relatorio_detalhado])
    
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"investigacao_gcpj_{timestamp}.xlsx"
    report_path = os.path.join(base_path, report_filename)
    
    with pd.ExcelWriter(report_path) as writer:
        analise_df.to_excel(writer, sheet_name='Resumo', index=False)
        
        # Correspondência exata
        pd.DataFrame(list(correspondencia_exata), columns=['GCPJ']).to_excel(
            writer, sheet_name='Correspondencia_Exata', index=False)
        
        # Correspondência flexível
        pd.DataFrame(list(correspondencia_flexivel), columns=['GCPJ']).to_excel(
            writer, sheet_name='Correspondencia_Flexivel', index=False)
        
        # Não correspondentes (amostra)
        pd.DataFrame(list(nao_correspondentes)[:1000], columns=['GCPJ']).to_excel(
            writer, sheet_name='Nao_Correspondentes', index=False)
        
        # Detalhes de correspondência flexível (amostra)
        detalhes_list = []
        for gcpj, detalhes in list(correspondencia_detalhes.items())[:1000]:
            detalhes_list.append({
                'GCPJ_Primario': gcpj,
                'Chave_Match': detalhes['chave_match'],
                'GCPJ_Secundarios': ', '.join(detalhes['gcpj_secundarios'])
            })
        
        pd.DataFrame(detalhes_list).to_excel(
            writer, sheet_name='Detalhes_Flexivel', index=False)
    
    print(f"✅ Relatório salvo em: {report_path}")
    
    return relatorio_detalhado

if __name__ == "__main__":
    try:
        resultado = investigar_correspondencia_gcpj()
        print(f"\n🎉 Investigação concluída com sucesso!")
        print(f"📊 Taxa de melhoria potencial: +{(resultado['taxa_flexivel'] - resultado['taxa_exata']):.2f}%")
    except Exception as e:
        print(f"❌ Erro durante investigação: {str(e)}")
        import traceback
        traceback.print_exc()
