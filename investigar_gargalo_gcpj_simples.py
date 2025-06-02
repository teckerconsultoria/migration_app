import pandas as pd
import numpy as np
from config import Config
import os
import re

def investigar_correspondencia_gcpj():
    """
    Investigacao detalhada do gargalo de correspondencia GCPJ
    entre as fontes primaria e secundaria
    """
    print("INVESTIGACAO DO GARGALO GCPJ")
    print("=" * 50)
    
    # Carregar dados
    print("Carregando dados...")
    
    base_path = r"C:\desenvolvimento\migration_app"
    primary_df = pd.read_excel(os.path.join(base_path, "cópia-MOYA E LARA_BASE GCPJ ATIVOS - 07_04_2025.xlsx"))
    secondary_df = pd.read_excel(os.path.join(base_path, "4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA _ABRIL_disp_24_04_2025.xlsx"))
    
    print(f"[OK] Primaria: {len(primary_df)} registros")
    print(f"[OK] Secundaria: {len(secondary_df)} registros")
    
    # Analisar GCPJs únicos
    print("\nANALISE DE GCPJs UNICOS")
    print("-" * 30)
    
    # GCPJs primários
    gcpj_primary = primary_df['GCPJ'].dropna()
    gcpj_primary_unique = gcpj_primary.nunique()
    gcpj_primary_total = len(gcpj_primary)
    
    print(f"Primaria - Total GCPJs: {gcpj_primary_total}")
    print(f"Primaria - GCPJs unicos: {gcpj_primary_unique}")
    print(f"Primaria - Taxa duplicacao: {((gcpj_primary_total - gcpj_primary_unique) / gcpj_primary_total * 100):.1f}%")
    
    # GCPJs secundários
    gcpj_secondary = secondary_df['GCPJ'].dropna()
    gcpj_secondary_unique = gcpj_secondary.nunique()
    gcpj_secondary_total = len(gcpj_secondary)
    
    print(f"Secundaria - Total GCPJs: {gcpj_secondary_total}")
    print(f"Secundaria - GCPJs unicos: {gcpj_secondary_unique}")
    print(f"Secundaria - Taxa duplicacao: {((gcpj_secondary_total - gcpj_secondary_unique) / gcpj_secondary_total * 100):.1f}%")
    
    # Correspondência direta
    print("\nANALISE DE CORRESPONDENCIA DIRETA")
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
    
    print(f"GCPJs unicos na Primaria: {len(primary_gcpj_norm)}")
    print(f"GCPJs unicos na Secundaria: {len(secondary_gcpj_norm)}")
    print(f"Correspondencia EXATA: {len(correspondencia_exata)}")
    print(f"Taxa de correspondencia exata: {(len(correspondencia_exata) / len(primary_gcpj_norm) * 100):.2f}%")
    
    # Análise de padrões
    print("\nANALISE DE PADROES DE GCPJ")
    print("-" * 35)
    
    def analisar_padroes(gcpj_set, nome):
        print(f"\n[{nome}]:")
        
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
    
    prefixos_prim, tamanhos_prim = analisar_padroes(primary_gcpj_norm, "PRIMARIA")
    prefixos_sec, tamanhos_sec = analisar_padroes(secondary_gcpj_norm, "SECUNDARIA")
    
    # Análise de correspondência por prefixo
    print("\nCORRESPONDENCIA POR PREFIXO")
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
    
    print(f"Correspondencia FLEXIVEL: {len(correspondencia_flexivel)}")
    print(f"Taxa de correspondencia flexivel: {(len(correspondencia_flexivel) / len(primary_gcpj_norm) * 100):.2f}%")
    print(f"Melhoria vs exata: +{(len(correspondencia_flexivel) - len(correspondencia_exata))}")
    
    # Análise dos não correspondentes
    print("\nANALISE DOS NAO CORRESPONDENTES")
    print("-" * 38)
    
    nao_correspondentes = primary_gcpj_norm - correspondencia_flexivel
    print(f"GCPJs sem correspondencia: {len(nao_correspondentes)}")
    print(f"Taxa de falha: {(len(nao_correspondentes) / len(primary_gcpj_norm) * 100):.2f}%")
    
    # Analisar padrões dos não correspondentes
    if nao_correspondentes:
        print("\nPadroes dos nao correspondentes:")
        prefixos_nao_corresp = {}
        for gcpj in list(nao_correspondentes)[:100]:  # Amostra
            if len(str(gcpj)) >= 2:
                prefixo = str(gcpj)[:2]
                prefixos_nao_corresp[prefixo] = prefixos_nao_corresp.get(prefixo, 0) + 1
        
        top_nao_corresp = sorted(prefixos_nao_corresp.items(), key=lambda x: x[1], reverse=True)[:10]
        print(f"  Top prefixos nao correspondentes: {top_nao_corresp}")
        
        # Exemplos de não correspondentes
        exemplos_nao_corresp = list(nao_correspondentes)[:10]
        print(f"  Exemplos: {exemplos_nao_corresp}")
    
    # Recomendações específicas
    print("\nRECOMENDACOES ESPECIFICAS")
    print("-" * 32)
    
    melhoria_potencial = len(correspondencia_flexivel) - len(correspondencia_exata)
    taxa_atual = len(correspondencia_exata) / len(primary_gcpj_norm) * 100
    taxa_potencial = len(correspondencia_flexivel) / len(primary_gcpj_norm) * 100
    
    print(f"1. IMPLEMENTAR MATCHING FLEXIVEL")
    print(f"   - Taxa atual: {taxa_atual:.2f}%")
    print(f"   - Taxa potencial: {taxa_potencial:.2f}%")
    print(f"   - Melhoria: +{melhoria_potencial} registros (+{(taxa_potencial-taxa_atual):.2f}%)")
    
    print(f"\n2. INVESTIGAR PREFIXOS PROBLEMATICOS")
    if nao_correspondentes:
        problematicos = list(prefixos_nao_corresp.keys())[:3]
        print(f"   - Prefixos criticos: {problematicos}")
        print(f"   - Pode representar filiais/regioes diferentes")
    
    print(f"\n3. AMPLIAR BASE SECUNDARIA")
    cobertura_atual = len(secondary_gcpj_norm) / len(primary_gcpj_norm) * 100
    print(f"   - Cobertura atual: {cobertura_atual:.1f}%")
    print(f"   - Buscar dados historicos complementares")
    
    # Salvar relatório detalhado
    print(f"\nSALVANDO RELATORIO DETALHADO...")
    
    relatorio_detalhado = {
        'correspondencia_exata': len(correspondencia_exata),
        'correspondencia_flexivel': len(correspondencia_flexivel),
        'taxa_exata': taxa_atual,
        'taxa_flexivel': taxa_potencial,
        'gcpj_primarios_unicos': len(primary_gcpj_norm),
        'gcpj_secundarios_unicos': len(secondary_gcpj_norm),
        'nao_correspondentes': len(nao_correspondentes)
    }
    
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"investigacao_gcpj_{timestamp}.xlsx"
    report_path = os.path.join(base_path, report_filename)
    
    # Criar DataFrame simplificado
    analise_df = pd.DataFrame([relatorio_detalhado])
    analise_df.to_excel(report_path, index=False)
    
    print(f"[OK] Relatorio salvo em: {report_path}")
    
    return relatorio_detalhado

if __name__ == "__main__":
    try:
        resultado = investigar_correspondencia_gcpj()
        print(f"\n[SUCESSO] Investigacao concluida!")
        print(f"Taxa de melhoria potencial: +{(resultado['taxa_flexivel'] - resultado['taxa_exata']):.2f}%")
    except Exception as e:
        print(f"[ERRO] Erro durante investigacao: {str(e)}")
        import traceback
        traceback.print_exc()
