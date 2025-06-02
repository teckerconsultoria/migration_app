import pandas as pd
import numpy as np
from config import Config
import os

def analisar_completude():
    """
    Análise diagnóstica da taxa de completude por coluna do template
    conforme os mapeamentos definidos
    """
    print("=== DIAGNOSTICO DE COMPLETUDE POR COLUNA ===\n")
    
    # Carregar template
    print("[INFO] Carregando template...")
    base_path = r"C:\desenvolvimento\migration_app"
    template_path = os.path.join(base_path, "template-banco-bradesco-sa.xlsx")
    template_df = pd.read_excel(template_path, sheet_name='Sheet')
    template_columns = template_df.columns.tolist()
    print(f"[OK] Template carregado: {len(template_columns)} colunas\n")
    
    # Carregar fontes de dados
    print("[INFO] Carregando fontes de dados...")
    
    # Fonte primária
    primary_files = [
        os.path.join(base_path, "cópia-MOYA E LARA_BASE GCPJ ATIVOS - 07_04_2025.xlsx"),
        os.path.join(base_path, "copiaMOYA E LARA_BASE GCPJ ATIVOS.xlsx")  # Nome alternativo
    ]
    
    primary_df = None
    for file in primary_files:
        if os.path.exists(file):
            primary_df = pd.read_excel(file)
            print(f"[OK] Fonte primaria carregada: {os.path.basename(file)} ({len(primary_df)} registros)")
            break
    
    if primary_df is None:
        print("[ERRO] Arquivo primario nao encontrado")
        print(f"[INFO] Procurados: {[os.path.basename(f) for f in primary_files]}")
        return
    
    # Fonte secundária
    secondary_files = [
        os.path.join(base_path, "4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA _ABRIL_disp_24_04_2025.xlsx"),
        os.path.join(base_path, "4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PREVIA BASE ATIVA.xlsx")  # Nome alternativo
    ]
    
    secondary_df = None
    for file in secondary_files:
        if os.path.exists(file):
            secondary_df = pd.read_excel(file)
            print(f"[OK] Fonte secundaria carregada: {os.path.basename(file)} ({len(secondary_df)} registros)")
            break
    
    if secondary_df is None:
        print("[ERRO] Arquivo secundario nao encontrado")
        print(f"[INFO] Procurados: {[os.path.basename(f) for f in secondary_files]}")
        return
    
    print(f"\n[DADOS] Carregados:")
    print(f"   Template: {len(template_columns)} colunas")
    print(f"   Primaria: {len(primary_df)} registros, {len(primary_df.columns)} colunas")
    print(f"   Secundaria: {len(secondary_df)} registros, {len(secondary_df.columns)} colunas")
    
    # Mostrar colunas disponíveis para diagnóstico
    print(f"\n[COLUNAS] Fonte primaria:")
    for i, col in enumerate(primary_df.columns):
        print(f"   {i+1:2d}. {col}")
    
    print(f"\n[COLUNAS] Fonte secundaria:")
    for i, col in enumerate(secondary_df.columns):
        print(f"   {i+1:2d}. {col}")
    
    # Analisar mapeamentos
    print("\n" + "="*80)
    print("ANALISE DE COMPLETUDE POR TIPO DE MAPEAMENTO")
    print("="*80)
    
    # Classificar cada coluna do template
    column_analysis = []
    
    for template_col in template_columns:
        analysis = {
            'template_column': template_col,
            'source_type': 'VAZIA',
            'source_column': '-',
            'total_records': len(primary_df),
            'available_records': 0,
            'valid_records': 0,
            'completude_percent': 0.0,
            'notes': 'Sem mapeamento definido'
        }
        
        # Verificar mapeamento direto (primário)
        for template_key, source_key in Config.COLUMN_MAPPINGS.items():
            if template_key == template_col:
                analysis['source_type'] = 'PRIMARIA'
                analysis['source_column'] = source_key
                
                if source_key in primary_df.columns:
                    # Calcular completude
                    total = len(primary_df)
                    available = primary_df[source_key].notna().sum()
                    valid = primary_df[source_key].apply(
                        lambda x: pd.notna(x) and str(x).strip() != ''
                    ).sum()
                    
                    analysis['total_records'] = total
                    analysis['available_records'] = int(available)
                    analysis['valid_records'] = int(valid)
                    analysis['completude_percent'] = round((valid / total) * 100, 2) if total > 0 else 0
                    analysis['notes'] = 'Mapeamento direto da fonte primaria'
                else:
                    analysis['notes'] = f'ERRO: Coluna {source_key} nao encontrada na fonte primaria'
                break
        
        # Verificar valores constantes
        if template_col in Config.CONSTANT_VALUES:
            analysis['source_type'] = 'CONSTANTE'
            analysis['source_column'] = f"'{Config.CONSTANT_VALUES[template_col]}'"
            analysis['available_records'] = len(primary_df)
            analysis['valid_records'] = len(primary_df)
            analysis['completude_percent'] = 100.0
            analysis['notes'] = 'Valor constante - sempre preenchido'
        
        # Verificar mapeamento secundário (via GCPJ)
        if template_col in Config.SECONDARY_MAPPINGS:
            source_col = Config.SECONDARY_MAPPINGS[template_col]
            analysis['source_type'] = 'SECUNDARIA (GCPJ)'
            analysis['source_column'] = source_col
            
            if source_col in secondary_df.columns and 'GCPJ' in secondary_df.columns:
                # Calcular quantos GCPJs únicos existem na secundária com dados válidos
                valid_gcpj_secondary = secondary_df[
                    (secondary_df['GCPJ'].notna()) & 
                    (secondary_df[source_col].notna()) &
                    (secondary_df[source_col].astype(str).str.strip() != '')
                ]['GCPJ'].nunique()
                
                # Calcular quantos GCPJs únicos existem na primária
                gcpj_col_primary = None
                for template_key, source_key in Config.COLUMN_MAPPINGS.items():
                    if source_key == 'GCPJ':
                        gcpj_col_primary = source_key
                        break
                
                if gcpj_col_primary and gcpj_col_primary in primary_df.columns:
                    total_gcpj_primary = primary_df[gcpj_col_primary].notna().sum()
                    
                    # Estimativa de correspondência (seria necessário fazer o matching real para ser preciso)
                    analysis['total_records'] = int(total_gcpj_primary)
                    analysis['available_records'] = int(valid_gcpj_secondary)
                    analysis['valid_records'] = int(valid_gcpj_secondary)  # Simplificação
                    analysis['completude_percent'] = round((valid_gcpj_secondary / total_gcpj_primary) * 100, 2) if total_gcpj_primary > 0 else 0
                    analysis['notes'] = f'Mapeamento via GCPJ - {valid_gcpj_secondary} GCPJs validos na secundaria'
                else:
                    analysis['notes'] = 'ERRO: Coluna GCPJ nao encontrada na fonte primaria'
            else:
                analysis['notes'] = f'ERRO: Coluna {source_col} ou GCPJ nao encontrada na fonte secundaria'
        
        column_analysis.append(analysis)
    
    # Gerar relatório
    print("\n" + "="*120)
    print("RELATORIO DETALHADO DE COMPLETUDE POR COLUNA")
    print("="*120)
    
    # Cabeçalho
    print(f"{'COLUNA TEMPLATE':<35} {'FONTE':<15} {'COLUNA ORIGEM':<20} {'TOTAL':<8} {'VALIDOS':<8} {'%':<8} {'OBSERVACOES'}")
    print("-" * 120)
    
    # Estatísticas por tipo
    stats_by_type = {}
    
    for analysis in column_analysis:
        template_col = analysis['template_column'][:34]  # Limitar tamanho
        source_type = analysis['source_type']
        source_col = analysis['source_column'][:19]  # Limitar tamanho
        total = analysis['total_records']
        valid = analysis['valid_records']
        percent = analysis['completude_percent']
        notes = analysis['notes'][:50]  # Limitar tamanho
        
        print(f"{template_col:<35} {source_type:<15} {source_col:<20} {total:<8} {valid:<8} {percent:<7.1f}% {notes}")
        
        # Acumular estatísticas
        if source_type not in stats_by_type:
            stats_by_type[source_type] = {'count': 0, 'total_percent': 0}
        stats_by_type[source_type]['count'] += 1
        stats_by_type[source_type]['total_percent'] += percent
    
    # Resumo estatístico
    print("\n" + "="*80)
    print("RESUMO ESTATISTICO")
    print("="*80)
    
    for source_type, stats in stats_by_type.items():
        avg_percent = stats['total_percent'] / stats['count'] if stats['count'] > 0 else 0
        print(f"{source_type:<20}: {stats['count']:2d} colunas | Completude media: {avg_percent:6.2f}%")
    
    total_columns = len(template_columns)
    mapped_columns = sum(1 for a in column_analysis if a['source_type'] != 'VAZIA')
    unmapped_columns = total_columns - mapped_columns
    
    print(f"\nRESUMO GERAL:")
    print(f"  Total de colunas no template: {total_columns}")
    print(f"  Colunas com mapeamento: {mapped_columns}")
    print(f"  Colunas vazias (futuras fases): {unmapped_columns}")
    print(f"  Taxa de mapeamento: {(mapped_columns/total_columns)*100:.1f}%")
    
    # Identificar gargalos
    print(f"\n[GARGALOS] IDENTIFICADOS:")
    low_completude = [a for a in column_analysis if a['completude_percent'] < 50 and a['source_type'] != 'VAZIA']
    
    if low_completude:
        for analysis in low_completude:
            print(f"   [ALERTA] {analysis['template_column']}: {analysis['completude_percent']:.1f}% ({analysis['source_type']})")
    else:
        print("   [OK] Nenhum gargalo critico identificado (todas as colunas mapeadas >50%)")
    
    # Salvar relatório em arquivo
    print(f"\n[SAVE] Salvando relatorio detalhado...")
    
    df_report = pd.DataFrame(column_analysis)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"relatorio_completude_{timestamp}.xlsx"
    report_path = os.path.join(base_path, report_filename)
    
    df_report.to_excel(report_path, index=False)
    print(f"[OK] Relatorio salvo em: {report_path}")
    
    return column_analysis, stats_by_type

if __name__ == "__main__":
    try:
        analysis, stats = analisar_completude()
    except Exception as e:
        print(f"[ERRO] Erro durante analise: {str(e)}")
        import traceback
        traceback.print_exc()
