"""
DIAGNÓSTICO DE COMPLETUDE POR GCPJ - ANÁLISE GRANULAR

Este script analisa TODOS os registros da base primária, identificando para cada GCPJ:
- Quais colunas do template podem ser preenchidas
- Quais colunas ficarão vazias e por que motivo
- Fontes de dados disponíveis por registro
- Taxa de completude individual

CARACTERÍSTICAS:
✅ Processa TODOS os registros da base primária (sem limitação)
✅ Mostra progresso a cada N registros (padrão: 1000)
✅ Gera relatório detalhado com registros mais problemáticos
✅ Identifica motivos específicos de falha por coluna
✅ Exporta para Excel com múltiplas abas

USO:
    # Análise completa de todos os registros
    resultados, resumo = executar_diagnostico_por_gcpj()
    
    # Personalizar relatório e progresso
    resultados, resumo = executar_diagnostico_por_gcpj(
        tipo_relatorio='problematicos',  # 'problematicos', 'completos', 'todos'
        limite_relatorio=100,           # Top N no Excel
        progresso_a_cada=500           # Progresso a cada 500 registros
    )
"""

import pandas as pd
import os
import logging
from datetime import datetime
import numpy as np

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DiagnosticoCompletudePorGCPJ:
    def __init__(self, base_path="C:/desenvolvimento/migration_app"):
        self.base_path = base_path
        
        # Mapeamentos do config.py
        self.column_mappings = {
            'CÓD. INTERNO': 'GCPJ',
            'PROCESSO': 'PROCESSO',
            'PROCEDIMENTO': 'TIPO_ACAO',
            'NOME PARTE CONTRÁRIA PRINCIPAL': 'ENVOLVIDO',
            'CPF/CNPJ': 'CPF',
            'ORGANIZAÇÃO CLIENTE': 'REGIONAL',
            'TIPO DE OPERAÇÃO/CARTEIRA': 'CARTEIRA',
            'AGÊNCIA': 'AGENCIA',
            'CONTA': 'CONTA',
            'VARA': 'ORGAO_JULGADOR',
            'FORUM': 'ORGAO_JULGADOR',
            'COMARCA': 'COMARCA',
            'UF': 'UF',
            'GESTOR': 'GESTOR'
        }
        
        self.constant_values = {
            'ESCRITÓRIO': 'MOYA E LARA SOCIEDADE DE ADVOGADOS',
            'MONITORAMENTO': 'Não'
        }
        
        self.secondary_mappings = {
            'SEGMENTO DO CONTRATO': 'TIPO',
            'OPERAÇÃO': 'PROCADV_CONTRATO'
        }
        
    def carregar_dados(self):
        """Carrega os arquivos de dados"""
        try:
            # Template
            template_path = os.path.join(self.base_path, "template-banco-bradesco-sa.xlsx")
            self.template_df = pd.read_excel(template_path, sheet_name='Sheet')
            self.template_columns = list(self.template_df.columns)
            logger.info(f"Template carregado: {len(self.template_columns)} colunas")
            
            # Fonte Primária
            primary_path = os.path.join(self.base_path, "cópia-MOYA E LARA_BASE GCPJ ATIVOS - 07_04_2025.xlsx")
            self.primary_df = pd.read_excel(primary_path)
            logger.info(f"Fonte primária carregada: {len(self.primary_df)} registros, {len(self.primary_df.columns)} colunas")
            
            # Fonte Secundária
            secondary_path = os.path.join(self.base_path, "4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA _ABRIL_disp_24_04_2025.xlsx")
            self.secondary_df = pd.read_excel(secondary_path)
            logger.info(f"Fonte secundária carregada: {len(self.secondary_df)} registros, {len(self.secondary_df.columns)} colunas")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {str(e)}")
            return False
    
    def normalizar_gcpj(self, gcpj_value):
        """Normaliza valor GCPJ para comparação"""
        if pd.isna(gcpj_value):
            return None
        
        if isinstance(gcpj_value, (float, int)):
            return str(int(gcpj_value))
        else:
            return str(gcpj_value).strip()
    
    def gerar_chaves_gcpj(self, gcpj_value):
        """Gera chaves alternativas para mapeamento GCPJ"""
        if not gcpj_value:
            return []
            
        keys = [gcpj_value]
        
        # Ignorando os dois primeiros dígitos
        if len(gcpj_value) > 2:
            keys.append(gcpj_value[2:])
            
        # Variações de prefixo
        if gcpj_value.startswith('16'):
            keys.append('22' + gcpj_value[2:])
            keys.append('24' + gcpj_value[2:])
        elif gcpj_value.startswith('13'):
            keys.append('22' + gcpj_value[2:])
            keys.append('24' + gcpj_value[2:])
        elif gcpj_value.startswith('22'):
            keys.append('16' + gcpj_value[2:])
        elif gcpj_value.startswith('24'):
            keys.append('16' + gcpj_value[2:])
            
        return keys
    
    def criar_mapeamento_secundario(self):
        """Cria mapeamento da fonte secundária por GCPJ"""
        mapeamento = {}
        
        for idx, row in self.secondary_df.iterrows():
            if 'GCPJ' in row and pd.notna(row['GCPJ']):
                gcpj_normalizado = self.normalizar_gcpj(row['GCPJ'])
                if gcpj_normalizado:
                    chaves = self.gerar_chaves_gcpj(gcpj_normalizado)
                    
                    # Dados disponíveis para esse GCPJ
                    dados_disponiveis = {}
                    for template_col, source_col in self.secondary_mappings.items():
                        if source_col in row and pd.notna(row[source_col]):
                            dados_disponiveis[template_col] = row[source_col]
                    
                    # Adicionar para todas as chaves
                    for chave in chaves:
                        mapeamento[chave] = dados_disponiveis
        
        logger.info(f"Mapeamento secundário criado com {len(mapeamento)} chaves")
        return mapeamento
    
    def analisar_registro_gcpj(self, row_primaria, mapeamento_secundario):
        """Analisa completude de um registro específico por GCPJ"""
        gcpj_value = self.normalizar_gcpj(row_primaria.get('GCPJ'))
        if not gcpj_value:
            return None
        
        resultado = {
            'GCPJ': gcpj_value,
            'colunas_disponiveis': [],
            'colunas_faltantes': [],
            'detalhes_fonte': {},
            'taxa_completude': 0
        }
        
        colunas_preenchidas = set()
        
        # 1. Verificar colunas da fonte primária
        for template_col, source_col in self.column_mappings.items():
            if source_col in row_primaria and pd.notna(row_primaria[source_col]):
                colunas_preenchidas.add(template_col)
                resultado['detalhes_fonte'][template_col] = {
                    'fonte': 'Primária',
                    'coluna_origem': source_col,
                    'valor': str(row_primaria[source_col])[:50] + "..." if len(str(row_primaria[source_col])) > 50 else str(row_primaria[source_col]),
                    'disponivel': True
                }
            else:
                resultado['detalhes_fonte'][template_col] = {
                    'fonte': 'Primária',
                    'coluna_origem': source_col,
                    'disponivel': False,
                    'motivo': 'Dado não encontrado ou vazio na fonte primária'
                }
        
        # 2. Verificar colunas constantes
        for template_col, valor_constante in self.constant_values.items():
            colunas_preenchidas.add(template_col)
            resultado['detalhes_fonte'][template_col] = {
                'fonte': 'Constante',
                'valor': valor_constante,
                'disponivel': True
            }
        
        # 3. Verificar correspondência na fonte secundária
        chaves_gcpj = self.gerar_chaves_gcpj(gcpj_value)
        dados_secundarios = None
        
        for chave in chaves_gcpj:
            if chave in mapeamento_secundario:
                dados_secundarios = mapeamento_secundario[chave]
                break
        
        for template_col, source_col in self.secondary_mappings.items():
            if dados_secundarios and template_col in dados_secundarios:
                colunas_preenchidas.add(template_col)
                resultado['detalhes_fonte'][template_col] = {
                    'fonte': 'Secundária (via GCPJ)',
                    'coluna_origem': source_col,
                    'valor': str(dados_secundarios[template_col])[:50] + "..." if len(str(dados_secundarios[template_col])) > 50 else str(dados_secundarios[template_col]),
                    'chave_gcpj_usada': next((k for k in chaves_gcpj if k in mapeamento_secundario), None),
                    'disponivel': True
                }
            else:
                resultado['detalhes_fonte'][template_col] = {
                    'fonte': 'Secundária (via GCPJ)',
                    'coluna_origem': source_col,
                    'disponivel': False,
                    'motivo': 'GCPJ não encontrado na fonte secundária ou dado vazio',
                    'chaves_tentadas': chaves_gcpj
                }
        
        # 4. Identificar colunas do template que ficarão vazias
        colunas_template_set = set(self.template_columns)
        colunas_mapeadas = set(self.column_mappings.keys()) | set(self.secondary_mappings.keys()) | set(self.constant_values.keys())
        colunas_sem_mapeamento = colunas_template_set - colunas_mapeadas
        
        # Adicionar colunas sem mapeamento
        for col in colunas_sem_mapeamento:
            resultado['detalhes_fonte'][col] = {
                'fonte': 'Nenhuma',
                'disponivel': False,
                'motivo': 'Sem mapeamento definido - reservada para fases futuras'
            }
        
        # 5. Calcular listas finais
        resultado['colunas_disponiveis'] = sorted(list(colunas_preenchidas))
        resultado['colunas_faltantes'] = sorted([col for col in self.template_columns if col not in colunas_preenchidas])
        resultado['taxa_completude'] = (len(colunas_preenchidas) / len(self.template_columns)) * 100
        
        return resultado
    
    def gerar_diagnostico_completo(self, progresso_a_cada=1000):
        """Gera diagnóstico completo por GCPJ - SEMPRE processa TODOS os registros"""
        logger.info("Iniciando diagnóstico de completude por GCPJ...")
        
        if not self.carregar_dados():
            return None
        
        # Criar mapeamento da fonte secundária
        mapeamento_secundario = self.criar_mapeamento_secundario()
        
        # Analisar TODOS os registros da fonte primária
        resultados = []
        total_registros = len(self.primary_df)
        logger.info(f"Total de registros a processar: {total_registros:,}")
        
        for idx, row in self.primary_df.iterrows():
            # Mostrar progresso a cada N registros
            if (idx + 1) % progresso_a_cada == 0:
                percentual = ((idx + 1) / total_registros) * 100
                logger.info(f"Processando registro {idx + 1:,} de {total_registros:,} ({percentual:.1f}%)")
            
            resultado = self.analisar_registro_gcpj(row, mapeamento_secundario)
            if resultado:
                resultado['indice_original'] = idx
                resultados.append(resultado)
        
        logger.info(f"✅ Diagnóstico concluído para {len(resultados):,} registros ({total_registros:,} processados)")
        return resultados
    
    def gerar_relatorio_resumido(self, resultados):
        """Gera relatório resumido com estatísticas"""
        if not resultados:
            return None
        
        # Estatísticas gerais
        total_registros = len(resultados)
        taxas_completude = [r['taxa_completude'] for r in resultados]
        taxa_media = sum(taxas_completude) / len(taxas_completude)
        
        # Distribuição por faixas de completude
        faixas = {
            '90-100%': sum(1 for t in taxas_completude if t >= 90),
            '70-89%': sum(1 for t in taxas_completude if 70 <= t < 90),
            '50-69%': sum(1 for t in taxas_completude if 50 <= t < 70),
            '0-49%': sum(1 for t in taxas_completude if t < 50)
        }
        
        # Colunas mais problemáticas
        contador_faltantes = {}
        for resultado in resultados:
            for col in resultado['colunas_faltantes']:
                contador_faltantes[col] = contador_faltantes.get(col, 0) + 1
        
        colunas_mais_problematicas = sorted(contador_faltantes.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Problemas por fonte
        problemas_por_fonte = {'Primária': 0, 'Secundária (via GCPJ)': 0, 'Nenhuma': 0}
        for resultado in resultados:
            for col, detalhes in resultado['detalhes_fonte'].items():
                if not detalhes.get('disponivel', False):
                    fonte = detalhes.get('fonte', 'Desconhecida')
                    if fonte in problemas_por_fonte:
                        problemas_por_fonte[fonte] += 1
        
        resumo = {
            'total_registros': total_registros,
            'taxa_completude_media': round(taxa_media, 2),
            'distribuicao_faixas': faixas,
            'colunas_mais_problematicas': colunas_mais_problematicas,
            'problemas_por_fonte': problemas_por_fonte
        }
        
        return resumo
    
    def exportar_relatorio_detalhado(self, resultados, tipo='problematicos', limite=100):
        """Exporta relatório detalhado em Excel"""
        if not resultados:
            logger.error("Nenhum resultado para exportar")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Filtrar registros conforme tipo solicitado
        if tipo == 'problematicos':
            # Ordenar por menor taxa de completude
            resultados_filtrados = sorted(resultados, key=lambda x: x['taxa_completude'])[:limite]
            sufixo = f"problematicos_top{limite}"
        elif tipo == 'completos':
            # Ordenar por maior taxa de completude
            resultados_filtrados = sorted(resultados, key=lambda x: x['taxa_completude'], reverse=True)[:limite]
            sufixo = f"completos_top{limite}"
        else:
            # Todos os registros
            resultados_filtrados = resultados[:limite]
            sufixo = f"todos_top{limite}"
        
        # Preparar dados para Excel
        dados_resumo = []
        dados_detalhados = []
        
        for resultado in resultados_filtrados:
            # Dados resumo
            dados_resumo.append({
                'GCPJ': resultado['GCPJ'],
                'Taxa_Completude_%': round(resultado['taxa_completude'], 2),
                'Colunas_Disponíveis': len(resultado['colunas_disponiveis']),
                'Colunas_Faltantes': len(resultado['colunas_faltantes']),
                'Total_Colunas_Template': len(self.template_columns),
                'Lista_Colunas_Faltantes': '; '.join(resultado['colunas_faltantes'][:5]) + ('...' if len(resultado['colunas_faltantes']) > 5 else '')
            })
            
            # Dados detalhados (uma linha por coluna)
            for col, detalhes in resultado['detalhes_fonte'].items():
                dados_detalhados.append({
                    'GCPJ': resultado['GCPJ'],
                    'Coluna_Template': col,
                    'Fonte': detalhes.get('fonte', 'N/A'),
                    'Coluna_Origem': detalhes.get('coluna_origem', 'N/A'),
                    'Disponível': detalhes.get('disponivel', False),
                    'Valor': detalhes.get('valor', 'N/A'),
                    'Motivo_Falta': detalhes.get('motivo', 'N/A') if not detalhes.get('disponivel', False) else '',
                    'Chaves_GCPJ_Tentadas': '; '.join(detalhes.get('chaves_tentadas', [])) if detalhes.get('chaves_tentadas') else ''
                })
        
        # Criar DataFrames
        df_resumo = pd.DataFrame(dados_resumo)
        df_detalhados = pd.DataFrame(dados_detalhados)
        
        # Exportar para Excel com múltiplas abas
        arquivo_saida = os.path.join(self.base_path, f"diagnostico_gcpj_{sufixo}_{timestamp}.xlsx")
        
        with pd.ExcelWriter(arquivo_saida, engine='openpyxl') as writer:
            df_resumo.to_excel(writer, sheet_name='Resumo_por_GCPJ', index=False)
            df_detalhados.to_excel(writer, sheet_name='Detalhes_por_Coluna', index=False)
        
        logger.info(f"Relatório detalhado exportado para: {arquivo_saida}")
        return arquivo_saida
    
    def imprimir_resumo_executivo(self, resumo):
        """Imprime resumo executivo do diagnóstico"""
        if not resumo:
            print("Nenhum resumo disponível")
            return
        
        print(f"\n{'='*70}")
        print(f"DIAGNÓSTICO DE COMPLETUDE POR GCPJ - RESUMO EXECUTIVO")
        print(f"{'='*70}")
        
        print(f"📊 ESTATÍSTICAS GERAIS:")
        print(f"   Total de registros analisados: {resumo['total_registros']:,}")
        print(f"   Taxa de completude média: {resumo['taxa_completude_media']:.2f}%")
        
        print(f"\n📈 DISTRIBUIÇÃO POR FAIXAS DE COMPLETUDE:")
        for faixa, quantidade in resumo['distribuicao_faixas'].items():
            percentual = (quantidade / resumo['total_registros']) * 100
            print(f"   {faixa}: {quantidade:,} registros ({percentual:.1f}%)")
        
        print(f"\n🔴 TOP 10 COLUNAS MAIS PROBLEMÁTICAS:")
        for i, (coluna, quantidade) in enumerate(resumo['colunas_mais_problematicas'], 1):
            percentual = (quantidade / resumo['total_registros']) * 100
            print(f"   {i:2d}. {coluna:<35} {quantidade:,} registros ({percentual:.1f}%)")
        
        print(f"\n🔍 PROBLEMAS POR FONTE:")
        for fonte, quantidade in resumo['problemas_por_fonte'].items():
            print(f"   {fonte:<25} {quantidade:,} problemas")
        
        print(f"{'='*70}")


# Função principal para executar o diagnóstico
def executar_diagnostico_por_gcpj(tipo_relatorio='problematicos', limite_relatorio=50, progresso_a_cada=1000):
    """
    Função principal para executar o diagnóstico por GCPJ
    
    Args:
        tipo_relatorio: 'problematicos', 'completos' ou 'todos'
        limite_relatorio: Quantos registros incluir no relatório Excel
        progresso_a_cada: A cada quantos registros mostrar progresso
    """
    diagnostico = DiagnosticoCompletudePorGCPJ()
    
    print("Iniciando diagnóstico de completude por GCPJ...")
    print(f"🔍 Tipo de relatório: {tipo_relatorio}")
    print(f"📊 Registros no relatório Excel: {limite_relatorio}")
    print(f"⏱️ Progresso mostrado a cada: {progresso_a_cada:,} registros")
    print(f"📋 Processamento: TODOS os registros da base primária")
    
    # Gerar diagnóstico para TODOS os registros
    resultados = diagnostico.gerar_diagnostico_completo(progresso_a_cada)
    
    if resultados:
        # Gerar resumo
        resumo = diagnostico.gerar_relatorio_resumido(resultados)
        diagnostico.imprimir_resumo_executivo(resumo)
        
        # Exportar relatório detalhado
        arquivo_excel = diagnostico.exportar_relatorio_detalhado(resultados, tipo_relatorio, limite_relatorio)
        
        print(f"\n{'='*70}")
        print(f"✅ Diagnóstico por GCPJ concluído!")
        print(f"📄 Relatório detalhado salvo em: {arquivo_excel}")
        print(f"📋 Registros analisados: {len(resultados):,}")
        print(f"📊 Registros no relatório Excel: {min(limite_relatorio, len(resultados)):,}")
        print(f"{'='*70}")
        
        return resultados, resumo
    else:
        print("❌ Erro ao gerar diagnóstico por GCPJ")
        return None, None


# Executar se rodado diretamente
if __name__ == "__main__":
    # Análise completa - TODOS os registros da base primária
    resultados, resumo = executar_diagnostico_por_gcpj(
        tipo_relatorio='problematicos',  # Foca nos registros mais problemáticos
        limite_relatorio=100,  # Top 100 piores no Excel
        progresso_a_cada=1000  # Mostra progresso a cada 1000 registros
    )
