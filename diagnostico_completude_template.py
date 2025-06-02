import pandas as pd
import os
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DiagnosticoCompletude:
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
            logger.info(f"Template carregado: {len(self.template_df.columns)} colunas")
            
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
    
    def analisar_completude_primaria(self):
        """Analisa completude das colunas mapeadas da fonte primária"""
        resultado = {}
        
        for template_col, source_col in self.column_mappings.items():
            if source_col in self.primary_df.columns:
                total_registros = len(self.primary_df)
                registros_preenchidos = self.primary_df[source_col].notna().sum()
                taxa_completude = (registros_preenchidos / total_registros) * 100
                
                # Analisar qualidade dos dados
                valores_unicos = self.primary_df[source_col].nunique()
                valores_vazios = self.primary_df[source_col].isna().sum()
                
                resultado[template_col] = {
                    'fonte': 'Primária',
                    'coluna_origem': source_col,
                    'total_registros': total_registros,
                    'registros_preenchidos': int(registros_preenchidos),
                    'taxa_completude': round(taxa_completude, 2),
                    'valores_unicos': int(valores_unicos),
                    'valores_vazios': int(valores_vazios),
                    'disponivel': True
                }
            else:
                resultado[template_col] = {
                    'fonte': 'Primária',
                    'coluna_origem': source_col,
                    'disponivel': False,
                    'erro': f'Coluna {source_col} não encontrada na fonte primária'
                }
                
        return resultado
    
    def analisar_completude_secundaria(self):
        """Analisa completude das colunas mapeadas da fonte secundária via GCPJ"""
        resultado = {}
        
        # Primeiro, criar mapeamento GCPJ da fonte secundária
        gcpj_mapping = {}
        for idx, row in self.secondary_df.iterrows():
            if 'GCPJ' in row and pd.notna(row['GCPJ']):
                gcpj_value = str(int(row['GCPJ'])) if isinstance(row['GCPJ'], (float, int)) else str(row['GCPJ']).strip()
                gcpj_mapping[gcpj_value] = row
        
        logger.info(f"Mapeamento GCPJ criado com {len(gcpj_mapping)} registros da fonte secundária")
        
        # Analisar cada coluna do mapeamento secundário
        for template_col, source_col in self.secondary_mappings.items():
            if source_col in self.secondary_df.columns:
                # Contar quantos registros da fonte primária têm correspondência na secundária
                correspondencias = 0
                total_primary = len(self.primary_df)
                
                for idx, row in self.primary_df.iterrows():
                    if 'GCPJ' in row and pd.notna(row['GCPJ']):
                        gcpj_primary = str(int(row['GCPJ'])) if isinstance(row['GCPJ'], (float, int)) else str(row['GCPJ']).strip()
                        
                        if gcpj_primary in gcpj_mapping:
                            secondary_row = gcpj_mapping[gcpj_primary]
                            if pd.notna(secondary_row[source_col]):
                                correspondencias += 1
                
                taxa_correspondencia = (correspondencias / total_primary) * 100 if total_primary > 0 else 0
                
                # Estatísticas da coluna na fonte secundária
                valores_preenchidos_sec = self.secondary_df[source_col].notna().sum()
                total_sec = len(self.secondary_df)
                taxa_completude_sec = (valores_preenchidos_sec / total_sec) * 100 if total_sec > 0 else 0
                
                resultado[template_col] = {
                    'fonte': 'Secundária (via GCPJ)',
                    'coluna_origem': source_col,
                    'total_registros_primarios': total_primary,
                    'correspondencias_encontradas': int(correspondencias),
                    'taxa_correspondencia': round(taxa_correspondencia, 2),
                    'completude_fonte_secundaria': round(taxa_completude_sec, 2),
                    'valores_unicos_secundaria': int(self.secondary_df[source_col].nunique()),
                    'disponivel': True
                }
            else:
                resultado[template_col] = {
                    'fonte': 'Secundária (via GCPJ)',
                    'coluna_origem': source_col,
                    'disponivel': False,
                    'erro': f'Coluna {source_col} não encontrada na fonte secundária'
                }
                
        return resultado
    
    def analisar_constantes(self):
        """Analisa colunas com valores constantes"""
        resultado = {}
        
        for template_col, valor_constante in self.constant_values.items():
            resultado[template_col] = {
                'fonte': 'Constante',
                'valor_constante': valor_constante,
                'taxa_completude': 100.0,
                'disponivel': True
            }
            
        return resultado
    
    def identificar_colunas_vazias(self):
        """Identifica colunas do template que ficarão vazias"""
        colunas_mapeadas = set()
        colunas_mapeadas.update(self.column_mappings.keys())
        colunas_mapeadas.update(self.secondary_mappings.keys())
        colunas_mapeadas.update(self.constant_values.keys())
        
        colunas_template = set(self.template_df.columns)
        colunas_vazias = colunas_template - colunas_mapeadas
        
        resultado = {}
        for col in colunas_vazias:
            resultado[col] = {
                'fonte': 'Nenhuma (vazia para fases futuras)',
                'taxa_completude': 0.0,
                'disponivel': False,
                'motivo': 'Sem mapeamento definido - reservada para fases futuras'
            }
            
        return resultado
    
    def gerar_relatorio_completo(self):
        """Gera relatório completo de diagnóstico"""
        logger.info("Iniciando diagnóstico de completude...")
        
        if not self.carregar_dados():
            return None
        
        # Analisar cada tipo de fonte
        primarias = self.analisar_completude_primaria()
        secundarias = self.analisar_completude_secundaria()
        constantes = self.analisar_constantes()
        vazias = self.identificar_colunas_vazias()
        
        # Consolidar resultados
        relatorio = {}
        relatorio.update(primarias)
        relatorio.update(secundarias)
        relatorio.update(constantes)
        relatorio.update(vazias)
        
        return relatorio
    
    def exportar_relatorio(self, relatorio, formato='excel'):
        """Exporta o relatório em diferentes formatos"""
        if not relatorio:
            logger.error("Nenhum relatório para exportar")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Preparar dados para DataFrame
        dados_relatorio = []
        
        for template_col, dados in relatorio.items():
            linha = {
                'Coluna_Template': template_col,
                'Fonte': dados.get('fonte', 'N/A'),
                'Coluna_Origem': dados.get('coluna_origem', 'N/A'),
                'Taxa_Completude_%': dados.get('taxa_completude', 0),
                'Disponivel': dados.get('disponivel', False),
                'Observacoes': dados.get('erro', dados.get('motivo', ''))
            }
            
            # Adicionar campos específicos conforme a fonte
            if dados.get('fonte') == 'Primária':
                linha.update({
                    'Total_Registros': dados.get('total_registros', 0),
                    'Registros_Preenchidos': dados.get('registros_preenchidos', 0),
                    'Valores_Unicos': dados.get('valores_unicos', 0),
                    'Valores_Vazios': dados.get('valores_vazios', 0)
                })
            elif 'Secundária' in dados.get('fonte', ''):
                linha.update({
                    'Total_Registros': dados.get('total_registros_primarios', 0),
                    'Correspondencias_GCPJ': dados.get('correspondencias_encontradas', 0),
                    'Taxa_Correspondencia_%': dados.get('taxa_correspondencia', 0),
                    'Completude_Fonte_Sec_%': dados.get('completude_fonte_secundaria', 0)
                })
            elif dados.get('fonte') == 'Constante':
                linha.update({
                    'Valor_Constante': dados.get('valor_constante', '')
                })
            
            dados_relatorio.append(linha)
        
        # Criar DataFrame
        df_relatorio = pd.DataFrame(dados_relatorio)
        
        # Ordenar por taxa de completude (decrescente)
        df_relatorio = df_relatorio.sort_values('Taxa_Completude_%', ascending=False)
        
        # Exportar
        if formato == 'excel':
            arquivo_saida = os.path.join(self.base_path, f"diagnostico_completude_{timestamp}.xlsx")
            df_relatorio.to_excel(arquivo_saida, index=False)
            logger.info(f"Relatório exportado para: {arquivo_saida}")
            return arquivo_saida
        
        return df_relatorio
    
    def imprimir_resumo(self, relatorio):
        """Imprime resumo executivo do diagnóstico"""
        if not relatorio:
            print("Nenhum relatório disponível")
            return
        
        total_colunas = len(relatorio)
        colunas_100 = sum(1 for dados in relatorio.values() if dados.get('taxa_completude', 0) == 100)
        colunas_parciais = sum(1 for dados in relatorio.values() if 0 < dados.get('taxa_completude', 0) < 100)
        colunas_vazias = sum(1 for dados in relatorio.values() if dados.get('taxa_completude', 0) == 0)
        
        print(f"\n{'='*60}")
        print(f"RESUMO EXECUTIVO - DIAGNÓSTICO DE COMPLETUDE")
        print(f"{'='*60}")
        print(f"Total de colunas no template: {total_colunas}")
        print(f"Colunas 100% completas: {colunas_100} ({colunas_100/total_colunas*100:.1f}%)")
        print(f"Colunas parcialmente completas: {colunas_parciais} ({colunas_parciais/total_colunas*100:.1f}%)")
        print(f"Colunas vazias: {colunas_vazias} ({colunas_vazias/total_colunas*100:.1f}%)")
        
        print(f"\n{'='*60}")
        print(f"DETALHAMENTO POR COLUNA")
        print(f"{'='*60}")
        
        # Agrupar por fonte
        fontes = {}
        for col, dados in relatorio.items():
            fonte = dados.get('fonte', 'N/A')
            if fonte not in fontes:
                fontes[fonte] = []
            fontes[fonte].append((col, dados))
        
        for fonte, colunas in fontes.items():
            print(f"\n[{fonte.upper()}]")
            print("-" * 40)
            for col, dados in sorted(colunas, key=lambda x: x[1].get('taxa_completude', 0), reverse=True):
                taxa = dados.get('taxa_completude', 0)
                status = "✅" if taxa == 100 else "⚠️" if taxa > 0 else "❌"
                print(f"{status} {col:<35} {taxa:>6.1f}%")


# Função principal para executar o diagnóstico
def executar_diagnostico():
    """Função principal para executar o diagnóstico completo"""
    diagnostico = DiagnosticoCompletude()
    
    print("Iniciando diagnóstico de completude por coluna do template...")
    relatorio = diagnostico.gerar_relatorio_completo()
    
    if relatorio:
        # Imprimir resumo
        diagnostico.imprimir_resumo(relatorio)
        
        # Exportar relatório
        arquivo_excel = diagnostico.exportar_relatorio(relatorio, formato='excel')
        
        print(f"\n{'='*60}")
        print(f"Diagnóstico concluído!")
        print(f"Relatório detalhado salvo em: {arquivo_excel}")
        print(f"{'='*60}")
        
        return relatorio
    else:
        print("Erro ao gerar relatório de diagnóstico")
        return None


# Executar se rodado diretamente
if __name__ == "__main__":
    relatorio = executar_diagnostico()
