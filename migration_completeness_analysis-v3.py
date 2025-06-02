import pandas as pd
import numpy as np
from pathlib import Path
import json
import logging
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuração de logging com codificação UTF-8
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_completeness.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MigrationCompletenessAnalyzer:
    """
    Analisador de completude para migração de dados entre planilhas especializadas.
    Identifica gaps de dados por GCPJ e por coluna.
    """
    
    def __init__(self, weights=None):
        self.gcpj_list = []
        self.primary_df = None
        self.secondary_df = None
        self.juridico_df = None
        self.completeness_report = {}
        self.weights = weights or {'primary': 1/3, 'secondary': 1/3, 'juridico': 1/3}
        
        # Mapeamento de colunas do template para as fontes
        self.column_mapping = {
            'CÓD. INTERNO': ('primary', 'GCPJ'),
            'PROCESSO': ('primary', 'PROCESSO'),
            'PROCEDIMENTO': ('primary', 'TIPO_ACAO'),
            'NOME PARTE CONTRÁRIA PRINCIPAL': ('primary', 'ENVOLVIDO'),
            'CPF/CNPJ': ('primary', 'CPF'),
            'DEVEDOR SOLIDÁRIO 1': ('juridico', 'AVALISTA 1'),
            'CPF 1': ('juridico', 'CPF/CNPJ Avalista 1'),
            'DEVEDOR SOLIDÁRIO 2': ('juridico', 'AVALISTA 2'),
            'CPF 2': ('juridico', 'CPF/CNPJ Avalista 2'),
            'ORGANIZAÇÃO CLIENTE': ('constant', 'BANCO BRADESCO S.A'),
            'ESCRITÓRIO': ('constant', 'MOYA E LARA'),
            'TIPO DE OPERAÇÃO/CARTEIRA': ('primary', 'CARTEIRA'),
            'AGÊNCIA': ('primary', 'AGENCIA'),
            'CONTA': ('primary', 'CONTA'),
            'SEGMENTO DO CONTRATO': ('secondary', 'TIPO'),
            'VARA': ('primary', 'ORGAO_JULGADOR'),
            'COMARCA': ('primary', 'COMARCA'),
            'UF': ('primary', 'UF'),
            'GESTOR': ('primary', 'GESTOR'),
            'RESPONSÁVEL PROCESSO [ESCRITÓRIO]': ('primary', 'ADVOGADO_CONTRATADO')
        }
        
        # Colunas obrigatórias por base
        self.required_columns = {
            'primary': ['GCPJ', 'PROCESSO', 'TIPO_ACAO', 'ENVOLVIDO', 'CPF'],
            'secondary': ['GCPJ', 'TIPO'],
            'juridico': ['GCPJ', 'AVALISTA 1', 'CPF/CNPJ Avalista 1']
        }

    def _convert_to_json_serializable(self, obj):
        """Converte objetos para tipos serializáveis em JSON."""
        if isinstance(obj, (np.integer, np.floating)):
            return int(obj) if isinstance(obj, np.integer) else float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Series):
            return obj.tolist()
        return obj

    def validate_input_files(self, gcpj_file, primary_file, secondary_file, juridico_file):
        """Valida se os arquivos de entrada existem e contêm colunas esperadas."""
        logger.info("Validando arquivos de entrada...")
        for file_path, source in [(gcpj_file, 'gcpj'), (primary_file, 'primary'), 
                                (secondary_file, 'secondary'), (juridico_file, 'juridico')]:
            if not Path(file_path).exists():
                logger.error(f"Arquivo não encontrado: {file_path}")
                raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
            
            if source != 'gcpj':
                df = pd.read_excel(file_path, nrows=1)
                missing_cols = [col for col in self.required_columns[source] if col not in df.columns]
                if missing_cols:
                    logger.warning(f"Colunas faltantes em {file_path}: {missing_cols}. Continuando análise...")
                    for col, (src, field) in list(self.column_mapping.items()):
                        if src == source and field in missing_cols:
                            logger.warning(f"Ignorando mapeamento para {col} ({field})")
                            self.column_mapping[col] = ('constant', None)
        logger.info("OK Validação de arquivos concluída")

    def load_data(self, gcpj_file, primary_file, secondary_file, juridico_file):
        """Carrega todos os arquivos necessários para a análise."""
        logger.info("Carregando arquivos...")
        self.validate_input_files(gcpj_file, primary_file, secondary_file, juridico_file)
        
        # Carregar lista de GCPJs
        gcpj_df = pd.read_csv(gcpj_file)
        self.gcpj_list = gcpj_df['GCPJ'].astype(str).tolist()
        logger.info(f"OK {len(self.gcpj_list)} GCPJs carregados")
        
        # Carregar planilhas com índices e remover duplicatas
        self.primary_df = pd.read_excel(primary_file).set_index('GCPJ').drop_duplicates()
        self.primary_df.index = self.primary_df.index.astype(str)
        logger.info(f"OK Planilha primária: {len(self.primary_df)} registros após remoção de duplicatas")
        
        self.secondary_df = pd.read_excel(secondary_file).set_index('GCPJ').drop_duplicates()
        self.secondary_df.index = self.secondary_df.index.astype(str)
        logger.info(f"OK Planilha secundária: {len(self.secondary_df)} registros após remoção de duplicatas")
        
        self.juridico_df = pd.read_excel(juridico_file).set_index('GCPJ').drop_duplicates()
        self.juridico_df.index = self.juridico_df.index.astype(str)
        logger.info(f"OK Base jurídica: {len(self.juridico_df)} registros após remoção de duplicatas")
        
    def analyze_gcpj_completeness(self):
        """Analisa a completude dos dados por GCPJ."""
        logger.info("Analisando completude por GCPJ...")
        
        gcpj_analysis = []
        total_columns = len([c for c, (s, _) in self.column_mapping.items() if s != 'constant'])
        logger.info(f"Total de colunas não constantes: {total_columns}")
        
        if total_columns == 0:
            logger.error("Nenhuma coluna não constante encontrada no mapeamento!")
            raise ValueError("Nenhuma coluna não constante encontrada no mapeamento!")
        
        for gcpj in self.gcpj_list:
            analysis = {
                'GCPJ': gcpj,
                'in_primary': False,
                'in_secondary': False,
                'in_juridico': False,
                'primary_records': 0,
                'secondary_records': 0,
                'juridico_records': 0,
                'primary_has_duplicates': False,
                'secondary_has_duplicates': False,
                'juridico_has_duplicates': False,
                'completeness_score': 0.0,
                'missing_columns': [],
                'column_completeness': {}
            }
            
            # Verificar presença usando índices e tratar duplicatas
            primary_data = self.primary_df.loc[[gcpj]].head(1) if gcpj in self.primary_df.index else pd.DataFrame()
            secondary_data = self.secondary_df.loc[[gcpj]].head(1) if gcpj in self.secondary_df.index else pd.DataFrame()
            juridico_data = self.juridico_df.loc[[gcpj]].head(1) if gcpj in self.juridico_df.index else pd.DataFrame()
            
            analysis['in_primary'] = len(primary_data) > 0
            analysis['in_secondary'] = len(secondary_data) > 0
            analysis['in_juridico'] = len(juridico_data) > 0
            
            analysis['primary_records'] = len(self.primary_df.loc[[gcpj]]) if gcpj in self.primary_df.index else 0
            analysis['secondary_records'] = len(self.secondary_df.loc[[gcpj]]) if gcpj in self.secondary_df.index else 0
            analysis['juridico_records'] = len(self.juridico_df.loc[[gcpj]]) if gcpj in self.juridico_df.index else 0
            
            analysis['primary_has_duplicates'] = analysis['primary_records'] > 1
            analysis['secondary_has_duplicates'] = analysis['secondary_records'] > 1
            analysis['juridico_has_duplicates'] = analysis['juridico_records'] > 1
            
            # Verificar completude por coluna (somente colunas não constantes)
            filled_columns = 0
            for col, (source, field) in self.column_mapping.items():
                if source == 'constant':
                    analysis['column_completeness'][col] = True
                    # Não incrementamos filled_columns para colunas constantes
                else:
                    df = {'primary': primary_data, 'secondary': secondary_data, 'juridico': juridico_data}.get(source)
                    if df is not None and len(df) > 0 and field and field in df.columns:
                        has_data = df[field].notna().any()
                        analysis['column_completeness'][col] = has_data
                        if not has_data:
                            analysis['missing_columns'].append(col)
                        else:
                            filled_columns += 1
                    else:
                        analysis['column_completeness'][col] = False
                        analysis['missing_columns'].append(col)
            
            # Calcular score como porcentagem
            analysis['completeness_score'] = (filled_columns / total_columns) * 100
            analysis['completeness_score'] = round(min(max(analysis['completeness_score'], 0), 100), 2)
            logger.debug(f"GCPJ {gcpj}: filled_columns={filled_columns}, total_columns={total_columns}, score={analysis['completeness_score']}%")
            
            gcpj_analysis.append(analysis)
        
        self.gcpj_completeness_df = pd.DataFrame(gcpj_analysis)
        logger.info(f"OK Análise por GCPJ concluída: {len(self.gcpj_completeness_df)} registros")
        return self.gcpj_completeness_df
    
    def analyze_column_completeness(self):
        """Analisa a completude por coluna do template."""
        logger.info("Analisando completude por coluna...")
        
        column_analysis = []
        for template_col, (source, field) in self.column_mapping.items():
            analysis = {
                'template_column': template_col,
                'source': source,
                'source_field': field,
                'gcpj_with_data': 0,
                'gcpj_without_data': 0,
                'completeness_rate': 0
            }
            
            if source == 'constant':
                analysis['gcpj_with_data'] = len(self.gcpj_list)
                analysis['completeness_rate'] = 100
            else:
                gcpj_with_data = set()
                df = {'primary': self.primary_df, 'secondary': self.secondary_df, 
                      'juridico': self.juridico_df}.get(source)
                if df is not None and field and field in df.columns:
                    df_filtered = df[df.index.isin(self.gcpj_list)]
                    gcpj_with_data = set(df_filtered[df_filtered[field].notna()].index.unique())
                
                analysis['gcpj_with_data'] = len(gcpj_with_data)
                analysis['gcpj_without_data'] = len(self.gcpj_list) - len(gcpj_with_data)
                analysis['completeness_rate'] = (len(gcpj_with_data) / len(self.gcpj_list)) * 100
            
            column_analysis.append(analysis)
        
        self.column_completeness_df = pd.DataFrame(column_analysis)
        logger.info(f"OK Análise por coluna concluída: {len(self.column_completeness_df)} colunas")
        return self.column_completeness_df
    
    def generate_missing_data_lists(self):
        """Gera listas de GCPJs com dados faltantes."""
        logger.info("Gerando listas de GCPJs com dados faltantes...")
        
        gcpj_no_primary = self.gcpj_completeness_df[~self.gcpj_completeness_df['in_primary']]['GCPJ'].tolist()
        gcpj_no_secondary = self.gcpj_completeness_df[~self.gcpj_completeness_df['in_secondary']]['GCPJ'].tolist()
        gcpj_no_juridico = self.gcpj_completeness_df[~self.gcpj_completeness_df['in_juridico']]['GCPJ'].tolist()
        gcpj_no_data = self.gcpj_completeness_df[
            self.gcpj_completeness_df['completeness_score'] == 0
        ]['GCPJ'].tolist()
        
        return {
            'no_primary': gcpj_no_primary,
            'no_secondary': gcpj_no_secondary,
            'no_juridico': gcpj_no_juridico,
            'no_data_at_all': gcpj_no_data
        }
    
    def generate_dashboard(self, output_path='dashboard_completude.html'):
        """Gera um dashboard HTML interativo com os resultados da análise."""
        logger.info("Gerando dashboard...")
        
        # Preparar dados para visualização
        gcpj_summary = {
            'Total GCPJs': len(self.gcpj_list),
            'Com dados completos': len(self.gcpj_completeness_df[self.gcpj_completeness_df['completeness_score'] == 100]),
            'Com dados parciais': len(self.gcpj_completeness_df[
                (self.gcpj_completeness_df['completeness_score'] > 0) & 
                (self.gcpj_completeness_df['completeness_score'] < 100)
            ]),
            'Sem dados': len(self.gcpj_completeness_df[self.gcpj_completeness_df['completeness_score'] == 0])
        }
        
        # Distribuição de completude
        completeness_dist = self.gcpj_completeness_df['completeness_score'].value_counts().sort_index()
        chart_completeness_dist = {
            "type": "bar",
            "data": {
                "labels": [f"{x:.1f}%" for x in completeness_dist.index.tolist()],
                "datasets": [{
                    "label": "Quantidade de GCPJs",
                    "data": [self._convert_to_json_serializable(x) for x in completeness_dist.values.tolist()],
                    "backgroundColor": "rgba(54, 162, 235, 0.6)",
                    "borderColor": "rgba(54, 162, 235, 1)",
                    "borderWidth": 1
                }]
            },
            "options": {
                "scales": {
                    "y": {"title": {"display": True, "text": "Quantidade de GCPJs"}},
                    "x": {"title": {"display": True, "text": "Score de Completude (%)"}}
                },
                "plugins": {"title": {"display": True, "text": "Distribuição de Completude por GCPJ"}}
            }
        }
        
        # Presença nas bases
        base_presence = {
            'Base Primária': self.gcpj_completeness_df['in_primary'].sum(),
            'Base Secundária': self.gcpj_completeness_df['in_secondary'].sum(),
            'Base Jurídica': self.gcpj_completeness_df['in_juridico'].sum()
        }
        chart_base_presence = {
            "type": "bar",
            "data": {
                "labels": list(base_presence.keys()),
                "datasets": [{
                    "label": "Quantidade de GCPJs",
                    "data": [self._convert_to_json_serializable(x) for x in base_presence.values()],
                    "backgroundColor": ["rgba(46, 204, 113, 0.6)", "rgba(243, 156, 18, 0.6)", "rgba(52, 152, 219, 0.6)"],
                    "borderColor": ["rgba(46, 204, 113, 1)", "rgba(243, 156, 18, 1)", "rgba(52, 152, 219, 1)"],
                    "borderWidth": 1
                }]
            },
            "options": {
                "scales": {"y": {"title": {"display": True, "text": "Quantidade de GCPJs"}}},
                "plugins": {"title": {"display": True, "text": "GCPJs Presentes em Cada Base"}}
            }
        }
        
        # Completude por coluna
        col_data = self.column_completeness_df.sort_values('completeness_rate')
        chart_col_completeness = {
            "type": "bar",
            "data": {
                "labels": col_data['template_column'].tolist(),
                "datasets": [{
                    "label": "Taxa de Completude (%)",
                    "data": [self._convert_to_json_serializable(x) for x in col_data['completeness_rate'].tolist()],
                    "backgroundColor": "rgba(231, 76, 60, 0.6)",
                    "borderColor": "rgba(231, 76, 60, 1)",
                    "borderWidth": 1
                }]
            },
            "options": {
                "indexAxis": "y",
                "scales": {"x": {"title": {"display": True, "text": "Taxa de Completude (%)"}}},
                "plugins": {"title": {"display": True, "text": "Taxa de Completude por Coluna"}}
            }
        }
        
        # Top 10 colunas com mais dados faltantes
        top_missing = col_data.head(10)
        chart_top_missing = {
            "type": "bar",
            "data": {
                "labels": top_missing['template_column'].tolist(),
                "datasets": [{
                    "label": "GCPJs sem Dados",
                    "data": [self._convert_to_json_serializable(x) for x in top_missing['gcpj_without_data'].tolist()],
                    "backgroundColor": "rgba(231, 76, 60, 0.6)",
                    "borderColor": "rgba(231, 76, 60, 1)",
                    "borderWidth": 1
                }]
            },
            "options": {
                "indexAxis": "y",
                "scales": {"x": {"title": {"display": True, "text": "Quantidade de GCPJs sem Dados"}}},
                "plugins": {"title": {"display": True, "text": "Top 10 Colunas com Mais Dados Faltantes"}}
            }
        }
        
        # Gráfico de pizza para faixas de completude
        score_ranges = pd.cut(
            self.gcpj_completeness_df['completeness_score'],
            bins=[0, 30, 70, 100],
            labels=['0-30% (Alta Prioridade)', '30-70% (Média Prioridade)', '70-100% (Baixa Prioridade)'],
            include_lowest=True
        ).value_counts()
        chart_score_ranges = {
            "type": "pie",
            "data": {
                "labels": score_ranges.index.tolist(),
                "datasets": [{
                    "data": [self._convert_to_json_serializable(x) for x in score_ranges.values.tolist()],
                    "backgroundColor": ["rgba(231, 76, 60, 0.6)", "rgba(243, 156, 18, 0.6)", "rgba(46, 204, 113, 0.6)"],
                    "borderColor": ["rgba(231, 76, 60, 1)", "rgba(243, 156, 18, 1)", "rgba(46, 204, 113, 1)"],
                    "borderWidth": 1
                }]
            },
            "options": {
                "plugins": {"title": {"display": True, "text": "Distribuição de GCPJs por Faixa de Completude"}}
            }
        }
        
        # Box plot para completude
        chart_score_box = {
            "type": "boxplot",
            "data": {
                "labels": ["Completude"],
                "datasets": [{
                    "label": "Score de Completude (%)",
                    "data": [self.gcpj_completeness_df['completeness_score'].tolist()],
                    "backgroundColor": "rgba(54, 162, 235, 0.2)",
                    "borderColor": "rgba(54, 162, 235, 1)",
                    "borderWidth": 1
                }]
            },
            "options": {
                "scales": {"y": {"title": {"display": True, "text": "Score de Completude (%)"}}},
                "plugins": {"title": {"display": True, "text": "Distribuição Estatística do Score de Completude"}}
            }
        }
        
        # Gerar HTML do dashboard
        html_content = self._generate_html_dashboard(gcpj_summary, [
            chart_completeness_dist, chart_base_presence, chart_col_completeness,
            chart_top_missing, chart_score_ranges, chart_score_box
        ])
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"OK Dashboard salvo em: {output_path}")
    
    def _generate_html_dashboard(self, summary, charts):
        """Gera o conteúdo HTML do dashboard."""
        missing_lists = self.generate_missing_data_lists()
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dashboard de Completude - Migração de Dados</title>
            <meta charset="utf-8">
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/chartjs-chart-box-and-violin-plot"></script>
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background-color: #f0f4f8; }}
                .container {{ max-width: 1400px; margin: 0 auto; }}
                .header {{ background: linear-gradient(135deg, #2c3e50, #34495e); color: white; padding: 30px; border-radius: 10px; text-align: center; }}
                .header h1 {{ margin: 0; font-size: 2.5em; }}
                .header p {{ margin: 10px 0 0; font-size: 1.2em; }}
                .summary {{ display: flex; justify-content: space-between; flex-wrap: wrap; margin: 20px 0; }}
                .summary-card {{ background-color: white; padding: 20px; margin: 10px; border-radius: 10px; 
                                box-shadow: 0 4px 12px rgba(0,0,0,0.1); flex: 1; min-width: 200px; text-align: center; }}
                .summary-card h3 {{ margin: 0 0 10px; color: #34495e; }}
                .summary-card p {{ font-size: 1.8em; font-weight: bold; margin: 0; }}
                .success {{ background-color: #e8f5e9; }}
                .warning {{ background-color: #fff3e0; }}
                .danger {{ background-color: #ffebee; }}
                .table-container {{ background-color: white; padding: 20px; margin: 20px 0; border-radius: 10px; 
                                   box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e0e0e0; }}
                th {{ background-color: #3498db; color: white; }}
                .charts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; margin: 20px 0; }}
                canvas {{ background-color: white; padding: 15px; border-radius: 10px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Dashboard de Completude - Migração de Dados</h1>
                    <p>Análise realizada em: {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
                </div>
                
                <div class="summary">
                    <div class="summary-card">
                        <h3>Total de GCPJs</h3>
                        <p>{summary['Total GCPJs']}</p>
                    </div>
                    <div class="summary-card success">
                        <h3>Dados Completos</h3>
                        <p>{summary['Com dados completos']}</p>
                    </div>
                    <div class="summary-card warning">
                        <h3>Dados Parciais</h3>
                        <p>{summary['Com dados parciais']}</p>
                    </div>
                    <div class="summary-card danger">
                        <h3>Sem Dados</h3>
                        <p>{summary['Sem dados']}</p>
                    </div>
                </div>
                
                <div class="table-container">
                    <h2>GCPJs com Dados Faltantes</h2>
                    <table>
                        <tr>
                            <th>Categoria</th>
                            <th>Quantidade</th>
                            <th>Percentual</th>
                        </tr>
                        <tr>
                            <td>Sem dados na Base Primária</td>
                            <td>{len(missing_lists['no_primary'])}</td>
                            <td>{len(missing_lists['no_primary'])/len(self.gcpj_list)*100:.1f}%</td>
                        </tr>
                        <tr>
                            <td>Sem dados na Base Secundária</td>
                            <td>{len(missing_lists['no_secondary'])}</td>
                            <td>{len(missing_lists['no_secondary'])/len(self.gcpj_list)*100:.1f}%</td>
                        </tr>
                        <tr>
                            <td>Sem dados na Base Jurídica</td>
                            <td>{len(missing_lists['no_juridico'])}</td>
                            <td>{len(missing_lists['no_juridico'])/len(self.gcpj_list)*100:.1f}%</td>
                        </tr>
                        <tr style="background-color: #ffebee;">
                            <td><strong>Sem dados em nenhuma base</strong></td>
                            <td><strong>{len(missing_lists['no_data_at_all'])}</strong></td>
                            <td><strong>{len(missing_lists['no_data_at_all'])/len(self.gcpj_list)*100:.1f}%</strong></td>
                        </tr>
                    </table>
                </div>
                
                <div class="table-container">
                    <h2>Completude por Coluna do Template</h2>
                    <table>
                        <tr>
                            <th>Coluna</th>
                            <th>Fonte</th>
                            <th>GCPJs com Dados</th>
                            <th>GCPJs sem Dados</th>
                            <th>Taxa de Completude</th>
                        </tr>
                        {self._generate_column_table_rows()}
                    </table>
                </div>
                
                <div class="charts">
                    <canvas id="chartCompletenessDist"></canvas>
                    <canvas id="chartBasePresence"></canvas>
                    <canvas id="chartColCompleteness"></canvas>
                    <canvas id="chartTopMissing"></canvas>
                    <canvas id="chartScoreRanges"></canvas>
                    <canvas id="chartScoreBox"></canvas>
                </div>
            </div>
            <script>
                new Chart(document.getElementById('chartCompletenessDist'), {json.dumps(charts[0], default=self._convert_to_json_serializable)});
                new Chart(document.getElementById('chartBasePresence'), {json.dumps(charts[1], default=self._convert_to_json_serializable)});
                new Chart(document.getElementById('chartColCompleteness'), {json.dumps(charts[2], default=self._convert_to_json_serializable)});
                new Chart(document.getElementById('chartTopMissing'), {json.dumps(charts[3], default=self._convert_to_json_serializable)});
                new Chart(document.getElementById('chartScoreRanges'), {json.dumps(charts[4], default=self._convert_to_json_serializable)});
                new Chart(document.getElementById('chartScoreBox'), {json.dumps(charts[5], default=self._convert_to_json_serializable)});
            </script>
        </body>
        </html>
        """
        return html
    
    def _generate_column_table_rows(self):
        """Gera as linhas da tabela de completude por coluna."""
        rows = []
        for _, row in self.column_completeness_df.iterrows():
            color = ''
            if row['completeness_rate'] < 50:
                color = 'style="background-color: #ffebee;"'
            elif row['completeness_rate'] < 80:
                color = 'style="background-color: #fff3cd;"'
            
            rows.append(f"""
                <tr {color}>
                    <td>{row['template_column']}</td>
                    <td>{row['source']}</td>
                    <td>{row['gcpj_with_data']}</td>
                    <td>{row['gcpj_without_data']}</td>
                    <td>{row['completeness_rate']:.1f}%</td>
                </tr>
            """)
        return '\n'.join(rows)
    
    def export_missing_gcpj_lists(self, output_dir='missing_gcpj_lists'):
        """Exporta listas de GCPJs com dados faltantes para arquivos CSV."""
        logger.info("Exportando listas de GCPJs com dados faltantes...")
        Path(output_dir).mkdir(exist_ok=True)
        
        missing_lists = self.generate_missing_data_lists()
        for category, gcpj_list in missing_lists.items():
            if gcpj_list:
                df = pd.DataFrame({'GCPJ': gcpj_list})
                filename = f"{output_dir}/gcpj_{category}.csv"
                df.to_csv(filename, index=False)
                logger.info(f"OK Exportado: {filename} ({len(gcpj_list)} GCPJs)")
    
    def export_gcpj_details(self, output_file='gcpj_missing_columns.csv'):
        """Exporta um relatório de colunas faltantes por GCPJ, com priorização para saneamento."""
        logger.info("Exportando relatório de colunas faltantes por GCPJ...")
        details = []
        for _, row in self.gcpj_completeness_df.iterrows():
            gcpj = row['GCPJ']
            completeness_score = row['completeness_score']
            missing_columns = row['missing_columns']
            
            # Definir prioridade com base no completeness_score
            if completeness_score < 30:
                priority = 'Alta'
            elif completeness_score < 70:
                priority = 'Média'
            else:
                priority = 'Baixa'
            
            detail = {
                'GCPJ': gcpj,
                'completeness_score': completeness_score,
                'missing_columns': ';'.join(missing_columns) if missing_columns else 'Nenhuma',
                'priority': priority
            }
            details.append(detail)
        
        # Criar DataFrame e ordenar por completeness_score
        df = pd.DataFrame(details)
        df = df.sort_values(by='completeness_score', ascending=True)
        df.to_csv(output_file, index=False)
        logger.info(f"OK Exportado: {output_file} ({len(df)} registros)")

# Exemplo de uso
if __name__ == "__main__":
    analyzer = MigrationCompletenessAnalyzer(weights={'primary': 0.5, 'secondary': 0.3, 'juridico': 0.2})
    
    # Carrega os dados
    try:
        analyzer.load_data(
            gcpj_file='listagcpjnão_encontrados_octopus.csv',
            primary_file='cópiaMOYA E LARA_BASE GCPJ ATIVOS  07_04_2025.xlsx',
            secondary_file='4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA _ABRIL_disp_24_04_2025.xlsx',
            juridico_file='BASE JURIDICO_CPJ3C  07_04_2025_dados avalistas.xlsx'
        )
        
        # Executar análises
        gcpj_completeness = analyzer.analyze_gcpj_completeness()
        column_completeness = analyzer.analyze_column_completeness()
        
        # Gerar dashboard
        analyzer.generate_dashboard()
        
        # Exportar listas e detalhes
        analyzer.export_missing_gcpj_lists()
        analyzer.export_gcpj_details()
        
        # Salvar relatórios detalhados
        gcpj_completeness.to_csv('relatorio_completude_gcpj.csv', index=False)
        column_completeness.to_csv('relatorio_completude_coluna.csv', index=False)
        
        logger.info("OK Análise concluída! Verifique os arquivos gerados.")
    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")