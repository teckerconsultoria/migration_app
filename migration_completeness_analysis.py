import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class MigrationCompletenessAnalyzer:
    """
    Analisador de completude para migração de dados entre planilhas especializadas.
    Esta classe identifica gaps de dados por GCPJ e por coluna.
    """
    
    def __init__(self):
        self.gcpj_list = []
        self.primary_df = None
        self.secondary_df = None
        self.juridico_df = None
        self.completeness_report = {}
        
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
        
    def load_data(self, gcpj_file, primary_file, secondary_file, juridico_file):
        """
        Carrega todos os arquivos necessários para a análise.
        """
        print("Carregando arquivos...")
        
        # Carregar lista de GCPJs
        gcpj_df = pd.read_csv(gcpj_file)
        self.gcpj_list = gcpj_df['GCPJ'].astype(str).tolist()
        print(f"✓ {len(self.gcpj_list)} GCPJs carregados")
        
        # Carregar planilha primária
        self.primary_df = pd.read_excel(primary_file)
        self.primary_df['GCPJ'] = self.primary_df['GCPJ'].astype(str)
        print(f"✓ Planilha primária: {len(self.primary_df)} registros")
        
        # Carregar planilha secundária
        self.secondary_df = pd.read_excel(secondary_file)
        self.secondary_df['GCPJ'] = self.secondary_df['GCPJ'].astype(str)
        print(f"✓ Planilha secundária: {len(self.secondary_df)} registros")
        
        # Carregar base jurídica
        self.juridico_df = pd.read_excel(juridico_file)
        self.juridico_df['GCPJ'] = self.juridico_df['GCPJ'].astype(str)
        print(f"✓ Base jurídica: {len(self.juridico_df)} registros")
        
    def analyze_gcpj_completeness(self):
        """
        Analisa a completude dos dados por GCPJ.
        """
        print("\nAnalisando completude por GCPJ...")
        
        gcpj_analysis = []
        
        for gcpj in self.gcpj_list:
            analysis = {
                'GCPJ': gcpj,
                'in_primary': False,
                'in_secondary': False,
                'in_juridico': False,
                'primary_records': 0,
                'secondary_records': 0,
                'juridico_records': 0,
                'completeness_score': 0,
                'missing_columns': []
            }
            
            # Verificar presença em cada base
            primary_data = self.primary_df[self.primary_df['GCPJ'] == gcpj]
            secondary_data = self.secondary_df[self.secondary_df['GCPJ'] == gcpj]
            juridico_data = self.juridico_df[self.juridico_df['GCPJ'] == gcpj]
            
            analysis['in_primary'] = len(primary_data) > 0
            analysis['in_secondary'] = len(secondary_data) > 0
            analysis['in_juridico'] = len(juridico_data) > 0
            
            analysis['primary_records'] = len(primary_data)
            analysis['secondary_records'] = len(secondary_data)
            analysis['juridico_records'] = len(juridico_data)
            
            # Calcular score de completude
            bases_present = sum([analysis['in_primary'], analysis['in_secondary'], analysis['in_juridico']])
            analysis['completeness_score'] = (bases_present / 3) * 100
            
            # Identificar colunas faltantes
            for col, (source, field) in self.column_mapping.items():
                if source == 'constant':
                    continue
                    
                if source == 'primary' and not analysis['in_primary']:
                    analysis['missing_columns'].append(col)
                elif source == 'secondary' and not analysis['in_secondary']:
                    analysis['missing_columns'].append(col)
                elif source == 'juridico' and not analysis['in_juridico']:
                    analysis['missing_columns'].append(col)
            
            gcpj_analysis.append(analysis)
        
        self.gcpj_completeness_df = pd.DataFrame(gcpj_analysis)
        return self.gcpj_completeness_df
    
    def analyze_column_completeness(self):
        """
        Analisa a completude por coluna do template.
        """
        print("\nAnalisando completude por coluna...")
        
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
                # Contar GCPJs que têm dados para esta coluna
                gcpj_with_data = set()
                
                if source == 'primary':
                    df_filtered = self.primary_df[self.primary_df['GCPJ'].isin(self.gcpj_list)]
                    if field in df_filtered.columns:
                        gcpj_with_data = set(df_filtered[df_filtered[field].notna()]['GCPJ'].unique())
                
                elif source == 'secondary':
                    df_filtered = self.secondary_df[self.secondary_df['GCPJ'].isin(self.gcpj_list)]
                    if field in df_filtered.columns:
                        gcpj_with_data = set(df_filtered[df_filtered[field].notna()]['GCPJ'].unique())
                
                elif source == 'juridico':
                    df_filtered = self.juridico_df[self.juridico_df['GCPJ'].isin(self.gcpj_list)]
                    if field in df_filtered.columns:
                        gcpj_with_data = set(df_filtered[df_filtered[field].notna()]['GCPJ'].unique())
                
                analysis['gcpj_with_data'] = len(gcpj_with_data)
                analysis['gcpj_without_data'] = len(self.gcpj_list) - len(gcpj_with_data)
                analysis['completeness_rate'] = (len(gcpj_with_data) / len(self.gcpj_list)) * 100
            
            column_analysis.append(analysis)
        
        self.column_completeness_df = pd.DataFrame(column_analysis)
        return self.column_completeness_df
    
    def generate_missing_data_lists(self):
        """
        Gera listas de GCPJs com dados faltantes.
        """
        print("\nGerando listas de GCPJs com dados faltantes...")
        
        # GCPJs sem dados na base primária
        gcpj_no_primary = self.gcpj_completeness_df[~self.gcpj_completeness_df['in_primary']]['GCPJ'].tolist()
        
        # GCPJs sem dados na base secundária
        gcpj_no_secondary = self.gcpj_completeness_df[~self.gcpj_completeness_df['in_secondary']]['GCPJ'].tolist()
        
        # GCPJs sem dados na base jurídica
        gcpj_no_juridico = self.gcpj_completeness_df[~self.gcpj_completeness_df['in_juridico']]['GCPJ'].tolist()
        
        # GCPJs sem dados em nenhuma base
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
        """
        Gera um dashboard HTML interativo com os resultados da análise.
        """
        print("\nGerando dashboard...")
        
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
        
        # Criar gráficos
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # 1. Distribuição de completude por GCPJ
        completeness_dist = self.gcpj_completeness_df['completeness_score'].value_counts().sort_index()
        axes[0, 0].bar(completeness_dist.index, completeness_dist.values, color='skyblue')
        axes[0, 0].set_title('Distribuição de Completude por GCPJ')
        axes[0, 0].set_xlabel('Score de Completude (%)')
        axes[0, 0].set_ylabel('Quantidade de GCPJs')
        
        # 2. Presença em cada base
        base_presence = {
            'Base Primária': self.gcpj_completeness_df['in_primary'].sum(),
            'Base Secundária': self.gcpj_completeness_df['in_secondary'].sum(),
            'Base Jurídica': self.gcpj_completeness_df['in_juridico'].sum()
        }
        axes[0, 1].bar(base_presence.keys(), base_presence.values(), color=['green', 'orange', 'blue'])
        axes[0, 1].set_title('GCPJs Presentes em Cada Base')
        axes[0, 1].set_ylabel('Quantidade de GCPJs')
        
        # 3. Completude por coluna
        col_data = self.column_completeness_df.sort_values('completeness_rate')
        axes[1, 0].barh(col_data['template_column'], col_data['completeness_rate'], color='lightcoral')
        axes[1, 0].set_title('Taxa de Completude por Coluna')
        axes[1, 0].set_xlabel('Taxa de Completude (%)')
        
        # 4. Top 10 colunas com mais dados faltantes
        top_missing = col_data.head(10)
        axes[1, 1].barh(top_missing['template_column'], top_missing['gcpj_without_data'], color='red')
        axes[1, 1].set_title('Top 10 Colunas com Mais Dados Faltantes')
        axes[1, 1].set_xlabel('Quantidade de GCPJs sem dados')
        
        plt.tight_layout()
        plt.savefig('completude_graficos.png', dpi=300, bbox_inches='tight')
        
        # Gerar HTML do dashboard
        html_content = self._generate_html_dashboard(gcpj_summary)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✓ Dashboard salvo em: {output_path}")
    
    def _generate_html_dashboard(self, summary):
        """
        Gera o conteúdo HTML do dashboard.
        """
        missing_lists = self.generate_missing_data_lists()
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dashboard de Completude - Migração de Dados</title>
            <meta charset="utf-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; }}
                .summary-card {{ background-color: white; padding: 20px; margin: 10px; border-radius: 5px; 
                                box-shadow: 0 2px 5px rgba(0,0,0,0.1); display: inline-block; }}
                .table-container {{ background-color: white; padding: 20px; margin: 20px 0; border-radius: 5px; 
                                   box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #3498db; color: white; }}
                .warning {{ background-color: #f39c12; color: white; padding: 10px; border-radius: 3px; }}
                .danger {{ background-color: #e74c3c; color: white; padding: 10px; border-radius: 3px; }}
                .success {{ background-color: #27ae60; color: white; padding: 10px; border-radius: 3px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Dashboard de Completude - Migração de Dados</h1>
                    <p>Análise realizada em: {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
                </div>
                
                <h2>Resumo Geral</h2>
                <div>
                    <div class="summary-card">
                        <h3>Total de GCPJs</h3>
                        <p style="font-size: 24px; font-weight: bold;">{summary['Total GCPJs']}</p>
                    </div>
                    <div class="summary-card success">
                        <h3>Dados Completos</h3>
                        <p style="font-size: 24px; font-weight: bold;">{summary['Com dados completos']}</p>
                    </div>
                    <div class="summary-card warning">
                        <h3>Dados Parciais</h3>
                        <p style="font-size: 24px; font-weight: bold;">{summary['Com dados parciais']}</p>
                    </div>
                    <div class="summary-card danger">
                        <h3>Sem Dados</h3>
                        <p style="font-size: 24px; font-weight: bold;">{summary['Sem dados']}</p>
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
                
                <div class="table-container">
                    <h2>Visualizações</h2>
                    <img src="completude_graficos.png" style="max-width: 100%;">
                </div>
            </div>
        </body>
        </html>
        """
        return html
    
    def _generate_column_table_rows(self):
        """
        Gera as linhas da tabela de completude por coluna.
        """
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
        """
        Exporta listas de GCPJs com dados faltantes para arquivos CSV.
        """
        Path(output_dir).mkdir(exist_ok=True)
        
        missing_lists = self.generate_missing_data_lists()
        
        for category, gcpj_list in missing_lists.items():
            if gcpj_list:
                df = pd.DataFrame({'GCPJ': gcpj_list})
                filename = f"{output_dir}/gcpj_{category}.csv"
                df.to_csv(filename, index=False)
                print(f"✓ Exportado: {filename} ({len(gcpj_list)} GCPJs)")


# Exemplo de uso
if __name__ == "__main__":
    analyzer = MigrationCompletenessAnalyzer()
    
    # Carregar os dados
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
    
    # Exportar listas de GCPJs faltantes
    analyzer.export_missing_gcpj_lists()
    
    # Salvar relatórios detalhados
    gcpj_completeness.to_csv('relatorio_completude_por_gcpj.csv', index=False)
    column_completeness.to_csv('relatorio_completude_por_coluna.csv', index=False)
    
    print("\n✓ Análise concluída! Verifique os arquivos gerados.")
