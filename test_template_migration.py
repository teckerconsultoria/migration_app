import unittest
import os
import pandas as pd
import numpy as np
from config import Config
from migration.processor import MigrationProcessor
import shutil
import tempfile

class TestMigrationProcessor(unittest.TestCase):
    """Testes para a classe MigrationProcessor"""
    
    def setUp(self):
        """Configuração para os testes"""
        # Criar diretórios temporários para uploads e downloads
        self.temp_dir = tempfile.mkdtemp()
        self.upload_dir = os.path.join(self.temp_dir, 'uploads')
        self.download_dir = os.path.join(self.temp_dir, 'downloads')
        os.makedirs(self.upload_dir, exist_ok=True)
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Configuração de teste
        self.config = {
            'UPLOAD_FOLDER': self.upload_dir,
            'DOWNLOAD_FOLDER': self.download_dir,
            'PRIMARY_FILE': 'primary.xlsx',
            'SECONDARY_FILE': 'secondary.xlsx',
            'TEMPLATE_FILE': 'template.xlsx',
            'RESULT_FILE': 'result.xlsx',
            'COLUMN_MAPPINGS': {
                'COL1': 'source_col1',
                'COL2': 'source_col2',
                'CÓD. INTERNO': 'GCPJ'  # Mapeando GCPJ para CÓD. INTERNO
            },
            'CONSTANT_VALUES': {
                'COL3': 'Valor Constante'
            },
            'SECONDARY_MAPPINGS': {
                'COL4': 'sec_col1'
            }
        }
        
        # Criar arquivos de teste
        # 1. Arquivo primário
        primary_data = pd.DataFrame({
            'source_col1': ['A1', 'A2', 'A3'],
            'source_col2': ['B1', 'B2', 'B3'],
            'GCPJ': [123, 456, 789]  # Esta coluna será mapeada para 'CÓD. INTERNO'
        })
        primary_data.to_excel(os.path.join(self.upload_dir, self.config['PRIMARY_FILE']), index=False)
        
        # 2. Arquivo secundário
        secondary_data = pd.DataFrame({
            'GCPJ': [123, 456],
            'sec_col1': ['X1', 'X2']
        })
        secondary_data.to_excel(os.path.join(self.upload_dir, self.config['SECONDARY_FILE']), index=False)
        
        # 3. Arquivo de template
        template_data = pd.DataFrame(columns=['CÓD. INTERNO', 'COL1', 'COL2', 'COL3', 'COL4', 'COL5_VAZIA', 'COL6_VAZIA'])
        template_data.to_excel(os.path.join(self.upload_dir, self.config['TEMPLATE_FILE']), sheet_name='Sheet', index=False)
        
        # Inicializar o processador
        self.processor = MigrationProcessor(self.config)
        
    def tearDown(self):
        """Limpeza após os testes"""
        shutil.rmtree(self.temp_dir)
        
    def test_template_structure_preserved(self):
        """Testa se a estrutura do template é preservada no resultado"""
        # Executar a migração
        result = self.processor.process()
        
        # Verificar se o arquivo de resultado foi criado
        self.assertTrue(os.path.exists(result['filepath']))
        
        # Carregar o resultado e o template
        result_df = pd.read_excel(result['filepath'])
        template_df = pd.read_excel(os.path.join(self.upload_dir, self.config['TEMPLATE_FILE']), sheet_name='Sheet')
        
        # Verificar se as colunas do resultado são exatamente as mesmas do template
        self.assertEqual(list(result_df.columns), list(template_df.columns))
        
        # Verificar se o número de colunas é o mesmo
        self.assertEqual(len(result_df.columns), len(template_df.columns))
        
        # Verificar se as colunas vazias no template estão presentes no resultado
        for col in ['COL5_VAZIA', 'COL6_VAZIA']:
            self.assertIn(col, result_df.columns)
            # Verificar se as colunas estão vazias (contêm NaN)
            self.assertTrue(result_df[col].isna().all())
        
    def test_data_mapping(self):
        """Testa se os dados são corretamente mapeados dos arquivos fonte"""
        # Executar a migração
        result = self.processor.process()
        
        # Carregar o resultado
        result_df = pd.read_excel(result['filepath'])
        
        # Verificar mapeamento direto
        self.assertEqual(result_df['COL1'].tolist(), ['A1', 'A2', 'A3'])
        self.assertEqual(result_df['COL2'].tolist(), ['B1', 'B2', 'B3'])
        self.assertEqual(result_df['CÓD. INTERNO'].tolist(), [123, 456, 789])
        
        # Verificar valores constantes
        self.assertTrue((result_df['COL3'] == 'Valor Constante').all())
        
        # Verificar mapeamento secundário
        # Primeiro registro (GCPJ=123) deve ter o valor 'X1'
        self.assertEqual(result_df.iloc[0]['COL4'], 'X1')
        # Segundo registro (GCPJ=456) deve ter o valor 'X2'
        self.assertEqual(result_df.iloc[1]['COL4'], 'X2')
        # Terceiro registro (GCPJ=789) não está no secundário, deve ser NaN
        self.assertTrue(pd.isna(result_df.iloc[2]['COL4']))
        
    def test_statistics_generation(self):
        """Testa a geração de estatísticas"""
        # Executar a migração
        result = self.processor.process()
        
        # Carregar o resultado
        result_df = pd.read_excel(result['filepath'])
        
        # Gerar estatísticas
        stats = self.processor.generate_statistics(result_df)
        
        # Verificar estatísticas
        self.assertEqual(stats['total_rows'], 3)
        self.assertEqual(stats['fully_filled_columns'], 4)  # CÓD. INTERNO, COL1, COL2, COL3 estão 100% preenchidas
        self.assertEqual(stats['partially_filled_columns'], 1)  # COL4 está parcialmente preenchida
        self.assertEqual(stats['empty_columns'], 2)  # COL5_VAZIA, COL6_VAZIA estão vazias
        
        # Verificar porcentagem total de sucesso
        self.assertGreater(stats['success_percentage'], 0)
        
        # Verificar estatísticas por coluna
        for col in ['CÓD. INTERNO', 'COL1', 'COL2', 'COL3']:
            self.assertEqual(stats['column_stats'][col]['percentage'], 100)
        
        # A coluna COL4 deve ter 2/3 de preenchimento (66.67%)
        self.assertAlmostEqual(stats['column_stats']['COL4']['percentage'], 66.67, places=2)
        
        # As colunas vazias devem ter 0% de preenchimento
        for col in ['COL5_VAZIA', 'COL6_VAZIA']:
            self.assertEqual(stats['column_stats'][col]['percentage'], 0)

if __name__ == '__main__':
    unittest.main()
