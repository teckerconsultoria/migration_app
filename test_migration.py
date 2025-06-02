import os
import sys
from config import Config
from migration.processor import MigrationProcessor

def main():
    # Usar a configuração existente
    config = {
        'UPLOAD_FOLDER': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads'),
        'DOWNLOAD_FOLDER': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads'),
        'PRIMARY_FILE': 'primary.xlsx',
        'SECONDARY_FILE': 'secondary.xlsx',
        'TERTIARY_FILE': 'tertiary.xlsx',  # Adicionando o arquivo terciário
        'TEMPLATE_FILE': 'template.xlsx',
        'RESULT_FILE': 'migration_result.xlsx',
        'COLUMN_MAPPINGS': Config.COLUMN_MAPPINGS,
        'CONSTANT_VALUES': Config.CONSTANT_VALUES,
        'SECONDARY_MAPPINGS': Config.SECONDARY_MAPPINGS,
        'TERTIARY_MAPPINGS': Config.TERTIARY_MAPPINGS  # Adicionando os mapeamentos terciários
    }
    
    # Verificar se os arquivos existem
    for file_key in ['PRIMARY_FILE', 'SECONDARY_FILE', 'TERTIARY_FILE', 'TEMPLATE_FILE']:
        file_path = os.path.join(config['UPLOAD_FOLDER'], config[file_key])
        if os.path.exists(file_path):
            print(f"Arquivo {file_key} encontrado: {file_path}")
        else:
            print(f"AVISO: Arquivo {file_key} não encontrado: {file_path}")

    # Criar e executar o processador
    processor = MigrationProcessor(config)
    result = processor.process()
    
    print(f"Processamento concluído!")
    print(f"Arquivo gerado: {result['filepath']}")
    print(f"Estatísticas: {result['stats']}")

if __name__ == "__main__":
    main()
