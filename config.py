import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'hard-to-guess-string'
    UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
    DOWNLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads')
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max upload size
    ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
    
    # Migration configuration
    PRIMARY_FILE = 'primary.xlsx'
    SECONDARY_FILE = 'secondary.xlsx'
    TEMPLATE_FILE = 'template.xlsx'
    RESULT_FILE = 'migration_result.xlsx'
    
    # Column mappings
    COLUMN_MAPPINGS = {
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
    
    # Constant values
    CONSTANT_VALUES = {
        'ESCRITÓRIO': 'MOYA E LARA SOCIEDADE DE ADVOGADOS',
        'MONITORAMENTO': 'Não'
    }
    
    # Secondary file mappings (via GCPJ)
    SECONDARY_MAPPINGS = {
        'SEGMENTO DO CONTRATO': 'TIPO',
        'OPERAÇÃO': 'PROCADV_CONTRATO'
    }
