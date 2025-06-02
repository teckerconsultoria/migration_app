import pandas as pd

def validate_data(dataframe):
    """
    Valida um DataFrame para garantir que atende aos requisitos de migra??o.
    
    Args:
        dataframe (pd.DataFrame): DataFrame a ser validado
        
    Returns:
        dict: Resultado da valida??o com status e mensagens
    """
    result = {
        'valid': True,
        'message': 'Dados v?lidos',
        'details': []
    }
    
    # Verificar se o DataFrame est? vazio
    if dataframe.empty:
        result['valid'] = False
        result['message'] = 'O arquivo est? vazio'
        return result
    
    # Verificar colunas obrigat?rias
    required_columns = ['GCPJ', 'PROCESSO']  # Ajuste conforme necess?rio
    missing_columns = [col for col in required_columns if col not in dataframe.columns]
    
    if missing_columns:
        result['valid'] = False
        result['message'] = f"Colunas obrigat?rias ausentes: {', '.join(missing_columns)}"
        result['details'].append({
            'error_type': 'missing_columns',
            'columns': missing_columns
        })
        return result
    
    # Verificar tipos de dados
    # Exemplo: verificar se a coluna GCPJ cont?m apenas n?meros
    if 'GCPJ' in dataframe.columns:
        non_numeric = dataframe['GCPJ'].apply(lambda x: not str(x).isdigit()).sum()
        if non_numeric > 0:
            result['valid'] = False
            result['message'] = f"A coluna GCPJ cont?m {non_numeric} valores n?o num?ricos"
            result['details'].append({
                'error_type': 'invalid_data_type',
                'column': 'GCPJ',
                'issue': 'non_numeric',
                'count': non_numeric
            })
    
    # Verificar valores nulos em colunas cr?ticas
    critical_columns = ['GCPJ', 'PROCESSO']  # Ajuste conforme necess?rio
    null_issues = []
    
    for col in critical_columns:
        if col in dataframe.columns:
            null_count = dataframe[col].isnull().sum()
            if null_count > 0:
                null_issues.append({
                    'column': col,
                    'null_count': null_count
                })
    
    if null_issues:
        result['valid'] = False
        result['message'] = "Valores nulos em colunas cr?ticas"
        result['details'].append({
            'error_type': 'null_values',
            'issues': null_issues
        })
    
    return result
