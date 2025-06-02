import pandas as pd
import os

def validate_file_format(filepath, allowed_extensions=None):
    """Validate if the file has a supported extension"""
    if allowed_extensions is None:
        allowed_extensions = {'xlsx', 'xls'}
        
    ext = os.path.splitext(filepath)[1][1:].lower()
    return ext in allowed_extensions

def validate_primary_file(filepath):
    """Validate the primary source file structure"""
    try:
        df = pd.read_excel(filepath)
        
        # Check required columns
        required_columns = ['GCPJ', 'PROCESSO', 'TIPO_ACAO', 'ENVOLVIDO', 'CPF', 
                          'REGIONAL', 'CARTEIRA', 'AGENCIA', 'CONTA', 
                          'ORGAO_JULGADOR', 'COMARCA', 'UF', 'GESTOR']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return False, f"Missing columns in primary file: {', '.join(missing_columns)}"
        
        # Check if file has data
        if len(df) == 0:
            return False, "Primary file contains no data"
            
        return True, "Primary file is valid"
        
    except Exception as e:
        return False, f"Error validating primary file: {str(e)}"

def validate_secondary_file(filepath):
    """Validate the secondary source file structure"""
    try:
        df = pd.read_excel(filepath)
        
        # Check required columns
        required_columns = ['GCPJ', 'TIPO', 'PROCADV_CONTRATO']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return False, f"Missing columns in secondary file: {', '.join(missing_columns)}"
        
        # Check if file has data
        if len(df) == 0:
            return False, "Secondary file contains no data"
            
        return True, "Secondary file is valid"
        
    except Exception as e:
        return False, f"Error validating secondary file: {str(e)}"

def validate_template_file(filepath):
    """Validate the template file structure"""
    try:
        # Attempt to read the Sheet tab
        df = pd.read_excel(filepath, sheet_name='Sheet')
        
        # Check if file has columns
        if len(df.columns) == 0:
            return False, "Template file has no columns in Sheet tab"
            
        return True, "Template file is valid"
        
    except Exception as e:
        return False, f"Error validating template file: {str(e)}"
