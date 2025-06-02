# Quick Start Guide - Data Migration Tool

## Prerequisites

- Python 3.8 or higher
- Excel files for migration:
  - Primary file: cópiaMOYA E LARA_BASE GCPJ ATIVOS.xlsx
  - Secondary file: 4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA.xlsx
  - Template file: templatebancobradescosa.xlsx

## Setup Steps

1. Open a Command Prompt and navigate to the project directory:
```
cd C:\desenvolvimento\migration_app
```

2. Create a virtual environment:
```
python -m venv venv
```

3. Activate the virtual environment:
```
venv\Scripts\activate
```

4. Install required dependencies:
```
pip install -r requirements.txt
```

## Running the Application

1. Start the Flask application:
```
python app.py
```

2. Open your web browser and navigate to:
```
http://127.0.0.1:5000/
```

3. Follow the on-screen instructions to:
   - Upload the required files
   - Process the migration
   - View the results
   - Download the migrated data

## Troubleshooting

If you encounter any issues:

1. Ensure all required packages are installed:
```
pip install -r requirements.txt
```

2. Check that the uploaded files have the expected columns:
   - Primary file must have: GCPJ, PROCESSO, TIPO_ACAO, etc.
   - Secondary file must have: GCPJ, TIPO, PROCADV_CONTRATO
   - Template file must have a "Sheet" tab

3. Verify the uploads and downloads directories exist and are writable:
```
mkdir uploads
mkdir downloads
```

4. Check the application logs for detailed error messages.

## Support

For assistance, please contact the development team.
