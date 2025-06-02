# Data Migration Tool - Phase 1

## Overview

This application implements Phase 1 of the data migration process, transferring data from legacy Excel files to a new template format according to established mapping rules.

## Project Structure

```
migration_app/
│
├── app.py                  # Main Flask application
├── config.py               # Configuration settings
├── requirements.txt        # Project dependencies
│
├── static/                 # Static files (CSS, JS)
│   ├── css/
│   │   └── styles.css      # Custom styling for the application
│   └── js/
│       └── main.js         # JavaScript for frontend interactions
│
├── templates/              # HTML templates
│   ├── base.html           # Base template with layout and navigation
│   ├── index.html          # Home page with migration overview
│   ├── upload.html         # File upload interface
│   ├── processing.html     # Processing status page
│   └── results.html        # Results and statistics page
│
├── uploads/                # Directory for uploaded files
│
├── downloads/              # Directory for processed files
│
└── migration/              # Migration logic module
    ├── __init__.py         # Module initialization
    ├── processor.py        # Main data processing logic
    └── validators.py       # Input validation functions
```

## Installation

1. Create a virtual environment:
```bash
python -m venv venv
```

2. Activate the virtual environment:
```bash
# On Windows
venv\Scripts\activate

# On macOS/Linux
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Application

Start the Flask application:

```bash
python app.py
```

Then open your browser and navigate to `http://127.0.0.1:5000/`

## Migration Process

The application performs the following steps:

1. Upload the source files:
   - Primary source: cópiaMOYA E LARA_BASE GCPJ ATIVOS
   - Secondary source: 4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA
   - Template: templatebancobradescosa.xlsx

2. Process the migration according to the defined mapping:
   - Map direct columns from the primary source
   - Add constant values
   - Apply correspondence via GCPJ for partial columns

3. Download the resulting Excel file with all migrated data.

## Main Features

- Web-based interface for executing the migration
- Progress visualization during processing
- Detailed statistics about migration results
- Preservation of original columns alongside template columns
- Downloadable migration results

## Migration Mapping

The migration follows a specific column mapping defined in `config.py`:

- Direct mappings from primary source (14 columns)
- Constant values (2 columns)
- Secondary mappings via GCPJ correspondence (2 columns)

This results in 14 fully-filled columns, 4 partially-filled columns, and 14 columns left empty for future phases.
