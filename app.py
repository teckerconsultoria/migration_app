from flask import Flask, render_template, request, redirect, url_for, flash
import os
import pandas as pd
from migration.processor import process_data
from migration.validators import validate_data

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('Nenhum arquivo selecionado')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('Nenhum arquivo selecionado')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            # Create upload folder if it doesn't exist
            os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
            
            filename = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(filename)
            
            # Process the uploaded file
            try:
                return redirect(url_for('process', filename=file.filename))
            except Exception as e:
                flash(f'Erro ao processar arquivo: {str(e)}')
                return redirect(request.url)
        else:
            flash('Tipo de arquivo n?o permitido. Use CSV, XLSX ou XLS.')
            return redirect(request.url)
    return render_template('upload.html')

@app.route('/process/<filename>')
def process(filename):
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    if filename.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)
    
    # Validate data
    validation_results = validate_data(df)
    
    if not validation_results['valid']:
        flash(f'Valida??o falhou: {validation_results["message"]}')
        return redirect(url_for('upload_file'))
    
    # Process data
    results = process_data(df)
    
    return render_template('processing.html', filename=filename, results=results)

@app.route('/results')
def results():
    return render_template('results.html')

if __name__ == '__main__':
    app.run(debug=True)
