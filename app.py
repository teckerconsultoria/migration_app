from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory
import os
import pandas as pd
from werkzeug.utils import secure_filename
from config import Config
from migration.processor import MigrationProcessor

app = Flask(__name__)
app.config.from_object(Config)

# Ensure upload and download folders exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['DOWNLOAD_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        # Check if required files are present
        if 'primary_file' not in request.files or 'secondary_file' not in request.files or 'template_file' not in request.files:
            flash('All required files must be provided')
            return redirect(request.url)
            
        primary_file = request.files['primary_file']
        secondary_file = request.files['secondary_file']
        template_file = request.files['template_file']
        
        # Validate files
        for file, name in [(primary_file, 'Primary'), (secondary_file, 'Secondary'), (template_file, 'Template')]:
            if file.filename == '':
                flash(f'{name} file not selected')
                return redirect(request.url)
                
            if not allowed_file(file.filename):
                flash(f'{name} file has an invalid format. Only Excel files are allowed.')
                return redirect(request.url)
        
        # Save files with standardized names
        primary_file.save(os.path.join(app.config['UPLOAD_FOLDER'], app.config['PRIMARY_FILE']))
        secondary_file.save(os.path.join(app.config['UPLOAD_FOLDER'], app.config['SECONDARY_FILE']))
        template_file.save(os.path.join(app.config['UPLOAD_FOLDER'], app.config['TEMPLATE_FILE']))
        
        # Redirect to processing page
        return redirect(url_for('processing'))
    
    return render_template('upload.html')

@app.route('/processing')
def processing():
    # Initialize the processor
    processor = MigrationProcessor(app.config)
    
    try:
        # Execute the migration
        result = processor.process()
        
        # Redirect to results page
        return redirect(url_for('results', filename=result['filename']))
        
    except Exception as e:
        flash(f'Error during processing: {str(e)}')
        return redirect(url_for('upload'))

@app.route('/results/<filename>')
def results(filename):
    # Load statistics for display
    processor = MigrationProcessor(app.config)
    filepath = os.path.join(app.config['DOWNLOAD_FOLDER'], filename)
    
    if not os.path.exists(filepath):
        flash('Result file not found')
        return redirect(url_for('index'))
    
    # Read the file to get statistics
    try:
        df = pd.read_excel(filepath)
        stats = processor.generate_statistics(df)
        return render_template('results.html', filename=filename, stats=stats)
    except Exception as e:
        flash(f'Error loading results: {str(e)}')
        return redirect(url_for('index'))

@app.route('/download/<filename>')
def download(filename):
    return send_from_directory(app.config['DOWNLOAD_FOLDER'], filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
