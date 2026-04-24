import os
import uuid
import io
import tempfile
import importlib
import atexit
import time
from datetime import datetime
from flask import Flask, request, render_template, redirect, url_for, flash, send_file, session, jsonify
import pandas as pd
import base64
import hashlib

# Plotly for interactive charts
try:
    import plotly.express as px
    import plotly.io as pio
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except Exception:
    PLOTLY_AVAILABLE = False
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import seaborn as sns

from werkzeug.utils import secure_filename

# PDF generation
try:
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle
    from reportlab.lib.units import inch
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

app = Flask(__name__)
# Use environment variable for secret in production
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET', 'change-me')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PLOTS_DIR = os.path.join(BASE_DIR, 'static', 'plots')
TEMP_DIR = os.path.join(BASE_DIR, '.temp_data')
os.makedirs(PLOTS_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

def cleanup_old_plots():
    """Clean up plot files older than 1 hour"""
    now = time.time()
    for f in os.listdir(PLOTS_DIR):
        fpath = os.path.join(PLOTS_DIR, f)
        if os.path.isfile(fpath) and f.endswith('.png'):
            # Remove files older than 1 hour
            if os.stat(fpath).st_mtime < now - 3600:
                try:
                    os.remove(fpath)
                    app.logger.info(f"Cleaned up old plot: {f}")
                except Exception as e:
                    app.logger.warning(f"Failed to clean up plot {f}: {e}")

# Schedule cleanup of old plot files on app exit
atexit.register(cleanup_old_plots)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_file_hash(file_content):
    """Generate hash for file content to use as identifier"""
    return hashlib.md5(file_content).hexdigest()[:8]

def safe_read_file(file, filename):
    """Safely read file with appropriate engine based on extension"""
    ext = filename.rsplit('.', 1)[1].lower()
    
    # Save file content for hashing
    file_content = file.read()
    file_hash = get_file_hash(file_content)
    
    # Reset file pointer for reading
    file.seek(0)
    
    try:
        if ext in ('xlsx', 'xls'):
            if ext == 'xlsx':
                if importlib.util.find_spec('openpyxl') is None:
                    return None, 'openpyxl_missing', file_hash
                df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
            else:  # .xls
                if importlib.util.find_spec('xlrd') is None:
                    # Try to auto-install xlrd
                    try:
                        import subprocess
                        import sys
                        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'xlrd>=2.0.1'])
                        importlib.invalidate_caches()
                        # Try reading again after installation
                        df = pd.read_excel(io.BytesIO(file_content), engine='xlrd')
                    except:
                        return None, 'xlrd_missing', file_hash
                else:
                    df = pd.read_excel(io.BytesIO(file_content), engine='xlrd')
        else:
            # Try different encodings for CSV
            try:
                df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='latin1')
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='cp1252')
        
        return df, None, file_hash
    except Exception as e:
        return None, str(e), file_hash

@app.route('/')
def index():
    return render_template('index.html', plotly_available=PLOTLY_AVAILABLE, pdf_available=PDF_AVAILABLE)

@app.route('/favicon.ico')
def favicon():
    return ('', 204)

@app.route('/download/csv/<file_id>')
def download_csv(file_id):
    """Download specific file as CSV"""
    if 'multi_files' not in session:
        flash('No data available to download')
        return redirect(url_for('index'))
    
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        flash('File not found')
        return redirect(url_for('index'))
    
    file_info = files_data[file_id]
    df_path = file_info['path']
    
    if not os.path.exists(df_path):
        flash('Data file not found')
        return redirect(url_for('index'))
    
    df = pd.read_pickle(df_path)
    
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    
    return send_file(
        io.BytesIO(buffer.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'{file_info["name"]}_analysis.csv'
    )

@app.route('/download/combined/csv')
def download_combined_csv():
    """Download combined data from all files as CSV"""
    if 'multi_files' not in session:
        flash('No data available to download')
        return redirect(url_for('index'))
    
    files_data = session.get('multi_files', {})
    if not files_data:
        flash('No files to combine')
        return redirect(url_for('index'))
    
    # Combine all dataframes
    combined_df = None
    for file_id, file_info in files_data.items():
        df_path = file_info['path']
        if os.path.exists(df_path):
            df = pd.read_pickle(df_path)
            df['_source_file'] = file_info['name']  # Add source file column
            if combined_df is None:
                combined_df = df
            else:
                combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    if combined_df is None:
        flash('No data to combine')
        return redirect(url_for('index'))
    
    buffer = io.StringIO()
    combined_df.to_csv(buffer, index=False)
    buffer.seek(0)
    
    return send_file(
        io.BytesIO(buffer.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name='combined_data_analysis.csv'
    )

@app.route('/download/excel/<file_id>')
def download_excel(file_id):
    """Download specific file as Excel"""
    if 'multi_files' not in session:
        flash('No data available to download')
        return redirect(url_for('index'))
    
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        flash('File not found')
        return redirect(url_for('index'))
    
    file_info = files_data[file_id]
    df_path = file_info['path']
    
    if not os.path.exists(df_path):
        flash('Data file not found')
        return redirect(url_for('index'))
    
    df = pd.read_pickle(df_path)

    try:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Data')
        buffer.seek(0)
    except Exception as e:
        flash('Excel export failed. Ensure `openpyxl` is installed: pip install openpyxl')
        return redirect(url_for('index'))

    return send_file(
        buffer,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'{file_info["name"]}_analysis.xlsx'
    )

@app.route('/download/combined/excel')
def download_combined_excel():
    """Download combined data from all files as Excel with multiple sheets"""
    if 'multi_files' not in session:
        flash('No data available to download')
        return redirect(url_for('index'))
    
    files_data = session.get('multi_files', {})
    if not files_data:
        flash('No files to combine')
        return redirect(url_for('index'))
    
    try:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            # Write individual files as separate sheets
            for file_id, file_info in files_data.items():
                df_path = file_info['path']
                if os.path.exists(df_path):
                    df = pd.read_pickle(df_path)
                    sheet_name = file_info['name'][:25]  # Excel sheet name max 31 chars
                    df.to_excel(writer, index=False, sheet_name=sheet_name)
            
            # Write combined data
            combined_df = None
            for file_id, file_info in files_data.items():
                df_path = file_info['path']
                if os.path.exists(df_path):
                    df = pd.read_pickle(df_path)
                    df['_source_file'] = file_info['name']
                    if combined_df is None:
                        combined_df = df
                    else:
                        combined_df = pd.concat([combined_df, df], ignore_index=True)
            
            if combined_df is not None:
                combined_df.to_excel(writer, index=False, sheet_name='Combined_Data')
        
        buffer.seek(0)
    except Exception as e:
        flash(f'Excel export failed: {str(e)}')
        return redirect(url_for('index'))

    return send_file(
        buffer,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='combined_data_analysis.xlsx'
    )

@app.route('/download/pdf/<file_id>')
def download_pdf(file_id):
    """Download specific file as PDF report"""
    if not PDF_AVAILABLE:
        flash('PDF export requires reportlab. Install with: pip install reportlab pillow')
        return redirect(url_for('index'))
    
    if 'multi_files' not in session:
        flash('No data available to download')
        return redirect(url_for('index'))
    
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        flash('File not found')
        return redirect(url_for('index'))
    
    file_info = files_data[file_id]
    df_path = file_info['path']
    
    if not os.path.exists(df_path):
        flash('Data file not found')
        return redirect(url_for('index'))
    
    df = pd.read_pickle(df_path)
    
    # Clean up old plots before creating new ones
    cleanup_old_plots()
    
    # Create PDF
    buffer = io.BytesIO()
    pdf = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    
    # Enhanced styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=12,
        fontName='Helvetica-Bold'
    )
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=12,
        textColor=colors.HexColor('#2e5c8a'),
        spaceAfter=6,
        fontName='Helvetica-Bold'
    )
    
    elements = []
    
    # Title with timestamp
    elements.append(Paragraph(f'Data Analysis Report: {file_info["name"]}', title_style))
    elements.append(Paragraph(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}', 
                             ParagraphStyle('Date', parent=styles['Normal'], fontSize=9, textColor=colors.grey)))
    elements.append(Spacer(1, 0.2*inch))
    
    # Dataset info
    elements.append(Paragraph('Dataset Overview', heading_style))
    info_data = [
        ['Rows', str(len(df))],
        ['Columns', str(len(df.columns))],
        ['Missing Values', str(df.isna().sum().sum())],
        ['Memory Usage', f'{df.memory_usage(deep=True).sum() / 1024:.2f} KB']
    ]
    info_table = Table(info_data, colWidths=[1.5*inch, 2*inch])
    info_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f0f0f0')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('PADDING', (0, 0), (-1, -1), 6),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 0.2*inch))
    
    # Summary stats
    elements.append(Paragraph('Summary Statistics', heading_style))
    desc_df = df.describe(include='all').round(2)
    
    if not desc_df.empty:
        desc_data = [desc_df.columns.tolist()]
        for row in desc_df.values.tolist():
            desc_data.append([str(val) if pd.notna(val) else 'N/A' for val in row])
        
        col_width = min(0.9*inch, 6*inch / len(desc_df.columns))
        desc_table = Table(desc_data, colWidths=[0.8*inch] + [col_width]*len(desc_df.columns))
        desc_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2e5c8a')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('FONTSIZE', (0, 1), (-1, -1), 7),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')])
        ]))
        elements.append(desc_table)
    
    elements.append(Spacer(1, 0.2*inch))
    
    # Data preview
    elements.append(Paragraph('Data Preview (first 10 rows)', heading_style))
    preview_df = df.head(10)
    
    preview_data = [preview_df.columns.tolist()]
    for row in preview_df.values.tolist():
        preview_data.append([str(val)[:50] + '...' if len(str(val)) > 50 else str(val) for val in row])
    
    max_cols = min(8, len(preview_df.columns))
    preview_data = [row[:max_cols] for row in preview_data]
    
    preview_table = Table(preview_data, colWidths=[0.9*inch] * max_cols)
    preview_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2e5c8a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 7),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')]),
    ]))
    elements.append(preview_table)
    
    pdf.build(elements)
    buffer.seek(0)
    
    return send_file(
        buffer,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f'{file_info["name"]}_report_{datetime.now().strftime("%Y%m%d_%H%M")}.pdf'
    )

@app.route('/download/combined/pdf')
def download_combined_pdf():
    """Download combined analysis of all files as PDF report"""
    if not PDF_AVAILABLE:
        flash('PDF export requires reportlab. Install with: pip install reportlab pillow')
        return redirect(url_for('index'))
    
    if 'multi_files' not in session:
        flash('No data available to download')
        return redirect(url_for('index'))
    
    files_data = session.get('multi_files', {})
    if not files_data:
        flash('No files to analyze')
        return redirect(url_for('index'))
    
    # Create PDF
    buffer = io.BytesIO()
    pdf = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=12,
        fontName='Helvetica-Bold'
    )
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=12,
        textColor=colors.HexColor('#2e5c8a'),
        spaceAfter=6,
        fontName='Helvetica-Bold'
    )
    
    elements = []
    
    # Title
    elements.append(Paragraph('Combined Data Analysis Report', title_style))
    elements.append(Paragraph(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}', 
                             ParagraphStyle('Date', parent=styles['Normal'], fontSize=9, textColor=colors.grey)))
    elements.append(Spacer(1, 0.2*inch))
    
    # Summary of files
    elements.append(Paragraph('Files Analyzed', heading_style))
    file_summary = [['File Name', 'Rows', 'Columns', 'Size (KB)']]
    total_rows = 0
    total_cols = set()
    
    for file_id, file_info in files_data.items():
        df_path = file_info['path']
        if os.path.exists(df_path):
            df = pd.read_pickle(df_path)
            file_summary.append([
                file_info['name'],
                str(len(df)),
                str(len(df.columns)),
                f'{os.path.getsize(df_path) / 1024:.1f}'
            ])
            total_rows += len(df)
            total_cols.update(df.columns.tolist())
    
    file_table = Table(file_summary, colWidths=[2.5*inch, 0.8*inch, 0.8*inch, 0.8*inch])
    file_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2e5c8a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')])
    ]))
    elements.append(file_table)
    elements.append(Spacer(1, 0.1*inch))
    
    # Overall statistics
    elements.append(Paragraph('Overall Statistics', heading_style))
    overall_data = [
        ['Total Files', str(len(files_data))],
        ['Total Rows', str(total_rows)],
        ['Total Unique Columns', str(len(total_cols))]
    ]
    overall_table = Table(overall_data, colWidths=[1.5*inch, 2*inch])
    overall_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f0f0f0')),
        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
        ('PADDING', (0, 0), (-1, -1), 6),
    ]))
    elements.append(overall_table)
    
    pdf.build(elements)
    buffer.seek(0)
    
    return send_file(
        buffer,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f'combined_analysis_report_{datetime.now().strftime("%Y%m%d_%H%M")}.pdf'
    )

@app.route('/files')
def get_files():
    """Return list of uploaded files"""
    if 'multi_files' not in session:
        return jsonify({'files': []})
    
    files_data = session.get('multi_files', {})
    files_list = []
    for file_id, file_info in files_data.items():
        files_list.append({
            'id': file_id,
            'name': file_info['name'],
            'rows': file_info.get('rows', 0),
            'columns': file_info.get('columns', 0),
            'hash': file_info.get('hash', '')
        })
    
    return jsonify({'files': files_list})

@app.route('/columns/<file_id>')
def get_columns_for_file(file_id):
    """Return list of columns with metadata for specific file"""
    if 'multi_files' not in session:
        return jsonify({'columns': []})
    
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        return jsonify({'columns': []})
    
    df_path = files_data[file_id]['path']
    if not os.path.exists(df_path):
        return jsonify({'columns': []})
    
    df = pd.read_pickle(df_path)
    cols = []
    for c in df.columns:
        series = df[c]
        is_numeric = pd.api.types.is_numeric_dtype(series)
        unique_cnt = int(series.nunique(dropna=True))
        is_datetime = pd.api.types.is_datetime64_any_dtype(series)
        if not is_datetime and pd.api.types.is_string_dtype(series):
            sample = series.dropna().astype(str).head(200)
            if len(sample):
                parsed = pd.to_datetime(sample, errors='coerce')
                is_datetime = parsed.notna().sum() >= max(1, int(len(sample) * 0.8))
        is_categorical = (not is_numeric) and (unique_cnt <= 50)
        cols.append({
            'name': c,
            'dtype': str(series.dtype),
            'is_numeric': is_numeric,
            'unique_count': unique_cnt,
            'is_datetime': bool(is_datetime),
            'is_categorical': bool(is_categorical)
        })
    return jsonify({'columns': cols, 'file_name': files_data[file_id]['name']})

@app.route('/columns/all')
def get_all_columns():
    """Return common columns across all files"""
    if 'multi_files' not in session:
        return jsonify({'columns': []})
    
    files_data = session.get('multi_files', {})
    if not files_data:
        return jsonify({'columns': []})
    
    # Get columns from first file
    first_file = next(iter(files_data.values()))
    df_path = first_file['path']
    if not os.path.exists(df_path):
        return jsonify({'columns': []})
    
    df = pd.read_pickle(df_path)
    all_columns = set(df.columns)
    
    # Find common columns across all files
    for file_id, file_info in files_data.items():
        df_path = file_info['path']
        if os.path.exists(df_path):
            df = pd.read_pickle(df_path)
            all_columns = all_columns.intersection(set(df.columns))
    
    return jsonify({'columns': sorted(list(all_columns))})

# ==================== DELETE FUNCTIONALITY ====================

@app.route('/delete_row', methods=['POST'])
def delete_row():
    """Delete a specific row from a file"""
    if 'multi_files' not in session:
        return jsonify({'error': 'No files available'}), 400
    
    data = request.get_json()
    file_id = data.get('file_id')
    row_index = data.get('row_index')
    
    if not file_id or row_index is None:
        return jsonify({'error': 'Missing file_id or row_index'}), 400
    
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        return jsonify({'error': 'File not found'}), 404
    
    file_info = files_data[file_id]
    df_path = file_info['path']
    
    if not os.path.exists(df_path):
        return jsonify({'error': 'Data file not found'}), 404
    
    try:
        # Read the dataframe
        df = pd.read_pickle(df_path)
        
        # Check if row index is valid
        if row_index < 0 or row_index >= len(df):
            return jsonify({'error': 'Invalid row index'}), 400
        
        # Delete the row
        df = df.drop(index=row_index).reset_index(drop=True)
        
        # Save back to pickle
        df.to_pickle(df_path)
        
        # Update file info
        files_data[file_id]['rows'] = len(df)
        session['multi_files'] = files_data
        
        # Get updated preview (first 5 rows)
        preview_df = df.head(5)
        preview_html = preview_df.to_html(classes='table table-striped table-hover', 
                                         index=False, 
                                         na_rep='—')
        
        return jsonify({
            'success': True,
            'message': 'Row deleted successfully',
            'rows_remaining': len(df),
            'preview': preview_html,
            'preview_rows': preview_df.fillna('—').values.tolist(),
            'columns_list': df.columns.tolist()
        })
        
    except Exception as e:
        app.logger.exception('Error deleting row')
        return jsonify({'error': str(e)}), 500

@app.route('/delete_rows_bulk', methods=['POST'])
def delete_rows_bulk():
    """Delete multiple rows from a file"""
    if 'multi_files' not in session:
        return jsonify({'error': 'No files available'}), 400
    
    data = request.get_json()
    file_id = data.get('file_id')
    row_indices = data.get('row_indices', [])
    
    if not file_id or not row_indices:
        return jsonify({'error': 'Missing file_id or row_indices'}), 400
    
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        return jsonify({'error': 'File not found'}), 404
    
    file_info = files_data[file_id]
    df_path = file_info['path']
    
    if not os.path.exists(df_path):
        return jsonify({'error': 'Data file not found'}), 404
    
    try:
        # Read the dataframe
        df = pd.read_pickle(df_path)
        
        # Filter out invalid indices
        valid_indices = [i for i in row_indices if 0 <= i < len(df)]
        
        if not valid_indices:
            return jsonify({'error': 'No valid row indices provided'}), 400
        
        # Delete the rows
        df = df.drop(index=valid_indices).reset_index(drop=True)
        
        # Save back to pickle
        df.to_pickle(df_path)
        
        # Update file info
        files_data[file_id]['rows'] = len(df)
        session['multi_files'] = files_data
        
        # Get updated preview (first 5 rows)
        preview_df = df.head(5)
        preview_html = preview_df.to_html(classes='table table-striped table-hover', 
                                         index=False, 
                                         na_rep='—')
        
        return jsonify({
            'success': True,
            'message': f'{len(valid_indices)} row(s) deleted successfully',
            'rows_remaining': len(df),
            'preview': preview_html,
            'preview_rows': preview_df.fillna('—').values.tolist(),
            'columns_list': df.columns.tolist()
        })
        
    except Exception as e:
        app.logger.exception('Error deleting rows')
        return jsonify({'error': str(e)}), 500

@app.route('/delete_file', methods=['POST'])
def delete_file():
    """Delete an uploaded file"""
    if 'multi_files' not in session:
        return jsonify({'error': 'No files available'}), 400
    
    data = request.get_json()
    file_id = data.get('file_id')
    
    if not file_id:
        return jsonify({'error': 'Missing file_id'}), 400
    
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        return jsonify({'error': 'File not found'}), 404
    
    try:
        # Delete the pickle file
        file_info = files_data[file_id]
        if os.path.exists(file_info['path']):
            os.remove(file_info['path'])
        
        # Remove from session
        del files_data[file_id]
        session['multi_files'] = files_data
        
        return jsonify({
            'success': True,
            'message': 'File deleted successfully',
            'files_remaining': len(files_data)
        })
        
    except Exception as e:
        app.logger.exception('Error deleting file')
        return jsonify({'error': str(e)}), 500

# ==================== END DELETE FUNCTIONALITY ====================

@app.route('/generate_chart', methods=['POST'])
def generate_chart():
    """Generate a chart based on user selection across files"""
    if 'multi_files' not in session:
        return jsonify({'error': 'No data available; please upload files first.'}), 400

    files_data = session.get('multi_files', {})
    if not files_data:
        return jsonify({'error': 'No files available.'}), 400

    data = request.get_json() or {}
    chart_type = data.get('type')
    x = data.get('x')
    y = data.get('y')
    nbins = int(data.get('nbins', 30))
    comparison_mode = data.get('comparison_mode', False)
    selected_files = data.get('selected_files', [])

    if comparison_mode and not selected_files:
        return jsonify({'error': 'Please select files for comparison'}), 400

    try:
        if comparison_mode and len(selected_files) > 1:
            # Comparison chart across multiple files
            return generate_comparison_chart(files_data, selected_files, chart_type, x, y, nbins)
        else:
            # Single file chart (use first file or specified file)
            file_id = selected_files[0] if selected_files else next(iter(files_data.keys()))
            file_info = files_data[file_id]
            df_path = file_info['path']
            if not os.path.exists(df_path):
                return jsonify({'error': 'Data file not found'}), 400
            
            df = pd.read_pickle(df_path)
            return generate_single_chart(df, chart_type, x, y, nbins, file_info['name'])
            
    except Exception as e:
        app.logger.exception('Error while creating chart')
        return jsonify({'error': f'Error creating chart: {str(e)}'}), 500

def generate_single_chart(df, chart_type, x, y, nbins, filename):
    """Generate chart for a single dataframe"""
    # Basic validation
    if chart_type not in ('bar', 'barh', 'pie', 'scatter', 'hist', 'line'):
        return jsonify({'error': 'Unsupported chart type'}), 400

    if x is None or x == '':
        return jsonify({'error': 'X column not specified'}), 400
    if x not in df.columns:
        return jsonify({'error': f'X column "{x}" not found in data.'}), 400
    if y and y not in df.columns:
        return jsonify({'error': f'Y column "{y}" not found in data.'}), 400

    if PLOTLY_AVAILABLE:
        if chart_type == 'pie':
            if y and pd.api.types.is_numeric_dtype(df[y]):
                fig = px.pie(df, names=x, values=y, title=f'Pie: {x} vs {y} - {filename}')
            else:
                counts = df[x].value_counts().reset_index()
                counts.columns = [x, 'count']
                if len(counts) > 25:
                    counts = counts.head(25)
                    fig = px.pie(counts, names=x, values='count', title=f'Pie (top 25): {x} - {filename}')
                else:
                    fig = px.pie(counts, names=x, values='count', title=f'Pie: {x} - {filename}')
            
            fig.update_traces(textinfo='percent+label', textposition='inside')
            fig.update_layout(template='plotly_white', height=400, margin=dict(t=60, b=40, l=40, r=40))
            
        elif chart_type in ('bar', 'barh'):
            if y and pd.api.types.is_numeric_dtype(df[y]):
                grouped = df.groupby(x)[y].sum().reset_index()
                val_col = y
            else:
                grouped = df.groupby(x).size().reset_index(name='count')
                val_col = 'count'
            
            if grouped.empty:
                return jsonify({'error': 'No data available for selected columns.'}), 400
            
            if chart_type == 'barh':
                fig = px.bar(grouped, x=val_col, y=x, orientation='h', 
                            title=f'Bar (horizontal): {x} vs {val_col} - {filename}')
            else:
                fig = px.bar(grouped, x=x, y=val_col, 
                            title=f'Bar: {x} vs {val_col} - {filename}')
            
            fig.update_layout(height=400, margin=dict(t=60, b=40, l=40, r=40))
            
        elif chart_type == 'scatter':
            if not (pd.api.types.is_numeric_dtype(df[x]) and pd.api.types.is_numeric_dtype(df[y])):
                return jsonify({'error': 'Scatter requires two numeric columns.'}), 400
            fig = px.scatter(df, x=x, y=y, trendline='ols', 
                            title=f'Scatter: {x} vs {y} - {filename}')
            fig.update_layout(height=400)
            
        elif chart_type == 'hist':
            if not pd.api.types.is_numeric_dtype(df[x]):
                return jsonify({'error': 'Histogram requires a numeric column.'}), 400
            fig = px.histogram(df, x=x, nbins=nbins, 
                              title=f'Histogram: {x} - {filename}')
            fig.update_layout(height=400)
            
        elif chart_type == 'line':
            if not pd.api.types.is_datetime64_any_dtype(df[x]):
                try:
                    df = df.copy()
                    df[x] = pd.to_datetime(df[x], errors='coerce')
                except Exception:
                    pass
            if not pd.api.types.is_datetime64_any_dtype(df[x]):
                return jsonify({'error': 'Line chart requires a datetime X column.'}), 400
            
            df_line = df.dropna(subset=[x])
            if y and pd.api.types.is_numeric_dtype(df[y]):
                fig = px.line(df_line, x=x, y=y, title=f'Line: {x} vs {y} - {filename}')
            else:
                counts = df_line.groupby(x).size().reset_index(name='count')
                if counts.empty:
                    return jsonify({'error': 'No data available for selected columns.'}), 400
                fig = px.line(counts, x=x, y='count', title=f'Line (counts): {x} - {filename}')
            
            fig.update_layout(height=400)

        html = pio.to_html(fig, full_html=False, include_plotlyjs=False)
        return jsonify({'html': html})
    
    else:
        # Matplotlib fallback
        img_buf = io.BytesIO()
        plt.figure(figsize=(8,5))
        
        if chart_type == 'pie':
            if y and pd.api.types.is_numeric_dtype(df[y]):
                data_for = df.groupby(x)[y].sum()
            else:
                data_for = df[x].value_counts()
            plt.pie(data_for.values, labels=data_for.index.astype(str), autopct='%1.1f%%')
            plt.title(f'Pie: {x} - {filename}')
            
        elif chart_type in ('bar','barh'):
            if y and pd.api.types.is_numeric_dtype(df[y]):
                grouped = df.groupby(x)[y].sum()
            else:
                grouped = df.groupby(x).size()
            
            if chart_type == 'bar':
                plt.bar(range(len(grouped)), grouped.values)
                plt.xticks(range(len(grouped)), grouped.index.astype(str), rotation=45, ha='right')
            else:
                plt.barh(range(len(grouped)), grouped.values)
                plt.yticks(range(len(grouped)), grouped.index.astype(str))
            plt.title(f'Bar: {x} - {filename}')
            
        elif chart_type == 'scatter':
            plt.scatter(df[x], df[y])
            plt.xlabel(x)
            plt.ylabel(y)
            plt.title(f'Scatter: {x} vs {y} - {filename}')
            
        elif chart_type == 'hist':
            plt.hist(df[x].dropna(), bins=nbins)
            plt.title(f'Histogram: {x} - {filename}')
            
        elif chart_type == 'line':
            df_line = df.copy()
            df_line[x] = pd.to_datetime(df_line[x], errors='coerce')
            df_line = df_line.dropna(subset=[x]).sort_values(by=x)
            if y and pd.api.types.is_numeric_dtype(df[y]):
                plt.plot(df_line[x], df_line[y], marker='o', linestyle='-')
            else:
                counts = df_line.groupby(x).size().sort_index()
                plt.plot(range(len(counts)), counts.values, marker='o', linestyle='-')
                plt.xticks(range(len(counts)), [d.strftime('%Y-%m-%d') for d in counts.index], rotation=45, ha='right')
            plt.title(f'Line: {x} - {filename}')
        
        plt.tight_layout()
        plt.savefig(img_buf, format='png')
        plt.close()
        img_buf.seek(0)
        img_b64 = base64.b64encode(img_buf.read()).decode('ascii')
        
        return jsonify({'image': img_b64})

def generate_comparison_chart(files_data, selected_files, chart_type, x, y, nbins):
    """Generate comparison chart across multiple files — all chart types supported."""
    if not PLOTLY_AVAILABLE:
        return jsonify({'error': 'Comparison charts require Plotly. Install: pip install plotly'}), 400

    # Safe loader: returns (df, file_info) or (None, None)
    def load_df(file_id):
        if file_id not in files_data:
            return None, None
        info = files_data[file_id]
        if not os.path.exists(info['path']):
            return None, None
        return pd.read_pickle(info['path']), info

    fig = None

    # ── Bar / Bar-horizontal ──────────────────────────────────────────
    if chart_type in ('bar', 'barh'):
        parts = []
        for fid in selected_files:
            df, info = load_df(fid)
            if df is None or x not in df.columns:
                continue
            if y and y in df.columns and pd.api.types.is_numeric_dtype(df[y]):
                g = df.groupby(x)[y].sum().reset_index()
            else:
                g = df.groupby(x).size().reset_index(name='count')
            g['_file'] = info['name']
            parts.append(g)

        if not parts:
            return jsonify({'error': f'Column "{x}" not found in any selected file'}), 400

        combined = pd.concat(parts, ignore_index=True)
        first    = parts[0]
        val_col  = y if (y and y in first.columns and pd.api.types.is_numeric_dtype(first[y])) else 'count'

        if chart_type == 'barh':
            fig = px.bar(combined, x=val_col, y=x, color='_file', orientation='h',
                         barmode='group', title=f'Comparison: {x}', labels={'_file': 'File'})
        else:
            fig = px.bar(combined, x=x, y=val_col, color='_file',
                         barmode='group', title=f'Comparison: {x}', labels={'_file': 'File'})

    # ── Scatter ───────────────────────────────────────────────────────
    elif chart_type == 'scatter':
        if not y:
            return jsonify({'error': 'Scatter requires a Y column'}), 400
        fig = go.Figure()
        for fid in selected_files:
            df, info = load_df(fid)
            if df is None or x not in df.columns or y not in df.columns:
                continue
            if not (pd.api.types.is_numeric_dtype(df[x]) and pd.api.types.is_numeric_dtype(df[y])):
                continue
            fig.add_trace(go.Scatter(x=df[x], y=df[y], mode='markers', name=info['name']))
        fig.update_layout(title=f'Scatter comparison: {x} vs {y}',
                          xaxis_title=x, yaxis_title=y)

    # ── Pie (one subplot per file) ────────────────────────────────────
    elif chart_type == 'pie':
        valid = []
        for fid in selected_files:
            df, info = load_df(fid)
            if df is not None and x in df.columns:
                valid.append((df, info))
        if not valid:
            return jsonify({'error': f'Column "{x}" not found in any selected file'}), 400

        cols  = min(len(valid), 2)
        rows  = (len(valid) + 1) // 2
        specs = [[{'type': 'pie'}] * cols for _ in range(rows)]
        fig   = make_subplots(rows=rows, cols=cols,
                              subplot_titles=[i['name'] for _, i in valid],
                              specs=specs)
        for idx, (df, info) in enumerate(valid):
            r, c = divmod(idx, cols)
            if y and y in df.columns and pd.api.types.is_numeric_dtype(df[y]):
                labels, values = df[x].astype(str), df[y]
            else:
                vc = df[x].value_counts()
                labels, values = vc.index.astype(str), vc.values
            fig.add_trace(go.Pie(labels=labels, values=values,
                                 name=info['name'], textinfo='percent+label'),
                          row=r + 1, col=c + 1)
        fig.update_layout(title=f'Pie comparison: {x}', height=400 * rows)

    # ── Histogram ─────────────────────────────────────────────────────
    elif chart_type == 'hist':
        fig = go.Figure()
        for fid in selected_files:
            df, info = load_df(fid)
            if df is None or x not in df.columns or not pd.api.types.is_numeric_dtype(df[x]):
                continue
            fig.add_trace(go.Histogram(x=df[x].dropna(), name=info['name'],
                                       opacity=0.7, nbinsx=nbins))
        fig.update_layout(title=f'Histogram comparison: {x}', barmode='overlay')

    # ── Line ──────────────────────────────────────────────────────────
    elif chart_type == 'line':
        fig = go.Figure()
        for fid in selected_files:
            df, info = load_df(fid)
            if df is None or x not in df.columns:
                continue
            df = df.copy()
            if not pd.api.types.is_datetime64_any_dtype(df[x]):
                df[x] = pd.to_datetime(df[x], errors='coerce')
            df_line = df.dropna(subset=[x]).sort_values(x)
            if y and y in df_line.columns and pd.api.types.is_numeric_dtype(df_line[y]):
                fig.add_trace(go.Scatter(x=df_line[x], y=df_line[y],
                                         mode='lines+markers', name=info['name']))
            else:
                counts = df_line.groupby(x).size().reset_index(name='count')
                fig.add_trace(go.Scatter(x=counts[x], y=counts['count'],
                                         mode='lines+markers', name=info['name']))
        fig.update_layout(title=f'Line comparison: {x}')

    else:
        return jsonify({'error': f'Chart type "{chart_type}" not supported in comparison mode'}), 400

    if fig is None or len(fig.data) == 0:
        return jsonify({'error': 'No data could be plotted — check the selected columns exist in all files'}), 400

    fig.update_layout(height=fig.layout.height or 450,
                      margin=dict(t=60, b=40, l=40, r=40))
    return jsonify({'html': pio.to_html(fig, full_html=False, include_plotlyjs=False)})


# ==================== DATA CLEANING ====================

@app.route('/clean/remove_duplicates', methods=['POST'])
def clean_remove_duplicates():
    """Remove duplicate rows from a file"""
    if 'multi_files' not in session:
        return jsonify({'error': 'No files available'}), 400
    data = request.get_json()
    file_id = data.get('file_id')
    if not file_id:
        return jsonify({'error': 'Missing file_id'}), 400
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        return jsonify({'error': 'File not found'}), 404
    file_info = files_data[file_id]
    if not os.path.exists(file_info['path']):
        return jsonify({'error': 'Data file not found'}), 404
    try:
        df = pd.read_pickle(file_info['path'])
        before = len(df)
        df = df.drop_duplicates().reset_index(drop=True)
        after = len(df)
        removed = before - after
        df.to_pickle(file_info['path'])
        files_data[file_id]['rows'] = after
        session['multi_files'] = files_data
        preview_df = df.head(5)
        
        # Get updated stats
        duplicates = int(df.duplicated().sum())
        missing = int(df.isna().sum().sum())
        missing_by_col = {col: int(df[col].isna().sum()) for col in df.columns if df[col].isna().sum() > 0}
        
        return jsonify({
            'success': True,
            'message': f'Removed {removed} duplicate row(s). {after} rows remaining.',
            'removed': removed,
            'rows_remaining': after,
            'preview_rows': preview_df.fillna('—').values.tolist(),
            'columns_list': df.columns.tolist(),
            'stats': {
                'rows': after,
                'duplicates': duplicates,
                'missing': missing,
                'missing_by_col': missing_by_col
            }
        })
    except Exception as e:
        app.logger.exception('Error removing duplicates')
        return jsonify({'error': str(e)}), 500

@app.route('/clean/fill_missing', methods=['POST'])
def clean_fill_missing():
    """Fill missing values in a file"""
    if 'multi_files' not in session:
        return jsonify({'error': 'No files available'}), 400
    
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON data received'}), 400
        
    file_id = data.get('file_id')
    strategy = data.get('strategy', 'mean')  # mean, median, mode, zero, empty_string
    
    if not file_id:
        return jsonify({'error': 'Missing file_id'}), 400
        
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        return jsonify({'error': 'File not found'}), 404
        
    file_info = files_data[file_id]
    if not os.path.exists(file_info['path']):
        return jsonify({'error': 'Data file not found'}), 404
        
    try:
        df = pd.read_pickle(file_info['path'])
        missing_before = int(df.isna().sum().sum())
        
        if missing_before == 0:
            preview_df = df.head(5)
            return jsonify({
                'success': True, 
                'message': 'No missing values found.', 
                'filled': 0,
                'rows_remaining': len(df),
                'preview_rows': preview_df.fillna('—').values.tolist(),
                'columns_list': df.columns.tolist(),
                'stats': {
                    'rows': len(df),
                    'duplicates': int(df.duplicated().sum()),
                    'missing': 0,
                    'missing_by_col': {}
                }
            })
        
        # Fill missing values based on strategy
        for col in df.columns:
            if df[col].isna().sum() == 0:
                continue
                
            if pd.api.types.is_numeric_dtype(df[col]):
                if strategy == 'mean':
                    df[col] = df[col].fillna(df[col].mean())
                elif strategy == 'median':
                    df[col] = df[col].fillna(df[col].median())
                elif strategy == 'mode':
                    mode_val = df[col].mode()
                    df[col] = df[col].fillna(mode_val[0] if len(mode_val) > 0 else 0)
                elif strategy == 'zero':
                    df[col] = df[col].fillna(0)
                else:  # empty_string or any other
                    df[col] = df[col].fillna('')
            else:
                # Non-numeric columns
                if strategy == 'mode':
                    mode_val = df[col].mode()
                    df[col] = df[col].fillna(mode_val[0] if len(mode_val) > 0 else '')
                else:
                    df[col] = df[col].fillna('')
        
        missing_after = int(df.isna().sum().sum())
        filled = missing_before - missing_after
        
        # Save back to pickle
        df.to_pickle(file_info['path'])
        
        # Update session with new row count (rows remain same, just values filled)
        files_data[file_id]['rows'] = len(df)
        session['multi_files'] = files_data
        
        # Get updated stats
        preview_df = df.head(5)
        duplicates = int(df.duplicated().sum())
        missing_by_col = {col: int(df[col].isna().sum()) for col in df.columns if df[col].isna().sum() > 0}
        
        return jsonify({
            'success': True,
            'message': f'Filled {filled} missing value(s) using {strategy} strategy.',
            'filled': filled,
            'rows_remaining': len(df),
            'preview_rows': preview_df.fillna('—').values.tolist(),
            'columns_list': df.columns.tolist(),
            'stats': {
                'rows': len(df),
                'duplicates': duplicates,
                'missing': missing_after,
                'missing_by_col': missing_by_col
            }
        })
        
    except Exception as e:
        app.logger.exception('Error filling missing values')
        return jsonify({'error': str(e)}), 500

@app.route('/clean/manual_edit', methods=['POST'])
def clean_manual_edit():
    """Apply manual edits to specific cells"""
    if 'multi_files' not in session:
        return jsonify({'error': 'No files available'}), 400
        
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON received"}), 400

    file_id = data.get('file_id')
    edits = data.get('edits', [])

    if not file_id:
        return jsonify({"error": "Missing file_id"}), 400
        
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        return jsonify({"error": "Invalid file id"}), 400

    file_info = files_data[file_id]
    if not os.path.exists(file_info['path']):
        return jsonify({"error": "Data file not found"}), 404

    try:
        df = pd.read_pickle(file_info['path'])

        for edit in edits:
            row_idx = edit.get('row')
            col_name = edit.get('column')
            new_value = edit.get('value')
            
            if row_idx is not None and col_name in df.columns:
                if 0 <= row_idx < len(df):
                    df.at[row_idx, col_name] = new_value

        # Save back to pickle
        df.to_pickle(file_info['path'])
        
        # Update session with new row count
        files_data[file_id]['rows'] = len(df)
        session['multi_files'] = files_data

        # Get updated stats
        duplicates = int(df.duplicated().sum())
        missing = int(df.isna().sum().sum())
        missing_by_col = {col: int(df[col].isna().sum()) for col in df.columns if df[col].isna().sum() > 0}

        return jsonify({
            "message": "Manual edits applied",
            "success": True,
            "preview_rows": df.head(5).fillna('—').values.tolist(),
            "columns_list": df.columns.tolist(),
            "stats": {
                'rows': len(df),
                'duplicates': duplicates,
                'missing': missing,
                'missing_by_col': missing_by_col
            }
        })
    except Exception as e:
        app.logger.exception('Error in manual edit')
        return jsonify({'error': str(e)}), 500
    
@app.route('/clean/stats', methods=['GET'])
def clean_stats():
    """Get cleaning stats for a file"""
    if 'multi_files' not in session:
        return jsonify({'error': 'No files available'}), 400
    file_id = request.args.get('file_id')
    if not file_id:
        return jsonify({'error': 'Missing file_id'}), 400
    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        return jsonify({'error': 'File not found'}), 404
    file_info = files_data[file_id]
    if not os.path.exists(file_info['path']):
        return jsonify({'error': 'Data file not found'}), 404
    try:
        df = pd.read_pickle(file_info['path'])
        duplicates = int(df.duplicated().sum())
        missing = int(df.isna().sum().sum())
        missing_by_col = {col: int(df[col].isna().sum()) for col in df.columns if df[col].isna().sum() > 0}
        return jsonify({
            'rows': len(df),
            'columns': len(df.columns),
            'duplicates': duplicates,
            'missing': missing,
            'missing_by_col': missing_by_col
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500



# ==================== END DATA CLEANING ====================

@app.route('/analyze', methods=['POST'])
def analyze():
    if 'files[]' not in request.files:
        flash('No files uploaded')
        return redirect(url_for('index'))
    
    files = request.files.getlist('files[]')
    
    if not files or files[0].filename == '':
        flash('No selected files')
        return redirect(url_for('index'))
    
    successful_uploads = []
    failed_uploads = []
    files_data = {}
    
    for file in files:
        if not allowed_file(file.filename):
            failed_uploads.append({'name': file.filename, 'error': 'Unsupported file type'})
            continue
        
        filename = secure_filename(file.filename)
        df, error, file_hash = safe_read_file(file, filename)
        
        if df is None:
            if error == 'openpyxl_missing':
                failed_uploads.append({'name': filename, 'error': 'Missing openpyxl. Install: pip install openpyxl'})
            elif error == 'xlrd_missing':
                failed_uploads.append({'name': filename, 'error': 'Missing xlrd. Install: pip install xlrd>=2.0.1'})
            else:
                failed_uploads.append({'name': filename, 'error': f'Error reading file: {error}'})
            continue
        
        # Store dataframe in temp file
        temp_file = os.path.join(TEMP_DIR, f'{uuid.uuid4().hex}.pkl')
        df.to_pickle(temp_file)
        
        file_id = hashlib.md5(f"{filename}{file_hash}{time.time()}".encode()).hexdigest()[:12]
        files_data[file_id] = {
            'name': filename,
            'path': temp_file,
            'hash': file_hash,
            'rows': len(df),
            'columns': len(df.columns),
            'upload_time': time.time()
        }
        
        successful_uploads.append({
            'id': file_id,
            'name': filename,
            'rows': len(df),
            'columns': len(df.columns)
        })
    
    # Store in session
    session['multi_files'] = files_data
    
    if successful_uploads and not failed_uploads:
        flash(f'Successfully uploaded {len(successful_uploads)} file(s)')
    elif successful_uploads and failed_uploads:
        flash(f'Uploaded {len(successful_uploads)} file(s). {len(failed_uploads)} file(s) failed.')
    else:
        flash('No files were successfully uploaded')
        return redirect(url_for('index'))
    
    # Prepare data for results page
    file_previews = []
    for file_id, file_info in files_data.items():
        df = pd.read_pickle(file_info['path'])
        preview_df = df.head(5)
        preview_html = preview_df.to_html(classes='table table-striped table-hover', index=False, na_rep='—')
        
        file_previews.append({
            'id': file_id,
            'name': file_info['name'],
            'rows': file_info['rows'],
            'columns': file_info['columns'],
            'columns_list': df.columns.tolist(),
            'preview_rows': preview_df.fillna('—').values.tolist(),
            'preview': preview_html
        })
    
    # ← THE RETURN MUST BE HERE, AFTER THE LOOP (CORRECT INDENTATION)
    return render_template('results.html', 
                         file_previews=file_previews,
                         successful_uploads=successful_uploads,
                         failed_uploads=failed_uploads,
                         pdf_available=PDF_AVAILABLE, 
                         plotly_available=PLOTLY_AVAILABLE)

# Cleanup temporary files on shutdown   
def cleanup_temp_files():
    """Remove temporary pickle files"""
    for f in os.listdir(TEMP_DIR):
        fpath = os.path.join(TEMP_DIR, f)
        try:
            if os.path.isfile(fpath):
                os.remove(fpath)
        except Exception as e:
            app.logger.warning(f"Failed to remove temp file {f}: {e}")


@app.route('/download_selected_columns')
def download_selected_columns():
    """Download a user-selected subset of columns as CSV, Excel, or PDF."""
    file_id   = request.args.get('file_id', '')
    columns   = request.args.getlist('columns')
    fmt       = request.args.get('format', 'csv')

    if 'multi_files' not in session:
        flash('No data available to download')
        return redirect(url_for('index'))

    files_data = session.get('multi_files', {})
    if file_id not in files_data:
        flash('File not found')
        return redirect(url_for('index'))

    file_info = files_data[file_id]
    if not os.path.exists(file_info['path']):
        flash('Data file not found')
        return redirect(url_for('index'))

    df         = pd.read_pickle(file_info['path'])
    valid_cols = [c for c in columns if c in df.columns]
    if not valid_cols:
        flash('None of the requested columns exist in this file')
        return redirect(url_for('index'))

    df_filtered = df[valid_cols]
    base_name   = file_info['name'].rsplit('.', 1)[0]

    if fmt == 'csv':
        buf = io.StringIO()
        df_filtered.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(io.BytesIO(buf.getvalue().encode('utf-8')),
                         mimetype='text/csv', as_attachment=True,
                         download_name=f'{base_name}_selected_columns.csv')
    elif fmt == 'excel':
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                df_filtered.to_excel(writer, index=False, sheet_name='Selected Columns')
            buf.seek(0)
            return send_file(buf,
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             as_attachment=True,
                             download_name=f'{base_name}_selected_columns.xlsx')
        except Exception as e:
            flash(f'Excel export failed: {e}')
            return redirect(url_for('index'))
    elif fmt == 'pdf':
        if not PDF_AVAILABLE:
            flash('PDF requires reportlab: pip install reportlab pillow')
            return redirect(url_for('index'))
        buf    = io.BytesIO()
        pdf    = SimpleDocTemplate(buf, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
        styles = getSampleStyleSheet()
        ts     = ParagraphStyle('CT', parent=styles['Heading1'], fontSize=16,
                                textColor=colors.HexColor('#1f4788'), spaceAfter=12,
                                fontName='Helvetica-Bold')
        hs     = ParagraphStyle('CH', parent=styles['Heading2'], fontSize=12,
                                textColor=colors.HexColor('#2e5c8a'), spaceAfter=6,
                                fontName='Helvetica-Bold')
        elems  = [
            Paragraph(f'Selected Columns Report: {file_info["name"]}', ts),
            Paragraph(f'Columns: {", ".join(valid_cols)}  |  Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}',
                      ParagraphStyle('S', parent=styles['Normal'], fontSize=9, textColor=colors.grey)),
            Spacer(1, 0.2*inch), Paragraph('Dataset Overview', hs),
        ]
        it = Table([
            ['Rows', str(len(df_filtered))], ['Selected Columns', str(len(valid_cols))],
            ['Missing Values', str(df_filtered.isna().sum().sum())],
            ['Memory', f'{df_filtered.memory_usage(deep=True).sum()/1024:.2f} KB'],
        ], colWidths=[1.5*inch, 2*inch])
        it.setStyle(TableStyle([('BACKGROUND',(0,0),(0,-1),colors.HexColor('#f0f0f0')),
                                ('GRID',(0,0),(-1,-1),1,colors.grey),('PADDING',(0,0),(-1,-1),6),
                                ('FONTSIZE',(0,0),(-1,-1),9)]))
        elems += [it, Spacer(1,0.2*inch), Paragraph('Summary Statistics', hs)]
        desc = df_filtered.describe(include='all').round(2)
        if not desc.empty:
            dd = [[''] + desc.columns.tolist()]
            for idx, row in zip(desc.index, desc.values.tolist()):
                dd.append([str(idx)] + [str(v) if pd.notna(v) else 'N/A' for v in row])
            cw = min(0.9*inch, 6*inch / max(1, len(desc.columns)))
            dt = Table(dd, colWidths=[0.8*inch] + [cw]*len(desc.columns))
            dt.setStyle(TableStyle([
                ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#2e5c8a')),
                ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
                ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,-1),7),
                ('GRID',(0,0),(-1,-1),1,colors.grey),
                ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f0f0f0')])]))
            elems.append(dt)
        elems += [Spacer(1,0.2*inch), Paragraph('Data Preview (first 20 rows)', hs)]
        prev  = df_filtered.head(20)
        mc    = min(8, len(valid_cols)); pcols = valid_cols[:mc]
        pd_   = [pcols] + [[str(v)[:40]+'…' if len(str(v))>40 else str(v) for v in row]
                            for row in prev[pcols].values.tolist()]
        cw2   = min(1.0*inch, 7*inch / max(1, mc))
        pt    = Table(pd_, colWidths=[cw2]*mc)
        pt.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#2e5c8a')),
            ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
            ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,-1),7),
            ('GRID',(0,0),(-1,-1),1,colors.grey),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f0f0f0')])]))
        elems.append(pt)
        pdf.build(elems); buf.seek(0)
        return send_file(buf, mimetype='application/pdf', as_attachment=True,
                         download_name=f'{base_name}_selected_columns_{datetime.now().strftime("%Y%m%d_%H%M")}.pdf')
    else:
        flash(f'Unknown format: {fmt}')
        return redirect(url_for('index'))

atexit.register(cleanup_temp_files)

if __name__ == '__main__':
    debug_mode = os.environ.get('FLASK_DEBUG', '1') == '1'
    app.run(debug=debug_mode)


