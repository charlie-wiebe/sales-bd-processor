import os
import psycopg2
import time
from flask import Flask, request, jsonify, render_template_string
from typing import List
import io
import tempfile
import base64
import csv
import sys
from datetime import datetime
import threading
import json
import uuid
import traceback

app = Flask(__name__)

# Global job storage
jobs = {}
job_lock = threading.Lock()

def setup_ssl_files():
    """Setup SSL certificate files from environment variables"""
    ssl_files = {}
    
    # Check if SSL certs are provided as base64 encoded strings
    if os.getenv('DB_SSL_ROOT_CERT'):
        root_cert_file = tempfile.NamedTemporaryFile(mode='w', suffix='.crt', delete=False)
        root_cert_file.write(base64.b64decode(os.getenv('DB_SSL_ROOT_CERT')).decode('utf-8'))
        root_cert_file.close()
        ssl_files['sslrootcert'] = root_cert_file.name
    
    if os.getenv('DB_SSL_CLIENT_CERT'):
        client_cert_file = tempfile.NamedTemporaryFile(mode='w', suffix='.crt', delete=False)
        client_cert_file.write(base64.b64decode(os.getenv('DB_SSL_CLIENT_CERT')).decode('utf-8'))
        client_cert_file.close()
        ssl_files['sslcert'] = client_cert_file.name
    
    if os.getenv('DB_SSL_CLIENT_KEY'):
        client_key_file = tempfile.NamedTemporaryFile(mode='w', suffix='.key', delete=False)
        client_key_file.write(base64.b64decode(os.getenv('DB_SSL_CLIENT_KEY')).decode('utf-8'))
        client_key_file.close()
        ssl_files['sslkey'] = client_key_file.name
    
    return ssl_files

# Database configuration from environment
def get_db_config():
    config = {
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', 5432))
    }
    
    # Add SSL configuration
    ssl_mode = os.getenv('DB_SSL_MODE', 'prefer')  # prefer, require, verify-ca, verify-full
    config['sslmode'] = ssl_mode
    
    if ssl_mode in ['verify-ca', 'verify-full']:
        ssl_files = setup_ssl_files()
        config.update(ssl_files)
    
    return config

QUERY_TEMPLATE = """
SELECT 
    slug,
    COUNT(*) AS sales_bd_count
FROM (
    SELECT DISTINCT ON (LOWER(lp.first_name), LOWER(lp.last_name), lcs.slug)
        lcs.slug,
        lp.id AS profile_id
    FROM linkedin_profile_position3 lpp3
    INNER JOIN linkedin_company lc ON lpp3.linkedin_company_id = lc.id
    INNER JOIN linkedin_company_slug lcs ON lc.id = lcs.linkedin_company_id
    INNER JOIN linkedin_profile lp ON lpp3.linkedin_profile_id = lp.id
    LEFT JOIN company_position3 cp3 ON cp3.linkedin_profile_position3_id = lpp3.id
    LEFT JOIN job_title jt ON cp3.job_title_id = jt.id
    WHERE lcs.slug IN ({})
      AND lcs.slug_status = 'A'
      AND lp.slug_status = 'A'
      AND lpp3.is_current = true
      AND lpp3.obsolete = false
      AND (lpp3.end_date IS NULL OR lpp3.end_date > CURRENT_DATE)
      AND jt.tags && ARRAY[31, 45]
      AND NOT (
        LOWER(lpp3.title) LIKE '%investor%' OR LOWER(lpp3.title) LIKE '%advisor%'
        OR LOWER(lpp3.title) LIKE '%board%' OR LOWER(lpp3.title) LIKE '%founder%'
        OR LOWER(lpp3.title) LIKE '%vp%' OR LOWER(lpp3.title) LIKE '%vice%'
        OR LOWER(lpp3.title) LIKE '%cro%' OR LOWER(lpp3.title) LIKE '%chief%'
        OR LOWER(lpp3.title) LIKE '%director%' OR LOWER(lpp3.title) LIKE '%ops%'
        OR LOWER(lpp3.title) LIKE '%operations%' OR LOWER(lpp3.title) LIKE '%enablement%'
        OR LOWER(lpp3.title) LIKE '%coordinator%' OR LOWER(lpp3.title) LIKE '%assistant%'
        OR LOWER(lpp3.title) LIKE '%customer success%' OR LOWER(lpp3.title) LIKE '%client success%'
        OR LOWER(lpp3.title) LIKE '%support%' OR LOWER(lpp3.title) LIKE '%manager%'
        OR LOWER(lpp3.title) LIKE '%solutions%' OR LOWER(lpp3.title) LIKE '%owner%'
        OR LOWER(lpp3.title) LIKE '%ceo%' OR LOWER(lpp3.title) LIKE '%partner%'
      )
    ORDER BY LOWER(lp.first_name), LOWER(lp.last_name), lcs.slug, lp.updated_at DESC NULLS LAST
) AS deduplicated_people
GROUP BY slug;
"""

def process_companies_background(job_id, companies, batch_size, input_format):
    """Background processing function with better error handling"""
    conn = None
    try:
        with job_lock:
            jobs[job_id]['status'] = 'processing'
            jobs[job_id]['started_at'] = datetime.now().isoformat()
        
        print(f"[JOB {job_id}] START: Processing {len(companies)} companies in background", flush=True)
        sys.stdout.flush()
        
        # Test database connection first
        print(f"[JOB {job_id}] Testing database connection...", flush=True)
        sys.stdout.flush()
        
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        conn.close()
        conn = None
        
        print(f"[JOB {job_id}] Database connection successful", flush=True)
        sys.stdout.flush()
        
        # Create timestamped filename
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        final_filename = f'sales_bd_results_{timestamp}.csv'
        partial_filename = f'sales_bd_partial_{timestamp}.csv'
        
        # Ensure data directory exists
        data_dir = '/data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        final_filepath = os.path.join(data_dir, final_filename)
        partial_filepath = os.path.join(data_dir, partial_filename)
        
        all_results = []
        
        # Create lookup dict for HubSpot IDs if provided
        slug_to_hubspot_id = {}
        if input_format == 'csv':
            for company in companies:
                slug_to_hubspot_id[company['slug']] = company['hubspot_company_id']
        
        total_batches = (len(companies) + batch_size - 1) // batch_size
        
        with job_lock:
            jobs[job_id]['total_batches'] = total_batches
            jobs[job_id]['completed_batches'] = 0
        
        print(f"[JOB {job_id}] Processing {len(companies)} companies in {total_batches} batches of {batch_size}", flush=True)
        sys.stdout.flush()
        
        # Process in batches
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            print(f"[JOB {job_id}] BATCH {batch_num}/{total_batches}: Processing {len(batch)} companies...", flush=True)
            sys.stdout.flush()
            
            # Format for SQL IN clause - just the slugs
            slug_list = "'" + "', '".join([company['slug'] for company in batch]) + "'"
            query = QUERY_TEMPLATE.format(slug_list)
            
            # Execute query with timeout
            batch_results = []
            try:
                print(f"[JOB {job_id}] BATCH {batch_num}: Connecting to database...", flush=True)
                sys.stdout.flush()
                
                conn = psycopg2.connect(**db_config)
                cursor = conn.cursor()
                
                print(f"[JOB {job_id}] BATCH {batch_num}: Executing query...", flush=True)
                sys.stdout.flush()
                
                cursor.execute(query)
                
                print(f"[JOB {job_id}] BATCH {batch_num}: Fetching results...", flush=True)
                sys.stdout.flush()
                
                # Fetch results manually (no pandas)
                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                
                print(f"[JOB {job_id}] BATCH {batch_num}: Got {len(results)} raw results", flush=True)
                sys.stdout.flush()
                
                # Convert to list of dicts
                for row in results:
                    row_dict = dict(zip(column_names, row))
                    
                    # Add HubSpot company ID if provided
                    if input_format == 'csv':
                        row_dict['hubspot_company_id'] = slug_to_hubspot_id.get(row_dict['slug'], '')
                    
                    batch_results.append(row_dict)
                
                all_results.extend(batch_results)
                conn.close()
                conn = None
                
                print(f"[JOB {job_id}] BATCH {batch_num}: Processed {len(batch_results)} results", flush=True)
                sys.stdout.flush()
                
                # Save partial results after each batch
                if all_results:
                    print(f"[JOB {job_id}] BATCH {batch_num}: Saving partial results...", flush=True)
                    sys.stdout.flush()
                    
                    with open(partial_filepath, 'w', newline='') as csvfile:
                        fieldnames = all_results[0].keys()
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(all_results)
                
                # Update job status
                with job_lock:
                    jobs[job_id]['completed_batches'] = batch_num
                    jobs[job_id]['total_results'] = len(all_results)
                    jobs[job_id]['partial_file'] = partial_filename
                
                print(f"[JOB {job_id}] BATCH {batch_num}/{total_batches}: COMPLETE - {len(batch_results)} new results (Total: {len(all_results)})", flush=True)
                sys.stdout.flush()
                
                # Brief pause to be nice to the database
                time.sleep(1.0)
                
            except Exception as batch_error:
                if conn:
                    conn.close()
                    conn = None
                
                error_msg = f"Batch {batch_num} error: {str(batch_error)}"
                print(f"[JOB {job_id}] BATCH {batch_num}: ERROR - {error_msg}", flush=True)
                print(f"[JOB {job_id}] BATCH {batch_num}: Traceback - {traceback.format_exc()}", flush=True)
                sys.stdout.flush()
                
                with job_lock:
                    if 'batch_errors' not in jobs[job_id]:
                        jobs[job_id]['batch_errors'] = []
                    jobs[job_id]['batch_errors'].append(error_msg)
                
                # Continue with next batch instead of failing entire job
                continue
        
        # Save final results
        if all_results:
            print(f"[JOB {job_id}] Saving final results to {final_filename}...", flush=True)
            sys.stdout.flush()
            
            # Write final CSV
            with open(final_filepath, 'w', newline='') as csvfile:
                fieldnames = all_results[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_results)
            
            # Remove partial file since we have final results
            if os.path.exists(partial_filepath):
                os.remove(partial_filepath)
            
            # Update job status
            with job_lock:
                jobs[job_id]['status'] = 'completed'
                jobs[job_id]['completed_at'] = datetime.now().isoformat()
                jobs[job_id]['final_file'] = final_filename
                jobs[job_id]['total_results'] = len(all_results)
                if 'partial_file' in jobs[job_id]:
                    del jobs[job_id]['partial_file']
            
            print(f"[JOB {job_id}] COMPLETE: {len(all_results)} companies with Sales/BD employees. Saved to {final_filename}", flush=True)
            sys.stdout.flush()
        else:
            with job_lock:
                jobs[job_id]['status'] = 'completed'
                jobs[job_id]['completed_at'] = datetime.now().isoformat()
                jobs[job_id]['error'] = 'No results found'
            
            print(f"[JOB {job_id}] COMPLETE: No companies found with Sales/BD employees", flush=True)
            sys.stdout.flush()
            
    except Exception as e:
        if conn:
            conn.close()
        
        error_msg = str(e)
        traceback_msg = traceback.format_exc()
        
        with job_lock:
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['error'] = error_msg
            jobs[job_id]['traceback'] = traceback_msg
            jobs[job_id]['failed_at'] = datetime.now().isoformat()
        
        print(f"[JOB {job_id}] FAILED: {error_msg}", flush=True)
        print(f"[JOB {job_id}] TRACEBACK: {traceback_msg}", flush=True)
        sys.stdout.flush()

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Sales/BD Processor</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        .form-group { margin: 20px 0; }
        textarea { width: 100%; height: 200px; }
        button { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; margin: 5px; }
        .section { margin-top: 30px; padding: 20px; background: #f8f9fa; border-radius: 4px; }
        .status { font-family: monospace; white-space: pre-wrap; }
        .example { background: #e9ecef; padding: 10px; border-radius: 4px; margin: 10px 0; font-family: monospace; }
        .job-info { margin: 10px 0; padding: 10px; background: white; border-radius: 3px; }
        .progress-bar { width: 100%; height: 20px; background: #e9ecef; border-radius: 10px; margin: 10px 0; }
        .progress-fill { height: 100%; background: #007bff; border-radius: 10px; transition: width 0.3s; }
        .error { color: red; font-size: 12px; }
        .file-list { background: white; padding: 10px; border-radius: 3px; margin: 10px 0; }
        .file-item { margin: 5px 0; padding: 5px; background: #f8f9fa; border-radius: 3px; }
    </style>
</head>
<body>
    <h1>Sales/BD Employee Counter</h1>
    <p>Process companies in batches to count Sales and Business Development employees.</p>
    
    <div class="section">
        <h3>Start New Job</h3>
        <form id="jobForm">
            <div class="form-group">
                <label>Input Format:</label>
                <div>
                    <input type="radio" id="slugsOnly" name="inputFormat" value="slugs" checked>
                    <label for="slugsOnly">Company slugs only (one per line)</label>
                </div>
                <div>
                    <input type="radio" id="csvFormat" name="inputFormat" value="csv">
                    <label for="csvFormat">CSV format: hubspot_company_id,slug</label>
                </div>
            </div>
            
            <div class="form-group">
                <label>Company Data:</label>
                <div class="example" id="slugExample">
                    Example (slugs only):
                    nooksapp
                    hubspot
                    salesforce
                </div>
                <div class="example" id="csvExample" style="display:none;">
                    Example (CSV format):
                    12345,nooksapp
                    67890,hubspot
                    11111,salesforce
                </div>
                <textarea id="companies" placeholder="Enter your company data here..."></textarea>
            </div>
            
            <div class="form-group">
                <label>Batch Size:</label>
                <input type="number" id="batchSize" value="100" min="10" max="500">
                <small>Start with 100 for reliability. Increase if working well.</small>
            </div>
            <button type="submit">Start Background Job</button>
        </form>
    </div>
    
    <div class="section">
        <h3>Job Status & Files</h3>
        <button onclick="checkStatus()">Refresh Status</button>
        <button onclick="listFiles()">List All CSV Files</button>
        <button onclick="downloadLatest()">Download Latest CSV</button>
        <button onclick="testConnection()">Test DB Connection</button>
        <div id="jobStatus">Click "Refresh Status" to check current jobs</div>
        <div id="fileList"></div>
    </div>

    <script>
        let currentJobId = localStorage.getItem('currentJobId');

        // Toggle examples based on input format
        document.querySelectorAll('input[name="inputFormat"]').forEach(radio => {
            radio.addEventListener('change', function() {
                if (this.value === 'slugs') {
                    document.getElementById('slugExample').style.display = 'block';
                    document.getElementById('csvExample').style.display = 'none';
                    document.getElementById('companies').placeholder = 'nooksapp\\nhubspot\\nsalesforce\\n...';
                } else {
                    document.getElementById('slugExample').style.display = 'none';
                    document.getElementById('csvExample').style.display = 'block';
                    document.getElementById('companies').placeholder = '12345,nooksapp\\n67890,hubspot\\n11111,salesforce\\n...';
                }
            });
        });

        function testConnection() {
            fetch('/test-connection')
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        alert('Database connection successful!');
                    } else {
                        alert('Database connection failed: ' + data.error);
                    }
                })
                .catch(error => {
                    alert('Error testing connection: ' + error.message);
                });
        }

        function listFiles() {
            fetch('/list-files')
                .then(response => response.json())
                .then(data => {
                    const container = document.getElementById('fileList');
                    if (data.success && data.files && data.files.length > 0) {
                        container.innerHTML = '<div class="file-list"><h4>Saved CSV Files:</h4>' + 
                            data.files.map(file => 
                                `<div class="file-item">
                                    <strong>${file.name}</strong> - ${file.size} bytes - ${new Date(file.modified).toLocaleString()}
                                    <br><a href="/download-csv/${file.name}" download>Download</a>
                                </div>`
                            ).join('') + '</div>';
                    } else {
                        container.innerHTML = '<div class="file-list"><em>No CSV files found</em></div>';
                    }
                })
                .catch(error => {
                    document.getElementById('fileList').innerHTML = '<div class="file-list"><em>Error loading files</em></div>';
                });
        }

        function checkStatus() {
            fetch('/status')
                .then(response => response.json())
                .then(data => {
                    const container = document.getElementById('jobStatus');
                    if (data.jobs && data.jobs.length > 0) {
                        container.innerHTML = data.jobs.map(job => {
                            let statusHtml = `<div class="job-info">
                                <strong>Job ${job.id.substring(0, 8)}...</strong> - ${job.status.toUpperCase()}
                                <br>Companies: ${job.total_companies || 'N/A'}
                                <br>Created: ${new Date(job.created_at).toLocaleString()}`;
                            
                            if (job.status === 'processing') {
                                const progress = job.total_batches > 0 ? Math.round((job.completed_batches / job.total_batches) * 100) : 0;
                                statusHtml += `<br>Progress: ${job.completed_batches}/${job.total_batches} batches (${progress}%)
                                    <div class="progress-bar"><div class="progress-fill" style="width: ${progress}%"></div></div>
                                    <br>Results so far: ${job.total_results || 0}`;
                            }
                            
                            if (job.status === 'completed' && job.final_file) {
                                statusHtml += `<br>Results: ${job.total_results} companies
                                    <br><a href="/download-csv/${job.final_file}" download>Download Final CSV</a>`;
                            }
                            
                            if (job.status === 'processing' && job.partial_file) {
                                statusHtml += `<br><a href="/download-csv/${job.partial_file}" download>Download Partial Results</a>`;
                            }
                            
                            if (job.error) {
                                statusHtml += `<br><span class="error">Error: ${job.error}</span>`;
                            }
                            
                            if (job.batch_errors && job.batch_errors.length > 0) {
                                statusHtml += `<br><span class="error">Batch errors: ${job.batch_errors.length}</span>`;
                            }
                            
                            statusHtml += '</div>';
                            return statusHtml;
                        }).join('');
                    } else {
                        container.innerHTML = '<em>No jobs found</em>';
                    }
                })
                .catch(error => {
                    document.getElementById('jobStatus').innerHTML = '<em>Error loading status</em>';
                });
        }

        function downloadLatest() {
            window.open('/download-latest', '_blank');
        }
        
        document.getElementById('jobForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const inputFormat = document.querySelector('input[name="inputFormat"]:checked').value;
            const inputData = document.getElementById('companies').value.trim();
            const batchSize = parseInt(document.getElementById('batchSize').value);
            
            if (!inputData) {
                alert('Please enter company data');
                return;
            }
            
            // Parse input based on format
            let companies = [];
            if (inputFormat === 'slugs') {
                companies = inputData.split('\\n').filter(line => line.trim()).map(slug => ({slug: slug.trim()}));
            } else {
                companies = inputData.split('\\n').filter(line => line.trim()).map(line => {
                    const [hubspot_id, slug] = line.split(',').map(s => s.trim());
                    return {hubspot_company_id: hubspot_id, slug: slug};
                });
            }
            
            if (companies.length === 0) {
                alert('No valid company data found');
                return;
            }
            
            try {
                const response = await fetch('/start-job', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ companies, batch_size: batchSize, input_format: inputFormat })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    currentJobId = result.job_id;
                    localStorage.setItem('currentJobId', currentJobId);
                    alert(`Job started! Job ID: ${currentJobId.substring(0, 8)}...\\nProcessing ${companies.length} companies in the background.`);
                    checkStatus(); // Refresh status immediately
                } else {
                    alert('Error starting job: ' + result.error);
                }
            } catch (error) {
                alert('Error: ' + error.message);
            }
        });

        // Auto-refresh status every 15 seconds if there's an active job
        setInterval(() => {
            if (currentJobId) {
                checkStatus();
            }
        }, 15000);

        // Load status and files on page load
        window.onload = function() {
            checkStatus();
            listFiles();
        };
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/list-files')
def list_files():
    """List all CSV files in the data directory"""
    try:
        data_dir = '/data'
        if not os.path.exists(data_dir):
            return jsonify({'success': False, 'error': 'Data directory not found', 'files': []})
        
        csv_files = []
        for filename in os.listdir(data_dir):
            if filename.endswith('.csv'):
                filepath = os.path.join(data_dir, filename)
                stat = os.stat(filepath)
                csv_files.append({
                    'name': filename,
                    'size': stat.st_size,
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                })
        
        # Sort by modification time, newest first
        csv_files.sort(key=lambda x: x['modified'], reverse=True)
        
        return jsonify({'success': True, 'files': csv_files})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'files': []})

@app.route('/test-connection')
def test_connection():
    """Test database connection"""
    try:
        print("[TEST] Testing database connection...", flush=True)
        sys.stdout.flush()
        
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        conn.close()
        
        print("[TEST] Database connection successful", flush=True)
        sys.stdout.flush()
        
        return jsonify({'success': True, 'message': 'Database connection successful'})
    except Exception as e:
        error_msg = str(e)
        print(f"[TEST] Database connection failed: {error_msg}", flush=True)
        sys.stdout.flush()
        
        return jsonify({'success': False, 'error': error_msg})

@app.route('/start-job', methods=['POST'])
def start_job():
    """Start a background processing job"""
    try:
        data = request.json
        companies = data['companies']
        batch_size = data.get('batch_size', 100)  # Reduced default
        input_format = data.get('input_format', 'slugs')
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        print(f"[API] Starting job {job_id} for {len(companies)} companies", flush=True)
        sys.stdout.flush()
        
        # Store job info
        with job_lock:
            jobs[job_id] = {
                'id': job_id,
                'status': 'queued',
                'created_at': datetime.now().isoformat(),
                'total_companies': len(companies),
                'batch_size': batch_size,
                'input_format': input_format
            }
        
        # Start background thread
        thread = threading.Thread(
            target=process_companies_background,
            args=(job_id, companies, batch_size, input_format)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'message': f'Job started processing {len(companies)} companies in background'
        })
        
    except Exception as e:
        print(f"[API] Error starting job: {e}", flush=True)
        sys.stdout.flush()
        return jsonify({'success': False, 'error': str(e)})

@app.route('/status')
def get_status():
    """Get status of all jobs"""
    try:
        with job_lock:
            job_list = list(jobs.values())
        
        # Sort by creation time, newest first
        job_list.sort(key=lambda x: x['created_at'], reverse=True)
        
        return jsonify({
            'api_success': True,
            'jobs': job_list
        })
    except Exception as e:
        return jsonify({'api_success': False, 'error': str(e), 'jobs': []})

@app.route('/download-csv/<filename>')
def download_csv(filename):
    """Download a specific CSV file"""
    try:
        filepath = os.path.join('/data', filename)
        if not os.path.exists(filepath) or not filename.endswith('.csv'):
            return 'File not found', 404
            
        with open(filepath, 'r') as f:
            content = f.read()
        
        return content, 200, {
            'Content-Type': 'text/csv',
            'Content-Disposition': f'attachment; filename={filename}'
        }
    except Exception as e:
        return f'Error reading file: {e}', 500

@app.route('/download-latest')
def download_latest():
    """Download the most recent CSV file"""
    try:
        data_dir = '/data'
        if not os.path.exists(data_dir):
            return 'No files found', 404
        
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        if not csv_files:
            return 'No CSV files found', 404
        
        # Get most recent file
        latest_file = max(csv_files, key=lambda f: os.path.getctime(os.path.join(data_dir, f)))
        
        return download_csv(latest_file)
        
    except Exception as e:
        return f'Error: {e}', 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)