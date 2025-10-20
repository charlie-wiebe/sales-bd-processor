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

app = Flask(__name__)

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
    lcs.slug,
    COUNT(DISTINCT lp.id) AS sales_bd_count
FROM linkedin_profile_position3 lpp3
INNER JOIN linkedin_company lc ON lpp3.linkedin_company_id = lc.id
INNER JOIN linkedin_company_slug lcs ON lc.id = lcs.linkedin_company_id
INNER JOIN linkedin_profile lp ON lpp3.linkedin_profile_id = lp.id
LEFT JOIN company_position3 cp3 ON cp3.linkedin_profile_position3_id = lpp3.id
LEFT JOIN job_title jt ON cp3.job_title_id = jt.id
WHERE lcs.slug IN ({})
  AND lpp3.is_current = true
  AND lpp3.obsolete = false
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
GROUP BY lcs.slug
HAVING COUNT(DISTINCT lp.id) > 0;
"""

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Sales/BD Processor</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        .form-group { margin: 20px 0; }
        textarea { width: 100%; height: 200px; }
        button { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
        .results { margin-top: 30px; padding: 20px; background: #f8f9fa; border-radius: 4px; }
        .progress { margin: 10px 0; }
        .status { font-family: monospace; white-space: pre-wrap; }
        .example { background: #e9ecef; padding: 10px; border-radius: 4px; margin: 10px 0; font-family: monospace; }
        .progress-bar { width: 100%; height: 20px; background: #e9ecef; border-radius: 10px; margin: 10px 0; }
        .progress-fill { height: 100%; background: #007bff; border-radius: 10px; transition: width 0.3s; }
        .download-section { margin: 20px 0; padding: 15px; background: #d4edda; border-radius: 4px; }
    </style>
</head>
<body>
    <h1>Sales/BD Employee Counter</h1>
    <p>Process companies in batches to count Sales and Business Development employees.</p>
    
    <form id="processForm">
        <div class="form-group">
            <label>Input Format (choose one):</label>
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
            <input type="number" id="batchSize" value="1000" min="100" max="5000">
            <small>Larger batches = faster processing, but may timeout</small>
        </div>
        <button type="submit">Process Companies</button>
    </form>
    
    <div id="results" class="results" style="display:none;">
        <h3>Processing Status</h3>
        <div class="progress-bar">
            <div id="progressFill" class="progress-fill" style="width: 0%"></div>
        </div>
        <div id="progressText">0% complete (0 / 0 batches)</div>
        <div id="status" class="status"></div>
        <div id="downloadSection" class="download-section" style="display:none;">
            <strong>Partial Results Available:</strong>
            <div id="downloadLink"></div>
        </div>
    </div>

    <script>
        let currentResults = [];
        let totalBatches = 0;
        let completedBatches = 0;
        let isProcessing = false;

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

        function updateProgress(completed, total, message) {
            const percentage = total > 0 ? Math.round((completed / total) * 100) : 0;
            document.getElementById('progressFill').style.width = percentage + '%';
            document.getElementById('progressText').textContent = 
                `${percentage}% complete (${completed} / ${total} batches)`;
            
            if (message) {
                document.getElementById('status').textContent += message + '\\n';
            }
        }

        function savePartialResults() {
            if (currentResults.length > 0) {
                const filename = `partial_results_${Date.now()}.csv`;
                const csvContent = convertToCSV(currentResults);
                const blob = new Blob([csvContent], { type: 'text/csv' });
                const url = window.URL.createObjectURL(blob);
                
                document.getElementById('downloadSection').style.display = 'block';
                document.getElementById('downloadLink').innerHTML = 
                    `<a href="${url}" download="${filename}">Download Partial Results (${currentResults.length} companies)</a>`;
            }
        }

        function convertToCSV(data) {
            if (data.length === 0) return '';
            
            const headers = Object.keys(data[0]);
            const csvRows = [headers.join(',')];
            
            for (const row of data) {
                const values = headers.map(header => {
                    const value = row[header] || '';
                    return typeof value === 'string' && value.includes(',') ? `"${value}"` : value;
                });
                csvRows.push(values.join(','));
            }
            
            return csvRows.join('\\n');
        }
        
        document.getElementById('processForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            if (isProcessing) {
                alert('Processing already in progress. Please wait or refresh the page to start over.');
                return;
            }
            
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
            
            // Reset state
            currentResults = [];
            totalBatches = Math.ceil(companies.length / batchSize);
            completedBatches = 0;
            isProcessing = true;
            
            document.getElementById('results').style.display = 'block';
            document.getElementById('downloadSection').style.display = 'none';
            document.getElementById('status').textContent = `Starting processing of ${companies.length} companies in ${totalBatches} batches...\\n`;
            updateProgress(0, totalBatches);
            
            try {
                const response = await fetch('/process', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ companies, batch_size: batchSize, input_format: inputFormat })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    updateProgress(totalBatches, totalBatches, `\\nComplete! Found ${result.total_results} companies with Sales/BD employees.`);
                    document.getElementById('downloadLink').innerHTML = `<a href="/download/${result.filename}" download>Download Final Results CSV</a>`;
                    document.getElementById('downloadSection').style.display = 'block';
                } else {
                    updateProgress(completedBatches, totalBatches, `\\nError: ${result.error}`);
                    savePartialResults();
                }
            } catch (error) {
                updateProgress(completedBatches, totalBatches, `\\nError: ${error.message}`);
                savePartialResults();
            } finally {
                isProcessing = false;
            }
        });

        // Handle page unload
        window.addEventListener('beforeunload', function(e) {
            if (isProcessing) {
                e.preventDefault();
                e.returnValue = 'Processing is still in progress. Are you sure you want to leave?';
                return e.returnValue;
            }
        });
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/test-connection')
def test_connection():
    """Test database connection"""
    try:
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        conn.close()
        return jsonify({'success': True, 'message': 'Database connection successful'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/process', methods=['POST'])
def process_companies():
    try:
        data = request.json
        companies = data['companies']  # List of {slug: "...", hubspot_company_id: "..."} or {slug: "..."}
        batch_size = data.get('batch_size', 1000)
        input_format = data.get('input_format', 'slugs')
        
        db_config = get_db_config()
        all_results = []
        
        # Create lookup dict for HubSpot IDs if provided
        slug_to_hubspot_id = {}
        if input_format == 'csv':
            for company in companies:
                slug_to_hubspot_id[company['slug']] = company['hubspot_company_id']
        
        total_batches = (len(companies) + batch_size - 1) // batch_size
        
        # Process in batches
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            # Force output to flush immediately
            print(f"[BATCH {batch_num}/{total_batches}] Processing {len(batch)} companies...", flush=True)
            sys.stdout.flush()
            
            # Format for SQL IN clause - just the slugs
            slug_list = "'" + "', '".join([company['slug'] for company in batch]) + "'"
            query = QUERY_TEMPLATE.format(slug_list)
            
            # Execute query
            try:
                conn = psycopg2.connect(**db_config)
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Fetch results manually (no pandas)
                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                
                # Convert to list of dicts
                batch_results = []
                for row in results:
                    row_dict = dict(zip(column_names, row))
                    
                    # Add HubSpot company ID if provided
                    if input_format == 'csv':
                        row_dict['hubspot_company_id'] = slug_to_hubspot_id.get(row_dict['slug'], '')
                    
                    batch_results.append(row_dict)
                
                all_results.extend(batch_results)
                conn.close()
                
                print(f"[BATCH {batch_num}/{total_batches}] Complete: {len(batch_results)} companies with results (Total so far: {len(all_results)})", flush=True)
                sys.stdout.flush()
                
                # Brief pause to be nice to the database
                time.sleep(0.5)
                
            except Exception as e:
                print(f"[BATCH {batch_num}/{total_batches}] ERROR: {e}", flush=True)
                sys.stdout.flush()
                continue
        
        # Save results to CSV
        if all_results:
            filename = f'sales_bd_results_{int(time.time())}.csv'
            filepath = f'/tmp/{filename}'
            
            # Write CSV manually
            with open(filepath, 'w', newline='') as csvfile:
                fieldnames = all_results[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_results)
            
            print(f"[COMPLETE] Processed {total_batches} batches. Final results: {len(all_results)} companies with Sales/BD employees", flush=True)
            sys.stdout.flush()
            
            return jsonify({
                'success': True,
                'total_results': len(all_results),
                'filename': filename,
                'batches_processed': total_batches
            })
        else:
            print(f"[COMPLETE] Processed {total_batches} batches. No companies found with Sales/BD employees", flush=True)
            sys.stdout.flush()
            return jsonify({'success': False, 'error': 'No results found'})
            
    except Exception as e:
        print(f"[ERROR] Processing failed: {e}", flush=True)
        sys.stdout.flush()
        return jsonify({'success': False, 'error': str(e)})

@app.route('/download/<filename>')
def download_file(filename):
    try:
        with open(f'/tmp/{filename}', 'r') as f:
            content = f.read()
        
        return content, 200, {
            'Content-Type': 'text/csv',
            'Content-Disposition': f'attachment; filename={filename}'
        }
    except Exception as e:
        return f'File not found: {e}', 404

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)