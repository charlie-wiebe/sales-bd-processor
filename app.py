import os
import psycopg2
import pandas as pd
import time
from flask import Flask, request, jsonify, render_template_string
from typing import List
import io

app = Flask(__name__)

# Database configuration from environment
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': int(os.getenv('DB_PORT', 5432))
}

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
    </style>
</head>
<body>
    <h1>Sales/BD Employee Counter</h1>
    <form id="processForm">
        <div class="form-group">
            <label>Company Slugs (one per line):</label>
            <textarea id="companies" placeholder="nooksapp
hubspot
salesforce
..."></textarea>
        </div>
        <div class="form-group">
            <label>Batch Size:</label>
            <input type="number" id="batchSize" value="1000" min="100" max="5000">
        </div>
        <button type="submit">Process Companies</button>
    </form>
    
    <div id="results" class="results" style="display:none;">
        <h3>Processing Status</h3>
        <div id="status"></div>
        <div id="progress"></div>
        <div id="downloadLink"></div>
    </div>

    <script>
        document.getElementById('processForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const companies = document.getElementById('companies').value.trim().split('\\n').filter(c => c.trim());
            const batchSize = parseInt(document.getElementById('batchSize').value);
            
            if (companies.length === 0) {
                alert('Please enter company slugs');
                return;
            }
            
            document.getElementById('results').style.display = 'block';
            document.getElementById('status').innerHTML = `Processing ${companies.length} companies in batches of ${batchSize}...`;
            
            try {
                const response = await fetch('/process', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ companies, batch_size: batchSize })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    document.getElementById('status').innerHTML = `Complete! Found ${result.total_results} companies with Sales/BD employees.`;
                    document.getElementById('downloadLink').innerHTML = `<a href="/download/${result.filename}" download>Download Results CSV</a>`;
                } else {
                    document.getElementById('status').innerHTML = `Error: ${result.error}`;
                }
            } catch (error) {
                document.getElementById('status').innerHTML = `Error: ${error.message}`;
            }
        });
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/process', methods=['POST'])
def process_companies():
    try:
        data = request.json
        companies = data['companies']
        batch_size = data.get('batch_size', 1000)
        
        all_results = []
        
        # Process in batches
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(companies) + batch_size - 1) // batch_size
            
            print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} companies)")
            
            # Format for SQL IN clause
            slug_list = "'" + "', '".join(batch) + "'"
            query = QUERY_TEMPLATE.format(slug_list)
            
            # Execute query
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                df = pd.read_sql(query, conn)
                all_results.append(df)
                conn.close()
                
                print(f"Batch {batch_num} complete: {len(df)} companies with results")
                time.sleep(0.5)  # Be nice to the database
                
            except Exception as e:
                print(f"Error in batch {batch_num}: {e}")
                continue
        
        # Combine all results
        if all_results:
            final_df = pd.concat(all_results, ignore_index=True)
            filename = f'sales_bd_results_{int(time.time())}.csv'
            final_df.to_csv(f'/tmp/{filename}', index=False)
            
            return jsonify({
                'success': True,
                'total_results': len(final_df),
                'filename': filename
            })
        else:
            return jsonify({'success': False, 'error': 'No results found'})
            
    except Exception as e:
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