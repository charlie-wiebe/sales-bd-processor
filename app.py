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

# [REST OF THE CODE CONTINUES WITH EXISTING ROUTES...]