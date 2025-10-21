#!/usr/bin/env python3
"""
Smart Sales BD Processor - Individual Processing with Persistent Deduplication
Processes companies one at a time with fault tolerance and resume capability
"""

import os
import psycopg2
import time
from flask import Flask, request, jsonify, render_template_string
from typing import List, Dict, Any, Optional
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
import hashlib
import logging
from pathlib import Path
import fcntl
import shutil
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global job storage
jobs = {}
job_lock = threading.Lock()

@dataclass
class ProcessingResult:
    """Structure for individual company processing results with both metrics"""
    company_slug: str
    company_name: str
    hubspot_company_id: str
    processed_at: str
    processing_time_seconds: float
    success: bool
    sales_bd_count: int = 0
    sdr_bdr_count: int = 0
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def get_hash(self) -> str:
        """Generate hash for deduplication based on company_slug"""
        return hashlib.md5(self.company_slug.encode()).hexdigest()

class SmartSalesBDProcessor:
    """Smart individual processor with persistent deduplication"""
    
    def __init__(self, data_dir: str = "/data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # File paths
        self.results_file = self.data_dir / "sales_bd_results.jsonl"
        self.processed_slugs_file = self.data_dir / "processed_slugs.txt"
        self.progress_file = self.data_dir / "progress.json"
        self.final_csv_file = self.data_dir / f"sales_bd_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # In-memory cache for fast lookups
        self.processed_slugs = set()
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        
        # Database config
        self.db_config = None
        
        # Load existing state
        self._load_state()
        
    def _load_state(self):
        """Load existing processed slugs and progress from disk"""
        try:
            # Load processed slugs
            if self.processed_slugs_file.exists():
                with open(self.processed_slugs_file, 'r') as f:
                    self.processed_slugs = set(line.strip() for line in f if line.strip())
                logger.info(f"Loaded {len(self.processed_slugs)} previously processed company slugs")
            
            # Load progress stats
            if self.progress_file.exists():
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    self.total_processed = progress.get('total_processed', 0)
                    self.total_successful = progress.get('total_successful', 0)
                    self.total_failed = progress.get('total_failed', 0)
                logger.info(f"Loaded progress: {self.total_processed} processed, {self.total_successful} successful, {self.total_failed} failed")
                
        except Exception as e:
            logger.warning(f"Could not load previous state: {e}")
    
    def _save_progress(self):
        """Save current progress to disk"""
        try:
            progress = {
                'total_processed': self.total_processed,
                'total_successful': self.total_successful,
                'total_failed': self.total_failed,
                'last_updated': datetime.now().isoformat()
            }
            
            # Atomic write
            temp_file = self.progress_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(progress, f, indent=2)
            temp_file.replace(self.progress_file)
            
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
    
    def _append_processed_slug(self, slug: str):
        """Append company slug to processed list (atomic operation)"""
        try:
            with open(self.processed_slugs_file, 'a') as f:
                # Use file locking for thread safety
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                f.write(f"{slug}\n")
                f.flush()
                os.fsync(f.fileno())
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                
            self.processed_slugs.add(slug)
            
        except Exception as e:
            logger.error(f"Failed to append processed slug {slug}: {e}")
    
    def _append_result(self, result: ProcessingResult):
        """Append result to results file (atomic operation)"""
        try:
            # Atomic append using temporary file and rename
            temp_file = self.results_file.with_suffix('.tmp')
            
            # If results file exists, copy it to temp first
            if self.results_file.exists():
                shutil.copy2(self.results_file, temp_file)
            
            # Append new result
            with open(temp_file, 'a') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                json.dump(result.to_dict(), f)
                f.write('\n')
                f.flush()
                os.fsync(f.fileno())
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            
            # Atomic replace
            temp_file.replace(self.results_file)
            
        except Exception as e:
            logger.error(f"Failed to append result for {result.company_slug}: {e}")
    
    def is_already_processed(self, slug: str) -> bool:
        """Check if company has already been processed"""
        return slug in self.processed_slugs
    
    def get_unprocessed_companies(self, all_companies: list) -> list:
        """Filter out already processed companies"""
        unprocessed = []
        for company in all_companies:
            slug = company.get('slug', '')
            if not self.is_already_processed(slug):
                unprocessed.append(company)
        
        logger.info(f"Found {len(unprocessed)} unprocessed companies out of {len(all_companies)} total")
        return unprocessed
    
    def process_single_company_master(self, company: Dict[str, Any]) -> ProcessingResult:
        """Process a single company with master query (both metrics in one shot)"""
        slug = company.get('slug', 'unknown')
        hubspot_id = company.get('hubspot_company_id', '')
        
        start_time = time.time()
        
        try:
            logger.debug(f"Processing company {slug} with master query")
            
            # Execute the master query for this single company
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Set query timeout
            cursor.execute("SET statement_timeout = '60s'")  # Longer timeout for master query
            
            # Use the master query template for single company
            query = MASTER_QUERY_TEMPLATE.format(f"'{slug}'")
            cursor.execute(query)
            
            result = cursor.fetchone()
            conn.close()
            
            processing_time = time.time() - start_time
            
            if result:
                # Master query returns: slug, sales_bd_count, sdr_bdr_count
                sales_bd_count = result[1] if result[1] is not None else 0
                sdr_bdr_count = result[2] if result[2] is not None else 0
                
                return ProcessingResult(
                    company_slug=slug,
                    company_name=slug,  # Using slug as name for now
                    hubspot_company_id=hubspot_id,
                    processed_at=datetime.now().isoformat(),
                    processing_time_seconds=processing_time,
                    success=True,
                    sales_bd_count=sales_bd_count,
                    sdr_bdr_count=sdr_bdr_count
                )
            else:
                return ProcessingResult(
                    company_slug=slug,
                    company_name=slug,
                    hubspot_company_id=hubspot_id,
                    processed_at=datetime.now().isoformat(),
                    processing_time_seconds=processing_time,
                    success=True,
                    sales_bd_count=0,
                    sdr_bdr_count=0
                )
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Failed to process company {slug} with master query: {e}")
            
            return ProcessingResult(
                company_slug=slug,
                company_name=slug,
                hubspot_company_id=hubspot_id,
                processed_at=datetime.now().isoformat(),
                processing_time_seconds=processing_time,
                success=False,
                error_message=str(e)
            )
    
    def process_single_company(self, company: Dict[str, Any]) -> ProcessingResult:
        """Process a single company with sales/BD query only (legacy method)"""
        slug = company.get('slug', 'unknown')
        hubspot_id = company.get('hubspot_company_id', '')
        
        start_time = time.time()
        
        try:
            logger.debug(f"Processing company {slug} for sales/BD count")
            
            # Execute the sales BD query for this single company
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Set query timeout
            cursor.execute("SET statement_timeout = '30s'")
            
            # Use the existing query template for single company
            query = QUERY_TEMPLATE.format(f"'{slug}'")
            cursor.execute(query)
            
            result = cursor.fetchone()
            conn.close()
            
            processing_time = time.time() - start_time
            
            if result:
                sales_bd_count = result[1]  # Second column is the count
                return ProcessingResult(
                    company_slug=slug,
                    company_name=slug,  # Using slug as name for now
                    hubspot_company_id=hubspot_id,
                    processed_at=datetime.now().isoformat(),
                    processing_time_seconds=processing_time,
                    success=True,
                    sales_bd_count=sales_bd_count,
                    sdr_bdr_count=0  # Not calculated in this method
                )
            else:
                return ProcessingResult(
                    company_slug=slug,
                    company_name=slug,
                    hubspot_company_id=hubspot_id,
                    processed_at=datetime.now().isoformat(),
                    processing_time_seconds=processing_time,
                    success=True,
                    sales_bd_count=0,
                    sdr_bdr_count=0
                )
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Failed to process company {slug}: {e}")
            
            return ProcessingResult(
                company_slug=slug,
                company_name=slug,
                hubspot_company_id=hubspot_id,
                processed_at=datetime.now().isoformat(),
                processing_time_seconds=processing_time,
                success=False,
                error_message=str(e)
            )
    
    def process_single_company_sdr(self, company: Dict[str, Any]) -> ProcessingResult:
        """Process a single company with SDR/BDR database query only (legacy method)"""
        slug = company.get('slug', 'unknown')
        hubspot_id = company.get('hubspot_company_id', '')
        
        start_time = time.time()
        
        try:
            logger.debug(f"Processing company {slug} for SDR count")
            
            # Execute the SDR query for this single company
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Set query timeout
            cursor.execute("SET statement_timeout = '30s'")
            
            # Use the SDR query template for single company
            query = SDR_QUERY_TEMPLATE.format(f"'{slug}'")
            cursor.execute(query)
            
            result = cursor.fetchone()
            conn.close()
            
            processing_time = time.time() - start_time
            
            if result:
                sdr_bdr_count = result[1]  # Second column is the count
                return ProcessingResult(
                    company_slug=slug,
                    company_name=slug,  # Using slug as name for now
                    hubspot_company_id=hubspot_id,
                    processed_at=datetime.now().isoformat(),
                    processing_time_seconds=processing_time,
                    success=True,
                    sales_bd_count=0,  # Not calculated in this method
                    sdr_bdr_count=sdr_bdr_count
                )
            else:
                return ProcessingResult(
                    company_slug=slug,
                    company_name=slug,
                    hubspot_company_id=hubspot_id,
                    processed_at=datetime.now().isoformat(),
                    processing_time_seconds=processing_time,
                    success=True,
                    sales_bd_count=0,
                    sdr_bdr_count=0
                )
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Failed to process company {slug} for SDR count: {e}")
            
            return ProcessingResult(
                company_slug=slug,
                company_name=slug,
                hubspot_company_id=hubspot_id,
                processed_at=datetime.now().isoformat(),
                processing_time_seconds=processing_time,
                success=False,
                error_message=str(e)
            )
    
    def process_companies_batch(self, companies: List[Dict[str, Any]], batch_size: int = 1, use_master_query: bool = True) -> List[ProcessingResult]:
        """Process companies in batches with configurable batch size"""
        results = []
        
        # Process companies in batches
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            
            if batch_size == 1:
                # Individual processing - use master query for both metrics
                company = batch[0]
                if use_master_query:
                    result = self.process_single_company_master(company)
                else:
                    result = self.process_single_company(company)
                results.append(result)
            else:
                # Batch processing - use batch query
                batch_results = self._process_batch_query(batch, use_master_query)
                results.extend(batch_results)
        
        return results
    
    def _process_batch_query(self, companies: List[Dict[str, Any]], use_master_query: bool = True) -> List[ProcessingResult]:
        """Process a batch of companies with a single database query"""
        if not companies:
            return []
        
        start_time = time.time()
        slugs = [company.get('slug', '') for company in companies]
        slug_list = "', '".join(slugs)
        
        try:
            logger.debug(f"Processing batch of {len(companies)} companies")
            
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Set query timeout based on batch size
            timeout = min(300, 30 + (len(companies) * 5))  # 30s base + 5s per company, max 5 minutes
            cursor.execute(f"SET statement_timeout = '{timeout}s'")
            
            if use_master_query:
                # Use master query for both metrics
                query = MASTER_QUERY_TEMPLATE.format(f"'{slug_list}'")
            else:
                # Use sales/BD query only
                query = QUERY_TEMPLATE.format(f"'{slug_list}'")
            
            cursor.execute(query)
            query_results = cursor.fetchall()
            conn.close()
            
            processing_time = time.time() - start_time
            
            # Create a mapping of slug to results
            result_map = {}
            for row in query_results:
                slug = row[0]
                if use_master_query:
                    # Master query: slug, sales_bd_count, sdr_bdr_count
                    result_map[slug] = {
                        'sales_bd_count': row[1] if row[1] is not None else 0,
                        'sdr_bdr_count': row[2] if row[2] is not None else 0
                    }
                else:
                    # Sales/BD query: slug, sales_bd_count
                    result_map[slug] = {
                        'sales_bd_count': row[1] if row[1] is not None else 0,
                        'sdr_bdr_count': 0
                    }
            
            # Create ProcessingResult for each company
            results = []
            for company in companies:
                slug = company.get('slug', 'unknown')
                hubspot_id = company.get('hubspot_company_id', '')
                
                if slug in result_map:
                    counts = result_map[slug]
                    results.append(ProcessingResult(
                        company_slug=slug,
                        company_name=slug,
                        hubspot_company_id=hubspot_id,
                        processed_at=datetime.now().isoformat(),
                        processing_time_seconds=processing_time / len(companies),  # Distribute time across batch
                        success=True,
                        sales_bd_count=counts['sales_bd_count'],
                        sdr_bdr_count=counts['sdr_bdr_count']
                    ))
                else:
                    # Company not found in results
                    results.append(ProcessingResult(
                        company_slug=slug,
                        company_name=slug,
                        hubspot_company_id=hubspot_id,
                        processed_at=datetime.now().isoformat(),
                        processing_time_seconds=processing_time / len(companies),
                        success=True,
                        sales_bd_count=0,
                        sdr_bdr_count=0
                    ))
            
            return results
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Failed to process batch of {len(companies)} companies: {e}")
            
            # Return failed results for all companies in batch
            return [ProcessingResult(
                company_slug=company.get('slug', 'unknown'),
                company_name=company.get('slug', 'unknown'),
                hubspot_company_id=company.get('hubspot_company_id', ''),
                processed_at=datetime.now().isoformat(),
                processing_time_seconds=processing_time / len(companies),
                success=False,
                error_message=str(e)
            ) for company in companies]
    
    def process_all_with_batch(self, companies: list, job_id: str, batch_size: int = 1):
        """Process all companies with configurable batch size (default 1 for individual)"""
        
        # Filter out already processed companies
        unprocessed = self.get_unprocessed_companies(companies)
        
        if not unprocessed:
            logger.info("No unprocessed companies found. All done!")
            with job_lock:
                jobs[job_id]['status'] = 'completed'
                jobs[job_id]['message'] = 'All companies already processed'
            return
        
        processing_mode = "individual" if batch_size == 1 else f"batch (size {batch_size})"
        logger.info(f"Starting {processing_mode} processing of {len(unprocessed)} companies...")
        
        with job_lock:
            jobs[job_id]['status'] = 'processing'
            jobs[job_id]['total_unprocessed'] = len(unprocessed)
            jobs[job_id]['batch_size'] = batch_size
            jobs[job_id]['started_at'] = datetime.now().isoformat()
        
        start_time = time.time()
        
        # Process companies in batches
        for i in range(0, len(unprocessed), batch_size):
            batch = unprocessed[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(unprocessed) + batch_size - 1) // batch_size
            
            try:
                if batch_size == 1:
                    slug = batch[0].get('slug', 'unknown')
                    logger.info(f"[{job_id}] [{i+1}/{len(unprocessed)}] Processing company {slug}...")
                else:
                    slugs = [c.get('slug', 'unknown') for c in batch]
                    logger.info(f"[{job_id}] [Batch {batch_num}/{total_batches}] Processing {len(batch)} companies: {', '.join(slugs[:3])}{'...' if len(batch) > 3 else ''}")
                
                # Process batch using new method with master query
                batch_results = self.process_companies_batch(batch, batch_size=batch_size, use_master_query=True)
                
                # Save results and update counters
                for result in batch_results:
                    slug = result.company_slug
                    
                    # Save result immediately (persistent)
                    self._append_result(result)
                    self._append_processed_slug(slug)
                    
                    # Update counters
                    self.total_processed += 1
                    if result.success:
                        self.total_successful += 1
                        logger.info(f"[{job_id}] ✅ SUCCESS: {slug} - Sales/BD: {result.sales_bd_count}, SDR/BDR: {result.sdr_bdr_count} ({result.processing_time_seconds:.2f}s)")
                    else:
                        self.total_failed += 1
                        logger.warning(f"[{job_id}] ❌ FAILED: {slug} - {result.error_message}")
                
                # Update job status
                with job_lock:
                    jobs[job_id]['processed_count'] = min(i + batch_size, len(unprocessed))
                    jobs[job_id]['total_successful'] = self.total_successful
                    jobs[job_id]['total_failed'] = self.total_failed
                    if batch_size == 1:
                        jobs[job_id]['current_company'] = batch[0].get('slug', 'unknown')
                    else:
                        jobs[job_id]['current_batch'] = f"Batch {batch_num}/{total_batches}"
                
                # Save progress every 10 companies
                if i % 10 == 0:
                    self._save_progress()
                    elapsed = time.time() - start_time
                    rate = i / elapsed * 60  # companies per minute
                    logger.info(f"[{job_id}] Progress: {i}/{len(unprocessed)} ({i/len(unprocessed)*100:.1f}%) - Rate: {rate:.1f} companies/min")
                
                # Brief pause to be nice to the database
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"[{job_id}] Unexpected error processing {slug}: {e}")
                self.total_failed += 1
                continue
        
        # Generate final CSV
        self._generate_final_csv()
        
        # Final save
        self._save_progress()
        
        elapsed = time.time() - start_time
        with job_lock:
            jobs[job_id]['status'] = 'completed'
            jobs[job_id]['completed_at'] = datetime.now().isoformat()
            jobs[job_id]['final_csv'] = self.final_csv_file.name
            jobs[job_id]['processing_time_seconds'] = elapsed
        
        logger.info(f"[{job_id}] 🎉 Individual processing complete!")
        logger.info(f"[{job_id}] 📊 Stats: {self.total_successful} successful, {self.total_failed} failed, {elapsed:.1f}s total")
    
    def process_all_individual_sdr(self, companies: list, job_id: str):
        """Process all companies individually for SDR/BDR count with smart resume"""
        
        # Setup SDR-specific file paths
        sdr_results_file = self.data_dir / "sdr_bdr_results.jsonl"
        sdr_processed_slugs_file = self.data_dir / "sdr_processed_slugs.txt"
        sdr_progress_file = self.data_dir / "sdr_progress.json"
        sdr_final_csv_file = self.data_dir / f"sdr_bdr_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Load existing SDR processed slugs
        sdr_processed_slugs = set()
        if sdr_processed_slugs_file.exists():
            with open(sdr_processed_slugs_file, 'r') as f:
                sdr_processed_slugs = set(line.strip() for line in f if line.strip())
            logger.info(f"Loaded {len(sdr_processed_slugs)} previously processed SDR company slugs")
        
        # Filter out already processed companies
        unprocessed = []
        for company in companies:
            slug = company.get('slug', '')
            if slug not in sdr_processed_slugs:
                unprocessed.append(company)
        
        if not unprocessed:
            logger.info("No unprocessed SDR companies found. All done!")
            with job_lock:
                jobs[job_id]['status'] = 'completed'
                jobs[job_id]['message'] = 'All SDR companies already processed'
            return
        
        logger.info(f"Starting individual SDR processing of {len(unprocessed)} companies...")
        
        with job_lock:
            jobs[job_id]['status'] = 'processing'
            jobs[job_id]['total_unprocessed'] = len(unprocessed)
            jobs[job_id]['started_at'] = datetime.now().isoformat()
        
        start_time = time.time()
        sdr_successful = 0
        sdr_failed = 0
        
        for i, company in enumerate(unprocessed, 1):
            slug = company.get('slug', 'unknown')
            
            try:
                logger.info(f"[{job_id}] [{i}/{len(unprocessed)}] Processing SDR company {slug}...")
                
                # Process individual company for SDR count
                result = self.process_single_company_sdr(company)
                
                # Save result immediately (persistent)
                self._append_result_to_file(result, sdr_results_file)
                self._append_slug_to_file(slug, sdr_processed_slugs_file)
                sdr_processed_slugs.add(slug)
                
                # Update counters
                if result.success:
                    sdr_successful += 1
                    logger.info(f"[{job_id}] ✅ SUCCESS: {slug} - {result.sales_bd_count} SDR/BDR ({result.processing_time_seconds:.2f}s)")
                else:
                    sdr_failed += 1
                    logger.warning(f"[{job_id}] ❌ FAILED: {slug} - {result.error_message}")
                
                # Update job status
                with job_lock:
                    jobs[job_id]['processed_count'] = i
                    jobs[job_id]['total_successful'] = sdr_successful
                    jobs[job_id]['total_failed'] = sdr_failed
                    jobs[job_id]['current_company'] = slug
                
                # Save progress every 10 companies
                if i % 10 == 0:
                    self._save_sdr_progress(sdr_successful, sdr_failed, i, sdr_progress_file)
                    elapsed = time.time() - start_time
                    rate = i / elapsed * 60  # companies per minute
                    logger.info(f"[{job_id}] SDR Progress: {i}/{len(unprocessed)} ({i/len(unprocessed)*100:.1f}%) - Rate: {rate:.1f} companies/min")
                
                # Brief pause to be nice to the database
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"[{job_id}] Unexpected error processing SDR {slug}: {e}")
                sdr_failed += 1
                continue
        
        # Generate final SDR CSV
        self._generate_sdr_final_csv(sdr_results_file, sdr_final_csv_file)
        
        # Final save
        self._save_sdr_progress(sdr_successful, sdr_failed, len(unprocessed), sdr_progress_file)
        
        elapsed = time.time() - start_time
        with job_lock:
            jobs[job_id]['status'] = 'completed'
            jobs[job_id]['completed_at'] = datetime.now().isoformat()
            jobs[job_id]['final_csv'] = sdr_final_csv_file.name
            jobs[job_id]['processing_time_seconds'] = elapsed
        
        logger.info(f"[{job_id}] 🎉 Individual SDR processing complete!")
        logger.info(f"[{job_id}] 📊 SDR Stats: {sdr_successful} successful, {sdr_failed} failed, {elapsed:.1f}s total")
    
    def _append_result_to_file(self, result: ProcessingResult, results_file):
        """Append result to specified results file (atomic operation)"""
        try:
            # Atomic append using temporary file and rename
            temp_file = results_file.with_suffix('.tmp')
            
            # If results file exists, copy it to temp first
            if results_file.exists():
                shutil.copy2(results_file, temp_file)
            
            # Append new result
            with open(temp_file, 'a') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                json.dump(result.to_dict(), f)
                f.write('\n')
                f.flush()
                os.fsync(f.fileno())
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            
            # Atomic replace
            temp_file.replace(results_file)
            
        except Exception as e:
            logger.error(f"Failed to append result for {result.company_slug}: {e}")
    
    def _append_slug_to_file(self, slug: str, slugs_file):
        """Append company slug to specified processed list (atomic operation)"""
        try:
            with open(slugs_file, 'a') as f:
                # Use file locking for thread safety
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                f.write(f"{slug}\n")
                f.flush()
                os.fsync(f.fileno())
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                
        except Exception as e:
            logger.error(f"Failed to append processed slug {slug}: {e}")
    
    def _save_sdr_progress(self, successful: int, failed: int, processed: int, progress_file):
        """Save SDR progress to specified file"""
        try:
            progress = {
                'total_processed': processed,
                'total_successful': successful,
                'total_failed': failed,
                'last_updated': datetime.now().isoformat()
            }
            
            # Atomic write
            temp_file = progress_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(progress, f, indent=2)
            temp_file.replace(progress_file)
            
        except Exception as e:
            logger.error(f"Failed to save SDR progress: {e}")
    
    def _generate_sdr_final_csv(self, results_file, final_csv_file):
        """Generate final SDR CSV from JSONL results"""
        try:
            if not results_file.exists():
                return
            
            results = []
            with open(results_file, 'r') as f:
                for line in f:
                    if line.strip():
                        result = json.loads(line.strip())
                        if result.get('success', False):
                            results.append({
                                'slug': result['company_slug'],
                                'hubspot_company_id': result.get('hubspot_company_id', ''),
                                'sdr_bdr_count': result.get('sales_bd_count', 0)  # Reusing field name
                            })
            
            if results:
                with open(final_csv_file, 'w', newline='') as csvfile:
                    fieldnames = ['slug', 'hubspot_company_id', 'sdr_bdr_count']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(results)
                
                logger.info(f"Generated final SDR CSV with {len(results)} successful results: {final_csv_file.name}")
            
        except Exception as e:
            logger.error(f"Failed to generate final SDR CSV: {e}")
    
    def _generate_final_csv(self):
        """Generate final CSV from JSONL results with both metrics"""
        try:
            if not self.results_file.exists():
                logger.warning("Results file does not exist, cannot generate CSV")
                return
            
            results = []
            with open(self.results_file, 'r') as f:
                for line in f:
                    result = json.loads(line)
                    if result.get('success', False):
                        results.append({
                            'slug': result['company_slug'],
                            'hubspot_company_id': result.get('hubspot_company_id', ''),
                            'sales_bd_count': result.get('sales_bd_count', 0),
                            'sdr_bdr_count': result.get('sdr_bdr_count', 0)
                        })
            
            if results:
                with open(self.final_csv_file, 'w', newline='') as csvfile:
                    fieldnames = ['slug', 'hubspot_company_id', 'sales_bd_count', 'sdr_bdr_count']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(results)
                
                logger.info(f"Generated final CSV with {len(results)} successful results (both metrics): {self.final_csv_file.name}")
            
        except Exception as e:
            logger.error(f"Failed to generate final CSV: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        return {
            'total_processed': self.total_processed,
            'total_successful': self.total_successful,
            'total_failed': self.total_failed,
            'processed_slugs_count': len(self.processed_slugs),
            'results_file_exists': self.results_file.exists(),
            'results_file_size_mb': self.results_file.stat().st_size / 1024 / 1024 if self.results_file.exists() else 0,
            'final_csv_exists': self.final_csv_file.exists(),
            'final_csv_size_mb': self.final_csv_file.stat().st_size / 1024 / 1024 if self.final_csv_file.exists() else 0
        }

# Global processor instance
processor = SmartSalesBDProcessor()

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

# Optimized Tag-First Sales/BD Query
QUERY_TEMPLATE = """
-- Tag-first but optimized for performance
WITH target_company AS (
    SELECT lc.id as company_id, lcs.slug
    FROM linkedin_company_slug lcs
    INNER JOIN linkedin_company lc ON lc.id = lcs.linkedin_company_id
    WHERE lcs.slug IN ({}) AND lc.slug_status = 'A'
),
sales_bd_positions AS (
    SELECT DISTINCT
        lpp3.linkedin_profile_id,
        lpp3.title,
        lp.first_name,
        lp.last_name,
        tc.slug
    FROM target_company tc
    INNER JOIN linkedin_profile_position3 lpp3 ON lpp3.linkedin_company_id = tc.company_id
    INNER JOIN linkedin_profile lp ON lpp3.linkedin_profile_id = lp.id
    -- OPTIMIZED: Use INNER JOINs instead of LEFT JOINs (faster, more accurate)
    INNER JOIN company_position3 cp3 ON cp3.linkedin_profile_position3_id = lpp3.id
    INNER JOIN job_title jt ON cp3.job_title_id = jt.id
    WHERE lp.slug_status = 'A'
      AND lpp3.is_current = true
      AND lpp3.obsolete = false
      AND (lpp3.end_date IS NULL OR lpp3.end_date > CURRENT_DATE)
      -- TAG-FIRST: Use either tag 31 OR tag 45 (fixed from original AND logic)
      AND (jt.tags && ARRAY[31] OR jt.tags && ARRAY[45])
      -- EXCLUSIONS using ILIKE for GIN trigram optimization
      AND NOT (
          lpp3.title ILIKE '%investor%' OR lpp3.title ILIKE '%advisor%'
          OR lpp3.title ILIKE '%board%' OR lpp3.title ILIKE '%founder%'
          OR lpp3.title ILIKE '%vp%' OR lpp3.title ILIKE '%vice president%'
          OR lpp3.title ILIKE '%cro%' OR lpp3.title ILIKE '%chief%'
          OR lpp3.title ILIKE '%director%' OR lpp3.title ILIKE '%head of%'
          OR lpp3.title ILIKE '%coordinator%' OR lpp3.title ILIKE '%assistant%'
          OR lpp3.title ILIKE '%customer success%' OR lpp3.title ILIKE '%client success%'
          OR lpp3.title ILIKE '%support%' OR lpp3.title ILIKE '%manager%'
          OR lpp3.title ILIKE '%solutions%' OR lpp3.title ILIKE '%owner%'
          OR lpp3.title ILIKE '%ceo%' OR lpp3.title ILIKE '%partner%'
          OR lpp3.title ILIKE '%operations%' OR lpp3.title ILIKE '%enablement%'
      )
)
SELECT 
    slug,
    COUNT(*) AS sales_bd_count
FROM (
    SELECT DISTINCT ON (LOWER(sbp.first_name), LOWER(sbp.last_name), sbp.slug)
        sbp.slug,
        sbp.linkedin_profile_id
    FROM sales_bd_positions sbp
    ORDER BY LOWER(sbp.first_name), LOWER(sbp.last_name), sbp.slug
) AS deduplicated_people
GROUP BY slug;
"""

# Optimized Smart SDR/BDR Query - Individual processing with expanded traditional list
SDR_QUERY_TEMPLATE = """
-- Ultra-optimized Smart SDR Query - Only check prior roles when necessary
WITH target_company AS (
    SELECT lc.id as company_id, lcs.slug
    FROM linkedin_company_slug lcs
    INNER JOIN linkedin_company lc ON lc.id = lcs.linkedin_company_id
    WHERE lcs.slug IN ({}) AND lc.slug_status = 'A'
),
current_positions AS (
    SELECT DISTINCT
        lpp3.linkedin_profile_id,
        lpp3.title,
        lp.first_name,
        lp.last_name,
        tc.slug,
        CASE 
            WHEN (
                -- Traditional SDR titles - IMMEDIATE INCLUDE (EXPANDED LIST)
                lpp3.title ILIKE 'sdr' OR lpp3.title ILIKE 'bdr'
                OR lpp3.title ILIKE 'senior sdr' OR lpp3.title ILIKE 'senior bdr'
                OR lpp3.title ILIKE 'associate sdr' OR lpp3.title ILIKE 'associate bdr'
                OR lpp3.title ILIKE 'junior sdr' OR lpp3.title ILIKE 'junior bdr'
                OR lpp3.title ILIKE '%sales development representative%'
                OR lpp3.title ILIKE '%business development representative%'
                -- EXPANDED ADDITIONS:
                OR lpp3.title ILIKE '%business development%'
                OR lpp3.title ILIKE '%sales development%'
                OR lpp3.title ILIKE '%account development representative%'
                OR lpp3.title ILIKE '%account development%'
                OR lpp3.title ILIKE '%sales development associate%'
                OR lpp3.title ILIKE '%inside sales representative%'
            ) THEN 'include_immediately'
            WHEN (
                -- Creative titles - NEED TO CHECK PRIOR ROLE (CLEANED UP)
                lpp3.title ILIKE '%growth specialist%' OR lpp3.title ILIKE '%growth representative%'
                OR lpp3.title ILIKE '%account engagement specialist%'
                OR lpp3.title ILIKE '%inbound success coach%'
                OR lpp3.title ILIKE '%lead development%'
            ) THEN 'check_prior_role'
            ELSE 'exclude'
        END as classification
    FROM target_company tc
    INNER JOIN linkedin_profile_position3 lpp3 ON lpp3.linkedin_company_id = tc.company_id
    INNER JOIN linkedin_profile lp ON lpp3.linkedin_profile_id = lp.id
    WHERE lp.slug_status = 'A'
      AND lpp3.is_current = true
      AND lpp3.obsolete = false
      AND (lpp3.end_date IS NULL OR lpp3.end_date > CURRENT_DATE)
      AND (
          lpp3.title ILIKE '%sdr%' OR lpp3.title ILIKE '%bdr%'
          OR lpp3.title ILIKE '%sales development%' OR lpp3.title ILIKE '%business development%'
          OR lpp3.title ILIKE '%growth specialist%' OR lpp3.title ILIKE '%growth representative%'
          OR lpp3.title ILIKE '%account engagement%' OR lpp3.title ILIKE '%lead development%'
          OR lpp3.title ILIKE '%account development%' OR lpp3.title ILIKE '%inside sales representative%'
      )
      AND NOT (
          lpp3.title ILIKE '%manager%' OR lpp3.title ILIKE '%director%'
          OR lpp3.title ILIKE '%vp%' OR lpp3.title ILIKE '%vice president%'
          OR lpp3.title ILIKE '%head of%' OR lpp3.title ILIKE '%chief%'
          OR lpp3.title ILIKE '%founder%' OR lpp3.title ILIKE '%ceo%' OR lpp3.title ILIKE '%cro%'
      )
),
-- ONLY check prior roles for people who need it (massive performance gain)
final_results AS (
    SELECT 
        linkedin_profile_id, first_name, last_name, slug,
        'include' as decision
    FROM current_positions 
    WHERE classification = 'include_immediately'
    
    UNION ALL
    
    SELECT 
        cp.linkedin_profile_id, cp.first_name, cp.last_name, cp.slug,
        CASE 
            WHEN EXISTS (
                SELECT 1
                FROM linkedin_profile_position3 lpp3_prior
                WHERE lpp3_prior.linkedin_profile_id = cp.linkedin_profile_id
                  AND lpp3_prior.linkedin_company_id != (
                      SELECT lpp3_current.linkedin_company_id 
                      FROM linkedin_profile_position3 lpp3_current 
                      WHERE lpp3_current.linkedin_profile_id = cp.linkedin_profile_id 
                        AND lpp3_current.is_current = true 
                      LIMIT 1
                  )
                  AND lpp3_prior.obsolete = false
                  AND lpp3_prior.end_date IS NOT NULL
                  AND (
                      lpp3_prior.title ILIKE 'sdr' OR lpp3_prior.title ILIKE 'bdr'
                      OR lpp3_prior.title ILIKE 'senior sdr' OR lpp3_prior.title ILIKE 'senior bdr'
                      OR lpp3_prior.title ILIKE '%sales development representative%'
                      OR lpp3_prior.title ILIKE '%business development representative%'
                  )
                  AND NOT (
                      lpp3_prior.title ILIKE '%manager%' OR lpp3_prior.title ILIKE '%director%'
                      OR lpp3_prior.title ILIKE '%vp%' OR lpp3_prior.title ILIKE '%head of%'
                  )
                ORDER BY lpp3_prior.end_date DESC
                LIMIT 1
            ) THEN 'include'
            ELSE 'exclude'
        END as decision
    FROM current_positions cp
    WHERE classification = 'check_prior_role'
)
SELECT 
    slug,
    COUNT(*) AS sdr_bdr_count
FROM (
    SELECT DISTINCT ON (LOWER(fr.first_name), LOWER(fr.last_name), fr.slug)
        fr.slug,
        fr.linkedin_profile_id
    FROM final_results fr
    WHERE fr.decision = 'include'
    ORDER BY LOWER(fr.first_name), LOWER(fr.last_name), fr.slug
) AS deduplicated_sdrs
GROUP BY slug;
"""

# Master Query Template - Gets both metrics in one shot for maximum efficiency
MASTER_QUERY_TEMPLATE = """
-- Ultra-optimized Master Query - Gets both Sales/BD total AND SDR/BDR count
WITH target_company AS (
    SELECT lc.id as company_id, lcs.slug
    FROM linkedin_company_slug lcs
    INNER JOIN linkedin_company lc ON lc.id = lcs.linkedin_company_id
    WHERE lcs.slug IN ({}) AND lc.slug_status = 'A'
),
-- GET ALL SALES/BD PEOPLE (Tag-based)
all_sales_bd AS (
    SELECT DISTINCT
        lpp3.linkedin_profile_id,
        lpp3.title,
        lp.first_name,
        lp.last_name,
        tc.slug,
        'sales_bd' as category
    FROM target_company tc
    INNER JOIN linkedin_profile_position3 lpp3 ON lpp3.linkedin_company_id = tc.company_id
    INNER JOIN linkedin_profile lp ON lpp3.linkedin_profile_id = lp.id
    INNER JOIN company_position3 cp3 ON cp3.linkedin_profile_position3_id = lpp3.id
    INNER JOIN job_title jt ON cp3.job_title_id = jt.id
    WHERE lp.slug_status = 'A'
      AND lpp3.is_current = true
      AND lpp3.obsolete = false
      AND (lpp3.end_date IS NULL OR lpp3.end_date > CURRENT_DATE)
      AND (jt.tags && ARRAY[31] OR jt.tags && ARRAY[45])
      AND NOT (
          lpp3.title ILIKE '%investor%' OR lpp3.title ILIKE '%advisor%'
          OR lpp3.title ILIKE '%board%' OR lpp3.title ILIKE '%founder%'
          OR lpp3.title ILIKE '%vp%' OR lpp3.title ILIKE '%vice president%'
          OR lpp3.title ILIKE '%cro%' OR lpp3.title ILIKE '%chief%'
          OR lpp3.title ILIKE '%director%' OR lpp3.title ILIKE '%head of%'
          OR lpp3.title ILIKE '%coordinator%' OR lpp3.title ILIKE '%assistant%'
          OR lpp3.title ILIKE '%customer success%' OR lpp3.title ILIKE '%client success%'
          OR lpp3.title ILIKE '%support%' OR lpp3.title ILIKE '%manager%'
          OR lpp3.title ILIKE '%solutions%' OR lpp3.title ILIKE '%owner%'
          OR lpp3.title ILIKE '%ceo%' OR lpp3.title ILIKE '%partner%'
          OR lpp3.title ILIKE '%operations%' OR lpp3.title ILIKE '%enablement%'
      )
),
-- GET SDR/BDR SUBSET (Smart individual processing)
sdr_candidates AS (
    SELECT DISTINCT
        lpp3.linkedin_profile_id,
        lpp3.title,
        lp.first_name,
        lp.last_name,
        tc.slug,
        CASE 
            WHEN (
                -- Traditional SDR titles - IMMEDIATE INCLUDE
                lpp3.title ILIKE 'sdr' OR lpp3.title ILIKE 'bdr'
                OR lpp3.title ILIKE 'senior sdr' OR lpp3.title ILIKE 'senior bdr'
                OR lpp3.title ILIKE 'associate sdr' OR lpp3.title ILIKE 'associate bdr'
                OR lpp3.title ILIKE 'junior sdr' OR lpp3.title ILIKE 'junior bdr'
                OR lpp3.title ILIKE '%sales development representative%'
                OR lpp3.title ILIKE '%business development representative%'
                OR lpp3.title ILIKE '%business development%'
                OR lpp3.title ILIKE '%sales development%'
                OR lpp3.title ILIKE '%account development representative%'
                OR lpp3.title ILIKE '%account development%'
                OR lpp3.title ILIKE '%sales development associate%'
                OR lpp3.title ILIKE '%inside sales representative%'
            ) THEN 'include_immediately'
            WHEN (
                -- Creative titles - CHECK PRIOR ROLE
                lpp3.title ILIKE '%growth specialist%' OR lpp3.title ILIKE '%growth representative%'
                OR lpp3.title ILIKE '%account engagement specialist%'
                OR lpp3.title ILIKE '%inbound success coach%'
                OR lpp3.title ILIKE '%lead development%'
            ) THEN 'check_prior_role'
            ELSE 'exclude'
        END as sdr_classification
    FROM target_company tc
    INNER JOIN linkedin_profile_position3 lpp3 ON lpp3.linkedin_company_id = tc.company_id
    INNER JOIN linkedin_profile lp ON lpp3.linkedin_profile_id = lp.id
    WHERE lp.slug_status = 'A'
      AND lpp3.is_current = true
      AND lpp3.obsolete = false
      AND (lpp3.end_date IS NULL OR lpp3.end_date > CURRENT_DATE)
      AND (
          lpp3.title ILIKE '%sdr%' OR lpp3.title ILIKE '%bdr%'
          OR lpp3.title ILIKE '%sales development%' OR lpp3.title ILIKE '%business development%'
          OR lpp3.title ILIKE '%growth specialist%' OR lpp3.title ILIKE '%growth representative%'
          OR lpp3.title ILIKE '%account engagement%' OR lpp3.title ILIKE '%lead development%'
          OR lpp3.title ILIKE '%account development%' OR lpp3.title ILIKE '%inside sales representative%'
      )
      AND NOT (
          lpp3.title ILIKE '%manager%' OR lpp3.title ILIKE '%director%'
          OR lpp3.title ILIKE '%vp%' OR lpp3.title ILIKE '%vice president%'
          OR lpp3.title ILIKE '%head of%' OR lpp3.title ILIKE '%chief%'
          OR lpp3.title ILIKE '%founder%' OR lpp3.title ILIKE '%ceo%' OR lpp3.title ILIKE '%cro%'
      )
),
-- RESOLVE SDR/BDR with prior role checking
final_sdrs AS (
    -- Immediate includes
    SELECT linkedin_profile_id, first_name, last_name, slug, 'sdr_bdr' as category
    FROM sdr_candidates 
    WHERE sdr_classification = 'include_immediately'
    
    UNION ALL
    
    -- Prior role checks
    SELECT sc.linkedin_profile_id, sc.first_name, sc.last_name, sc.slug, 'sdr_bdr' as category
    FROM sdr_candidates sc
    WHERE sc.sdr_classification = 'check_prior_role'
      AND EXISTS (
          SELECT 1
          FROM linkedin_profile_position3 lpp3_prior
          WHERE lpp3_prior.linkedin_profile_id = sc.linkedin_profile_id
            AND lpp3_prior.linkedin_company_id != (
                SELECT lpp3_current.linkedin_company_id 
                FROM linkedin_profile_position3 lpp3_current 
                WHERE lpp3_current.linkedin_profile_id = sc.linkedin_profile_id 
                  AND lpp3_current.is_current = true 
                LIMIT 1
            )
            AND lpp3_prior.obsolete = false
            AND lpp3_prior.end_date IS NOT NULL
            AND (
                lpp3_prior.title ILIKE 'sdr' OR lpp3_prior.title ILIKE 'bdr'
                OR lpp3_prior.title ILIKE 'senior sdr' OR lpp3_prior.title ILIKE 'senior bdr'
                OR lpp3_prior.title ILIKE '%sales development representative%'
                OR lpp3_prior.title ILIKE '%business development representative%'
            )
            AND NOT (
                lpp3_prior.title ILIKE '%manager%' OR lpp3_prior.title ILIKE '%director%'
                OR lpp3_prior.title ILIKE '%vp%' OR lpp3_prior.title ILIKE '%head of%'
            )
          ORDER BY lpp3_prior.end_date DESC
          LIMIT 1
      )
),
-- DEDUPLICATE BOTH DATASETS
deduplicated_sales_bd AS (
    SELECT DISTINCT ON (LOWER(first_name), LOWER(last_name), slug)
        slug, linkedin_profile_id, category
    FROM all_sales_bd
    ORDER BY LOWER(first_name), LOWER(last_name), slug
),
deduplicated_sdrs AS (
    SELECT DISTINCT ON (LOWER(first_name), LOWER(last_name), slug)
        slug, linkedin_profile_id, category
    FROM final_sdrs
    ORDER BY LOWER(first_name), LOWER(last_name), slug
)
-- FINAL RESULTS: Both metrics in one query
SELECT 
    COALESCE(sb.slug, sdr.slug) as slug,
    COALESCE(COUNT(DISTINCT sb.linkedin_profile_id), 0) as sales_bd_count,
    COALESCE(COUNT(DISTINCT sdr.linkedin_profile_id), 0) as sdr_bdr_count
FROM deduplicated_sales_bd sb
FULL OUTER JOIN deduplicated_sdrs sdr ON sb.slug = sdr.slug
GROUP BY COALESCE(sb.slug, sdr.slug);
"""

def process_companies_individual(job_id, companies, input_format, batch_size=1):
    """Individual processing function with smart resume and deduplication"""
    try:
        print(f"[JOB {job_id}] START: Individual processing of {len(companies)} companies", flush=True)
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
        
        print(f"[JOB {job_id}] Database connection successful", flush=True)
        sys.stdout.flush()
        
        # Set database config for processor
        processor.db_config = db_config
        
        # Convert companies to proper format
        formatted_companies = []
        for company in companies:
            if input_format == 'csv':
                formatted_companies.append({
                    'slug': company['slug'],
                    'hubspot_company_id': company['hubspot_company_id']
                })
            else:
                formatted_companies.append({
                    'slug': company,
                    'hubspot_company_id': ''
                })
        
        # Use batch_size parameter (passed from API endpoint)
        # Process all companies with configurable batch size
        processor.process_all_with_batch(formatted_companies, job_id, batch_size)
        
    except Exception as e:
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

def process_companies_individual_sdr(job_id, companies, input_format, batch_size=1):
    """Individual SDR processing function with smart resume and deduplication"""
    try:
        print(f"[JOB {job_id}] START: Individual SDR processing of {len(companies)} companies", flush=True)
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
        
        print(f"[JOB {job_id}] Database connection successful", flush=True)
        sys.stdout.flush()
        
        # Set database config for processor
        processor.db_config = db_config
        
        # Convert companies to proper format
        formatted_companies = []
        for company in companies:
            if input_format == 'csv':
                formatted_companies.append({
                    'slug': company['slug'],
                    'hubspot_company_id': company['hubspot_company_id']
                })
            else:
                formatted_companies.append({
                    'slug': company,
                    'hubspot_company_id': ''
                })
        
        # Use batch_size parameter (passed from API endpoint)
        # Process all companies for SDR count with configurable batch size
        processor.process_all_with_batch(formatted_companies, job_id, batch_size)
        
    except Exception as e:
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
    <h1>🚀 Smart Sales/BD Employee Counter</h1>
    <p>Process companies <strong>individually</strong> with smart resume and persistent deduplication.</p>
    <div style="background: #d4edda; padding: 10px; border-radius: 4px; margin: 10px 0;">
        <strong>✅ New Features:</strong> Individual processing, fault tolerance, smart resume, query timeouts, persistent deduplication
    </div>
    
    <div class="section">
        <h3>Start New Job (Gets Both Sales/BD + SDR/BDR Counts)</h3>
        <form id="jobForm">
            <div class="form-group">
                <p><strong>✅ Master Query:</strong> Gets both Sales/BD count (tag-based) AND SDR/BDR count (title-based) in one optimized run!</p>
            </div>
            
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
                <input type="number" id="batchSize" value="1" min="1" max="500">
                <small>1 = Individual processing (recommended, gets both Sales/BD + SDR/BDR counts). Higher = Batch processing (faster but less detailed).</small>
            </div>
            <button type="submit">Start Background Job</button>
        </form>
    </div>
    
    <div class="section">
        <h3>Job Status & Files (CSV includes both Sales/BD and SDR/BDR counts)</h3>
        <button onclick="checkStatus()">Refresh Status</button>
        <button onclick="listFiles()">List All CSV Files</button>
        <button onclick="downloadLatest()">Download Latest CSV</button>
        <button onclick="downloadCurrent()">Download Current Results</button>
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
                            
                            if (job.status === 'processing') {
                                statusHtml += `<br><a href="/download-current-results" download>Download Current Results</a>`;
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
        
        function downloadCurrent() {
            window.open('/download-current-results', '_blank');
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
                // Use master query endpoint for both metrics
                const endpoint = '/start-job';
                const requestBody = { 
                    companies, 
                    batch_size: batchSize, 
                    input_format: inputFormat 
                };
                
                const response = await fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(requestBody)
                });
                
                const result = await response.json();
                
                if (result.success) {
                    currentJobId = result.job_id;
                    localStorage.setItem('currentJobId', currentJobId);
                    const processingMode = batchSize === 1 ? 'Individual' : `Batch (${batchSize})`;
                    alert(`Job started! Job ID: ${currentJobId.substring(0, 8)}...\nProcessing ${companies.length} companies with ${processingMode} processing.\nWill get both Sales/BD and SDR/BDR counts.`);
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
    """Start an individual processing job with smart resume"""
    try:
        data = request.json
        companies = data['companies']
        input_format = data.get('input_format', 'slugs')
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        print(f"[API] Starting INDIVIDUAL job {job_id} for {len(companies)} companies", flush=True)
        sys.stdout.flush()
        
        # Get processor stats for context
        stats = processor.get_stats()
        
        # Store job info
        with job_lock:
            jobs[job_id] = {
                'id': job_id,
                'status': 'queued',
                'created_at': datetime.now().isoformat(),
                'total_companies': len(companies),
                'input_format': input_format,
                'processing_mode': 'individual',
                'previous_stats': stats
            }
        
        # Get batch size from request
        batch_size = int(data.get('batch_size', 1))
        
        # Start background thread with individual processing
        thread = threading.Thread(
            target=process_companies_individual,
            args=(job_id, companies, input_format, batch_size)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'message': f'Individual processing job started for {len(companies)} companies',
            'processing_mode': 'individual',
            'smart_resume': True,
            'previous_processed': stats['total_processed']
        })
        
    except Exception as e:
        print(f"[API] Error starting job: {e}", flush=True)
        sys.stdout.flush()
        return jsonify({'success': False, 'error': str(e)})

# DEPRECATED: SDR endpoint - now using master query in /start-job for both metrics
# @app.route('/start-sdr-job', methods=['POST'])
# def start_sdr_job():
#     """DEPRECATED: Start an individual SDR processing job with smart resume
#     Now using master query in /start-job endpoint for both metrics"""
#     pass

@app.route('/status')
def get_status():
    """Get status of all jobs and processor statistics"""
    try:
        with job_lock:
            job_list = list(jobs.values())
        
        # Sort by creation time, newest first
        job_list.sort(key=lambda x: x['created_at'], reverse=True)
        
        # Get processor statistics
        processor_stats = processor.get_stats()
        
        return jsonify({
            'api_success': True,
            'jobs': job_list,
            'processor_stats': processor_stats,
            'processing_mode': 'individual_with_smart_resume',
            'timestamp': datetime.now().isoformat()
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
            return 'No data directory found', 404
        
        # Get all CSV files
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        if not csv_files:
            return 'No CSV files found', 404
        
        # Get most recent file
        latest_file = max(csv_files, key=lambda f: os.path.getctime(os.path.join(data_dir, f)))
        
        return download_csv(latest_file)
        
    except Exception as e:
        return f'Error: {e}', 500

@app.route('/download-current-results')
def download_current_results():
    """Download current results from the active results file"""
    try:
        # Try to get current results from the processor's results file
        results_file = processor.results_file
        
        if not results_file.exists():
            return 'No results file found', 404
        
        # Generate CSV from current JSONL results
        results = []
        with open(results_file, 'r') as f:
            for line in f:
                try:
                    result = json.loads(line)
                    if result.get('success', False):
                        results.append({
                            'slug': result['company_slug'],
                            'hubspot_company_id': result.get('hubspot_company_id', ''),
                            'sales_bd_count': result.get('sales_bd_count', 0),
                            'sdr_bdr_count': result.get('sdr_bdr_count', 0)
                        })
                except json.JSONDecodeError:
                    continue  # Skip malformed lines
        
        if not results:
            return 'No successful results found', 404
        
        # Create CSV in memory
        output = io.StringIO()
        fieldnames = ['slug', 'hubspot_company_id', 'sales_bd_count', 'sdr_bdr_count']
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
        
        # Create response
        csv_content = output.getvalue()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'current_results_{timestamp}.csv'
        
        response = app.response_class(
            csv_content,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
        return response
        
    except Exception as e:
        return f'Error generating current results: {e}', 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)