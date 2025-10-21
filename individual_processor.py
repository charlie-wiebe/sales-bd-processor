#!/usr/bin/env python3
"""
Smart Individual Company Processor with Persistent Deduplication
Processes companies one at a time with fault tolerance and resume capability
"""

import json
import os
import time
import hashlib
import logging
from datetime import datetime
from typing import Dict, Set, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path
import fcntl
import tempfile
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingResult:
    """Structure for individual company processing results"""
    company_id: str
    company_name: str
    processed_at: str
    processing_time_seconds: float
    success: bool
    error_message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def get_hash(self) -> str:
        """Generate hash for deduplication based on company_id"""
        return hashlib.md5(self.company_id.encode()).hexdigest()

class SmartProcessor:
    """Smart individual processor with persistent deduplication"""
    
    def __init__(self, data_dir: str = "/data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # File paths
        self.results_file = self.data_dir / "company_results.jsonl"
        self.processed_ids_file = self.data_dir / "processed_ids.txt"
        self.progress_file = self.data_dir / "progress.json"
        self.lock_file = self.data_dir / "processor.lock"
        
        # In-memory cache for fast lookups
        self.processed_ids: Set[str] = set()
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        
        # Load existing state
        self._load_state()
        
    def _load_state(self):
        """Load existing processed IDs and progress from disk"""
        try:
            # Load processed IDs
            if self.processed_ids_file.exists():
                with open(self.processed_ids_file, 'r') as f:
                    self.processed_ids = set(line.strip() for line in f if line.strip())
                logger.info(f"Loaded {len(self.processed_ids)} previously processed company IDs")
            
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
    
    def _append_processed_id(self, company_id: str):
        """Append company ID to processed list (atomic operation)"""
        try:
            with open(self.processed_ids_file, 'a') as f:
                # Use file locking for thread safety
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                f.write(f"{company_id}\n")
                f.flush()
                os.fsync(f.fileno())
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                
            self.processed_ids.add(company_id)
            
        except Exception as e:
            logger.error(f"Failed to append processed ID {company_id}: {e}")
    
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
            logger.error(f"Failed to append result for {result.company_id}: {e}")
    
    def is_already_processed(self, company_id: str) -> bool:
        """Check if company has already been processed"""
        return company_id in self.processed_ids
    
    def get_unprocessed_companies(self, all_companies: list) -> list:
        """Filter out already processed companies"""
        unprocessed = []
        for company in all_companies:
            company_id = str(company.get('id', company.get('company_id', '')))
            if not self.is_already_processed(company_id):
                unprocessed.append(company)
        
        logger.info(f"Found {len(unprocessed)} unprocessed companies out of {len(all_companies)} total")
        return unprocessed
    
    def process_company(self, company: Dict[str, Any]) -> ProcessingResult:
        """Process a single company - OVERRIDE THIS METHOD"""
        # This is a placeholder - implement your actual processing logic here
        company_id = str(company.get('id', company.get('company_id', 'unknown')))
        company_name = company.get('name', company.get('company_name', 'Unknown'))
        
        start_time = time.time()
        
        try:
            # Simulate processing (replace with your actual logic)
            time.sleep(0.1)  # Remove this in real implementation
            
            # Your actual processing logic goes here:
            # result_data = query_company_data(company)
            # processed_data = transform_data(result_data)
            
            processing_time = time.time() - start_time
            
            return ProcessingResult(
                company_id=company_id,
                company_name=company_name,
                processed_at=datetime.now().isoformat(),
                processing_time_seconds=processing_time,
                success=True,
                data={'sample': 'data'}  # Replace with actual data
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Failed to process company {company_id}: {e}")
            
            return ProcessingResult(
                company_id=company_id,
                company_name=company_name,
                processed_at=datetime.now().isoformat(),
                processing_time_seconds=processing_time,
                success=False,
                error_message=str(e)
            )
    
    def process_all(self, companies: list, max_companies: Optional[int] = None):
        """Process all companies individually with smart resume"""
        
        # Filter out already processed companies
        unprocessed = self.get_unprocessed_companies(companies)
        
        if max_companies:
            unprocessed = unprocessed[:max_companies]
        
        if not unprocessed:
            logger.info("No unprocessed companies found. All done!")
            return
        
        logger.info(f"Starting individual processing of {len(unprocessed)} companies...")
        
        start_time = time.time()
        
        for i, company in enumerate(unprocessed, 1):
            company_id = str(company.get('id', company.get('company_id', 'unknown')))
            
            try:
                logger.info(f"[{i}/{len(unprocessed)}] Processing company {company_id}...")
                
                # Process individual company
                result = self.process_company(company)
                
                # Save result immediately (persistent)
                self._append_result(result)
                self._append_processed_id(company_id)
                
                # Update counters
                self.total_processed += 1
                if result.success:
                    self.total_successful += 1
                    logger.info(f"✅ SUCCESS: {company_id} ({result.processing_time_seconds:.2f}s)")
                else:
                    self.total_failed += 1
                    logger.warning(f"❌ FAILED: {company_id} - {result.error_message}")
                
                # Save progress every 10 companies
                if i % 10 == 0:
                    self._save_progress()
                    elapsed = time.time() - start_time
                    rate = i / elapsed * 60  # companies per minute
                    logger.info(f"Progress: {i}/{len(unprocessed)} ({i/len(unprocessed)*100:.1f}%) - Rate: {rate:.1f} companies/min")
                
            except Exception as e:
                logger.error(f"Unexpected error processing {company_id}: {e}")
                self.total_failed += 1
                continue
        
        # Final save
        self._save_progress()
        
        elapsed = time.time() - start_time
        logger.info(f"🎉 Individual processing complete!")
        logger.info(f"📊 Stats: {self.total_successful} successful, {self.total_failed} failed, {elapsed:.1f}s total")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        return {
            'total_processed': self.total_processed,
            'total_successful': self.total_successful,
            'total_failed': self.total_failed,
            'processed_ids_count': len(self.processed_ids),
            'results_file_exists': self.results_file.exists(),
            'results_file_size_mb': self.results_file.stat().st_size / 1024 / 1024 if self.results_file.exists() else 0
        }
    
    def get_results(self, limit: Optional[int] = None) -> list:
        """Get processed results from file"""
        results = []
        if not self.results_file.exists():
            return results
        
        try:
            with open(self.results_file, 'r') as f:
                for line_num, line in enumerate(f):
                    if limit and line_num >= limit:
                        break
                    if line.strip():
                        results.append(json.loads(line.strip()))
            return results
        except Exception as e:
            logger.error(f"Failed to read results: {e}")
            return []

# Example usage
if __name__ == "__main__":
    # Initialize processor
    processor = SmartProcessor()
    
    # Example companies data (replace with your actual data source)
    sample_companies = [
        {'id': 'comp_1', 'name': 'Company A'},
        {'id': 'comp_2', 'name': 'Company B'},
        {'id': 'comp_3', 'name': 'Company C'},
    ]
    
    # Process all companies
    processor.process_all(sample_companies)
    
    # Get stats
    stats = processor.get_stats()
    print(f"Processing stats: {stats}")
