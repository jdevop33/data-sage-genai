from google.cloud import storage
from google.cloud import aiplatform
from typing import List, Dict, Optional
import PyPDF2
import json
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MunicipalDocumentProcessor:
    def __init__(self, 
                 project_id: str = "panda-17d82",
                 location: str = "us-central1",
                 bucket_name: str = "panda-17d82-municipal-data",
                 index_id: str = "municipal-docs-index"):
        """Initialize the document processor with GCP settings."""
        self.project_id = project_id
        self.location = location
        self.bucket_name = bucket_name
        self.index_id = index_id
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        aiplatform.init(project=project_id, location=location)
        
    def process_pdf(self, blob_name: str) -> List[Dict]:
        """Process a single PDF from GCS into chunks with metadata."""
        try:
            blob = self.bucket.blob(blob_name)
            tmp_path = f"/tmp/{os.path.basename(blob_name)}"
            blob.download_to_filename(tmp_path)
            
            chunks = []
            with open(tmp_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                metadata = self._extract_metadata(blob_name)
                
                for page_num in range(len(pdf_reader.pages)):
                    text = pdf_reader.pages[page_num].extract_text()
                    if text.strip():  # Only process non-empty pages
                        chunks.append({
                            'text': text,
                            'metadata': {
                                **metadata,
                                'page': page_num + 1,
                                'total_pages': len(pdf_reader.pages),
                                'processed_at': datetime.utcnow().isoformat()
                            }
                        })
            
            os.remove(tmp_path)  # Cleanup
            return chunks
            
        except Exception as e:
            logger.error(f"Error processing {blob_name}: {str(e)}")
            raise

    def _extract_metadata(self, blob_name: str) -> Dict:
        """Extract metadata from file path and name."""
        parts = blob_name.split('/')
        filename = parts[-1].lower()
        
        # Determine document type
        doc_type = 'other'
        if 'bylaw' in filename:
            doc_type = 'bylaw'
        elif 'policy' in filename:
            doc_type = 'policy'
        elif 'minutes' in filename:
            doc_type = 'minutes'
        elif 'report' in filename:
            doc_type = 'report'
            
        return {
            'source': blob_name,
            'municipality': 'esquimalt',
            'document_type': doc_type,
            'year': self._extract_year(filename),
            'filename': os.path.basename(blob_name)
        }

    def _extract_year(self, filename: str) -> Optional[int]:
        """Extract year from filename if present."""
        try:
            # Look for 4-digit numbers that could be years
            import re
            years = re.findall(r'20\d{2}', filename)
            return int(years[0]) if years else None
        except:
            return None

    def process_directory(self, prefix: str = 'esquimalt_data/pdfs/') -> List[Dict]:
        """Process all PDFs in a directory."""
        all_chunks = []
        blobs = self.bucket.list_blobs(prefix=prefix)
        
        for blob in blobs:
            if blob.name.endswith('.pdf'):
                logger.info(f"Processing {blob.name}")
                try:
                    chunks = self.process_pdf(blob.name)
                    all_chunks.extend(chunks)
                    logger.info(f"Successfully processed {blob.name}: {len(chunks)} chunks")
                except Exception as e:
                    logger.error(f"Failed to process {blob.name}: {str(e)}")
                    continue
        
        return all_chunks

    def save_chunks(self, chunks: List[Dict], output_prefix: str = 'processed/'):
        """Save processed chunks to GCS."""
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        output_blob = self.bucket.blob(
            f"{output_prefix}chunks_{timestamp}.json"
        )
        
        output_blob.upload_from_string(
            json.dumps(chunks, indent=2),
            content_type='application/json'
        )
        
        logger.info(f"Saved {len(chunks)} chunks to {output_blob.name}")
        return output_blob.name