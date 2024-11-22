from google.cloud import storage
import os
import json
import PyPDF2
import re
from datetime import datetime
from typing import List, Dict

def extract_metadata(filename: str) -> Dict:
    """Extract metadata from filename."""
    date_pattern = r'(\d{4})[-_]?(\d{2})?[-_]?(\d{2})?'
    date_match = re.search(date_pattern, filename)
    
    doc_type = 'other'
    if 'minutes' in filename.lower():
        doc_type = 'minutes'
    elif 'agenda' in filename.lower():
        doc_type = 'agenda'
    elif 'bylaw' in filename.lower():
        doc_type = 'bylaw'
    elif 'permit' in filename.lower():
        doc_type = 'permit'
    
    return {
        'filename': filename,
        'doc_type': doc_type,
        'year': date_match.group(1) if date_match else None,
        'municipality': 'esquimalt',
        'processed_at': datetime.utcnow().isoformat()
    }

def chunk_text(text: str, chunk_size: int = 1000, overlap: int = 100) -> List[str]:
    """Split text into overlapping chunks."""
    chunks = []
    start = 0
    text_len = len(text)
    
    while start < text_len:
        end = start + chunk_size
        if end > text_len:
            end = text_len
        
        # Adjust chunk to end at a sentence or paragraph
        if end < text_len:
            while end > start and text[end] not in '.!?\n':
                end -= 1
            if end == start:
                end = start + chunk_size
        
        chunks.append(text[start:end])
        start = end - overlap
    
    return chunks

def process_pdf(file_path: str) -> List[Dict]:
    """Process PDF into chunks with metadata."""
    chunks = []
    try:
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            metadata = extract_metadata(os.path.basename(file_path))
            
            full_text = ""
            for page_num in range(len(pdf_reader.pages)):
                text = pdf_reader.pages[page_num].extract_text()
                if text:
                    full_text += f"\nPage {page_num + 1}:\n{text}"
            
            text_chunks = chunk_text(full_text)
            
            for i, chunk in enumerate(text_chunks):
                chunks.append({
                    'text': chunk,
                    'metadata': {
                        **metadata,
                        'chunk_index': i,
                        'total_chunks': len(text_chunks)
                    }
                })
            
            print(f"Processed {file_path}: {len(chunks)} chunks")
            
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
    
    return chunks

def main():
    input_dir = 'data/municipal_docs'
    output_dir = 'data/processed'
    os.makedirs(output_dir, exist_ok=True)
    
    all_chunks = []
    for filename in os.listdir(input_dir):
        if filename.endswith('.pdf'):
            file_path = os.path.join(input_dir, filename)
            chunks = process_pdf(file_path)
            all_chunks.extend(chunks)
            
            # Save chunks for this file
            output_file = os.path.join(output_dir, f"{filename}.json")
            with open(output_file, 'w') as f:
                json.dump(chunks, f, indent=2)
    
    # Save summary
    with open(os.path.join(output_dir, '_summary.json'), 'w') as f:
        json.dump({
            'total_documents': len(os.listdir(input_dir)),
            'total_chunks': len(all_chunks),
            'processed_at': datetime.utcnow().isoformat()
        }, f, indent=2)

if __name__ == '__main__':
    main()
