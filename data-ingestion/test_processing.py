import os
import json
from process_municipal_docs import process_pdf, extract_metadata
from generate_embeddings import generate_embeddings

def test_single_document():
    """Test processing on a single document."""
    input_dir = 'data/municipal_docs'
    
    # Get first PDF file
    test_file = next(f for f in os.listdir(input_dir) if f.endswith('.pdf'))
    file_path = os.path.join(input_dir, test_file)
    
    # Process PDF
    chunks = process_pdf(file_path)
    print(f"\nProcessed {test_file}:")
    print(f"Number of chunks: {len(chunks)}")
    print(f"First chunk length: {len(chunks[0]['text'])}")
    print(f"Metadata: {chunks[0]['metadata']}")
    
    # Generate embeddings for first chunk
    embeddings = generate_embeddings([chunks[0]['text']], batch_size=1)
    print(f"\nGenerated embedding dimensions: {len(embeddings[0])}")

if __name__ == '__main__':
    test_single_document()
