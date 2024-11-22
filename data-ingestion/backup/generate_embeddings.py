from google.cloud import aiplatform
import json
import os
from typing import List, Dict
from datetime import datetime
import time

def batch_generator(items: List, batch_size: int):
    """Generate batches from a list."""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]

def generate_embeddings(texts: List[str], batch_size: int = 5) -> List[List[float]]:
    """Generate embeddings in batches."""
    aiplatform.init(project='panda-17d82', location='us-central1')
    model = aiplatform.TextEmbeddingModel.from_pretrained("textembedding-gecko@001")
    
    all_embeddings = []
    for batch in batch_generator(texts, batch_size):
        try:
            embeddings = model.get_embeddings(batch)
            all_embeddings.extend([embedding.values for embedding in embeddings])
            time.sleep(1)  # Rate limiting
        except Exception as e:
            print(f"Error generating embeddings for batch: {str(e)}")
    
    return all_embeddings

def main():
    processed_dir = 'data/processed'
    embeddings_dir = 'data/embeddings'
    os.makedirs(embeddings_dir, exist_ok=True)
    
    # Process each JSON file
    for filename in os.listdir(processed_dir):
        if filename.endswith('.json') and not filename.startswith('_'):
            print(f"Processing embeddings for {filename}")
            
            try:
                with open(os.path.join(processed_dir, filename), 'r') as f:
                    chunks = json.load(f)
                
                texts = [chunk['text'] for chunk in chunks]
                embeddings = generate_embeddings(texts)
                
                output_file = os.path.join(embeddings_dir, f"{filename}_embeddings.json")
                with open(output_file, 'w') as f:
                    json.dump({
                        'chunks': chunks,
                        'embeddings': embeddings,
                        'metadata': {
                            'model': 'textembedding-gecko@001',
                            'processed_at': datetime.utcnow().isoformat()
                        }
                    }, f, indent=2)
                
                print(f"Generated embeddings for {filename}: {len(embeddings)} vectors")
                
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")

if __name__ == '__main__':
    main()
