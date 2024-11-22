from google.cloud import aiplatform
from google.cloud import storage
import json
import logging
from typing import List, Dict
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmbeddingGenerator:
    def __init__(self,
                 project_id: str = "panda-17d82",
                 location: str = "us-central1",
                 bucket_name: str = "panda-17d82-municipal-data"):
        """Initialize the embedding generator."""
        self.project_id = project_id
        self.location = location
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        aiplatform.init(project=project_id, location=location)
        self.embedding_model = aiplatform.TextEmbeddingModel.from_pretrained(
            "textembedding-gecko@001"
        )

    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts."""
        try:
            embeddings = self.embedding_model.get_embeddings(texts)
            return [embedding.values for embedding in embeddings]
        except Exception as e:
            logger.error(f"Error generating embeddings: {str(e)}")
            raise

    def process_chunks(self, chunks_file: str) -> str:
        """Process chunks from a JSON file and generate embeddings."""
        try:
            # Read chunks
            blob = self.bucket.blob(chunks_file)
            chunks = json.loads(blob.download_as_string())
            
            # Generate embeddings
            texts = [chunk['text'] for chunk in chunks]
            embeddings = self.generate_embeddings(texts)
            
            # Combine chunks with embeddings
            processed_data = {
                'chunks': chunks,
                'embeddings': embeddings,
                'metadata': {
                    'processed_at': datetime.utcnow().isoformat(),
                    'model': 'textembedding-gecko@001',
                    'source_file': chunks_file
                }
            }
            
            # Save results
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            output_path = f'embeddings/municipal_embeddings_{timestamp}.json'
            output_blob = self.bucket.blob(output_path)
            
            output_blob.upload_from_string(
                json.dumps(processed_data, indent=2),
                content_type='application/json'
            )
            
            logger.info(f"Saved embeddings to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error processing chunks: {str(e)}")
            raise