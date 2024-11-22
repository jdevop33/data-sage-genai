import logging
import os
from process_municipal_docs import main as process_docs
from generate_embeddings import EmbeddingGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # 1. Process PDFs
        logger.info("Starting PDF processing...")
        process_docs()
        
        # 2. Generate embeddings
        logger.info("Generating embeddings...")
        generator = EmbeddingGenerator()
        processed_dir = '../data/processed'
        
        for filename in os.listdir(processed_dir):
            if filename.endswith('.json'):
                try:
                    logger.info(f"Processing {filename}")
                    chunks_file = os.path.join(processed_dir, filename)
                    output_path = generator.process_chunks(chunks_file)
                    logger.info(f"Successfully processed {filename} -> {output_path}")
                except Exception as e:
                    logger.error(f"Error processing {filename}: {str(e)}")
        
        logger.info("Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
