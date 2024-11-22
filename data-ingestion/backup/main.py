# Copyright 2024 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  https://www.apache.org/licenses/LICENSE-2.0

from flask import Flask, request, jsonify
from google.cloud import aiplatform
from google.cloud import storage
import os
import json
import logging
from datetime import datetime
from municipal_processor import MunicipalDocumentProcessor
from embedding_generator import EmbeddingGenerator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize global variables
PROJECT_ID = "panda-17d82"
LOCATION = "us-central1"
BUCKET_NAME = "panda-17d82-municipal-data"
INDEX_ID = "municipal-docs-index"

# Initialize processors
doc_processor = MunicipalDocumentProcessor(
    project_id=PROJECT_ID,
    location=LOCATION,
    bucket_name=BUCKET_NAME,
    index_id=INDEX_ID
)
embedding_gen = EmbeddingGenerator(
    project_id=PROJECT_ID,
    location=LOCATION,
    bucket_name=BUCKET_NAME
)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})

@app.route('/process-documents', methods=['POST'])
def process_documents():
    """Endpoint to trigger document processing."""
    try:
        data = request.get_json()
        prefix = data.get('prefix', 'esquimalt_data/pdfs/')
        
        # Process documents
        chunks = doc_processor.process_directory(prefix)
        chunks_file = doc_processor.save_chunks(chunks)
        
        # Generate embeddings
        embeddings_file = embedding_gen.process_chunks(chunks_file)
        
        return jsonify({
            "status": "success",
            "processed_documents": len(chunks),
            "chunks_file": chunks_file,
            "embeddings_file": embeddings_file
        })
    
    except Exception as e:
        logger.error(f"Error processing documents: {str(e)}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/query', methods=['POST'])
def query_documents():
    """Endpoint to query the vector index."""
    try:
        data = request.get_json()
        query = data.get('query')
        if not query:
            return jsonify({"error": "No query provided"}), 400
        
        # Initialize Vertex AI
        aiplatform.init(project=PROJECT_ID, location=LOCATION)
        
        # Get the index
        index = aiplatform.MatchingEngineIndex(index_id=INDEX_ID)
        
        # Query the index
        response = index.find_neighbors(
            query_vectors=[query],
            num_neighbors=5
        )
        
        # Format results
        results = []
        for neighbor in response[0]:
            results.append({
                "text": neighbor.text,
                "metadata": neighbor.metadata,
                "score": float(neighbor.distance)
            })
        
        return jsonify({
            "query": query,
            "results": results
        })
    
    except Exception as e:
        logger.error(f"Error querying index: {str(e)}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get system statistics."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # Count documents
        pdf_count = len(list(bucket.list_blobs(prefix='esquimalt_data/pdfs/')))
        processed_count = len(list(bucket.list_blobs(prefix='processed/')))
        embeddings_count = len(list(bucket.list_blobs(prefix='embeddings/')))
        
        return jsonify({
            "total_pdfs": pdf_count,
            "processed_documents": processed_count,
            "embedding_files": embeddings_count,
            "last_updated": datetime.utcnow().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=True)