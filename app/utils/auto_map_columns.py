from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

CANONICAL_FIELDS = {
    "transaction_id": ["txn_id", "transaction_no"],
    "reference_number": ["rrn", "reference", "retrieval_reference"],
    "terminal_id": ["terminal_id", "atm_id", "term_id"],
    "merchant_id": ["merchant_id", "mid"],
    "amount": ["amount", "txn_amount"],
    "transaction_date": ["txn_date", "transaction_date"],
    "authorization_code": ["auth_code", "authorization"]
}

def auto_map_columns(uploaded_columns, threshold=0.7):
    """
    Map uploaded columns to canonical fields using TF-IDF and cosine similarity.
    This is a lightweight alternative to sentence-transformers.
    """
    mappings = {}
    
    # Convert pandas Index to list if needed
    if hasattr(uploaded_columns, 'tolist'):
        uploaded_columns = uploaded_columns.tolist()
    
    if not uploaded_columns:
        return mappings
    
    # Prepare canonical field names (convert to readable text)
    canonical_names = list(CANONICAL_FIELDS.keys())
    canonical_texts = [name.replace("_", " ") for name in canonical_names]
    
    # Prepare uploaded column names (convert to string and lowercase)
    uploaded_texts = [str(col).replace("_", " ").lower() for col in uploaded_columns]
    
    # Create TF-IDF vectorizer with character n-grams for better fuzzy matching
    vectorizer = TfidfVectorizer(
        analyzer='char_wb',
        ngram_range=(2, 4),
        lowercase=True
    )
    
    # Fit on both canonical and uploaded columns
    all_texts = canonical_texts + uploaded_texts
    tfidf_matrix = vectorizer.fit_transform(all_texts)
    
    # Split back into canonical and uploaded
    canonical_vectors = tfidf_matrix[:len(canonical_texts)]
    uploaded_vectors = tfidf_matrix[len(canonical_texts):]
    
    # Calculate cosine similarity
    similarities = cosine_similarity(uploaded_vectors, canonical_vectors)
    
    # Map each uploaded column to best matching canonical field
    for idx, col in enumerate(uploaded_columns):
        scores = similarities[idx]
        best_idx = np.argmax(scores)
        best_score = scores[best_idx]
        best_match = canonical_names[best_idx]
        
        if best_score >= threshold:
            mappings[col] = {
                "mapped_to": best_match,
                "confidence": round(float(best_score), 2)
            }
        else:
            mappings[col] = {
                "mapped_to": None,
                "confidence": round(float(best_score), 2)
            }
    
    return mappings
