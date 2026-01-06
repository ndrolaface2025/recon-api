from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

CANONICAL_FIELDS = {
    "transaction_id": ["txn_id", "transaction_no", "transaction_id", "txnid"],
    "reference_number": ["rrn", "RRN", "reference", "retrieval_reference", "ref_number", "retrieval_ref"],
    "terminal_id": ["terminal_id", "atm_id", "term_id", "terminalid", "TerminalID"],
    "merchant_id": ["merchant_id", "mid", "merchantid"],
    "amount": ["amount", "txn_amount", "transaction_amount", "amt", "Amount", "amountminor", "amount_minor", "AmountMinor", "dr", "DR", "debit", "Debit", "cr", "CR", "credit", "Credit"],
    "date": ["txn_date", "transaction_date", "datetime", "date_time", "date", "time", "timestamp", "DateTime", "posted_datetime", "PostedDateTime", "posted_date", "PostedDate", "transaction_datetime", "TransactionDateTime"],
    "authorization_code": ["auth_code", "authorization", "auth", "authcode"],
    "currency": ["currency", "ccy", "curr", "currency_code", "Currency"],
    "stan": ["stan", "STAN", "system_trace", "trace_number", "trace"],
    "response_code": ["response_code", "resp_code", "responsecode", "status_code", "ResponseCode"],
    "location": ["location", "loc", "branch", "city", "place", "Location"],
    "pan": ["pan", "card_number", "card", "card_no", "pan_masked", "PAN_masked"],
    "account_number": ["account_number", "account", "acc_no", "account_masked", "acct"],
    "transaction_type": ["transaction_type", "txn_type", "trans_type", "type"],
    "balance": ["balance", "available_balance", "bal", "account_balance"],
    "fee": ["fee", "charge", "transaction_fee", "surcharge"],
    "response_description": ["response_desc", "response_description", "status_desc", "message"]
}

def auto_map_columns(uploaded_columns, threshold=0.7):
    """
    Map uploaded columns to canonical fields using exact matching first,
    then TF-IDF and cosine similarity for fuzzy matching.
    """
    mappings = {}
    
    # Convert pandas Index to list if needed
    if hasattr(uploaded_columns, 'tolist'):
        uploaded_columns = uploaded_columns.tolist()
    
    if not uploaded_columns:
        return mappings
    
    # First pass: Check for exact matches (case-insensitive) with canonical field aliases
    for col in uploaded_columns:
        col_lower = str(col).lower()
        exact_match = None
        
        # Check if column name matches any canonical field or its aliases
        for canonical_field, aliases in CANONICAL_FIELDS.items():
            # Check canonical field name itself
            if col_lower == canonical_field.replace("_", "").lower() or col_lower == canonical_field.lower():
                exact_match = canonical_field
                break
            
            # Check aliases
            for alias in aliases:
                if col_lower == alias.lower():
                    exact_match = canonical_field
                    break
            
            if exact_match:
                break
        
        if exact_match:
            mappings[col] = {
                "mapped_to": exact_match,
                "confidence": 1.0
            }
    
    # Second pass: For unmapped columns, use TF-IDF fuzzy matching
    unmapped_columns = [col for col in uploaded_columns if col not in mappings]
    
    if unmapped_columns:
        # Prepare canonical field names (convert to readable text)
        canonical_names = list(CANONICAL_FIELDS.keys())
        canonical_texts = [name.replace("_", " ") for name in canonical_names]
        
        # Prepare uploaded column names (convert to string and lowercase)
        uploaded_texts = [str(col).replace("_", " ").lower() for col in unmapped_columns]
        
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
        for idx, col in enumerate(unmapped_columns):
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
