import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from app.utils.llm_detect_source import llm_detect_source
import hashlib
import json

CONFIDENCE_THRESHOLD = 0.75  # Increased to use LLM more often
# threshold = 0.75

# Simple in-memory cache for LLM results (keyed by column headers hash)
_llm_cache = {}

def get_headers_hash(headers):
    """Create a hash of column headers for caching"""
    headers_str = json.dumps(sorted(headers), sort_keys=True)
    return hashlib.md5(headers_str.encode()).hexdigest()

def extract_features(df):
    headers = [c.lower() for c in df.columns]

    # More specific ATM indicators
    has_atm_indicators = int(any("atm" in h or "terminal" in h and ("id" in h or "location" in h) for h in headers))
    has_transaction_type = int(any("transaction" in h and "type" in h or "transactiontype" in h for h in headers))
    
    # PAN should only count for CARD if it's not masked and in ATM context
    # If we have ATM indicators + masked PAN, it's ATM not CARD
    has_pure_pan = int(any("pan" in h and "mask" not in h for h in headers))
    
    return {
        "column_count": len(headers),
        "has_rrn": int(any("rrn" in h for h in headers)),
        "has_terminal": int(any("terminal" in h for h in headers)),
        "has_merchant": int(any("merchant" in h or "mid" in h for h in headers)),
        "has_pan": has_pure_pan,  # Only non-masked PAN
        "has_auth": int(any("auth" in h for h in headers)),
        "has_balance": int(any("balance" in h for h in headers)),
        # Detect debit/credit columns: full names OR abbreviations (DR/CR)
        "has_debit_credit": int(any("debit" in h or "credit" in h or h == "dr" or h == "cr" or "dr_" in h or "cr_" in h for h in headers)),
        "has_mti": int(any("mti" in h or "message" in h and "type" in h for h in headers)),
        "has_direction": int(any("direction" in h or "inbound" in h or "outbound" in h for h in headers)),
        "has_processing_code": int(any("processing" in h and "code" in h or "proc" in h and "code" in h for h in headers)),
        # Additional CBS indicators
        "has_posted": int(any("posted" in h or "status" in h for h in headers)),
        "has_account": int(any("account" in h for h in headers)),
        # Enhanced ATM detection
        "has_atm_indicators": has_atm_indicators,
        "has_transaction_type": has_transaction_type,
    }

# ---- TRAIN MODEL ON STARTUP ----
# Training patterns based on typical file structures:
# - ATM: ATM operational logs with location/transaction type
# - SWITCH: Raw switch messages with MTI/Direction (can be ATM/POS/CARDS channel)
# - POS: POS transactions with merchant details
# - CARD: Card network settlement files (pure card data, no ATM context)
# - BANK: Core banking system posted transactions with DR/CR columns
training_data = pd.DataFrame([
    # ATM operational files (customer-facing) - has terminal, RRN, auth, transaction type
    # column_count, has_rrn, has_terminal, has_merchant, has_pan, has_auth, has_balance, has_debit_credit, 
    # has_mti, has_direction, has_processing_code, has_posted, has_account, has_atm_indicators, has_transaction_type, label
    [14, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, "ATM"],  # Your ATM file pattern
    [10, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, "ATM"],
    [11, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, "ATM"],
    [12, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, "ATM"],
    
    # SWITCH files (raw switch messages) - ATM channel
    [12, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, "SWITCH"],
    [13, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, "SWITCH"],
    [14, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, "SWITCH"],
    
    # SWITCH files - POS channel (has merchant indicators)
    [15, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, "SWITCH"],
    [16, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, "SWITCH"],
    
    # POS transaction files (not switch) - merchant focused
    [12, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    [13, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    [11, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    
    # CARD network files - pure card data, NO ATM indicators
    [20, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
    [18, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
    [15, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, "CARD"],
    
    # BANK/CBS posted transactions (with DR/CR columns)
    [8, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, "BANK"],
    [10, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, "BANK"],
    [10, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, "BANK"],
    [12, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, "BANK"],
], columns=[
    "column_count", "has_rrn", "has_terminal", "has_merchant",
    "has_pan", "has_auth", "has_balance", "has_debit_credit",
    "has_mti", "has_direction", "has_processing_code",
    "has_posted", "has_account", "has_atm_indicators", "has_transaction_type", "label"
])

X = training_data.drop("label", axis=1)
y = training_data["label"]

encoder = LabelEncoder()
y_enc = encoder.fit_transform(y)

model = RandomForestClassifier(n_estimators=200, random_state=42)
model.fit(X, y_enc)

def predict_source(df):
    features = extract_features(df)
    df_feat = pd.DataFrame([features])

    probs = model.predict_proba(df_feat)[0]
    idx = probs.argmax()

    return {
        "source": encoder.inverse_transform([idx])[0],
        "confidence": round(float(probs[idx]), 2),
        "features": features
    }


def predict_source_with_fallback(df):
    # Always run ML prediction first
    ml_result = predict_source(df)
    
    print(f"ML Prediction - Source: {ml_result['source']}, Confidence: {ml_result['confidence']}")

    # Only call LLM if ML confidence is below threshold
    if ml_result["confidence"] < CONFIDENCE_THRESHOLD:
        headers = list(df.columns)
        headers_hash = get_headers_hash(headers)
        
        # Check cache first
        if headers_hash in _llm_cache:
            print(f"Using cached LLM result for headers hash: {headers_hash}")
            llm_result = _llm_cache[headers_hash]
        else:
            print(f"ML confidence ({ml_result['confidence']}) below threshold ({CONFIDENCE_THRESHOLD}). Calling LLM for verification...")
            sample_rows = df.head(3).to_dict(orient="records")

            llm_result = llm_detect_source(headers, sample_rows)
            print("LLM Result:", llm_result)
            
            # Cache the result
            _llm_cache[headers_hash] = llm_result
            print(f"Cached LLM result for headers hash: {headers_hash}")

        # Check if LLM failed (returned UNKNOWN or error)
        llm_source = llm_result.get("source", "UNKNOWN")
        llm_failed = (
            llm_source == "UNKNOWN" or 
            "error" in llm_result or 
            "Rate limit exceeded" in llm_result.get("reason", "")
        )
        
        if llm_failed:
            print(f"LLM failed or returned UNKNOWN. Falling back to ML prediction: {ml_result['source']}")
            # Use ML prediction as fallback when LLM fails
            result = {
                "ml_prediction": {
                    "source": ml_result["source"],
                    "confidence": ml_result["confidence"],
                    "features": ml_result["features"]
                },
                "llm_prediction": {
                    "source": llm_result.get("source", "UNKNOWN"),
                    "channel": llm_result.get("channel", "UNKNOWN"),
                    "reason": llm_result.get("reason", "No reason provided"),
                    "raw": llm_result
                },
                "decision": "ML_FALLBACK",  # Indicate this was a fallback
                "recommended_source": ml_result["source"],  # Use ML prediction
                "agreement": False,
                "confidence_threshold": CONFIDENCE_THRESHOLD,
                "cached": headers_hash in _llm_cache,
                "fallback_reason": "LLM failed or returned UNKNOWN, using ML prediction"
            }
        else:
            # Combine both results when LLM is successful
            result = {
                "ml_prediction": {
                    "source": ml_result["source"],
                    "confidence": ml_result["confidence"],
                    "features": ml_result["features"]
                },
                "llm_prediction": {
                    "source": llm_result.get("source", "UNKNOWN"),
                    "channel": llm_result.get("channel", "UNKNOWN"),
                    "reason": llm_result.get("reason", "No reason provided"),
                    "raw": llm_result
                },
                "decision": "LLM",
                "recommended_source": llm_result.get("source", "UNKNOWN"),
                "agreement": ml_result["source"] == llm_result.get("source", "").replace("_FILE", ""),
                "confidence_threshold": CONFIDENCE_THRESHOLD,
                "cached": headers_hash in _llm_cache
            }
    else:
        print(f"ML confidence ({ml_result['confidence']}) is high. Using ML prediction only.")
        # Use ML prediction only when confidence is high
        result = {
            "ml_prediction": {
                "source": ml_result["source"],
                "confidence": ml_result["confidence"],
                "features": ml_result["features"]
            },
            "llm_prediction": None,
            "decision": "ML",
            "recommended_source": ml_result["source"],
            "agreement": True,
            "confidence_threshold": CONFIDENCE_THRESHOLD,
            "cached": False
        }
    
    return result
