import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from app.utils.llm_detect_source import llm_detect_source

CONFIDENCE_THRESHOLD = 0.75  # Increased to use LLM more often
# threshold = 0.75

def extract_features(df):
    headers = [c.lower() for c in df.columns]

    return {
        "column_count": len(headers),
        "has_rrn": int(any("rrn" in h for h in headers)),
        "has_terminal": int(any("terminal" in h or "atm" in h for h in headers)),
        "has_merchant": int(any("merchant" in h or "mid" in h for h in headers)),
        "has_pan": int(any("pan" in h or "card" in h for h in headers)),
        "has_auth": int(any("auth" in h for h in headers)),
        "has_balance": int(any("balance" in h for h in headers)),
        "has_debit_credit": int(any("debit" in h or "credit" in h for h in headers)),
    }

# ---- TRAIN MODEL ON STARTUP ----
training_data = pd.DataFrame([
    [10,1,1,0,0,0,0,0,"ATM"],
    [12,1,1,1,0,1,0,0,"POS"],
    [20,1,0,0,1,1,0,0,"CARD"],
    [8,0,0,0,0,0,1,1,"BANK"]
], columns=[
    "column_count","has_rrn","has_terminal","has_merchant",
    "has_pan","has_auth","has_balance","has_debit_credit","label"
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

    # Always run LLM for comparison
    print("Calling LLM for verification...")
    headers = list(df.columns)
    sample_rows = df.head(3).to_dict(orient="records")

    llm_result = llm_detect_source(headers, sample_rows)
    print("LLM Result:", llm_result)

    # Combine both results
    result = {
        "ml_prediction": {
            "source": ml_result["source"],
            "confidence": ml_result["confidence"],
            "features": ml_result["features"]
        },
        "llm_prediction": {
            "source": llm_result.get("source", "UNKNOWN"),
            "reason": llm_result.get("reason", "No reason provided"),
            "raw": llm_result
        },
        "decision": "LLM" if ml_result["confidence"] < CONFIDENCE_THRESHOLD else "ML",
        "recommended_source": llm_result.get("source", "UNKNOWN") if ml_result["confidence"] < CONFIDENCE_THRESHOLD else ml_result["source"],
        "agreement": ml_result["source"] == llm_result.get("source", "").replace("_FILE", ""),
        "confidence_threshold": CONFIDENCE_THRESHOLD
    }
    
    return result
