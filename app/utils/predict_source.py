# import pandas as pd
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.preprocessing import LabelEncoder
# from app.utils.llm_detect_source import llm_detect_source
# import hashlib
# import json

# CONFIDENCE_THRESHOLD = 0.75  # Increased to use LLM more often
# # threshold = 0.75

# # Simple in-memory cache for LLM results (keyed by column headers hash)
# _llm_cache = {}

# def get_headers_hash(headers):
#     """Create a hash of column headers for caching"""
#     headers_str = json.dumps(sorted(headers), sort_keys=True)
#     return hashlib.md5(headers_str.encode()).hexdigest()

# def extract_features(df):
#     headers = [c.lower() for c in df.columns]

#     # More specific ATM indicators
#     has_atm_indicators = int(any("atm" in h or "terminal" in h and ("id" in h or "location" in h) for h in headers))
#     has_transaction_type = int(any("transaction" in h and "type" in h or "transactiontype" in h for h in headers))
    
#     # PAN should only count for CARD if it's not masked and in ATM context
#     # If we have ATM indicators + masked PAN, it's ATM not CARD
#     has_pure_pan = int(any("pan" in h and "mask" not in h for h in headers))
    
#     # Mobile Money / E-Money indicators
#     has_mobile_number = int(any("mobile" in h and "number" in h or "phone" in h and "number" in h or "customer_mobile" in h for h in headers))
#     has_service_name = int(any("service" in h and "name" in h or "provider" in h or "payer" in h and "client" in h for h in headers))
#     has_payment_mode = int(any("payment" in h and "mode" in h or "channel" in h and "type" in h for h in headers))
#     has_narration = int(any("narration" in h or "description" in h or "message" in h for h in headers))
    
#     return {
#         "column_count": len(headers),
#         "has_rrn": int(any("rrn" in h for h in headers)),
#         "has_terminal": int(any("terminal" in h for h in headers)),
#         "has_merchant": int(any("merchant" in h or "mid" in h for h in headers)),
#         "has_pan": has_pure_pan,  # Only non-masked PAN
#         "has_auth": int(any("auth" in h for h in headers)),
#         "has_balance": int(any("balance" in h for h in headers)),
#         # Detect debit/credit columns: full names OR abbreviations (DR/CR)
#         "has_debit_credit": int(any("debit" in h or "credit" in h or h == "dr" or h == "cr" or "dr_" in h or "cr_" in h for h in headers)),
#         "has_mti": int(any("mti" in h or "message" in h and "type" in h for h in headers)),
#         "has_direction": int(any("direction" in h or "inbound" in h or "outbound" in h for h in headers)),
#         "has_processing_code": int(any("processing" in h and "code" in h or "proc" in h and "code" in h for h in headers)),
#         # Additional CBS indicators
#         "has_posted": int(any("posted" in h or "status" in h for h in headers)),
#         "has_account": int(any("account" in h for h in headers)),
#         # Enhanced ATM detection
#         "has_atm_indicators": has_atm_indicators,
#         "has_transaction_type": has_transaction_type,
#         # Mobile Money / E-Money indicators
#         "has_mobile_number": has_mobile_number,
#         "has_service_name": has_service_name,
#         "has_payment_mode": has_payment_mode,
#         "has_narration": has_narration,
#     }

# # ---- TRAIN MODEL ON STARTUP ----
# # Training patterns based on typical file structures:
# # - ATM: ATM operational logs with location/transaction type
# # - SWITCH: Raw switch messages with MTI/Direction (can be ATM/POS/CARDS channel)
# # - POS: POS transactions with merchant details
# # - CARD: Card network settlement files (pure card data, no ATM context)
# # - BANK: Core banking system posted transactions with DR/CR columns
# # - MOBILE_MONEY: E-Money/Mobile Money platform transactions with mobile number, service name, payment mode
# training_data = pd.DataFrame([
#     # ATM operational files (customer-facing) - has terminal, RRN, auth, transaction type
#     # column_count, has_rrn, has_terminal, has_merchant, has_pan, has_auth, has_balance, has_debit_credit, 
#     # has_mti, has_direction, has_processing_code, has_posted, has_account, has_atm_indicators, has_transaction_type,
#     # has_mobile_number, has_service_name, has_payment_mode, has_narration, label
#     [14, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, "ATM"],  # Your ATM file pattern
#     [10, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, "ATM"],
#     [11, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, "ATM"],
#     [12, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, "ATM"],
    
#     # SWITCH files (raw switch messages) - ATM channel
#     [12, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [13, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [14, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    
#     # SWITCH files - POS channel (has merchant indicators)
#     [15, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [16, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    
#     # POS transaction files (not switch) - merchant focused
#     [12, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
#     [13, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
#     [11, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    
#     # CARD network files - pure card data, NO ATM indicators
#     [20, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
#     [18, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
#     [15, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, "CARD"],
    
#     # BANK/CBS posted transactions (with DR/CR columns)
#     [8, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, "BANK"],
#     [10, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, "BANK"],
#     [10, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, "BANK"],
#     [12, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, "BANK"],
    
#     # MOBILE_MONEY / E-Money platform files - has mobile number, service name, payment mode
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # Your E-Money file pattern
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # With status
#     [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # With account
#     [13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # With posted status
# ], columns=[
#     "column_count", "has_rrn", "has_terminal", "has_merchant",
#     "has_pan", "has_auth", "has_balance", "has_debit_credit",
#     "has_mti", "has_direction", "has_processing_code",
#     "has_posted", "has_account", "has_atm_indicators", "has_transaction_type",
#     "has_mobile_number", "has_service_name", "has_payment_mode", "has_narration", "label"
# ])

# X = training_data.drop("label", axis=1)
# y = training_data["label"]

# encoder = LabelEncoder()
# y_enc = encoder.fit_transform(y)

# model = RandomForestClassifier(n_estimators=200, random_state=42)
# model.fit(X, y_enc)

# def predict_source(df):
#     features = extract_features(df)
#     df_feat = pd.DataFrame([features])

#     probs = model.predict_proba(df_feat)[0]
#     idx = probs.argmax()

#     return {
#         "source": encoder.inverse_transform([idx])[0],
#         "confidence": round(float(probs[idx]), 2),
#         "features": features
#     }


# def predict_source_with_fallback(df):
#     # Always run ML prediction first
#     ml_result = predict_source(df)
    
#     print(f"ML Prediction - Source: {ml_result['source']}, Confidence: {ml_result['confidence']}")

#     # Only call LLM if ML confidence is below threshold
#     if ml_result["confidence"] < CONFIDENCE_THRESHOLD:
#         headers = list(df.columns)
#         headers_hash = get_headers_hash(headers)
        
#         # Check cache first
#         if headers_hash in _llm_cache:
#             print(f"Using cached LLM result for headers hash: {headers_hash}")
#             llm_result = _llm_cache[headers_hash]
#         else:
#             print(f"ML confidence ({ml_result['confidence']}) below threshold ({CONFIDENCE_THRESHOLD}). Calling LLM for verification...")
#             sample_rows = df.head(3).to_dict(orient="records")

#             llm_result = llm_detect_source(headers, sample_rows)
#             print("LLM Result:", llm_result)
            
#             # Cache the result
#             _llm_cache[headers_hash] = llm_result
#             print(f"Cached LLM result for headers hash: {headers_hash}")

#         # Check if LLM failed (returned UNKNOWN or error)
#         llm_source = llm_result.get("source", "UNKNOWN")
#         llm_failed = (
#             llm_source == "UNKNOWN" or 
#             "error" in llm_result or 
#             "Rate limit exceeded" in llm_result.get("reason", "")
#         )
        
#         if llm_failed:
#             print(f"LLM failed or returned UNKNOWN. Falling back to ML prediction: {ml_result['source']}")
#             # Use ML prediction as fallback when LLM fails
#             result = {
#                 "ml_prediction": {
#                     "source": ml_result["source"],
#                     "confidence": ml_result["confidence"],
#                     "features": ml_result["features"]
#                 },
#                 "llm_prediction": {
#                     "source": llm_result.get("source", "UNKNOWN"),
#                     "channel": llm_result.get("channel", "UNKNOWN"),
#                     "reason": llm_result.get("reason", "No reason provided"),
#                     "raw": llm_result
#                 },
#                 "decision": "ML_FALLBACK",  # Indicate this was a fallback
#                 "recommended_source": ml_result["source"],  # Use ML prediction
#                 "agreement": False,
#                 "confidence_threshold": CONFIDENCE_THRESHOLD,
#                 "cached": headers_hash in _llm_cache,
#                 "fallback_reason": "LLM failed or returned UNKNOWN, using ML prediction"
#             }
#         else:
#             # Combine both results when LLM is successful
#             result = {
#                 "ml_prediction": {
#                     "source": ml_result["source"],
#                     "confidence": ml_result["confidence"],
#                     "features": ml_result["features"]
#                 },
#                 "llm_prediction": {
#                     "source": llm_result.get("source", "UNKNOWN"),
#                     "channel": llm_result.get("channel", "UNKNOWN"),
#                     "reason": llm_result.get("reason", "No reason provided"),
#                     "raw": llm_result
#                 },
#                 "decision": "LLM",
#                 "recommended_source": llm_result.get("source", "UNKNOWN"),
#                 "agreement": ml_result["source"] == llm_result.get("source", "").replace("_FILE", ""),
#                 "confidence_threshold": CONFIDENCE_THRESHOLD,
#                 "cached": headers_hash in _llm_cache
#             }
#     else:
#         print(f"ML confidence ({ml_result['confidence']}) is high. Using ML prediction only.")
#         # Use ML prediction only when confidence is high
#         result = {
#             "ml_prediction": {
#                 "source": ml_result["source"],
#                 "confidence": ml_result["confidence"],
#                 "features": ml_result["features"]
#             },
#             "llm_prediction": None,
#             "decision": "ML",
#             "recommended_source": ml_result["source"],
#             "agreement": True,
#             "confidence_threshold": CONFIDENCE_THRESHOLD,
#             "cached": False
#         }
    
#     return result

# # def get_source_system_from_features(features, filename=None, headers=None):
# #     """
# #     Improved CBS detection: returns 'CBS' if account column and CBS-like columns or filename are present.
# #     """
# #     cbs_column_signatures = [
# #         "reviveraccountnumber", "txn_date_time", "transactionid", "host_ref_number"
# #     ]
# #     cbs_indicators = 0
# #     if headers:
# #         normalized_headers = [h.lower().strip().replace(' ', '').replace('_', '') for h in headers]
# #         cbs_indicators += int(any(h in cbs_column_signatures for h in normalized_headers))
# #     if filename and "cbs" in filename.lower():
# #         cbs_indicators += 1
# #     if features.get("has_account") and cbs_indicators > 0:
# #         return "CBS"
# #     # Example logic: if it has debit/credit columns and account, it's CBS
# #     if features.get("has_debit_credit") and features.get("has_account"):
# #         return "CBS"
# #     # Add more rules for other systems if needed
# #     return "UNKNOWN"

# # Example usage after prediction:
# # result = predict_source_with_fallback(df)
# # features = result["ml_prediction"]["features"]
# # source_system = get_source_system_from_features(features)
# # print(f"Source system: {source_system}")


# import pandas as pd
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.preprocessing import LabelEncoder
# from app.utils.llm_detect_source import llm_detect_source
# import hashlib
# import json

# CONFIDENCE_THRESHOLD = 0.75  # Increased to use LLM more often
# # threshold = 0.75

# # Simple in-memory cache for LLM results (keyed by column headers hash)
# _llm_cache = {}

# def get_headers_hash(headers):
#     """Create a hash of column headers for caching"""
#     headers_str = json.dumps(sorted(headers), sort_keys=True)
#     return hashlib.md5(headers_str.encode()).hexdigest()

# def extract_features(df):
#     headers = [c.lower() for c in df.columns]

#     # More specific ATM indicators
#     has_atm_indicators = int(any("atm" in h or "terminal" in h and ("id" in h or "location" in h) for h in headers))
#     has_transaction_type = int(any("transaction" in h and "type" in h or "transactiontype" in h for h in headers))
    
#     # PAN should only count for CARD if it's not masked and in ATM context
#     # If we have ATM indicators + masked PAN, it's ATM not CARD
#     has_pure_pan = int(any("pan" in h and "mask" not in h for h in headers))
    
#     # CBS-specific indicators (NEW)
#     has_host_ref = int(any("host" in h and "ref" in h or "host_ref" in h or "hostref" in h for h in headers))
#     has_reviver_account = int(any("reviver" in h and "account" in h or "reviveraccount" in h for h in headers))
#     has_cbs_codes = 0
    
#     # Check for CBS transaction codes in data (NEW)
#     if has_host_ref:
#         for col in df.columns:
#             if "host" in col.lower() and "ref" in col.lower():
#                 sample_values = df[col].astype(str).head(10)
#                 if any(any(code in str(val).upper() for code in ["UDCN", "MBCN", "CBCN", "MBCM", "IBCN"]) 
#                        for val in sample_values):
#                     has_cbs_codes = 1
#                     break
    
#     # Mobile Money / E-Money indicators
#     has_mobile_number = int(any("mobile" in h and "number" in h or "phone" in h and "number" in h or "customer_mobile" in h for h in headers))
#     has_service_name = int(any("service" in h and "name" in h or "provider" in h or "payer" in h and "client" in h for h in headers))
#     has_payment_mode = int(any("payment" in h and "mode" in h or "channel" in h and "type" in h for h in headers))
#     has_narration = int(any("narration" in h or "description" in h or "message" in h or "remarks" in h for h in headers))
    
#     # Distinguish between CBS and Mobile Money (NEW LOGIC)
#     # CBS can have mobile numbers for customer contact, but won't have service_name/payment_mode
#     is_likely_cbs = (has_host_ref or has_reviver_account or has_cbs_codes)
#     is_likely_mobile_money = (has_mobile_number and (has_service_name or has_payment_mode))
    
#     # If CBS indicators are present, don't count mobile_number as a mobile money indicator
#     if is_likely_cbs and not is_likely_mobile_money:
#         has_mobile_number = 0
    
#     return {
#         "column_count": len(headers),
#         "has_rrn": int(any("rrn" in h for h in headers)),
#         "has_terminal": int(any("terminal" in h for h in headers)),
#         "has_merchant": int(any("merchant" in h or "mid" in h for h in headers)),
#         "has_pan": has_pure_pan,  # Only non-masked PAN
#         "has_auth": int(any("auth" in h for h in headers)),
#         "has_balance": int(any("balance" in h for h in headers)),
#         # Detect debit/credit columns: full names OR abbreviations (DR/CR)
#         "has_debit_credit": int(any("debit" in h or "credit" in h or h == "dr" or h == "cr" or "dr_" in h or "cr_" in h for h in headers)),
#         "has_mti": int(any("mti" in h or "message" in h and "type" in h for h in headers)),
#         "has_direction": int(any("direction" in h or "inbound" in h or "outbound" in h for h in headers)),
#         "has_processing_code": int(any("processing" in h and "code" in h or "proc" in h and "code" in h for h in headers)),
#         # Additional CBS indicators
#         "has_posted": int(any("posted" in h or "status" in h for h in headers)),
#         "has_account": int(any("account" in h for h in headers)),
#         # Enhanced ATM detection
#         "has_atm_indicators": has_atm_indicators,
#         "has_transaction_type": has_transaction_type,
#         # CBS-specific features (NEW)
#         "has_host_ref": has_host_ref,
#         "has_reviver_account": has_reviver_account,
#         "has_cbs_codes": has_cbs_codes,
#         # Mobile Money / E-Money indicators
#         "has_mobile_number": has_mobile_number,
#         "has_service_name": has_service_name,
#         "has_payment_mode": has_payment_mode,
#         "has_narration": has_narration,
#     }

# # ---- TRAIN MODEL ON STARTUP ----
# # Training patterns based on typical file structures:
# # - ATM: ATM operational logs with location/transaction type
# # - SWITCH: Raw switch messages with MTI/Direction (can be ATM/POS/CARDS channel)
# # - POS: POS transactions with merchant details
# # - CARD: Card network settlement files (pure card data, no ATM context)
# # - BANK: Core banking system posted transactions with DR/CR columns
# # - CBS: Core Banking System files with host_ref, reviver_account, CBS codes (NEW)
# # - MOBILE_MONEY: E-Money/Mobile Money platform transactions with mobile number, service name, payment mode
# training_data = pd.DataFrame([
#     # ATM operational files (customer-facing) - has terminal, RRN, auth, transaction type
#     # column_count, has_rrn, has_terminal, has_merchant, has_pan, has_auth, has_balance, has_debit_credit, 
#     # has_mti, has_direction, has_processing_code, has_posted, has_account, has_atm_indicators, has_transaction_type,
#     # has_host_ref, has_reviver_account, has_cbs_codes, has_mobile_number, has_service_name, has_payment_mode, has_narration, label
#     [14, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],  # Your ATM file pattern
#     [10, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],
#     [11, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
#     [12, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],
    
#     # SWITCH files (raw switch messages) - ATM channel
#     [12, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [13, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [14, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    
#     # SWITCH files - POS channel (has merchant indicators)
#     [15, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [16, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    
#     # POS transaction files (not switch) - merchant focused
#     [12, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
#     [13, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
#     [11, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    
#     # CARD network files - pure card data, NO ATM indicators
#     [20, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
#     [18, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
#     [15, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
    
#     # BANK/CBS posted transactions (with DR/CR columns)
#     [8, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [10, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [10, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [12, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    
#     # CBS files (Core Banking System) - NEW
#     # Has host_ref, reviver_account, cbs_codes, mobile (for contact), account, but NO service_name/payment_mode
#     [8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],  # Your CBS file pattern
#     [9, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],  # With RRN
#     [10, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],  # With posted and DR/CR
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 1, "CBS"],  # Without reviver but has host_ref and cbs_codes
#     [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, "CBS"],  # Has reviver_account
    
#     # MOBILE_MONEY / E-Money platform files - has mobile number, service name, payment mode
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # Your E-Money file pattern
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # With status
#     [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # With account
#     [13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # With posted status
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, "MOBILE_MONEY"],  # Without narration
#     [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, "MOBILE_MONEY"],  # Without service_name
# ], columns=[
#     "column_count", "has_rrn", "has_terminal", "has_merchant",
#     "has_pan", "has_auth", "has_balance", "has_debit_credit",
#     "has_mti", "has_direction", "has_processing_code",
#     "has_posted", "has_account", "has_atm_indicators", "has_transaction_type",
#     "has_host_ref", "has_reviver_account", "has_cbs_codes",
#     "has_mobile_number", "has_service_name", "has_payment_mode", "has_narration", "label"
# ])

# X = training_data.drop("label", axis=1)
# y = training_data["label"]

# encoder = LabelEncoder()
# y_enc = encoder.fit_transform(y)

# model = RandomForestClassifier(n_estimators=200, random_state=42)
# model.fit(X, y_enc)

# def predict_source(df):
#     features = extract_features(df)
#     df_feat = pd.DataFrame([features])

#     probs = model.predict_proba(df_feat)[0]
#     idx = probs.argmax()

#     return {
#         "source": encoder.inverse_transform([idx])[0],
#         "confidence": round(float(probs[idx]), 2),
#         "features": features
#     }


# def predict_source_with_fallback(df):
#     # Always run ML prediction first
#     ml_result = predict_source(df)
    
#     print(f"ML Prediction - Source: {ml_result['source']}, Confidence: {ml_result['confidence']}")

#     # Only call LLM if ML confidence is below threshold
#     if ml_result["confidence"] < CONFIDENCE_THRESHOLD:
#         headers = list(df.columns)
#         headers_hash = get_headers_hash(headers)
        
#         # Check cache first
#         if headers_hash in _llm_cache:
#             print(f"Using cached LLM result for headers hash: {headers_hash}")
#             llm_result = _llm_cache[headers_hash]
#         else:
#             print(f"ML confidence ({ml_result['confidence']}) below threshold ({CONFIDENCE_THRESHOLD}). Calling LLM for verification...")
#             sample_rows = df.head(3).to_dict(orient="records")

#             llm_result = llm_detect_source(headers, sample_rows)
#             print("LLM Result:", llm_result)
            
#             # Cache the result
#             _llm_cache[headers_hash] = llm_result
#             print(f"Cached LLM result for headers hash: {headers_hash}")

#         # Check if LLM failed (returned UNKNOWN or error)
#         llm_source = llm_result.get("source", "UNKNOWN")
#         llm_failed = (
#             llm_source == "UNKNOWN" or 
#             "error" in llm_result or 
#             "Rate limit exceeded" in llm_result.get("reason", "")
#         )
        
#         if llm_failed:
#             print(f"LLM failed or returned UNKNOWN. Falling back to ML prediction: {ml_result['source']}")
#             # Use ML prediction as fallback when LLM fails
#             result = {
#                 "ml_prediction": {
#                     "source": ml_result["source"],
#                     "confidence": ml_result["confidence"],
#                     "features": ml_result["features"]
#                 },
#                 "llm_prediction": {
#                     "source": llm_result.get("source", "UNKNOWN"),
#                     "channel": llm_result.get("channel", "UNKNOWN"),
#                     "reason": llm_result.get("reason", "No reason provided"),
#                     "raw": llm_result
#                 },
#                 "decision": "ML_FALLBACK",  # Indicate this was a fallback
#                 "recommended_source": ml_result["source"],  # Use ML prediction
#                 "agreement": False,
#                 "confidence_threshold": CONFIDENCE_THRESHOLD,
#                 "cached": headers_hash in _llm_cache,
#                 "fallback_reason": "LLM failed or returned UNKNOWN, using ML prediction"
#             }
#         else:
#             # Combine both results when LLM is successful
#             result = {
#                 "ml_prediction": {
#                     "source": ml_result["source"],
#                     "confidence": ml_result["confidence"],
#                     "features": ml_result["features"]
#                 },
#                 "llm_prediction": {
#                     "source": llm_result.get("source", "UNKNOWN"),
#                     "channel": llm_result.get("channel", "UNKNOWN"),
#                     "reason": llm_result.get("reason", "No reason provided"),
#                     "raw": llm_result
#                 },
#                 "decision": "LLM",
#                 "recommended_source": llm_result.get("source", "UNKNOWN"),
#                 "agreement": ml_result["source"] == llm_result.get("source", "").replace("_FILE", ""),
#                 "confidence_threshold": CONFIDENCE_THRESHOLD,
#                 "cached": headers_hash in _llm_cache
#             }
#     else:
#         print(f"ML confidence ({ml_result['confidence']}) is high. Using ML prediction only.")
#         # Use ML prediction only when confidence is high
#         result = {
#             "ml_prediction": {
#                 "source": ml_result["source"],
#                 "confidence": ml_result["confidence"],
#                 "features": ml_result["features"]
#             },
#             "llm_prediction": None,
#             "decision": "ML",
#             "recommended_source": ml_result["source"],
#             "agreement": True,
#             "confidence_threshold": CONFIDENCE_THRESHOLD,
#             "cached": False
#         }
    
#     return result

# def get_source_system_from_features(features, filename=None, headers=None):
#     """
#     Improved CBS detection: returns 'CBS' if account column and CBS-like columns or filename are present.
#     """
#     cbs_column_signatures = [
#         "reviveraccountnumber", "txn_date_time", "transactionid", "host_ref_number"
#     ]
#     cbs_indicators = 0
#     if headers:
#         normalized_headers = [h.lower().strip().replace(' ', '').replace('_', '') for h in headers]
#         cbs_indicators += int(any(h in cbs_column_signatures for h in normalized_headers))
#     if filename and "cbs" in filename.lower():
#         cbs_indicators += 1
#     if features.get("has_account") and cbs_indicators > 0:
#         return "CBS"
#     # Example logic: if it has debit/credit columns and account, it's CBS
#     if features.get("has_debit_credit") and features.get("has_account"):
#         return "CBS"
#     # Add more rules for other systems if needed
#     return "UNKNOWN"

# Example usage after prediction:
# result = predict_source_with_fallback(df)
# features = result["ml_prediction"]["features"]
# source_system = get_source_system_from_features(features)
# print(f"Source system: {source_system}")


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
    
    # CBS-specific indicators (NEW)
    has_host_ref = int(any("host" in h and "ref" in h or "host_ref" in h or "hostref" in h for h in headers))
    has_reviver_account = int(any("reviver" in h and "account" in h or "reviveraccount" in h for h in headers))
    has_cbs_codes = 0
    
    # Check for CBS transaction codes in data (NEW)
    if has_host_ref:
        for col in df.columns:
            if "host" in col.lower() and "ref" in col.lower():
                sample_values = df[col].astype(str).head(10)
                if any(any(code in str(val).upper() for code in ["UDCN", "MBCN", "CBCN", "MBCM", "IBCN"]) 
                       for val in sample_values):
                    has_cbs_codes = 1
                    break
    
    # Mobile Money / E-Money indicators
    has_mobile_number = int(any("mobile" in h and "number" in h or "phone" in h and "number" in h or "customer_mobile" in h for h in headers))
    has_service_name = int(any("service" in h and "name" in h or "provider" in h or "payer" in h and "client" in h for h in headers))
    has_payment_mode = int(any("payment" in h and "mode" in h or "channel" in h and "type" in h for h in headers))
    has_narration = int(any("narration" in h or "description" in h or "message" in h or "remarks" in h for h in headers))
    
    # Distinguish between CBS and Mobile Money (NEW LOGIC)
    # CBS can have mobile numbers for customer contact, but won't have service_name/payment_mode
    is_likely_cbs = (has_host_ref or has_reviver_account or has_cbs_codes)
    is_likely_mobile_money = (has_mobile_number and (has_service_name or has_payment_mode))
    
    # If CBS indicators are present, don't count mobile_number as a mobile money indicator
    if is_likely_cbs and not is_likely_mobile_money:
        has_mobile_number = 0
    
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
        # CBS-specific features (NEW)
        "has_host_ref": has_host_ref,
        "has_reviver_account": has_reviver_account,
        "has_cbs_codes": has_cbs_codes,
        # Mobile Money / E-Money indicators
        "has_mobile_number": has_mobile_number,
        "has_service_name": has_service_name,
        "has_payment_mode": has_payment_mode,
        "has_narration": has_narration,
    }

# ---- TRAIN MODEL ON STARTUP ----
# Training patterns based on typical file structures:
# - ATM: ATM operational logs with location/transaction type
# - SWITCH: Raw switch messages with MTI/Direction (can be ATM/POS/CARDS channel)
# - POS: POS transactions with merchant details
# - CARD: Card network settlement files (pure card data, no ATM context)
# - BANK: Core banking system posted transactions with DR/CR columns
# - CBS: Core Banking System files with host_ref, reviver_account, CBS codes (NEW)
# - MOBILE_MONEY: E-Money/Mobile Money platform transactions with mobile number, service name, payment mode
training_data = pd.DataFrame([
    # ATM operational files (customer-facing) - has terminal, RRN, auth, transaction type
    # column_count, has_rrn, has_terminal, has_merchant, has_pan, has_auth, has_balance, has_debit_credit, 
    # has_mti, has_direction, has_processing_code, has_posted, has_account, has_atm_indicators, has_transaction_type,
    # has_host_ref, has_reviver_account, has_cbs_codes, has_mobile_number, has_service_name, has_payment_mode, has_narration, label
    [14, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],  # Your ATM file pattern
    [10, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],
    [11, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
    [12, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],
    
    # SWITCH files (raw switch messages) - ATM channel
    [12, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    [13, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    [14, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    
    # SWITCH files - POS channel (has merchant indicators)
    [15, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    [16, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    
    # POS transaction files (not switch) - merchant focused
    [12, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    [13, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    [11, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    
    # CARD network files - pure card data, NO ATM indicators
    [20, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
    [18, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
    [15, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
    
    # BANK/CBS posted transactions (with DR/CR columns)
    [8, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    [10, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    [10, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    [12, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    
    # CBS files (Core Banking System) - NEW
    # Has host_ref, reviver_account, cbs_codes, mobile (for contact), account, but NO service_name/payment_mode
    [8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],  # Your CBS file pattern
    [9, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],  # With RRN
    [10, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],  # With posted and DR/CR
    [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 1, "CBS"],  # Without reviver but has host_ref and cbs_codes
    [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, "CBS"],  # Has reviver_account
    
    # MOBILE_MONEY / E-Money platform files - has mobile number, service name, payment mode
    [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # Your E-Money file pattern
    [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # With status
    [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # With account
    [13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],  # With posted status
    [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, "MOBILE_MONEY"],  # Without narration
    [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, "MOBILE_MONEY"],  # Without service_name
], columns=[
    "column_count", "has_rrn", "has_terminal", "has_merchant",
    "has_pan", "has_auth", "has_balance", "has_debit_credit",
    "has_mti", "has_direction", "has_processing_code",
    "has_posted", "has_account", "has_atm_indicators", "has_transaction_type",
    "has_host_ref", "has_reviver_account", "has_cbs_codes",
    "has_mobile_number", "has_service_name", "has_payment_mode", "has_narration", "label"
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


def detect_channel(df, source):
    """
    Detect the channel (ATM, POS, MOBILE_MONEY, CARD, etc.) based on source and features.
    For CBS files, this determines which channel the transactions belong to.
    """
    headers = [c.lower() for c in df.columns]
    
    # If source is CBS, we need to determine the channel
    if source == "CBS":
        # Check for Mobile Money indicators
        has_mobile_indicators = (
            any("mobile" in h and "number" in h for h in headers) or
            any("phone" in h for h in headers)
        )
        
        # Check transaction patterns in Host_ref_number for Mobile Money codes
        has_mm_codes = False
        for col in df.columns:
            if "host" in col.lower() and "ref" in col.lower():
                sample_values = df[col].astype(str).head(20)
                # Mobile Money CBS codes typically include: MBCN, MBCM
                if any(any(code in str(val).upper() for code in ["MBCN", "MBCM"]) 
                       for val in sample_values):
                    has_mm_codes = True
                    break
        
        # Check for ATM indicators
        has_atm_indicators = (
            any("atm" in h for h in headers) or
            any("terminal" in h and ("id" in h or "location" in h) for h in headers)
        )
        
        # Check transaction patterns for ATM codes
        has_atm_codes = False
        for col in df.columns:
            if "host" in col.lower() and "ref" in col.lower():
                sample_values = df[col].astype(str).head(20)
                # ATM CBS codes might be different from Mobile Money
                if any(any(code in str(val).upper() for code in ["UDCN", "CBCN", "IBCN"]) 
                       for val in sample_values):
                    has_atm_codes = True
                    break
        
        # Check for POS indicators
        has_pos_indicators = any("merchant" in h or "mid" in h for h in headers)
        
        # Decision logic for CBS channel detection
        if has_mm_codes or (has_mobile_indicators and not has_atm_indicators):
            return "MOBILE_MONEY"
        elif has_atm_indicators or has_atm_codes:
            return "ATM"
        elif has_pos_indicators:
            return "POS"
        else:
            # Default to the transaction codes pattern
            # If we see mostly MBCN/MBCM -> Mobile Money
            # If we see mostly UDCN/CBCN -> ATM/General CBS
            return "CBS_GENERAL"  # Generic CBS channel
    
    # For non-CBS sources, channel = source
    elif source == "ATM":
        return "ATM"
    elif source == "POS":
        return "POS"
    elif source == "MOBILE_MONEY":
        return "MOBILE_MONEY"
    elif source == "CARD":
        return "CARD"
    elif source == "SWITCH":
        return "SWITCH"
    elif source == "BANK":
        return "BANK"
    else:
        return source


def predict_source_with_fallback(df):
    # Always run ML prediction first
    ml_result = predict_source(df)
    
    # Detect channel based on source and features
    detected_channel = detect_channel(df, ml_result["source"])
    
    print(f"ML Prediction - Source: {ml_result['source']}, Channel: {detected_channel}, Confidence: {ml_result['confidence']}")

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
                    "channel": detected_channel,
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
                "recommended_channel": detected_channel,
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
                    "channel": detected_channel,
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
                "recommended_channel": llm_result.get("channel", detected_channel),
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
                "channel": detected_channel,
                "confidence": ml_result["confidence"],
                "features": ml_result["features"]
            },
            "llm_prediction": None,
            "decision": "ML",
            "recommended_source": ml_result["source"],
            "recommended_channel": detected_channel,
            "agreement": True,
            "confidence_threshold": CONFIDENCE_THRESHOLD,
            "cached": False
        }
    
    return result

# def get_source_system_from_features(features, filename=None, headers=None):
#     """
#     Improved CBS detection: returns 'CBS' if account column and CBS-like columns or filename are present.
#     """
#     cbs_column_signatures = [
#         "reviveraccountnumber", "txn_date_time", "transactionid", "host_ref_number"
#     ]
#     cbs_indicators = 0
#     if headers:
#         normalized_headers = [h.lower().strip().replace(' ', '').replace('_', '') for h in headers]
#         cbs_indicators += int(any(h in cbs_column_signatures for h in normalized_headers))
#     if filename and "cbs" in filename.lower():
#         cbs_indicators += 1
#     if features.get("has_account") and cbs_indicators > 0:
#         return "CBS"
#     # Example logic: if it has debit/credit columns and account, it's CBS
#     if features.get("has_debit_credit") and features.get("has_account"):
#         return "CBS"
#     # Add more rules for other systems if needed
#     return "UNKNOWN"

# Example usage after prediction:
# result = predict_source_with_fallback(df)
# features = result["ml_prediction"]["features"]
# source_system = get_source_system_from_features(features)
# print(f"Source system: {source_system}")