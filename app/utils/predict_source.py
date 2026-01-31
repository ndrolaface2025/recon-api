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


# import pandas as pd
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.preprocessing import LabelEncoder
# from app.utils.llm_detect_source import llm_detect_source
# import hashlib
# import json

# CONFIDENCE_THRESHOLD = 0.75  # Increased to use LLM more often

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
    
#     # CBS-specific indicators
#     has_host_ref = int(any("host" in h and "ref" in h or "host_ref" in h or "hostref" in h for h in headers))
#     has_reviver_account = int(any("reviver" in h and "account" in h or "reviveraccount" in h for h in headers))
#     has_cbs_codes = 0
    
#     # Check for CBS transaction codes in data
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
    
#     # Distinguish between CBS and Mobile Money
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
#         # CBS-specific features
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
# # Training patterns based on typical file structures
# training_data = pd.DataFrame([
#     # ATM operational files (customer-facing) - has terminal, RRN, auth, transaction type
#     # column_count, has_rrn, has_terminal, has_merchant, has_pan, has_auth, has_balance, has_debit_credit, 
#     # has_mti, has_direction, has_processing_code, has_posted, has_account, has_atm_indicators, has_transaction_type,
#     # has_host_ref, has_reviver_account, has_cbs_codes, has_mobile_number, has_service_name, has_payment_mode, has_narration, label
#     [14, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],
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
    
#     # CBS files (Core Banking System)
#     [8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],
#     [9, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],
#     [10, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 1, "CBS"],
#     [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, "CBS"],
    
#     # MOBILE_MONEY / E-Money platform files
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, "MOBILE_MONEY"],
#     [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, "MOBILE_MONEY"],
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


# def detect_channel(df, source):
#     """
#     Detect the channel (ATM, POS, MOBILE_MONEY, CARD, etc.) based on source and features.
#     For CBS and BANK files, this determines which channel the transactions belong to.
#     """
#     headers = [c.lower() for c in df.columns]
    
#     # Enhanced detection for BANK source files
#     if source == "BANK":
#         # Check for ATM structural patterns
#         has_stan = any("stan" in h for h in headers)
#         has_rrn = any("rrn" in h for h in headers)
#         has_fc_txn = any("fc_txn" in h or ("fc" in h and "txn" in h) for h in headers)
#         # Check for masked account (both "account_masked" and "masked_account" patterns)
#         has_masked_account = any(("account" in h and "mask" in h) or ("mask" in h and "account" in h) for h in headers)
#         has_dr_cr_columns = any(h == "dr" or h == "cr" or "dr_" in h or "cr_" in h for h in headers)
#         has_account_column = any("account" in h for h in headers)
        
#         # CRITICAL FIX: Prioritize STAN + RRN + Account structure for ATM
#         # This is the strongest indicator of ATM transactions
#         is_atm_structure = (
#             (has_stan and has_rrn and has_account_column) or  # STAN + RRN + Account = ATM
#             (has_stan and has_rrn and has_dr_cr_columns) or   # STAN + RRN + DR/CR = ATM
#             (has_fc_txn and has_stan and has_dr_cr_columns)   # FC_TXN + STAN + DR/CR = ATM
#         )
        
#         # Check for Mobile Money indicators
#         has_mobile_indicators = (
#             any("mobile" in h and "number" in h for h in headers) or
#             any("phone" in h for h in headers) or
#             any("service" in h and "name" in h for h in headers) or
#             any("payment" in h and "mode" in h for h in headers)
#         )
        
#         # Check for POS indicators
#         has_pos_indicators = (
#             any("merchant" in h or "mid" in h for h in headers) or
#             any("pos" in h for h in headers)
#         )
        
#         # Check for Internet/Online Banking indicators
#         has_internet_banking_indicators = (
#             any("internet" in h for h in headers) or
#             any("online" in h for h in headers) or
#             any("web" in h for h in headers)
#         )
        
#         # PRIORITY 1: Check for ATM structural pattern FIRST (before other checks)
#         # This is the most reliable indicator for your ATM files
#         if is_atm_structure and not has_mobile_indicators and not has_pos_indicators:
#             return "ATM"
        
#         # Analyze transaction descriptions/narrations for channel hints
#         description_cols = [col for col in df.columns if any(
#             keyword in col.lower() for keyword in ["description", "narration", "remarks", "message"]
#         )]
        
#         channel_hints = {
#             "ATM": 0,
#             "POS": 0,
#             "INTERNET_BANKING": 0,
#             "MOBILE_MONEY": 0
#         }
        
#         if description_cols:
#             for col in description_cols:
#                 sample_values = df[col].astype(str).head(20)
#                 for val in sample_values:
#                     val_upper = str(val).upper()
#                     # Skip empty/null values
#                     if val_upper in ["", "NAN", "NONE", "0"]:
#                         continue
#                     # ATM keywords
#                     if any(keyword in val_upper for keyword in ["ATM", "CASH WITHDRAWAL", "AUTOMATED TELLER", "WITHDRAWAL"]):
#                         channel_hints["ATM"] += 1
#                     # POS keywords
#                     if any(keyword in val_upper for keyword in ["POS", "PURCHASE", "MERCHANT", "CARD PAYMENT"]):
#                         channel_hints["POS"] += 1
#                     # Internet Banking keywords
#                     if any(keyword in val_upper for keyword in ["INTERNET", "ONLINE", "WEB", "E-BANKING", "TRANSFER", "NEFT", "RTGS", "IMPS"]):
#                         channel_hints["INTERNET_BANKING"] += 1
#                     # Mobile Money keywords
#                     if any(keyword in val_upper for keyword in ["MOBILE", "M-PESA", "AIRTEL", "MTN", "MOBILE MONEY"]):
#                         channel_hints["MOBILE_MONEY"] += 1
        
#         # PRIORITY 2: Check for explicit header indicators
#         has_atm_in_headers = any("atm" in h for h in headers)
#         if has_atm_in_headers or channel_hints["ATM"] > 0:
#             return "ATM"
#         elif has_mobile_indicators or channel_hints["MOBILE_MONEY"] > 0:
#             return "MOBILE_MONEY"
#         elif has_pos_indicators or channel_hints["POS"] > 0:
#             return "POS"
#         elif has_internet_banking_indicators or channel_hints["INTERNET_BANKING"] > 0:
#             return "INTERNET_BANKING"
        
#         # PRIORITY 3: Final fallback - use structure even without explicit indicators
#         if is_atm_structure:
#             return "ATM"
        
#         # PRIORITY 4: Check description hints
#         if channel_hints and max(channel_hints.values()) > 0:
#             max_channel = max(channel_hints, key=channel_hints.get)
#             return max_channel
        
#         return "BANK"  # Default to generic BANK if no clear channel
    
#     # Enhanced detection for CBS source files
#     elif source == "CBS":
#         # Check for Mobile Money indicators
#         has_mobile_indicators = (
#             any("mobile" in h and "number" in h for h in headers) or
#             any("phone" in h for h in headers)
#         )
        
#         # Check transaction patterns in Host_ref_number for Mobile Money codes
#         has_mm_codes = False
#         for col in df.columns:
#             if "host" in col.lower() and "ref" in col.lower():
#                 sample_values = df[col].astype(str).head(20)
#                 if any(any(code in str(val).upper() for code in ["MBCN", "MBCM"]) 
#                        for val in sample_values):
#                     has_mm_codes = True
#                     break
        
#         # Check for ATM indicators
#         has_atm_indicators = (
#             any("atm" in h for h in headers) or
#             any("terminal" in h and ("id" in h or "location" in h) for h in headers)
#         )
        
#         # Check transaction patterns for ATM codes
#         has_atm_codes = False
#         for col in df.columns:
#             if "host" in col.lower() and "ref" in col.lower():
#                 sample_values = df[col].astype(str).head(20)
#                 if any(any(code in str(val).upper() for code in ["UDCN", "CBCN", "IBCN"]) 
#                        for val in sample_values):
#                     has_atm_codes = True
#                     break
        
#         # Check for POS indicators
#         has_pos_indicators = any("merchant" in h or "mid" in h for h in headers)
        
#         # Decision logic for CBS channel detection
#         if has_mm_codes or (has_mobile_indicators and not has_atm_indicators):
#             return "MOBILE_MONEY"
#         elif has_atm_indicators or has_atm_codes:
#             return "ATM"
#         elif has_pos_indicators:
#             return "POS"
#         else:
#             return "CBS_GENERAL"
    
#     # For other sources, channel = source
#     elif source == "ATM":
#         return "ATM"
#     elif source == "POS":
#         return "POS"
#     elif source == "MOBILE_MONEY":
#         return "MOBILE_MONEY"
#     elif source == "CARD":
#         return "CARD"
#     elif source == "SWITCH":
#         return "SWITCH"
#     else:
#         return source


# def predict_source_with_fallback(df):
#     # Always run ML prediction first
#     ml_result = predict_source(df)
    
#     # Detect channel based on source and features
#     detected_channel = detect_channel(df, ml_result["source"])
    
#     print(f"ML Prediction - Source: {ml_result['source']}, Channel: {detected_channel}, Confidence: {ml_result['confidence']}")

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
#                     "channel": detected_channel,
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
#                 "recommended_channel": detected_channel,
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
#                     "channel": detected_channel,
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
#                 "recommended_channel": llm_result.get("channel", detected_channel),
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
#                 "channel": detected_channel,
#                 "confidence": ml_result["confidence"],
#                 "features": ml_result["features"]
#             },
#             "llm_prediction": None,
#             "decision": "ML",
#             "recommended_source": ml_result["source"],
#             "recommended_channel": detected_channel,
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


# import pandas as pd
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.preprocessing import LabelEncoder
# from app.utils.llm_detect_source import llm_detect_source
# import hashlib
# import json

# CONFIDENCE_THRESHOLD = 0.75

# # Simple in-memory cache for LLM results (keyed by column headers hash)
# _llm_cache = {}

# def clear_cache():
#     """Clear the LLM cache - call this when you update detection logic"""
#     global _llm_cache
#     _llm_cache = {}
#     print("Cache cleared!")

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
#     has_pure_pan = int(any("pan" in h and "mask" not in h for h in headers))
    
#     # CBS-specific indicators
#     has_host_ref = int(any("host" in h and "ref" in h or "host_ref" in h or "hostref" in h for h in headers))
#     has_reviver_account = int(any("reviver" in h and "account" in h or "reviveraccount" in h for h in headers))
#     has_cbs_codes = 0
    
#     # Check for CBS transaction codes in data
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
    
#     # Distinguish between CBS and Mobile Money
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
#         "has_pan": has_pure_pan,
#         "has_auth": int(any("auth" in h for h in headers)),
#         "has_balance": int(any("balance" in h for h in headers)),
#         "has_debit_credit": int(any("debit" in h or "credit" in h or h == "dr" or h == "cr" or "dr_" in h or "cr_" in h for h in headers)),
#         "has_mti": int(any("mti" in h or "message" in h and "type" in h for h in headers)),
#         "has_direction": int(any("direction" in h or "inbound" in h or "outbound" in h for h in headers)),
#         "has_processing_code": int(any("processing" in h and "code" in h or "proc" in h and "code" in h for h in headers)),
#         "has_posted": int(any("posted" in h or "status" in h for h in headers)),
#         "has_account": int(any("account" in h for h in headers)),
#         "has_atm_indicators": has_atm_indicators,
#         "has_transaction_type": has_transaction_type,
#         "has_host_ref": has_host_ref,
#         "has_reviver_account": has_reviver_account,
#         "has_cbs_codes": has_cbs_codes,
#         "has_mobile_number": has_mobile_number,
#         "has_service_name": has_service_name,
#         "has_payment_mode": has_payment_mode,
#         "has_narration": has_narration,
#     }

# # Training data
# training_data = pd.DataFrame([
#     [14, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],
#     [10, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],
#     [11, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
#     [12, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],
#     [12, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [13, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [14, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [15, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [16, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [12, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
#     [13, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
#     [11, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
#     [20, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
#     [18, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
#     [15, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "CARD"],
#     [8, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [10, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [10, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [12, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],
#     [9, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],
#     [10, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 1, "CBS"],
#     [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, "CBS"],
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, "MOBILE_MONEY"],
#     [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, "MOBILE_MONEY"],
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


# def detect_channel(df, source):
#     """
#     Detect the channel based on source and structural patterns.
#     CRITICAL FIX: Prioritizes STAN + RRN + Account pattern for ATM detection.
#     """
#     headers = [c.lower() for c in df.columns]
    
#     print(f"\n[DEBUG] detect_channel called - Source: {source}")
#     print(f"[DEBUG] Headers: {headers}")
    
#     # Enhanced detection for BANK source files
#     if source == "BANK":
#         # Check for ATM structural patterns - THESE ARE THE MOST IMPORTANT
#         has_stan = any("stan" in h for h in headers)
#         has_rrn = any("rrn" in h for h in headers)
#         has_fc_txn = any("fc_txn" in h or ("fc" in h and "txn" in h) for h in headers)
#         has_dr_cr_columns = any(h == "dr" or h == "cr" or "dr_" in h or "cr_" in h for h in headers)
#         has_account_column = any("account" in h for h in headers)
        
#         print(f"[DEBUG] ATM Structure Checks:")
#         print(f"  has_stan: {has_stan}")
#         print(f"  has_rrn: {has_rrn}")
#         print(f"  has_account_column: {has_account_column}")
#         print(f"  has_dr_cr_columns: {has_dr_cr_columns}")
#         print(f"  has_fc_txn: {has_fc_txn}")
        
#         # CRITICAL: ATM pattern detection - MUST match STAN + RRN + Account
#         is_atm_structure = (
#             (has_stan and has_rrn and has_account_column) or  # Your file matches THIS
#             (has_stan and has_rrn and has_dr_cr_columns) or   # Your file ALSO matches THIS
#             (has_fc_txn and has_stan and has_dr_cr_columns)   # Your file ALSO matches THIS
#         )
        
#         print(f"[DEBUG] is_atm_structure: {is_atm_structure}")
        
#         # Check for other channel indicators
#         has_mobile_indicators = any("mobile" in h and "number" in h for h in headers) or any("phone" in h for h in headers)
#         has_pos_indicators = any("merchant" in h or "mid" in h or "pos" in h for h in headers)
        
#         print(f"[DEBUG] has_mobile_indicators: {has_mobile_indicators}")
#         print(f"[DEBUG] has_pos_indicators: {has_pos_indicators}")
        
#         # PRIORITY 1: ATM structural pattern (HIGHEST PRIORITY - CHECK FIRST!)
#         if is_atm_structure and not has_mobile_indicators and not has_pos_indicators:
#             print(f"[DEBUG] ✅ Returning ATM (structural match)")
#             return "ATM"
        
#         # PRIORITY 2: Explicit ATM in headers
#         if any("atm" in h for h in headers):
#             print(f"[DEBUG] ✅ Returning ATM (header match)")
#             return "ATM"
        
#         # PRIORITY 3: Other channels
#         if has_mobile_indicators:
#             print(f"[DEBUG] Returning MOBILE_MONEY")
#             return "MOBILE_MONEY"
#         elif has_pos_indicators:
#             print(f"[DEBUG] Returning POS")
#             return "POS"
        
#         # PRIORITY 4: Final ATM fallback
#         if is_atm_structure:
#             print(f"[DEBUG] ✅ Returning ATM (fallback with structure)")
#             return "ATM"
        
#         print(f"[DEBUG] Returning BANK (no clear channel)")
#         return "BANK"
    
#     # CBS source detection
#     elif source == "CBS":
#         has_mobile_indicators = any("mobile" in h and "number" in h for h in headers) or any("phone" in h for h in headers)
#         has_mm_codes = False
#         for col in df.columns:
#             if "host" in col.lower() and "ref" in col.lower():
#                 sample_values = df[col].astype(str).head(20)
#                 if any(any(code in str(val).upper() for code in ["MBCN", "MBCM"]) for val in sample_values):
#                     has_mm_codes = True
#                     break
        
#         has_atm_indicators = any("atm" in h for h in headers) or any("terminal" in h and ("id" in h or "location" in h) for h in headers)
#         has_atm_codes = False
#         for col in df.columns:
#             if "host" in col.lower() and "ref" in col.lower():
#                 sample_values = df[col].astype(str).head(20)
#                 if any(any(code in str(val).upper() for code in ["UDCN", "CBCN", "IBCN"]) for val in sample_values):
#                     has_atm_codes = True
#                     break
        
#         has_pos_indicators = any("merchant" in h or "mid" in h for h in headers)
        
#         if has_mm_codes or (has_mobile_indicators and not has_atm_indicators):
#             return "MOBILE_MONEY"
#         elif has_atm_indicators or has_atm_codes:
#             return "ATM"
#         elif has_pos_indicators:
#             return "POS"
#         else:
#             return "CBS_GENERAL"
    
#     # For other sources, channel = source
#     return source


# def predict_source_with_fallback(df):
#     # Always run ML prediction first
#     ml_result = predict_source(df)
    
#     # Detect channel based on source and features
#     detected_channel = detect_channel(df, ml_result["source"])
    
#     print(f"\n[MAIN] ML Prediction - Source: {ml_result['source']}, Channel: {detected_channel}, Confidence: {ml_result['confidence']}")

#     # Only call LLM if ML confidence is below threshold
#     if ml_result["confidence"] < CONFIDENCE_THRESHOLD:
#         headers = list(df.columns)
#         headers_hash = get_headers_hash(headers)
        
#         # Check cache first
#         if headers_hash in _llm_cache:
#             print(f"[MAIN] Using cached LLM result for headers hash: {headers_hash}")
#             print(f"[MAIN] ⚠️  CACHE HIT - If you updated the code, clear the cache!")
#             llm_result = _llm_cache[headers_hash]
#         else:
#             print(f"[MAIN] ML confidence ({ml_result['confidence']}) below threshold ({CONFIDENCE_THRESHOLD}). Calling LLM...")
#             sample_rows = df.head(3).to_dict(orient="records")

#             llm_result = llm_detect_source(headers, sample_rows)
#             print(f"[MAIN] LLM Result: {llm_result}")
            
#             # Cache the result
#             _llm_cache[headers_hash] = llm_result
#             print(f"[MAIN] Cached LLM result for headers hash: {headers_hash}")

#         llm_source = llm_result.get("source", "UNKNOWN")
#         llm_failed = (
#             llm_source == "UNKNOWN" or 
#             "error" in llm_result or 
#             "Rate limit exceeded" in llm_result.get("reason", "")
#         )
        
#         if llm_failed:
#             print(f"[MAIN] LLM failed. Using ML prediction: {ml_result['source']}")
#             result = {
#                 "ml_prediction": {
#                     "source": ml_result["source"],
#                     "channel": detected_channel,
#                     "confidence": ml_result["confidence"],
#                     "features": ml_result["features"]
#                 },
#                 "llm_prediction": {
#                     "source": llm_result.get("source", "UNKNOWN"),
#                     "channel": llm_result.get("channel", "UNKNOWN"),
#                     "reason": llm_result.get("reason", "No reason provided"),
#                     "raw": llm_result
#                 },
#                 "decision": "ML_FALLBACK",
#                 "recommended_source": ml_result["source"],
#                 "recommended_channel": detected_channel,
#                 "agreement": False,
#                 "confidence_threshold": CONFIDENCE_THRESHOLD,
#                 "cached": headers_hash in _llm_cache,
#                 "fallback_reason": "LLM failed or returned UNKNOWN, using ML prediction"
#             }
#         else:
#             result = {
#                 "ml_prediction": {
#                     "source": ml_result["source"],
#                     "channel": detected_channel,
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
#                 "recommended_channel": llm_result.get("channel", detected_channel),
#                 "agreement": ml_result["source"] == llm_result.get("source", "").replace("_FILE", ""),
#                 "confidence_threshold": CONFIDENCE_THRESHOLD,
#                 "cached": headers_hash in _llm_cache
#             }
#     else:
#         print(f"[MAIN] ML confidence ({ml_result['confidence']}) is high. Using ML prediction only.")
#         result = {
#             "ml_prediction": {
#                 "source": ml_result["source"],
#                 "channel": detected_channel,
#                 "confidence": ml_result["confidence"],
#                 "features": ml_result["features"]
#             },
#             "llm_prediction": None,
#             "decision": "ML",
#             "recommended_source": ml_result["source"],
#             "recommended_channel": detected_channel,
#             "agreement": True,
#             "confidence_threshold": CONFIDENCE_THRESHOLD,
#             "cached": False
#         }
    
#     print(f"\n[MAIN] ✅ FINAL RESULT: Source={result['recommended_source']}, Channel={result['recommended_channel']}")
#     return result


# # IMPORTANT: Add this to your initialization or endpoint to clear cache when needed
# # Call this function after updating the detection logic
# # clear_cache()


# import pandas as pd
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.preprocessing import LabelEncoder
# from app.utils.llm_detect_source import llm_detect_source
# import hashlib
# import json

# CONFIDENCE_THRESHOLD = 0.75  # Increased to use LLM more often

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
    
#     # CBS-specific indicators
#     has_host_ref = int(any("host" in h and "ref" in h or "host_ref" in h or "hostref" in h for h in headers))
#     has_reviver_account = int(any("reviver" in h and "account" in h or "reviveraccount" in h for h in headers))
#     has_cbs_codes = 0
    
#     # Check for CBS transaction codes in data
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
    
#     # Distinguish between CBS and Mobile Money
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
#         # CBS-specific features
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
# # Training patterns based on typical file structures
# training_data = pd.DataFrame([
#     # ATM operational files (customer-facing) - has terminal, RRN, auth, transaction type
#     # column_count, has_rrn, has_terminal, has_merchant, has_pan, has_auth, has_balance, has_debit_credit, 
#     # has_mti, has_direction, has_processing_code, has_posted, has_account, has_atm_indicators, has_transaction_type,
#     # has_host_ref, has_reviver_account, has_cbs_codes, has_mobile_number, has_service_name, has_payment_mode, has_narration, label
#     [14, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, "ATM"],
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
    
#     # CBS files (Core Banking System)
#     [8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],
#     [9, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],
#     [10, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, "CBS"],
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 1, "CBS"],
#     [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, "CBS"],
    
#     # MOBILE_MONEY / E-Money platform files
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, "MOBILE_MONEY"],
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, "MOBILE_MONEY"],
#     [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, "MOBILE_MONEY"],
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


# def detect_channel(df, source):
#     """
#     Detect the channel (ATM, POS, MOBILE_MONEY, CARD, etc.) based on source and features.
#     For CBS and BANK files, this determines which channel the transactions belong to.
#     """
#     headers = [c.lower() for c in df.columns]
    
#     # Enhanced detection for BANK source files
#     if source == "BANK":
#         # Check for ATM structural patterns
#         has_stan = any("stan" in h for h in headers)
#         has_rrn = any("rrn" in h for h in headers)
#         has_fc_txn = any("fc_txn" in h or ("fc" in h and "txn" in h) for h in headers)
#         # Check for masked account (both "account_masked" and "masked_account" patterns)
#         has_masked_account = any(("account" in h and "mask" in h) or ("mask" in h and "account" in h) for h in headers)
#         has_dr_cr_columns = any(h == "dr" or h == "cr" or "dr_" in h or "cr_" in h for h in headers)
#         has_account_column = any("account" in h for h in headers)
        
#         # CRITICAL FIX: Prioritize STAN + RRN + Account structure for ATM
#         # This is the strongest indicator of ATM transactions
#         is_atm_structure = (
#             (has_stan and has_rrn and has_account_column) or  # STAN + RRN + Account = ATM
#             (has_stan and has_rrn and has_dr_cr_columns) or   # STAN + RRN + DR/CR = ATM
#             (has_fc_txn and has_stan and has_dr_cr_columns)   # FC_TXN + STAN + DR/CR = ATM
#         )
        
#         # Check for Mobile Money indicators
#         has_mobile_indicators = (
#             any("mobile" in h and "number" in h for h in headers) or
#             any("phone" in h for h in headers) or
#             any("service" in h and "name" in h for h in headers) or
#             any("payment" in h and "mode" in h for h in headers)
#         )
        
#         # Check for POS indicators
#         # CRITICAL FIX: "pos" check must be for whole word, not substring (to avoid matching "posted", "position", etc.)
#         has_pos_indicators = (
#             any("merchant" in h or "mid" in h for h in headers) or
#             any(h == "pos" or h.startswith("pos_") or h.endswith("_pos") for h in headers)
#         )
        
#         # Check for Internet/Online Banking indicators
#         has_internet_banking_indicators = (
#             any("internet" in h for h in headers) or
#             any("online" in h for h in headers) or
#             any("web" in h for h in headers)
#         )
        
#         # PRIORITY 1: Check for ATM structural pattern FIRST (before other checks)
#         # This is the most reliable indicator for your ATM files
#         if is_atm_structure and not has_mobile_indicators and not has_pos_indicators:
#             return "ATM"
        
#         # Analyze transaction descriptions/narrations for channel hints
#         description_cols = [col for col in df.columns if any(
#             keyword in col.lower() for keyword in ["description", "narration", "remarks", "message"]
#         )]
        
#         channel_hints = {
#             "ATM": 0,
#             "POS": 0,
#             "INTERNET_BANKING": 0,
#             "MOBILE_MONEY": 0
#         }
        
#         if description_cols:
#             for col in description_cols:
#                 sample_values = df[col].astype(str).head(20)
#                 for val in sample_values:
#                     val_upper = str(val).upper()
#                     # Skip empty/null values
#                     if val_upper in ["", "NAN", "NONE", "0"]:
#                         continue
#                     # ATM keywords
#                     if any(keyword in val_upper for keyword in ["ATM", "CASH WITHDRAWAL", "AUTOMATED TELLER", "WITHDRAWAL"]):
#                         channel_hints["ATM"] += 1
#                     # POS keywords
#                     if any(keyword in val_upper for keyword in ["POS", "PURCHASE", "MERCHANT", "CARD PAYMENT"]):
#                         channel_hints["POS"] += 1
#                     # Internet Banking keywords
#                     if any(keyword in val_upper for keyword in ["INTERNET", "ONLINE", "WEB", "E-BANKING", "TRANSFER", "NEFT", "RTGS", "IMPS"]):
#                         channel_hints["INTERNET_BANKING"] += 1
#                     # Mobile Money keywords
#                     if any(keyword in val_upper for keyword in ["MOBILE", "M-PESA", "AIRTEL", "MTN", "MOBILE MONEY"]):
#                         channel_hints["MOBILE_MONEY"] += 1
        
#         # PRIORITY 2: Check for explicit header indicators
#         has_atm_in_headers = any("atm" in h for h in headers)
#         if has_atm_in_headers or channel_hints["ATM"] > 0:
#             return "ATM"
#         elif has_mobile_indicators or channel_hints["MOBILE_MONEY"] > 0:
#             return "MOBILE_MONEY"
#         elif has_pos_indicators or channel_hints["POS"] > 0:
#             return "POS"
#         elif has_internet_banking_indicators or channel_hints["INTERNET_BANKING"] > 0:
#             return "INTERNET_BANKING"
        
#         # PRIORITY 3: Final fallback - use structure even without explicit indicators
#         if is_atm_structure:
#             return "ATM"
        
#         # PRIORITY 4: Check description hints
#         if channel_hints and max(channel_hints.values()) > 0:
#             max_channel = max(channel_hints, key=channel_hints.get)
#             return max_channel
        
#         return "BANK"  # Default to generic BANK if no clear channel
    
#     # Enhanced detection for CBS source files
#     elif source == "CBS":
#         # Check for Mobile Money indicators
#         has_mobile_indicators = (
#             any("mobile" in h and "number" in h for h in headers) or
#             any("phone" in h for h in headers)
#         )
        
#         # Check transaction patterns in Host_ref_number for Mobile Money codes
#         has_mm_codes = False
#         for col in df.columns:
#             if "host" in col.lower() and "ref" in col.lower():
#                 sample_values = df[col].astype(str).head(20)
#                 if any(any(code in str(val).upper() for code in ["MBCN", "MBCM"]) 
#                        for val in sample_values):
#                     has_mm_codes = True
#                     break
        
#         # Check for ATM indicators
#         has_atm_indicators = (
#             any("atm" in h for h in headers) or
#             any("terminal" in h and ("id" in h or "location" in h) for h in headers)
#         )
        
#         # Check transaction patterns for ATM codes
#         has_atm_codes = False
#         for col in df.columns:
#             if "host" in col.lower() and "ref" in col.lower():
#                 sample_values = df[col].astype(str).head(20)
#                 if any(any(code in str(val).upper() for code in ["UDCN", "CBCN", "IBCN"]) 
#                        for val in sample_values):
#                     has_atm_codes = True
#                     break
        
#         # Check for POS indicators
#         has_pos_indicators = any("merchant" in h or "mid" in h for h in headers)
        
#         # Decision logic for CBS channel detection
#         if has_mm_codes or (has_mobile_indicators and not has_atm_indicators):
#             return "MOBILE_MONEY"
#         elif has_atm_indicators or has_atm_codes:
#             return "ATM"
#         elif has_pos_indicators:
#             return "POS"
#         else:
#             return "CBS_GENERAL"
    
#     # For other sources, channel = source
#     elif source == "ATM":
#         return "ATM"
#     elif source == "POS":
#         return "POS"
#     elif source == "MOBILE_MONEY":
#         return "MOBILE_MONEY"
#     elif source == "CARD":
#         return "CARD"
#     elif source == "SWITCH":
#         return "SWITCH"
#     else:
#         return source


# def predict_source_with_fallback(df):
#     # Always run ML prediction first
#     ml_result = predict_source(df)
    
#     # Detect channel based on source and features
#     detected_channel = detect_channel(df, ml_result["source"])
    
#     print(f"ML Prediction - Source: {ml_result['source']}, Channel: {detected_channel}, Confidence: {ml_result['confidence']}")

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
#                     "channel": detected_channel,
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
#                 "recommended_channel": detected_channel,
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
#                     "channel": detected_channel,
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
#                 "recommended_channel": llm_result.get("channel", detected_channel),
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
#                 "channel": detected_channel,
#                 "confidence": ml_result["confidence"],
#                 "features": ml_result["features"]
#             },
#             "llm_prediction": None,
#             "decision": "ML",
#             "recommended_source": ml_result["source"],
#             "recommended_channel": detected_channel,
#             "agreement": True,
#             "confidence_threshold": CONFIDENCE_THRESHOLD,
#             "cached": False
#         }
    
#     return result


# import pandas as pd
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.preprocessing import LabelEncoder
# from app.utils.llm_detect_source import llm_detect_source
# import hashlib
# import json

# CONFIDENCE_THRESHOLD = 0.75  # Increased to use LLM more often

# # Simple in-memory cache for LLM results (keyed by column headers hash)
# _llm_cache = {}

# def get_headers_hash(headers):
#     """Create a hash of column headers for caching"""
#     headers_str = json.dumps(sorted(headers), sort_keys=True)
#     return hashlib.md5(headers_str.encode()).hexdigest()

# def detect_card_network(card_number):
#     """
#     Detect card network (VISA, MASTERCARD, AMEX, etc.) from card number.
#     Handles both full and masked card numbers.
#     Returns the network name or None if unable to detect.
#     """
#     # Convert to string and remove any spaces, dashes, or asterisks for prefix detection
#     card_str = str(card_number).replace(" ", "").replace("-", "").replace("*", "")
    
#     # Extract first available digits (BIN - Bank Identification Number)
#     # For masked cards like "453789******7041", we get "453789"
#     first_digits = ""
#     for char in card_str:
#         if char.isdigit():
#             first_digits += char
#         elif len(first_digits) >= 6:  # We have enough to determine network
#             break
    
#     if len(first_digits) < 1:
#         return None
    
#     # Card network detection rules based on BIN ranges
#     # VISA: starts with 4
#     if first_digits[0] == '4':
#         return "VISA"
    
#     # MASTERCARD: 51-55, 2221-2720
#     if len(first_digits) >= 2:
#         first_two = int(first_digits[:2])
#         if 51 <= first_two <= 55:
#             return "MASTERCARD"
#         if len(first_digits) >= 4:
#             first_four = int(first_digits[:4])
#             if 2221 <= first_four <= 2720:
#                 return "MASTERCARD"
    
#     # AMERICAN EXPRESS: 34, 37
#     if len(first_digits) >= 2:
#         first_two = int(first_digits[:2])
#         if first_two in [34, 37]:
#             return "AMEX"
    
#     # DISCOVER: 6011, 622126-622925, 644-649, 65
#     if len(first_digits) >= 2:
#         first_two = int(first_digits[:2])
#         if first_two == 65:
#             return "DISCOVER"
#         if len(first_digits) >= 3:
#             first_three = int(first_digits[:3])
#             if 644 <= first_three <= 649:
#                 return "DISCOVER"
#         if len(first_digits) >= 4:
#             first_four = int(first_digits[:4])
#             if first_four == 6011:
#                 return "DISCOVER"
#             if len(first_digits) >= 6:
#                 first_six = int(first_digits[:6])
#                 if 622126 <= first_six <= 622925:
#                     return "DISCOVER"
    
#     # DINERS CLUB: 36, 38, 300-305
#     if len(first_digits) >= 2:
#         first_two = int(first_digits[:2])
#         if first_two in [36, 38]:
#             return "DINERS"
#         if len(first_digits) >= 3:
#             first_three = int(first_digits[:3])
#             if 300 <= first_three <= 305:
#                 return "DINERS"
    
#     # JCB: 3528-3589
#     if len(first_digits) >= 4:
#         first_four = int(first_digits[:4])
#         if 3528 <= first_four <= 3589:
#             return "JCB"
    
#     # UNIONPAY: starts with 62
#     if len(first_digits) >= 2:
#         first_two = int(first_digits[:2])
#         if first_two == 62:
#             return "UNIONPAY"
    
#     # RUPAY (India): 60, 65, 81, 82, 508
#     if len(first_digits) >= 2:
#         first_two = int(first_digits[:2])
#         if first_two in [60, 81, 82]:
#             return "RUPAY"
#         if len(first_digits) >= 3:
#             first_three = int(first_digits[:3])
#             if first_three == 508:
#                 return "RUPAY"
    
#     return "UNKNOWN_CARD_NETWORK"

# def detect_card_network_from_df(df):
#     """
#     Detect the card network from card numbers in the dataframe.
#     Returns the most common network found, or None if no card numbers present.
#     """
#     headers = [c.lower() for c in df.columns]
    
#     # Find card number columns
#     card_cols = [col for col in df.columns if any(
#         keyword in col.lower() for keyword in ["card", "cardno", "card_no", "pan"]
#     )]
    
#     if not card_cols:
#         return None
    
#     # Collect network detections from sample of card numbers
#     networks = []
#     for col in card_cols:
#         # Sample up to 20 rows to detect network
#         sample_values = df[col].dropna().head(20)
#         for card_num in sample_values:
#             network = detect_card_network(card_num)
#             if network and network != "UNKNOWN_CARD_NETWORK":
#                 networks.append(network)
    
#     if not networks:
#         return None
    
#     # Return the most common network
#     from collections import Counter
#     network_counts = Counter(networks)
#     most_common_network = network_counts.most_common(1)[0][0]
    
#     return most_common_network

# def extract_features(df):
#     headers = [c.lower() for c in df.columns]

#     # CRITICAL: Detect masked card numbers (this indicates CARD file, not ATM file)
#     has_masked_card = int(any(("card" in h and "mask" in h) or ("mask" in h and "card" in h) for h in headers))
#     has_card_number = int(any("card" in h and ("number" in h or "no" in h or h.endswith("card")) for h in headers))
    
#     # FX/Currency conversion indicators (strong CARD file indicator)
#     has_fx_rate = int(any("fx" in h or "fxrate" in h or ("rate" in h and any(curr in h for curr in ["txn", "home", "foreign"])) for h in headers))
#     has_multiple_currency = int(any("txncurrency" in h or "homecurrency" in h or ("home" in h and "currency" in h) or ("txn" in h and "currency" in h) for h in headers))
    
#     # Channel column (indicates multi-channel card tracking)
#     has_channel_column = int(any(h == "channel" or h == "txn_channel" or h == "txnchannel" for h in headers))
    
#     # More specific ATM indicators
#     has_atm_indicators = int(any("atm" in h or "terminal" in h and ("id" in h or "location" in h) for h in headers))
#     has_transaction_type = int(any("transaction" in h and "type" in h or "transactiontype" in h or "txntype" in h for h in headers))
    
#     # PAN should only count for CARD if it's not masked and in ATM context
#     # If we have ATM indicators + masked PAN, it's ATM not CARD
#     has_pure_pan = int(any("pan" in h and "mask" not in h for h in headers))
    
#     # CBS-specific indicators
#     has_host_ref = int(any("host" in h and "ref" in h or "host_ref" in h or "hostref" in h for h in headers))
#     has_reviver_account = int(any("reviver" in h and "account" in h or "reviveraccount" in h for h in headers))
#     has_cbs_codes = 0
    
#     # Check for CBS transaction codes in data
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
    
#     # Distinguish between CBS and Mobile Money
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
#         # Detect debit/credit columns: full names OR abbreviations (DR/CR) OR combined DRCR
#         "has_debit_credit": int(any("debit" in h or "credit" in h or "drcr" in h or h == "dr" or h == "cr" or "dr_" in h or "cr_" in h for h in headers)),
#         "has_mti": int(any("mti" in h or "message" in h and "type" in h for h in headers)),
#         "has_direction": int(any("direction" in h or "inbound" in h or "outbound" in h for h in headers)),
#         "has_processing_code": int(any("processing" in h and "code" in h or "proc" in h and "code" in h for h in headers)),
#         # Additional CBS indicators
#         "has_posted": int(any("posted" in h or "status" in h for h in headers)),
#         "has_account": int(any("account" in h for h in headers)),
#         # Enhanced ATM detection
#         "has_atm_indicators": has_atm_indicators,
#         "has_transaction_type": has_transaction_type,
#         # CBS-specific features
#         "has_host_ref": has_host_ref,
#         "has_reviver_account": has_reviver_account,
#         "has_cbs_codes": has_cbs_codes,
#         # Mobile Money / E-Money indicators
#         "has_mobile_number": has_mobile_number,
#         "has_service_name": has_service_name,
#         "has_payment_mode": has_payment_mode,
#         "has_narration": has_narration,
#         # NEW CARD-specific features
#         "has_masked_card": has_masked_card,
#         "has_card_number": has_card_number,
#         "has_fx_rate": has_fx_rate,
#         "has_multiple_currency": has_multiple_currency,
#         "has_channel_column": has_channel_column,
#     }

# # ---- TRAIN MODEL ON STARTUP ----
# # Training patterns based on typical file structures
# training_data = pd.DataFrame([
#     # ATM operational files (customer-facing) - has terminal, RRN, auth, transaction type
#     # column_count, has_rrn, has_terminal, has_merchant, has_pan, has_auth, has_balance, has_debit_credit, 
#     # has_mti, has_direction, has_processing_code, has_posted, has_account, has_atm_indicators, has_transaction_type,
#     # has_host_ref, has_reviver_account, has_cbs_codes, has_mobile_number, has_service_name, has_payment_mode, has_narration,
#     # has_masked_card, has_card_number, has_fx_rate, has_multiple_currency, has_channel_column, label
#     [14, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
#     [10, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
#     [11, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
#     [12, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
    
#     # SWITCH files (raw switch messages) - ATM channel
#     [12, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [13, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [14, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    
#     # SWITCH files - POS channel (has merchant indicators)
#     [15, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
#     [16, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    
#     # POS transaction files (not switch) - merchant focused
#     [12, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
#     [13, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
#     [11, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    
#     # CARD network files - NEW PATTERNS with masked cards, FX, and channel column
#     [16, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, "CARD"],  # Your file pattern: masked card + FX + channel + DRCR
#     [17, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, "CARD"],  # With description/narration
#     [15, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, "CARD"],  # Without FX but with channel
#     [16, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1, "CARD"],  # Without FX rate but with currencies
#     [18, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, "CARD"],  # With transaction type
#     # Original CARD patterns (pure card data, NO ATM indicators, no masked pattern)
#     [20, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, "CARD"],
#     [18, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, "CARD"],
#     [15, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, "CARD"],
    
#     # BANK/CBS posted transactions (with DR/CR columns) - WITHOUT strong CARD indicators
#     [8, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [10, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [10, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [12, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     # BANK files with STAN+RRN+Account (ATM transactions in bank format) - NO CARD indicators
#     [16, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
#     [17, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    
#     # CBS files (Core Banking System)
#     [8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, "CBS"],
#     [9, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, "CBS"],
#     [10, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, "CBS"],
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, "CBS"],
#     [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, "CBS"],
    
#     # MOBILE_MONEY / E-Money platform files
#     [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
#     [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
#     [13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
#     [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
#     [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
# ], columns=[
#     "column_count", "has_rrn", "has_terminal", "has_merchant",
#     "has_pan", "has_auth", "has_balance", "has_debit_credit",
#     "has_mti", "has_direction", "has_processing_code",
#     "has_posted", "has_account", "has_atm_indicators", "has_transaction_type",
#     "has_host_ref", "has_reviver_account", "has_cbs_codes",
#     "has_mobile_number", "has_service_name", "has_payment_mode", "has_narration",
#     "has_masked_card", "has_card_number", "has_fx_rate", "has_multiple_currency", "has_channel_column",
#     "label"
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
#     predicted_source = encoder.inverse_transform([idx])[0]
    
#     # POST-PROCESSING: Strong CARD file indicators override other predictions
#     is_card_file = False
#     card_network = None
    
#     # Priority 1: Masked card + FX + Channel = definitely CARD
#     if (features["has_masked_card"] and features["has_fx_rate"] and features["has_channel_column"]):
#         if predicted_source != "CARD":
#             print(f"Overriding prediction from {predicted_source} to CARD (masked card + FX + channel)")
#         is_card_file = True
#     # Priority 2: Masked card + multiple currencies + channel = CARD
#     elif (features["has_masked_card"] and features["has_multiple_currency"] and features["has_channel_column"]):
#         if predicted_source != "CARD":
#             print(f"Overriding prediction from {predicted_source} to CARD (masked card + currencies + channel)")
#         is_card_file = True
#     # Priority 3: Card number + FX + channel = CARD
#     elif (features["has_card_number"] and features["has_fx_rate"] and features["has_channel_column"]):
#         if predicted_source != "CARD":
#             print(f"Overriding prediction from {predicted_source} to CARD (card number + FX + channel)")
#         is_card_file = True
#     # Priority 4: Masked card + FX (even without explicit channel column) = likely CARD
#     elif (features["has_masked_card"] and features["has_fx_rate"]):
#         if predicted_source != "CARD":
#             print(f"Overriding prediction from {predicted_source} to CARD (masked card + FX)")
#         is_card_file = True
#     elif predicted_source == "CARD":
#         is_card_file = True
    
#     # If this is a CARD file, detect the actual card network (VISA, MASTERCARD, etc.)
#     # but keep the source as "CARD" for database compatibility
#     if is_card_file:
#         card_network = detect_card_network_from_df(df)
#         if card_network:
#             print(f"Detected card network: {card_network}")
#         else:
#             print("Could not detect specific card network")
#         predicted_source = "CARD"  # Always return CARD as source for database compatibility

#     return {
#         "source": predicted_source,
#         "confidence": round(float(probs[idx]), 2),
#         "features": features,
#         "card_network": card_network  # Add card network as metadata
#     }


# def detect_channel(df, source):
#     """
#     Detect the channel (ATM, POS, MOBILE_MONEY, CARD, etc.) based on source and features.
#     For CBS and BANK files, this determines which channel the transactions belong to.
#     For CARD source files, the channel is always 'CARD'.
#     """
#     headers = [c.lower() for c in df.columns]
    
#     # For CARD source, always return 'CARD' as the channel
#     # The Channel column in the data (ATM, POS, etc.) indicates transaction location,
#     # but the system channel for card files is 'CARD'
#     if source == "CARD":
#         return "CARD"
    
#     # Enhanced detection for BANK source files
#     if source == "BANK":
#         # Check for ATM structural patterns
#         has_stan = any("stan" in h for h in headers)
#         has_rrn = any("rrn" in h for h in headers)
#         has_fc_txn = any("fc_txn" in h or ("fc" in h and "txn" in h) for h in headers)
#         # Check for masked account (both "account_masked" and "masked_account" patterns)
#         has_masked_account = any(("account" in h and "mask" in h) or ("mask" in h and "account" in h) for h in headers)
#         has_dr_cr_columns = any(h == "dr" or h == "cr" or "dr_" in h or "cr_" in h or "drcr" in h for h in headers)
#         has_account_column = any("account" in h for h in headers)
        
#         # CRITICAL FIX: Prioritize STAN + RRN + Account structure for ATM
#         # This is the strongest indicator of ATM transactions
#         is_atm_structure = (
#             (has_stan and has_rrn and has_account_column) or  # STAN + RRN + Account = ATM
#             (has_stan and has_rrn and has_dr_cr_columns) or   # STAN + RRN + DR/CR = ATM
#             (has_fc_txn and has_stan and has_dr_cr_columns)   # FC_TXN + STAN + DR/CR = ATM
#         )
        
#         # Check for Mobile Money indicators
#         has_mobile_indicators = (
#             any("mobile" in h and "number" in h for h in headers) or
#             any("phone" in h for h in headers) or
#             any("service" in h and "name" in h for h in headers) or
#             any("payment" in h and "mode" in h for h in headers)
#         )
        
#         # Check for POS indicators
#         # CRITICAL FIX: "pos" check must be for whole word, not substring (to avoid matching "posted", "position", etc.)
#         has_pos_indicators = (
#             any("merchant" in h or "mid" in h for h in headers) or
#             any(h == "pos" or h.startswith("pos_") or h.endswith("_pos") for h in headers)
#         )
        
#         # Check for Internet/Online Banking indicators
#         has_internet_banking_indicators = (
#             any("internet" in h for h in headers) or
#             any("online" in h for h in headers) or
#             any("web" in h for h in headers)
#         )
        
#         # PRIORITY 1: Check for ATM structural pattern FIRST (before other checks)
#         # This is the most reliable indicator for your ATM files
#         if is_atm_structure and not has_mobile_indicators and not has_pos_indicators:
#             return "ATM"
        
#         # Analyze transaction descriptions/narrations for channel hints
#         description_cols = [col for col in df.columns if any(
#             keyword in col.lower() for keyword in ["description", "narration", "remarks", "message"]
#         )]
        
#         channel_hints = {
#             "ATM": 0,
#             "POS": 0,
#             "INTERNET_BANKING": 0,
#             "MOBILE_MONEY": 0
#         }
        
#         if description_cols:
#             for col in description_cols:
#                 sample_values = df[col].astype(str).head(20)
#                 for val in sample_values:
#                     val_upper = str(val).upper()
#                     # Skip empty/null values
#                     if val_upper in ["", "NAN", "NONE", "0"]:
#                         continue
#                     # ATM keywords
#                     if any(keyword in val_upper for keyword in ["ATM", "CASH WITHDRAWAL", "AUTOMATED TELLER", "WITHDRAWAL"]):
#                         channel_hints["ATM"] += 1
#                     # POS keywords
#                     if any(keyword in val_upper for keyword in ["POS", "PURCHASE", "MERCHANT", "CARD PAYMENT"]):
#                         channel_hints["POS"] += 1
#                     # Internet Banking keywords
#                     if any(keyword in val_upper for keyword in ["INTERNET", "ONLINE", "WEB", "E-BANKING", "TRANSFER", "NEFT", "RTGS", "IMPS"]):
#                         channel_hints["INTERNET_BANKING"] += 1
#                     # Mobile Money keywords
#                     if any(keyword in val_upper for keyword in ["MOBILE", "M-PESA", "AIRTEL", "MTN", "MOBILE MONEY"]):
#                         channel_hints["MOBILE_MONEY"] += 1
        
#         # PRIORITY 2: Check for explicit header indicators
#         has_atm_in_headers = any("atm" in h for h in headers)
#         if has_atm_in_headers or channel_hints["ATM"] > 0:
#             return "ATM"
#         elif has_mobile_indicators or channel_hints["MOBILE_MONEY"] > 0:
#             return "MOBILE_MONEY"
#         elif has_pos_indicators or channel_hints["POS"] > 0:
#             return "POS"
#         elif has_internet_banking_indicators or channel_hints["INTERNET_BANKING"] > 0:
#             return "INTERNET_BANKING"
        
#         # PRIORITY 3: Final fallback - use structure even without explicit indicators
#         if is_atm_structure:
#             return "ATM"
        
#         # PRIORITY 4: Check description hints
#         if channel_hints and max(channel_hints.values()) > 0:
#             max_channel = max(channel_hints, key=channel_hints.get)
#             return max_channel
        
#         return "BANK"  # Default to generic BANK if no clear channel
    
#     # Enhanced detection for CBS source files
#     elif source == "CBS":
#         # Check for Mobile Money indicators
#         has_mobile_indicators = (
#             any("mobile" in h and "number" in h for h in headers) or
#             any("phone" in h for h in headers)
#         )
        
#         # Check transaction patterns in Host_ref_number for Mobile Money codes
#         has_mm_codes = False
#         for col in df.columns:
#             if "host" in col.lower() and "ref" in col.lower():
#                 sample_values = df[col].astype(str).head(20)
#                 if any(any(code in str(val).upper() for code in ["MBCN", "MBCM"]) 
#                        for val in sample_values):
#                     has_mm_codes = True
#                     break
        
#         # --- Enhanced logic for CBS: Use FC_TXN_ID and masked account_no to distinguish ATM vs Cards ---
#         has_fc_txn_id = any(h.replace("_", "").lower() == "fctxnid" for h in headers) or any(h.lower() == "fc_txn_id" for h in headers)

#         # Card-like columns (PAN, card_number, expiry, etc.)
#         card_like_columns = [
#             "pan", "card_number", "cardnumber", "expiry", "expiry_date", "card_expiry", "card_type"
#         ]
#         has_card_column = any(any(card_col in h.replace("_", "").lower() for card_col in card_like_columns) for h in headers)

#         # Masked account_no detection (e.g., 0012******5346)
#         masked_account_col = None
#         for h in headers:
#             if h in ["account_no", "accountnumber", "account"]:
#                 masked_account_col = h
#                 break
#         has_masked_account = False
#         if masked_account_col:
#             sample_values = df[masked_account_col].astype(str).head(10)
#             has_masked_account = any("*" in v for v in sample_values)

#         # Check for ATM indicators (as before)
#         has_atm_indicators = (
#             any("atm" in h for h in headers) or
#             any("terminal" in h and ("id" in h or "location" in h) for h in headers)
#         )
#         # Check transaction patterns for ATM codes (as before)
#         has_atm_codes = False
#         for col in df.columns:
#             if "host" in col.lower() and "ref" in col.lower():
#                 sample_values = df[col].astype(str).head(20)
#                 if any(any(code in str(val).upper() for code in ["UDCN", "CBCN", "IBCN"]) 
#                        for val in sample_values):
#                     has_atm_codes = True
#                     break
#         # Check for POS indicators (as before)
#         has_pos_indicators = any("merchant" in h or "mid" in h for h in headers)

#         # --- Decision logic for CBS channel detection ---
#         if has_mm_codes or (has_mobile_indicators and not has_atm_indicators):
#             return "MOBILE_MONEY"
#         elif has_fc_txn_id:
#             return "ATM"
#         elif has_card_column or has_masked_account:
#             return "CARDS"
#         elif has_atm_indicators or has_atm_codes:
#             return "ATM"
#         elif has_pos_indicators:
#             return "POS"
#         else:
#             return "CBS_GENERAL"
    
#     # For other sources, channel = source
#     elif source == "ATM":
#         return "ATM"
#     elif source == "POS":
#         return "POS"
#     elif source == "MOBILE_MONEY":
#         return "MOBILE_MONEY"
#     elif source == "SWITCH":
#         return "SWITCH"
#     else:
#         return source


# def predict_source_with_fallback(df):
#     # Always run ML prediction first
#     ml_result = predict_source(df)
    
#     # Detect channel based on source and features
#     detected_channel = detect_channel(df, ml_result["source"])
    
#     print(f"ML Prediction - Source: {ml_result['source']}, Channel: {detected_channel}, Confidence: {ml_result['confidence']}")
#     if ml_result.get("card_network"):
#         print(f"Card Network Detected: {ml_result['card_network']}")

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
#                     "channel": detected_channel,
#                     "confidence": ml_result["confidence"],
#                     "features": ml_result["features"],
#                     "card_network": ml_result.get("card_network")
#                 },
#                 "llm_prediction": {
#                     "source": llm_result.get("source", "UNKNOWN"),
#                     "channel": llm_result.get("channel", "UNKNOWN"),
#                     "reason": llm_result.get("reason", "No reason provided"),
#                     "raw": llm_result
#                 },
#                 "decision": "ML_FALLBACK",  # Indicate this was a fallback
#                 "recommended_source": ml_result["source"],  # Use ML prediction
#                 "recommended_channel": detected_channel,
#                 "card_network": ml_result.get("card_network"),
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
#                     "channel": detected_channel,
#                     "confidence": ml_result["confidence"],
#                     "features": ml_result["features"],
#                     "card_network": ml_result.get("card_network")
#                 },
#                 "llm_prediction": {
#                     "source": llm_result.get("source", "UNKNOWN"),
#                     "channel": llm_result.get("channel", "UNKNOWN"),
#                     "reason": llm_result.get("reason", "No reason provided"),
#                     "raw": llm_result
#                 },
#                 "decision": "LLM",
#                 "recommended_source": llm_result.get("source", "UNKNOWN"),
#                 "recommended_channel": llm_result.get("channel", detected_channel),
#                 "card_network": ml_result.get("card_network"),
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
#                 "channel": detected_channel,
#                 "confidence": ml_result["confidence"],
#                 "features": ml_result["features"],
#                 "card_network": ml_result.get("card_network")
#             },
#             "llm_prediction": None,
#             "decision": "ML",
#             "recommended_source": ml_result["source"],
#             "recommended_channel": detected_channel,
#             "card_network": ml_result.get("card_network"),
#             "agreement": True,
#             "confidence_threshold": CONFIDENCE_THRESHOLD,
#             "cached": False
#         }
    
#     return result

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from app.utils.llm_detect_source import llm_detect_source
import hashlib
import json

CONFIDENCE_THRESHOLD = 0.75  # Increased to use LLM more often

# Simple in-memory cache for LLM results (keyed by column headers hash)
_llm_cache = {}

def get_headers_hash(headers):
    """Create a hash of column headers for caching"""
    headers_str = json.dumps(sorted(headers), sort_keys=True)
    return hashlib.md5(headers_str.encode()).hexdigest()

def detect_card_network(card_number):
    """
    Detect card network (VISA, MASTERCARD, AMEX, etc.) from card number.
    Handles both full and masked card numbers.
    Returns the network name or None if unable to detect.
    """
    # Convert to string and remove any spaces, dashes, or asterisks for prefix detection
    card_str = str(card_number).replace(" ", "").replace("-", "").replace("*", "")
    
    # Extract first available digits (BIN - Bank Identification Number)
    # For masked cards like "453789******7041", we get "453789"
    first_digits = ""
    for char in card_str:
        if char.isdigit():
            first_digits += char
        elif len(first_digits) >= 6:  # We have enough to determine network
            break
    
    if len(first_digits) < 1:
        return None
    
    # Card network detection rules based on BIN ranges
    # VISA: starts with 4
    if first_digits[0] == '4':
        return "VISA"
    
    # MASTERCARD: 51-55, 2221-2720
    if len(first_digits) >= 2:
        first_two = int(first_digits[:2])
        if 51 <= first_two <= 55:
            return "MASTERCARD"
        if len(first_digits) >= 4:
            first_four = int(first_digits[:4])
            if 2221 <= first_four <= 2720:
                return "MASTERCARD"
    
    # AMERICAN EXPRESS: 34, 37
    if len(first_digits) >= 2:
        first_two = int(first_digits[:2])
        if first_two in [34, 37]:
            return "AMEX"
    
    # DISCOVER: 6011, 622126-622925, 644-649, 65
    if len(first_digits) >= 2:
        first_two = int(first_digits[:2])
        if first_two == 65:
            return "DISCOVER"
        if len(first_digits) >= 3:
            first_three = int(first_digits[:3])
            if 644 <= first_three <= 649:
                return "DISCOVER"
        if len(first_digits) >= 4:
            first_four = int(first_digits[:4])
            if first_four == 6011:
                return "DISCOVER"
            if len(first_digits) >= 6:
                first_six = int(first_digits[:6])
                if 622126 <= first_six <= 622925:
                    return "DISCOVER"
    
    # DINERS CLUB: 36, 38, 300-305
    if len(first_digits) >= 2:
        first_two = int(first_digits[:2])
        if first_two in [36, 38]:
            return "DINERS"
        if len(first_digits) >= 3:
            first_three = int(first_digits[:3])
            if 300 <= first_three <= 305:
                return "DINERS"
    
    # JCB: 3528-3589
    if len(first_digits) >= 4:
        first_four = int(first_digits[:4])
        if 3528 <= first_four <= 3589:
            return "JCB"
    
    # UNIONPAY: starts with 62
    if len(first_digits) >= 2:
        first_two = int(first_digits[:2])
        if first_two == 62:
            return "UNIONPAY"
    
    # RUPAY (India): 60, 65, 81, 82, 508
    if len(first_digits) >= 2:
        first_two = int(first_digits[:2])
        if first_two in [60, 81, 82]:
            return "RUPAY"
        if len(first_digits) >= 3:
            first_three = int(first_digits[:3])
            if first_three == 508:
                return "RUPAY"
    
    return "UNKNOWN_CARD_NETWORK"

def detect_card_network_from_df(df):
    """
    Detect the card network from card numbers in the dataframe.
    Returns the most common network found, or None if no card numbers present.
    """
    headers = [c.lower() for c in df.columns]
    
    # Find card number columns
    card_cols = [col for col in df.columns if any(
        keyword in col.lower() for keyword in ["card", "cardno", "card_no", "pan"]
    )]
    
    if not card_cols:
        return None
    
    # Collect network detections from sample of card numbers
    networks = []
    for col in card_cols:
        # Sample up to 20 rows to detect network
        sample_values = df[col].dropna().head(20)
        for card_num in sample_values:
            network = detect_card_network(card_num)
            if network and network != "UNKNOWN_CARD_NETWORK":
                networks.append(network)
    
    if not networks:
        return None
    
    # Return the most common network
    from collections import Counter
    network_counts = Counter(networks)
    most_common_network = network_counts.most_common(1)[0][0]
    
    return most_common_network

def extract_features(df):
    headers = [c.lower() for c in df.columns]

    # CRITICAL: Detect masked card numbers (this indicates CARD file, not ATM file)
    has_masked_card = int(any(("card" in h and "mask" in h) or ("mask" in h and "card" in h) for h in headers))
    has_card_number = int(any("card" in h and ("number" in h or "no" in h or h.endswith("card")) for h in headers))
    
    # FX/Currency conversion indicators (strong CARD file indicator)
    has_fx_rate = int(any("fx" in h or "fxrate" in h or ("rate" in h and any(curr in h for curr in ["txn", "home", "foreign"])) for h in headers))
    has_multiple_currency = int(any("txncurrency" in h or "homecurrency" in h or ("home" in h and "currency" in h) or ("txn" in h and "currency" in h) for h in headers))
    
    # Channel column (indicates multi-channel card tracking)
    has_channel_column = int(any(h == "channel" or h == "txn_channel" or h == "txnchannel" for h in headers))
    
    # More specific ATM indicators
    has_atm_indicators = int(any("atm" in h or "terminal" in h and ("id" in h or "location" in h) for h in headers))
    has_transaction_type = int(any("transaction" in h and "type" in h or "transactiontype" in h or "txntype" in h for h in headers))
    
    # PAN should only count for CARD if it's not masked and in ATM context
    # If we have ATM indicators + masked PAN, it's ATM not CARD
    has_pure_pan = int(any("pan" in h and "mask" not in h for h in headers))
    
    # CBS-specific indicators
    has_host_ref = int(any("host" in h and "ref" in h or "host_ref" in h or "hostref" in h for h in headers))
    has_reviver_account = int(any("reviver" in h and "account" in h or "reviveraccount" in h for h in headers))
    has_cbs_codes = 0
    
    # Check for CBS transaction codes in data
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
    
    # Distinguish between CBS and Mobile Money
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
        # Detect debit/credit columns: full names OR abbreviations (DR/CR) OR combined DRCR
        "has_debit_credit": int(any("debit" in h or "credit" in h or "drcr" in h or h == "dr" or h == "cr" or "dr_" in h or "cr_" in h for h in headers)),
        "has_mti": int(any("mti" in h or "message" in h and "type" in h for h in headers)),
        "has_direction": int(any("direction" in h or "inbound" in h or "outbound" in h for h in headers)),
        "has_processing_code": int(any("processing" in h and "code" in h or "proc" in h and "code" in h for h in headers)),
        # Additional CBS indicators
        "has_posted": int(any("posted" in h or "status" in h for h in headers)),
        "has_account": int(any("account" in h for h in headers)),
        # Enhanced ATM detection
        "has_atm_indicators": has_atm_indicators,
        "has_transaction_type": has_transaction_type,
        # CBS-specific features
        "has_host_ref": has_host_ref,
        "has_reviver_account": has_reviver_account,
        "has_cbs_codes": has_cbs_codes,
        # Mobile Money / E-Money indicators
        "has_mobile_number": has_mobile_number,
        "has_service_name": has_service_name,
        "has_payment_mode": has_payment_mode,
        "has_narration": has_narration,
        # NEW CARD-specific features
        "has_masked_card": has_masked_card,
        "has_card_number": has_card_number,
        "has_fx_rate": has_fx_rate,
        "has_multiple_currency": has_multiple_currency,
        "has_channel_column": has_channel_column,
    }

# ---- TRAIN MODEL ON STARTUP ----
# Training patterns based on typical file structures
training_data = pd.DataFrame([
    # ATM operational files (customer-facing) - has terminal, RRN, auth, transaction type
    # column_count, has_rrn, has_terminal, has_merchant, has_pan, has_auth, has_balance, has_debit_credit, 
    # has_mti, has_direction, has_processing_code, has_posted, has_account, has_atm_indicators, has_transaction_type,
    # has_host_ref, has_reviver_account, has_cbs_codes, has_mobile_number, has_service_name, has_payment_mode, has_narration,
    # has_masked_card, has_card_number, has_fx_rate, has_multiple_currency, has_channel_column, label
    [14, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
    [10, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
    [11, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
    [12, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "ATM"],
    
    # SWITCH files (raw switch messages) - ATM channel
    [12, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    [13, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    [14, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    
    # SWITCH files - POS channel (has merchant indicators)
    [15, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    [16, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "SWITCH"],
    
    # POS transaction files (not switch) - merchant focused
    [12, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    [13, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    [11, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "POS"],
    
    # CARD network files - NEW PATTERNS with masked cards, FX, and channel column
    [16, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, "CARD"],  # Your file pattern: masked card + FX + channel + DRCR
    [17, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, "CARD"],  # With description/narration
    [15, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, "CARD"],  # Without FX but with channel
    [16, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1, "CARD"],  # Without FX rate but with currencies
    [18, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, "CARD"],  # With transaction type
    # Original CARD patterns (pure card data, NO ATM indicators, no masked pattern)
    [20, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, "CARD"],
    [18, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, "CARD"],
    [15, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, "CARD"],
    
    # BANK/CBS posted transactions (with DR/CR columns) - WITHOUT strong CARD indicators
    [8, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    [10, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    [10, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    [12, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    # BANK files with STAN+RRN+Account (ATM transactions in bank format) - NO CARD indicators
    [16, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    [17, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "BANK"],
    
    # CBS files (Core Banking System)
    [8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, "CBS"],
    [9, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, "CBS"],
    [10, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, "CBS"],
    [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, "CBS"],
    [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, "CBS"],
    # CBS Card transactions (masked account + card_number + DR/CR + RRN + STAN)
    [12, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, "CBS"],
    [11, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, "CBS"],
    [13, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, "CBS"],
    
    # MOBILE_MONEY / E-Money platform files
    [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
    [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
    [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
    [13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
    [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
    [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, "MOBILE_MONEY"],
], columns=[
    "column_count", "has_rrn", "has_terminal", "has_merchant",
    "has_pan", "has_auth", "has_balance", "has_debit_credit",
    "has_mti", "has_direction", "has_processing_code",
    "has_posted", "has_account", "has_atm_indicators", "has_transaction_type",
    "has_host_ref", "has_reviver_account", "has_cbs_codes",
    "has_mobile_number", "has_service_name", "has_payment_mode", "has_narration",
    "has_masked_card", "has_card_number", "has_fx_rate", "has_multiple_currency", "has_channel_column",
    "label"
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
    predicted_source = encoder.inverse_transform([idx])[0]
    
    # POST-PROCESSING: Strong CARD file indicators override other predictions
    # BUT DO NOT override CBS predictions - CBS can have card transactions
    is_card_file = False
    card_network = None
    
    # Only apply CARD overrides if ML did NOT predict CBS
    if predicted_source != "CBS":
        # Priority 1: Masked card + FX + Channel = definitely CARD
        if (features["has_masked_card"] and features["has_fx_rate"] and features["has_channel_column"]):
            if predicted_source != "CARD":
                print(f"Overriding prediction from {predicted_source} to CARD (masked card + FX + channel)")
            is_card_file = True
        # Priority 2: Masked card + multiple currencies + channel = CARD
        elif (features["has_masked_card"] and features["has_multiple_currency"] and features["has_channel_column"]):
            if predicted_source != "CARD":
                print(f"Overriding prediction from {predicted_source} to CARD (masked card + currencies + channel)")
            is_card_file = True
        # Priority 3: Card number + FX + channel = CARD
        elif (features["has_card_number"] and features["has_fx_rate"] and features["has_channel_column"]):
            if predicted_source != "CARD":
                print(f"Overriding prediction from {predicted_source} to CARD (card number + FX + channel)")
            is_card_file = True
        # Priority 4: Masked card + FX (even without explicit channel column) = likely CARD
        elif (features["has_masked_card"] and features["has_fx_rate"]):
            if predicted_source != "CARD":
                print(f"Overriding prediction from {predicted_source} to CARD (masked card + FX)")
            is_card_file = True
        elif predicted_source == "CARD":
            is_card_file = True
    
    # Also detect card network for CBS files that have card transactions
    if predicted_source == "CBS" and (features["has_masked_card"] or features["has_card_number"]):
        card_network = detect_card_network_from_df(df)
        if card_network:
            print(f"CBS file with card transactions - Detected card network: {card_network}")
    
    # If this is a CARD file, detect the actual card network (VISA, MASTERCARD, etc.)
    # but keep the source as "CARD" for database compatibility
    if is_card_file:
        card_network = detect_card_network_from_df(df)
        if card_network:
            print(f"Detected card network: {card_network}")
        else:
            print("Could not detect specific card network")
        predicted_source = "CARD"  # Always return CARD as source for database compatibility

    return {
        "source": predicted_source,
        "confidence": round(float(probs[idx]), 2),
        "features": features,
        "card_network": card_network  # Add card network as metadata
    }


def detect_channel(df, source):
    """
    Detect the channel (ATM, POS, MOBILE_MONEY, CARD, etc.) based on source and features.
    For CBS and BANK files, this determines which channel the transactions belong to.
    For CARD source files, the channel is always 'CARD'.
    """
    headers = [c.lower() for c in df.columns]
    
    # For CARD source, always return 'CARD' as the channel
    # The Channel column in the data (ATM, POS, etc.) indicates transaction location,
    # but the system channel for card files is 'CARD'
    if source == "CARD":
        return "CARD"
    
    # Enhanced detection for BANK source files
    if source == "BANK":
        # Check for ATM structural patterns
        has_stan = any("stan" in h for h in headers)
        has_rrn = any("rrn" in h for h in headers)
        has_fc_txn = any("fc_txn" in h or ("fc" in h and "txn" in h) for h in headers)
        # Check for masked account (both "account_masked" and "masked_account" patterns)
        has_masked_account_header = any(("account" in h and "mask" in h) or ("mask" in h and "account" in h) for h in headers)
        has_dr_cr_columns = any(h == "dr" or h == "cr" or "dr_" in h or "cr_" in h or "drcr" in h for h in headers)
        has_account_column = any("account" in h for h in headers)
        
        # Check for masked account values (e.g., 0012******5346)
        # This is a strong indicator of CARD transactions
        masked_account_col = None
        for h in headers:
            if h in ["account_no", "accountnumber", "account", "account_number"]:
                masked_account_col = h
                break
        has_masked_account_value = False
        if masked_account_col:
            sample_values = df[masked_account_col].astype(str).head(10)
            has_masked_account_value = any("*" in v for v in sample_values)
        
        # Combine both types of masked account checks
        has_masked_account = has_masked_account_header or has_masked_account_value
        
        # 🔥 PRIORITY 0: Check for masked account + STAN/RRN = CARDS (not ATM)
        # If account is masked with asterisks, it's card data even if it has ATM in narration
        if has_masked_account_value and has_stan and has_rrn:
            return "CARDS"
        
        # CRITICAL FIX: Prioritize STAN + RRN + Account structure for ATM
        # This is the strongest indicator of ATM transactions
        is_atm_structure = (
            (has_stan and has_rrn and has_account_column and not has_masked_account) or  # STAN + RRN + Account = ATM (but NOT masked)
            (has_stan and has_rrn and has_dr_cr_columns and not has_masked_account) or   # STAN + RRN + DR/CR = ATM (but NOT masked)
            (has_fc_txn and has_stan and has_dr_cr_columns)   # FC_TXN + STAN + DR/CR = ATM
        )
        
        # Check for Mobile Money indicators
        has_mobile_indicators = (
            any("mobile" in h and "number" in h for h in headers) or
            any("phone" in h for h in headers) or
            any("service" in h and "name" in h for h in headers) or
            any("payment" in h and "mode" in h for h in headers)
        )
        
        # Check for POS indicators
        # CRITICAL FIX: "pos" check must be for whole word, not substring (to avoid matching "posted", "position", etc.)
        has_pos_indicators = (
            any("merchant" in h or "mid" in h for h in headers) or
            any(h == "pos" or h.startswith("pos_") or h.endswith("_pos") for h in headers)
        )
        
        # Check for Internet/Online Banking indicators
        has_internet_banking_indicators = (
            any("internet" in h for h in headers) or
            any("online" in h for h in headers) or
            any("web" in h for h in headers)
        )
        
        # PRIORITY 1: Check for ATM structural pattern FIRST (before other checks)
        # This is the most reliable indicator for your ATM files
        if is_atm_structure and not has_mobile_indicators and not has_pos_indicators:
            return "ATM"
        
        # Analyze transaction descriptions/narrations for channel hints
        description_cols = [col for col in df.columns if any(
            keyword in col.lower() for keyword in ["description", "narration", "remarks", "message"]
        )]
        
        channel_hints = {
            "ATM": 0,
            "POS": 0,
            "INTERNET_BANKING": 0,
            "MOBILE_MONEY": 0
        }
        
        if description_cols:
            for col in description_cols:
                sample_values = df[col].astype(str).head(20)
                for val in sample_values:
                    val_upper = str(val).upper()
                    # Skip empty/null values
                    if val_upper in ["", "NAN", "NONE", "0"]:
                        continue
                    # ATM keywords
                    if any(keyword in val_upper for keyword in ["ATM", "CASH WITHDRAWAL", "AUTOMATED TELLER", "WITHDRAWAL"]):
                        channel_hints["ATM"] += 1
                    # POS keywords
                    if any(keyword in val_upper for keyword in ["POS", "PURCHASE", "MERCHANT", "CARD PAYMENT"]):
                        channel_hints["POS"] += 1
                    # Internet Banking keywords
                    if any(keyword in val_upper for keyword in ["INTERNET", "ONLINE", "WEB", "E-BANKING", "TRANSFER", "NEFT", "RTGS", "IMPS"]):
                        channel_hints["INTERNET_BANKING"] += 1
                    # Mobile Money keywords
                    if any(keyword in val_upper for keyword in ["MOBILE", "M-PESA", "AIRTEL", "MTN", "MOBILE MONEY"]):
                        channel_hints["MOBILE_MONEY"] += 1
        
        # PRIORITY 2: Check for explicit header indicators
        has_atm_in_headers = any("atm" in h for h in headers)
        if has_atm_in_headers or channel_hints["ATM"] > 0:
            return "ATM"
        elif has_mobile_indicators or channel_hints["MOBILE_MONEY"] > 0:
            return "MOBILE_MONEY"
        elif has_pos_indicators or channel_hints["POS"] > 0:
            return "POS"
        elif has_internet_banking_indicators or channel_hints["INTERNET_BANKING"] > 0:
            return "INTERNET_BANKING"
        
        # PRIORITY 3: Final fallback - use structure even without explicit indicators
        if is_atm_structure:
            return "ATM"
        
        # PRIORITY 4: Check description hints
        if channel_hints and max(channel_hints.values()) > 0:
            max_channel = max(channel_hints, key=channel_hints.get)
            return max_channel
        
        return "BANK"  # Default to generic BANK if no clear channel
    
    # Enhanced detection for CBS source files
    elif source == "CBS":
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
                if any(any(code in str(val).upper() for code in ["MBCN", "MBCM"]) 
                       for val in sample_values):
                    has_mm_codes = True
                    break
        
        # --- Enhanced logic for CBS: Use FC_TXN_ID and masked account_no to distinguish ATM vs Cards ---
        has_fc_txn_id = any(h.replace("_", "").lower() == "fctxnid" for h in headers) or any(h.lower() == "fc_txn_id" for h in headers)

        # Card-like columns (PAN, card_number, expiry, etc.)
        card_like_columns = [
            "pan", "card_number", "cardnumber", "expiry", "expiry_date", "card_expiry", "card_type"
        ]
        has_card_column = any(any(card_col in h.replace("_", "").lower() for card_col in card_like_columns) for h in headers)

        # Masked account_no detection (e.g., 0012******5346)
        # This is a strong indicator of CARD transactions in CBS
        masked_account_col = None
        for h in headers:
            if h in ["account_no", "accountnumber", "account"]:
                masked_account_col = h
                break
        has_masked_account = False
        if masked_account_col:
            sample_values = df[masked_account_col].astype(str).head(10)
            has_masked_account = any("*" in v for v in sample_values)

        # Check for ATM indicators in headers (but not in narration)
        has_atm_header_indicators = (
            any("atm" in h for h in headers) or
            any("terminal" in h and ("id" in h or "location" in h) for h in headers)
        )
        
        # Check transaction patterns for ATM codes
        has_atm_codes = False
        for col in df.columns:
            if "host" in col.lower() and "ref" in col.lower():
                sample_values = df[col].astype(str).head(20)
                if any(any(code in str(val).upper() for code in ["UDCN", "CBCN", "IBCN"]) 
                       for val in sample_values):
                    has_atm_codes = True
                    break
        
        # Check for POS indicators
        has_pos_indicators = any("merchant" in h or "mid" in h for h in headers)

        # --- Decision logic for CBS channel detection ---
        # CRITICAL: Masked account_no is the PRIMARY indicator for CARDS in CBS
        # The narration field may say "ATM" but that describes the transaction type,
        # not the system channel. If accounts are masked, it's a CARD file.
        
        if has_mm_codes or (has_mobile_indicators and not has_masked_account):
            return "MOBILE_MONEY"
        elif has_masked_account or has_card_column:
            # Masked accounts = CARD transactions in CBS, regardless of narration
            return "CARDS"
        elif has_fc_txn_id and not has_masked_account:
            # FC_TXN_ID without masked accounts = ATM channel
            return "ATM"
        elif has_atm_header_indicators or has_atm_codes:
            return "ATM"
        elif has_pos_indicators:
            return "POS"
        else:
            return "CBS_GENERAL"
    
    # For other sources, channel = source
    elif source == "ATM":
        return "ATM"
    elif source == "POS":
        return "POS"
    elif source == "MOBILE_MONEY":
        return "MOBILE_MONEY"
    elif source == "SWITCH":
        return "SWITCH"
    else:
        return source


def predict_source_with_fallback(df):
    # Check if CSV has a 'source' column with explicit source values
    source_from_data = None
    source_column_names = ['source', 'Source', 'SOURCE', 'source_type', 'Source_Type']
    for col in source_column_names:
        if col in df.columns:
            # Get unique values from source column
            unique_sources = df[col].dropna().unique()
            if len(unique_sources) > 0:
                # Get most common source value
                source_value = df[col].mode().iloc[0] if not df[col].mode().empty else unique_sources[0]
                source_value_upper = str(source_value).upper().strip()
                
                # Check if it's a valid source type
                valid_sources = ['CBS', 'BANK', 'CARD', 'ATM', 'SWITCH', 'NETWORK', 'POS', 'MOBILE_MONEY', 'WALLET']
                if source_value_upper in valid_sources:
                    source_from_data = source_value_upper
                    print(f"✓ Found explicit source in data column '{col}': {source_from_data}")
                    break
    
    # Always run ML prediction first
    ml_result = predict_source(df)
    
    # Override ML prediction if we found source in data
    if source_from_data:
        print(f"⚠️  Overriding ML prediction ({ml_result['source']}) with source from data: {source_from_data}")
        ml_result["source"] = source_from_data
        ml_result["source_override"] = True
        ml_result["original_ml_prediction"] = ml_result.get("source")
    
    # Detect channel based on source and features
    detected_channel = detect_channel(df, ml_result["source"])
    
    print(f"ML Prediction - Source: {ml_result['source']}, Channel: {detected_channel}, Confidence: {ml_result['confidence']}")
    if ml_result.get("card_network"):
        print(f"Card Network Detected: {ml_result['card_network']}")

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
                    "features": ml_result["features"],
                    "card_network": ml_result.get("card_network")
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
                "card_network": ml_result.get("card_network"),
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
                    "features": ml_result["features"],
                    "card_network": ml_result.get("card_network")
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
                "card_network": ml_result.get("card_network"),
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
                "features": ml_result["features"],
                "card_network": ml_result.get("card_network")
            },
            "llm_prediction": None,
            "decision": "ML",
            "recommended_source": ml_result["source"],
            "recommended_channel": detected_channel,
            "card_network": ml_result.get("card_network"),
            "agreement": True,
            "confidence_threshold": CONFIDENCE_THRESHOLD,
            "cached": False
        }
    
    return result


