from openai import OpenAI
import json
import os
from dotenv import load_dotenv
import time

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def llm_detect_source(headers, sample_rows):
    prompt = f"""
            You are part of a banking reconciliation system.
            You must classify the uploaded file into EXACTLY ONE source type AND identify its channel.

                SOURCE TYPES:

                1. ATM_FILE
                - ATM transaction log with customer/operational details
                - **KEY INDICATORS**: Location field (city/branch names)
                - **KEY INDICATORS**: TransactionType (Withdrawal, Deposit, BalanceInquiry)
                - **KEY INDICATORS**: Account_masked or AccountNumber (customer accounts)
                - May ALSO contain switch reference fields (RRN, STAN, AUTH) for tracking
                - Terminal ID present
                - Customer-facing/operational format
                - Focus is on the ATM customer transaction, not the switch message

                2. SWITCH_FILE
                - RAW switch/authorization message log (ISO 8583 style)
                - **KEY INDICATORS**: MTI (Message Type Indicator like 100, 200, 210, 420, etc.)
                - **KEY INDICATORS**: Direction (INBOUND/OUTBOUND)
                - **KEY INDICATORS**: Source/Destination (ATM, ISSUER, ACQUIRER) - routing information
                - Contains RRN, STAN, ResponseCode, AuthID
                - NO Location or TransactionType fields
                - NO customer account details
                - Technical message-level data for switch routing
                - NOT posted to accounts (raw authorization data)

                3. CBS_BANK_FILE
                - Core banking / Flexcube POSTED transactions
                - Has "Posted", "POSTED", or posting status
                - Contains Debit/Credit OR running balance
                - Account number present
                - May ALSO contain RRN/STAN (switch reference) but the key is it's POSTED to accounts
                - Transaction ID formats like FC_TXN_ID, reference numbers

                4. CARD_NETWORK_FILE
                - PAN based settlement file
                - Network specific fields (interchange, scheme codes)

                5. MOBILE_MONEY_PLATFORM_FILE
                - E-Money/Mobile Money platform transaction file
                - **KEY INDICATORS**: Mobile Number field (Customer Mobile Number, Phone Number)
                - **KEY INDICATORS**: Service Name (NFS AIRTEL, NFS MTN, Mobile Money Service)
                - **KEY INDICATORS**: Payment Mode (ANDROID, USSD, Mobile, Mobile App, USSD Menu)
                - **KEY INDICATORS**: Payer Client or Provider name
                - **KEY INDICATORS**: Overall Status or Transaction Status
                - **KEY INDICATORS**: Receiver Narration or Description with payment details
                - Contains RRN (Retrieval Reference Number) in narration
                - May contain actionCode (0000 for success)
                - Transaction ID, Receipt Number present
                - Focus is on mobile money/e-wallet transactions
                - NOT a CBS file (this is from mobile money platform, not core banking)

                CHANNELS (Transaction Type):
                
                1. ATM - ATM withdrawals/deposits
                   - Indicators: TerminalID column, ATM fields, withdrawal/deposit transactions
                   - Processing codes: 01xxxx (withdrawal), 21xxxx (deposit), 31xxxx (balance inquiry)
                   - Description/Narration mentions: "Terminal", "ATM", "Withdrawal", "Cash", "Dispense"
                
                2. POS - Point of Sale/merchant transactions
                   - Indicators: MerchantID, MerchantName, merchant category codes
                   - Processing codes: 00xxxx (purchase), 20xxxx (refund)
                   - May have acquirer/merchant settlement data
                   - Description/Narration mentions: "Merchant", "POS", "Purchase", "Sale", "Payment"
                
                3. CARDS - Card network settlement/interchange
                   - Indicators: PAN (card number), interchange fees, scheme codes
                   - Settlement/clearing files from Visa/Mastercard
                   - May contain network-specific fields
                   - Description/Narration mentions: "Card", "Interchange", "Scheme", "Network"

                4. MOBILE_MONEY - Mobile Money/E-Wallet transactions
                   - Indicators: Mobile Number, Customer Phone Number
                   - Service Name: NFS AIRTEL, NFS MTN, Mobile Money Service
                   - Payment Mode: ANDROID, USSD, Mobile, Mobile App
                   - Provider/Client fields (IZYANE, Airtel Money, MTN Money, etc.)
                   - Description/Narration mentions: "Mobile", "USSD", "Wallet", "E-Money"

                CHANNEL DETECTION STRATEGY:
                1. First check column headers for TerminalID, MerchantID, PAN-related fields, or Mobile Number
                2. If Mobile Number + Service Name + Payment Mode present → MOBILE_MONEY
                3. If TerminalID present (no mobile fields) → ATM
                4. If MerchantID present → POS
                5. If PAN/Card fields dominant → CARDS
                6. If unclear from headers, examine Description/Narration/Message fields in sample rows
                7. Look for keywords: "Terminal/ATM" → ATM, "Merchant/POS" → POS, "Card/Interchange" → CARDS, "Mobile/USSD/Wallet" → MOBILE_MONEY
                8. If no clear indicators in headers or data, return "UNKNOWN"

                CRITICAL RULES:
                1. If file has Location field AND TransactionType → It's ATM_FILE (operational ATM log)
                2. If file has MTI AND Direction fields → It's SWITCH_FILE (technical switch messages)
                3. If file has Source/Destination (routing) but NO Location/TransactionType → It's SWITCH_FILE
                4. If file has Account_masked AND Location → It's ATM_FILE (even if it has RRN/STAN)
                5. If file has BOTH switch fields AND posted status → It's CBS_BANK_FILE
                6. If file has Mobile Number + Service Name + Payment Mode → It's MOBILE_MONEY_PLATFORM_FILE (channel: MOBILE_MONEY)
                7. ATM_FILE focuses on customer transactions; SWITCH_FILE focuses on message routing
                8. SWITCH files can be for ATM, POS, or CARDS - check ProcessingCode, MerchantID, or terminal type
                9. If TerminalID present + NO MerchantID + NO Mobile fields → likely ATM channel
                10. If MerchantID or MerchantName present → likely POS channel
                11. If strong card/PAN focus + interchange/scheme data → likely CARDS channel
                12. If Mobile Number + Payment Mode (USSD/ANDROID/Mobile) → definitely MOBILE_MONEY channel

                Input file signals:
                Headers: {headers}
                Sample Rows: {sample_rows}

                Rules:
                - Choose the MOST LIKELY source type based on column headers and data patterns
                - Identify the channel by checking BOTH column headers AND sample data content
                - For CBS files: Examine Description/Narration fields for channel keywords
                - For Mobile Money: Look for Mobile Number, Service Name, Payment Mode combinations
                - If multiple match, explain why one is preferred
                - Do NOT guess randomly - use "UNKNOWN" if truly ambiguous

                Respond ONLY in JSON:
                {{
                "source": "ATM_FILE | SWITCH_FILE | CBS_BANK_FILE | CARD_NETWORK_FILE | MOBILE_MONEY_PLATFORM_FILE",
                "channel": "ATM | POS | CARDS | MOBILE_MONEY | UNKNOWN",
                "reason": "short explanation of both source and channel"
                }}
"""


    # Retry logic with exponential backoff for rate limits
    max_retries = 1
    base_delay = 2  # Start with 2 seconds
    
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            
            llm_response_text = response.choices[0].message.content
            print("Raw LLM Response:", llm_response_text)
            
            # Strip markdown code blocks if present
            llm_response_text = llm_response_text.strip()
            if llm_response_text.startswith("```json"):
                llm_response_text = llm_response_text[7:]  # Remove ```json
            elif llm_response_text.startswith("```"):
                llm_response_text = llm_response_text[3:]  # Remove ```
            
            if llm_response_text.endswith("```"):
                llm_response_text = llm_response_text[:-3]  # Remove trailing ```
            
            llm_response_text = llm_response_text.strip()
            
            # Parse JSON response
            try:
                llm_result = json.loads(llm_response_text)
                return llm_result
            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
                # Return a fallback structure if parsing fails
                return {
                    "source": "UNKNOWN",
                    "channel": "UNKNOWN",
                    "reason": f"LLM response could not be parsed: {llm_response_text}",
                    "raw_response": llm_response_text
                }
                
        except Exception as e:
            error_message = str(e)
            
            # Check if it's a rate limit error
            if "429" in error_message or "rate_limit" in error_message.lower():
                if attempt < max_retries - 1:
                    # Exponential backoff: 2s, 4s, 8s
                    delay = base_delay * (2 ** attempt)
                    print(f"Rate limit hit (attempt {attempt + 1}/{max_retries}). Retrying in {delay} seconds...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"Rate limit exceeded after {max_retries} attempts. Returning fallback.")
                    return {
                        "source": "UNKNOWN",
                        "channel": "UNKNOWN",
                        "reason": f"Rate limit exceeded after {max_retries} retries",
                        "error": error_message
                    }
            else:
                # For other errors, return immediately
                print(f"OpenAI API error: {error_message}")
                return {
                    "source": "UNKNOWN",
                    "channel": "UNKNOWN",
                    "reason": f"OpenAI API error: {error_message}",
                    "error": error_message
                }
