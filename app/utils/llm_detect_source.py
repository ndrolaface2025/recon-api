from openai import OpenAI
import json
import os
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def llm_detect_source(headers, sample_rows):
    prompt = f"""
            You are part of a banking reconciliation system.
            You must classify the uploaded file into EXACTLY ONE category.

                Categories and definitions:

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

                CRITICAL RULES:
                1. If file has Location field AND TransactionType → It's ATM_FILE (operational ATM log)
                2. If file has MTI AND Direction fields → It's SWITCH_FILE (technical switch messages)
                3. If file has Source/Destination (routing) but NO Location/TransactionType → It's SWITCH_FILE
                4. If file has Account_masked AND Location → It's ATM_FILE (even if it has RRN/STAN)
                5. If file has BOTH switch fields AND posted status → It's CBS_BANK_FILE
                6. ATM_FILE focuses on customer transactions; SWITCH_FILE focuses on message routing

                Input file signals:
                Headers: {headers}
                Sample Rows: {sample_rows}

                Rules:
                - Choose the MOST LIKELY category
                - If multiple match, explain why one is preferred
                - Do NOT guess randomly

                Respond ONLY in JSON:
                {{
                "source": "ATM_FILE | SWITCH_FILE | CBS_BANK_FILE | CARD_NETWORK_FILE",
                "reason": "short explanation"
                }}
"""


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
            "reason": f"LLM response could not be parsed: {llm_response_text}",
            "raw_response": llm_response_text
        }
