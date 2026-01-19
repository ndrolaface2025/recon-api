"""
Smart Search Detector - Auto-detect field types from search values
"""
import re
from typing import Dict, List, Set


class SmartSearchDetector:
    """Intelligently detect whether a value is RRN, account number, or amount"""
    
    @staticmethod
    def detect_field_type(value: str) -> str:
        """
        Detect field type based on value pattern
        
        Rules:
        - Amount: Pure digits (2-6 digits) like "2000", "5000"
        - Account Number: Contains 'x' or is 10+ digits with pattern like "xxxxxx7890"
        - Reference Number (RRN): 12+ digits like "427654421259"
        - Default: Search across all fields
        
        Args:
            value: The search value to classify
            
        Returns:
            'amount', 'account_number', 'reference_number', or 'unknown'
        """
        value = value.strip()
        
        # Check if it's pure numeric
        if value.isdigit():
            length = len(value)
            
            # Short numbers (2-6 digits) are likely amounts
            if 2 <= length <= 6:
                return 'amount'
            
            # Medium numbers (7-11 digits) could be account numbers
            elif 7 <= length <= 11:
                return 'account_number'
            
            # Long numbers (12+ digits) are likely reference numbers
            elif length >= 12:
                return 'reference_number'
        
        # Contains 'x' pattern (masked account number like "xxxxxx7890")
        if 'x' in value.lower():
            return 'account_number'
        
        # Contains letters and numbers (could be reference with prefix)
        if re.match(r'^[A-Z]{2,5}\d+', value, re.IGNORECASE):
            return 'reference_number'
        
        return 'unknown'
    
    @staticmethod
    def parse_search_query(search_query: str) -> Dict[str, List[str]]:
        """
        Parse comma-separated search query and categorize values
        
        Args:
            search_query: Comma-separated string like "427654421259, xxxxxx7890, 2000, 5000"
            
        Returns:
            Dictionary with categorized values:
            {
                'amounts': ['2000', '5000'],
                'account_numbers': ['xxxxxx7890'],
                'reference_numbers': ['427654421259'],
                'unknown': []
            }
        """
        if not search_query:
            return {
                'amounts': [],
                'account_numbers': [],
                'reference_numbers': [],
                'unknown': []
            }
        
        # Split by comma and clean up
        values = [v.strip() for v in search_query.split(',') if v.strip()]
        
        categorized = {
            'amounts': [],
            'account_numbers': [],
            'reference_numbers': [],
            'unknown': []
        }
        
        for value in values:
            field_type = SmartSearchDetector.detect_field_type(value)
            
            if field_type == 'amount':
                categorized['amounts'].append(value)
            elif field_type == 'account_number':
                categorized['account_numbers'].append(value)
            elif field_type == 'reference_number':
                categorized['reference_numbers'].append(value)
            else:
                categorized['unknown'].append(value)
        
        return categorized
    
    @staticmethod
    def get_detection_summary(search_query: str) -> str:
        """
        Get a human-readable summary of detection results
        
        Args:
            search_query: The comma-separated search query
            
        Returns:
            Human-readable summary string
        """
        categorized = SmartSearchDetector.parse_search_query(search_query)
        
        summary_parts = []
        
        if categorized['amounts']:
            summary_parts.append(f"Amounts: {', '.join(categorized['amounts'])}")
        
        if categorized['account_numbers']:
            summary_parts.append(f"Accounts: {', '.join(categorized['account_numbers'])}")
        
        if categorized['reference_numbers']:
            summary_parts.append(f"RRNs: {', '.join(categorized['reference_numbers'])}")
        
        if categorized['unknown']:
            summary_parts.append(f"Unknown: {', '.join(categorized['unknown'])}")
        
        return " | ".join(summary_parts) if summary_parts else "No values detected"


# Example usage and tests
if __name__ == "__main__":
    detector = SmartSearchDetector()
    
    # Test individual detection
    test_cases = [
        ("2000", "amount"),
        ("5000", "amount"),
        ("xxxxxx7890", "account_number"),
        ("1234567890", "account_number"),
        ("427654421259", "reference_number"),
        ("390447500669", "reference_number"),
        ("834368215469", "reference_number"),
        ("REF123456", "reference_number"),
    ]
    
    print("Individual Field Type Detection:")
    print("=" * 50)
    for value, expected in test_cases:
        detected = detector.detect_field_type(value)
        status = "✅" if detected == expected else "❌"
        print(f"{status} '{value}' -> {detected} (expected: {expected})")
    
    print("\n" + "=" * 50)
    print("\nComma-Separated Query Parsing:")
    print("=" * 50)
    
    # Test comma-separated query
    query = "427654421259, xxxxxx7890, 2000, 5000, 390447500669"
    result = detector.parse_search_query(query)
    
    print(f"\nInput: {query}\n")
    print(f"Amounts: {result['amounts']}")
    print(f"Accounts: {result['account_numbers']}")
    print(f"RRNs: {result['reference_numbers']}")
    print(f"Unknown: {result['unknown']}")
    
    print(f"\nSummary: {detector.get_detection_summary(query)}")
