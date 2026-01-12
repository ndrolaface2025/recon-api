"""
Rule Complexity Analyzer
Analyzes matching rule complexity for informational and monitoring purposes.

NOTE: All rules (SIMPLE and COMPLEX) are now executed via the application layer
for consistency and flexibility. The complexity classification is maintained for:
- Performance monitoring
- Rule optimization suggestions
- Debugging and analysis

Previously, SIMPLE rules used stored procedures and COMPLEX rules used Python.
Now, the unified application layer handles both types efficiently.
"""

from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class RuleComplexity:
    """Enum-like class for rule complexity levels"""
    SIMPLE = "SIMPLE"  # Can be handled by stored procedure
    COMPLEX = "COMPLEX"  # Requires application layer processing


class RuleComplexityAnalyzer:
    """
    Analyzes matching rule structure to determine execution strategy
    """
    
    @staticmethod
    def analyze(conditions: Dict[str, Any]) -> str:
        """
        Analyze rule conditions to determine complexity
        
        Args:
            conditions: JSONB conditions object from matching rule
            
        Returns:
            RuleComplexity.SIMPLE or RuleComplexity.COMPLEX
        """
        try:
            # Check for new format with condition_groups
            if "condition_groups" in conditions:
                return RuleComplexityAnalyzer._analyze_condition_groups(
                    conditions["condition_groups"]
                )
            
            # Check for old format with match_on
            elif "match_on" in conditions:
                return RuleComplexityAnalyzer._analyze_match_on(
                    conditions["match_on"]
                )
            
            else:
                logger.warning("Unknown conditions format, defaulting to COMPLEX")
                return RuleComplexity.COMPLEX
                
        except Exception as e:
            logger.error(f"Error analyzing rule complexity: {e}")
            # Default to COMPLEX for safety
            return RuleComplexity.COMPLEX
    
    @staticmethod
    def _analyze_condition_groups(condition_groups: List[Dict[str, Any]]) -> str:
        """
        Analyze condition_groups format (new hierarchical format)
        
        Complex indicators:
        - OR group_type at any level
        - Nested groups (more than 1 level deep)
        - Specific source pairs (not all sources)
        """
        if not condition_groups:
            return RuleComplexity.SIMPLE
        
        for group in condition_groups:
            # Check for OR operator
            if group.get("group_type") == "OR":
                logger.info("Rule contains OR group - marked as COMPLEX")
                return RuleComplexity.COMPLEX
            
            # Check for nested groups
            conditions = group.get("conditions", [])
            if RuleComplexityAnalyzer._has_nested_groups(conditions):
                logger.info("Rule contains nested groups - marked as COMPLEX")
                return RuleComplexity.COMPLEX
            
            # Check for specific source pairs
            if RuleComplexityAnalyzer._has_source_specific_conditions(conditions):
                logger.info("Rule contains source-specific conditions - marked as COMPLEX")
                return RuleComplexity.COMPLEX
        
        logger.info("Rule contains only simple AND conditions - marked as SIMPLE")
        return RuleComplexity.SIMPLE
    
    @staticmethod
    def _analyze_match_on(match_on: List[Dict[str, Any]]) -> str:
        """
        Analyze match_on format (old flat array format)
        
        This format only supports simple AND conditions
        """
        # match_on is always simple AND-only
        logger.info("Rule uses legacy match_on format - marked as SIMPLE")
        return RuleComplexity.SIMPLE
    
    @staticmethod
    def _has_nested_groups(conditions: List[Dict[str, Any]]) -> bool:
        """
        Check if conditions contain nested groups (recursively)
        """
        for condition in conditions:
            if "group_type" in condition:
                # This is a nested group
                return True
        return False
    
    @staticmethod
    def _has_source_specific_conditions(conditions: List[Dict[str, Any]]) -> bool:
        """
        Check if any condition specifies specific source pairs
        (e.g., only match amount between ATM and SWITCH)
        """
        for condition in conditions:
            # If condition has 'sources' array, it's source-specific
            if "sources" in condition and condition["sources"]:
                return True
        return False
    
    @staticmethod
    def get_execution_strategy(conditions: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get complete execution strategy with reasoning
        
        Returns:
            {
                "complexity": "SIMPLE" | "COMPLEX",
                "executor": "stored_procedure" | "application_layer",
                "reason": "explanation of why this strategy was chosen",
                "features_detected": ["OR_conditions", "nested_groups", etc.]
            }
        """
        complexity = RuleComplexityAnalyzer.analyze(conditions)
        features = []
        
        # Detect features
        if "condition_groups" in conditions:
            for group in conditions["condition_groups"]:
                if group.get("group_type") == "OR":
                    features.append("OR_operator")
                
                conditions_list = group.get("conditions", [])
                if RuleComplexityAnalyzer._has_nested_groups(conditions_list):
                    features.append("nested_groups")
                
                if RuleComplexityAnalyzer._has_source_specific_conditions(conditions_list):
                    features.append("source_specific_matching")
        
        # NOTE: All rules now use application_layer for consistency
        # Complexity classification is kept for informational/monitoring purposes
        # Previous approach used stored procedure for SIMPLE rules
        
        if complexity == RuleComplexity.SIMPLE:
            executor = "application_layer"
            reason = "Rule contains simple AND conditions. Executed via application layer for consistency and flexibility."
        else:
            executor = "application_layer"
            reason = f"Rule contains complex logic ({', '.join(features)}). Executed via application layer."
        
        return {
            "complexity": complexity,
            "executor": executor,
            "reason": reason,
            "features_detected": features
        }


# Convenience functions
def is_simple_rule(conditions: Dict[str, Any]) -> bool:
    """Check if rule is simple enough for stored procedure"""
    return RuleComplexityAnalyzer.analyze(conditions) == RuleComplexity.SIMPLE


def is_complex_rule(conditions: Dict[str, Any]) -> bool:
    """Check if rule requires application layer processing"""
    return RuleComplexityAnalyzer.analyze(conditions) == RuleComplexity.COMPLEX


def get_executor_type(conditions: Dict[str, Any]) -> str:
    """Get the executor type: 'stored_procedure' or 'application_layer'"""
    strategy = RuleComplexityAnalyzer.get_execution_strategy(conditions)
    return strategy["executor"]
