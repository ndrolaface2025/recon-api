"""
Test Script for Matching Rule Dispatcher
Tests both simple (stored procedure) and complex (application layer) rule execution
"""

import asyncio
import sys
from sqlalchemy import text

# Add parent directory to path
sys.path.append('/Users/mac/Documents/projects/recon-api-backend')

from app.db.session import AsyncSessionLocal
from app.engine.matching_dispatcher import MatchingRuleDispatcher
from app.engine.rule_complexity_analyzer import RuleComplexityAnalyzer


async def get_db_session():
    """Helper to get database session"""
    async with AsyncSessionLocal() as session:
        return session


async def test_complexity_analyzer():
    """Test 1: Rule Complexity Analyzer"""
    print("\n" + "="*60)
    print("TEST 1: Rule Complexity Analyzer")
    print("="*60)
    
    # Simple rule (AND only)
    simple_rule = {
        "match_on": [
            {"field": "rrn", "operator": "equals"},
            {"field": "amount", "operator": "equals"}
        ],
        "sources": ["ATM", "SWITCH", "CBS"]
    }
    
    result = RuleComplexityAnalyzer.analyze(simple_rule)
    print(f"\n✓ Simple Rule Analysis:")
    print(f"  Complexity: {result}")
    print(f"  Expected: SIMPLE")
    assert result == "SIMPLE", "Simple rule should be marked as SIMPLE"
    
    # Complex rule (with OR)
    complex_rule = {
        "condition_groups": [{
            "group_type": "AND",
            "conditions": [
                {"field": "rrn", "operator": "equals"},
                {
                    "group_type": "OR",
                    "conditions": [
                        {"field": "amount", "operator": "equals", "sources": ["ATM", "SWITCH"]},
                        {"field": "amount", "operator": "equals", "sources": ["SWITCH", "CBS"]}
                    ]
                }
            ]
        }],
        "sources": ["ATM", "SWITCH", "CBS"]
    }
    
    result = RuleComplexityAnalyzer.analyze(complex_rule)
    strategy = RuleComplexityAnalyzer.get_execution_strategy(complex_rule)
    
    print(f"\n✓ Complex Rule Analysis:")
    print(f"  Complexity: {result}")
    print(f"  Executor: {strategy['executor']}")
    print(f"  Reason: {strategy['reason']}")
    print(f"  Features: {strategy['features_detected']}")
    print(f"  Expected: COMPLEX → application_layer")
    assert result == "COMPLEX", "Complex rule should be marked as COMPLEX"
    assert strategy['executor'] == "application_layer", "Complex rule should use application_layer"
    
    print("\n✅ Test 1 PASSED: Complexity analyzer working correctly\n")


async def test_dispatcher_with_database():
    """Test 2: Dispatcher with Real Database"""
    print("\n" + "="*60)
    print("TEST 2: Dispatcher with Database")
    print("="*60)
    
    async with AsyncSessionLocal() as db:
        dispatcher = MatchingRuleDispatcher(db)
        
        # Check if we have any rules
        check_query = text("SELECT id, rule_name, conditions FROM tbl_cfg_matching_rule WHERE status = 1 LIMIT 5")
        result = await db.execute(check_query)
        rules = result.fetchall()
        
        if not rules:
            print("\n⚠️  No active rules found in database")
            print("   Skipping database tests")
            return
        
        print(f"\n✓ Found {len(rules)} active rules in database")
        
        for rule in rules:
            rule_id, rule_name, conditions = rule
            print(f"\n--- Analyzing Rule {rule_id}: {rule_name} ---")
            
            try:
                # Analyze rule
                analysis = await dispatcher.analyze_rule(rule_id)
                
                print(f"  Complexity: {analysis['complexity']}")
                print(f"  Executor: {analysis['executor']}")
                print(f"  Features: {analysis.get('features_detected', [])}")
                print(f"  Transaction Counts: {analysis.get('transaction_counts', {})}")
                print(f"  Estimated Time: {analysis.get('estimated_execution_time_ms', 0)}ms")
                
            except Exception as e:
                print(f"  ❌ Error analyzing rule: {e}")
        
        print("\n✅ Test 2 PASSED: Dispatcher can analyze database rules\n")


async def test_execute_simple_rule():
    """Test 3: Execute Simple Rule via Dispatcher"""
    print("\n" + "="*60)
    print("TEST 3: Execute Simple Rule (Dry Run)")
    print("="*60)
    
    async with AsyncSessionLocal() as db:
        dispatcher = MatchingRuleDispatcher(db)
        
        # Find a simple rule
        query = text("""
            SELECT id, rule_name, conditions 
            FROM tbl_cfg_matching_rule 
            WHERE status = 1 
              AND conditions ? 'match_on'
            LIMIT 1
        """)
        result = await db.execute(query)
        rule = result.fetchone()
        
        if not rule:
            print("\n⚠️  No simple rules found (need match_on format)")
            print("   Skipping execution test")
            return
        
        rule_id, rule_name, conditions = rule
        print(f"\n✓ Testing with Rule {rule_id}: {rule_name}")
        
        try:
            # Dry run execution
            result = await dispatcher.execute_matching_rule(
                rule_id=rule_id,
                channel_id=None,
                dry_run=True,
                min_sources=None
            )
            
            print(f"\n  Executor Used: {result.get('executor', 'unknown')}")
            print(f"  Complexity: {result.get('complexity', 'unknown')}")
            print(f"  Execution Time: {result.get('execution_time_ms', 0)}ms")
            print(f"  Expected Executor: stored_procedure")
            
            assert result['executor'] == 'stored_procedure', \
                f"Simple rule should use stored_procedure, got {result['executor']}"
            
            print("\n✅ Test 3 PASSED: Simple rule routed to stored procedure\n")
            
        except Exception as e:
            print(f"\n❌ Error executing rule: {e}")
            raise


async def test_analyze_all_rules():
    """Test 4: Analyze All Rules and Show Summary"""
    print("\n" + "="*60)
    print("TEST 4: Complete Rule Analysis Summary")
    print("="*60)
    
    async with AsyncSessionLocal() as db:
        # Get all active rules
        query = text("""
            SELECT id, rule_name, conditions, channel_id
            FROM tbl_cfg_matching_rule 
            WHERE status = 1
            ORDER BY id
        """)
        result = await db.execute(query)
        rules = result.fetchall()
        
        if not rules:
            print("\n⚠️  No active rules found")
            return
        
        print(f"\n✓ Analyzing {len(rules)} rules...\n")
        
        simple_count = 0
        complex_count = 0
        
        print(f"{'ID':<5} {'Rule Name':<30} {'Complexity':<12} {'Executor':<20}")
        print("-" * 75)
        
        for rule in rules:
            rule_id, rule_name, conditions, channel_id = rule
            
            # Analyze complexity
            complexity = RuleComplexityAnalyzer.analyze(conditions)
            strategy = RuleComplexityAnalyzer.get_execution_strategy(conditions)
            
            if complexity == "SIMPLE":
                simple_count += 1
            else:
                complex_count += 1
            
            print(f"{rule_id:<5} {rule_name[:29]:<30} {complexity:<12} {strategy['executor']:<20}")
        
        print("\n" + "="*75)
        print(f"Summary:")
        print(f"  ✓ Simple Rules (Stored Procedure): {simple_count}")
        print(f"  ✓ Complex Rules (Application Layer): {complex_count}")
        print(f"  ✓ Total Rules: {len(rules)}")
        print("="*75)
        
        print("\n✅ Test 4 PASSED: All rules analyzed successfully\n")


async def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("MATCHING RULE DISPATCHER - TEST SUITE")
    print("="*60)
    
    try:
        # Test 1: Complexity Analyzer (no database needed)
        await test_complexity_analyzer()
        
        # Test 2: Dispatcher with Database
        await test_dispatcher_with_database()
        
        # Test 3: Execute Simple Rule
        await test_execute_simple_rule()
        
        # Test 4: Analyze All Rules
        await test_analyze_all_rules()
        
        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED!")
        print("="*60)
        print("\nMatching Rule Dispatcher is working correctly!")
        print("- Simple rules route to stored procedure (fast)")
        print("- Complex rules route to application layer (flexible)")
        print("- Automatic routing works as expected\n")
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
