#!/usr/bin/env python3
"""
Create Matching Rule Script
Creates a matching rule in the staging database for testing.

Usage:
    python scripts/create_matching_rule.py --password <db-password>
"""

import asyncio
import argparse
import sys
from datetime import datetime
import asyncpg
import json


MATCHING_RULE = {
    "rule_name": "ATM-3WAY-RRN",
    "rule_desc": "3-way ATM matching by reference number",
    "channel_name": "ATM",
    "conditions": {
        "matching_type": "3-way",
        "sources": ["ATM", "SWITCH", "CBS"],
        "source_list": {
            "sourceA": 1,
            "sourceB": 2,
            "sourceC": 3
        },
        "condition_groups": [
            {
                "matching_fieldA": "reference_number",
                "operator0": "equalto",
                "matching_fieldB": "reference_number",
                "logicalOperator": "",
                "operator1": "==",
                "matching_fieldC": "reference_number"
            }
        ],
        "logic_expression": "ATM.reference_number == SWITCH.reference_number == CBS.reference_number"
    },
    "status": 1
}


async def create_matching_rule(conn_params):
    """Create a matching rule in the database"""
    
    print("🎯 Creating matching rule...")
    print(f"📡 Connecting to database...")
    
    conn = await asyncpg.connect(
        host=conn_params['host'],
        port=conn_params['port'],
        user=conn_params['user'],
        password=conn_params['password'],
        database=conn_params['database']
    )
    
    try:
        # Get channel ID
        channel = await conn.fetchrow(
            "SELECT id FROM tbl_cfg_channels WHERE channel_name = $1",
            MATCHING_RULE["channel_name"]
        )
        
        if not channel:
            print(f"❌ Channel '{MATCHING_RULE['channel_name']}' not found. Please run seed script first.")
            return False
        
        channel_id = channel['id']
        
        # Get user ID
        user = await conn.fetchrow("SELECT id FROM tbl_cfg_users LIMIT 1")
        user_id = user['id'] if user else 1
        
        # Check if rule already exists
        existing = await conn.fetchval(
            "SELECT id FROM tbl_cfg_matching_rule WHERE rule_name = $1 AND channel_id = $2",
            MATCHING_RULE["rule_name"], channel_id
        )
        
        if existing:
            print(f"⚠️  Rule '{MATCHING_RULE['rule_name']}' already exists (ID: {existing})")
            
            # Update it
            await conn.execute("""
                UPDATE tbl_cfg_matching_rule
                SET conditions = $1, updated_at = $2, updated_by = $3
                WHERE id = $4
            """, json.dumps(MATCHING_RULE["conditions"]), datetime.now(), user_id, existing)
            
            print(f"✅ Updated existing rule (ID: {existing})")
            return True
        
        # Create new rule
        rule_id = await conn.fetchval("""
            INSERT INTO tbl_cfg_matching_rule (
                rule_name, channel_id, rule_desc, conditions, status,
                created_at, created_by, updated_by, version_number
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
        """, 
            MATCHING_RULE["rule_name"],
            channel_id,
            MATCHING_RULE["rule_desc"],
            json.dumps(MATCHING_RULE["conditions"]),
            MATCHING_RULE["status"],
            datetime.now(),
            user_id,
            user_id,
            1
        )
        
        print(f"✅ Created matching rule '{MATCHING_RULE['rule_name']}' (ID: {rule_id})")
        
        # Display rule details
        print("\n📋 Rule Details:")
        print(f"   Name: {MATCHING_RULE['rule_name']}")
        print(f"   Channel: {MATCHING_RULE['channel_name']} (ID: {channel_id})")
        print(f"   Type: {MATCHING_RULE['conditions']['matching_type']}")
        print(f"   Sources: {', '.join(MATCHING_RULE['conditions']['sources'])}")
        print(f"   Logic: {MATCHING_RULE['conditions']['logic_expression']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser(description="Create matching rule")
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    parser.add_argument("--user", default="postgres", help="Database user")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument("--database", default="recon_db", help="Database name")
    
    args = parser.parse_args()
    
    conn_params = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database
    }
    
    success = asyncio.run(create_matching_rule(conn_params))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
