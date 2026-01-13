#!/usr/bin/env python3
"""
Seed Staging Database Script
Seeds the staging database with initial configuration data.
Can be run locally or inside Docker container on Hostinger.

Usage:
    # Local (connecting to remote):
    python scripts/seed_staging_database.py --host <hostinger-ip> --port 5432 --user postgres --password <pwd> --database recon_db
    
    # Inside Docker container:
    python scripts/seed_staging_database.py
"""

import asyncio
import sys
import argparse
from datetime import datetime
import asyncpg


# Configuration data to seed
SEED_DATA = {
    "modules": [
        {
            "module_name": "Reconciliation",
            "module_description": "Core reconciliation module",
            "module_permission_json": '{"read": true, "write": true, "execute": true}'
        }
    ],
    "roles": [
        {
            "name": "Admin",
            "description": "Administrator role with full access",
            "permission_json": '{"all": true}'
        },
        {
            "name": "Operator",
            "description": "Operator role with limited access",
            "permission_json": '{"read": true, "write": true}'
        }
    ],
    "sources": [
        {
            "source_name": "ATM",
            "source_type": 1,
            "source_json": '{"type": "atm", "format": "csv"}',
            "status": 1
        },
        {
            "source_name": "SWITCH",
            "source_type": 2,
            "source_json": '{"type": "switch", "format": "csv"}',
            "status": 1
        },
        {
            "source_name": "CBS",
            "source_type": 3,
            "source_json": '{"type": "cbs", "format": "csv"}',
            "status": 1
        }
    ],
    "users": [
        {
            "f_name": "System",
            "l_name": "Admin",
            "email": "admin@example.com",
            "username": "admin",
            "status": True,
            "role": 1  # Will be set after role is created
        }
    ],
    "channels": [
        {
            "channel_name": "ATM",
            "channel_description": "ATM Channel Reconciliation",
            "status": True
        }
    ]
}


async def seed_database(conn_params):
    """Seed the database with initial data"""
    
    print("🌱 Starting database seeding...")
    print(f"📡 Connecting to: {conn_params['host']}:{conn_params['port']}/{conn_params['database']}")
    
    try:
        # Connect to database
        conn = await asyncpg.connect(
            host=conn_params['host'],
            port=conn_params['port'],
            user=conn_params['user'],
            password=conn_params['password'],
            database=conn_params['database']
        )
        
        print("✅ Connected to database")
        
        # Check if tables exist
        tables_exist = await conn.fetchval("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'tbl_cfg_module'
        """)
        
        if not tables_exist:
            print("❌ Tables don't exist. Please run migrations first:")
            print("   alembic upgrade head")
            await conn.close()
            return False
        
        print("✅ Tables exist")
        
        # Start transaction
        async with conn.transaction():
            # 1. Seed Modules
            print("\n📦 Seeding modules...")
            for module in SEED_DATA["modules"]:
                existing = await conn.fetchval(
                    "SELECT id FROM tbl_cfg_module WHERE module_name = $1",
                    module["module_name"]
                )
                if not existing:
                    module_id = await conn.fetchval("""
                        INSERT INTO tbl_cfg_module (module_name, module_description, module_permission_json, created_at)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id
                    """, module["module_name"], module["module_description"], 
                         module["module_permission_json"], datetime.now())
                    print(f"   ✅ Created module: {module['module_name']} (ID: {module_id})")
                else:
                    print(f"   ⏭️  Module exists: {module['module_name']} (ID: {existing})")
            
            # Get module_id for roles
            module_id = await conn.fetchval("SELECT id FROM tbl_cfg_module WHERE module_name = $1", "Reconciliation")
            
            # 2. Seed Roles
            print("\n👥 Seeding roles...")
            role_ids = {}
            for role in SEED_DATA["roles"]:
                existing = await conn.fetchval(
                    "SELECT id FROM tbl_cfg_roles WHERE name = $1",
                    role["name"]
                )
                if not existing:
                    role_id = await conn.fetchval("""
                        INSERT INTO tbl_cfg_roles (module_id, name, description, permission_json, created_at)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id
                    """, module_id, role["name"], role["description"], 
                         role["permission_json"], datetime.now())
                    role_ids[role["name"]] = role_id
                    print(f"   ✅ Created role: {role['name']} (ID: {role_id})")
                else:
                    role_ids[role["name"]] = existing
                    print(f"   ⏭️  Role exists: {role['name']} (ID: {existing})")
            
            # 3. Seed Sources
            print("\n🔌 Seeding sources...")
            source_ids = {}
            for source in SEED_DATA["sources"]:
                existing = await conn.fetchval(
                    "SELECT id FROM tbl_cfg_source WHERE source_name = $1",
                    source["source_name"]
                )
                if not existing:
                    source_id = await conn.fetchval("""
                        INSERT INTO tbl_cfg_source (source_name, source_type, source_json, status, created_at, version_number)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        RETURNING id
                    """, source["source_name"], source["source_type"], source["source_json"],
                         source["status"], datetime.now(), 1)
                    source_ids[source["source_name"]] = source_id
                    print(f"   ✅ Created source: {source['source_name']} (ID: {source_id})")
                else:
                    source_ids[source["source_name"]] = existing
                    print(f"   ⏭️  Source exists: {source['source_name']} (ID: {existing})")
            
            # 4. Seed Users
            print("\n👤 Seeding users...")
            user_ids = {}
            for user in SEED_DATA["users"]:
                existing = await conn.fetchval(
                    "SELECT id FROM tbl_cfg_users WHERE username = $1",
                    user["username"]
                )
                if not existing:
                    user_id = await conn.fetchval("""
                        INSERT INTO tbl_cfg_users (f_name, l_name, email, username, role, status, created_at, version_number)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        RETURNING id
                    """, user["f_name"], user["l_name"], user["email"], user["username"],
                         role_ids["Admin"], user["status"], datetime.now(), 1)
                    user_ids[user["username"]] = user_id
                    print(f"   ✅ Created user: {user['username']} (ID: {user_id})")
                else:
                    user_ids[user["username"]] = existing
                    print(f"   ⏭️  User exists: {user['username']} (ID: {existing})")
            
            # Get admin user id
            admin_user_id = user_ids["admin"]
            
            # 5. Seed Channels
            print("\n📡 Seeding channels...")
            for channel in SEED_DATA["channels"]:
                existing = await conn.fetchval(
                    "SELECT id FROM tbl_cfg_channels WHERE channel_name = $1",
                    channel["channel_name"]
                )
                if not existing:
                    # Get source IDs for this channel
                    atm_id = source_ids.get("ATM")
                    switch_id = source_ids.get("SWITCH")
                    cbs_id = source_ids.get("CBS")
                    
                    channel_id = await conn.fetchval("""
                        INSERT INTO tbl_cfg_channels (
                            channel_name, channel_description, 
                            cannel_source_id, switch_source_id, cbs_source_id,
                            status, created_at, created_by, version_number
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        RETURNING id
                    """, channel["channel_name"], channel["channel_description"],
                         atm_id, switch_id, cbs_id,
                         channel["status"], datetime.now(), admin_user_id, 1)
                    print(f"   ✅ Created channel: {channel['channel_name']} (ID: {channel_id})")
                else:
                    print(f"   ⏭️  Channel exists: {channel['channel_name']} (ID: {existing})")
        
        print("\n✅ Database seeding completed successfully!")
        
        # Print summary
        print("\n📊 Summary:")
        module_count = await conn.fetchval("SELECT COUNT(*) FROM tbl_cfg_module")
        role_count = await conn.fetchval("SELECT COUNT(*) FROM tbl_cfg_roles")
        source_count = await conn.fetchval("SELECT COUNT(*) FROM tbl_cfg_source")
        user_count = await conn.fetchval("SELECT COUNT(*) FROM tbl_cfg_users")
        channel_count = await conn.fetchval("SELECT COUNT(*) FROM tbl_cfg_channels")
        
        print(f"   Modules: {module_count}")
        print(f"   Roles: {role_count}")
        print(f"   Sources: {source_count}")
        print(f"   Users: {user_count}")
        print(f"   Channels: {channel_count}")
        
        await conn.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Error seeding database: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(description="Seed staging database with initial data")
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
    
    success = asyncio.run(seed_database(conn_params))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
