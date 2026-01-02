from datetime import datetime
import json


sources = [
            {
                "source_name": "ATM",
                "source_type": 1,
                "source_json": json.dumps([
                    {"column": "atm_index", "required": True, "type": "string"},
                    {"column": "amount", "required": True, "type": "decimal"},
                    {"column": "datetime", "required": True, "type": "datetime"},
                    {"column": "terminalId", "required": False, "type": "string"},
                    {"column": "location", "required": False, "type": "string"},
                    {"column": "pan_masked", "required": False, "type": "string"},
                    {"column": "account_number", "required": True, "type": "string"},
                    {"column": "stan", "required": True, "type": "string"},
                    {"column": "rrn", "required": True, "type": "string"},
                ]),
                "status": 1,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "version_number": 1,
            },
            {
                "source_name": "SWITCH",
                "source_type": 2,
                "source_json": json.dumps([
                    {"column": "datetime", "required": True, "type": "datetime"},
                    {"column": "direction", "required": False, "type": "string"},
                    {"column": "mit", "required": True, "type": "number"},
                    {"column": "pan_masked", "required": False, "type": "string"},
                    {"column": "account_number", "required": True, "type": "string"},
                    {"column": "stan", "required": True, "type": "string"},
                    {"column": "rrn", "required": True, "type": "string"},
                    {"column": "terminalId", "required": False, "type": "string"},
                ]),
                "status": 1,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "version_number": 1,
            },
            {
                "source_name": "CBS",
                "source_type": 3,
                "source_json": json.dumps([
                    {"column": "postdatetime", "required": True, "type": "datetime"},
                    {"column": "fc_txn_id", "required": True, "type": "string"},
                    {"column": "rrn", "required": True, "type": "string"},
                    {"column": "stan", "required": True, "type": "string"},
                    {"column": "dr", "required": True, "type": "string"},
                    {"column": "cr", "required": True, "type": "string"},
                    {"column": "currency", "required": False, "type": "string"},
                ]),
                "status": 1,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "version_number": 1,
            },
        ]