import json
from typing import Any, Dict, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.transactions import Transaction
from app.db.models.upload_file import UploadFile

class UploadRepository:

    @staticmethod
    async def saveUploadedFileDetails(db: AsyncSession,fileData :Dict[str, Any]) -> Dict[str, Any]:
        try:
            file_entry = UploadFile(
                file_name=fileData['file_name'],
                file_details= json.dumps (fileData['file_details']),
                channel_id = fileData['channel_id'],
                status=fileData['status'],
                record_details= json.dumps(fileData['record_details']),
                version_number=fileData['version_number'],
                created_by=fileData['created_by']
            )
            db.add(file_entry)
            await db.commit()
            await db.refresh(file_entry)
            return { "error": False, "status": "success","insertedId": file_entry.id}
        except Exception as e:
            await db.rollback()
            print("uploadRepository-saveUploadedFileDetails", str(e))
            return { "error": True, "status": "error", "message": str(e)}
        
    @staticmethod
    async def saveFileDetails(db: AsyncSession,fileData: List[Dict[str, Any]],fileJson: Dict[str, Any]) -> Dict[str, Any]:
        try:
            duplicates = []
            new_records = []
            for row in fileData:
                stmt = select(Transaction).where(
                    Transaction.channel_id == fileJson["channel_id"],
                    Transaction.source_id == fileJson["source_id"],
                    Transaction.amount == str(row["amount"]),
                    Transaction.date == row["datetime"],
                    Transaction.account_number == row["account_masked"],
                    Transaction.ccy == row["currency"],
                )

                result = await db.execute(stmt)
                existing_record = result.scalar_one_or_none()

                if existing_record:
                    duplicates.append(row)
                    continue

                new_records.append(
                    Transaction(
                        channel_id=fileJson["channel_id"],
                        source_id=fileJson["source_id"],
                        amount=str(row["amount"]),
                        date=row["datetime"],
                        account_number=row["account_masked"],
                        ccy=row["currency"],
                        recon_reference_number=fileJson["recon_reference_number"],
                        otherDetails=json.dumps(row),
                        file_transactions_id=fileJson["file_transactions_id"],
                        created_by=fileJson["created_by"],
                        updated_by=fileJson["updated_by"],
                        version_number=fileJson["version_number"],
                    )
                )

            if new_records:
                db.add_all(new_records)
                await db.commit()

            return {
                "status": "success",
                "message": f"{len(new_records)} records inserted, {len(duplicates)} duplicates skipped",
                "recordsSaved": len(new_records),
                "duplicateRecords": duplicates
            }
        except Exception as e:
            await db.rollback()
            print("uploadRepository-saveFileDetails", str(e))
            return { "error": True, "status": "error", "message": str(e)}