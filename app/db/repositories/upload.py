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
    async def saveFileDetails(
        db: AsyncSession,
        fileData: List[Dict[str, Any]],
        fileJson: Dict[str, Any],
        getDateTimeColumnName: str,
        getAmountColumnName: str,
        getAcountNumberColumnName: str,
        getCurrencyColumnName: str
        ) -> Dict[str, Any]:
        try:
            duplicates = []
            new_records = []
            for row in fileData:
                conditions = [
                    Transaction.channel_id == fileJson["channel_id"],
                    Transaction.source_id == fileJson["source_id"],
                ]
                if getAmountColumnName is not None and getAmountColumnName in row:
                    conditions.append(
                        Transaction.amount == str(row[getAmountColumnName])
                    )

                if getDateTimeColumnName is not None and getDateTimeColumnName in row:
                    conditions.append(
                        Transaction.date == str(row[getDateTimeColumnName])
                    )

                if getCurrencyColumnName is not None and getCurrencyColumnName in row:
                    conditions.append(
                        Transaction.ccy == str(row[getCurrencyColumnName])
                    )
                stmt = select(Transaction).where(*conditions).limit(1)
                result = await db.execute(stmt)
                existing_record = result.scalar_one_or_none()

                if existing_record:
                    duplicates.append(row)
                    continue

                data = {
                    "channel_id": fileJson["channel_id"],
                    "source_id": fileJson["source_id"],
                    "otherDetails": json.dumps(row, default=str),
                    "file_transactions_id": fileJson["file_transactions_id"],
                    "created_by": fileJson["created_by"],
                    "updated_by": fileJson["updated_by"],
                    "version_number": fileJson["version_number"],
                }
                if getAmountColumnName is not None and getAmountColumnName in row:
                    data["amount"] = str(row[getAmountColumnName])

                if getDateTimeColumnName is not None and getDateTimeColumnName in row:
                    data["date"] = str(row[getDateTimeColumnName])

                if getAcountNumberColumnName is not None and getAcountNumberColumnName in row:
                    data["account_number"] = str(row[getAcountNumberColumnName])

                if getCurrencyColumnName is not None and getCurrencyColumnName in row:
                    data["ccy"] = str(row[getCurrencyColumnName])  
                
                new_records.append(Transaction(**data))

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