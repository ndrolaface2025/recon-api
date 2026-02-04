from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case
from app.db.models.channel_config import ChannelConfig
from app.db.models.transactions import Transaction
from typing import List

class DashboardService:
    @staticmethod
    async def get_match_status_percentage(db: AsyncSession, current_user, payload) -> dict:
        query = (
                select(
                    func.count(Transaction.recon_reference_number).label("total_transactions"),

                    func.count(Transaction.recon_reference_number)
                    .filter(Transaction.match_status == 1)
                    .label("matched_count"),

                    func.count(Transaction.source_id)
                    .label("total_sources"),

                    func.count(Transaction.recon_reference_number)
                    .filter(Transaction.match_status == 2)
                    .label("partial_count"),

                    func.count(Transaction.recon_reference_number.is_(None))
                    .filter(Transaction.match_status == 0)
                    .label("unmatched_count")
                )
                .join(ChannelConfig, ChannelConfig.id == Transaction.channel_id)
                .where(
                    Transaction.created_by == current_user.id,
                    ChannelConfig.channel_name == payload
                )
            )
        result = await db.execute(query)
        row = result.one()
        total = row.total_transactions + row.unmatched_count
        matched = row.matched_count
        unmatched = row.unmatched_count
        partial = row.partial_count
        matched_percentage = (matched / total * 100) if total > 0 else 0
        unmatched_percentage = (unmatched / total * 100) if total > 0 else 0
        partial_percentage = (partial / total * 100) if total > 0 else 0
        return {
            "total_transactions": total,
            "matched_count": matched,
            "matched_percentage": round(matched_percentage, 2),
            "unmatched_count": unmatched,
            "unmatched_percentage": round(unmatched_percentage, 2),
            "partial_count": partial,
            "partial_percentage": round(partial_percentage, 2)
        }