from collections import defaultdict

from sqlalchemy import (
    BigInteger,
    Integer,
    cast,
    case,
    func,
    literal,
    select,
)
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.transactions import Transaction
from app.db.models.manualTransaction import ManualTransaction
from app.db.models.channel_config import ChannelConfig as Channel
from app.db.models.user_config import UserConfig as User
from app.db.models.source_config import SourceConfig as Source
from app.db.models.upload_file import UploadFile
from app.db.models.matching_rule_config import MatchingRuleConfig as MatchingRule


async def get_reconciliation_runs(
    db: AsyncSession,
    page: int = 1,
    size: int = 10,
    from_date: str | None = None,
    to_date: str | None = None,
    channel_id: int | None = None,
    maker_id: int | None = None,
    checker_id: int | None = None,
    status: str | None = None,
    recon_group_number: str | None = None,
):
    offset = (page - 1) * size

    where_filters = []

    if from_date:
        where_filters.append(Transaction.date >= from_date)

    if to_date:
        where_filters.append(Transaction.date <= to_date)

    if channel_id:
        where_filters.append(Transaction.channel_id == channel_id)

    if maker_id:
        where_filters.append(Transaction.created_by == maker_id)

    if checker_id:
        where_filters.append(Transaction.updated_by == checker_id)

    if recon_group_number:
        where_filters.append(
            Transaction.recon_group_number.ilike(f"%{recon_group_number}%")
        )

    derived_status = case(
        (
            func.bool_and(Transaction.reconciliation_status == 2),
            "COMPLETED",
        ),
        else_="IN_PROGRESS",
    )

    base_stmt = (
        select(
            Transaction.recon_group_number.label("report_id"),
            Channel.id.label("channel_id"),
            Channel.channel_name.label("channel_name"),
            func.min(Transaction.date).label("recon_date"),
            func.min(Transaction.created_at).label("start_time"),
            func.max(
                func.coalesce(Transaction.updated_at, Transaction.created_at)
            ).label("end_time"),
            func.extract(
                "epoch",
                func.max(func.coalesce(Transaction.updated_at, Transaction.created_at))
                - func.min(Transaction.created_at),
            )
            .cast(Integer)
            .label("execution_seconds"),
            func.count().label("total"),
            func.count().filter(Transaction.match_status == 1).label("matched"),
            func.count().filter(Transaction.match_status == 2).label("partial"),
            func.count()
            .filter(
                (Transaction.match_status == 0) | (Transaction.match_status.is_(None))
            )
            .label("unmatched"),
            (
                func.count().filter(Transaction.match_status == 1)
                * 100.0
                / func.nullif(func.count(), 0)
            ).label("match_rate"),
            func.min(Transaction.created_by).label("maker_id"),
            func.min(User.f_name).label("maker_name"),
            func.max(Transaction.updated_by).label("checker_id"),
            func.max(User.f_name).label("checker_name"),
            derived_status.label("status"),
        )
        .join(Channel, Channel.id == Transaction.channel_id)
        .outerjoin(User, User.id.in_([Transaction.created_by, Transaction.updated_by]))
        .where(Transaction.recon_group_number.isnot(None))
        .where(*where_filters)
        .group_by(
            Transaction.recon_group_number,
            Channel.id,
            Channel.channel_name,
        )
    )

    if status:
        base_stmt = base_stmt.having(derived_status == status)

    count_stmt = select(func.count()).select_from(base_stmt.subquery())
    total_records = (await db.execute(count_stmt)).scalar()

    stmt = (
        base_stmt.order_by(func.min(Transaction.created_at).desc())
        .limit(size)
        .offset(offset)
    )

    rows = (await db.execute(stmt)).mappings().all()

    return {
        "page": page,
        "size": size,
        "total_records": total_records,
        "data": rows,
    }


async def get_uploaded_files_by_recon(
    db: AsyncSession,
    recon_group_number: str,
):
    """
    Steps:
    1. Get distinct file_transactions_id for a recon
    2. Join upload_files for metadata
    3. Aggregate txn counts per file
    """

    stmt = (
        select(
            UploadFile.id.label("file_id"),
            UploadFile.file_name.label("file_name"),
            Source.source_name.label("source"),
            UploadFile.created_at.label("uploaded_at"),
            User.f_name.label("uploaded_by"),
            UploadFile.total_records.label("total"),
            UploadFile.processed_records.label("processed"),
            UploadFile.success_records.label("success"),
            UploadFile.duplicate_records.label("duplicate"),
            UploadFile.failed_records.label("failed"),
        )
        .join(
            Transaction,
            Transaction.file_transactions_id == UploadFile.id,
        )
        .join(Source, Source.id == Transaction.source_id)
        .join(User, User.id == UploadFile.created_by)
        .where(Transaction.recon_group_number == recon_group_number)
        .group_by(
            UploadFile.id,
            UploadFile.file_name,
            Source.source_name,
            UploadFile.created_at,
            User.f_name,
        )
        .order_by(UploadFile.created_at)
    )

    result = await db.execute(stmt)
    return result.mappings().all()


async def get_rules_by_recon_group(
    db: AsyncSession,
    recon_group_number: str,
):
    rule_ids_subq = (
        select(Transaction.match_rule_id)
        .where(Transaction.recon_group_number == recon_group_number)
        .where(Transaction.match_rule_id.isnot(None))
        .distinct()
        .subquery()
    )

    stmt = (
        select(
            MatchingRule.id.label("rule_id"),
            MatchingRule.rule_name,
            MatchingRule.channel_id,
            Channel.channel_name,
            MatchingRule.rule_desc,
            MatchingRule.conditions,
            MatchingRule.tolerance,
            MatchingRule.status,
            MatchingRule.created_at,
        )
        .join(
            rule_ids_subq,
            rule_ids_subq.c.match_rule_id == MatchingRule.id,
        )
        .join(
            Channel,
            Channel.id == MatchingRule.channel_id,
        )
        .order_by(MatchingRule.rule_name)
    )

    result = await db.execute(stmt)
    return result.mappings().all()


async def get_reconciliation_mode_breakdown(
    db: AsyncSession,
    recon_group_number: str,
):
    """
    reconciled_mode:
      1 -> AUTOMATIC
      2 -> MANUAL
    """

    stmt = (
        select(
            case(
                (Transaction.reconciled_mode == 1, "AUTOMATIC"),
                (Transaction.reconciled_mode == 2, "MANUAL"),
                else_="UNKNOWN",
            ).label("type"),
            func.count().label("total"),
            func.count().filter(Transaction.match_status == 1).label("matched"),
            func.count().filter(Transaction.match_status == 2).label("partial"),
            func.count()
            .filter(
                (Transaction.match_status == 0) | (Transaction.match_status.is_(None))
            )
            .label("unmatched"),
            (
                func.count().filter(Transaction.match_status == 1)
                * 100.0
                / func.nullif(func.count(), 0)
            ).label("match_rate"),
        )
        .where(Transaction.recon_group_number == recon_group_number)
        .group_by(Transaction.reconciled_mode)
        .order_by(
            case(
                (Transaction.reconciled_mode == 1, 0),
                (Transaction.reconciled_mode == 2, 1),
                else_=2,
            )
        )
    )

    result = await db.execute(stmt)
    return result.mappings().all()


async def get_transactions_by_recon_and_mode(
    db: AsyncSession,
    recon_group_number: str,
    mode: str,  # AUTOMATIC | MANUAL
    page: int = 1,
    size: int = 20,
):
    offset = (page - 1) * size
    mode = mode.upper()

    if mode == "AUTOMATIC":
        base_stmt = (
            select(
                Transaction.id,
                Transaction.txn_id,
                Transaction.reference_number,
                Transaction.amount,
                Transaction.ccy,
                Transaction.account_number,
                Transaction.match_status,
                Transaction.reconciled_mode,
                Transaction.created_at,
            )
            .where(Transaction.recon_group_number == recon_group_number)
            .where(Transaction.reconciled_mode == 1)
            .order_by(Transaction.created_at.desc())
        )

    elif mode == "MANUAL":
        Reconciler = aliased(User)
        base_stmt = (
            select(
                ManualTransaction.id,
                ManualTransaction.manual_txn_id.label("txn_id"),
                ManualTransaction.reference_number,
                ManualTransaction.amount,
                ManualTransaction.ccy,
                ManualTransaction.account_number,
                Transaction.match_status,
                Transaction.reconciled_mode,
                Transaction.match_conditon,
                ManualTransaction.created_at,
                ManualTransaction.comment,
                Reconciler.f_name.label("reconciled_by"),
            )
            .join(
                Transaction,
                Transaction.id == cast(ManualTransaction.manual_txn_id, BigInteger),
            )
            .outerjoin(
                Reconciler,
                Reconciler.id == ManualTransaction.reconciled_by,
            )
            .where(Transaction.recon_group_number == recon_group_number)
            .order_by(ManualTransaction.created_at.desc())
        )

    else:
        raise ValueError("mode must be AUTOMATIC or MANUAL")

    count_stmt = select(func.count()).select_from(base_stmt.subquery())
    total = (await db.execute(count_stmt)).scalar()

    stmt = base_stmt.limit(size).offset(offset)
    rows = (await db.execute(stmt)).mappings().all()

    return {
        "page": page,
        "size": size,
        "total_records": total,
        "data": rows,
    }


async def get_full_transactions_by_recon_reference(
    db: AsyncSession,
    recon_reference_number: str,
):
    Reconciler = aliased(User)

    stmt = (
        select(
            Source.id.label("source_id"),
            Source.source_name,
            Transaction.id,
            Transaction.txn_id,
            Transaction.recon_reference_number,
            Transaction.reference_number,
            Transaction.source_reference_number,
            Transaction.amount,
            Transaction.ccy,
            Transaction.account_number,
            Transaction.date,
            Transaction.created_at,
            Transaction.updated_at,
            Transaction.match_status,
            Transaction.reconciliation_status,
            Transaction.reconciled_mode,
            Transaction.match_conditon,
            Transaction.comment,
            Transaction.reconciled_by,
            Reconciler.f_name.label("reconciled_by_name"),
            Transaction.created_by,
            Transaction.updated_by,
        )
        .join(
            Source,
            Source.id == Transaction.source_id,
        )
        .outerjoin(
            Reconciler,
            Reconciler.id == Transaction.reconciled_by,
        )
        .where(Transaction.recon_reference_number == recon_reference_number)
        .order_by(Source.source_name, Transaction.created_at)
    )

    rows = (await db.execute(stmt)).mappings().all()

    return {
        "recon_reference_number": recon_reference_number,
        "total_records": len(rows),
        "data": rows,
    }


async def get_transactions_by_recon_group_grouped(
    db: AsyncSession,
    recon_group_number: str,
    mode: str | None = None,
):
    Reconciler = aliased(User)

    manual_refs_subq = (
        select(Transaction.recon_reference_number)
        .join(
            ManualTransaction,
            Transaction.id == cast(ManualTransaction.manual_txn_id, BigInteger),
        )
        .where(Transaction.recon_group_number == recon_group_number)
        .where(Transaction.recon_reference_number.isnot(None))
        .distinct()
        .subquery()
    )

    stmt = (
        select(
            Transaction.recon_reference_number,
            Source.id.label("source_id"),
            Source.source_name,
            Transaction.id,
            Transaction.txn_id,
            Transaction.reference_number,
            Transaction.source_reference_number,
            Transaction.amount,
            Transaction.ccy,
            Transaction.account_number,
            Transaction.date,
            Transaction.created_at,
            Transaction.updated_at,
            Transaction.match_status,
            Transaction.reconciliation_status,
            Transaction.reconciled_mode,
            Transaction.match_conditon,
            Transaction.created_by,
            Transaction.updated_by,
            ManualTransaction.id.label("manual_id"),
            ManualTransaction.comment.label("manual_comment"),
            ManualTransaction.created_at.label("manual_created_at"),
            Reconciler.f_name.label("reconciled_by"),
        )
        .join(Source, Source.id == Transaction.source_id)
        .outerjoin(
            ManualTransaction,
            Transaction.id == cast(ManualTransaction.manual_txn_id, BigInteger),
        )
        .outerjoin(
            Reconciler,
            Reconciler.id == Transaction.reconciled_by,
        )
        .where(Transaction.recon_group_number == recon_group_number)
        .where(Transaction.recon_reference_number.isnot(None))
        .order_by(Transaction.recon_reference_number, Source.source_name)
    )

    if mode:
        mode = mode.upper()

        if mode == "MANUAL":
            stmt = stmt.where(Transaction.recon_reference_number.in_(manual_refs_subq))

        elif mode == "AUTOMATIC":
            stmt = stmt.where(~Transaction.recon_reference_number.in_(manual_refs_subq))

        else:
            raise ValueError("mode must be AUTOMATIC or MANUAL")

    rows = (await db.execute(stmt)).mappings().all()

    grouped = defaultdict(list)
    for r in rows:
        grouped[r.recon_reference_number].append(r)

    result = []
    for ref, items in grouped.items():
        result.append(
            {
                "recon_reference_number": ref,
                "total_records": len(items),
                "data": items,
            }
        )

    return {
        "recon_group_number": recon_group_number,
        "mode": mode or "BOTH",
        "total_groups": len(result),
        "groups": result,
    }
