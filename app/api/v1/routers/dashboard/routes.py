from fastapi import APIRouter,status, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.v1.routers.dashboard.request import transactions_matched
from app.db.models.user_config import UserConfig
from app.db.session import get_db
from app.dependencies.auth_dependencies import get_current_user
from app.api.v1.routers.dashboard.service import DashboardService
from loguru import logger
dashboard_routes = APIRouter(prefix="/dashboard", tags=["Dashboard"])

@dashboard_routes.post("/transaction-reconciliation-metrics")
async def get_all_transactions(payload: transactions_matched, db: AsyncSession = Depends(get_db),
                               current_user: UserConfig = Depends(get_current_user)):
    try:
        results = {}
        for channel in payload.channel_name:
            logger.info(f"Channel: {channel}")
            transactions = await DashboardService.get_match_status_percentage(db, current_user, channel)
            results[channel] = transactions
        return JSONResponse(content={"message": "Success", "results": results}, status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.error(f"Error in getting transaction reconciliation metrics: {str(e)}")
        return JSONResponse(
            content={"message": "Something went wrong, Please try again later"}, 
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)