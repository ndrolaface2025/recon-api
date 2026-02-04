from pydantic import BaseModel, Field
from typing import Optional

class GeneralLedgerCreateRequest(BaseModel):
    general_ledger: Optional[str] = None
    gl_role: Optional[str] = None
    channel_id: Optional[str] = None
    status: Optional[bool] = None
    apply_to_all_channels: Optional[bool] = None
    gl_description: Optional[str] = None
