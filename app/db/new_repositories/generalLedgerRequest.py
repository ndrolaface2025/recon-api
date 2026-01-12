from pydantic import BaseModel, Field
from typing import Optional

class GeneralLedgerCreateRequest(BaseModel):
    general_ledger: str
    gl_role: Optional[str] = None
    channel_id: Optional[str] = None
    apply_to_all_channels: bool = False
    gl_description: Optional[str] = None
