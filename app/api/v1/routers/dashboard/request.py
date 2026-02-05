from pydantic import BaseModel, Field
from typing import List, Dict, Literal

class transactions_matched(BaseModel):
    channel_name: list[Literal["ATM", "Mobile Money", "Internet Banking", "CARDS"]] = Field(default=["ATM"])