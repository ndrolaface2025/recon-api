from sqlalchemy import Column, String, CHAR, TIMESTAMP
from app.flexcube_db.base import FlexcubeBase


class GltmGlmaster(FlexcubeBase):
    __tablename__ = "GLTM_GLMASTER"

    gl_code = Column("GL_CODE", String(20), primary_key=True)
    gl_desc = Column("GL_DESC", String(100))
    gl_type = Column("GL_TYPE", String(20))
    gl_category = Column("GL_CATEGORY", String(20))

    ccy_restriction = Column("CCY_RESTRICTION", CHAR(1))
    leaf = Column("LEAF", CHAR(1))
    posting_allowed = Column("POSTING_ALLOWED", CHAR(1))
    balance_type = Column("BALANCE_TYPE", String(20))

    created_by = Column("CREATED_BY", String(30))
    auth_by = Column("AUTH_BY", String(30))
    created_at = Column("CREATED_AT", TIMESTAMP)
