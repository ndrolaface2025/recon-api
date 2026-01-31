from sqlalchemy import select, func, or_
from sqlalchemy.orm import Session

from app.flexcube_db.models.gltm_glmaster import GltmGlmaster


class GltmGlmasterRepository:
    def __init__(self, db: Session):
        self.db = db

    def lookup_gl(
        self,
        search: str | None = None,
        limit: int = 50,
    ):

        query = select(
            GltmGlmaster.gl_code,
            GltmGlmaster.gl_desc,
        )

        if search:
            search_like = f"%{search.upper()}%"
            query = query.where(
                or_(
                    func.upper(GltmGlmaster.gl_code).like(search_like),
                    func.upper(GltmGlmaster.gl_desc).like(search_like),
                )
            )

        query = query.order_by(GltmGlmaster.gl_code).limit(limit)

        rows = self.db.execute(query).all()

        return [
            {
                "gl_code": row.gl_code,
                "gl_desc": row.gl_desc,
            }
            for row in rows
        ]
