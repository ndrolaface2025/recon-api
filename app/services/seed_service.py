from app.db.repositories.seed import SeedRepository
from sqlalchemy.ext.asyncio import AsyncSession
from app.utils.seedData import sources


class SeedService:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def seed_sources(self):
        # Define the schema for each source as Python objects, then dump to JSON text.
        # This makes the structure explicit and avoids JSON-encoding errors in literals.
        
        inserted = 0
        skipped = []

        for source in sources:
            existing = await SeedRepository.get_by_source_name(
                self.db, source["source_type"]
            )

            if existing:
                skipped.append(source["source_type"])
                continue

            await SeedRepository.create(self.db, source)
            inserted += 1

        return {
            "message":"script executed successfully",
            "inserted": inserted,
            "skipped": skipped,
        }
