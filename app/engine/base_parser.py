from abc import ABC, abstractmethod
from typing import List, Dict
class BaseParser(ABC):
    @abstractmethod
    async def parse(self, file_path: str) -> List[Dict]:
        ...
