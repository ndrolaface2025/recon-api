from abc import ABC, abstractmethod
from typing import Dict, Any
class BaseMatcher(ABC):
    @abstractmethod
    def match(self, sources: Dict[str, list]) -> Dict[str, Any]:
        ...
