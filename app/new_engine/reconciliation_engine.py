from app.services.channel_registry import CHANNEL_REGISTRY
from app.engine.loader import load
from typing import Dict, Any
class ReconciliationEngine:
    async def run(self, channel: str, inputs: Dict[str, str]) -> Dict[str, Any]:
        config = CHANNEL_REGISTRY.get(channel)
        if not config:
            raise ValueError(f"unknown channel {channel}")
        parsed = {}
        for source, path in inputs.items():
            parser_path = config['sources'].get(source)
            if not parser_path:
                raise ValueError(f"unknown source {source} for channel {channel}")
            ParserCls = load(parser_path)
            parser = ParserCls()
            parsed[source] = await parser.parse(path)
        NormalizerCls = load(config['normalizer'])
        normalizer = NormalizerCls()
        normalized = {}
        for source, rows in parsed.items():
            normalized[source] = normalizer.normalize(rows, source_system=source)
        MatcherCls = load(config['matcher'])
        matcher = MatcherCls()
        result = matcher.match(normalized)
        return result
