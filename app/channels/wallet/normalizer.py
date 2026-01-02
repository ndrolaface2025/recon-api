from typing import List, Dict
class Normalizer:
    def normalize(self, rows: List[Dict], source_system: str = None) -> List[Dict]:
        out = []
        for r in rows:
            rrn = (r.get('rrn') or r.get('RRN') or '').strip()
            try:
                amount = float(r.get('amount') or 0)
            except:
                amount = 0.0
            out.append({
                "source_system": source_system,
                "rrn": rrn,
                "stan": r.get('stan') or None,
                "amount": amount,
                "currency": r.get('currency') or 'INR',
                "pan_masked": r.get('pan_masked') or None,
                "transaction_time": r.get('transaction_time') or None,
                "raw": r
            })
        return out
