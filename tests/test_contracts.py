import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from topic_radar.contracts import SignalEnvelope


class ContractTest(unittest.TestCase):
    def test_fixture_keeps_extension_fields(self):
        rows = json.loads((Path(__file__).parents[1] / "fixtures" / "signals.json").read_text(encoding="utf-8"))
        events = [SignalEnvelope.from_dict(row) for row in rows]
        self.assertEqual(events[0].attributes["region"], "华东")
        self.assertEqual(events[0].event_id, events[1].event_id)
        self.assertLess(events[2].occurred_at, events[2].received_at)


if __name__ == "__main__":
    unittest.main()
