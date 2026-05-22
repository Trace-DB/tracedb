import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DURABILITY_DOC = ROOT / "docs" / "durability-semantics-v0.md"


class DurabilitySemanticsDocsTests(unittest.TestCase):
    def test_durability_semantics_v0_declares_recovery_and_non_guarantees(self) -> None:
        markdown = DURABILITY_DOC.read_text(encoding="utf-8")

        required_sections = [
            "# TraceDB Durability Semantics v0",
            "## Scope",
            "## Durable Artifacts",
            "## Recovery Semantics",
            "## Snapshot And Restore",
            "## Idempotency Semantics",
            "## Known Non-Guarantees",
            "## Operator Checks",
        ]
        for section in required_sections:
            self.assertIn(section, markdown)

        required_claims = [
            "local-first, single-process engine",
            "WAL commit frames",
            "file.sync_data()",
            "manifest.tdb",
            "checkpoint",
            "torn WAL tail",
            "checksum mismatch",
            "engine.write.lock",
            "000001.twal.lock",
            "http-idempotency-cache.json",
            "not cross-replica",
            "not managed-cloud backup/DR",
            "not crash-atomic exactly-once",
            "source and target directories must differ",
        ]
        for claim in required_claims:
            self.assertIn(claim, markdown)

    def test_platform_contract_links_to_durability_semantics(self) -> None:
        contract = (ROOT / "docs" / "platform-contract-v0.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("docs/durability-semantics-v0.md", contract)


if __name__ == "__main__":
    unittest.main()
