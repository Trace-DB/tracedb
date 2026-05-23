import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class SchemaValidationDocsTests(unittest.TestCase):
    def test_schema_apply_docs_declare_hardening_rules(self) -> None:
        docs = "\n".join(
            [
                (ROOT / "docs" / "api" / "v1-http.md").read_text(encoding="utf-8"),
                (ROOT / "docs" / "platform-contract-v0.md").read_text(
                    encoding="utf-8"
                ),
            ]
        )

        required_claims = [
            "GraphQL-safe identifiers",
            "duplicate columns",
            "overlapping scalar/text/vector columns",
            "reserved TraceDB result metadata fields",
            "vector source columns",
            "before WAL append",
        ]
        for claim in required_claims:
            with self.subTest(claim=claim):
                self.assertIn(claim, docs)


if __name__ == "__main__":
    unittest.main()
