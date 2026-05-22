import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

REQUIRED_TAGLINE = "TraceDB is an AI-native transactional candidate-stream database."
REQUIRED_MANIFESTO = (
    "One logical record. One commit epoch. Many native views. "
    "No external sync drift. Explain every candidate."
)

CORE_FRAMING_DOCS = [
    ROOT / "README.md",
    ROOT / "docs" / "TraceDB.md",
    ROOT / "docs" / "platform-contract-v0.md",
]

BANNED_OVERCLAIM_PHRASES = [
    "100m",
    "100 m",
    "100 million",
    "valuation",
    "semi-working",
    "railway alpha",
    "current tracefield runtime",
    "implemented tracefield runtime",
    "production tracefield runtime",
    "product tracefield runtime",
    "tracefield runtime is current",
    "tracefield runtime is implemented",
    "tracefield is the current runtime",
    "tracefield is an implemented runtime",
    "tracefield is the product runtime",
    "tracefield is a production runtime",
    "memory calculus is implemented",
    "memory calculus is available",
    "implemented memory calculus",
    "current memory calculus runtime",
    "tensor compute/storage platform",
    "traceDB is a tensor compute platform",
    "traceDB is a tensor storage platform",
]


def read_doc(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def public_docs() -> list[Path]:
    paths = {ROOT / "README.md"}
    for pattern in [
        "docs/**/*.md",
        "clients/**/README.md",
        "benchmarks/realworld/*.md",
        "apps/**/README.md",
        "deploy/**/README.md",
    ]:
        paths.update(ROOT.glob(pattern))
    return sorted(paths)


def normalize(markdown: str) -> str:
    return " ".join(markdown.split())


class ProductFramingDocsTests(unittest.TestCase):
    def test_core_docs_include_product_framing(self) -> None:
        for path in CORE_FRAMING_DOCS:
            with self.subTest(path=path.relative_to(ROOT)):
                markdown = normalize(read_doc(path))
                self.assertIn(REQUIRED_TAGLINE, markdown)
                self.assertIn(REQUIRED_MANIFESTO, markdown)

    def test_public_docs_do_not_make_valuation_or_runtime_overclaims(self) -> None:
        for path in public_docs():
            markdown = normalize(read_doc(path)).lower()
            for phrase in BANNED_OVERCLAIM_PHRASES:
                with self.subTest(path=path.relative_to(ROOT), phrase=phrase):
                    self.assertNotIn(phrase.lower(), markdown)


if __name__ == "__main__":
    unittest.main()
