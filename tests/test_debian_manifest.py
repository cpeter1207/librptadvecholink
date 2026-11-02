"""Runtime dependencies include providers with the required playout support."""

import unittest
from pathlib import Path


class RuntimeProviders(unittest.TestCase):
    def test_runtime_includes_default_media_providers(self):
        control = (Path(__file__).resolve().parents[1] / "debian/control").read_text()
        runtime = next(
            stanza
            for stanza in control.split("\n\n")
            if stanza.startswith("Package: librptadvecholink1\n")
        )
        dependencies = runtime.partition("Depends:")[2].partition("Description:")[0]
        for provider in (
            "librptadv-codec-gsm1",
            "librptadv-codec-speex1",
            "librptadv-jitter-adapter1 (>= 0.1.0~alpha18-1)",
        ):
            with self.subTest(provider=provider):
                self.assertIn(provider, dependencies)


if __name__ == "__main__":
    unittest.main()
