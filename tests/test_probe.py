import unittest
from unittest.mock import patch

from probe import normalize_root_and_prefix, pinned_major_from_query, probe_rundown


class TestProbeHelpers(unittest.TestCase):
    def test_normalize_root_and_prefix(self) -> None:
        self.assertEqual(
            normalize_root_and_prefix("jpeg"),
            ("libjpeg", "libjpeg.so", None),
        )
        self.assertEqual(
            normalize_root_and_prefix("libjpeg.so"),
            ("libjpeg", "libjpeg.so", None),
        )
        self.assertEqual(
            normalize_root_and_prefix("libjpeg.so.62"),
            ("libjpeg", "libjpeg.so", 62),
        )

    def test_pinned_major_from_query(self) -> None:
        self.assertEqual(pinned_major_from_query("libjpeg.so.62"), 62)
        self.assertIsNone(pinned_major_from_query("libjpeg"))

    @patch("os.path.realpath")
    @patch("glob.glob")
    def test_probe_rundown_from_fs(self, mock_glob, mock_realpath) -> None:
        def fake_glob(pattern: str):
            mapping = {
                "/tmp/libroot": ["/tmp/libroot"],
                "/tmp/libroot/lib*.so*": [
                    "/tmp/libroot/libjpeg.so.62",
                    "/tmp/libroot/libz.so.1",
                ],
                "/tmp/libroot/*/lib*.so*": [
                    "/tmp/libroot/sub/libpng.so.16",
                ],
            }
            return mapping.get(pattern, [])

        mock_glob.side_effect = fake_glob
        mock_realpath.side_effect = lambda p: p

        out = probe_rundown(["/tmp/libroot"], no_ldconfig=True)

        manifest = out["manifest"]
        self.assertIn("libjpeg", manifest)
        self.assertIn("libz", manifest)
        self.assertIn("libpng", manifest)
        self.assertEqual(manifest["libjpeg"]["majors"], [62])
        self.assertEqual(manifest["libpng"]["majors"], [16])


if __name__ == "__main__":
    unittest.main()
