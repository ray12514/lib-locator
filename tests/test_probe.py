import unittest

from probe import normalize_root_and_prefix, pinned_major_from_query


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
            ("libjpeg", "libjpeg.so.62", 62),
        )

    def test_pinned_major_from_query(self) -> None:
        self.assertEqual(pinned_major_from_query("libjpeg.so.62"), 62)
        self.assertIsNone(pinned_major_from_query("libjpeg"))


if __name__ == "__main__":
    unittest.main()
