import csv
import tempfile
import unittest
from pathlib import Path

from scraper import (
    CSV_FIELDS,
    City,
    CsvSink,
    calculate_distance,
    canonicalize_url,
    classify_chain,
    extract_coordinates,
    generate_grid,
    load_chains,
    normalize_text,
)


class TextAndClassificationTests(unittest.TestCase):
    def setUp(self):
        self.chains = load_chains(Path("supermarket_chains.json"))

    def test_normalizes_accents_and_punctuation(self):
        self.assertEqual(normalize_text("ATACADÃO — Témara"), "atacadao temara")

    def test_most_specific_brand_wins(self):
        self.assertEqual(classify_chain("Carrefour Market Gourmet Rabat", self.chains).brand, "Carrefour Gourmet")
        self.assertEqual(classify_chain("Marjane Market Agdal", self.chains).brand, "Marjane Market")

    def test_alias_and_word_boundaries(self):
        self.assertEqual(classify_chain("Acima Maarif", self.chains).brand, "Marjane Market")
        self.assertIsNone(classify_chain("Bimbo bakery", self.chains))

    def test_rejects_corporate_and_logistics_results(self):
        self.assertIsNone(classify_chain("BIM Maroc head office", self.chains))
        self.assertIsNone(classify_chain("Entrepôt Marjane", self.chains))


class GeographyAndUrlTests(unittest.TestCase):
    def test_distance(self):
        distance = calculate_distance(33.5731, -7.5898, 34.0209, -6.8416)
        self.assertGreater(distance, 80)
        self.assertLess(distance, 100)

    def test_grid_contains_center_and_uses_population(self):
        small = City("Small", 33.0, -7.0, 50_000)
        large = City("Large", 33.0, -7.0, 2_000_000)
        self.assertEqual(generate_grid(small, 8.0), [(33.0, -7.0)])
        self.assertIn((33.0, -7.0), generate_grid(large, 8.0))
        self.assertGreater(len(generate_grid(large, 8.0)), 1)

    def test_extracts_both_maps_coordinate_formats(self):
        self.assertEqual(extract_coordinates("https://x/data=!3d33.5!4d-7.6"), (33.5, -7.6))
        self.assertEqual(extract_coordinates("https://x/place/foo/@31.6,-8.1,15z"), (31.6, -8.1))

    def test_canonical_url_removes_tracking_query(self):
        self.assertEqual(
            canonicalize_url("https://www.google.com/maps/place/Test/data=abc?entry=ttu"),
            "https://www.google.com/maps/place/Test/data=abc",
        )


class CsvTests(unittest.IsolatedAsyncioTestCase):
    async def test_sink_deduplicates_and_writes_expected_header(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "output.csv"
            sink = CsvSink(path)
            row = {field: field for field in CSV_FIELDS}
            self.assertTrue(await sink.add(row, "same-key"))
            self.assertFalse(await sink.add(row, "same-key"))
            with path.open(encoding="utf-8-sig", newline="") as handle:
                written_rows = list(csv.DictReader(handle))
            self.assertEqual(len(written_rows), 1)
            self.assertEqual(list(written_rows[0]), CSV_FIELDS)
            self.assertEqual(sink.count, 1)


if __name__ == "__main__":
    unittest.main()
