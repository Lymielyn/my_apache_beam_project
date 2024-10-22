"""
Python script to test the Apache Beam pipeline for the transactions data.

This version uses composite transformation
"""

from direct_runner_transactions import CompTransform
import unittest
import sys
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)

class CompTransformTest(unittest.TestCase):
    # Create a test pipeline - test the CompTransform class
    def test_comp_transform(self):
        with TestPipeline() as p:
            # CSV-formatted rows with commas
            LINES = [
                "2009-01-09 02:54:25 UTC, wallet00000e719adfeaa64b5a, wallet00000e719adfeaa64b5a, 450.76",
                "2019-01-09 02:54:25 UTC, wallet00000e719adfeaa88b5a, wallet00000e609adfeaa64b5a, 12.76",
                "2019-01-09 02:54:25 UTC, wallet00000e567adfeaa64b5a, wallet00000e788adfeaa64b5a, 300.47",
                "2019-01-09 02:54:25 UTC, wallet00000e567adfeaa64b5a, wallet00000e788adfeaa64b5a, 40.60",
                "2020-01-09 02:54:25 UTC, wallet00000e567adfeaa64b5a, wallet00000e788adfeaa64b5a, 10078.66"
            ]
            # Expected JSON output
            EXPECTED_OUTPUT = [
                '{"date": "2019-01-09", "total_amount": 341.07}',
                '{"date": "2020-01-09", "total_amount": 10078.66}'
            ]
            
            output = (
                p
                 | beam.Create(LINES)
                 | CompTransform()
             )

            assert_that(output, equal_to(EXPECTED_OUTPUT))

if __name__ == "__main__":
   main(out=None)