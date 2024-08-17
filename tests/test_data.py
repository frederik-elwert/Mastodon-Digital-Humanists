"""Tests for data integrity."""

import math
import unittest
from pathlib import Path
import csv

HERE = Path(__file__)
ROOT = HERE.parent.parent.resolve()
USERS_PATH = ROOT.joinpath("resources", "users.csv")


class TestData(unittest.TestCase):
    """A test case for data integrity."""

    def test_users(self):
        """Test the users CSV file has the right number of columns."""
        with USERS_PATH.open(newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            self.assertIn("account", reader.fieldnames,
                f"users.csv has wrong format or does not contain column 'account'")            
            errors = [
                (line_number, line)
                for line_number, line in enumerate(reader, start=2)
                if None in line or None in list(line.values())
            ]
        if errors:
            message = "Lines with incorrect number of columns:\n"
            max_line = max(i for i, _ in errors)
            width = int(0.5 + math.log10(max_line))
            for line_number, line in errors:
                message += f"[line {line_number:{width}}]: {line}\n"
            self.fail(message)
