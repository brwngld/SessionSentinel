import os
import datetime
import pytest
from helpers.data_processing import export_all_formats


@pytest.fixture
def dummy_data():
    # 5 rows, 3 columns
    headers = ["ID", "Name", "Amount"]
    rows = [
        ["1", "Alice", "100"],
        ["2", "Bob", "200"],
        ["3", "Charlie", "300"],
        ["4", "Diana", "400"],
    ]
    return headers, rows

def test_export_all_formats(dummy_data, tmp_path):
    headers, rows = dummy_data
    start_date = "24/03/2026"
    label = "Test Report"

    # Run export into pytest's temp directory
    export_all_formats(
        data=rows,
        headers=headers,
        report_rows=rows,
        start_date=start_date,
        label=label,
        output_dir=str(tmp_path)
    )

    # Check that all four files exist
    files = list(tmp_path.iterdir())
    exts = {f.suffix for f in files}
    assert ".csv" in exts
    assert ".xlsx" in exts
    assert ".pdf" in exts
    assert ".docx" in exts

    # Optional: check row counts in CSV
    csv_file = [f for f in files if f.suffix == ".csv"][0]
    with open(csv_file, encoding="utf-8") as f:
        lines = f.readlines()
    assert len(lines) == len(rows) + 1  # header + rows