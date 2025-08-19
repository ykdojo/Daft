# Test Setup for Lance Schema Blob Issue #4939

## Test Location

**File**: `tests/io/lancedb/test_lance_schema_blob.py`

### Why This Location?

The test was placed in the **existing** folder `tests/io/lancedb/` because:
- This folder already contains Lance-related tests (`test_lancedb_reads.py` and `test_lancedb_writes.py`)
- The issue #4939 is specifically about Lance write operations with blob schema encoding
- This is the most logical location alongside other Lance I/O tests

### Other Tests in Same Folder

The `tests/io/lancedb/` folder contains:
- `test_lancedb_reads.py` - Tests for reading from Lance datasets
- `test_lancedb_writes.py` - Tests for writing to Lance datasets
- `test_lance_schema_blob.py` - Our new test for the blob schema issue

## How to Run the Test

### Run This Specific Test File
```bash
DAFT_RUNNER=native .venv/bin/pytest tests/io/lancedb/test_lance_schema_blob.py -v
```

### Run a Single Test Function
```bash
DAFT_RUNNER=native .venv/bin/pytest tests/io/lancedb/test_lance_schema_blob.py::test_lance_schema_blob_encoding -v
```

### Important: DAFT_RUNNER Environment Variable
The `DAFT_RUNNER` environment variable **must** be set to either `native` or `ray` when running tests. This is enforced by `tests/conftest.py`.

## Will This Run Automatically on PRs?

**Yes**, this test will run automatically on every PR because:

1. The PR test suite workflow (`.github/workflows/pr-test-suite.yml`) runs on:
   - Every push to main branch
   - Every pull request to main branch

2. The unit test job in the workflow runs:
   ```bash
   pytest --ignore tests/integration
   ```
   This includes all tests under `tests/io/` that are not marked as integration tests.

3. Since our test is in `tests/io/lancedb/` and not marked with the `integration` marker, it will be included in the standard unit test run.

## Current Test Status

The test currently contains two simple failing assertions:
- `assert 1 == 0` - Placeholder for the actual Lance blob encoding test
- `assert result is False` - Demonstration of test pattern

These are intentionally failing to demonstrate the test setup. The actual implementation for issue #4939 would replace these with proper Lance schema blob encoding tests.
