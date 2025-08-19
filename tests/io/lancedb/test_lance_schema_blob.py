"""Test for Lance schema blob encoding issue #4939."""

from __future__ import annotations

import os
import tempfile

import lance
import pyarrow as pa
import pytest

import daft
from daft import Schema


@pytest.mark.xfail(reason="Issue #4939: Daft writes Legacy binary encoding instead of blob encoding")
def test_daft_lance_blob_encoding_issue():
    """Test for issue #4939: Lance schema does not work with blob encoding.

    Issue: https://github.com/Eventual-Inc/Daft/issues/4939

    This test reproduces the panic error when using Daft's write_lance()
    with blob metadata and then trying to read with Lance's take_blobs() API.

    Expected error:
    PanicException: Expected a struct encoding because we have a struct field
    in the schema but got the encoding Legacy(ArrayEncoding { array_encoding:
    Some(Binary(Binary {...})) })
    """
    # Create a temporary directory for the test
    with tempfile.TemporaryDirectory() as tmpdir:
        lance_path = os.path.join(tmpdir, "test_blob.lance")

        # Step 1: Create PyArrow schema with blob metadata
        schema = pa.schema(
            [
                pa.field("blob", pa.large_binary(), metadata={"lance-encoding:blob": "true"}),
            ]
        )

        # Step 2: Create Daft dataframe with binary data
        df = daft.from_pydict(
            {
                "blob": [b"foo", b"bar", b"baz"],
            }
        )

        # Step 3: Write to Lance using Daft with explicit schema
        daft_schema = Schema.from_pyarrow_schema(schema)
        df.write_lance(lance_path, schema=daft_schema)

        # Step 4: Try to read using Lance's take_blobs API
        # This should panic with the struct encoding error
        ds = lance.dataset(lance_path)

        # Verify the schema metadata was preserved
        assert ds.schema[0].metadata is not None
        assert b"lance-encoding:blob" in ds.schema[0].metadata
        assert ds.schema[0].metadata[b"lance-encoding:blob"] == b"true"

        # This should NOT panic but currently does due to issue #4939
        # When fixed, take_blobs should return BlobFile objects without panicking
        blobs = ds.take_blobs("blob", [0])

        # If we get here without a panic, the issue is fixed
        assert len(blobs) == 1
        blob_content = blobs[0].read()
        assert blob_content == b"foo"
