"""UNRUN: real local file inputs for the hosted source preflight bounds.

No account, child, custody owner, provider or Native object is created. These
future tests invoke only the new source reader/inventory helpers using actual
files under localdev. The separate two genuine hosted receipt tests are intact.
"""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest


class HostedSourceBounds(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / "hosted_owner_macos_account.py"
        spec = importlib.util.spec_from_file_location("hosted_source_bounds_only", path)
        cls.source = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.source)  # No main/guardian/runtime invocation.

    def setUp(self):
        base = Path.home() / "localdev"
        self.assertTrue(base.is_dir())
        self.space = tempfile.TemporaryDirectory(prefix="hosted-source-bounds-", dir=base)
        self.addCleanup(self.space.cleanup)
        self.root = Path(self.space.name)

    def test_exact_empty_and_binary_file_are_read(self):
        path = self.root / "source"
        for body in [b"", b"a\x00b\xff"]:
            path.write_bytes(body)
            self.assertEqual(self.source.bounded_source_bytes(path, len(body)), body)

    def test_actual_oversized_sparse_seal_is_refused_before_body_read(self):
        path = self.root / "seal.json"
        with path.open("wb") as stream:
            stream.truncate(16 * 1024 * 1024 + 1)
        with self.assertRaisesRegex(RuntimeError, "^source_read_size_or_kind$"):
            self.source.bounded_source_bytes(path, 16 * 1024 * 1024)

    def test_actual_oversized_sparse_member_is_refused_before_body_read(self):
        path = self.root / "member"
        with path.open("wb") as stream:
            stream.truncate(512 * 1024 * 1024 + 1)
        with self.assertRaisesRegex(RuntimeError, "^source_read_size_or_kind$"):
            self.source.bounded_source_bytes(path, 512 * 1024 * 1024)

    def test_inventory_and_reader_refuse_real_aliases(self):
        original = self.root / "original"
        original.write_bytes(b"source")
        alias = self.root / "alias"
        alias.symlink_to(original)
        with self.assertRaisesRegex(RuntimeError, "^source_alias_refused$"):
            self.source.bounded_source_inventory(self.root)
        with self.assertRaisesRegex(RuntimeError, "^source_read_size_or_kind$"):
            self.source.bounded_source_bytes(alias, 64)
        alias.unlink()
        os.link(original, alias)
        with self.assertRaisesRegex(RuntimeError, "^source_member_kind$"):
            self.source.bounded_source_inventory(self.root)
        with self.assertRaisesRegex(RuntimeError, "^source_read_size_or_kind$"):
            self.source.bounded_source_bytes(original, 64)

    def test_descriptor_refuses_real_symlink_and_directory_fifo(self):
        original = self.root / "original"
        original.write_bytes(b"source")
        alias = self.root / "alias"
        alias.symlink_to(original)
        with self.assertRaises(OSError):
            with self.source.source_read_descriptor(alias):
                self.fail("nofollow descriptor accepted an actual symlink")
        fifo = self.root / "fifo"
        os.mkfifo(fifo)
        with self.assertRaises(OSError):
            with self.source.source_read_descriptor(fifo, directory=True):
                self.fail("directory descriptor accepted an actual FIFO")

    def test_inventory_preserves_exact_nested_relative_names(self):
        (self.root / "nested").mkdir()
        (self.root / "one").write_bytes(b"1")
        (self.root / "nested/two").write_bytes(b"2")
        observed, directories = self.source.bounded_source_inventory(self.root)
        self.assertEqual({path for path, identity in directories}, {self.root, self.root / "nested"})
        self.assertEqual(set(observed), {"one", "nested/two"})
        self.assertEqual(observed["nested/two"], self.root / "nested/two")


if __name__ == "__main__":
    unittest.main()
