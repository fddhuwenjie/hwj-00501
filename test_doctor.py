#!/usr/bin/env python3
"""Doctor 健康诊断命令的单元与端到端测试。"""

import gzip
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from typing import Any, Dict

import sys
sys.path.insert(0, str(Path(__file__).parent))

from sqlite_backup import (
    Store, Doctor, cmd_doctor, build_parser,
    _extract_backup_to, safe_exists_db
)


class TestDoctorBase(unittest.TestCase):
    """测试基类，提供临时目录和辅助方法。"""

    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="test_doctor_"))
        self.store_dir = self.tmpdir / ".backups"
        self.db_path = self.tmpdir / "app.db"
        self._create_sample_db(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _create_sample_db(self, path: Path, with_extra: bool = False) -> None:
        """创建一个示例数据库。"""
        with closing(sqlite3.connect(path)) as conn:
            conn.executescript("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    email TEXT NOT NULL
                );
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL
                );
            """)
            conn.executemany(
                "INSERT INTO users(username, email) VALUES(?,?)",
                [("alice", "alice@x.com"), ("bob", "bob@x.com")]
            )
            conn.executemany(
                "INSERT INTO orders(user_id, amount) VALUES(?,?)",
                [(1, 99.9), (2, 199.9)]
            )
            if with_extra:
                conn.execute("""
                    CREATE TABLE logs (
                        id INTEGER PRIMARY KEY,
                        message TEXT NOT NULL
                    )
                """)
                conn.execute("INSERT INTO logs(message) VALUES(?)", ("test",))
            conn.commit()

    def _create_backup(self, store: Store, db: Path, gzip: bool = False) -> str:
        """创建一个备份，返回 backup id。"""
        from sqlite_backup import cmd_backup
        args = build_parser().parse_args([
            "--store", str(store.root),
            "backup", str(db)
        ] + (["--gzip"] if gzip else []))
        cmd_backup(args)
        return store.latest_backup_id(db.name)

    def _create_snapshot(self, store: Store, db: Path, parent: str = None) -> str:
        """创建一个快照，返回 snapshot id。"""
        from sqlite_backup import cmd_snapshot
        pargs = ["--store", str(store.root), "snapshot", str(db)]
        if parent:
            pargs.extend(["--parent", parent])
        args = build_parser().parse_args(pargs)
        cmd_snapshot(args)
        return store.latest_snapshot_id(db.name)


class TestDoctorHealthyRepository(TestDoctorBase):
    """测试正常健康的备份仓库。"""

    def test_healthy_repository_no_issues(self) -> None:
        """正常仓库应该没有任何错误或警告。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)
        sid = self._create_snapshot(store, self.db_path)

        doctor = Doctor(store)
        report = doctor.run_all()

        self.assertEqual(report["summary"]["errors"], 0,
                         f"不应有错误, issues: {report['issues']}")
        self.assertEqual(report["summary"]["warnings"], 0,
                         f"不应有警告, issues: {report['issues']}")
        self.assertEqual(report["summary"]["total_issues"], 0)
        self.assertGreater(report["summary"]["total_checks"], 0)
        self.assertEqual(report["summary"]["affected_ids"], [])

    def test_healthy_with_target_database(self) -> None:
        """带目标数据库的正常仓库检查。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        doctor = Doctor(store, target_db=self.db_path)
        report = doctor.run_all()

        self.assertEqual(report["summary"]["errors"], 0)
        self.assertEqual(report["summary"]["warnings"], 0)

    def test_json_output(self) -> None:
        """测试 JSON 输出格式。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        args = build_parser().parse_args([
            "--store", str(store.root),
            "doctor", "--json"
        ])
        import io
        from contextlib import redirect_stdout

        f = io.StringIO()
        with redirect_stdout(f):
            exit_code = cmd_doctor(args)
        output = f.getvalue()

        self.assertEqual(exit_code, 0)
        parsed = json.loads(output)
        self.assertIn("summary", parsed)
        self.assertIn("issues", parsed)
        self.assertIn("total_checks", parsed["summary"])
        self.assertIn("errors", parsed["summary"])
        self.assertIn("warnings", parsed["summary"])


class TestDoctorMissingSnapshot(TestDoctorBase):
    """测试缺失快照文件的场景。"""

    def test_missing_snapshot_file_detected(self) -> None:
        """应该检测到缺失的 snapshot.json 文件。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)
        sid = self._create_snapshot(store, self.db_path)

        snap_file = store.root / store._read_index()["snapshots"][sid]["snapshot_path"]
        os.unlink(snap_file)

        doctor = Doctor(store)
        report = doctor.run_all()

        self.assertGreaterEqual(report["summary"]["errors"], 1)
        codes = [i["code"] for i in report["issues"]]
        self.assertIn("SNAPSHOT_FILE_MISSING", codes)
        affected = [i["affected_id"] for i in report["issues"] if i["affected_id"]]
        self.assertIn(sid, affected)


class TestDoctorCorruptedGzipBackup(TestDoctorBase):
    """测试损坏的 gzip 备份。"""

    def test_corrupted_gzip_backup_detected(self) -> None:
        """应该检测到损坏的 gzip 备份文件。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path, gzip=True)

        manifest = store.load_backup_manifest(bid)
        backup_file = store.root / store.get_backup(bid)["manifest_path"]
        backup_file = backup_file.parent / manifest["file_name"]

        with open(backup_file, "wb") as f:
            f.write(b"\x1f\x8b\x08\x00this is corrupted garbage")

        doctor = Doctor(store)
        report = doctor.run_all()

        self.assertGreaterEqual(report["summary"]["errors"], 1)
        codes = [i["code"] for i in report["issues"]]
        self.assertIn("BACKUP_GZIP_CORRUPTED", codes)
        affected = [i["affected_id"] for i in report["issues"] if i["affected_id"]]
        self.assertIn(bid, affected)

    def test_backup_checksum_mismatch_detected(self) -> None:
        """应该检测到备份文件 SHA256 不匹配。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        manifest = store.load_backup_manifest(bid)
        backup_file = store.root / store.get_backup(bid)["manifest_path"]
        backup_file = backup_file.parent / manifest["file_name"]

        with open(backup_file, "ab") as f:
            f.write(b"corrupted content")

        doctor = Doctor(store)
        report = doctor.run_all()

        codes = [i["code"] for i in report["issues"]]
        self.assertIn("BACKUP_CHECKSUM_MISMATCH", codes)


class TestDoctorBrokenChain(TestDoctorBase):
    """测试断裂的快照依赖链。"""

    def test_broken_snapshot_chain_detected(self) -> None:
        """应该检测到断裂的快照依赖链。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)
        sid1 = self._create_snapshot(store, self.db_path)

        os.unlink(self.db_path)
        self._create_sample_db(self.db_path, with_extra=True)
        sid2 = self._create_snapshot(store, self.db_path, parent=sid1)

        idx = store._read_index()
        del idx["snapshots"][sid1]
        store._write_index(idx)

        doctor = Doctor(store)
        report = doctor.run_all()

        self.assertGreaterEqual(report["summary"]["errors"], 1)
        codes = [i["code"] for i in report["issues"]]
        self.assertIn("SNAPSHOT_CHAIN_BROKEN", codes)
        affected = [i["affected_id"] for i in report["issues"] if i["affected_id"]]
        self.assertIn(sid2, affected)

    def test_missing_base_backup_detected(self) -> None:
        """应该检测到快照依赖的基准备份不存在。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)
        sid = self._create_snapshot(store, self.db_path)

        idx = store._read_index()
        del idx["backups"][bid]
        store._write_index(idx)

        doctor = Doctor(store)
        report = doctor.run_all()

        codes = [i["code"] for i in report["issues"]]
        self.assertIn("SNAPSHOT_BASE_MISSING", codes)
        self.assertIn("SNAPSHOT_CHAIN_BASE_MISSING", codes)


class TestDoctorStrictMode(TestDoctorBase):
    """测试 strict 模式的退出码。"""

    def test_strict_mode_with_warnings_returns_nonzero(self) -> None:
        """strict 模式下，存在 warning 时应返回非零退出码。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        manifest = store.load_backup_manifest(bid)
        manifest["stats"]["tables"].append("non_existent_table")
        manifest_path = store.root / store.get_backup(bid)["manifest_path"]
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

        args = build_parser().parse_args([
            "--store", str(store.root),
            "doctor", "--strict"
        ])
        import io
        from contextlib import redirect_stdout

        f = io.StringIO()
        with redirect_stdout(f):
            exit_code = cmd_doctor(args)

        self.assertEqual(exit_code, 1, "strict 模式下有 warning 应返回 1")

    def test_strict_mode_no_issues_returns_zero(self) -> None:
        """strict 模式下，无问题时应返回 0。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        args = build_parser().parse_args([
            "--store", str(store.root),
            "doctor", "--strict"
        ])
        import io
        from contextlib import redirect_stdout

        f = io.StringIO()
        with redirect_stdout(f):
            exit_code = cmd_doctor(args)

        self.assertEqual(exit_code, 0)

    def test_non_strict_mode_warnings_return_zero(self) -> None:
        """非 strict 模式下，只有 warning 应返回 0。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        manifest = store.load_backup_manifest(bid)
        manifest["stats"]["tables"].append("non_existent_table")
        manifest_path = store.root / store.get_backup(bid)["manifest_path"]
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

        args = build_parser().parse_args([
            "--store", str(store.root),
            "doctor"
        ])
        import io
        from contextlib import redirect_stdout

        f = io.StringIO()
        with redirect_stdout(f):
            exit_code = cmd_doctor(args)

        self.assertEqual(exit_code, 0, "非 strict 模式下只有 warning 应返回 0")


class TestDoctorIndexCorruption(TestDoctorBase):
    """测试 index.json 损坏的场景。"""

    def test_corrupted_index_json_detected(self) -> None:
        """应该检测到损坏的 index.json。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        with open(store.index_path, "w") as f:
            f.write("{ this is not valid json }")

        doctor = Doctor(store)
        report = doctor.run_all()

        self.assertGreaterEqual(report["summary"]["errors"], 1)
        codes = [i["code"] for i in report["issues"]]
        self.assertIn("INDEX_CORRUPTED", codes)


class TestDoctorTargetDatabase(TestDoctorBase):
    """测试目标数据库检查。"""

    def test_target_database_missing_detected(self) -> None:
        """应该检测到不存在的目标数据库。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        missing_db = self.tmpdir / "nonexistent.db"
        doctor = Doctor(store, target_db=missing_db)
        report = doctor.run_all()

        codes = [i["code"] for i in report["issues"]]
        self.assertIn("TARGET_DB_MISSING", codes)

    def test_target_database_not_sqlite_detected(self) -> None:
        """应该检测到非 SQLite 文件作为目标数据库。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        fake_db = self.tmpdir / "fake.db"
        with open(fake_db, "wb") as f:
            f.write(b"not a sqlite file")

        doctor = Doctor(store, target_db=fake_db)
        report = doctor.run_all()

        codes = [i["code"] for i in report["issues"]]
        self.assertIn("TARGET_DB_NOT_SQLITE", codes)

    def test_target_database_schema_mismatch_warning(self) -> None:
        """目标数据库 schema 与基准备份不一致时应给出 warning。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        os.unlink(self.db_path)
        self._create_sample_db(self.db_path, with_extra=True)

        doctor = Doctor(store, target_db=self.db_path)
        report = doctor.run_all()

        codes = [i["code"] for i in report["issues"]]
        self.assertIn("TARGET_DB_EXTRA_TABLES", codes)
        self.assertEqual(report["summary"]["errors"], 0)
        self.assertGreaterEqual(report["summary"]["warnings"], 1)


class TestDoctorMissingFiles(TestDoctorBase):
    """测试各种缺失文件的情况。"""

    def test_missing_backup_manifest_detected(self) -> None:
        """应该检测到缺失的 manifest.json。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        manifest_path = store.root / store.get_backup(bid)["manifest_path"]
        os.unlink(manifest_path)

        doctor = Doctor(store)
        report = doctor.run_all()

        codes = [i["code"] for i in report["issues"]]
        self.assertIn("BACKUP_MANIFEST_MISSING", codes)

    def test_missing_backup_data_file_detected(self) -> None:
        """应该检测到缺失的备份数据文件。"""
        store = Store(self.store_dir)
        bid = self._create_backup(store, self.db_path)

        manifest = store.load_backup_manifest(bid)
        backup_file = store.root / store.get_backup(bid)["manifest_path"]
        backup_file = backup_file.parent / manifest["file_name"]
        os.unlink(backup_file)

        doctor = Doctor(store)
        report = doctor.run_all()

        codes = [i["code"] for i in report["issues"]]
        self.assertIn("BACKUP_FILE_MISSING", codes)


class TestDoctorReportContent(unittest.TestCase):
    """测试诊断报告内容是否完整。"""

    def test_report_contains_required_fields(self) -> None:
        """报告应包含所有要求的字段。"""
        from sqlite_backup import render_doctor_report, Issue

        report = {
            "summary": {
                "total_checks": 10,
                "errors": 2,
                "warnings": 1,
                "total_issues": 3,
                "affected_ids": ["bk_test1", "sn_test2"],
            },
            "issues": [
                {
                    "severity": "error",
                    "code": "TEST_ERROR",
                    "message": "测试错误信息",
                    "affected_id": "bk_test1",
                    "suggestion": "建议修复动作",
                    "details": {"key": "value"},
                },
                {
                    "severity": "warning",
                    "code": "TEST_WARN",
                    "message": "测试警告信息",
                    "affected_id": "sn_test2",
                    "suggestion": "建议检查",
                    "details": {},
                },
            ],
        }

        text_report = render_doctor_report(report, json_output=False)
        self.assertIn("总检查数: 10", text_report)
        self.assertIn("错误数:   2", text_report)
        self.assertIn("警告数:   1", text_report)
        self.assertIn("bk_test1", text_report)
        self.assertIn("sn_test2", text_report)
        self.assertIn("TEST_ERROR", text_report)
        self.assertIn("TEST_WARN", text_report)
        self.assertIn("测试错误信息", text_report)
        self.assertIn("建议修复动作", text_report)

        json_report = render_doctor_report(report, json_output=True)
        parsed = json.loads(json_report)
        self.assertEqual(parsed["summary"]["total_checks"], 10)
        self.assertEqual(len(parsed["issues"]), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
