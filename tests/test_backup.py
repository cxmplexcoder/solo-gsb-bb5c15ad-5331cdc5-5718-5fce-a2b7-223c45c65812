"""备份/恢复工具测试（app.backup + app.locking）。

每个用例使用独立的临时数据目录与备份目录，不依赖全局 CALIB_DATA_DIR
（整套 pytest 与其他测试模块同进程运行、共享环境变量）。CLI 行为通过
子进程设置独立环境变量验证。覆盖：

- 在线备份保留全部会话的评分、裁决、批处理规则、校准范围与合并预览；
- SHA-256 旁路摘要的生成与校验（篡改/缺失摘要即拒绝）；
- 恢复前校验：数据库损坏、外键孤儿、表结构不兼容（缺列/额外表）一律拒绝；
- 服务存活（互斥锁占用）时拒绝恢复，且原库保持不变；
- 恢复在同目录临时库准备后原子替换，并清空 merge_previews；
- 备份期间并发写入仍可得到通过完整性检查的一致性快照。
"""
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app import backup as bk  # noqa: E402
from app import database as dbmod  # noqa: E402
from app.locking import (  # noqa: E402
    LockUnavailableError,
    acquire_service_lock,
    release_service_lock,
)

SID = "backup-session"


def new_dirs():
    data = tempfile.mkdtemp(prefix="calib-bk-data-")
    backups = tempfile.mkdtemp(prefix="calib-bk-files-")
    return data, backups


def seed_database(data_dir, sid=SID):
    """在隔离目录中直接建库并写入：会话（judge_item 范围）、评分、裁决、规则、预览。"""
    path = os.path.join(data_dir, "calibration.db")
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(dbmod.SCHEMA)
    conn.execute("INSERT INTO sessions(id, scope) VALUES(?, 'judge_item')", (sid,))
    rows = [
        (sid, "C1", "J1", "P1", 8.5, 0),
        (sid, "C2", "J1", "P1", 7.5, 1),
        (sid, "C1", "J2", "P2", 9.0, 2),
    ]
    conn.executemany(
        "INSERT INTO scores(session_id, contestant, judge, item, score, row_order) "
        "VALUES(?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.execute(
        "INSERT INTO decisions(score_id, session_id, use_raw, reason) "
        "VALUES(1, ?, 1, '保留原分')",
        (sid,),
    )
    conn.execute(
        "INSERT INTO batch_rules(session_id, position, name, judge, use_raw, priority) "
        "VALUES(?, 0, '规则甲', 'J1', 1, 5)",
        (sid,),
    )
    conn.execute(
        "INSERT INTO merge_previews(token, session_id, scope, records_json, basis_json) "
        "VALUES('tok123', ?, 'judge_item', '[]', '[]')",
        (sid,),
    )
    conn.commit()
    conn.close()
    return path


def make_backup(data_dir, backups, name="snapshot.db"):
    src = os.path.join(data_dir, "calibration.db")
    dest = os.path.join(backups, name)
    return bk.run_backup(dest, src_path=src)


def row_count(path, table):
    conn = sqlite3.connect(path)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()


class BackupTests(unittest.TestCase):
    def setUp(self):
        self.data, self.backups = new_dirs()
        self.src = seed_database(self.data)

    def test_01_backup_creates_db_and_sha256_sidecar(self):
        path, digest, counts = make_backup(self.data, self.backups)
        self.assertTrue(os.path.isfile(path))
        side = path + ".sha256"
        self.assertTrue(os.path.isfile(side))
        with open(side, encoding="ascii") as f:
            line = f.readline()
        self.assertTrue(line.startswith(digest + " *"))
        self.assertEqual(bk.sha256_file(path), digest)
        self.assertEqual(counts["sessions"], 1)
        self.assertEqual(counts["scores"], 3)
        self.assertEqual(counts["decisions"], 1)
        self.assertEqual(counts["batch_rules"], 1)
        self.assertEqual(counts["merge_previews"], 1)

    def test_02_backup_preserves_all_content_including_scope(self):
        path, _, _ = make_backup(self.data, self.backups)
        conn = sqlite3.connect(path)
        try:
            self.assertEqual(
                conn.execute(
                    "SELECT scope FROM sessions WHERE id=?", (SID,)
                ).fetchone()[0],
                "judge_item",
            )
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM scores").fetchone()[0], 3)
            self.assertEqual(
                conn.execute("SELECT use_raw, reason FROM decisions").fetchone(),
                (1, "保留原分"),
            )
            self.assertEqual(
                conn.execute("SELECT name, judge, priority FROM batch_rules").fetchone(),
                ("规则甲", "J1", 5),
            )
            self.assertEqual(
                conn.execute("SELECT token FROM merge_previews").fetchone()[0], "tok123"
            )
        finally:
            conn.close()

    def test_03_verify_ok(self):
        path, digest, _ = make_backup(self.data, self.backups)
        got_digest, counts = bk.run_verify(path)
        self.assertEqual(got_digest, digest)
        self.assertEqual(counts["scores"], 3)

    def test_04_default_path_avoids_overwrite_same_second(self):
        old = os.environ.get("CALIB_BACKUP_DIR")
        os.environ["CALIB_BACKUP_DIR"] = self.backups
        try:
            p1 = bk.default_backup_path()
            open(p1, "wb").close()
            p2 = bk.default_backup_path()
        finally:
            if old is None:
                del os.environ["CALIB_BACKUP_DIR"]
            else:
                os.environ["CALIB_BACKUP_DIR"] = old
        self.assertNotEqual(p1, p2)
        self.assertTrue(p2.endswith("-2.db"))

    def test_05_backup_missing_source_errors(self):
        empty = tempfile.mkdtemp()
        with self.assertRaises(bk.BackupError) as cm:
            bk.run_backup(
                os.path.join(self.backups, "x.db"),
                src_path=os.path.join(empty, "calibration.db"),
            )
        self.assertIn("不存在", str(cm.exception))

    def test_06_cli_backup_and_verify_in_subprocess(self):
        env = dict(os.environ, CALIB_DATA_DIR=self.data, CALIB_BACKUP_DIR=self.backups)
        rc = subprocess.run(
            [sys.executable, "-m", "app.backup", "backup"],
            cwd=ROOT, env=env, capture_output=True, text=True,
        )
        self.assertEqual(rc.returncode, 0, rc.stderr)
        made = [f for f in os.listdir(self.backups) if f.endswith(".db")]
        self.assertEqual(len(made), 1)
        rc = subprocess.run(
            [sys.executable, "-m", "app.backup", "verify",
             os.path.join(self.backups, made[0])],
            cwd=ROOT, env=env, capture_output=True, text=True,
        )
        self.assertEqual(rc.returncode, 0, rc.stderr)
        self.assertIn("[成功]", rc.stdout)


class RestoreTests(unittest.TestCase):
    def setUp(self):
        self.data, self.backups = new_dirs()
        self.src = seed_database(self.data)
        self.backup_path, _, _ = make_backup(self.data, self.backups)

    def _wipe_live(self):
        conn = sqlite3.connect(self.src)
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("DELETE FROM decisions")
        conn.execute("DELETE FROM scores")
        conn.execute("DELETE FROM batch_rules")
        conn.execute("DELETE FROM merge_previews")
        conn.execute("DELETE FROM sessions")
        conn.commit()
        conn.close()

    def test_10_restore_rebuilds_content_and_clears_merge_previews(self):
        self._wipe_live()
        result = bk.run_restore(self.backup_path, target_data_dir=self.data)
        self.assertTrue(result["replaced"])
        self.assertEqual(row_count(self.src, "scores"), 3)
        self.assertEqual(row_count(self.src, "decisions"), 1)
        self.assertEqual(row_count(self.src, "batch_rules"), 1)
        self.assertEqual(row_count(self.src, "merge_previews"), 0)
        conn = sqlite3.connect(self.src)
        scope = conn.execute("SELECT scope FROM sessions WHERE id=?", (SID,)).fetchone()[0]
        conn.close()
        self.assertEqual(scope, "judge_item")

    def test_11_restore_leaves_no_temp_files(self):
        bk.run_restore(self.backup_path, target_data_dir=self.data)
        leftovers = [
            f for f in os.listdir(self.data)
            if f.startswith(".restore-") or f.endswith("-journal")
        ]
        self.assertEqual(leftovers, [])
        self.assertEqual(sorted(os.listdir(self.data)), ["calibration.db", "calibration.lock"])

    def test_12_restore_refused_while_service_lock_held_original_unchanged(self):
        before = bk.sha256_file(self.src)
        fd = acquire_service_lock(self.data)
        try:
            with self.assertRaises(LockUnavailableError):
                bk.run_restore(self.backup_path, target_data_dir=self.data)
        finally:
            release_service_lock(fd)
        self.assertEqual(bk.sha256_file(self.src), before)

    def test_13_cli_restore_returns_1_when_locked(self):
        fd = acquire_service_lock(self.data)
        try:
            env = dict(os.environ, CALIB_DATA_DIR=self.data, CALIB_BACKUP_DIR=self.backups)
            proc = subprocess.run(
                [sys.executable, "-m", "app.backup", "restore", "--yes", self.backup_path],
                cwd=ROOT, env=env, capture_output=True, text=True,
            )
            self.assertEqual(proc.returncode, 1)
            self.assertIn("互斥锁", proc.stderr)
        finally:
            release_service_lock(fd)

    def test_14_cli_restore_happy_path_in_subprocess(self):
        self._wipe_live()
        env = dict(os.environ, CALIB_DATA_DIR=self.data, CALIB_BACKUP_DIR=self.backups)
        proc = subprocess.run(
            [sys.executable, "-m", "app.backup", "restore", "--yes", self.backup_path],
            cwd=ROOT, env=env, capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("[成功]", proc.stdout)
        self.assertIn("merge_previews", proc.stdout)
        self.assertEqual(row_count(self.src, "scores"), 3)
        self.assertEqual(row_count(self.src, "merge_previews"), 0)

    def test_15_checksum_tamper_rejects_and_keeps_original(self):
        bad = os.path.join(self.backups, "tampered.db")
        with open(self.backup_path, "rb") as src, open(bad, "wb") as dst:
            dst.write(src.read())
        with open(bad, "r+b") as f:
            f.seek(4096)
            f.write(b"\xaa" * 32)
        with open(self.backup_path + ".sha256") as src, open(bad + ".sha256", "w") as dst:
            dst.write(src.read())
        before = bk.sha256_file(self.src)
        with self.assertRaises(bk.BackupError) as cm:
            bk.run_verify(bad)
        self.assertIn("SHA-256", str(cm.exception))
        with self.assertRaises(bk.BackupError):
            bk.run_restore(bad, target_data_dir=self.data)
        self.assertEqual(bk.sha256_file(self.src), before)

    def test_16_missing_sidecar_rejects(self):
        nosum = os.path.join(self.backups, "nosum.db")
        with open(self.backup_path, "rb") as src, open(nosum, "wb") as dst:
            dst.write(src.read())
        with self.assertRaises(bk.BackupError) as cm:
            bk.run_restore(nosum, target_data_dir=self.data)
        self.assertIn("摘要文件", str(cm.exception))

    def test_17_not_a_database_rejects_and_keeps_original(self):
        bogus = os.path.join(self.backups, "bogus.db")
        with open(bogus, "wb") as f:
            f.write(b"definitely not sqlite" * 50)
        with open(bogus + ".sha256", "w") as f:
            f.write(f"{bk.sha256_file(bogus)} *bogus.db\n")
        before = bk.sha256_file(self.src)
        with self.assertRaises(bk.BackupError):
            bk.run_restore(bogus, target_data_dir=self.data)
        self.assertEqual(bk.sha256_file(self.src), before)

    def test_18_foreign_key_orphan_rejects(self):
        orphan = os.path.join(self.backups, "orphan.db")
        src = sqlite3.connect(self.src)
        dst = sqlite3.connect(orphan)
        src.backup(dst)
        dst.close()
        src.close()
        # 在 FK 关闭的独立连接中删除父表行，制造 scores 孤儿。
        c = sqlite3.connect(orphan)
        c.execute("PRAGMA foreign_keys=OFF")
        c.execute("DELETE FROM sessions")
        c.commit()
        c.close()
        bk.write_checksum(orphan)
        before = bk.sha256_file(self.src)
        with self.assertRaises(bk.BackupError) as cm:
            bk.run_restore(orphan, target_data_dir=self.data)
        self.assertIn("外键", str(cm.exception))
        self.assertEqual(bk.sha256_file(self.src), before)

    def test_19_incompatible_schema_missing_column_rejects(self):
        bad_schema = os.path.join(self.backups, "bad-schema.db")
        conn = sqlite3.connect(bad_schema)
        conn.executescript(dbmod.SCHEMA.replace("row_order INTEGER NOT NULL,\n    ", ""))
        conn.commit()
        conn.close()
        bk.write_checksum(bad_schema)
        with self.assertRaises(bk.BackupError) as cm:
            bk.run_restore(bad_schema, target_data_dir=self.data)
        self.assertIn("不兼容", str(cm.exception))
        self.assertIn("row_order", str(cm.exception))

    def test_20_extra_table_rejects(self):
        extra = os.path.join(self.backups, "extra-table.db")
        conn = sqlite3.connect(extra)
        conn.executescript(dbmod.SCHEMA)
        conn.execute("CREATE TABLE rogue_table(x INTEGER)")
        conn.commit()
        conn.close()
        bk.write_checksum(extra)
        with self.assertRaises(bk.BackupError) as cm:
            bk.run_restore(extra, target_data_dir=self.data)
        self.assertIn("额外表", str(cm.exception))

    def test_21_restore_into_fresh_data_dir(self):
        fresh = tempfile.mkdtemp()
        result = bk.run_restore(self.backup_path, target_data_dir=fresh)
        self.assertFalse(result["replaced"])
        target = os.path.join(fresh, "calibration.db")
        self.assertTrue(os.path.isfile(target))
        self.assertEqual(row_count(target, "scores"), 3)
        self.assertEqual(row_count(target, "merge_previews"), 0)

    def test_22_staging_failure_cleans_temp_and_keeps_original(self):
        # 模拟未完全停止的旧库留下 WAL：临时库已创建后才会检测到，
        # 此时必须清理 .restore-* 临时文件且原库保持不变。
        with open(os.path.join(self.data, "calibration.db-wal"), "wb") as f:
            f.write(b"\x00" * 32)
        before = bk.sha256_file(self.src)
        with self.assertRaises(bk.BackupError) as cm:
            bk.run_restore(self.backup_path, target_data_dir=self.data)
        self.assertIn("WAL", str(cm.exception))
        self.assertEqual(bk.sha256_file(self.src), before)
        leftovers = [f for f in os.listdir(self.data) if f.startswith(".restore-")]
        self.assertEqual(leftovers, [])


class LiveBackupTests(unittest.TestCase):
    def setUp(self):
        self.data, self.backups = new_dirs()
        self.src = seed_database(self.data)

    def test_30_backup_consistent_under_concurrent_writes(self):
        stop = threading.Event()
        errors = []

        def writer():
            conn = sqlite3.connect(self.src, timeout=30)
            conn.execute("PRAGMA busy_timeout=30000")
            n = 0
            try:
                while not stop.is_set():
                    conn.execute(
                        "INSERT INTO sessions(id, scope) VALUES(?, 'judge') "
                        "ON CONFLICT(id) DO UPDATE SET updated_at=datetime('now')",
                        (f"live-{os.getpid()}-{n}",),
                    )
                    conn.commit()
                    n += 1
            except Exception as exc:  # pragma: no cover - 诊断用
                errors.append(exc)
            finally:
                conn.close()

        t = threading.Thread(target=writer)
        t.start()
        try:
            out = os.path.join(self.backups, "live.db")
            path, _, counts = bk.run_backup(out, src_path=self.src)
        finally:
            stop.set()
            t.join()
        self.assertEqual(errors, [])
        # 在线备份接口产出的快照必须通过完整性、外键与结构校验。
        verified = bk.inspect_database(path, label="备份")
        self.assertEqual(verified["scores"], 3)
        self.assertGreaterEqual(verified["sessions"], counts["sessions"])
        self.assertEqual(bk.run_verify(path)[1]["scores"], 3)


if __name__ == "__main__":
    unittest.main()
