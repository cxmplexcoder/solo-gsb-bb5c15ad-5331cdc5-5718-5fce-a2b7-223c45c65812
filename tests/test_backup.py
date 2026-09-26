"""备份/恢复工具与互斥锁测试（仅标准库，可直接 python3 运行）。"""
import hashlib
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

_data_dir = tempfile.mkdtemp(prefix="calib_bk_data_")
_backup_dir = tempfile.mkdtemp(prefix="calib_bk_out_")
os.environ["CALIB_DATA_DIR"] = _data_dir
os.environ["CALIB_BACKUP_DIR"] = _backup_dir

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import backup as bk  # noqa: E402
from app import database as db  # noqa: E402
from app.service_lock import ServiceLock  # noqa: E402

SCORES_CSV = [
    {"contestant": "C1", "judge": "JA", "item": "P1", "score": 8.3},
    {"contestant": "C2", "judge": "JA", "item": "P1", "score": 8.0},
    {"contestant": "C1", "judge": "JB", "item": "P1", "score": 7.9},
    {"contestant": "C2", "judge": "JB", "item": "P1", "score": 7.8},
]


def seed_session(session_id="s1", scope="judge_item"):
    """写入一个带评分、裁决、批处理规则与校准范围的会话。"""
    with db.get_conn() as conn:
        db.ensure_session(conn, session_id, scope)
        db.replace_scores(conn, session_id, SCORES_CSV)
        db.upsert_decision(conn, session_id, 1, True, "复核确认")
        db.replace_rules(
            conn,
            session_id,
            [{"name": "r1", "judge": "JA", "item": None, "raw_min": None, "raw_max": None,
              "cal_min": None, "cal_max": None, "use_raw": True,
              "reason_template": "规则理由", "priority": 5}],
        )


def read_live(session_id="s1"):
    with db.get_conn() as conn:
        return {
            "scope": db.get_scope(conn, session_id),
            "scores": db.load_scores(conn, session_id),
            "decisions": db.load_decisions(conn, session_id),
            "rules": db.load_rules(conn, session_id),
        }


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        h.update(fh.read())
    return h.hexdigest()


class TestBackup(unittest.TestCase):
    def setUp(self):
        for path in (db.db_path(), db.db_path() + "-wal", db.db_path() + "-shm"):
            if os.path.exists(path):
                os.unlink(path)
        for f in Path(_backup_dir).glob("*"):
            f.unlink()
        db.init_db()

    def test_01_backup_creates_snapshot_and_checksum(self):
        seed_session()
        final = bk.create_backup()
        self.assertTrue(final.is_file())
        sidecar = Path(str(final) + ".sha256")
        self.assertTrue(sidecar.is_file())
        # 校验文件为 sha256sum 格式且摘要与内容一致
        digest, _, name = sidecar.read_text(encoding="utf-8").strip().partition(" ")
        self.assertEqual(digest, sha256(final))
        self.assertEqual(name.strip(), final.name)
        # 快照保留评分、裁决、批处理规则与校准范围
        conn = sqlite3.connect(final)
        conn.row_factory = sqlite3.Row
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM scores").fetchone()[0], 4)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM batch_rules").fetchone()[0], 1)
        self.assertEqual(
            conn.execute("SELECT scope FROM sessions WHERE id='s1'").fetchone()[0],
            "judge_item",
        )
        conn.close()

    def test_02_backup_consistent_while_writer_active(self):
        """源库被另一连接写入（未提交）时，在线备份仍成功且快照为已提交状态。"""
        seed_session()
        writer = sqlite3.connect(db.db_path())
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "INSERT INTO scores(session_id, contestant, judge, item, score, row_order) "
            "VALUES('s1', 'C9', 'JZ', 'P9', 1.0, 99)"
        )
        try:
            final = bk.create_backup()  # 写事务未提交期间备份
        finally:
            writer.rollback()
            writer.close()
        conn = sqlite3.connect(final)
        rows = conn.execute("SELECT COUNT(*) FROM scores WHERE session_id='s1'").fetchone()[0]
        conn.close()
        self.assertEqual(rows, 4)  # 未提交的 C9 不在快照中，快照一致

    def test_03_backup_missing_database(self):
        os.unlink(db.db_path())
        with self.assertRaises(bk.BackupError) as ctx:
            bk.create_backup()
        self.assertIn("不存在", str(ctx.exception))

    def test_04_restore_roundtrip(self):
        seed_session()
        final = bk.create_backup()
        # 备份后改动 live 库：删会话、改结构无关数据
        with db.get_conn() as conn:
            db.delete_session(conn, "s1")
            db.ensure_session(conn, "other", "judge")
        self.assertIsNone(read_live()["scope"])
        stats = bk.restore_backup(final)
        self.assertEqual(stats["sessions"], 1)
        self.assertEqual(stats["scores"], 4)
        self.assertEqual(stats["decisions"], 1)
        self.assertEqual(stats["batch_rules"], 1)
        live = read_live()
        self.assertEqual(live["scope"], "judge_item")
        self.assertEqual(len(live["scores"]), 4)
        self.assertEqual(live["decisions"], {1: (True, "复核确认")})
        self.assertEqual(len(live["rules"]), 1)
        self.assertEqual(live["rules"][0]["name"], "r1")

    def test_05_restore_clears_merge_previews(self):
        seed_session()
        with db.get_conn() as conn:
            token = db.save_merge_preview(conn, "s1", "judge", SCORES_CSV, [])
        final = bk.create_backup()
        # 备份里确有预览快照
        conn = sqlite3.connect(final)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM merge_previews").fetchone()[0], 1)
        conn.close()
        bk.restore_backup(final)
        with db.get_conn() as conn:
            n = conn.execute("SELECT COUNT(*) FROM merge_previews").fetchone()[0]
            self.assertIsNone(db.consume_merge_preview(conn, token, "s1"))
        self.assertEqual(n, 0)  # 旧合并标识全部失效

    def test_06_restore_rejects_bad_checksum_and_keeps_original(self):
        seed_session()
        final = bk.create_backup()
        before = sha256(db.db_path())
        # 篡改备份内容（保持是合法 sqlite 头之外的破坏）
        with open(final, "r+b") as fh:
            fh.seek(4096)
            fh.write(b"\xff" * 128)
        with self.assertRaises(bk.BackupError) as ctx:
            bk.restore_backup(final)
        self.assertIn("SHA-256", str(ctx.exception))
        self.assertEqual(sha256(db.db_path()), before)  # 原库保持不变

    def test_07_restore_rejects_missing_sidecar(self):
        seed_session()
        final = bk.create_backup()
        os.unlink(str(final) + ".sha256")
        before = sha256(db.db_path())
        with self.assertRaises(bk.BackupError) as ctx:
            bk.restore_backup(final)
        self.assertIn("校验文件", str(ctx.exception))
        self.assertEqual(sha256(db.db_path()), before)

    def test_08_restore_rejects_garbage_file(self):
        seed_session()
        garbage = Path(_backup_dir) / "garbage.db"
        garbage.write_bytes(b"not a sqlite database at all" * 100)
        with open(str(garbage) + ".sha256", "w", encoding="utf-8") as fh:
            fh.write(f"{sha256(garbage)}  {garbage.name}\n")
        before = sha256(db.db_path())
        with self.assertRaises(bk.BackupError) as ctx:
            bk.restore_backup(garbage)
        self.assertIn("损坏", str(ctx.exception))
        self.assertEqual(sha256(db.db_path()), before)

    def test_09_restore_rejects_foreign_key_violation(self):
        seed_session()
        bad = Path(_backup_dir) / "fk.db"
        conn = sqlite3.connect(bad)
        conn.executescript(db.SCHEMA)
        conn.execute("INSERT INTO sessions(id) VALUES('s1')")
        conn.execute(
            "INSERT INTO scores(session_id, contestant, judge, item, score, row_order) "
            "VALUES('ghost', 'C1', 'J1', 'P1', 1.0, 0)"  # 引用不存在的会话
        )
        conn.commit()
        conn.close()
        with open(str(bad) + ".sha256", "w", encoding="utf-8") as fh:
            fh.write(f"{sha256(bad)}  {bad.name}\n")
        before = sha256(db.db_path())
        with self.assertRaises(bk.BackupError) as ctx:
            bk.restore_backup(bad)
        self.assertIn("外键", str(ctx.exception))
        self.assertEqual(sha256(db.db_path()), before)

    def test_10_restore_rejects_incompatible_schema(self):
        seed_session()
        bad = Path(_backup_dir) / "old.db"
        conn = sqlite3.connect(bad)
        # 与当前版本不兼容的旧结构：scores 缺 row_order，且多一张未知表
        conn.executescript(
            """
            CREATE TABLE sessions (id TEXT PRIMARY KEY, scope TEXT NOT NULL DEFAULT 'judge',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')));
            CREATE TABLE scores (id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL, contestant TEXT NOT NULL, judge TEXT NOT NULL,
                item TEXT NOT NULL, score REAL NOT NULL);
            CREATE TABLE decisions (score_id INTEGER PRIMARY KEY, session_id TEXT NOT NULL,
                use_raw INTEGER NOT NULL DEFAULT 0, reason TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (datetime('now')));
            CREATE TABLE batch_rules (id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL, position INTEGER NOT NULL);
            CREATE TABLE merge_previews (token TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                scope TEXT NOT NULL, records_json TEXT NOT NULL, basis_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')), consumed_at TEXT);
            CREATE TABLE legacy_extra (id INTEGER PRIMARY KEY);
            """
        )
        conn.commit()
        conn.close()
        with open(str(bad) + ".sha256", "w", encoding="utf-8") as fh:
            fh.write(f"{sha256(bad)}  {bad.name}\n")
        before = sha256(db.db_path())
        with self.assertRaises(bk.BackupError) as ctx:
            bk.restore_backup(bad)
        msg = str(ctx.exception)
        self.assertIn("不兼容", msg)
        self.assertIn("row_order", msg)  # 指出缺少的列
        self.assertIn("legacy_extra", msg)  # 指出未知表
        self.assertEqual(sha256(db.db_path()), before)

    def test_11_restore_refused_while_service_alive(self):
        seed_session()
        final = bk.create_backup()
        lock = ServiceLock()  # 模拟运行中的服务持有锁
        self.assertTrue(lock.acquire())
        try:
            before = sha256(db.db_path())
            with self.assertRaises(bk.BackupError) as ctx:
                bk.restore_backup(final)
            self.assertIn("服务正在运行", str(ctx.exception))
            self.assertEqual(sha256(db.db_path()), before)
        finally:
            lock.release()
        # 服务停止（释放锁）后同一备份可以恢复
        bk.restore_backup(final)

    def test_12_restore_into_empty_data_dir(self):
        seed_session()
        final = bk.create_backup()
        os.unlink(db.db_path())
        stats = bk.restore_backup(final)
        self.assertEqual(stats["scores"], 4)
        self.assertEqual(read_live()["scope"], "judge_item")

    def test_13_no_temp_files_left(self):
        seed_session()
        final = bk.create_backup()
        with open(final, "r+b") as fh:  # 制造校验失败
            fh.seek(4096)
            fh.write(b"\x00" * 64)
        with self.assertRaises(bk.BackupError):
            bk.restore_backup(final)
        leftovers = [f for f in os.listdir(_data_dir) if f.startswith(".restore-")]
        self.assertEqual(leftovers, [])
        # 成功的恢复同样不留临时文件
        good = bk.create_backup()
        bk.restore_backup(good)
        leftovers = [f for f in os.listdir(_data_dir) if f.startswith(".restore-")]
        self.assertEqual(leftovers, [])

    def test_14_service_lock_mutual_exclusion(self):
        first, second = ServiceLock(), ServiceLock()
        self.assertTrue(first.acquire())
        try:
            self.assertFalse(second.acquire())  # 同一时刻只有一个持有者
        finally:
            first.release()
        self.assertTrue(second.acquire())
        second.release()

    def test_15_cli_success_and_failure_output(self):
        import io
        from contextlib import redirect_stderr, redirect_stdout

        seed_session()
        out = io.StringIO()
        with redirect_stdout(out):
            rc = bk.main(["backup"])
        self.assertEqual(rc, 0)
        self.assertIn("备份成功", out.getvalue())
        final = Path(out.getvalue().split("：")[1].split("（")[0])

        with redirect_stdout(out := io.StringIO()):
            rc = bk.main(["restore", str(final)])
        self.assertEqual(rc, 0)
        self.assertIn("恢复成功", out.getvalue())
        self.assertIn("merge_previews 已清空", out.getvalue())

        err = io.StringIO()
        with redirect_stderr(err):
            rc = bk.main(["restore", str(final) + ".missing"])
        self.assertEqual(rc, 1)
        self.assertIn("恢复失败", err.getvalue())
        self.assertIn("不存在", err.getvalue())


if __name__ == "__main__":
    unittest.main()
