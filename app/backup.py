"""SQLite 数据卷的本地备份与恢复命令行工具。

用法（容器内工作目录为 /app）：

    python -m app.backup backup                 # 在线备份（服务运行中可用）
    python -m app.backup restore <备份文件.db>   # 停服后恢复

- 数据库路径取自 :mod:`app.database`（``CALIB_DATA_DIR``），备份输出目录取
  环境变量 ``CALIB_BACKUP_DIR``（默认 ``/backups``，Compose 已挂载到宿主机
  ``./backups``）。
- 备份走 SQLite 在线备份接口（``Connection.backup``）：源库被服务读写时也
  能生成一致快照，全部会话的评分、裁决、批处理规则与校准范围一并保留；
  每个备份附带 ``<文件名>.sha256`` 校验文件（sha256sum 格式）。
- 恢复依次校验 SHA-256 摘要、``integrity_check``、``foreign_key_check``
  与当前表结构（与 :mod:`app.database` 的 SCHEMA 比对），然后在数据目录内
  的临时库中清空 ``merge_previews``（旧合并标识全部失效）并用 ``os.replace``
  原子替换；任一步失败即报错退出，原库保持不变。
- 服务存活（持有数据目录互斥锁）时拒绝恢复；恢复期间持锁，服务无法启动。
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from . import database as db
from .service_lock import ServiceLock, lock_path

BACKUP_DIR = os.environ.get("CALIB_BACKUP_DIR", "/backups")

# 在线备份每步拷贝的页数；源库被并发写入时备份会在步骤间短暂等待重试，
# 不会长时间阻塞服务写入。
_BACKUP_PAGES_PER_STEP = 1024


class BackupError(RuntimeError):
    """备份/恢复失败；消息直接展示给命令行用户。"""


# ---------- 通用小工具 ----------

def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(dirpath: str) -> None:
    fd = os.open(dirpath, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _unlink_quiet(path) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def _check_integrity(conn: sqlite3.Connection, label: str) -> None:
    rows = [r[0] for r in conn.execute("PRAGMA integrity_check").fetchall()]
    if rows != ["ok"]:
        raise BackupError(f"{label}完整性检查失败：{'; '.join(str(r) for r in rows[:3])}")


def _check_foreign_keys(conn: sqlite3.Connection, label: str) -> None:
    rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    if rows:
        table, rowid = rows[0][0], rows[0][1]
        raise BackupError(
            f"{label}存在 {len(rows)} 条外键违规（首条：表 {table} 行 {rowid}），数据不一致"
        )


# ---------- 表结构比对（以 app/database.py 的 SCHEMA 为准） ----------

def _read_structure(conn: sqlite3.Connection) -> dict[str, list[tuple]]:
    """读取全部用户表的列定义（列名、类型、非空、主键），按表名字典返回。"""
    structure: dict[str, list[tuple]] = {}
    tables = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    for (table,) in tables:
        quoted = '"' + table.replace('"', '""') + '"'
        cols = conn.execute(f"PRAGMA table_info({quoted})").fetchall()
        structure[table] = [(c[1], str(c[2]).upper(), c[3], c[5]) for c in cols]
    return structure


def _expected_structure() -> dict[str, list[tuple]]:
    """用当前 SCHEMA 在内存库中建表，得到本版本应有的表结构。"""
    mem = sqlite3.connect(":memory:")
    try:
        mem.executescript(db.SCHEMA)
        return _read_structure(mem)
    finally:
        mem.close()


def _structure_problems(conn: sqlite3.Connection) -> list[str]:
    expected = _expected_structure()
    actual = _read_structure(conn)
    problems: list[str] = []
    for table in sorted(set(expected) | set(actual)):
        if table not in actual:
            problems.append(f"缺少表 {table}")
            continue
        if table not in expected:
            problems.append(f"存在当前版本不认识的表 {table}")
            continue
        if expected[table] != actual[table]:
            exp_cols = {c[0] for c in expected[table]}
            act_cols = {c[0] for c in actual[table]}
            missing = sorted(exp_cols - act_cols)
            extra = sorted(act_cols - exp_cols)
            if missing:
                problems.append(f"表 {table} 缺少列 {', '.join(missing)}")
            if extra:
                problems.append(f"表 {table} 多出列 {', '.join(extra)}")
            if not missing and not extra:
                problems.append(f"表 {table} 的列定义与当前版本不一致")
    return problems


# ---------- 备份 ----------

def create_backup(dest_dir: "str | os.PathLike[str] | None" = None) -> Path:
    """在线备份当前数据库，返回备份文件路径（同目录生成 .sha256 校验文件）。

    服务运行并写入时也可调用：在线备份接口保证快照一致。生成的备份在写入
    校验文件前先通过完整性与外键检查；任何失败都不会留下看似可用的半截备份。
    """
    dest = Path(dest_dir if dest_dir is not None else BACKUP_DIR)
    src_path = db.db_path()
    if not os.path.isfile(src_path):
        raise BackupError(
            f"数据库文件不存在：{src_path}（服务尚未写入数据或数据目录挂载有误）"
        )
    dest.mkdir(parents=True, exist_ok=True)

    # 以 O_EXCL 抢占唯一文件名，避免并发备份互相覆盖。
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    final: Path | None = None
    for attempt in range(100):
        name = f"calibration-{stamp}.db" if attempt == 0 else f"calibration-{stamp}-{attempt}.db"
        candidate = dest / name
        try:
            fd = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            continue
        os.close(fd)
        final = candidate
        break
    if final is None:
        raise BackupError(f"无法在 {dest} 生成唯一的备份文件名")

    sidecar = Path(str(final) + ".sha256")
    try:
        try:
            src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
            try:
                dst = sqlite3.connect(final)
                try:
                    # 在线备份：源库可继续被服务读写，目标得到一致快照。
                    src.backup(dst, pages=_BACKUP_PAGES_PER_STEP)
                    _check_integrity(dst, "备份快照")
                    _check_foreign_keys(dst, "备份快照")
                finally:
                    dst.close()
            finally:
                src.close()
            _fsync_file(final)
            digest = _sha256_file(final)
            sidecar.write_text(f"{digest}  {final.name}\n", encoding="utf-8")
            _fsync_file(sidecar)
            _fsync_dir(str(dest))
        except BackupError:
            raise
        except (sqlite3.Error, OSError) as exc:
            raise BackupError(f"备份过程出错：{exc}") from exc
    except BaseException:
        # 失败不留下看似可用的半截备份及其校验文件。
        _unlink_quiet(final)
        _unlink_quiet(sidecar)
        raise
    return final


# ---------- 恢复 ----------

def _validate_backup_file(backup: Path) -> None:
    """校验 SHA-256 摘要、数据库完整性、外键与当前表结构；不合格抛 BackupError。"""
    sidecar = Path(str(backup) + ".sha256")
    if not sidecar.is_file():
        raise BackupError(f"缺少校验文件 {sidecar.name}，备份不完整")
    try:
        tokens = sidecar.read_text(encoding="utf-8").split()
    except (OSError, UnicodeDecodeError) as exc:
        raise BackupError(f"校验文件 {sidecar.name} 无法读取：{exc}") from exc
    expected = tokens[0].strip().lower() if tokens else ""
    if len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
        raise BackupError(f"校验文件 {sidecar.name} 内容不是有效的 SHA-256 摘要")
    actual = _sha256_file(backup)
    if actual != expected:
        raise BackupError(
            f"SHA-256 校验不匹配（期望 {expected}，实际 {actual}），备份文件已损坏或被篡改"
        )
    try:
        conn = sqlite3.connect(f"file:{backup}?mode=ro", uri=True)
        try:
            _check_integrity(conn, "备份数据库")
            _check_foreign_keys(conn, "备份数据库")
            problems = _structure_problems(conn)
        finally:
            conn.close()
    except sqlite3.Error as exc:
        raise BackupError(f"备份不是有效的 SQLite 数据库或已损坏：{exc}") from exc
    if problems:
        raise BackupError("备份表结构与当前版本不兼容：" + "；".join(problems))


def restore_backup(backup_file: "str | os.PathLike[str]") -> dict[str, int]:
    """从备份恢复数据库，返回恢复后的数据统计；失败抛 BackupError，原库不变。

    全程持有数据目录互斥锁：服务存活时抢锁失败直接拒绝；恢复期间服务无法
    启动。备份内容先在同目录临时库中准备好（清空 merge_previews，使旧合并
    标识失效），最后 os.replace 原子替换。
    """
    backup = Path(backup_file)
    if not backup.is_file():
        raise BackupError(f"备份文件不存在：{backup}")

    lock = ServiceLock()
    if not lock.acquire():
        raise BackupError(
            f"服务正在运行（数据目录锁 {lock_path()} 被持有），拒绝恢复；"
            "请先停止服务（docker compose stop calibration）再执行恢复"
        )
    try:
        _validate_backup_file(backup)

        data_dir = db.DEFAULT_DATA_DIR
        os.makedirs(data_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".restore-", suffix=".db", dir=data_dir)
        os.close(fd)
        try:
            try:
                shutil.copyfile(backup, tmp)
                conn = sqlite3.connect(tmp)
                try:
                    conn.execute("PRAGMA foreign_keys = ON")
                    # 恢复后旧合并预览标识一律失效：待合并快照不属于可恢复数据。
                    conn.execute("DELETE FROM merge_previews")
                    conn.commit()
                    _check_integrity(conn, "恢复后数据库")
                    _check_foreign_keys(conn, "恢复后数据库")
                    stats = {
                        "sessions": conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0],
                        "scores": conn.execute("SELECT COUNT(*) FROM scores").fetchone()[0],
                        "decisions": conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0],
                        "batch_rules": conn.execute("SELECT COUNT(*) FROM batch_rules").fetchone()[0],
                    }
                finally:
                    conn.close()
                _fsync_file(Path(tmp))
            except BackupError:
                raise
            except (sqlite3.Error, OSError) as exc:
                raise BackupError(f"准备恢复数据时出错：{exc}") from exc

            target = db.db_path()
            # 替换前清掉旧库的日志残留（默认回滚日志模式下通常不存在）。
            for suffix in ("-wal", "-shm", "-journal"):
                _unlink_quiet(target + suffix)
            try:
                os.replace(tmp, target)  # 同目录 rename，原子替换
            except OSError as exc:
                raise BackupError(f"原子替换失败：{exc}（原库未改动）") from exc
            try:
                _fsync_dir(data_dir)
            except OSError:
                pass
            return stats
        except BaseException:
            _unlink_quiet(tmp)
            raise
    finally:
        lock.release()


# ---------- 命令行入口 ----------

def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m app.backup",
        description="SQLite 数据卷的本地备份与恢复（备份可在服务运行中执行；恢复需先停止服务）。",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_backup = sub.add_parser("backup", help="在线备份数据库，生成 .db 与 .sha256 校验文件")
    p_backup.add_argument(
        "--dir",
        dest="dest_dir",
        default=None,
        help=f"备份输出目录（默认取 CALIB_BACKUP_DIR，未设置时为 {BACKUP_DIR}）",
    )

    p_restore = sub.add_parser("restore", help="校验并恢复备份（需先停止服务）")
    p_restore.add_argument("file", help="备份文件路径（同目录需存在 <文件>.sha256 校验文件）")

    args = parser.parse_args(argv)

    if args.command == "backup":
        try:
            final = create_backup(args.dest_dir)
        except BackupError as exc:
            print(f"备份失败：{exc}", file=sys.stderr)
            return 1
        except Exception as exc:  # 兜底：未预期的错误也以「失败原因」形式呈现
            print(f"备份失败：{exc}", file=sys.stderr)
            return 1
        print(f"备份成功：{final}（{final.stat().st_size} 字节）")
        print(f"SHA-256：{_sha256_file(final)}（校验文件 {final.name}.sha256）")
        return 0

    try:
        stats = restore_backup(args.file)
    except BackupError as exc:
        print(f"恢复失败：{exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # 兜底：未预期的错误也以「失败原因」形式呈现
        print(f"恢复失败：{exc}", file=sys.stderr)
        return 1
    print(f"恢复成功：{db.db_path()} 已替换为 {args.file}")
    print(
        f"会话 {stats['sessions']} 个，评分 {stats['scores']} 条，"
        f"裁决 {stats['decisions']} 条，批处理规则 {stats['batch_rules']} 条"
    )
    print("merge_previews 已清空：恢复前的合并预览标识全部失效，需重新预览")
    return 0


if __name__ == "__main__":
    sys.exit(main())
