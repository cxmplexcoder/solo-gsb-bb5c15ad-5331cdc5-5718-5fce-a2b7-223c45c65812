"""SQLite 数据卷的本地命令行备份与恢复工具。

用法（容器内执行）：

    python -m app.backup backup [-o 备份文件路径]
    python -m app.backup verify  备份文件路径
    python -m app.backup restore [--yes] 备份文件路径

设计要点：

- **备份**使用 SQLite 在线备份接口（``Connection.backup``），服务运行并
  持续写入时也能得到与某一时刻一致的完整快照（全部会话的评分、裁决、
  批处理规则、校准范围、合并预览均保留）；源库以只读方式打开。备份文件
  旁生成 ``<备份>.sha256`` 摘要文件（sha256sum 格式）。
- **恢复**依次校验：① SHA-256 摘要；② 数据库完整性（quick_check）；
  ③ 外键（foreign_key_check）；④ 当前表结构（与代码中的权威 schema
  逐表/列/索引/外键比对）。校验全部在原库之外、只读进行。
- 校验通过后，在数据目录内的**临时库**中完成准备（在线备份接口复制 +
  清空 ``merge_previews`` 使旧合并标识失效 + 复核完整性/外键），最后
  ``os.replace`` **原子替换**原库；任一步失败均删除临时文件、原库保持
  不变。
- 恢复与服务进程共用 ``app.locking`` 的跨进程互斥锁：服务存活时取锁
  失败立即拒绝；恢复持锁期间服务无法启动，杜绝「检查后服务启动」竞态。
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import os
import sqlite3
import sys
import tempfile
import urllib.parse
from datetime import datetime
from typing import Any

from . import database as dbmod
from .locking import LockUnavailableError, restore_lock

# 恢复后必须存在的表（即当前应用版本的全部用户表；merge_previews 随后被清空）。
REQUIRED_TABLES = ("sessions", "scores", "decisions", "batch_rules", "merge_previews")

SHA256_SUFFIX = ".sha256"
_SHA256_BUF = 1024 * 1024


class BackupError(RuntimeError):
    """备份/恢复过程中可向用户展示的失败原因。"""


# --------------------------------------------------------------------------- #
# SHA-256 摘要
# --------------------------------------------------------------------------- #

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_SHA256_BUF), b""):
            h.update(chunk)
    return h.hexdigest()


def write_checksum(backup_path: str) -> str:
    """为备份文件写入 sha256sum 格式的旁路摘要文件，返回摘要值。"""
    digest = sha256_file(backup_path)
    with open(backup_path + SHA256_SUFFIX, "w", encoding="ascii") as f:
        # sha256sum 格式：<64 位十六进制> *<文件名>，用 * 表示按二进制读取。
        f.write(f"{digest} *{os.path.basename(backup_path)}\n")
    return digest


def read_checksum_file(backup_path: str) -> str | None:
    """读取旁路摘要文件中的十六进制摘要；文件不存在时返回 None。"""
    sidecar = backup_path + SHA256_SUFFIX
    if not os.path.isfile(sidecar):
        return None
    with open(sidecar, "r", encoding="ascii") as f:
        line = f.readline().strip()
    if not line:
        return None
    digest = line.split()[0] if (" " in line or "*" in line) else line
    return digest.lower() or None


# --------------------------------------------------------------------------- #
# 数据库检查（只读）
# --------------------------------------------------------------------------- #

def _readonly_connect(path: str) -> sqlite3.Connection:
    uri = "file:" + urllib.parse.quote(os.path.abspath(path)) + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _user_tables(conn: sqlite3.Connection) -> list[str]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]


def _schema_signature(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """提取用户表结构签名：列定义、索引、外键（忽略内部表）。"""
    sig: dict[str, dict[str, Any]] = {}
    for table in _user_tables(conn):
        cols = []
        for _cid, name, ctype, notnull, dflt, pk in conn.execute(
            f"PRAGMA table_info({_ident(table)})"
        ).fetchall():
            cols.append(
                {
                    "name": name,
                    "type": (ctype or "").upper(),
                    "notnull": int(notnull),
                    "default": dflt,
                    "pk": int(pk),
                }
            )
        indexes = set()
        # 按列序取 name/unique/origin（不同 SQLite 版本尾部可能多出 partial 等列）。
        for row in conn.execute(f"PRAGMA index_list({_ident(table)})").fetchall():
            name, unique, origin = row[1], row[2], row[3]
            idx_cols = tuple(
                r[2]
                for r in conn.execute(f"PRAGMA index_info({_ident(name)})").fetchall()
            )
            indexes.add((str(name), int(unique), tuple(idx_cols), str(origin)))
        fks = set()
        for row in conn.execute(f"PRAGMA foreign_key_list({_ident(table)})").fetchall():
            # (id, seq, table, from, to, on_update, on_delete, match)
            fks.add((row[0], row[2], tuple(row[3]), tuple(row[4]), row[6], row[5]))
        sig[table] = {"columns": cols, "indexes": sorted(indexes), "fks": sorted(fks)}
    return sig


def _ident(name: str) -> str:
    """受信标识符（表名/索引名均来自 sqlite_master，非用户输入）。"""
    return '"' + name.replace('"', '""') + '"'


def _current_schema_signature() -> dict[str, dict[str, Any]]:
    """以代码中的权威 SCHEMA 在内存库里建表，得到期望结构签名。"""
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(dbmod.SCHEMA)
        return _schema_signature(conn)
    finally:
        conn.close()


def check_schema_compatible(conn: sqlite3.Connection) -> None:
    """校验连接上的库与当前应用表结构一致，否则抛出 BackupError。"""
    expected = _current_schema_signature()
    actual = _schema_signature(conn)

    missing_tables = sorted(set(expected) - set(actual))
    extra_tables = sorted(set(actual) - set(expected))
    problems: list[str] = []
    if missing_tables:
        problems.append(f"缺少表: {', '.join(missing_tables)}")
    if extra_tables:
        problems.append(f"存在当前版本无法识别的额外表: {', '.join(extra_tables)}")

    for table in sorted(set(expected) & set(actual)):
        exp_cols = {c["name"]: c for c in expected[table]["columns"]}
        act_cols = {c["name"]: c for c in actual[table]["columns"]}
        missing_cols = sorted(set(exp_cols) - set(act_cols))
        extra_cols = sorted(set(act_cols) - set(exp_cols))
        if missing_cols:
            problems.append(f"表 {table} 缺少列: {', '.join(missing_cols)}")
        if extra_cols:
            problems.append(f"表 {table} 存在多余列: {', '.join(extra_cols)}")
        for name in sorted(set(exp_cols) & set(act_cols)):
            if exp_cols[name] != act_cols[name]:
                problems.append(
                    f"表 {table} 列 {name} 定义不匹配: "
                    f"当前版本要求 {exp_cols[name]}，备份为 {act_cols[name]}"
                )
        if expected[table]["indexes"] != actual[table]["indexes"]:
            problems.append(f"表 {table} 的索引与当前版本不一致")
        if expected[table]["fks"] != actual[table]["fks"]:
            problems.append(f"表 {table} 的外键与当前版本不一致")

    if problems:
        raise BackupError("备份与当前表结构不兼容，已拒绝恢复：\n  - " + "\n  - ".join(problems))


def _check_integrity(conn: sqlite3.Connection, label: str) -> None:
    result = [r[0] for r in conn.execute("PRAGMA quick_check").fetchall()]
    if result != ["ok"]:
        detail = "; ".join(result[:5])
        raise BackupError(f"{label}数据库完整性检查失败: {detail}")


def _check_foreign_keys(conn: sqlite3.Connection, label: str) -> None:
    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        detail = ", ".join(f"表 {r[0]} 行 {r[1]}" for r in violations[:5])
        raise BackupError(f"{label}外键完整性检查失败（{len(violations)} 处）: {detail}")


def inspect_database(path: str, label: str = "备份") -> dict[str, int]:
    """对一份库文件做完整只读校验（完整性 + 外键 + 当前表结构）并返回行数统计。"""
    if not os.path.isfile(path):
        raise BackupError(f"数据库文件不存在: {path}")
    try:
        conn = _readonly_connect(path)
    except sqlite3.DatabaseError as exc:
        raise BackupError(f"{label}不是有效的 SQLite 数据库文件: {exc}") from exc
    try:
        _check_integrity(conn, label)
        _check_foreign_keys(conn, label)
        check_schema_compatible(conn)
        counts = {}
        for table in REQUIRED_TABLES:
            counts[table] = conn.execute(
                f"SELECT COUNT(*) FROM {_ident(table)}"
            ).fetchone()[0]
        return counts
    except sqlite3.DatabaseError as exc:
        raise BackupError(f"{label}数据库无法读取或已损坏: {exc}") from exc
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# backup
# --------------------------------------------------------------------------- #

def default_backup_dir() -> str:
    return os.environ.get("CALIB_BACKUP_DIR", "/backups")


def default_backup_path() -> str:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base = os.path.join(default_backup_dir(), f"calibration-{stamp}.db")
    if not os.path.exists(base) and not os.path.exists(base + SHA256_SUFFIX):
        return base
    # 同一秒内多次备份不覆盖：追加 -2、-3 … 后缀。
    idx = 2
    while True:
        candidate = os.path.join(
            default_backup_dir(), f"calibration-{stamp}-{idx}.db"
        )
        if not os.path.exists(candidate) and not os.path.exists(candidate + SHA256_SUFFIX):
            return candidate
        idx += 1


def run_backup(dest: str, src_path: str | None = None) -> tuple[str, str, dict[str, int]]:
    """在线备份数据库到 dest，写旁路摘要，并对产物做完整性自检。

    src_path 默认为当前 CALIB_DATA_DIR 下的库；恢复/测试可显式指定其他源库。
    """
    src = src_path or dbmod.db_path()
    if not os.path.isfile(src):
        raise BackupError(
            f"当前数据库不存在: {src}（请先启动一次服务完成初始化，或确认 CALIB_DATA_DIR 配置）"
        )

    dest = os.path.abspath(dest)
    dest_dir = os.path.dirname(dest) or "."
    os.makedirs(dest_dir, exist_ok=True)

    # 源库只读打开；在线备份接口在复制期间可容忍服务并发写入，
    # 复制结束即得到某一时刻的一致性快照。
    src_conn = _readonly_connect(src)
    out_conn = sqlite3.connect(dest)
    try:
        try:
            # pages<0：单次完成全部复制；内部在源库被写时自动重试，
            # 最终快照对服务并发写入保持一致性。
            src_conn.backup(out_conn, pages=-1)
        except sqlite3.DatabaseError as exc:
            raise BackupError(f"在线备份失败: {exc}") from exc
    except BaseException:
        # 失败时清理半成品（旁路摘要此时尚未生成）。
        with contextlib.suppress(OSError):
            os.unlink(dest)
        raise
    finally:
        out_conn.close()
        src_conn.close()

    digest = write_checksum(dest)
    counts = inspect_database(dest, label="生成的备份")
    return dest, digest, counts


# --------------------------------------------------------------------------- #
# verify
# --------------------------------------------------------------------------- #

def run_verify(backup_path: str) -> tuple[str, dict[str, int]]:
    """校验备份文件的摘要（若旁路摘要存在）、完整性、外键与表结构。"""
    if not os.path.isfile(backup_path):
        raise BackupError(f"备份文件不存在: {backup_path}")
    saved = read_checksum_file(backup_path)
    if saved is None:
        raise BackupError(
            f"未找到摘要文件 {os.path.basename(backup_path)}{SHA256_SUFFIX}，"
            "无法确认备份未被改动（请使用本工具生成的备份）"
        )
    actual = sha256_file(backup_path)
    if actual != saved:
        raise BackupError(
            f"SHA-256 校验失败：备份可能已损坏或被篡改\n"
            f"  摘要文件记录: {saved}\n"
            f"  文件实际摘要: {actual}"
        )
    counts = inspect_database(backup_path, label="备份")
    return actual, counts


# --------------------------------------------------------------------------- #
# restore
# --------------------------------------------------------------------------- #

def _checksum_only(backup_path: str) -> None:
    if not os.path.isfile(backup_path):
        raise BackupError(f"备份文件不存在: {backup_path}")
    saved = read_checksum_file(backup_path)
    if saved is None:
        raise BackupError(
            f"未找到摘要文件 {os.path.basename(backup_path)}{SHA256_SUFFIX}，"
            "无法确认备份未被改动（请使用本工具生成的备份）"
        )
    actual = sha256_file(backup_path)
    if actual != saved:
        raise BackupError(
            "SHA-256 校验失败：备份可能已损坏或被篡改\n"
            f"  摘要文件记录: {saved}\n"
            f"  文件实际摘要: {actual}"
        )


def run_restore(backup_path: str, target_data_dir: str | None = None) -> dict[str, Any]:
    """停服恢复：校验 → 互斥锁 → 临时库准备 → 原子替换原库。

    任何一步失败都抛出 BackupError，且原数据库文件保持不变。
    target_data_dir 默认为 CALIB_DATA_DIR；测试可指向隔离的临时目录，
    锁文件同样位于该目录内。
    """
    backup_path = os.path.abspath(backup_path)

    # 1) 先在不触碰原库、不取锁的情况下完成全部只读校验。
    _checksum_only(backup_path)
    inspect_database(backup_path, label="备份")

    restore_dir = target_data_dir or dbmod.data_dir()
    target = os.path.join(restore_dir, "calibration.db")
    os.makedirs(restore_dir, exist_ok=True)

    # 2) 取得与服务互斥的文件锁（非阻塞；服务存活立即拒绝），
    #    持锁直到原子替换结束，期间服务启动会被阻塞。
    with restore_lock(restore_dir):
        return _stage_and_replace(backup_path, restore_dir, target)


def _stage_and_replace(backup_path: str, data_dir: str, target: str) -> dict[str, Any]:
    # 3) 同目录临时库准备（与目标在同一文件系统，保证 rename 原子）。
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=".restore-", suffix=".db", dir=data_dir)
    os.close(tmp_fd)
    os.unlink(tmp_path)  # 交给 sqlite 创建，避免空文件残留
    replaced_done = False
    try:
        bak_conn = _readonly_connect(backup_path)
        tmp_conn = sqlite3.connect(tmp_path)
        try:
            try:
                bak_conn.backup(tmp_conn, pages=-1)
            except sqlite3.DatabaseError as exc:
                raise BackupError(f"恢复准备失败（无法复制备份到临时库）: {exc}") from exc

            # 恢复点之后旧的合并预览一次性标识一律失效。
            tmp_conn.execute("DELETE FROM merge_previews")
            tmp_conn.commit()

            # 4) 临时库（即将生效的库）再复核完整性与外键。
            _check_integrity(tmp_conn, "临时")
            _check_foreign_keys(tmp_conn, "临时")
        finally:
            tmp_conn.close()
            bak_conn.close()

        # 5) 检查同目录残留的 WAL/SHM/回滚日志，避免原子替换后被旧状态污染。
        wal = target + "-wal"
        shm = target + "-shm"
        journal = target + "-journal"
        if os.path.exists(wal) or os.path.exists(shm):
            raise BackupError(
                f"发现 {os.path.basename(target)} 的 WAL/SHM 文件，数据库可能处于"
                "WAL 模式或有进程正在使用。请确认服务已完全停止后重试。"
            )

        original_exists = os.path.exists(target)
        original_digest = sha256_file(target) if original_exists else None
        original_size = os.path.getsize(target) if original_exists else 0

        # 6) 原子替换（同目录 rename，POSIX 保证原子）。
        os.replace(tmp_path, target)
        replaced_done = True

        # 旧回滚日志属于被替换掉的旧库，必须一并移除（否则下次打开可能误回滚新库）。
        with contextlib.suppress(FileNotFoundError):
            os.unlink(journal)
        with contextlib.suppress(AttributeError):
            os.sync()
        _sync_dir(data_dir)

        counts = inspect_database(target, label="恢复后")
        return {
            "target": target,
            "backup": backup_path,
            "counts": counts,
            "replaced": original_exists,
            "previous_size": original_size,
            "previous_digest": original_digest,
        }
    finally:
        # 任何一步失败（含校验拒绝）都删除临时库；成功 rename 后文件已不存在。
        if not replaced_done and os.path.exists(tmp_path):
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            for suffix in ("-journal", "-wal", "-shm"):
                with contextlib.suppress(OSError):
                    os.unlink(tmp_path + suffix)


def _sync_dir(path: str) -> None:
    """同步目录项，保证 rename 落盘（Linux/macOS）。"""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def _format_counts(counts: dict[str, int]) -> str:
    labels = {
        "sessions": "会话",
        "scores": "评分",
        "decisions": "裁决",
        "batch_rules": "批处理规则",
        "merge_previews": "合并预览（恢复后应清空）",
    }
    return "，".join(f"{labels[k]} {counts[k]} 条" for k in REQUIRED_TABLES)


def _print_ok(msg: str) -> None:
    print(f"[成功] {msg}")


def _print_err(msg: str) -> None:
    print(f"[失败] {msg}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.backup",
        description="SQLite 数据卷备份与恢复工具（在线一致性备份 / 校验 / 停服原子恢复）",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_backup = sub.add_parser("backup", help="对当前数据库做在线一致性备份")
    p_backup.add_argument(
        "-o", "--output",
        help="备份文件输出路径（默认 $CALIB_BACKUP_DIR/calibration-时间戳.db，"
             "CALIB_BACKUP_DIR 默认 /backups）",
    )

    p_verify = sub.add_parser("verify", help="校验备份的 SHA-256、完整性、外键与表结构")
    p_verify.add_argument("backup", help="备份文件路径（同目录需有对应的 .sha256 摘要）")

    p_restore = sub.add_parser(
        "restore",
        help="停服恢复：校验通过后在临时库准备并原子替换当前数据库",
    )
    p_restore.add_argument("backup", help="备份文件路径（同目录需有对应的 .sha256 摘要）")
    p_restore.add_argument(
        "-y", "--yes", action="store_true",
        help="跳过确认提示（脚本/CI 使用）",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "backup":
            print(f"正在备份当前数据库 {dbmod.db_path()} …")
            path, digest, counts = run_backup(args.output or default_backup_path())
            _print_ok(
                f"在线一致性备份已生成：{path}\n"
                f"        SHA-256：{digest}\n"
                f"        摘要文件：{os.path.basename(path)}{SHA256_SUFFIX}\n"
                f"        内容统计：{_format_counts(counts)}"
            )
            return 0

        if args.command == "verify":
            digest, counts = run_verify(args.backup)
            _print_ok(
                f"备份校验通过：{args.backup}\n"
                f"        SHA-256：{digest}\n"
                f"        内容统计：{_format_counts(counts)}"
            )
            return 0

        if args.command == "restore":
            if not args.yes and sys.stdin.isatty():
                ans = input(
                    f"将用备份 {args.backup} 恢复 {dbmod.db_path()}（当前数据会被整体替换），"
                    "确认请输入 yes: "
                ).strip()
                if ans != "yes":
                    print("已取消恢复，原库未改动。")
                    return 1
            result = run_restore(args.backup)
            counts = result["counts"]
            replaced = "已原子替换原库" if result["replaced"] else "原库不存在，已新建"
            _print_ok(
                f"恢复完成（{replaced}）：{result['target']}\n"
                f"        备份来源：{result['backup']}\n"
                f"        内容统计：{_format_counts(counts)}\n"
                f"        merge_previews 已清空，旧合并预览标识全部失效。"
            )
            return 0

        return 2
    except BackupError as exc:
        _print_err(str(exc))
        return 1
    except LockUnavailableError as exc:
        _print_err(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
