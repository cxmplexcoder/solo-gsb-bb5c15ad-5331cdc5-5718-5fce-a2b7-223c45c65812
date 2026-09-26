"""跨进程互斥锁：服务进程与停服恢复工具共用同一把文件锁。

锁文件固定放在数据目录（与数据库同卷），由内核维护的 ``flock`` 在所有
挂载该卷的容器之间生效，用于消除「恢复工具检查时服务未运行、检查后
服务恰好启动」的竞态：

- 服务启动时**先阻塞取得锁**，在整个存活期持有，停服时释放；
- 恢复工具以**非阻塞**方式取锁：服务存活（或另一个恢复在进行）时
  立即报错退出，根本不会触碰数据库文件；
- 恢复工具取锁成功后才在临时库上做准备并原子替换，期间任何服务
  启动都会阻塞在取锁上，绝不会打开替换到一半的数据库。

备份不改变数据库文件，无需持有该锁。
"""
from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from typing import Iterator

LOCK_FILENAME = "calibration.lock"


class LockUnavailableError(RuntimeError):
    """互斥锁已被占用（服务仍存活，或另一个恢复工具正在运行）。"""


def lock_path(data_dir: str) -> str:
    return os.path.join(data_dir, LOCK_FILENAME)


def _open_lock_file(data_dir: str) -> int:
    os.makedirs(data_dir, exist_ok=True)
    fd = os.open(lock_path(data_dir), os.O_RDWR | os.O_CREAT, 0o600)
    os.fchmod(fd, 0o600)
    return fd


def acquire_service_lock(data_dir: str) -> int:
    """服务启动时调用：阻塞取得互斥锁，返回须持有到停服的锁文件描述符。"""
    fd = _open_lock_file(data_dir)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


def release_service_lock(fd: int) -> None:
    """服务停止时释放锁。"""
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@contextmanager
def restore_lock(data_dir: str) -> Iterator[None]:
    """恢复工具专用：非阻塞取得互斥锁，占用期间拒绝一切服务启动。"""
    fd = _open_lock_file(data_dir)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise LockUnavailableError(
                "检测到服务仍在运行（互斥锁被占用），已拒绝恢复。"
                "请先执行 `docker compose stop calibration` 停止服务，"
                "再运行恢复命令。"
            )
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
