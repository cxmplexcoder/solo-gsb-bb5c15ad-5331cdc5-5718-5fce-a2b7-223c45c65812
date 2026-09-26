"""服务存活互斥锁：协调运行中的服务与离线恢复工具。

锁文件为数据目录下的 ``.service.lock``，使用 ``flock`` 排他锁（随文件描述符
关闭自动释放，进程退出即失效，不会残留「死锁」状态）：

- 服务在启动时抢锁并持有整个生命周期；抢不到说明已有实例在运行或恢复
  操作进行中，拒绝启动；
- 恢复工具以非阻塞方式抢同一把锁，抢不到说明服务存活，拒绝恢复；抢到后
  在整个恢复期间持有，此时服务无法启动，堵住「检查之后服务才启动」的竞态。
"""
from __future__ import annotations

import os

from . import database as db

try:  # POSIX（容器/Linux）使用 flock；其他平台（如 Windows 本地开发）退化为无锁。
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None

LOCK_FILE_NAME = ".service.lock"


def lock_path() -> str:
    return os.path.join(db.DEFAULT_DATA_DIR, LOCK_FILE_NAME)


class ServiceLock:
    """数据目录上的排他文件锁；同一时刻全系统只有一个持有者。"""

    def __init__(self) -> None:
        self._fh = None

    @property
    def held(self) -> bool:
        return self._fh is not None

    def acquire(self) -> bool:
        """非阻塞抢锁：成功返回 True 并保持持有，被他人持有返回 False。"""
        if self._fh is not None:
            return True
        if fcntl is None:  # 无 flock 的平台无法提供互斥保证，直接放行（仅本地开发场景）。
            return True
        os.makedirs(db.DEFAULT_DATA_DIR, exist_ok=True)
        fh = open(lock_path(), "a+", encoding="utf-8")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        self._fh = fh
        return True

    def release(self) -> None:
        if self._fh is not None:
            self._fh.close()  # 关闭文件描述符即释放 flock
            self._fh = None

    def __enter__(self) -> "ServiceLock":
        self.acquire()
        return self

    def __exit__(self, *_exc) -> None:
        self.release()
