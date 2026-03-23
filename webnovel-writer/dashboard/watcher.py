"""
Watchdog 文件变更监听器 + SSE 推送

监控 PROJECT_ROOT/.webnovel/ 目录下 state.json / index.db 等文件的写事件，
通过 SSE 通知所有已连接的前端客户端刷新数据。
"""

import asyncio
import json
import time
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class _WebnovelFileHandler(FileSystemEventHandler):
    """仅关注 .webnovel/ 目录下关键文件的修改/创建事件。"""

    WATCH_NAMES = {"state.json", "workflow_state.json"}
    INDEX_DB_PREFIX = "index.db"

    def __init__(self, notify_callback, watch_root: Path, extra_roots: list[Path] | None = None):
        super().__init__()
        self._notify = notify_callback
        self._watch_root = watch_root.resolve()
        self._reports_root = (watch_root / "reports").resolve()
        self._extra_roots = [
            Path(root).resolve()
            for root in (extra_roots or [])
        ]

    @staticmethod
    def _is_under(path: Path, root: Path) -> bool:
        try:
            path.resolve().relative_to(root.resolve())
            return True
        except ValueError:
            return False

    def _should_notify(self, file_path: Path) -> bool:
        name = file_path.name.lower()
        if name in self.WATCH_NAMES:
            return True
        # 不监听 index.db*：
        # - dashboard 读查询本身可能引发 wal/shm 抖动，形成“自触发刷新回路”；
        # - 前端改为基于 state/workflow 事件刷新，数据库明细在页面进入时按需拉取。
        if name == self.INDEX_DB_PREFIX or name.startswith(f"{self.INDEX_DB_PREFIX}-"):
            return False

        resolved = file_path.resolve()
        # 递归监听 reports 目录，覆盖审查报告/趋势报告等文件变更
        if self._is_under(resolved, self._reports_root):
            return True

        for root in self._extra_roots:
            if self._is_under(resolved, root):
                return True

        return False

    def on_modified(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if self._should_notify(path):
            self._notify(event.src_path, "modified")

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if self._should_notify(path):
            self._notify(event.src_path, "created")

    def on_moved(self, event):
        if event.is_directory:
            return
        dest = Path(getattr(event, "dest_path", "") or "")
        if dest and self._should_notify(dest):
            self._notify(str(dest), "moved")


class FileWatcher:
    """管理 watchdog Observer 和 SSE 客户端订阅。"""

    def __init__(self):
        self._observer: Observer | None = None
        self._subscribers: list[asyncio.Queue] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._last_dispatch_ts: float = 0.0
        self._dispatch_min_interval_sec: float = 1.5
        self._pending_msg: str | None = None
        self._pending_handle: asyncio.Handle | None = None

    # --- 订阅管理 ---

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    # --- 推送 ---

    def _on_change(self, path: str, kind: str):
        """在 watchdog 线程中调用，向主事件循环投递通知。"""
        msg = json.dumps({"file": Path(path).name, "kind": kind, "ts": time.time()})
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._dispatch, msg)

    def _dispatch(self, msg: str):
        now = time.time()
        elapsed = now - self._last_dispatch_ts
        if elapsed < self._dispatch_min_interval_sec:
            # 高频文件变更时合并消息，仅推送窗口内最后一条，避免前端连续重绘。
            self._pending_msg = msg
            if self._pending_handle is None and self._loop and not self._loop.is_closed():
                wait_sec = max(0.0, self._dispatch_min_interval_sec - elapsed)
                self._pending_handle = self._loop.call_later(wait_sec, self._flush_pending)
            return

        self._broadcast(msg)
        self._last_dispatch_ts = now

    def _flush_pending(self):
        self._pending_handle = None
        msg = self._pending_msg
        self._pending_msg = None
        if not msg:
            return
        self._broadcast(msg)
        self._last_dispatch_ts = time.time()

    def _broadcast(self, msg: str):
        for q in list(self._subscribers):
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                # 高频事件下保留订阅，丢弃最旧消息，保证前端至少收到最新变更。
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    q.put_nowait(msg)
                except asyncio.QueueFull:
                    # 极端并发下允许本次消息丢弃，但不移除订阅。
                    pass

    # --- 生命周期 ---

    def start(
        self,
        watch_dir: Path,
        loop: asyncio.AbstractEventLoop,
        extra_watch_dirs: list[Path] | None = None,
    ):
        """启动 watchdog observer，监听 watch_dir 以及可选扩展目录。"""
        self._loop = loop
        if self._observer:
            self.stop()

        watch_dir = watch_dir.resolve()
        extras = [Path(p).resolve() for p in (extra_watch_dirs or [])]
        handler = _WebnovelFileHandler(self._on_change, watch_dir, extras)
        self._observer = Observer()
        self._observer.schedule(handler, str(watch_dir), recursive=True)

        scheduled = {str(watch_dir)}
        for extra in extras:
            if not extra.is_dir():
                continue
            try:
                extra.relative_to(watch_dir)
                continue
            except ValueError:
                pass
            key = str(extra)
            if key in scheduled:
                continue
            self._observer.schedule(handler, key, recursive=True)
            scheduled.add(key)

        self._observer.daemon = True
        self._observer.start()

    def stop(self):
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=3)
            self._observer = None
        if self._pending_handle is not None:
            self._pending_handle.cancel()
            self._pending_handle = None
        self._pending_msg = None
        self._last_dispatch_ts = 0.0
