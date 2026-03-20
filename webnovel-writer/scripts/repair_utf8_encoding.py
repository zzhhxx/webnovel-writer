#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
repair_utf8_encoding.py

批量修复项目内历史编码文件，统一回写为 UTF-8（无 BOM）。

目标：
1. 处理历史遗留 BOM / GBK / GB18030 / BIG5 文件
2. 统一编码，避免运行期崩溃或显示乱码
3. 提供 dry-run 与备份，便于安全落地
"""

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Iterable

from project_locator import resolve_project_root


TEXT_SUFFIXES = {".json", ".md", ".txt", ".yaml", ".yml", ".env", ".csv"}
SCAN_DIRS = [
    ".webnovel",
    "正文",
    "大纲",
    "设定集",
    "审查报告",
]
UTF8_BOM = b"\xef\xbb\xbf"


def _iter_default_targets(project_root: Path) -> Iterable[Path]:
    for rel in SCAN_DIRS:
        base = project_root / rel
        if not base.exists():
            continue
        if base.is_file():
            if base.suffix.lower() in TEXT_SUFFIXES:
                yield base
            continue
        for p in base.rglob("*"):
            if p.is_file() and p.suffix.lower() in TEXT_SUFFIXES:
                yield p


def _resolve_paths(project_root: Path, raw_paths: list[str]) -> list[Path]:
    resolved: list[Path] = []
    seen = set()
    for raw in raw_paths:
        p = Path(raw)
        if not p.is_absolute():
            p = project_root / p
        p = p.resolve()
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        resolved.append(p)
    return resolved


def _decode_text(raw: bytes) -> tuple[str, str, bool]:
    # BOM 文件：直接去 BOM 并判定需要修复
    if raw.startswith(UTF8_BOM):
        return raw.decode("utf-8-sig"), "utf-8-sig", True

    # 标准 UTF-8：无需修复
    try:
        return raw.decode("utf-8"), "utf-8", False
    except UnicodeDecodeError:
        pass

    # 历史本地编码：可解码则修复
    for enc in ("gb18030", "gbk", "big5"):
        try:
            return raw.decode(enc), enc, True
        except UnicodeDecodeError:
            continue

    raise UnicodeDecodeError("utf-8", raw, 0, max(1, len(raw)), "无法识别编码")


def _atomic_write_utf8(path: Path, text: str, backup: bool) -> None:
    temp_path: str | None = None
    fd, temp_path = tempfile.mkstemp(
        suffix=".tmp",
        prefix=path.stem + "_",
        dir=path.parent,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())

        if backup and path.exists():
            backup_path = path.with_suffix(path.suffix + ".encbak")
            if not backup_path.exists():
                shutil.copy2(path, backup_path)

        os.replace(temp_path, path)
        temp_path = None
    finally:
        if temp_path is not None:
            try:
                os.unlink(temp_path)
            except OSError:
                pass


def _repair_one(path: Path, *, dry_run: bool, backup: bool) -> tuple[str, str]:
    if not path.exists():
        return "skipped", f"missing: {path}"
    if not path.is_file():
        return "skipped", f"not-file: {path}"

    try:
        raw = path.read_bytes()
    except OSError as exc:
        return "failed", f"read-error: {path} ({exc})"

    if b"\x00" in raw:
        return "skipped", f"binary: {path}"

    try:
        text, source_encoding, needs_repair = _decode_text(raw)
    except UnicodeDecodeError:
        return "failed", f"decode-failed: {path}"

    if not needs_repair:
        return "ok", f"utf8: {path}"

    if dry_run:
        return "repaired", f"would-repair: {path} (source={source_encoding})"

    try:
        _atomic_write_utf8(path, text, backup=backup)
        return "repaired", f"repaired: {path} (source={source_encoding})"
    except Exception as exc:
        return "failed", f"repair-failed: {path} ({exc})"


def main() -> int:
    parser = argparse.ArgumentParser(description="批量修复历史编码文件并统一为 UTF-8")
    parser.add_argument("--project-root", type=str, default=".", help="项目根目录（可传工作区根）")
    parser.add_argument("--path", action="append", default=[], help="仅修复指定路径（可重复）")
    parser.add_argument("--dry-run", action="store_true", help="仅检测，不写入")
    parser.add_argument("--no-backup", action="store_true", help="修复时不生成 .encbak 备份")
    args = parser.parse_args()

    root = resolve_project_root(args.project_root)
    backup = not args.no_backup

    if args.path:
        targets = _resolve_paths(root, args.path)
    else:
        targets = list(_iter_default_targets(root))

    if not targets:
        print("没有找到可处理文件。")
        return 0

    stats = {"ok": 0, "repaired": 0, "skipped": 0, "failed": 0}
    for p in targets:
        status, msg = _repair_one(p, dry_run=args.dry_run, backup=backup)
        stats[status] += 1
        print(msg)

    print(
        f"\nsummary: ok={stats['ok']} repaired={stats['repaired']} "
        f"skipped={stats['skipped']} failed={stats['failed']}"
    )

    return 1 if stats["failed"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())

