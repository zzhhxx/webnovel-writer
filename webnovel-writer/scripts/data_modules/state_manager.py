#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
State Manager - 状态管理模块 (v5.4)

管理 state.json 的读写操作：
- 实体状态管理
- 进度追踪
- 关系记录

v5.1 变更（v5.4 沿用）:
- 集成 SQLStateManager，同步写入 SQLite (index.db)
- state.json 保留精简数据，大数据自动迁移到 SQLite
"""

import json
import logging
import re
import sqlite3
import sys
import time
from copy import deepcopy
from pathlib import Path

from runtime_compat import enable_windows_utf8_stdio
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
import filelock

from .config import get_config
from .observability import safe_append_perf_timing, safe_log_tool_call


logger = logging.getLogger(__name__)

try:
    # 当 scripts 目录在 sys.path 中（常见：从 scripts/ 运行）
    from security_utils import atomic_write_json, read_json_safe, read_text_safe
except ImportError:  # pragma: no cover
    # 当以 `python -m scripts.data_modules...` 从仓库根目录运行
    from scripts.security_utils import atomic_write_json, read_json_safe, read_text_safe


@dataclass
class EntityState:
    """实体状态"""
    id: str
    name: str
    type: str  # 角色/地点/物品/势力
    tier: str = "装饰"  # 核心/重要/次要/装饰
    aliases: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    first_appearance: int = 0
    last_appearance: int = 0


@dataclass
class Relationship:
    """实体关系"""
    from_entity: str
    to_entity: str
    type: str
    description: str
    chapter: int


@dataclass
class StateChange:
    """状态变化记录"""
    entity_id: str
    field: str
    old_value: Any
    new_value: Any
    reason: str
    chapter: int
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class _EntityPatch:
    """待写入的实体增量补丁（用于锁内合并）"""
    entity_type: str
    entity_id: str
    replace: bool = False
    base_entity: Optional[Dict[str, Any]] = None  # 新建实体时的完整快照（用于填充缺失字段）
    top_updates: Dict[str, Any] = field(default_factory=dict)
    current_updates: Dict[str, Any] = field(default_factory=dict)
    appearance_chapter: Optional[int] = None


class StateManager:
    """状态管理器（v5.1 entities_v3 格式 + SQLite 同步，v5.4 沿用）"""

    # v5.0 引入的实体类型
    ENTITY_TYPES = ["角色", "地点", "物品", "势力", "招式"]
    SQLITE_RETRY_KEY = "_sqlite_sync_pending"

    def __init__(self, config=None, enable_sqlite_sync: bool = True):
        """
        初始化状态管理器

        参数:
        - config: 配置对象
        - enable_sqlite_sync: 是否启用 SQLite 同步 (默认 True)
        """
        self.config = config or get_config()
        self._state: Dict[str, Any] = {}
        # 与 security_utils.atomic_write_json 保持一致：state.json.lock
        self._lock_path = self.config.state_file.with_suffix(self.config.state_file.suffix + ".lock")

        # v5.1 引入: SQLite 同步
        self._enable_sqlite_sync = enable_sqlite_sync
        self._sql_state_manager = None
        if enable_sqlite_sync:
            try:
                from .sql_state_manager import SQLStateManager
                self._sql_state_manager = SQLStateManager(self.config)
            except ImportError:
                pass  # SQLStateManager 不可用时静默降级

        # 待写入的增量（锁内重读 + 合并 + 写入）
        self._pending_entity_patches: Dict[tuple[str, str], _EntityPatch] = {}
        self._pending_alias_entries: Dict[str, List[Dict[str, str]]] = {}
        self._pending_state_changes: List[Dict[str, Any]] = []
        self._pending_structured_relationships: List[Dict[str, Any]] = []
        self._pending_disambiguation_warnings: List[Dict[str, Any]] = []
        self._pending_disambiguation_pending: List[Dict[str, Any]] = []
        self._pending_progress_chapter: Optional[int] = None
        self._pending_progress_words_delta: int = 0
        self._pending_chapter_meta: Dict[str, Any] = {}

        # v5.1 引入: 缓存待同步到 SQLite 的数据
        self._pending_sqlite_data: Dict[str, Any] = {
            "entities_appeared": [],
            "entities_new": [],
            "state_changes": [],
            "relationships_new": [],
            "chapter": None
        }

        self._load_state()

    def _now_progress_timestamp(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _ensure_state_schema(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """确保 state.json 具备运行所需的关键字段（尽量不破坏既有数据）。"""
        if not isinstance(state, dict):
            state = {}

        state.setdefault("project_info", {})
        state.setdefault("progress", {})
        state.setdefault("protagonist_state", {})

        # relationships: 旧版本可能是 list（实体关系），v5.0 运行态用 dict（人物关系/重要关系）
        relationships = state.get("relationships")
        if isinstance(relationships, list):
            state.setdefault("structured_relationships", [])
            if isinstance(state.get("structured_relationships"), list):
                state["structured_relationships"].extend(relationships)
            state["relationships"] = {}
        elif not isinstance(relationships, dict):
            state["relationships"] = {}

        state.setdefault("world_settings", {"power_system": [], "factions": [], "locations": []})
        state.setdefault("plot_threads", {"active_threads": [], "foreshadowing": []})
        state.setdefault("review_checkpoints", [])
        state.setdefault("chapter_meta", {})
        state.setdefault(
            "strand_tracker",
            {
                "last_quest_chapter": 0,
                "last_fire_chapter": 0,
                "last_constellation_chapter": 0,
                "current_dominant": "quest",
                "chapters_since_switch": 0,
                "history": [],
            },
        )

        entities_v3 = state.get("entities_v3")
        # v5.1 引入: entities_v3, alias_index, state_changes, structured_relationships 已迁移到 index.db
        # 不再在 state.json 中初始化或维护这些字段

        if not isinstance(state.get("disambiguation_warnings"), list):
            state["disambiguation_warnings"] = []

        if not isinstance(state.get("disambiguation_pending"), list):
            state["disambiguation_pending"] = []

        # progress 基础字段
        progress = state["progress"]
        if not isinstance(progress, dict):
            progress = {}
            state["progress"] = progress
        progress.setdefault("current_chapter", 0)
        progress.setdefault("total_words", 0)
        progress.setdefault("last_updated", self._now_progress_timestamp())

        return state

    def _load_state(self):
        """加载状态文件"""
        if self.config.state_file.exists():
            self._state = read_json_safe(self.config.state_file, default={})
            self._state = self._ensure_state_schema(self._state)
        else:
            self._state = self._ensure_state_schema({})

    def save_state(self):
        """
        保存状态文件（锁内重读 + 合并 + 原子写入）。

        解决多 Agent 并行下的“读-改-写覆盖”风险：
        - 获取锁
        - 重新读取磁盘最新 state.json
        - 仅合并本实例产生的增量（pending_*）
        - 原子化写入
        """
        # 无增量时不写入，避免无意义覆盖
        has_pending = any(
            [
                self._pending_entity_patches,
                self._pending_alias_entries,
                self._pending_state_changes,
                self._pending_structured_relationships,
                self._pending_disambiguation_warnings,
                self._pending_disambiguation_pending,
                self._pending_chapter_meta,
                self._pending_progress_chapter is not None,
                self._pending_progress_words_delta != 0,
            ]
        )
        has_retry_queue = isinstance(self._state.get(self.SQLITE_RETRY_KEY), list) and bool(
            self._state.get(self.SQLITE_RETRY_KEY)
        )
        if not has_pending and not has_retry_queue:
            return

        self.config.ensure_dirs()

        lock = filelock.FileLock(str(self._lock_path), timeout=10)
        try:
            with lock:
                disk_state = read_json_safe(self.config.state_file, default={})
                disk_state = self._ensure_state_schema(disk_state)
                retry_entries = disk_state.get(self.SQLITE_RETRY_KEY, [])
                if isinstance(retry_entries, list):
                    for item in retry_entries:
                        if not isinstance(item, dict):
                            continue
                        retry_payload = item.get("pending")
                        if not isinstance(retry_payload, dict):
                            continue
                        snapshot = self._deserialize_sqlite_pending_snapshot(retry_payload)
                        self._merge_sqlite_pending_snapshot(snapshot)
                # 本次写盘先移除旧重试队列；若仍失败会写入新的合并队列
                disk_state.pop(self.SQLITE_RETRY_KEY, None)

                # progress（合并为 max(chapter) + words_delta 累加）
                if self._pending_progress_chapter is not None or self._pending_progress_words_delta != 0:
                    progress = disk_state.get("progress", {})
                    if not isinstance(progress, dict):
                        progress = {}
                        disk_state["progress"] = progress

                    try:
                        current_chapter = int(progress.get("current_chapter", 0) or 0)
                    except (TypeError, ValueError):
                        current_chapter = 0

                    if self._pending_progress_chapter is not None:
                        progress["current_chapter"] = max(current_chapter, int(self._pending_progress_chapter))

                    if self._pending_progress_words_delta:
                        try:
                            total_words = int(progress.get("total_words", 0) or 0)
                        except (TypeError, ValueError):
                            total_words = 0
                        progress["total_words"] = total_words + int(self._pending_progress_words_delta)

                    progress["last_updated"] = self._now_progress_timestamp()

                # v5.1 引入: 强制使用 SQLite 模式，移除大数据字段
                # 确保 state.json 中不存在这些膨胀字段
                for field in ["entities_v3", "alias_index", "state_changes", "structured_relationships"]:
                    disk_state.pop(field, None)
                # 标记已迁移
                disk_state["_migrated_to_sqlite"] = True

                # disambiguation_warnings（追加去重 + 截断）
                if self._pending_disambiguation_warnings:
                    warnings_list = disk_state.get("disambiguation_warnings")
                    if not isinstance(warnings_list, list):
                        warnings_list = []
                        disk_state["disambiguation_warnings"] = warnings_list

                    def _warn_key(w: Dict[str, Any]) -> tuple:
                        return (
                            w.get("chapter"),
                            w.get("mention"),
                            w.get("chosen_id"),
                            w.get("confidence"),
                        )

                    existing_keys = {_warn_key(w) for w in warnings_list if isinstance(w, dict)}
                    for w in self._pending_disambiguation_warnings:
                        if not isinstance(w, dict):
                            continue
                        k = _warn_key(w)
                        if k in existing_keys:
                            continue
                        warnings_list.append(w)
                        existing_keys.add(k)

                    # 只保留最近 N 条，避免文件无限增长
                    max_keep = self.config.max_disambiguation_warnings
                    if len(warnings_list) > max_keep:
                        disk_state["disambiguation_warnings"] = warnings_list[-max_keep:]

                # disambiguation_pending（追加去重 + 截断）
                if self._pending_disambiguation_pending:
                    pending_list = disk_state.get("disambiguation_pending")
                    if not isinstance(pending_list, list):
                        pending_list = []
                        disk_state["disambiguation_pending"] = pending_list

                    def _pending_key(w: Dict[str, Any]) -> tuple:
                        return (
                            w.get("chapter"),
                            w.get("mention"),
                            w.get("suggested_id"),
                            w.get("confidence"),
                        )

                    existing_keys = {_pending_key(w) for w in pending_list if isinstance(w, dict)}
                    for w in self._pending_disambiguation_pending:
                        if not isinstance(w, dict):
                            continue
                        k = _pending_key(w)
                        if k in existing_keys:
                            continue
                        pending_list.append(w)
                        existing_keys.add(k)

                    max_keep = self.config.max_disambiguation_pending
                    if len(pending_list) > max_keep:
                        disk_state["disambiguation_pending"] = pending_list[-max_keep:]

                # chapter_meta（新增：按章节号覆盖写入）
                if self._pending_chapter_meta:
                    chapter_meta = disk_state.get("chapter_meta")
                    if not isinstance(chapter_meta, dict):
                        chapter_meta = {}
                        disk_state["chapter_meta"] = chapter_meta
                    chapter_meta.update(self._pending_chapter_meta)

                # 原子写入（锁已持有，不再二次加锁）
                atomic_write_json(self.config.state_file, disk_state, use_lock=False, backup=True)

                # v5.1 引入: 同步到 SQLite（失败时保留 pending 以便重试）
                sqlite_pending_snapshot = self._snapshot_sqlite_pending()
                sqlite_sync_ok = self._sync_to_sqlite()

                # 同步内存为磁盘最新快照
                self._state = disk_state

                # state.json 侧 pending 已写盘，直接清空
                self._pending_disambiguation_warnings.clear()
                self._pending_disambiguation_pending.clear()
                self._pending_chapter_meta.clear()
                self._pending_progress_chapter = None
                self._pending_progress_words_delta = 0

                # SQLite 侧 pending：成功后清空，失败则恢复快照（避免静默丢数据）
                if sqlite_sync_ok:
                    self._pending_entity_patches.clear()
                    self._pending_alias_entries.clear()
                    self._pending_state_changes.clear()
                    self._pending_structured_relationships.clear()
                    self._clear_pending_sqlite_data()
                else:
                    self._restore_sqlite_pending(sqlite_pending_snapshot)
                    self._persist_sqlite_retry_queue(
                        disk_state,
                        sqlite_pending_snapshot,
                        "SQLite sync failed in save_state",
                    )
                    raise RuntimeError(
                        "SQLite 同步失败：本次待写数据已持久化到 _sqlite_sync_pending，请重试同一命令。"
                    )

        except filelock.Timeout:
            raise RuntimeError("无法获取 state.json 文件锁，请稍后重试")

    @staticmethod
    def _normalize_state_change_key(change: Dict[str, Any], default_chapter: int = 0) -> tuple[str, str, str, str, int]:
        """生成状态变化去重键（用于避免双路径写入重复）。"""
        entity_id = str(change.get("entity_id", "") or "").strip()
        field = str(change.get("field", "") or "").strip()
        old_value = str(change.get("old", change.get("old_value", "")) or "")
        new_value = str(change.get("new", change.get("new_value", "")) or "")
        chapter_raw = change.get("chapter", default_chapter)
        try:
            chapter = int(chapter_raw or 0)
        except (TypeError, ValueError):
            chapter = int(default_chapter or 0)
        return (entity_id, field, old_value, new_value, chapter)

    @staticmethod
    def _normalize_relationship_key(rel: Dict[str, Any], default_chapter: int = 0) -> tuple[str, str, str, int]:
        """生成关系去重键（用于避免双路径重复 upsert）。"""
        from_entity = str(rel.get("from", rel.get("from_entity", "")) or "").strip()
        to_entity = str(rel.get("to", rel.get("to_entity", "")) or "").strip()
        rel_type = str(rel.get("type", "") or "").strip()
        chapter_raw = rel.get("chapter", default_chapter)
        try:
            chapter = int(chapter_raw or 0)
        except (TypeError, ValueError):
            chapter = int(default_chapter or 0)
        return (from_entity, to_entity, rel_type, chapter)

    def _sync_to_sqlite(self) -> bool:
        """同步待处理数据到 SQLite（v5.1 引入，v5.4 沿用）"""
        if not self._sql_state_manager:
            return True

        # 方式1: 通过 process_chapter_result 收集的数据
        sqlite_data = self._pending_sqlite_data
        chapter = sqlite_data.get("chapter")

        # 记录已处理的 (entity_id, chapter) 组合，避免重复写入 appearances
        processed_appearances = set()
        # 记录已处理的状态变化/关系，避免 process_chapter_entities + pending 双写
        processed_state_changes = set()
        processed_relationships = set()

        if chapter is not None:
            try:
                self._sql_state_manager.process_chapter_entities(
                    chapter=chapter,
                    entities_appeared=sqlite_data.get("entities_appeared", []),
                    entities_new=sqlite_data.get("entities_new", []),
                    state_changes=sqlite_data.get("state_changes", []),
                    relationships_new=sqlite_data.get("relationships_new", [])
                )
                # 仅将“已真实写入 DB”的记录加入去重集合，避免因前置实体未落库导致数据被误跳过。
                persisted_appearance_keys: set[tuple[str, int]] = set()
                persisted_state_change_keys: set[tuple[str, str, str, str, int]] = set()
                persisted_relationship_keys: set[tuple[str, str, str, int]] = set()
                try:
                    with self._sql_state_manager._index_manager._get_conn() as conn:
                        cursor = conn.cursor()
                        for row in cursor.execute(
                            "SELECT entity_id FROM appearances WHERE chapter = ?",
                            (int(chapter),),
                        ).fetchall():
                            entity_id = str(row[0] or "").strip()
                            if entity_id:
                                persisted_appearance_keys.add((entity_id, int(chapter)))

                        for row in cursor.execute(
                            """
                            SELECT entity_id, field, old_value, new_value, chapter
                            FROM state_changes
                            WHERE chapter = ?
                            """,
                            (int(chapter),),
                        ).fetchall():
                            persisted_state_change_keys.add(
                                (
                                    str(row[0] or "").strip(),
                                    str(row[1] or "").strip(),
                                    str(row[2] or ""),
                                    str(row[3] or ""),
                                    self._to_int(row[4], default=int(chapter)),
                                )
                            )

                        for row in cursor.execute(
                            """
                            SELECT from_entity, to_entity, type, chapter
                            FROM relationships
                            WHERE chapter = ?
                            """,
                            (int(chapter),),
                        ).fetchall():
                            persisted_relationship_keys.add(
                                (
                                    str(row[0] or "").strip(),
                                    str(row[1] or "").strip(),
                                    str(row[2] or "").strip(),
                                    self._to_int(row[3], default=int(chapter)),
                                )
                            )
                except Exception:
                    # 查询失败时宁可不去重，也不冒数据丢失风险
                    persisted_appearance_keys = set()
                    persisted_state_change_keys = set()
                    persisted_relationship_keys = set()

                for entity in sqlite_data.get("entities_appeared", []):
                    entity_id = str(entity.get("id", "") or "").strip()
                    key = (entity_id, int(chapter))
                    if entity_id and key in persisted_appearance_keys:
                        processed_appearances.add(key)
                for entity in sqlite_data.get("entities_new", []):
                    entity_id = str(entity.get("suggested_id") or entity.get("id") or "").strip()
                    key = (entity_id, int(chapter))
                    if entity_id and key in persisted_appearance_keys:
                        processed_appearances.add(key)

                for change in sqlite_data.get("state_changes", []):
                    if not isinstance(change, dict):
                        continue
                    key = self._normalize_state_change_key(change, default_chapter=int(chapter))
                    if key in persisted_state_change_keys:
                        processed_state_changes.add(key)

                for rel in sqlite_data.get("relationships_new", []):
                    if not isinstance(rel, dict):
                        continue
                    key = self._normalize_relationship_key(rel, default_chapter=int(chapter))
                    if key in persisted_relationship_keys:
                        processed_relationships.add(key)
            except Exception as exc:
                logger.warning("SQLite sync failed (process_chapter_entities): %s", exc)
                return False

        # 方式2: 使用 add_entity/update_entity 收集的增量数据。
        # 数据缓存在 _pending_entity_patches 等变量中。
        return self._sync_pending_patches_to_sqlite(
            processed_appearances=processed_appearances,
            processed_state_changes=processed_state_changes,
            processed_relationships=processed_relationships,
        )

    def _sync_pending_patches_to_sqlite(
        self,
        processed_appearances: set = None,
        processed_state_changes: set = None,
        processed_relationships: set = None,
    ) -> bool:
        """同步 _pending_entity_patches 等到 SQLite（v5.1 引入，v5.4 沿用）

        Args:
            processed_appearances: 已通过 process_chapter_entities 处理的 (entity_id, chapter) 集合，
                                   用于避免重复写入 appearances 表（防止覆盖 mentions）
        """
        if not self._sql_state_manager:
            return True

        if processed_appearances is None:
            processed_appearances = set()
        if processed_state_changes is None:
            processed_state_changes = set()
        if processed_relationships is None:
            processed_relationships = set()

        # 元数据字段（不应写入 current_json）
        METADATA_FIELDS = {"canonical_name", "tier", "desc", "is_protagonist", "is_archived"}

        try:
            from .sql_state_manager import EntityData
            from .index_manager import EntityMeta, RelationshipEventMeta

            # 同步实体补丁
            for (entity_type, entity_id), patch in self._pending_entity_patches.items():
                if patch.base_entity:
                    # 新实体
                    entity_data = EntityData(
                        id=entity_id,
                        type=entity_type,
                        name=patch.base_entity.get("canonical_name", entity_id),
                        tier=patch.base_entity.get("tier", "装饰"),
                        desc=patch.base_entity.get("desc", ""),
                        current=patch.base_entity.get("current", {}),
                        aliases=[],
                        first_appearance=patch.base_entity.get("first_appearance", 0),
                        last_appearance=patch.base_entity.get("last_appearance", 0),
                        is_protagonist=patch.base_entity.get("is_protagonist", False)
                    )
                    self._sql_state_manager.upsert_entity(entity_data)

                    # 记录首次出场（跳过已处理的，避免覆盖 mentions）
                    if patch.appearance_chapter is not None:
                        if (entity_id, patch.appearance_chapter) not in processed_appearances:
                            self._sql_state_manager._index_manager.record_appearance(
                                entity_id=entity_id,
                                chapter=patch.appearance_chapter,
                                mentions=[entity_data.name],
                                confidence=1.0,
                                skip_if_exists=True  # 关键：不覆盖已有记录
                            )
                else:
                    # 更新现有实体
                    has_metadata_updates = bool(patch.top_updates and
                                                 any(k in METADATA_FIELDS for k in patch.top_updates))

                    # 非元数据的 top_updates 应该当作 current 更新
                    # 例如：realm, layer, location 等状态字段
                    non_metadata_top_updates = {
                        k: v for k, v in patch.top_updates.items()
                        if k not in METADATA_FIELDS
                    } if patch.top_updates else {}

                    # 合并 current_updates 和非元数据的 top_updates
                    effective_current_updates = {**non_metadata_top_updates}
                    if patch.current_updates:
                        effective_current_updates.update(patch.current_updates)

                    if has_metadata_updates:
                        # 有元数据更新：使用 upsert_entity(update_metadata=True)
                        existing = self._sql_state_manager.get_entity(entity_id)
                        if existing:
                            # 合并 current
                            current = existing.get("current_json", {})
                            if isinstance(current, str):
                                import json
                                current = json.loads(current) if current else {}
                            if effective_current_updates:
                                current.update(effective_current_updates)

                            new_canonical_name = patch.top_updates.get("canonical_name")
                            old_canonical_name = existing.get("canonical_name", "")

                            entity_meta = EntityMeta(
                                id=entity_id,
                                type=existing.get("type", entity_type),
                                canonical_name=new_canonical_name or old_canonical_name,
                                tier=patch.top_updates.get("tier", existing.get("tier", "装饰")),
                                desc=patch.top_updates.get("desc", existing.get("desc", "")),
                                current=current,
                                first_appearance=existing.get("first_appearance", 0),
                                last_appearance=patch.appearance_chapter or existing.get("last_appearance", 0),
                                is_protagonist=patch.top_updates.get("is_protagonist", existing.get("is_protagonist", False)),
                                is_archived=patch.top_updates.get("is_archived", existing.get("is_archived", False))
                            )
                            self._sql_state_manager._index_manager.upsert_entity(entity_meta, update_metadata=True)

                            # 如果 canonical_name 改名，自动注册新名字为 alias
                            if new_canonical_name and new_canonical_name != old_canonical_name:
                                self._sql_state_manager.register_alias(
                                    new_canonical_name, entity_id, existing.get("type", entity_type)
                                )
                    elif effective_current_updates:
                        # 只有 current 更新（包括非元数据的 top_updates）
                        self._sql_state_manager.update_entity_current(entity_id, effective_current_updates)

                    # 更新 last_appearance 并记录出场
                    if patch.appearance_chapter is not None:
                        self._sql_state_manager._update_last_appearance(entity_id, patch.appearance_chapter)
                        # 补充 appearances 记录
                        # 使用 skip_if_exists=True 避免覆盖已有记录的 mentions
                        if (entity_id, patch.appearance_chapter) not in processed_appearances:
                            self._sql_state_manager._index_manager.record_appearance(
                                entity_id=entity_id,
                                chapter=patch.appearance_chapter,
                                mentions=[],
                                confidence=1.0,
                                skip_if_exists=True  # 关键：不覆盖已有记录
                            )

            # 同步别名
            for alias, entries in self._pending_alias_entries.items():
                for entry in entries:
                    entity_type = entry.get("type")
                    entity_id = entry.get("id")
                    if entity_type and entity_id:
                        self._sql_state_manager.register_alias(alias, entity_id, entity_type)

            # 同步状态变化
            for change in self._pending_state_changes:
                if not isinstance(change, dict):
                    continue
                if self._normalize_state_change_key(change) in processed_state_changes:
                    continue
                self._sql_state_manager.record_state_change(
                    entity_id=change.get("entity_id", ""),
                    field=change.get("field", ""),
                    old_value=change.get("old", change.get("old_value", "")),
                    new_value=change.get("new", change.get("new_value", "")),
                    reason=change.get("reason", ""),
                    chapter=change.get("chapter", 0)
                )

            # 同步关系
            for rel in self._pending_structured_relationships:
                if not isinstance(rel, dict):
                    continue
                if self._normalize_relationship_key(rel) in processed_relationships:
                    continue
                self._sql_state_manager.upsert_relationship(
                    from_entity=rel.get("from_entity", ""),
                    to_entity=rel.get("to_entity", ""),
                    type=rel.get("type", "相识"),
                    description=rel.get("description", ""),
                    chapter=rel.get("chapter", 0)
                )
                # pending 补偿路径也记录关系事件，避免 relationship_events 缺失
                self._sql_state_manager._index_manager.record_relationship_event(
                    RelationshipEventMeta(
                        from_entity=str(rel.get("from_entity", "") or ""),
                        to_entity=str(rel.get("to_entity", "") or ""),
                        type=str(rel.get("type", "相识") or "相识"),
                        chapter=self._to_int(rel.get("chapter", 0), default=0),
                        action=str(rel.get("action", "update") or "update"),
                        polarity=self._to_int(rel.get("polarity", 0), default=0),
                        strength=self._to_float(rel.get("strength", 0.5), default=0.5),
                        description=str(rel.get("description", "") or ""),
                        scene_index=self._to_int(rel.get("scene_index", 0), default=0),
                        evidence=str(rel.get("evidence", "") or ""),
                        confidence=self._to_float(rel.get("confidence", 1.0), default=1.0),
                    )
                )

            return True

        except Exception as e:
            # SQLite 同步失败时记录警告，由 save_state 统一决定回滚与抛错
            logger.warning("SQLite sync failed: %s", e)
            return False

    def _snapshot_sqlite_pending(self) -> Dict[str, Any]:
        """抓取 SQLite 侧 pending 快照，用于同步失败回滚内存队列。"""
        return {
            "entity_patches": deepcopy(self._pending_entity_patches),
            "alias_entries": deepcopy(self._pending_alias_entries),
            "state_changes": deepcopy(self._pending_state_changes),
            "structured_relationships": deepcopy(self._pending_structured_relationships),
            "sqlite_data": deepcopy(self._pending_sqlite_data),
        }

    def _restore_sqlite_pending(self, snapshot: Dict[str, Any]) -> None:
        """恢复 SQLite 侧 pending 快照，避免同步失败后数据静默丢失。"""
        self._pending_entity_patches = snapshot.get("entity_patches", {})
        self._pending_alias_entries = snapshot.get("alias_entries", {})
        self._pending_state_changes = snapshot.get("state_changes", [])
        self._pending_structured_relationships = snapshot.get("structured_relationships", [])
        self._pending_sqlite_data = snapshot.get("sqlite_data", {
            "entities_appeared": [],
            "entities_new": [],
            "state_changes": [],
            "relationships_new": [],
            "chapter": None,
        })

    def _serialize_sqlite_pending_snapshot(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """将 pending 快照转为可写入 state.json 的结构。"""
        serialized_patches: List[Dict[str, Any]] = []
        for (entity_type, entity_id), patch in snapshot.get("entity_patches", {}).items():
            if not isinstance(patch, _EntityPatch):
                continue
            serialized_patches.append(
                {
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "replace": bool(patch.replace),
                    "base_entity": deepcopy(patch.base_entity),
                    "top_updates": deepcopy(patch.top_updates),
                    "current_updates": deepcopy(patch.current_updates),
                    "appearance_chapter": patch.appearance_chapter,
                }
            )

        return {
            "entity_patches": serialized_patches,
            "alias_entries": deepcopy(snapshot.get("alias_entries", {})),
            "state_changes": deepcopy(snapshot.get("state_changes", [])),
            "structured_relationships": deepcopy(snapshot.get("structured_relationships", [])),
            "sqlite_data": deepcopy(
                snapshot.get(
                    "sqlite_data",
                    {
                        "entities_appeared": [],
                        "entities_new": [],
                        "state_changes": [],
                        "relationships_new": [],
                        "chapter": None,
                    },
                )
            ),
        }

    def _deserialize_sqlite_pending_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """将 state.json 中的重试结构恢复为内存 pending 快照。"""
        entity_patches: Dict[tuple[str, str], _EntityPatch] = {}
        for item in payload.get("entity_patches", []) or []:
            if not isinstance(item, dict):
                continue
            entity_type = str(item.get("entity_type", "") or "").strip()
            entity_id = str(item.get("entity_id", "") or "").strip()
            if not entity_type or not entity_id:
                continue
            entity_patches[(entity_type, entity_id)] = _EntityPatch(
                entity_type=entity_type,
                entity_id=entity_id,
                replace=bool(item.get("replace", False)),
                base_entity=deepcopy(item.get("base_entity")),
                top_updates=deepcopy(item.get("top_updates", {})) if isinstance(item.get("top_updates"), dict) else {},
                current_updates=deepcopy(item.get("current_updates", {}))
                if isinstance(item.get("current_updates"), dict)
                else {},
                appearance_chapter=item.get("appearance_chapter"),
            )

        alias_entries = payload.get("alias_entries", {})
        if not isinstance(alias_entries, dict):
            alias_entries = {}

        state_changes = payload.get("state_changes", [])
        if not isinstance(state_changes, list):
            state_changes = []

        structured_relationships = payload.get("structured_relationships", [])
        if not isinstance(structured_relationships, list):
            structured_relationships = []

        sqlite_data = payload.get("sqlite_data", {})
        if not isinstance(sqlite_data, dict):
            sqlite_data = {}

        return {
            "entity_patches": entity_patches,
            "alias_entries": deepcopy(alias_entries),
            "state_changes": deepcopy(state_changes),
            "structured_relationships": deepcopy(structured_relationships),
            "sqlite_data": {
                "entities_appeared": list(sqlite_data.get("entities_appeared", []) or []),
                "entities_new": list(sqlite_data.get("entities_new", []) or []),
                "state_changes": list(sqlite_data.get("state_changes", []) or []),
                "relationships_new": list(sqlite_data.get("relationships_new", []) or []),
                "chapter": sqlite_data.get("chapter"),
            },
        }

    def _merge_sqlite_pending_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """将重试快照合并到当前 pending。"""
        for key, patch in snapshot.get("entity_patches", {}).items():
            if not isinstance(key, tuple) or len(key) != 2 or not isinstance(patch, _EntityPatch):
                continue
            existing = self._pending_entity_patches.get(key)
            if existing is None:
                self._pending_entity_patches[key] = deepcopy(patch)
                continue
            existing.replace = existing.replace or patch.replace
            if existing.base_entity is None and patch.base_entity is not None:
                existing.base_entity = deepcopy(patch.base_entity)
            existing.top_updates.update(patch.top_updates or {})
            existing.current_updates.update(patch.current_updates or {})
            if patch.appearance_chapter is not None:
                if existing.appearance_chapter is None:
                    existing.appearance_chapter = patch.appearance_chapter
                else:
                    existing.appearance_chapter = max(existing.appearance_chapter, patch.appearance_chapter)

        for alias, entries in (snapshot.get("alias_entries", {}) or {}).items():
            if not isinstance(entries, list):
                continue
            existing = self._pending_alias_entries.setdefault(alias, [])
            seen = {(str(e.get("type", "")), str(e.get("id", ""))) for e in existing if isinstance(e, dict)}
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                key = (str(entry.get("type", "")), str(entry.get("id", "")))
                if key in seen:
                    continue
                existing.append(deepcopy(entry))
                seen.add(key)

        self._pending_state_changes.extend(deepcopy(snapshot.get("state_changes", []) or []))
        self._pending_structured_relationships.extend(
            deepcopy(snapshot.get("structured_relationships", []) or [])
        )

        sqlite_data = snapshot.get("sqlite_data", {}) or {}
        for field in ("entities_appeared", "entities_new", "state_changes", "relationships_new"):
            self._pending_sqlite_data[field].extend(deepcopy(sqlite_data.get(field, []) or []))
        incoming_chapter = sqlite_data.get("chapter")
        if incoming_chapter is not None:
            current_chapter = self._pending_sqlite_data.get("chapter")
            if current_chapter is None:
                self._pending_sqlite_data["chapter"] = incoming_chapter
            else:
                try:
                    self._pending_sqlite_data["chapter"] = max(int(current_chapter), int(incoming_chapter))
                except (TypeError, ValueError):
                    self._pending_sqlite_data["chapter"] = incoming_chapter

    def _persist_sqlite_retry_queue(
        self,
        disk_state: Dict[str, Any],
        snapshot: Dict[str, Any],
        error_message: str,
    ) -> None:
        """将 SQLite 待同步数据持久化到 state.json，避免进程退出后丢失。"""
        queue = disk_state.get(self.SQLITE_RETRY_KEY, [])
        if not isinstance(queue, list):
            queue = []
        queue.append(
            {
                "created_at": datetime.now().isoformat(),
                "error": str(error_message),
                "pending": self._serialize_sqlite_pending_snapshot(snapshot),
            }
        )
        disk_state[self.SQLITE_RETRY_KEY] = queue[-20:]
        atomic_write_json(self.config.state_file, disk_state, use_lock=False, backup=True)

    def _clear_pending_sqlite_data(self):
        """清空待同步的 SQLite 数据"""
        self._pending_sqlite_data = {
            "entities_appeared": [],
            "entities_new": [],
            "state_changes": [],
            "relationships_new": [],
            "chapter": None
        }

    # ==================== 进度管理 ====================

    def get_current_chapter(self) -> int:
        """获取当前章节号"""
        return self._state.get("progress", {}).get("current_chapter", 0)

    def update_progress(self, chapter: int, words: int = 0):
        """更新进度"""
        if "progress" not in self._state:
            self._state["progress"] = {}
        self._state["progress"]["current_chapter"] = chapter
        if words > 0:
            total = self._state["progress"].get("total_words", 0)
            self._state["progress"]["total_words"] = total + words

        # 记录增量：锁内合并时用 max(chapter) + words_delta 累加
        if self._pending_progress_chapter is None:
            self._pending_progress_chapter = chapter
        else:
            self._pending_progress_chapter = max(self._pending_progress_chapter, chapter)
        if words > 0:
            self._pending_progress_words_delta += int(words)

    # ==================== 实体管理 (v5.1 SQLite-first) ====================

    def get_entity(self, entity_id: str, entity_type: str = None) -> Optional[Dict]:
        """获取实体（v5.1 引入：优先从 SQLite 读取）"""
        # v5.1 引入: 优先从 SQLite 读取
        if self._sql_state_manager:
            entity = self._sql_state_manager._index_manager.get_entity(entity_id)
            if entity:
                return entity

        # 回退到内存 state (兼容未迁移场景)
        entities_v3 = self._state.get("entities_v3", {})
        if entity_type:
            return entities_v3.get(entity_type, {}).get(entity_id)

        # 遍历所有类型查找
        for type_name, entities in entities_v3.items():
            if entity_id in entities:
                return entities[entity_id]
        return None

    def get_entity_type(self, entity_id: str) -> Optional[str]:
        """获取实体所属类型"""
        # v5.1 引入: 优先从 SQLite 读取
        if self._sql_state_manager:
            entity = self._sql_state_manager._index_manager.get_entity(entity_id)
            if entity:
                return entity.get("type")

        # 回退到内存 state
        for type_name, entities in self._state.get("entities_v3", {}).items():
            if entity_id in entities:
                return type_name
        return None

    def get_all_entities(self) -> Dict[str, Dict]:
        """获取所有实体（扁平化视图）"""
        # v5.1 引入: 优先从 SQLite 读取
        if self._sql_state_manager:
            result = {}
            for entity_type in self.ENTITY_TYPES:
                entities = self._sql_state_manager._index_manager.get_entities_by_type(entity_type)
                for e in entities:
                    eid = e.get("id")
                    if eid:
                        result[eid] = {**e, "type": entity_type}
            if result:
                return result

        # 回退到内存 state
        result = {}
        for type_name, entities in self._state.get("entities_v3", {}).items():
            for eid, e in entities.items():
                result[eid] = {**e, "type": type_name}
        return result

    def get_entities_by_type(self, entity_type: str) -> Dict[str, Dict]:
        """按类型获取实体"""
        # v5.1 引入: 优先从 SQLite 读取
        if self._sql_state_manager:
            entities = self._sql_state_manager._index_manager.get_entities_by_type(entity_type)
            if entities:
                return {e.get("id"): e for e in entities if e.get("id")}

        # 回退到内存 state
        return self._state.get("entities_v3", {}).get(entity_type, {})

    def get_entities_by_tier(self, tier: str) -> Dict[str, Dict]:
        """按层级获取实体"""
        # v5.1 引入: 优先从 SQLite 读取
        if self._sql_state_manager:
            result = {}
            for entity_type in self.ENTITY_TYPES:
                entities = self._sql_state_manager._index_manager.get_entities_by_tier(tier)
                for e in entities:
                    eid = e.get("id")
                    if eid and e.get("type") == entity_type:
                        result[eid] = {**e, "type": entity_type}
            if result:
                return result

        # 回退到内存 state
        result = {}
        for type_name, entities in self._state.get("entities_v3", {}).items():
            for eid, e in entities.items():
                if e.get("tier") == tier:
                    result[eid] = {**e, "type": type_name}
        return result

    def add_entity(self, entity: EntityState) -> bool:
        """添加新实体（v5.0 entities_v3 格式，v5.4 沿用）"""
        entity_type = entity.type
        if entity_type not in self.ENTITY_TYPES:
            entity_type = "角色"

        if "entities_v3" not in self._state:
            self._state["entities_v3"] = {t: {} for t in self.ENTITY_TYPES}

        if entity_type not in self._state["entities_v3"]:
            self._state["entities_v3"][entity_type] = {}

        # 检查是否已存在
        if entity.id in self._state["entities_v3"][entity_type]:
            return False

        # 转换为 v3 格式
        v3_entity = {
            "canonical_name": entity.name,
            "tier": entity.tier,
            "desc": "",
            "current": entity.attributes,
            "first_appearance": entity.first_appearance,
            "last_appearance": entity.last_appearance,
            "history": []
        }
        self._state["entities_v3"][entity_type][entity.id] = v3_entity

        # 记录实体补丁（新建：仅填充缺失字段，避免覆盖并发写入）
        patch = self._pending_entity_patches.get((entity_type, entity.id))
        if patch is None:
            patch = _EntityPatch(entity_type=entity_type, entity_id=entity.id)
            self._pending_entity_patches[(entity_type, entity.id)] = patch
        patch.replace = True
        patch.base_entity = v3_entity

        # v5.1 引入: 注册别名到 index.db (通过 SQLStateManager)
        if self._sql_state_manager:
            self._sql_state_manager._index_manager.register_alias(entity.name, entity.id, entity_type)
            for alias in entity.aliases:
                if alias:
                    self._sql_state_manager._index_manager.register_alias(alias, entity.id, entity_type)

        return True

    def _register_alias_internal(self, entity_id: str, entity_type: str, alias: str):
        """内部方法：注册别名到 index.db（v5.1 引入）"""
        if not alias:
            return
        # v5.1 引入: 直接写入 SQLite
        if self._sql_state_manager:
            self._sql_state_manager._index_manager.register_alias(alias, entity_id, entity_type)

    def update_entity(self, entity_id: str, updates: Dict[str, Any], entity_type: str = None) -> bool:
        """更新实体属性（v5.0 引入，v5.4 沿用）"""
        # v5.1+ SQLite-first:
        # - entity_type 可能来自 SQLite（entities 表），但 state.json 不再持久化 entities_v3。
        # - 因此不能假设 self._state["entities_v3"][type][id] 一定存在（issues7 日志曾 KeyError）。
        resolved_type = entity_type or self.get_entity_type(entity_id)
        if not resolved_type:
            return False
        if resolved_type not in self.ENTITY_TYPES:
            resolved_type = "角色"

        # 仅在内存存在 v3 实体时才更新内存快照（不强行创建，避免 state.json 再膨胀）
        entities_v3 = self._state.get("entities_v3")
        entity = None
        if isinstance(entities_v3, dict):
            bucket = entities_v3.get(resolved_type)
            if isinstance(bucket, dict):
                entity = bucket.get(entity_id)

        # SQLite 启用时，即使内存实体缺失，也要记录 patch，确保 current 能增量写回 index.db
        patch = None
        if self._sql_state_manager:
            patch = self._pending_entity_patches.get((resolved_type, entity_id))
            if patch is None:
                patch = _EntityPatch(entity_type=resolved_type, entity_id=entity_id)
                self._pending_entity_patches[(resolved_type, entity_id)] = patch

        if entity is None and patch is None:
            return False

        did_any = False
        for key, value in updates.items():
            if key == "attributes" and isinstance(value, dict):
                if entity is not None:
                    if "current" not in entity:
                        entity["current"] = {}
                    entity["current"].update(value)
                if patch is not None:
                    patch.current_updates.update(value)
                did_any = True
            elif key == "current" and isinstance(value, dict):
                if entity is not None:
                    if "current" not in entity:
                        entity["current"] = {}
                    entity["current"].update(value)
                if patch is not None:
                    patch.current_updates.update(value)
                did_any = True
            else:
                if entity is not None:
                    entity[key] = value
                if patch is not None:
                    patch.top_updates[key] = value
                did_any = True

        return did_any

    def update_entity_appearance(self, entity_id: str, chapter: int, entity_type: str = None):
        """更新实体出场章节"""
        if not entity_type:
            entity_type = self.get_entity_type(entity_id)
        if not entity_type:
            return

        entities_v3 = self._state.get("entities_v3")
        if not isinstance(entities_v3, dict):
            entities_v3 = {t: {} for t in self.ENTITY_TYPES}
            self._state["entities_v3"] = entities_v3
        entities_v3.setdefault(entity_type, {})

        entity = entities_v3[entity_type].get(entity_id)
        if entity:
            if entity.get("first_appearance", 0) == 0:
                entity["first_appearance"] = chapter
            entity["last_appearance"] = chapter

            # 记录补丁：锁内应用 first=min(non-zero), last=max
            patch = self._pending_entity_patches.get((entity_type, entity_id))
            if patch is None:
                patch = _EntityPatch(entity_type=entity_type, entity_id=entity_id)
                self._pending_entity_patches[(entity_type, entity_id)] = patch
            if patch.appearance_chapter is None:
                patch.appearance_chapter = chapter
            else:
                patch.appearance_chapter = max(int(patch.appearance_chapter), int(chapter))

    # ==================== 状态变化记录 ====================

    def record_state_change(
        self,
        entity_id: str,
        field: str,
        old_value: Any,
        new_value: Any,
        reason: str,
        chapter: int
    ):
        """记录状态变化"""
        if "state_changes" not in self._state:
            self._state["state_changes"] = []

        change = StateChange(
            entity_id=entity_id,
            field=field,
            old_value=old_value,
            new_value=new_value,
            reason=reason,
            chapter=chapter
        )
        change_dict = asdict(change)
        self._state["state_changes"].append(change_dict)
        self._pending_state_changes.append(change_dict)

        # 同时更新实体属性
        self.update_entity(entity_id, {"attributes": {field: new_value}})

    def get_state_changes(self, entity_id: Optional[str] = None) -> List[Dict]:
        """获取状态变化历史"""
        changes = self._state.get("state_changes", [])
        if entity_id:
            changes = [c for c in changes if c.get("entity_id") == entity_id]
        return changes

    # ==================== 关系管理 ====================

    def add_relationship(
        self,
        from_entity: str,
        to_entity: str,
        rel_type: str,
        description: str,
        chapter: int
    ):
        """添加关系"""
        rel = Relationship(
            from_entity=from_entity,
            to_entity=to_entity,
            type=rel_type,
            description=description,
            chapter=chapter
        )

        # v5.0 引入: 实体关系存入 structured_relationships，避免与 relationships(人物关系字典) 冲突
        if "structured_relationships" not in self._state:
            self._state["structured_relationships"] = []
        rel_dict = asdict(rel)
        self._state["structured_relationships"].append(rel_dict)
        self._pending_structured_relationships.append(rel_dict)

    def get_relationships(self, entity_id: Optional[str] = None) -> List[Dict]:
        """获取关系列表"""
        rels = self._state.get("structured_relationships", [])
        if entity_id:
            rels = [
                r for r in rels
                if r.get("from_entity") == entity_id or r.get("to_entity") == entity_id
            ]
        return rels

    # ==================== 批量操作 ====================

    def _record_disambiguation(self, chapter: int, uncertain_items: Any) -> List[str]:
        """
        记录消歧反馈到 state.json，便于 Writer/Context Agent 感知风险。

        约定：
        - >= extraction_confidence_medium：写入 disambiguation_warnings（采用但警告）
        - < extraction_confidence_medium：写入 disambiguation_pending（需人工确认）
        """
        if not isinstance(uncertain_items, list) or not uncertain_items:
            return []

        warnings: List[str] = []
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for item in uncertain_items:
            if not isinstance(item, dict):
                continue

            mention = str(item.get("mention", "") or "").strip()
            if not mention:
                continue

            raw_conf = item.get("confidence", 0.0)
            try:
                confidence = float(raw_conf)
            except (TypeError, ValueError):
                confidence = 0.0

            # 候选：支持 [{"type","id"}...] 或 ["id1","id2"] 两种形式
            candidates_raw = item.get("candidates", [])
            candidates: List[Dict[str, str]] = []
            if isinstance(candidates_raw, list):
                for c in candidates_raw:
                    if isinstance(c, dict):
                        cid = str(c.get("id", "") or "").strip()
                        ctype = str(c.get("type", "") or "").strip()
                        entry: Dict[str, str] = {}
                        if ctype:
                            entry["type"] = ctype
                        if cid:
                            entry["id"] = cid
                        if entry:
                            candidates.append(entry)
                    else:
                        cid = str(c).strip()
                        if cid:
                            candidates.append({"id": cid})

            entity_type = str(item.get("type", "") or "").strip()
            suggested_id = str(item.get("suggested", "") or "").strip()

            adopted_raw = item.get("adopted", None)
            chosen_id = ""
            if isinstance(adopted_raw, str):
                chosen_id = adopted_raw.strip()
            elif adopted_raw is True:
                chosen_id = suggested_id
            else:
                # 兼容字段名：entity_id / chosen_id
                chosen_id = str(item.get("entity_id") or item.get("chosen_id") or "").strip() or suggested_id

            context = str(item.get("context", "") or "").strip()
            note = str(item.get("warning", "") or "").strip()

            record: Dict[str, Any] = {
                "chapter": int(chapter),
                "mention": mention,
                "type": entity_type,
                "suggested_id": suggested_id,
                "chosen_id": chosen_id,
                "confidence": confidence,
                "candidates": candidates,
                "context": context,
                "note": note,
                "created_at": now,
            }

            if confidence >= float(self.config.extraction_confidence_medium):
                self._state.setdefault("disambiguation_warnings", []).append(record)
                self._pending_disambiguation_warnings.append(record)
                chosen_part = f" → {chosen_id}" if chosen_id else ""
                warnings.append(f"消歧警告: {mention}{chosen_part} (confidence: {confidence:.2f})")
            else:
                self._state.setdefault("disambiguation_pending", []).append(record)
                self._pending_disambiguation_pending.append(record)
                warnings.append(f"消歧需人工确认: {mention} (confidence: {confidence:.2f})")

        return warnings

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _extract_chapter_word_count(self, result: Dict[str, Any]) -> int:
        """从 Data Agent 结果中提取章节字数（缺失时返回 0）。"""
        chapter_info = result.get("chapter_info")
        if not isinstance(chapter_info, dict):
            chapter_info = {}

        candidates = [
            result.get("word_count"),
            result.get("chapter_word_count"),
            chapter_info.get("word_count"),
        ]
        for raw in candidates:
            count = self._to_int(raw, default=0)
            if count > 0:
                return count
        return 0

    @staticmethod
    def _extract_markdown_section(text: str, heading: str) -> str:
        """提取 Markdown 二级标题下的正文块。"""
        if not text:
            return ""
        pattern = rf"##\s*{re.escape(heading)}\s*\n(.+?)(?=\n##|\Z)"
        match = re.search(pattern, text, flags=re.DOTALL)
        return str(match.group(1)).strip() if match else ""

    @staticmethod
    def _extract_title_from_chapter_filename(filename: str) -> str:
        """从章节文件名提取标题（如：第001章-标题.md）。"""
        match = re.match(r"^第0*\d+章(?:[-—_ ]+(?P<title>.+?))?\.md$", filename.strip())
        if not match:
            return ""
        return str(match.group("title") or "").strip()

    @staticmethod
    def _extract_title_from_markdown(text: str, chapter: int) -> str:
        """从章节正文首个标题提取章节名。"""
        if not text:
            return ""
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped.startswith("#"):
                continue
            raw_title = re.sub(r"^#+\s*", "", stripped).strip()
            if not raw_title:
                continue
            raw_title = re.sub(rf"^第0*{int(chapter)}章[\s：:：\-—_]*", "", raw_title).strip()
            return raw_title
        return ""

    @staticmethod
    def _estimate_markdown_word_count(text: str) -> int:
        """估算正文字符数（用于填充 chapters.word_count）。"""
        if not text:
            return 0
        body = re.sub(r"```[\s\S]*?```", "", text)
        body = re.sub(r"^#{1,6}\s+.*$", "", body, flags=re.MULTILINE)
        body = re.sub(r"^\s*[-*_]{3,}\s*$", "", body, flags=re.MULTILINE)
        body = re.sub(r"\s+", "", body)
        return max(0, len(body))

    @staticmethod
    def _excerpt_plain_text(text: str, max_chars: int = 180) -> str:
        """提取简短纯文本摘要。"""
        if not text:
            return ""
        body = re.sub(r"```[\s\S]*?```", "", text)
        body = re.sub(r"^#{1,6}\s+.*$", "", body, flags=re.MULTILINE)
        lines = [line.strip() for line in body.splitlines() if line.strip()]
        if not lines:
            return ""
        plain = " ".join(lines).strip()
        return plain[: max(20, int(max_chars))]

    def _collect_backfill_candidate_chapters(self, index_manager) -> Dict[int, set[str]]:
        """收集可用于回填的章节号及来源。"""
        candidates: Dict[int, set[str]] = {}

        def _add(chapter_value: Any, source: str) -> None:
            chapter = self._to_int(chapter_value, default=0)
            if chapter <= 0:
                return
            candidates.setdefault(chapter, set()).add(source)

        # 来源1：正文文件
        try:
            from chapter_paths import extract_chapter_num_from_filename
        except ImportError:  # pragma: no cover
            from scripts.chapter_paths import extract_chapter_num_from_filename

        chapters_dir = self.config.chapters_dir
        if chapters_dir.exists():
            for chapter_file in chapters_dir.rglob("第*.md"):
                chapter_num = extract_chapter_num_from_filename(chapter_file.name)
                if chapter_num:
                    _add(chapter_num, "chapter_file")

        # 来源2：摘要文件
        summaries_dir = self.config.webnovel_dir / "summaries"
        if summaries_dir.exists():
            for summary_file in summaries_dir.glob("ch*.md"):
                match = re.match(r"^ch0*(\d+)\.md$", summary_file.name.lower())
                if match:
                    _add(match.group(1), "summary_file")

        # 来源3：state.chapter_meta
        chapter_meta = self._state.get("chapter_meta", {})
        if isinstance(chapter_meta, dict):
            for key in chapter_meta.keys():
                _add(key, "state.chapter_meta")

        # 来源4：当前进度章节（至少确保当前章可见）
        progress = self._state.get("progress", {})
        if isinstance(progress, dict):
            _add(progress.get("current_chapter"), "state.progress")

        # 来源5：SQLite 现有章节信号（允许缺少某些表）
        table_sources = [
            ("chapters", "db.chapters"),
            ("scenes", "db.scenes"),
            ("appearances", "db.appearances"),
            ("chapter_reading_power", "db.chapter_reading_power"),
        ]
        try:
            with index_manager._get_conn() as conn:
                for table_name, source_name in table_sources:
                    try:
                        rows = conn.execute(f"SELECT DISTINCT chapter FROM {table_name}").fetchall()
                    except sqlite3.OperationalError:
                        continue
                    for row in rows:
                        _add(row[0], source_name)
        except Exception:
            # 回填是 best-effort，DB 异常时保留已有文件/state来源
            pass

        return candidates

    def _build_backfill_chapter_payload(self, chapter: int, sources: set[str], index_manager) -> Dict[str, Any]:
        """根据已有信号推断单章 ChapterMeta 字段。"""
        try:
            from chapter_paths import find_chapter_file
        except ImportError:  # pragma: no cover
            from scripts.chapter_paths import find_chapter_file

        chapter_file = find_chapter_file(self.config.project_root, int(chapter))
        chapter_text = ""
        if chapter_file and chapter_file.exists():
            chapter_text = read_text_safe(
                chapter_file,
                default="",
                auto_repair=False,
                backup_on_repair=False,
            )

        summary_path = self.config.webnovel_dir / "summaries" / f"ch{int(chapter):04d}.md"
        summary_text = ""
        if summary_path.exists():
            summary_text = read_text_safe(
                summary_path,
                default="",
                auto_repair=False,
                backup_on_repair=False,
            )

        chapter_meta: Dict[str, Any] = {}
        raw_chapter_meta = self._state.get("chapter_meta", {})
        if isinstance(raw_chapter_meta, dict):
            for key in (str(int(chapter)).zfill(4), str(int(chapter))):
                value = raw_chapter_meta.get(key)
                if isinstance(value, dict):
                    chapter_meta = value
                    break

        title = str(
            chapter_meta.get("title")
            or chapter_meta.get("chapter_title")
            or chapter_meta.get("name")
            or ""
        ).strip()
        if not title and chapter_file:
            title = self._extract_title_from_chapter_filename(chapter_file.name)
        if not title:
            title = self._extract_title_from_markdown(chapter_text, int(chapter))

        location = str(
            chapter_meta.get("location")
            or chapter_meta.get("chapter_location")
            or chapter_meta.get("scene_location")
            or ""
        ).strip()
        if not location:
            ending = chapter_meta.get("ending")
            if isinstance(ending, dict):
                location = str(ending.get("location") or "").strip()

        summary = str(
            chapter_meta.get("summary")
            or chapter_meta.get("chapter_summary")
            or ""
        ).strip()
        if not summary and summary_text:
            summary = self._extract_markdown_section(summary_text, "剧情摘要")
            if not summary:
                summary = self._excerpt_plain_text(summary_text, max_chars=220)
        if not summary and chapter_text:
            summary = self._extract_markdown_section(chapter_text, "本章摘要")
        if not summary:
            summary = self._excerpt_plain_text(chapter_text, max_chars=220)

        word_count = self._to_int(
            chapter_meta.get("word_count", chapter_meta.get("chapter_word_count")),
            default=0,
        )
        if word_count <= 0:
            word_count = self._estimate_markdown_word_count(chapter_text)

        characters: List[str] = []
        try:
            with index_manager._get_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT entity_id
                    FROM appearances
                    WHERE chapter = ?
                    ORDER BY confidence DESC, entity_id ASC
                    """,
                    (int(chapter),),
                ).fetchall()
                seen = set()
                for row in rows:
                    entity_id = str(row[0] or "").strip()
                    if not entity_id or entity_id in seen:
                        continue
                    seen.add(entity_id)
                    characters.append(entity_id)
        except Exception:
            characters = []

        if not characters:
            raw_chars = chapter_meta.get("characters")
            if isinstance(raw_chars, list):
                characters = [str(v).strip() for v in raw_chars if str(v).strip()]

        return {
            "chapter": int(chapter),
            "sources": sorted(sources),
            "title": title,
            "location": location,
            "word_count": max(0, int(word_count or 0)),
            "characters": characters,
            "summary": summary,
        }

    def backfill_missing_chapter_index(
        self,
        *,
        from_chapter: Optional[int] = None,
        to_chapter: Optional[int] = None,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """
        回填 index.db 中缺失的 chapters 行（best-effort）。

        数据源（按可用性自动组合）：
        - 正文章节文件（正文/）
        - .webnovel/summaries/chNNNN.md
        - state.json.chapter_meta
        - SQLite appearances/scenes/chapter_reading_power
        """
        try:
            from .index_manager import IndexManager, ChapterMeta
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(f"无法加载 index_manager: {exc}") from exc

        index_manager = self._sql_state_manager._index_manager if self._sql_state_manager else IndexManager(self.config)

        candidates = self._collect_backfill_candidate_chapters(index_manager)
        ordered_chapters = sorted(candidates.keys())
        if from_chapter is not None:
            ordered_chapters = [ch for ch in ordered_chapters if ch >= int(from_chapter)]
        if to_chapter is not None:
            ordered_chapters = [ch for ch in ordered_chapters if ch <= int(to_chapter)]

        existing_chapters = set()
        with index_manager._get_conn() as conn:
            for row in conn.execute("SELECT chapter FROM chapters").fetchall():
                chapter = self._to_int(row[0], default=0)
                if chapter > 0:
                    existing_chapters.add(chapter)

        report: Dict[str, Any] = {
            "dry_run": bool(dry_run),
            "from_chapter": int(from_chapter) if from_chapter is not None else None,
            "to_chapter": int(to_chapter) if to_chapter is not None else None,
            "candidates": len(ordered_chapters),
            "already_present": 0,
            "missing": 0,
            "repaired": 0,
            "failed": 0,
            "repaired_chapters": [],
            "failed_items": [],
            "preview": [],
        }

        for chapter in ordered_chapters:
            if chapter in existing_chapters:
                report["already_present"] += 1
                continue

            report["missing"] += 1
            payload = self._build_backfill_chapter_payload(chapter, candidates.get(chapter, set()), index_manager)
            report["preview"].append(
                {
                    "chapter": int(payload["chapter"]),
                    "sources": list(payload["sources"]),
                    "title": payload["title"],
                    "word_count": int(payload["word_count"]),
                    "characters": list(payload["characters"]),
                    "has_summary": bool(payload["summary"]),
                }
            )

            if dry_run:
                continue

            try:
                index_manager.add_chapter(
                    ChapterMeta(
                        chapter=int(payload["chapter"]),
                        title=str(payload["title"]),
                        location=str(payload["location"]),
                        word_count=max(0, int(payload["word_count"])),
                        characters=list(payload["characters"]),
                        summary=str(payload["summary"]),
                    )
                )
                report["repaired"] += 1
                report["repaired_chapters"].append(int(payload["chapter"]))
            except Exception as exc:
                report["failed"] += 1
                report["failed_items"].append(
                    {"chapter": int(payload["chapter"]), "error": str(exc)}
                )

        return report

    def _sync_chapter_index_from_result(self, chapter: int, result: Dict[str, Any], word_count: int) -> None:
        """
        将章节级元数据落盘到 index.db 的 chapters/scenes。

        说明：
        - 旧流程常只写实体关系，未写 chapters/scenes，导致 Dashboard 章节视图缺失。
        - 这里做“尽力写入”：字段缺失时也至少保留 chapter 行，后续可补齐。
        """
        if not self._sql_state_manager:
            return

        try:
            from .index_manager import ChapterMeta, SceneMeta
        except Exception:
            return

        chapter_info = result.get("chapter_info")
        if not isinstance(chapter_info, dict):
            chapter_info = {}

        title = str(
            chapter_info.get("title")
            or result.get("chapter_title")
            or result.get("title")
            or ""
        ).strip()
        location = str(
            chapter_info.get("location")
            or result.get("chapter_location")
            or result.get("location")
            or ""
        ).strip()
        if not location:
            chapter_meta = result.get("chapter_meta")
            if isinstance(chapter_meta, dict):
                ending = chapter_meta.get("ending")
                if isinstance(ending, dict):
                    location = str(ending.get("location") or "").strip()

        summary = str(
            chapter_info.get("summary")
            or result.get("chapter_summary")
            or ""
        ).strip()

        characters = []
        raw_chars = chapter_info.get("characters")
        if isinstance(raw_chars, list):
            characters = [str(v).strip() for v in raw_chars if str(v).strip()]
        if not characters:
            seen = set()
            for row in result.get("entities_appeared", []):
                if not isinstance(row, dict):
                    continue
                eid = str(row.get("id", "") or "").strip()
                if not eid or eid in seen:
                    continue
                seen.add(eid)
                characters.append(eid)

        self._sql_state_manager._index_manager.add_chapter(
            ChapterMeta(
                chapter=int(chapter),
                title=title,
                location=location,
                word_count=max(0, int(word_count or 0)),
                characters=characters,
                summary=summary,
            )
        )

        scenes_raw = result.get("scenes")
        if not isinstance(scenes_raw, list):
            scenes_raw = result.get("scene_chunks")
        if not isinstance(scenes_raw, list) or not scenes_raw:
            return

        scenes: List[SceneMeta] = []
        for idx, raw in enumerate(scenes_raw, start=1):
            if not isinstance(raw, dict):
                continue
            scene_index = self._to_int(raw.get("scene_index", raw.get("index", idx)), default=idx)
            start_line = self._to_int(raw.get("start_line"), default=1)
            end_line = self._to_int(raw.get("end_line"), default=max(start_line, 1))
            if end_line < start_line:
                end_line = start_line
            scene_location = str(raw.get("location") or location or "").strip()
            scene_summary = str(raw.get("summary") or raw.get("content") or "").strip()
            raw_scene_chars = raw.get("characters")
            if isinstance(raw_scene_chars, list):
                scene_chars = [str(v).strip() for v in raw_scene_chars if str(v).strip()]
            else:
                scene_chars = []

            scenes.append(
                SceneMeta(
                    chapter=int(chapter),
                    scene_index=max(1, int(scene_index)),
                    start_line=max(1, int(start_line)),
                    end_line=max(1, int(end_line)),
                    location=scene_location,
                    summary=scene_summary,
                    characters=scene_chars,
                )
            )

        if scenes:
            self._sql_state_manager._index_manager.add_scenes(int(chapter), scenes)

    def process_chapter_result(self, chapter: int, result: Dict) -> List[str]:
        """
        处理 Data Agent 的章节处理结果（v5.1 引入，v5.4 沿用）

        输入格式:
        - entities_appeared: 出场实体列表
        - entities_new: 新实体列表
        - state_changes: 状态变化列表
        - relationships_new: 新关系列表

        返回警告列表
        """
        warnings = []

        # v5.1 引入: 记录章节号用于 SQLite 同步
        self._pending_sqlite_data["chapter"] = chapter

        # 处理出场实体
        for entity in result.get("entities_appeared", []):
            entity_id = entity.get("id")
            entity_type = entity.get("type")
            if entity_id:
                self.update_entity_appearance(entity_id, chapter, entity_type)
                # v5.1 引入: 缓存用于 SQLite 同步
                self._pending_sqlite_data["entities_appeared"].append(entity)

        # 处理新实体
        for entity in result.get("entities_new", []):
            entity_id = entity.get("suggested_id") or entity.get("id")
            if entity_id and entity_id != "NEW":
                new_entity = EntityState(
                    id=entity_id,
                    name=entity.get("name", ""),
                    type=entity.get("type", "角色"),
                    tier=entity.get("tier", "装饰"),
                    aliases=entity.get("mentions", []),
                    first_appearance=chapter,
                    last_appearance=chapter
                )
                if not self.add_entity(new_entity):
                    warnings.append(f"实体已存在: {entity_id}")
                # v5.1 引入: 缓存用于 SQLite 同步
                self._pending_sqlite_data["entities_new"].append(entity)

        # 处理状态变化
        for change in result.get("state_changes", []):
            self.record_state_change(
                entity_id=change.get("entity_id", ""),
                field=change.get("field", ""),
                old_value=change.get("old"),
                new_value=change.get("new"),
                reason=change.get("reason", ""),
                chapter=chapter
            )
            # v5.1 引入: 缓存用于 SQLite 同步
            self._pending_sqlite_data["state_changes"].append(change)

        # 处理关系
        for rel in result.get("relationships_new", []):
            self.add_relationship(
                from_entity=rel.get("from", ""),
                to_entity=rel.get("to", ""),
                rel_type=rel.get("type", ""),
                description=rel.get("description", ""),
                chapter=chapter
            )
            # v5.1 引入: 缓存用于 SQLite 同步
            self._pending_sqlite_data["relationships_new"].append(rel)

        # 处理消歧不确定项（不影响实体写入，但必须对 Writer 可见）
        warnings.extend(self._record_disambiguation(chapter, result.get("uncertain", [])))

        # 写入 chapter_meta（钩子/模式/结束状态）
        chapter_meta = result.get("chapter_meta")
        if isinstance(chapter_meta, dict):
            meta_key = f"{int(chapter):04d}"
            self._state.setdefault("chapter_meta", {})
            self._state["chapter_meta"][meta_key] = chapter_meta
            self._pending_chapter_meta[meta_key] = chapter_meta

        chapter_word_count = self._extract_chapter_word_count(result)
        # 更新进度（若 Data Agent 提供字数则同步 total_words）
        self.update_progress(chapter, words=chapter_word_count)
        # 补充章节级索引（chapters/scenes）
        self._sync_chapter_index_from_result(chapter, result, chapter_word_count)

        # 同步主角状态（entities_v3 → protagonist_state）
        self.sync_protagonist_from_entity()

        return warnings

    # ==================== 导出 ====================

    def export_for_context(self) -> Dict:
        """导出用于上下文的精简版状态（v5.0 引入，v5.4 沿用）"""
        # 从 entities_v3 构建精简视图
        entities_flat = {}
        for type_name, entities in self._state.get("entities_v3", {}).items():
            for eid, e in entities.items():
                entities_flat[eid] = {
                    "name": e.get("canonical_name", eid),
                    "type": type_name,
                    "tier": e.get("tier", "装饰"),
                    "current": e.get("current", {})
                }

        return {
            "progress": self._state.get("progress", {}),
            "entities": entities_flat,
            # v5.1 引入: alias_index 已迁移到 index.db，这里返回空（兼容性）
            "alias_index": {},
            "recent_changes": [],  # v5.1 引入: 从 index.db 查询
            "disambiguation": {
                "warnings": self._state.get("disambiguation_warnings", [])[-self.config.export_disambiguation_slice:],
                "pending": self._state.get("disambiguation_pending", [])[-self.config.export_disambiguation_slice:],
            },
        }

    # ==================== 主角同步 ====================

    def get_protagonist_entity_id(self) -> Optional[str]:
        """获取主角实体 ID（通过 is_protagonist 标记或 SQLite 查询）"""
        # 方式1: 通过 SQLStateManager 查询 (v5.1)
        if self._sql_state_manager:
            protagonist = self._sql_state_manager.get_protagonist()
            if protagonist:
                return protagonist.get("id")

        # 方式2: 通过 protagonist_state.name 查找别名
        protag_name = self._state.get("protagonist_state", {}).get("name")
        if protag_name and self._sql_state_manager:
            entities = self._sql_state_manager._index_manager.get_entities_by_alias(protag_name)
            for entry in entities:
                if entry.get("type") == "角色":
                    return entry.get("id")

        return None

    def sync_protagonist_from_entity(self, entity_id: str = None):
        """
        将主角实体的状态同步到 protagonist_state (v5.1: 从 SQLite 读取)

        用于确保 consistency-checker 等依赖 protagonist_state 的组件获取最新数据
        """
        if entity_id is None:
            entity_id = self.get_protagonist_entity_id()
        if entity_id is None:
            return

        entity = self.get_entity(entity_id, "角色")
        if not entity:
            return

        current = entity.get("current")
        if not isinstance(current, dict):
            current = entity.get("current_json", {})
        if isinstance(current, str):
            try:
                current = json.loads(current) if current else {}
            except (json.JSONDecodeError, TypeError):
                current = {}
        if not isinstance(current, dict):
            current = {}
        protag = self._state.setdefault("protagonist_state", {})

        # 同步境界
        if "realm" in current:
            power = protag.setdefault("power", {})
            power["realm"] = current["realm"]
            if "layer" in current:
                power["layer"] = current["layer"]

        # 同步位置
        if "location" in current:
            loc = protag.setdefault("location", {})
            loc["current"] = current["location"]
            if "last_chapter" in current:
                loc["last_chapter"] = current["last_chapter"]

    def sync_protagonist_to_entity(self, entity_id: str = None):
        """
        将 protagonist_state 同步到 entities_v3 中的主角实体

        用于初始化或手动编辑 protagonist_state 后保持一致性
        """
        if entity_id is None:
            entity_id = self.get_protagonist_entity_id()
        if entity_id is None:
            return

        protag = self._state.get("protagonist_state", {})
        if not protag:
            return

        updates = {}

        # 同步境界
        power = protag.get("power", {})
        if power.get("realm"):
            updates["realm"] = power["realm"]
        if power.get("layer"):
            updates["layer"] = power["layer"]

        # 同步位置
        loc = protag.get("location", {})
        if loc.get("current"):
            updates["location"] = loc["current"]

        if updates:
            self.update_entity(entity_id, updates, "角色")


# ==================== CLI 接口 ====================

def main():
    import argparse
    import sys
    from pydantic import ValidationError
    from .cli_output import print_success, print_error
    from .cli_args import normalize_global_project_root, load_json_arg
    from .schemas import validate_data_agent_output, format_validation_error, normalize_data_agent_output
    from .index_manager import IndexManager

    parser = argparse.ArgumentParser(description="State Manager CLI (v5.4)")
    parser.add_argument("--project-root", type=str, help="项目根目录")

    subparsers = parser.add_subparsers(dest="command")

    # 读取进度
    subparsers.add_parser("get-progress")

    # 获取实体
    get_entity_parser = subparsers.add_parser("get-entity")
    get_entity_parser.add_argument("--id", required=True)

    # 列出实体
    list_parser = subparsers.add_parser("list-entities")
    list_parser.add_argument("--type", help="按类型过滤")
    list_parser.add_argument("--tier", help="按层级过滤")

    # 处理章节结果
    process_parser = subparsers.add_parser("process-chapter")
    process_parser.add_argument("--chapter", type=int, required=True, help="章节号")
    process_parser.add_argument("--data", required=True, help="JSON 格式的处理结果")

    # 回填缺失章节索引
    backfill_parser = subparsers.add_parser("backfill-missing")
    backfill_parser.add_argument("--from-chapter", type=int, help="起始章节（含）")
    backfill_parser.add_argument("--to-chapter", type=int, help="结束章节（含）")
    backfill_parser.add_argument("--dry-run", action="store_true", help="仅预览，不写入")

    argv = normalize_global_project_root(sys.argv[1:])
    args = parser.parse_args(argv)
    command_started_at = time.perf_counter()

    # 初始化
    config = None
    if args.project_root:
        # 允许传入“工作区根目录”，统一解析到真正的 book project_root（必须包含 .webnovel/state.json）
        from project_locator import resolve_project_root
        from .config import DataModulesConfig

        resolved_root = resolve_project_root(args.project_root)
        config = DataModulesConfig.from_project_root(resolved_root)

    manager = StateManager(config)
    logger = IndexManager(config)
    tool_name = f"state_manager:{args.command or 'unknown'}"

    def _append_timing(success: bool, *, error_code: str | None = None, error_message: str | None = None, chapter: int | None = None):
        elapsed_ms = int((time.perf_counter() - command_started_at) * 1000)
        safe_append_perf_timing(
            manager.config.project_root,
            tool_name=tool_name,
            success=success,
            elapsed_ms=elapsed_ms,
            chapter=chapter,
            error_code=error_code,
            error_message=error_message,
        )

    def emit_success(data=None, message: str = "ok", chapter: int | None = None):
        print_success(data, message=message)
        safe_log_tool_call(logger, tool_name=tool_name, success=True)
        _append_timing(True, chapter=chapter)

    def emit_error(code: str, message: str, suggestion: str | None = None, chapter: int | None = None):
        print_error(code, message, suggestion=suggestion)
        safe_log_tool_call(
            logger,
            tool_name=tool_name,
            success=False,
            error_code=code,
            error_message=message,
        )
        _append_timing(False, error_code=code, error_message=message, chapter=chapter)

    if args.command == "get-progress":
        emit_success(manager._state.get("progress", {}), message="progress")

    elif args.command == "get-entity":
        entity = manager.get_entity(args.id)
        if entity:
            emit_success(entity, message="entity")
        else:
            emit_error("NOT_FOUND", f"未找到实体: {args.id}")

    elif args.command == "list-entities":
        if args.type:
            entities = manager.get_entities_by_type(args.type)
        elif args.tier:
            entities = manager.get_entities_by_tier(args.tier)
        else:
            entities = manager.get_all_entities()

        payload = [{"id": eid, **e} for eid, e in entities.items()]
        emit_success(payload, message="entities")

    elif args.command == "process-chapter":
        data = load_json_arg(args.data)
        validated = None
        last_exc = None
        for _ in range(3):
            try:
                validated = validate_data_agent_output(data)
                break
            except ValidationError as exc:
                last_exc = exc
                data = normalize_data_agent_output(data)
        if validated is None:
            err = format_validation_error(last_exc) if last_exc else {
                "code": "SCHEMA_VALIDATION_FAILED",
                "message": "数据结构校验失败",
                "details": {"errors": []},
                "suggestion": "请检查 data-agent 输出字段是否完整且类型正确",
            }
            emit_error(err["code"], err["message"], suggestion=err.get("suggestion"))
            return

        warnings = manager.process_chapter_result(args.chapter, validated.model_dump(by_alias=True))
        try:
            manager.save_state()
        except RuntimeError as exc:
            emit_error(
                "SQLITE_SYNC_FAILED",
                f"章节处理已中止: {exc}",
                suggestion="请重试同一条 process-chapter 命令，系统会尝试回放 _sqlite_sync_pending 队列。",
                chapter=args.chapter,
            )
            return
        emit_success({"chapter": args.chapter, "warnings": warnings}, message="chapter_processed", chapter=args.chapter)

    elif args.command == "backfill-missing":
        from_chapter = args.from_chapter
        to_chapter = args.to_chapter
        if (
            from_chapter is not None
            and to_chapter is not None
            and int(from_chapter) > int(to_chapter)
        ):
            emit_error(
                "INVALID_RANGE",
                f"章节范围无效: from_chapter={from_chapter} > to_chapter={to_chapter}",
                suggestion="请调整为 from_chapter <= to_chapter",
            )
            return

        try:
            report = manager.backfill_missing_chapter_index(
                from_chapter=from_chapter,
                to_chapter=to_chapter,
                dry_run=bool(args.dry_run),
            )
        except Exception as exc:
            emit_error(
                "BACKFILL_FAILED",
                f"回填失败: {exc}",
                suggestion="可先使用 --dry-run 预览待修复章节。",
            )
            return

        emit_success(
            report,
            message="backfill_preview" if args.dry_run else "backfill_done",
        )

    else:
        emit_error("UNKNOWN_COMMAND", "未指定有效命令", suggestion="请查看 --help")


if __name__ == "__main__":
    if sys.platform == "win32":
        enable_windows_utf8_stdio()
    main()
