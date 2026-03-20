#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path

import pytest


def _load_archive_module():
    import sys

    scripts_dir = Path(__file__).resolve().parents[2]
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))

    import archive_manager

    return archive_manager


@pytest.fixture
def archive_env(tmp_path):
    webnovel = tmp_path / ".webnovel"
    webnovel.mkdir(parents=True, exist_ok=True)
    state_path = webnovel / "state.json"
    state_path.write_text(
        '{"progress":{"current_chapter":10},"plot_threads":{},"review_checkpoints":[]}',
        encoding="utf-8",
    )
    return tmp_path


def test_archive_remove_from_state_missing_sections(archive_env):
    module = _load_archive_module()
    manager = module.ArchiveManager(project_root=archive_env)

    state = {
        "progress": {"current_chapter": 50},
    }

    updated = manager.remove_from_state(state, inactive_chars=[], resolved_threads=[], old_reviews=[])
    assert updated.get("progress", {}).get("current_chapter") == 50


def test_archive_check_trigger_conditions_edges(archive_env):
    module = _load_archive_module()
    manager = module.ArchiveManager(project_root=archive_env)

    manager.config["chapter_trigger"] = 10
    manager.config["file_size_trigger_mb"] = 9999.0

    trigger = manager.check_trigger_conditions({"progress": {"current_chapter": 20}})
    assert trigger["chapter_trigger"] is True
    assert trigger["should_archive"] is True


def test_archive_identify_old_reviews_handles_mixed_formats(archive_env):
    module = _load_archive_module()
    manager = module.ArchiveManager(project_root=archive_env)
    manager.config["review_old_threshold"] = 5

    state = {
        "progress": {"current_chapter": 30},
        "review_checkpoints": [
            {"chapters": "20-22", "report": "r1.md"},
            {"chapter_range": [10, 12], "date": "2026-01-01"},
            {"report": "Review_Ch5-6.md"},
        ],
    }

    results = manager.identify_old_reviews(state)
    assert len(results) == 3
    assert all(row["chapters_since_review"] >= 5 for row in results)


def test_archive_identify_old_reviews_skips_non_dict(archive_env):
    module = _load_archive_module()
    manager = module.ArchiveManager(project_root=archive_env)
    manager.config["review_old_threshold"] = 1

    state = {
        "progress": {"current_chapter": 20},
        "review_checkpoints": [
            "bad-review-item",
            {"chapters": "1-2", "report": "r1.md"},
        ],
    }
    results = manager.identify_old_reviews(state)
    assert len(results) == 1
    assert results[0]["review"]["report"] == "r1.md"


def test_archive_remove_from_state_uses_thread_fingerprint_not_content_only(archive_env):
    module = _load_archive_module()
    manager = module.ArchiveManager(project_root=archive_env)

    thread_a = {"content": "同一文案", "planted_chapter": 10, "status": "已回收", "resolved_chapter": 30}
    thread_b = {"content": "同一文案", "planted_chapter": 11, "status": "已回收", "resolved_chapter": 31}
    state = {
        "plot_threads": {
            "foreshadowing": [thread_a.copy(), thread_b.copy()],
            "resolved": [thread_a.copy(), thread_b.copy()],
        }
    }
    updated = manager.remove_from_state(
        state,
        inactive_chars=[],
        resolved_threads=[{"thread": thread_a.copy()}],
        old_reviews=[],
    )
    foreshadowing = updated["plot_threads"]["foreshadowing"]
    resolved = updated["plot_threads"]["resolved"]
    assert len(foreshadowing) == 1
    assert len(resolved) == 1
    assert foreshadowing[0]["planted_chapter"] == 11
    assert resolved[0]["planted_chapter"] == 11


def test_archive_remove_from_state_handles_dirty_review_checkpoints(archive_env):
    module = _load_archive_module()
    manager = module.ArchiveManager(project_root=archive_env)

    state = {
        "review_checkpoints": [
            "bad-item",
            {"chapters": "1-2", "report": "r1.md", "reviewed_at": "2026-01-01"},
            {"chapters": "3-4", "report": "r2.md"},
        ]
    }
    old_reviews = [
        {"review": {"report": "r1.md"}},
        {"review": "bad-review"},
    ]

    updated = manager.remove_from_state(
        state,
        inactive_chars=[],
        resolved_threads=[],
        old_reviews=old_reviews,
    )
    checkpoints = updated.get("review_checkpoints", [])
    assert "bad-item" in checkpoints
    assert any(isinstance(x, dict) and x.get("report") == "r2.md" for x in checkpoints)
    assert not any(isinstance(x, dict) and x.get("report") == "r1.md" for x in checkpoints)


def test_restore_character_keeps_archive_when_sql_update_fails(archive_env, monkeypatch):
    module = _load_archive_module()
    manager = module.ArchiveManager(project_root=archive_env)
    manager.save_archive(
        manager.characters_archive,
        [{"id": "lixue", "name": "李雪", "archived_at": "2026-01-01T00:00:00"}],
    )

    monkeypatch.setattr(
        manager._index_manager,
        "get_entity",
        lambda entity_id: {"current_json": {"status": "archived"}},
    )

    def _boom(*args, **kwargs):
        raise RuntimeError("db failed")

    monkeypatch.setattr(manager._index_manager, "update_entity_field", _boom)

    with pytest.raises(RuntimeError):
        manager.restore_character("李雪")

    archived = manager.load_archive(manager.characters_archive)
    assert len(archived) == 1
    assert archived[0]["name"] == "李雪"


def test_restore_character_skips_non_dict_archive_items(archive_env, monkeypatch):
    module = _load_archive_module()
    manager = module.ArchiveManager(project_root=archive_env)
    manager.save_archive(
        manager.characters_archive,
        [
            "bad-item",
            {"id": "lixue", "name": "李雪", "archived_at": "2026-01-01T00:00:00"},
        ],
    )

    monkeypatch.setattr(
        manager._index_manager,
        "get_entity",
        lambda entity_id: {"current_json": {"status": "archived"}},
    )
    monkeypatch.setattr(
        manager._index_manager,
        "update_entity_field",
        lambda entity_id, field, value: True,
    )

    manager.restore_character("李雪")
    archived = manager.load_archive(manager.characters_archive)
    assert archived == ["bad-item"]


def test_archive_characters_rolls_back_archive_when_sql_update_fails(archive_env, monkeypatch):
    module = _load_archive_module()
    manager = module.ArchiveManager(project_root=archive_env)
    manager.save_archive(manager.characters_archive, [{"id": "old", "name": "旧角色"}])

    monkeypatch.setattr(
        manager._index_manager,
        "get_entity",
        lambda entity_id: {"current_json": {"status": "active"}},
    )

    def _boom(*args, **kwargs):
        raise RuntimeError("db write failed")

    monkeypatch.setattr(manager._index_manager, "update_entity_field", _boom)

    inactive_list = [
        {
            "character": {"id": "lixue", "name": "李雪", "tier": "重要", "last_appearance_chapter": 1},
            "inactive_chapters": 100,
            "last_appearance": 1,
        }
    ]
    with pytest.raises(RuntimeError):
        manager.archive_characters(inactive_list, dry_run=False)

    archived = manager.load_archive(manager.characters_archive)
    assert archived == [{"id": "old", "name": "旧角色"}]


def test_archive_load_state_supports_utf8_bom(archive_env):
    module = _load_archive_module()
    state_path = archive_env / ".webnovel" / "state.json"
    state_path.write_text("\ufeff{\"progress\":{\"current_chapter\":12}}", encoding="utf-8")
    manager = module.ArchiveManager(project_root=archive_env)
    state = manager.load_state()
    assert state["progress"]["current_chapter"] == 12

