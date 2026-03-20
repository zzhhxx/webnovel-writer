#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sqlite3
import sys


def test_update_state_cli_add_review_writes_checkpoint(tmp_path, monkeypatch):
    import update_state as update_state_module

    webnovel_dir = tmp_path / ".webnovel"
    webnovel_dir.mkdir(parents=True, exist_ok=True)

    state = {
        "project_info": {},
        "progress": {"current_chapter": 1, "total_words": 0},
        "protagonist_state": {
            "power": {"realm": "炼气", "layer": 1, "bottleneck": None},
            "location": "村口",
        },
        "relationships": {},
        "world_settings": {},
        "plot_threads": {},
        "review_checkpoints": [],
    }
    state_file = webnovel_dir / "state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

    # 避免在测试里创建备份目录/修改权限等非核心行为
    monkeypatch.setattr(update_state_module.StateUpdater, "backup", lambda self: True)

    report_dir = tmp_path / "review"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_file = "review/report_1_2.md"
    (tmp_path / report_file).write_text("# report", encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        ["update_state", "--project-root", str(tmp_path), "--add-review", "1-2", report_file],
    )
    update_state_module.main()

    updated = json.loads(state_file.read_text(encoding="utf-8"))
    checkpoints = updated.get("review_checkpoints")
    assert isinstance(checkpoints, list)
    assert checkpoints[-1]["chapters"] == "1-2"
    assert checkpoints[-1]["report"] == report_file

    db_path = webnovel_dir / "index.db"
    assert db_path.exists()
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT start_chapter, end_chapter, report_file FROM review_metrics WHERE start_chapter=1 AND end_chapter=2"
        ).fetchone()
    assert row is not None
    assert row[2] == report_file


def test_update_state_cli_add_review_requires_existing_report_file(tmp_path, monkeypatch):
    import update_state as update_state_module

    webnovel_dir = tmp_path / ".webnovel"
    webnovel_dir.mkdir(parents=True, exist_ok=True)

    state = {
        "project_info": {},
        "progress": {"current_chapter": 1, "total_words": 0},
        "protagonist_state": {
            "power": {"realm": "炼气", "layer": 1, "bottleneck": None},
            "location": "村口",
        },
        "relationships": {},
        "world_settings": {},
        "plot_threads": {},
        "review_checkpoints": [],
    }
    state_file = webnovel_dir / "state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(update_state_module.StateUpdater, "backup", lambda self: True)
    monkeypatch.setattr(
        sys,
        "argv",
        ["update_state", "--project-root", str(tmp_path), "--add-review", "1-2", "missing/report.md"],
    )

    try:
        update_state_module.main()
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("expected SystemExit(1)")

    updated = json.loads(state_file.read_text(encoding="utf-8"))
    assert updated.get("review_checkpoints") == []

