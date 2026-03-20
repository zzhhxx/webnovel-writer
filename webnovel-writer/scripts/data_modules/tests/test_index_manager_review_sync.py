#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sqlite3
import sys


def test_index_manager_save_review_metrics_syncs_state_checkpoint(tmp_path, monkeypatch):
    from data_modules.config import DataModulesConfig
    from data_modules import index_manager as index_manager_module

    cfg = DataModulesConfig.from_project_root(tmp_path)
    cfg.ensure_dirs()

    state = {
        "project_info": {},
        "progress": {"current_chapter": 2, "total_words": 1000},
        "protagonist_state": {
            "power": {"realm": "炼气", "layer": 1, "bottleneck": None},
            "location": {"current": "村口"},
        },
        "relationships": {},
        "world_settings": {},
        "plot_threads": {},
        "review_checkpoints": [],
        "custom_block": {"keep": "yes"},
    }
    cfg.state_file.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8-sig")

    report = tmp_path / "审查报告" / "第1-2章审查报告.md"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("# report", encoding="utf-8")

    payload = {
        "start_chapter": 1,
        "end_chapter": 2,
        "overall_score": 80.0,
        "dimension_scores": {"节奏控制": 8},
        "severity_counts": {"high": 1},
        "critical_issues": [],
        "report_file": "审查报告/第1-2章审查报告.md",
        "notes": "sync test",
    }

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "index_manager",
            "--project-root",
            str(tmp_path),
            "save-review-metrics",
            "--data",
            json.dumps(payload, ensure_ascii=False),
        ],
    )
    index_manager_module.main()

    updated = json.loads(cfg.state_file.read_text(encoding="utf-8"))
    checkpoints = updated.get("review_checkpoints", [])
    assert checkpoints
    assert checkpoints[-1]["chapters"] == "1-2"
    assert checkpoints[-1]["report"] == "审查报告/第1-2章审查报告.md"
    assert updated.get("custom_block", {}).get("keep") == "yes"
    assert updated.get("progress", {}).get("total_words") == 1000

    with sqlite3.connect(cfg.index_db) as conn:
        row = conn.execute(
            "SELECT start_chapter, end_chapter, report_file FROM review_metrics WHERE start_chapter=1 AND end_chapter=2"
        ).fetchone()
    assert row is not None
    assert row[2] == "审查报告/第1-2章审查报告.md"


def test_index_manager_save_review_metrics_rejects_missing_report_file(tmp_path, monkeypatch, capsys):
    from data_modules.config import DataModulesConfig
    from data_modules import index_manager as index_manager_module

    cfg = DataModulesConfig.from_project_root(tmp_path)
    cfg.ensure_dirs()
    cfg.state_file.write_text(
        json.dumps({"review_checkpoints": [], "progress": {"current_chapter": 2}}, ensure_ascii=False),
        encoding="utf-8-sig",
    )

    payload = {
        "start_chapter": 1,
        "end_chapter": 2,
        "overall_score": 80.0,
        "report_file": "审查报告/不存在.md",
    }

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "index_manager",
            "--project-root",
            str(tmp_path),
            "save-review-metrics",
            "--data",
            json.dumps(payload, ensure_ascii=False),
        ],
    )
    index_manager_module.main()

    output = capsys.readouterr().out
    assert "INVALID_REPORT_FILE" in output

    updated = json.loads(cfg.state_file.read_text(encoding="utf-8"))
    assert updated.get("review_checkpoints", []) == []

    with sqlite3.connect(cfg.index_db) as conn:
        row = conn.execute(
            "SELECT start_chapter, end_chapter FROM review_metrics WHERE start_chapter=1 AND end_chapter=2"
        ).fetchone()
    assert row is None
