#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
from pathlib import Path


def _load_security_utils():
    scripts_dir = Path(__file__).resolve().parents[2]
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    import security_utils

    return security_utils


def test_read_json_safe_repairs_utf8_bom(tmp_path):
    module = _load_security_utils()
    payload = {"project_info": {"title": "测试书"}, "progress": {"current_chapter": 1}}
    target = tmp_path / "state.json"
    target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8-sig")

    loaded = module.read_json_safe(target, default=None, auto_repair=True, backup_on_repair=False)
    assert loaded == payload

    raw = target.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf")
    assert json.loads(raw.decode("utf-8")) == payload


def test_read_json_safe_repairs_gbk(tmp_path):
    module = _load_security_utils()
    payload = {"project_info": {"title": "中文标题"}, "review_checkpoints": []}
    target = tmp_path / "state.json"
    target.write_bytes(json.dumps(payload, ensure_ascii=False).encode("gbk"))

    loaded = module.read_json_safe(target, default=None, auto_repair=True, backup_on_repair=False)
    assert loaded == payload

    repaired = target.read_bytes()
    assert json.loads(repaired.decode("utf-8")) == payload


def test_read_text_safe_repairs_utf8_bom(tmp_path):
    module = _load_security_utils()
    text = "## 标题\n这是正文"
    target = tmp_path / "chapter.md"
    target.write_text(text, encoding="utf-8-sig")

    loaded = module.read_text_safe(target, default="", auto_repair=True, backup_on_repair=False)
    assert loaded == text

    repaired = target.read_bytes()
    assert not repaired.startswith(b"\xef\xbb\xbf")
    assert repaired.decode("utf-8") == text


def test_read_text_safe_repairs_gbk(tmp_path):
    module = _load_security_utils()
    text = "第一章：测试\n这是中文内容"
    target = tmp_path / "outline.md"
    target.write_bytes(text.encode("gbk"))

    loaded = module.read_text_safe(target, default="", auto_repair=True, backup_on_repair=False)
    assert loaded == text

    repaired = target.read_bytes()
    assert repaired.decode("utf-8") == text
