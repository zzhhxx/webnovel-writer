#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
from pathlib import Path

import pytest


def _ensure_scripts_on_path() -> None:
    scripts_dir = Path(__file__).resolve().parents[2]
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))


def test_load_json_arg_accepts_utf8_sig_file(tmp_path):
    _ensure_scripts_on_path()
    from data_modules.cli_args import load_json_arg

    payload = {"title": "测试", "chapter": 1}
    p = tmp_path / "payload.json"
    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8-sig")

    loaded = load_json_arg(f"@{p}")
    assert loaded == payload


def test_load_json_arg_accepts_gbk_file(tmp_path):
    _ensure_scripts_on_path()
    from data_modules.cli_args import load_json_arg

    payload = {"title": "中文标题", "chapter": 2}
    p = tmp_path / "payload.json"
    p.write_bytes(json.dumps(payload, ensure_ascii=False).encode("gbk"))

    loaded = load_json_arg(f"@{p}")
    assert loaded == payload


def test_load_json_arg_missing_file_raises(tmp_path):
    _ensure_scripts_on_path()
    from data_modules.cli_args import load_json_arg

    missing = tmp_path / "missing.json"
    with pytest.raises(FileNotFoundError):
        load_json_arg(f"@{missing}")

