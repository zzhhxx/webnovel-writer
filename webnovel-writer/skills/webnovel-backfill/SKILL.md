---
name: webnovel-backfill
description: 补齐缺失章节索引与追读力数据。默认先 dry-run 预览，可按需正式写入。可用于修复 Dashboard 数据缺失或历史项目落库不完整问题。
allowed-tools: Bash Read
---

# Webnovel Backfill

## 目标

- 对当前小说项目执行 `backfill-missing`。
- 默认先做 `--dry-run`，确认后再正式写入。
- 支持范围回填与仅读章节回填（不含追读力）。

## Step 0：环境确认

```bash
export WORKSPACE_ROOT="${CLAUDE_PROJECT_DIR:-$PWD}"

if [ -z "${CLAUDE_PLUGIN_ROOT}" ] || [ ! -d "${CLAUDE_PLUGIN_ROOT}/scripts" ]; then
  echo "ERROR: 未设置 CLAUDE_PLUGIN_ROOT 或缺少目录: ${CLAUDE_PLUGIN_ROOT}/scripts" >&2
  exit 1
fi
export SCRIPTS_DIR="${CLAUDE_PLUGIN_ROOT}/scripts"
```

## Step 1：解析真实项目根

```bash
export PROJECT_ROOT="$(python -X utf8 "${SCRIPTS_DIR}/webnovel.py" --project-root "${WORKSPACE_ROOT}" where)"
echo "PROJECT_ROOT=${PROJECT_ROOT}"
```

## Step 2：先执行 dry-run（默认）

```bash
python -X utf8 "${SCRIPTS_DIR}/webnovel.py" --project-root "${PROJECT_ROOT}" backfill-missing --dry-run
```

输出里重点关注：
- `missing` / `repaired`（章节索引）
- `reading_power.missing` / `reading_power.repaired`（追读力数据）

## Step 3：正式执行（用户明确要求时）

```bash
python -X utf8 "${SCRIPTS_DIR}/webnovel.py" --project-root "${PROJECT_ROOT}" backfill-missing
```

## 可选参数

- 指定章节范围：
```bash
python -X utf8 "${SCRIPTS_DIR}/webnovel.py" --project-root "${PROJECT_ROOT}" backfill-missing --from-chapter 10 --to-chapter 40 --dry-run
```

- 只补章节，不补追读力：
```bash
python -X utf8 "${SCRIPTS_DIR}/webnovel.py" --project-root "${PROJECT_ROOT}" backfill-missing --no-reading-power --dry-run
```

## Step 4：执行后校验

```bash
python -X utf8 "${SCRIPTS_DIR}/webnovel.py" --project-root "${PROJECT_ROOT}" index stats
python -X utf8 "${SCRIPTS_DIR}/webnovel.py" --project-root "${PROJECT_ROOT}" index get-recent-reading-power --limit 20
```

## 输出规范

- 先给出是否执行了 dry-run / 正式执行。
- 再给出关键统计：`missing / repaired / failed` 与 `reading_power` 对应字段。
- 若 `failed > 0`，附上失败章节列表与下一步建议（重跑范围回填）。
