import { memo, useState, useEffect, useCallback, useRef, lazy, Suspense } from 'react'
import { fetchJSON, subscribeSSE } from './api.js'

const GraphPage = lazy(() => import('./pages/GraphPage.jsx'))

// ====================================================================
// 主应用
// ====================================================================

export default function App() {
    const [page, setPage] = useState('dashboard')
    const [projectInfo, setProjectInfo] = useState(null)
    const [stateRefreshKey, setStateRefreshKey] = useState(0)
    const [dbRefreshKey, setDbRefreshKey] = useState(0)
    const [fileRefreshKey, setFileRefreshKey] = useState(0)
    const [connected, setConnected] = useState(false)
    const [isPageVisible, setIsPageVisible] = useState(
        () => typeof document === 'undefined' || document.visibilityState === 'visible',
    )
    const refreshTimerRef = useRef({ state: null, db: null, files: null })
    const lastRefreshAtRef = useRef({ state: 0, db: 0, files: 0 })
    const dbRevisionRef = useRef('')
    const pendingRefreshRef = useRef({ state: false, db: false, files: false })
    const projectInfoSignatureRef = useRef('')
    const traceSeqRef = useRef(0)
    const [debugEnabled, setDebugEnabled] = useState(() => loadRefreshDebugEnabled())
    const [debugPanelOpen, setDebugPanelOpen] = useState(false)
    const [refreshDebug, setRefreshDebug] = useState(() => createRefreshDebugState())
    const [refreshTrace, setRefreshTrace] = useState([])

    const recordRefreshEvent = useCallback((scope, phase, source) => {
        if (!debugEnabled) return
        const target = scope === 'db' || scope === 'files' ? scope : 'state'
        const ts = Date.now()
        setRefreshDebug((prev) => {
            const oldScope = prev[target] || { queued: 0, fired: 0, lastAt: 0, lastSource: '-' }
            const nextScope = { ...oldScope }
            if (phase === 'queued') nextScope.queued += 1
            if (phase === 'fired') nextScope.fired += 1
            nextScope.lastAt = ts
            nextScope.lastSource = source || phase
            return { ...prev, [target]: nextScope }
        })
        const id = traceSeqRef.current + 1
        traceSeqRef.current = id
        setRefreshTrace((prev) => [
            { id, ts, scope: target, phase, source: source || '-' },
            ...prev,
        ].slice(0, 40))
    }, [debugEnabled])

    useEffect(() => {
        saveRefreshDebugEnabled(debugEnabled)
        if (debugEnabled) return
        setDebugPanelOpen(false)
        setRefreshDebug(createRefreshDebugState())
        setRefreshTrace([])
        traceSeqRef.current = 0
    }, [debugEnabled])

    useEffect(() => {
        const controller = new AbortController()
        fetchJSON('/api/project/info', {}, { signal: controller.signal })
            .then((data) => {
                const nextSig = createSimpleSignature(data)
                if (projectInfoSignatureRef.current === nextSig) return
                projectInfoSignatureRef.current = nextSig
                setProjectInfo(data)
            })
            .catch((err) => {
                if (isAbortError(err)) return
                setProjectInfo(null)
            })
        return () => controller.abort()
    }, [stateRefreshKey])

    const scheduleRefresh = useCallback((scope, source = 'unknown') => {
        const target = scope === 'db' || scope === 'files' ? scope : 'state'
        const now = Date.now()
        const minIntervalMs = 1500
        const elapsed = now - (lastRefreshAtRef.current[target] || 0)

        const bump = () => {
            lastRefreshAtRef.current[target] = Date.now()
            recordRefreshEvent(target, 'fired', source)
            if (target === 'db') {
                setDbRefreshKey(k => k + 1)
                return
            }
            if (target === 'files') {
                setFileRefreshKey(k => k + 1)
                return
            }
            setStateRefreshKey(k => k + 1)
        }

        if (elapsed >= minIntervalMs) {
            bump()
            return
        }

        if (refreshTimerRef.current[target]) return

        const waitMs = Math.max(0, minIntervalMs - elapsed)
        recordRefreshEvent(target, 'queued', `${source}|throttle`)
        refreshTimerRef.current[target] = setTimeout(() => {
            refreshTimerRef.current[target] = null
            bump()
        }, waitMs)
    }, [recordRefreshEvent])

    const requestRefresh = useCallback((scope, source = 'unknown') => {
        const target = scope === 'db' || scope === 'files' ? scope : 'state'
        if (target === 'db' && !isDbRefreshActivePage(page)) {
            pendingRefreshRef.current.db = true
            recordRefreshEvent(target, 'queued', `${source}|inactive-page`)
            return
        }
        if (target === 'files' && page !== 'files') {
            pendingRefreshRef.current.files = true
            recordRefreshEvent(target, 'queued', `${source}|inactive-page`)
            return
        }
        if (!isPageVisible) {
            pendingRefreshRef.current[target] = true
            recordRefreshEvent(target, 'queued', `${source}|hidden`)
            return
        }
        scheduleRefresh(target, source)
    }, [isPageVisible, page, recordRefreshEvent, scheduleRefresh])

    useEffect(() => {
        if (typeof document === 'undefined') return () => { }

        const onVisibilityChange = () => {
            const visible = document.visibilityState === 'visible'
            setIsPageVisible(visible)
            if (!visible) return
            for (const scope of ['state', 'db', 'files']) {
                if (scope === 'db' && !isDbRefreshActivePage(page)) continue
                if (scope === 'files' && page !== 'files') continue
                if (!pendingRefreshRef.current[scope]) continue
                pendingRefreshRef.current[scope] = false
                scheduleRefresh(scope, 'visibility-resume')
            }
        }

        document.addEventListener('visibilitychange', onVisibilityChange)
        return () => document.removeEventListener('visibilitychange', onVisibilityChange)
    }, [page, scheduleRefresh])

    useEffect(() => {
        if (!isPageVisible) return
        const scopeChecks = [
            ['state', true],
            ['db', isDbRefreshActivePage(page)],
            ['files', page === 'files'],
        ]
        for (const [scope, enabled] of scopeChecks) {
            if (!enabled) continue
            if (!pendingRefreshRef.current[scope]) continue
            pendingRefreshRef.current[scope] = false
            scheduleRefresh(scope, 'page-activate')
        }
    }, [isPageVisible, page, scheduleRefresh])

    // SSE 订阅
    useEffect(() => {
        const refreshableStateFiles = new Set(['state.json', 'workflow_state.json'])
        const unsub = subscribeSSE(
            (evt) => {
                const file = String(evt?.file || '').toLowerCase()
                if (refreshableStateFiles.has(file)) {
                    requestRefresh('state', `sse:${file}`)
                    return
                }
                requestRefresh('files', `sse:${file || 'unknown'}`)
            },
            {
                onOpen: () => setConnected(true),
                onError: () => setConnected(false),
            },
        )
        return () => {
            unsub()
            setConnected(false)
            Object.values(refreshTimerRef.current).forEach((timerId) => {
                if (timerId) clearTimeout(timerId)
            })
            refreshTimerRef.current = { state: null, db: null, files: null }
        }
    }, [requestRefresh])

    // 数据库变化轮询：
    // watcher 为避免自触发回路忽略了 index.db*，这里低频轮询 revision 用于感知“仅数据库更新”的场景。
    useEffect(() => {
        if (!isPageVisible || !isDbRefreshActivePage(page)) {
            return () => { }
        }

        let disposed = false
        let activeController = null
        let isChecking = false

        const checkDbRevision = () => {
            if (isChecking) return
            isChecking = true
            const controller = new AbortController()
            activeController = controller
            fetchJSON('/api/db/revision', {}, { signal: controller.signal })
                .then((data) => {
                    if (disposed) return
                    const nextRevision = String(data?.revision || '')
                    const prevRevision = dbRevisionRef.current
                    if (!prevRevision) {
                        dbRevisionRef.current = nextRevision
                        return
                    }
                    if (prevRevision !== nextRevision) {
                        dbRevisionRef.current = nextRevision
                        requestRefresh('db', 'db-revision-poll')
                        return
                    }
                    dbRevisionRef.current = nextRevision
                })
                .catch((err) => {
                    if (!isAbortError(err)) {
                        // ignore non-critical polling errors
                    }
                })
                .finally(() => {
                    isChecking = false
                    if (activeController === controller) activeController = null
                })
        }

        checkDbRevision()
        const timer = setInterval(checkDbRevision, 5000)
        return () => {
            disposed = true
            clearInterval(timer)
            if (activeController) activeController.abort()
        }
    }, [isPageVisible, page, requestRefresh])

    const title = projectInfo?.project_info?.title || '未加载'
    const pendingSnapshot = pendingRefreshRef.current

    return (
        <div className="app-layout">
            <aside className="sidebar">
                <div className="sidebar-header">
                    <h1>PIXEL WRITER HUB</h1>
                    <div className="subtitle">{title}</div>
                </div>
                <nav className="sidebar-nav">
                    {NAV_ITEMS.map(item => (
                        <button
                            key={item.id}
                            className={`nav-item ${page === item.id ? 'active' : ''}`}
                            onClick={() => setPage(item.id)}
                        >
                            <span className="icon">{item.icon}</span>
                            <span>{item.label}</span>
                        </button>
                    ))}
                </nav>
                <div className="live-indicator">
                    <span className={`live-dot ${connected ? '' : 'disconnected'}`} />
                    {connected ? '实时同步中' : '未连接'}
                </div>
                <div style={{ marginTop: 8 }}>
                    <button
                        className={`filter-btn ${debugEnabled ? 'active' : ''}`}
                        type="button"
                        onClick={() => setDebugEnabled(v => !v)}
                    >
                        {debugEnabled ? '关闭调试' : '开启调试'}
                    </button>
                </div>
                {debugEnabled ? (
                    <RefreshDebugPanel
                        open={debugPanelOpen}
                        onToggle={() => setDebugPanelOpen(v => !v)}
                        page={page}
                        isPageVisible={isPageVisible}
                        stats={refreshDebug}
                        trace={refreshTrace}
                        pending={pendingSnapshot}
                    />
                ) : null}
            </aside>

            <main className="main-content">
                {page === 'dashboard' && (
                    <DashboardPage
                        data={projectInfo}
                        dbRefreshSignal={dbRefreshKey}
                    />
                )}
                {page === 'entities' && <EntitiesPage refreshSignal={dbRefreshKey} />}
                {page === 'graph' && (
                    <Suspense fallback={<div className="loading">图谱页面加载中…</div>}>
                        <GraphPage refreshSignal={dbRefreshKey} />
                    </Suspense>
                )}
                {page === 'chapters' && <ChaptersPage refreshSignal={dbRefreshKey} />}
                {page === 'files' && <FilesPage refreshSignal={fileRefreshKey} stateRefreshSignal={stateRefreshKey} />}
                {page === 'reading' && <ReadingPowerPage refreshSignal={dbRefreshKey} />}
            </main>
        </div>
    )
}

const NAV_ITEMS = [
    { id: 'dashboard', icon: '📊', label: '数据总览' },
    { id: 'entities', icon: '👤', label: '设定词典' },
    { id: 'graph', icon: '🕸️', label: '关系图谱' },
    { id: 'chapters', icon: '📝', label: '章节一览' },
    { id: 'files', icon: '📁', label: '文档浏览' },
    { id: 'reading', icon: '🔥', label: '追读力' },
]

const FULL_DATA_GROUPS = [
    { key: 'entities', title: '实体', columns: ['id', 'canonical_name', 'type', 'tier', 'first_appearance', 'last_appearance'], domain: 'core' },
    { key: 'chapters', title: '章节', columns: ['chapter', 'title', 'word_count', 'location', 'characters'], domain: 'core' },
    { key: 'scenes', title: '场景', columns: ['chapter', 'scene_index', 'location', 'time', 'summary'], domain: 'core' },
    { key: 'aliases', title: '别名', columns: ['alias', 'entity_id', 'entity_type'], domain: 'core' },
    { key: 'stateChanges', title: '状态变化', columns: ['entity_id', 'field', 'old_value', 'new_value', 'chapter'], domain: 'core' },
    { key: 'relationships', title: '关系', columns: ['from_entity', 'to_entity', 'type', 'chapter', 'description'], domain: 'network' },
    { key: 'relationshipEvents', title: '关系事件', columns: ['from_entity', 'to_entity', 'type', 'chapter', 'action', 'description'], domain: 'network' },
    { key: 'readingPower', title: '追读力', columns: ['chapter', 'hook_type', 'hook_strength', 'is_transition', 'override_count', 'debt_balance'], domain: 'network' },
    { key: 'overrides', title: 'Override 合约', columns: ['chapter', 'constraint_type', 'constraint_id', 'due_chapter', 'status'], domain: 'network' },
    { key: 'debts', title: '追读债务', columns: ['id', 'debt_type', 'current_amount', 'interest_rate', 'due_chapter', 'status'], domain: 'network' },
    { key: 'debtEvents', title: '债务事件', columns: ['debt_id', 'event_type', 'amount', 'chapter', 'note'], domain: 'network' },
    { key: 'reviewMetrics', title: '审查指标', columns: ['start_chapter', 'end_chapter', 'overall_score', 'severity_counts', 'created_at'], domain: 'quality' },
    { key: 'invalidFacts', title: '无效事实', columns: ['source_type', 'source_id', 'reason', 'status', 'chapter_discovered'], domain: 'quality' },
    { key: 'checklistScores', title: '写作清单评分', columns: ['chapter', 'template', 'score', 'completion_rate', 'completed_items', 'total_items'], domain: 'quality' },
    { key: 'ragQueries', title: 'RAG 查询日志', columns: ['query_type', 'query', 'results_count', 'latency_ms', 'chapter', 'created_at'], domain: 'ops' },
    { key: 'toolStats', title: '工具调用统计', columns: ['tool_name', 'success', 'retry_count', 'error_code', 'chapter', 'created_at'], domain: 'ops' },
]

const FULL_DATA_DOMAINS = [
    { id: 'overview', label: '总览' },
    { id: 'core', label: '基础档案' },
    { id: 'network', label: '关系与剧情' },
    { id: 'quality', label: '质量审查' },
    { id: 'ops', label: 'RAG 与工具' },
]

const FULL_DATA_REQUESTS = {
    entities: { path: '/api/entities' },
    chapters: { path: '/api/chapters' },
    scenes: { path: '/api/scenes', params: { limit: 200 } },
    aliases: { path: '/api/aliases' },
    stateChanges: { path: '/api/state-changes', params: { limit: 120 } },
    relationships: { path: '/api/relationships', params: { limit: 300 } },
    relationshipEvents: { path: '/api/relationship-events', params: { limit: 200 } },
    readingPower: { path: '/api/reading-power', params: { limit: 100 } },
    overrides: { path: '/api/overrides', params: { limit: 120 } },
    debts: { path: '/api/debts', params: { limit: 120 } },
    debtEvents: { path: '/api/debt-events', params: { limit: 150 } },
    reviewMetrics: { path: '/api/review-metrics', params: { limit: 50 } },
    invalidFacts: { path: '/api/invalid-facts', params: { limit: 120 } },
    checklistScores: { path: '/api/checklist-scores', params: { limit: 120 } },
    ragQueries: { path: '/api/rag-queries', params: { limit: 150 } },
    toolStats: { path: '/api/tool-stats', params: { limit: 200 } },
}


// ====================================================================
// 页面 1：数据总览
// ====================================================================

function DashboardPage({ data, dbRefreshSignal }) {
    if (!data) return <div className="loading">加载中…</div>

    const info = data.project_info || {}
    const progress = data.progress || {}
    const protagonist = data.protagonist_state || {}
    const strand = data.strand_tracker || {}
    const foreshadowing = data.plot_threads?.foreshadowing || []

    const totalWords = progress.total_words || 0
    const targetWords = info.target_words || 2000000
    const pct = targetWords > 0 ? Math.min(100, (totalWords / targetWords * 100)).toFixed(1) : 0

    const unresolvedForeshadow = foreshadowing.filter(f => {
        const s = (f.status || '').toLowerCase()
        return s !== '已回收' && s !== '已兑现' && s !== 'resolved'
    })

    // Strand 历史统计
    const history = strand.history || []
    const strandCounts = { quest: 0, fire: 0, constellation: 0 }
    history.forEach(h => {
        const key = h?.dominant || h?.strand
        if (strandCounts[key] !== undefined) strandCounts[key]++
    })
    const total = history.length || 1

    return (
        <>
            <div className="page-header">
                <h2>📊 数据总览</h2>
                <span className="card-badge badge-blue">{info.genre || '未知题材'}</span>
            </div>

            <div className="dashboard-grid">
                <div className="card stat-card">
                    <span className="stat-label">总字数</span>
                    <span className="stat-value">{formatNumber(totalWords)}</span>
                    <span className="stat-sub">目标 {formatNumber(targetWords)} 字 · {pct}%</span>
                    <div className="progress-track">
                        <div className="progress-fill" style={{ width: `${pct}%` }} />
                    </div>
                </div>

                <div className="card stat-card">
                    <span className="stat-label">当前章节</span>
                    <span className="stat-value">第 {progress.current_chapter || 0} 章</span>
                    <span className="stat-sub">目标 {info.target_chapters || '?'} 章 · 卷 {progress.current_volume || 1}</span>
                </div>

                <div className="card stat-card">
                    <span className="stat-label">主角状态</span>
                    <span className="stat-value plain">{protagonist.name || '未设定'}</span>
                    <span className="stat-sub">
                        {protagonist.power?.realm || protagonist.realm || '未知境界'}
                        {(protagonist.location?.current || protagonist.location)
                            ? ` · ${protagonist.location?.current || protagonist.location}`
                            : ''}
                    </span>
                </div>

                <div className="card stat-card">
                    <span className="stat-label">未回收伏笔</span>
                    <span className="stat-value" style={{ color: unresolvedForeshadow.length > 10 ? 'var(--accent-red)' : 'var(--accent-amber)' }}>
                        {unresolvedForeshadow.length}
                    </span>
                    <span className="stat-sub">总计 {foreshadowing.length} 条伏笔</span>
                </div>
            </div>

            {/* Strand Weave 比例 */}
            <div className="card dashboard-section-card">
                <div className="card-header">
                    <span className="card-title">Strand Weave 节奏分布</span>
                    <span className="card-badge badge-purple">{strand.current_dominant || '?'}</span>
                </div>
                <div className="strand-bar">
                    <div className="segment strand-quest" style={{ width: `${(strandCounts.quest / total * 100).toFixed(1)}%` }} />
                    <div className="segment strand-fire" style={{ width: `${(strandCounts.fire / total * 100).toFixed(1)}%` }} />
                    <div className="segment strand-constellation" style={{ width: `${(strandCounts.constellation / total * 100).toFixed(1)}%` }} />
                </div>
                <div className="strand-legend">
                    <span>🔵 Quest {(strandCounts.quest / total * 100).toFixed(0)}%</span>
                    <span>🔴 Fire {(strandCounts.fire / total * 100).toFixed(0)}%</span>
                    <span>🟣 Constellation {(strandCounts.constellation / total * 100).toFixed(0)}%</span>
                </div>
            </div>

            {/* 伏笔列表 */}
            {unresolvedForeshadow.length > 0 ? (
                <div className="card dashboard-section-card">
                    <div className="card-header">
                        <span className="card-title">⚠️ 待回收伏笔 (Top 20)</span>
                    </div>
                    <div className="table-wrap">
                        <table className="data-table">
                            <thead><tr><th>内容</th><th>状态</th><th>埋设章</th></tr></thead>
                            <tbody>
                                {unresolvedForeshadow.slice(0, 20).map((f, i) => (
                                    <tr key={i}>
                                        <td className="truncate" style={{ maxWidth: 400 }}>{f.content || f.description || '—'}</td>
                                        <td><span className="card-badge badge-amber">{f.status || '未知'}</span></td>
                                        <td>{f.chapter || f.planted_chapter || '—'}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>
            ) : null}

            <MergedDataView refreshSignal={dbRefreshSignal} />
        </>
    )
}


// ====================================================================
// 页面 2：设定词典
// ====================================================================

function EntitiesPage({ refreshSignal }) {
    const [entities, setEntities] = useState([])
    const [typeFilter, setTypeFilter] = useState('')
    const [selected, setSelected] = useState(null)
    const [changes, setChanges] = useState([])
    const [loadError, setLoadError] = useState('')
    const [reloadKey, setReloadKey] = useState(0)

    useEffect(() => {
        const controller = new AbortController()
        setLoadError('')
        fetchJSON('/api/entities', {}, { signal: controller.signal })
            .then((rows) => setEntities(Array.isArray(rows) ? rows : []))
            .catch((err) => {
                if (isAbortError(err)) return
                setEntities([])
                setLoadError('实体列表加载失败，请重试')
            })
        return () => controller.abort()
    }, [refreshSignal, reloadKey])

    useEffect(() => {
        if (!selected) {
            setChanges([])
            return
        }
        const controller = new AbortController()
        fetchJSON('/api/state-changes', { entity: selected.id, limit: 30 }, { signal: controller.signal })
            .then((rows) => setChanges(Array.isArray(rows) ? rows : []))
            .catch((err) => {
                if (isAbortError(err)) return
                setChanges([])
            })
        return () => controller.abort()
    }, [selected, refreshSignal])

    const types = [...new Set(entities.map(e => e.type))].sort()
    const filteredEntities = typeFilter ? entities.filter(e => e.type === typeFilter) : entities
    const entityVirtual = useVirtualRows(filteredEntities.length, {
        enabled: filteredEntities.length > 160,
        rowHeight: 44,
        maxHeight: 560,
    })
    const visibleEntities = filteredEntities.slice(entityVirtual.start, entityVirtual.end)

    return (
        <>
            <div className="page-header">
                <h2>👤 设定词典</h2>
                <span className="card-badge badge-green">{filteredEntities.length} / {entities.length} 个实体</span>
            </div>

            <div className="filter-group">
                <button className={`filter-btn ${typeFilter === '' ? 'active' : ''}`} onClick={() => setTypeFilter('')}>全部</button>
                {types.map(t => (
                    <button key={t} className={`filter-btn ${typeFilter === t ? 'active' : ''}`} onClick={() => setTypeFilter(t)}>{t}</button>
                ))}
            </div>

            <ErrorNotice
                message={loadError}
                onRetry={() => setReloadKey(v => v + 1)}
            />

            <div className="split-layout">
                <div className="split-main">
                    <div className="card">
                        <div
                            className="table-wrap"
                            ref={entityVirtual.containerRef}
                            onScroll={entityVirtual.onScroll}
                            style={entityVirtual.tableWrapStyle}
                        >
                            <table className="data-table">
                                <thead><tr><th>名称</th><th>类型</th><th>层级</th><th>首现</th><th>末现</th></tr></thead>
                                <tbody>
                                    {entityVirtual.topPad > 0 ? (
                                        <tr aria-hidden>
                                            <td colSpan={5} style={{ height: entityVirtual.topPad, padding: 0, border: 'none' }} />
                                        </tr>
                                    ) : null}
                                    {visibleEntities.map(e => (
                                        <tr
                                            key={e.id}
                                            role="button"
                                            tabIndex={0}
                                            className={`entity-row ${selected?.id === e.id ? 'selected' : ''}`}
                                            onKeyDown={evt => (evt.key === 'Enter' || evt.key === ' ') && (evt.preventDefault(), setSelected(e))}
                                            onClick={() => setSelected(e)}
                                        >
                                            <td className={e.is_protagonist ? 'entity-name protagonist' : 'entity-name'}>
                                                {e.canonical_name} {e.is_protagonist ? '⭐' : ''}
                                            </td>
                                            <td><span className="card-badge badge-blue">{e.type}</span></td>
                                            <td>{e.tier}</td>
                                            <td>{e.first_appearance || '—'}</td>
                                            <td>{e.last_appearance || '—'}</td>
                                        </tr>
                                    ))}
                                    {entityVirtual.bottomPad > 0 ? (
                                        <tr aria-hidden>
                                            <td colSpan={5} style={{ height: entityVirtual.bottomPad, padding: 0, border: 'none' }} />
                                        </tr>
                                    ) : null}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                {selected && (
                    <div className="split-side">
                        <div className="card">
                            <div className="card-header">
                                <span className="card-title">{selected.canonical_name}</span>
                                <span className="card-badge badge-purple">{selected.tier}</span>
                            </div>
                            <div className="entity-detail">
                                <p><strong>类型：</strong>{selected.type}</p>
                                <p><strong>ID：</strong><code>{selected.id}</code></p>
                                {selected.desc && <p className="entity-desc">{selected.desc}</p>}
                                {selected.current_json && (
                                    <div className="entity-current-block">
                                        <strong>当前状态：</strong>
                                        <pre className="entity-json">
                                            {formatJSON(selected.current_json)}
                                        </pre>
                                    </div>
                                )}
                            </div>
                            {changes.length > 0 ? (
                                <div className="entity-history">
                                    <div className="card-title">状态变化历史</div>
                                    <div className="table-wrap">
                                        <table className="data-table">
                                            <thead><tr><th>章</th><th>字段</th><th>变化</th></tr></thead>
                                            <tbody>
                                                {changes.map((c, i) => (
                                                    <tr key={i}>
                                                        <td>{c.chapter}</td>
                                                        <td>{c.field}</td>
                                                        <td>{c.old_value} → {c.new_value}</td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            ) : null}
                        </div>
                    </div>
                )}
            </div>
        </>
    )
}

// ====================================================================
// 页面 4：章节一览
// ====================================================================

function ChaptersPage({ refreshSignal }) {
    const [chapters, setChapters] = useState([])
    const [loadError, setLoadError] = useState('')
    const [reloadKey, setReloadKey] = useState(0)

    useEffect(() => {
        const controller = new AbortController()
        setLoadError('')
        fetchJSON('/api/chapters', {}, { signal: controller.signal })
            .then((rows) => setChapters(Array.isArray(rows) ? rows : []))
            .catch((err) => {
                if (isAbortError(err)) return
                setChapters([])
                setLoadError('章节数据加载失败，请重试')
            })
        return () => controller.abort()
    }, [refreshSignal, reloadKey])

    const totalWords = chapters.reduce((s, c) => s + (c.word_count || 0), 0)
    const chapterVirtual = useVirtualRows(chapters.length, {
        enabled: chapters.length > 180,
        rowHeight: 44,
        maxHeight: 560,
    })
    const visibleChapters = chapters.slice(chapterVirtual.start, chapterVirtual.end)

    return (
        <>
            <div className="page-header">
                <h2>📝 章节一览</h2>
                <span className="card-badge badge-green">{chapters.length} 章 · {formatNumber(totalWords)} 字</span>
            </div>
            <ErrorNotice message={loadError} onRetry={() => setReloadKey(v => v + 1)} />
            <div className="card">
                <div
                    className="table-wrap"
                    ref={chapterVirtual.containerRef}
                    onScroll={chapterVirtual.onScroll}
                    style={chapterVirtual.tableWrapStyle}
                >
                    <table className="data-table">
                        <thead><tr><th>章节</th><th>标题</th><th>字数</th><th>地点</th><th>角色</th></tr></thead>
                        <tbody>
                            {chapterVirtual.topPad > 0 ? (
                                <tr aria-hidden>
                                    <td colSpan={5} style={{ height: chapterVirtual.topPad, padding: 0, border: 'none' }} />
                                </tr>
                            ) : null}
                            {visibleChapters.map(c => (
                                <tr key={c.chapter}>
                                    <td className="chapter-no">第 {c.chapter} 章</td>
                                    <td>{c.title || '—'}</td>
                                    <td>{formatNumber(c.word_count || 0)}</td>
                                    <td>{c.location || '—'}</td>
                                    <td className="truncate chapter-characters">{c.characters || '—'}</td>
                                </tr>
                            ))}
                            {chapterVirtual.bottomPad > 0 ? (
                                <tr aria-hidden>
                                    <td colSpan={5} style={{ height: chapterVirtual.bottomPad, padding: 0, border: 'none' }} />
                                </tr>
                            ) : null}
                        </tbody>
                    </table>
                </div>
                {chapters.length === 0 ? <div className="empty-state"><div className="empty-icon">📭</div><p>暂无章节数据</p></div> : null}
            </div>
        </>
    )
}


// ====================================================================
// 页面 5：文档浏览
// ====================================================================

function FilesPage({ refreshSignal, stateRefreshSignal }) {
    const [tree, setTree] = useState({})
    const [selectedPath, setSelectedPath] = useState(null)
    const [content, setContent] = useState('')
    const [treeError, setTreeError] = useState('')
    const [readError, setReadError] = useState('')
    const [reloadKey, setReloadKey] = useState(0)
    const treeSignatureRef = useRef('')
    const contentSignatureRef = useRef('')
    const stateDrivenReadSignal = isStateMirrorFile(selectedPath) ? stateRefreshSignal : 0

    useEffect(() => {
        const controller = new AbortController()
        setTreeError('')
        fetchJSON('/api/files/tree', {}, { signal: controller.signal })
            .then((rows) => {
                const nextTree = rows && typeof rows === 'object' ? rows : {}
                const nextSig = createSimpleSignature(nextTree)
                if (treeSignatureRef.current === nextSig) return
                treeSignatureRef.current = nextSig
                setTree(nextTree)
            })
            .catch((err) => {
                if (isAbortError(err)) return
                treeSignatureRef.current = ''
                setTree({})
                setTreeError('文件树加载失败，请重试')
            })
        return () => controller.abort()
    }, [refreshSignal, reloadKey])

    useEffect(() => {
        if (!selectedPath) {
            setReadError('')
            setContent('')
            contentSignatureRef.current = ''
            return
        }
        const controller = new AbortController()
        setReadError('')
        fetchJSON('/api/files/read', { path: selectedPath }, { signal: controller.signal })
            .then((d) => {
                const nextContent = String(d?.content || '')
                const nextSig = `${selectedPath}:${nextContent.length}:${nextContent.slice(0, 240)}:${nextContent.slice(-240)}`
                if (contentSignatureRef.current === nextSig) return
                contentSignatureRef.current = nextSig
                setContent(nextContent)
            })
            .catch((err) => {
                if (isAbortError(err)) return
                setContent('[读取失败]')
                setReadError(`文件读取失败：${selectedPath}`)
            })
        return () => controller.abort()
    }, [selectedPath, refreshSignal, stateDrivenReadSignal, reloadKey])

    useEffect(() => {
        if (selectedPath) return
        const first = findFirstFilePath(tree)
        if (first) setSelectedPath(first)
    }, [tree, selectedPath])

    return (
        <>
            <div className="page-header">
                <h2>📁 文档浏览</h2>
            </div>
            <ErrorNotice
                message={treeError}
                onRetry={() => setReloadKey(v => v + 1)}
            />
            <div className="file-layout">
                <div className="file-tree-pane">
                    {Object.entries(tree).map(([folder, items]) => (
                        <div key={folder} className="folder-block">
                            <div className="folder-title">📂 {folder}</div>
                            <ul className="file-tree">
                                <TreeNodes items={items} selected={selectedPath} onSelect={setSelectedPath} />
                            </ul>
                        </div>
                    ))}
                </div>
                <div className="file-content-pane">
                    {selectedPath ? (
                        <div>
                            <div className="selected-path">{selectedPath}</div>
                            <ErrorNotice
                                message={readError}
                                onRetry={() => setReloadKey(v => v + 1)}
                            />
                            <div className="file-preview">{content}</div>
                        </div>
                    ) : (
                        <div className="empty-state"><div className="empty-icon">📄</div><p>选择左侧文件以预览内容</p></div>
                    )}
                </div>
            </div>
        </>
    )
}


// ====================================================================
// 页面 6：追读力
// ====================================================================

function ReadingPowerPage({ refreshSignal }) {
    const [data, setData] = useState([])
    const [loadError, setLoadError] = useState('')
    const [reloadKey, setReloadKey] = useState(0)

    useEffect(() => {
        const controller = new AbortController()
        setLoadError('')
        fetchJSON('/api/reading-power', { limit: 50 }, { signal: controller.signal })
            .then((rows) => setData(Array.isArray(rows) ? rows : []))
            .catch((err) => {
                if (isAbortError(err)) return
                setData([])
                setLoadError('追读力数据加载失败，请重试')
            })
        return () => controller.abort()
    }, [refreshSignal, reloadKey])
    const readingVirtual = useVirtualRows(data.length, {
        enabled: data.length > 180,
        rowHeight: 44,
        maxHeight: 560,
    })
    const visibleRows = data.slice(readingVirtual.start, readingVirtual.end)

    return (
        <>
            <div className="page-header">
                <h2>🔥 追读力分析</h2>
                <span className="card-badge badge-amber">{data.length} 章数据</span>
            </div>
            <ErrorNotice message={loadError} onRetry={() => setReloadKey(v => v + 1)} />
            <div className="card">
                <div
                    className="table-wrap"
                    ref={readingVirtual.containerRef}
                    onScroll={readingVirtual.onScroll}
                    style={readingVirtual.tableWrapStyle}
                >
                    <table className="data-table">
                        <thead><tr><th>章节</th><th>钩子类型</th><th>钩子强度</th><th>过渡章</th><th>Override</th><th>债务余额</th></tr></thead>
                        <tbody>
                            {readingVirtual.topPad > 0 ? (
                                <tr aria-hidden>
                                    <td colSpan={6} style={{ height: readingVirtual.topPad, padding: 0, border: 'none' }} />
                                </tr>
                            ) : null}
                            {visibleRows.map(r => (
                                <tr key={r.chapter}>
                                    <td className="chapter-no">第 {r.chapter} 章</td>
                                    <td>{r.hook_type || '—'}</td>
                                    <td>
                                        <span className={`card-badge ${r.hook_strength === 'strong' ? 'badge-green' : r.hook_strength === 'weak' ? 'badge-red' : 'badge-amber'}`}>
                                            {r.hook_strength || '—'}
                                        </span>
                                    </td>
                                    <td>{r.is_transition ? '✅' : '—'}</td>
                                    <td>{r.override_count || 0}</td>
                                    <td className={r.debt_balance > 0 ? 'debt-positive' : 'debt-normal'}>{(r.debt_balance || 0).toFixed(2)}</td>
                                </tr>
                            ))}
                            {readingVirtual.bottomPad > 0 ? (
                                <tr aria-hidden>
                                    <td colSpan={6} style={{ height: readingVirtual.bottomPad, padding: 0, border: 'none' }} />
                                </tr>
                            ) : null}
                        </tbody>
                    </table>
                </div>
                {data.length === 0 ? <div className="empty-state"><div className="empty-icon">🔥</div><p>暂无追读力数据</p></div> : null}
            </div>
        </>
    )
}

function findFirstFilePath(tree) {
    const roots = Object.values(tree || {})
    for (const items of roots) {
        const p = walkFirstFile(items)
        if (p) return p
    }
    return null
}

function walkFirstFile(items) {
    if (!Array.isArray(items)) return null
    for (const item of items) {
        if (item?.type === 'file' && item?.path) return item.path
        if (item?.type === 'dir' && Array.isArray(item.children)) {
            const p = walkFirstFile(item.children)
            if (p) return p
        }
    }
    return null
}


// ====================================================================
// 数据总览内嵌：全量数据视图
// ====================================================================

const MergedDataView = memo(function MergedDataView({ refreshSignal }) {
    const [expanded, setExpanded] = useState(false)
    const [loading, setLoading] = useState(false)
    const [payload, setPayload] = useState({})
    const [loadedGroups, setLoadedGroups] = useState({})
    const [loadError, setLoadError] = useState('')
    const [autoRefresh, setAutoRefresh] = useState(false)
    const [staleHint, setStaleHint] = useState(false)
    const [reloadKey, setReloadKey] = useState(0)
    const [domain, setDomain] = useState('core')
    const payloadSignatureRef = useRef('')
    const payloadRef = useRef({})
    const loadedGroupsRef = useRef({})
    const manualRefreshSeedRef = useRef(0)
    const lastReloadKeyRef = useRef(0)
    const lastRefreshTokenRef = useRef(0)
    const refreshToken = autoRefresh ? refreshSignal : 0

    useEffect(() => { payloadRef.current = payload }, [payload])
    useEffect(() => { loadedGroupsRef.current = loadedGroups }, [loadedGroups])

    useEffect(() => {
        if (!expanded) return

        let disposed = false
        const controller = new AbortController()

        const refreshChanged = refreshToken !== lastRefreshTokenRef.current
        lastRefreshTokenRef.current = refreshToken

        const manualReload = reloadKey !== lastReloadKeyRef.current
        if (manualReload) lastReloadKeyRef.current = reloadKey

        const forceReload = manualReload || (autoRefresh && refreshChanged)
        const targetKeys = getDomainGroupKeys(domain)
        const pendingKeys = forceReload
            ? targetKeys
            : targetKeys.filter((key) => !loadedGroupsRef.current[key])

        if (pendingKeys.length === 0) {
            setLoading(false)
            return () => {
                disposed = true
                controller.abort()
            }
        }

        async function loadDomainGroups() {
            setLoading(true)
            setLoadError('')
            const failedGroups = []
            const loadedEntries = await Promise.all(
                pendingKeys.map(async (key) => {
                    try {
                        const rows = await fetchFullDataGroup(key, controller.signal)
                        return [key, rows]
                    } catch (err) {
                        if (!isAbortError(err)) failedGroups.push(key)
                        return [key, null]
                    }
                }),
            )

            if (disposed) return

            const nextPayload = { ...payloadRef.current }
            const nextLoaded = { ...loadedGroupsRef.current }
            for (const [key, rows] of loadedEntries) {
                if (!Array.isArray(rows)) continue
                nextPayload[key] = rows
                nextLoaded[key] = true
            }

            const nextSignature = createMergedPayloadSignature(nextPayload)
            if (payloadSignatureRef.current !== nextSignature) {
                payloadSignatureRef.current = nextSignature
                setPayload(nextPayload)
            }
            setLoadedGroups(nextLoaded)

            if (failedGroups.length > 0) {
                setLoadError(`部分分组加载失败：${failedGroups.join(', ')}`)
            }
            setStaleHint(false)
            setLoading(false)
        }

        loadDomainGroups().catch((err) => {
            if (isAbortError(err) || disposed) return
            setLoadError('全量数据加载失败，请重试')
            setLoading(false)
        })
        return () => {
            disposed = true
            controller.abort()
        }
    }, [autoRefresh, domain, expanded, refreshToken, reloadKey])

    useEffect(() => {
        if (!expanded || autoRefresh) return
        if (refreshSignal <= manualRefreshSeedRef.current) return
        setStaleHint(true)
    }, [expanded, autoRefresh, refreshSignal])

    if (!expanded) {
        return (
            <div className="card dashboard-section-card">
                <div className="card-header">
                    <span className="card-title">🧪 全量数据视图</span>
                    <button
                        className="filter-btn"
                        type="button"
                        onClick={() => setExpanded(true)}
                    >
                        展开查看
                    </button>
                </div>
                <p className="stat-sub" style={{ margin: 0 }}>
                    默认折叠以降低页面刷新负载。点击后按当前快照拉取全量数据。
                </p>
            </div>
        )
    }

    if (loading) return <div className="loading">加载全量数据中…</div>

    const groups = getDomainGroups(domain)
    const renderedGroups = domain === 'overview' ? [] : groups
    const totalRows = FULL_DATA_GROUPS.reduce((sum, g) => sum + (payload[g.key] || []).length, 0)
    const nonEmptyGroups = FULL_DATA_GROUPS.filter(g => (payload[g.key] || []).length > 0).length
    const loadedGroupCount = FULL_DATA_GROUPS.filter(g => loadedGroups[g.key]).length
    const maxChapter = FULL_DATA_GROUPS.reduce((max, g) => {
        if (!loadedGroups[g.key]) return max
        const rows = payload[g.key] || []
        rows.slice(0, 120).forEach(r => {
            const c = extractChapter(r)
            if (c > max) max = c
        })
        return max
    }, 0)
    const domainStats = FULL_DATA_DOMAINS.filter(d => d.id !== 'overview').map(d => {
        const ds = getDomainGroups(d.id)
        const rowCount = ds.reduce((sum, g) => sum + (payload[g.key] || []).length, 0)
        const filled = ds.filter(g => (payload[g.key] || []).length > 0).length
        const loaded = ds.filter(g => loadedGroups[g.key]).length
        return { ...d, rowCount, filled, total: ds.length, loaded }
    })
    const currentDomainLoaded = groups.filter(g => loadedGroups[g.key]).length

    return (
        <>
            <div className="page-header section-page-header">
                <h2>🧪 全量数据视图</h2>
                <span className="card-badge badge-cyan">{FULL_DATA_GROUPS.length} 类数据源</span>
            </div>

            <div className="filter-group">
                <button
                    className="filter-btn"
                    type="button"
                    onClick={() => {
                        manualRefreshSeedRef.current = refreshSignal
                        setReloadKey(v => v + 1)
                    }}
                >
                    {domain === 'overview' ? '刷新所有分组' : '刷新当前分组'}
                </button>
                <button
                    className={`filter-btn ${autoRefresh ? 'active' : ''}`}
                    type="button"
                    onClick={() => setAutoRefresh(v => !v)}
                >
                    {autoRefresh ? '自动刷新：开' : '自动刷新：关'}
                </button>
                <button className="filter-btn" type="button" onClick={() => setExpanded(false)}>
                    收起
                </button>
            </div>
            {staleHint ? (
                <div className="loading" style={{ marginBottom: 12 }}>
                    检测到新数据，点击“
                    {domain === 'overview' ? '刷新所有分组' : '刷新当前分组'}
                    ”以更新视图。
                </div>
            ) : null}
            <ErrorNotice message={loadError} onRetry={() => setReloadKey(v => v + 1)} />

            <div className="demo-summary-grid">
                <div className="card stat-card">
                    <span className="stat-label">总记录数</span>
                    <span className="stat-value">{formatNumber(totalRows)}</span>
                    <span className="stat-sub">当前返回的全部数据行</span>
                </div>
                <div className="card stat-card">
                    <span className="stat-label">已覆盖数据源</span>
                    <span className="stat-value plain">{nonEmptyGroups}/{FULL_DATA_GROUPS.length}</span>
                    <span className="stat-sub">有数据的表 / 总表数（已加载 {loadedGroupCount}）</span>
                </div>
                <div className="card stat-card">
                    <span className="stat-label">最新章节触达</span>
                    <span className="stat-value plain">{maxChapter > 0 ? `第 ${maxChapter} 章` : '—'}</span>
                    <span className="stat-sub">按可识别 chapter 字段估算</span>
                </div>
                <div className="card stat-card">
                    <span className="stat-label">当前视图</span>
                    <span className="stat-value plain">{FULL_DATA_DOMAINS.find(d => d.id === domain)?.label}</span>
                    <span className="stat-sub">{groups.length} 个分组（已加载 {currentDomainLoaded}）</span>
                </div>
            </div>

            <div className="demo-domain-tabs">
                {FULL_DATA_DOMAINS.map(item => (
                    <button
                        key={item.id}
                        className={`demo-domain-tab ${domain === item.id ? 'active' : ''}`}
                        onClick={() => setDomain(item.id)}
                    >
                        {item.label}
                    </button>
                ))}
            </div>

            {domain === 'overview' ? (
                <div className="demo-domain-grid">
                    {domainStats.map(ds => (
                        <div className="card" key={ds.id}>
                            <div className="card-header">
                                <span className="card-title">{ds.label}</span>
                                <span className="card-badge badge-purple">{ds.loaded}/{ds.total}</span>
                            </div>
                            <div className="domain-stat-number">{formatNumber(ds.rowCount)}</div>
                            <div className="stat-sub">该数据域总记录数（非空分组 {ds.filled}）</div>
                        </div>
                    ))}
                </div>
            ) : null}

            {domain === 'overview' ? (
                <div className="card">
                    <div className="card-header">
                        <span className="card-title">总览模式</span>
                    </div>
                    <p className="stat-sub" style={{ margin: 0 }}>
                        总览仅展示聚合统计。切换到具体数据域后，会按需加载并展示该域明细表。
                    </p>
                </div>
            ) : null}

            {renderedGroups.map(g => {
                const count = (payload[g.key] || []).length
                return (
                    <div className="card demo-group-card" key={g.key}>
                        <div className="card-header">
                            <span className="card-title">{g.title}</span>
                            <span className={`card-badge ${count > 0 ? 'badge-blue' : 'badge-amber'}`}>{count} 条</span>
                        </div>
                        <MiniTable
                            rows={payload[g.key] || []}
                            columns={g.columns}
                            pageSize={12}
                        />
                    </div>
                )
            })}
        </>
    )
})

function MiniTable({ rows, columns, pageSize = 12 }) {
    const [page, setPage] = useState(1)

    useEffect(() => {
        setPage(1)
    }, [rows, columns, pageSize])

    if (!rows || rows.length === 0) {
        return <div className="empty-state compact"><p>暂无数据</p></div>
    }

    const totalPages = Math.max(1, Math.ceil(rows.length / pageSize))
    const safePage = Math.min(page, totalPages)
    const start = (safePage - 1) * pageSize
    const list = rows.slice(start, start + pageSize)

    return (
        <>
            <div className="table-wrap">
                <table className="data-table">
                    <thead>
                        <tr>{columns.map(c => <th key={c}>{c}</th>)}</tr>
                    </thead>
                    <tbody>
                        {list.map((row, i) => (
                            <tr key={i}>
                                {columns.map(c => (
                                    <td key={c} className="truncate" style={{ maxWidth: 240 }}>
                                        {formatCell(row?.[c])}
                                    </td>
                                ))}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
            <div className="table-pagination">
                <button
                    className="page-btn"
                    type="button"
                    onClick={() => setPage(p => Math.max(1, p - 1))}
                    disabled={safePage <= 1}
                >
                    上一页
                </button>
                <span className="page-info">
                    第 {safePage} / {totalPages} 页 · 共 {rows.length} 条
                </span>
                <button
                    className="page-btn"
                    type="button"
                    onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                    disabled={safePage >= totalPages}
                >
                    下一页
                </button>
            </div>
        </>
    )
}

function getDomainGroups(domain) {
    if (domain === 'overview') return FULL_DATA_GROUPS
    return FULL_DATA_GROUPS.filter((g) => g.domain === domain)
}

function getDomainGroupKeys(domain) {
    return getDomainGroups(domain).map((g) => g.key)
}

async function fetchFullDataGroup(key, signal) {
    const req = FULL_DATA_REQUESTS[key]
    if (!req?.path) return []
    const data = await fetchJSON(req.path, req.params || {}, { signal })
    return Array.isArray(data) ? data : []
}

function extractChapter(row) {
    if (!row || typeof row !== 'object') return 0
    const candidates = [
        row.chapter,
        row.start_chapter,
        row.end_chapter,
        row.chapter_discovered,
        row.first_appearance,
        row.last_appearance,
    ]
    for (const c of candidates) {
        const n = Number(c)
        if (Number.isFinite(n) && n > 0) return n
    }
    return 0
}

function createMergedPayloadSignature(payload) {
    if (!payload || typeof payload !== 'object') return ''
    const keys = Object.keys(payload).sort()
    const chunks = []
    for (const key of keys) {
        const rows = Array.isArray(payload[key]) ? payload[key] : []
        const sampleIndexes = buildSampleIndexes(rows.length, 12)
        const sampleSig = sampleIndexes.map((idx) => extractIdentity(rows[idx])).join(';')
        chunks.push(`${key}:${rows.length}:${sampleSig}`)
    }
    return chunks.join('|')
}

function buildSampleIndexes(length, sampleCount = 12) {
    if (!Number.isFinite(length) || length <= 0) return []
    if (length <= sampleCount) return Array.from({ length }, (_, i) => i)
    const indexes = new Set([0, length - 1, Math.floor(length / 2)])
    const step = (length - 1) / (sampleCount - 1)
    for (let i = 0; i < sampleCount; i += 1) {
        indexes.add(Math.floor(i * step))
    }
    return [...indexes].sort((a, b) => a - b)
}

function extractIdentity(row) {
    if (!row || typeof row !== 'object') return '-'
    const preferredKeys = [
        'id',
        'chapter',
        'start_chapter',
        'end_chapter',
        'entity_id',
        'alias',
        'query',
        'created_at',
    ]
    for (const key of preferredKeys) {
        const v = row[key]
        if (v !== undefined && v !== null && String(v) !== '') {
            return `${key}=${String(v)}`
        }
    }
    const keys = Object.keys(row).slice(0, 4).sort()
    return keys.map((k) => `${k}=${String(row[k])}`).join(',')
}

function isStateMirrorFile(path) {
    if (!path) return false
    const normalized = String(path).replace(/\\/g, '/').toLowerCase()
    return (
        normalized.endsWith('.webnovel/state.json')
        || normalized.endsWith('.webnovel/workflow_state.json')
    )
}

function isDbRefreshActivePage(page) {
    return page === 'entities' || page === 'graph' || page === 'chapters' || page === 'reading'
}

function createSimpleSignature(value) {
    try {
        return JSON.stringify(value)
    } catch {
        return String(value)
    }
}

function createRefreshDebugState() {
    return {
        state: { queued: 0, fired: 0, lastAt: 0, lastSource: '-' },
        db: { queued: 0, fired: 0, lastAt: 0, lastSource: '-' },
        files: { queued: 0, fired: 0, lastAt: 0, lastSource: '-' },
    }
}

function loadRefreshDebugEnabled() {
    if (typeof window === 'undefined') return false
    try {
        return window.localStorage.getItem('webnovel-dashboard-refresh-debug') === '1'
    } catch {
        return false
    }
}

function saveRefreshDebugEnabled(enabled) {
    if (typeof window === 'undefined') return
    try {
        window.localStorage.setItem('webnovel-dashboard-refresh-debug', enabled ? '1' : '0')
    } catch {
        // ignore storage failures
    }
}

function useVirtualRows(totalRows, options = {}) {
    const {
        enabled = false,
        rowHeight = 44,
        maxHeight = 560,
        overscan = 8,
    } = options
    const containerRef = useRef(null)
    const [windowRange, setWindowRange] = useState(() => (
        enabled
            ? computeVirtualWindow({ totalRows, rowHeight, overscan, scrollTop: 0, viewportHeight: maxHeight })
            : computeVirtualWindow({ totalRows, rowHeight, overscan, scrollTop: 0, viewportHeight: totalRows * rowHeight })
    ))

    const recompute = useCallback(() => {
        const viewportHeight = enabled
            ? (containerRef.current?.clientHeight || maxHeight)
            : (totalRows * rowHeight)
        const scrollTop = enabled
            ? (containerRef.current?.scrollTop || 0)
            : 0
        setWindowRange(computeVirtualWindow({
            totalRows,
            rowHeight,
            overscan,
            scrollTop,
            viewportHeight,
        }))
    }, [enabled, maxHeight, overscan, rowHeight, totalRows])

    useEffect(() => {
        recompute()
    }, [recompute, totalRows])

    useEffect(() => {
        if (!enabled) return () => { }
        const onResize = () => recompute()
        window.addEventListener('resize', onResize)
        return () => window.removeEventListener('resize', onResize)
    }, [enabled, recompute])

    return {
        ...windowRange,
        containerRef,
        onScroll: enabled ? recompute : undefined,
        tableWrapStyle: enabled ? { maxHeight, overflowY: 'auto' } : undefined,
    }
}

function computeVirtualWindow({ totalRows, rowHeight, overscan, scrollTop, viewportHeight }) {
    const total = Math.max(0, Number(totalRows) || 0)
    if (total === 0) {
        return { start: 0, end: 0, topPad: 0, bottomPad: 0 }
    }
    const safeRowHeight = Math.max(1, Number(rowHeight) || 44)
    const safeOverscan = Math.max(0, Number(overscan) || 0)
    const visibleCount = Math.max(1, Math.ceil((Number(viewportHeight) || safeRowHeight) / safeRowHeight))
    const rawStart = Math.floor((Number(scrollTop) || 0) / safeRowHeight)
    const start = Math.max(0, rawStart - safeOverscan)
    const end = Math.min(total, start + visibleCount + safeOverscan * 2)
    const topPad = start * safeRowHeight
    const bottomPad = Math.max(0, (total - end) * safeRowHeight)
    return { start, end, topPad, bottomPad }
}

function RefreshDebugPanel({ open, onToggle, page, isPageVisible, stats, trace, pending }) {
    return (
        <div className="card" style={{ marginTop: 12, padding: 10 }}>
            <div className="card-header" style={{ marginBottom: 6 }}>
                <span className="card-title">Refresh 调试</span>
                <button className="filter-btn" type="button" onClick={onToggle}>
                    {open ? '收起' : '展开'}
                </button>
            </div>
            <div className="stat-sub">页面: {page} · 可见: {isPageVisible ? '是' : '否'}</div>
            {!open ? null : (
                <>
                    <div className="table-wrap" style={{ marginTop: 8 }}>
                        <table className="data-table">
                            <thead>
                                <tr>
                                    <th>Scope</th>
                                    <th>Queued</th>
                                    <th>Fired</th>
                                    <th>Pending</th>
                                    <th>Last Source</th>
                                    <th>Last At</th>
                                </tr>
                            </thead>
                            <tbody>
                                {['state', 'db', 'files'].map((scope) => (
                                    <tr key={scope}>
                                        <td>{scope}</td>
                                        <td>{stats?.[scope]?.queued || 0}</td>
                                        <td>{stats?.[scope]?.fired || 0}</td>
                                        <td>{pending?.[scope] ? 'yes' : 'no'}</td>
                                        <td className="truncate" style={{ maxWidth: 180 }}>{stats?.[scope]?.lastSource || '-'}</td>
                                        <td>{formatTime(stats?.[scope]?.lastAt)}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                    <div className="card-title" style={{ marginTop: 10, marginBottom: 6 }}>最近事件</div>
                    <div style={{ maxHeight: 180, overflowY: 'auto', fontSize: 12, lineHeight: 1.45 }}>
                        {trace.length === 0 ? (
                            <div className="stat-sub">暂无事件</div>
                        ) : trace.map((evt) => (
                            <div key={evt.id} style={{ padding: '2px 0' }}>
                                [{formatTime(evt.ts)}] {evt.scope} · {evt.phase} · {evt.source}
                            </div>
                        ))}
                    </div>
                </>
            )}
        </div>
    )
}

function formatTime(ts) {
    if (!ts) return '-'
    return new Date(ts).toLocaleTimeString('zh-CN', { hour12: false })
}


// ====================================================================
// 子组件：文件树递归
// ====================================================================

function TreeNodes({ items, selected, onSelect, depth = 0 }) {
    const [expanded, setExpanded] = useState({})
    if (!items || items.length === 0) return null

    return items.map((item, i) => {
        const key = item.path || `${depth}-${i}`
        if (item.type === 'dir') {
            const isOpen = expanded[key]
            return (
                <li key={key}>
                    <div
                        className="tree-item"
                        role="button"
                        tabIndex={0}
                        onKeyDown={e => (e.key === 'Enter' || e.key === ' ') && (e.preventDefault(), setExpanded(prev => ({ ...prev, [key]: !prev[key] })))}
                        onClick={() => setExpanded(prev => ({ ...prev, [key]: !prev[key] }))}
                    >
                        <span className="tree-icon">{isOpen ? '📂' : '📁'}</span>
                        <span>{item.name}</span>
                    </div>
                    {isOpen && item.children && (
                        <ul className="tree-children">
                            <TreeNodes items={item.children} selected={selected} onSelect={onSelect} depth={depth + 1} />
                        </ul>
                    )}
                </li>
            )
        }
        return (
            <li key={key}>
                <div
                    className={`tree-item ${selected === item.path ? 'active' : ''}`}
                    role="button"
                    tabIndex={0}
                    onKeyDown={e => (e.key === 'Enter' || e.key === ' ') && (e.preventDefault(), onSelect(item.path))}
                    onClick={() => onSelect(item.path)}
                >
                    <span className="tree-icon">📄</span>
                    <span>{item.name}</span>
                </div>
            </li>
        )
    })
}


// ====================================================================
// 辅助：数字格式化
// ====================================================================

function ErrorNotice({ message, onRetry }) {
    if (!message) return null
    return (
        <div className="card" style={{ marginBottom: 12 }}>
            <div className="card-header">
                <span className="card-title">⚠️ 数据加载异常</span>
                <button className="filter-btn" type="button" onClick={onRetry}>
                    重试
                </button>
            </div>
            <p className="stat-sub" style={{ margin: 0 }}>{message}</p>
        </div>
    )
}

function isAbortError(err) {
    return err?.name === 'AbortError'
}

function formatNumber(n) {
    if (n >= 10000) return new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 1 }).format(n / 10000) + ' 万'
    return new Intl.NumberFormat('zh-CN').format(n)
}

function formatJSON(str) {
    try {
        return JSON.stringify(JSON.parse(str), null, 2)
    } catch {
        return str
    }
}

function formatCell(v) {
    if (v === null || v === undefined) return '—'
    if (typeof v === 'boolean') return v ? 'true' : 'false'
    if (typeof v === 'object') {
        try {
            return JSON.stringify(v)
        } catch {
            return String(v)
        }
    }
    const s = String(v)
    return s.length > 180 ? `${s.slice(0, 180)}...` : s
}
