import { memo, useState, useEffect, useMemo, useRef, lazy, Suspense } from 'react'
import { fetchJSON } from '../api.js'

const ForceGraph3D = lazy(() => import('react-force-graph-3d'))

const PARTICLE_SPEED = () => 0.006

const GraphPage = memo(function GraphPage({ refreshSignal }) {
    const [relationships, setRelationships] = useState([])
    const [graphData, setGraphData] = useState({ nodes: [], links: [] })
    const [graphWarning, setGraphWarning] = useState('')
    const [loadError, setLoadError] = useState('')
    const [autoRefresh, setAutoRefresh] = useState(false)
    const [reloadKey, setReloadKey] = useState(0)
    const signatureRef = useRef('')
    const refreshToken = useMemo(() => (autoRefresh ? refreshSignal : 0), [autoRefresh, refreshSignal])

    useEffect(() => {
        const controller = new AbortController()
        const signal = controller.signal
        setLoadError('')

        Promise.allSettled([
            fetchJSON('/api/relationships', { limit: 1000 }, { signal }),
            fetchJSON('/api/relationship-events', { limit: 2000 }, { signal }),
            fetchJSON('/api/entities', {}, { signal }),
        ]).then(([relsResult, relEventsResult, entsResult]) => {
            if (signal.aborted) return

            const allAborted = [relsResult, relEventsResult, entsResult].every(
                (r) => r.status === 'rejected' && isAbortError(r.reason),
            )
            if (allAborted) return

            const rels = relsResult.status === 'fulfilled' && Array.isArray(relsResult.value) ? relsResult.value : []
            const relEvents = relEventsResult.status === 'fulfilled' && Array.isArray(relEventsResult.value) ? relEventsResult.value : []
            const ents = entsResult.status === 'fulfilled' && Array.isArray(entsResult.value) ? entsResult.value : []

            const failedSegments = []
            if (relsResult.status === 'rejected' && !isAbortError(relsResult.reason)) failedSegments.push('relationships')
            if (relEventsResult.status === 'rejected' && !isAbortError(relEventsResult.reason)) failedSegments.push('relationship-events')
            if (entsResult.status === 'rejected' && !isAbortError(entsResult.reason)) failedSegments.push('entities')
            setGraphWarning(failedSegments.length > 0 ? `部分数据加载失败：${failedSegments.join(', ')}` : '')

            const effectiveRels = buildEffectiveRelationships(rels, relEvents)
            const typeColors = {
                '角色': '#4f8ff7', '地点': '#34d399', '星球': '#22d3ee', '神仙': '#f59e0b',
                '势力': '#8b5cf6', '招式': '#ef4444', '法宝': '#ec4899'
            }
            const relatedIds = new Set()
            effectiveRels.forEach(r => { relatedIds.add(r.from_entity); relatedIds.add(r.to_entity) })
            const entityMap = {}
            ents.forEach(e => { entityMap[e.id] = e })
            const tierWeights = {
                '核心': 8,
                '重要': 5,
                '次要': 3,
                '支线': 3,
                '装饰': 2,
                'S': 8,
                'A': 5,
                'B': 3,
            }

            const nodes = [...relatedIds].map(id => ({
                id,
                name: entityMap[id]?.canonical_name || id,
                val: tierWeights[entityMap[id]?.tier] || 2,
                color: typeColors[entityMap[id]?.type] || '#5c6078'
            }))
            const links = effectiveRels.map(r => ({
                source: r.from_entity,
                target: r.to_entity,
                name: r.type
            }))

            const nextSignature = createGraphSignature(effectiveRels, nodes)
            if (signatureRef.current === nextSignature) return
            signatureRef.current = nextSignature
            setRelationships(effectiveRels)
            setGraphData({ nodes, links })
        }).catch((err) => {
            if (isAbortError(err) || signal.aborted) return
            setLoadError('关系图谱加载失败，请重试')
        })

        return () => controller.abort()
    }, [refreshToken, reloadKey])

    return (
        <>
            <div className="page-header">
                <h2>🕸️ 关系图谱</h2>
                <span className="card-badge badge-blue">{relationships.length} 条引力链接</span>
            </div>
            <div className="filter-group">
                <button className="filter-btn" type="button" onClick={() => setReloadKey(v => v + 1)}>
                    刷新图谱
                </button>
                <button
                    className={`filter-btn ${autoRefresh ? 'active' : ''}`}
                    type="button"
                    onClick={() => setAutoRefresh(v => !v)}
                >
                    {autoRefresh ? '自动刷新：开' : '自动刷新：关'}
                </button>
            </div>
            {loadError ? (
                <div className="card" style={{ marginBottom: 12 }}>
                    <div className="card-header">
                        <span className="card-title">⚠️ 图谱加载异常</span>
                        <button className="filter-btn" type="button" onClick={() => setReloadKey(v => v + 1)}>
                            重试
                        </button>
                    </div>
                    <p className="stat-sub" style={{ margin: 0 }}>{loadError}</p>
                </div>
            ) : null}
            {graphWarning ? (
                <div className="loading" style={{ marginBottom: 12 }}>{graphWarning}</div>
            ) : null}
            <div className="card graph-shell">
                <Suspense fallback={<div className="loading">3D 图谱模块加载中…</div>}>
                    <ForceGraph3D
                        graphData={graphData}
                        nodeLabel="name"
                        nodeColor="color"
                        nodeRelSize={6}
                        linkColor={() => 'rgba(127, 90, 240, 0.35)'}
                        linkWidth={1}
                        linkDirectionalParticles={2}
                        linkDirectionalParticleWidth={1.5}
                        linkDirectionalParticleSpeed={PARTICLE_SPEED}
                        backgroundColor="#fffaf0"
                        showNavInfo={false}
                    />
                </Suspense>
            </div>
        </>
    )
})

function isAbortError(err) {
    return err?.name === 'AbortError'
}

function createGraphSignature(relationships, nodes) {
    const relRows = Array.isArray(relationships) ? relationships : []
    const nodeRows = Array.isArray(nodes) ? nodes : []
    const relSig = relRows
        .map((r) => `${r.from_entity}|${r.to_entity}|${r.type}|${r.chapter}|${r.description || ''}`)
        .join('||')
    const nodeSig = nodeRows
        .map((n) => `${n.id}|${n.name}|${n.val}|${n.color}`)
        .sort()
        .join('||')
    return `${relSig}##${nodeSig}`
}

function buildEffectiveRelationships(relationships, events) {
    const index = new Map()
    const rows = Array.isArray(relationships) ? relationships : []
    const eventRows = Array.isArray(events) ? events : []

    rows.forEach(row => {
        const from = row?.from_entity
        const to = row?.to_entity
        const type = row?.type
        if (!from || !to || !type) return
        const key = `${from}::${to}::${type}`
        index.set(key, {
            from_entity: from,
            to_entity: to,
            type,
            chapter: Number(row?.chapter) || 0,
            description: row?.description || '',
        })
    })

    eventRows
        .slice()
        .sort((a, b) => {
            const chapterDiff = (Number(a?.chapter) || 0) - (Number(b?.chapter) || 0)
            if (chapterDiff !== 0) return chapterDiff
            return (Number(a?.id) || 0) - (Number(b?.id) || 0)
        })
        .forEach(row => {
            const from = row?.from_entity
            const to = row?.to_entity
            const type = row?.type
            if (!from || !to || !type) return

            const key = `${from}::${to}::${type}`
            const action = String(row?.action || 'update').toLowerCase()
            if (action === 'remove') {
                index.delete(key)
                return
            }

            index.set(key, {
                from_entity: from,
                to_entity: to,
                type,
                chapter: Number(row?.chapter) || 0,
                description: row?.description || '',
            })
        })

    return [...index.values()].sort((a, b) => (Number(b?.chapter) || 0) - (Number(a?.chapter) || 0))
}

export default GraphPage
