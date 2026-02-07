/**
 * Tusk Cluster - Dashboard JavaScript
 */

let refreshInterval = null;

// ─────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────

function formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
}

function timeAgo(isoStr) {
    if (!isoStr) return 'N/A';
    const diff = Date.now() - new Date(isoStr).getTime();
    const secs = Math.floor(diff / 1000);
    if (secs < 60) return `${secs}s ago`;
    if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
    return `${Math.floor(secs / 3600)}h ago`;
}

function statusColor(status) {
    const colors = {
        idle: 'bg-green-500',
        busy: 'bg-yellow-500',
        offline: 'bg-gray-600',
        pending: 'bg-yellow-500',
        running: 'bg-blue-500',
        completed: 'bg-green-500',
        failed: 'bg-red-500',
        cancelled: 'bg-gray-500',
    };
    return colors[status] || 'bg-gray-600';
}

function statusBadge(status) {
    const color = statusColor(status);
    return `<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium">
        <span class="w-1.5 h-1.5 rounded-full ${color}"></span>
        ${status}
    </span>`;
}

// ─────────────────────────────────────────────────────────
// Cluster Status / Refresh
// ─────────────────────────────────────────────────────────

async function refreshCluster() {
    try {
        const [statusRes, workersRes, jobsRes, localRes] = await Promise.all([
            fetch('/api/cluster/status'),
            fetch('/api/cluster/workers'),
            fetch('/api/cluster/jobs'),
            fetch('/api/cluster/local/status'),
        ]);

        const status = await statusRes.json();
        const workers = await workersRes.json();
        const jobs = await jobsRes.json();
        const local = await localRes.json();

        updateStatus(status);
        updateWorkers(workers.workers || []);
        updateJobs(jobs.jobs || []);
        updateLocalStatus(local);
    } catch (e) {
        console.error('Failed to refresh cluster:', e);
    }
    // Always refresh catalog (independent of scheduler)
    refreshCatalog();
}

function refreshCatalog() {
    const container = document.getElementById('catalog-list');
    if (!container) return;

    let pipelines = [];
    try {
        pipelines = JSON.parse(localStorage.getItem('tusk_saved_pipelines') || '[]')
            .filter(p => p.cluster_enabled);
    } catch (e) {}

    if (pipelines.length === 0) {
        container.innerHTML = '<div class="text-[#8b949e] text-xs py-1">No shared pipelines</div>';
        return;
    }

    container.innerHTML = pipelines.map(p => {
        const transformCount = p.transforms?.length || 0;
        const sourceName = p.source?.name || 'unknown';
        return `
        <div class="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-[#21262d] text-sm group">
            <i data-lucide="git-branch" class="w-3.5 h-3.5 text-green-400 flex-shrink-0"></i>
            <div class="flex-1 min-w-0">
                <div class="text-xs font-medium truncate">${p.name}</div>
                <div class="text-xs text-[#484f58]">${sourceName} · ${transformCount} transforms</div>
            </div>
            <button onclick="submitPipelineJob('${p.id}')"
                    class="p-1 text-[#484f58] hover:text-green-400 opacity-0 group-hover:opacity-100" title="Run on cluster">
                <i data-lucide="play" class="w-3 h-3"></i>
            </button>
        </div>`;
    }).join('');

    if (typeof lucide !== 'undefined') lucide.createIcons();
}

// ─────────────────────────────────────────────────────────
// Pipeline → SQL Converter
// ─────────────────────────────────────────────────────────

function _quoteId(name) {
    return '"' + name.replace(/"/g, '""') + '"';
}

function _filterToSQL(t, src) {
    const col = _quoteId(t.column);
    const op = t.operator;
    const val = t.value;
    let cond;
    if (op === 'is_null') cond = `${col} IS NULL`;
    else if (op === 'is_not_null') cond = `${col} IS NOT NULL`;
    else if (op === 'eq') cond = `${col} = '${val}'`;
    else if (op === 'ne') cond = `${col} != '${val}'`;
    else if (op === 'gt') cond = `${col} > ${val}`;
    else if (op === 'gte') cond = `${col} >= ${val}`;
    else if (op === 'lt') cond = `${col} < ${val}`;
    else if (op === 'lte') cond = `${col} <= ${val}`;
    else if (op === 'contains') cond = `${col} LIKE '%${val}%'`;
    else if (op === 'starts_with') cond = `${col} LIKE '${val}%'`;
    else if (op === 'ends_with') cond = `${col} LIKE '%${val}'`;
    else cond = `${col} = '${val}'`;
    return `SELECT * FROM ${src} WHERE ${cond}`;
}

function _groupByToSQL(t, src) {
    const byCols = t.by.map(c => _quoteId(c)).join(', ');
    const aggMap = { sum: 'SUM', mean: 'AVG', min: 'MIN', max: 'MAX', count: 'COUNT', first: 'FIRST_VALUE', last: 'LAST_VALUE' };
    const selects = [...t.by.map(c => _quoteId(c))];
    for (const agg of (t.aggregations || [])) {
        const fn = aggMap[agg.agg] || 'SUM';
        const alias = agg.alias || `${agg.column}_${agg.agg}`;
        selects.push(`${fn}(${_quoteId(agg.column)}) AS ${_quoteId(alias)}`);
    }
    return `SELECT ${selects.join(', ')} FROM ${src} GROUP BY ${byCols}`;
}

function _sortToSQL(t, src) {
    const orders = t.columns.map((col, i) => {
        const dir = (t.descending && t.descending[i]) ? 'DESC' : 'ASC';
        return `${_quoteId(col)} ${dir}`;
    });
    return `SELECT * FROM ${src} ORDER BY ${orders.join(', ')}`;
}

function _renameToSQL(t, src) {
    // We need to know all columns, but we don't — use SELECT * with aliased renames
    // DataFusion doesn't support RENAME, so we do: SELECT *, old AS new ... but that duplicates
    // Best approach: use COLUMNS / EXCLUDE if DataFusion supports, else just pass through
    // Simplest: rename via subquery with explicit aliases
    const renames = t.mapping || {};
    const parts = [];
    for (const [oldName, newName] of Object.entries(renames)) {
        parts.push(`${_quoteId(oldName)} AS ${_quoteId(newName)}`);
    }
    // Include all other columns with * EXCLUDE
    const excluded = Object.keys(renames).map(c => _quoteId(c)).join(', ');
    return `SELECT * EXCLUDE (${excluded}), ${parts.join(', ')} FROM ${src}`;
}

function _dropNullsToSQL(t, src) {
    const cols = t.subset || [];
    if (cols.length === 0) return `SELECT * FROM ${src}`;
    const conds = cols.map(c => `${_quoteId(c)} IS NOT NULL`).join(' AND ');
    return `SELECT * FROM ${src} WHERE ${conds}`;
}

function pipelineToSQL(pipeline) {
    const source = pipeline.source || pipeline;
    if (!source) return null;

    // Build FROM clause based on source type
    let fromExpr;
    if (source.source_type === 'database' && source.query) {
        fromExpr = `(${source.query})`;
    } else if (source.path) {
        const path = source.path;
        const ext = path.split('.').pop().toLowerCase();
        if (ext === 'parquet') fromExpr = `read_parquet('${path}')`;
        else if (ext === 'csv' || ext === 'tsv') fromExpr = `read_csv('${path}')`;
        else if (ext === 'json' || ext === 'geojson' || ext === 'ndjson') fromExpr = `read_json('${path}')`;
        else { return null; }
    } else {
        return null;
    }

    const transforms = pipeline.transforms || [];
    if (transforms.length === 0) {
        return `SELECT * FROM ${fromExpr}`;
    }

    // Build CTE chain: each transform becomes a step
    const ctes = [];
    ctes.push(`_src AS (SELECT * FROM ${fromExpr})`);
    let prev = '_src';

    for (let i = 0; i < transforms.length; i++) {
        const t = transforms[i];
        const alias = `_t${i + 1}`;
        let sql;

        switch (t.type) {
            case 'filter':
                sql = _filterToSQL(t, prev);
                break;
            case 'select':
                sql = `SELECT ${t.columns.map(c => _quoteId(c)).join(', ')} FROM ${prev}`;
                break;
            case 'sort':
                sql = _sortToSQL(t, prev);
                break;
            case 'limit':
                sql = `SELECT * FROM ${prev} LIMIT ${parseInt(t.n) || 1000}`;
                break;
            case 'group_by':
                sql = _groupByToSQL(t, prev);
                break;
            case 'rename':
                sql = _renameToSQL(t, prev);
                break;
            case 'drop_nulls':
                sql = _dropNullsToSQL(t, prev);
                break;
            default:
                sql = `SELECT * FROM ${prev}`;
        }

        ctes.push(`${alias} AS (${sql})`);
        prev = alias;
    }

    return `WITH ${ctes.join(',\n')} SELECT * FROM ${prev}`;
}

async function submitPipelineJob(pipelineId) {
    let pipelines = [];
    try { pipelines = JSON.parse(localStorage.getItem('tusk_saved_pipelines') || '[]'); } catch (e) {}
    const pipeline = pipelines.find(p => p.id === pipelineId);
    if (!pipeline) { tuskToast('Pipeline not found', 'error'); return; }

    const source = pipeline.source || pipeline;

    // Sources that need materialization (database, OSM → Parquet first)
    const needsMaterialization = (source.source_type === 'database' && source.query)
        || source.source_type === 'osm';

    if (needsMaterialization) {
        const label = source.source_type === 'osm' ? 'OSM data' : 'database query';
        tuskToast(`Materializing ${label} to Parquet...`, 'info');
        try {
            const body = {
                source_type: source.source_type,
                name: source.name || pipeline.name || 'query',
            };
            if (source.source_type === 'database') {
                body.query = source.query;
                body.connection_id = source.connection_id;
            } else {
                body.path = source.path;
                body.osm_layer = source.osm_layer || 'all';
            }
            const res = await fetch('/api/data/materialize', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            const result = await res.json();
            if (result.error) {
                tuskToast('Materialization failed: ' + result.error, 'error');
                return;
            }
            // Replace source with the materialized Parquet file
            const matPipeline = JSON.parse(JSON.stringify(pipeline));
            matPipeline.source = {
                ...matPipeline.source,
                source_type: 'parquet',
                path: result.path,
            };
            const sql = pipelineToSQL(matPipeline);
            if (!sql) { tuskToast('Failed to generate SQL', 'error'); return; }
            const input = document.getElementById('job-sql');
            if (input) input.value = sql;
            tuskToast(`Materialized ${result.rows || '?'} rows → running on cluster`, 'success');
            submitJob();
            return;
        } catch (e) {
            tuskToast('Materialization failed: ' + e.message, 'error');
            return;
        }
    }

    // For file sources, generate SQL directly
    const sql = pipelineToSQL(pipeline);
    if (!sql) {
        tuskToast('Cannot convert pipeline to SQL — unsupported source type', 'warning');
        return;
    }

    const input = document.getElementById('job-sql');
    if (input) input.value = sql;
    tuskToast('Pipeline converted to SQL', 'info');
    submitJob();
}

function updateStatus(status) {
    const dot = document.getElementById('scheduler-status');
    const info = document.getElementById('scheduler-info');

    if (status.scheduler_online) {
        dot.className = 'w-2 h-2 rounded-full bg-green-500';
        info.textContent = `Connected to ${status.scheduler_address || 'localhost'}`;
    } else {
        dot.className = 'w-2 h-2 rounded-full bg-gray-600';
        info.textContent = 'Not connected';
    }

    document.getElementById('stat-workers').textContent = status.workers_online || 0;
    document.getElementById('stat-active').textContent = status.active_jobs || 0;
    document.getElementById('stat-completed').textContent = status.completed_jobs || 0;
    document.getElementById('stat-bytes').textContent = formatBytes(status.total_bytes_processed || 0);
    document.getElementById('workers-count').textContent = `${status.workers_online || 0}/${status.workers_total || 0}`;

    // Toggle connect/disconnect buttons
    const btnConnect = document.getElementById('btn-connect');
    const btnDisconnect = document.getElementById('btn-disconnect');
    if (status.scheduler_online) {
        btnConnect.classList.add('hidden');
        btnDisconnect.classList.remove('hidden');
    } else {
        btnConnect.classList.remove('hidden');
        btnDisconnect.classList.add('hidden');
    }
}

function updateWorkers(workers) {
    const container = document.getElementById('workers-list');

    if (!workers.length) {
        container.innerHTML = '<div class="text-[#8b949e] text-sm py-2">No workers connected</div>';
        return;
    }

    container.innerHTML = workers.map(w => `
        <div class="card p-2.5 text-sm">
            <div class="flex items-center justify-between mb-1.5">
                <span class="font-mono text-xs text-[#e6edf3] truncate" title="${w.id}">${w.id.substring(0, 12)}</span>
                ${statusBadge(w.status)}
            </div>
            <div class="grid grid-cols-2 gap-1 text-xs text-[#8b949e]">
                <div>CPU: <span class="text-[#e6edf3]">${(w.cpu_percent || 0).toFixed(0)}%</span></div>
                <div>Mem: <span class="text-[#e6edf3]">${(w.memory_mb || 0).toFixed(0)} MB</span></div>
                <div>Jobs: <span class="text-[#e6edf3]">${w.jobs_completed || 0}</span></div>
                <div>Data: <span class="text-[#e6edf3]">${formatBytes(w.bytes_processed || 0)}</span></div>
            </div>
            <div class="mt-1 text-xs text-[#484f58]">${timeAgo(w.last_heartbeat)}</div>
        </div>
    `).join('');
}

function updateJobs(jobs) {
    const activeContainer = document.getElementById('active-jobs');
    const historyContainer = document.getElementById('job-history');

    const active = jobs.filter(j => j.status === 'running' || j.status === 'pending');
    const history = jobs.filter(j => j.status !== 'running' && j.status !== 'pending');

    // Active jobs
    if (!active.length) {
        activeContainer.innerHTML = '<div class="text-[#8b949e] text-sm">No active jobs</div>';
    } else {
        activeContainer.innerHTML = active.map(j => `
            <div class="card p-3">
                <div class="flex items-center justify-between mb-2">
                    <span class="font-mono text-xs text-[#58a6ff]">${j.id || j.job_id || 'unknown'}</span>
                    ${statusBadge(j.status)}
                </div>
                <div class="font-mono text-sm text-[#e6edf3] mb-2 truncate" title="${(j.sql || '').replace(/"/g, '&quot;')}">${j.sql || ''}</div>
                ${j.progress != null ? `
                <div class="w-full bg-[#21262d] rounded-full h-1.5 mb-1">
                    <div class="bg-indigo-500 h-1.5 rounded-full transition-all" style="width: ${(j.progress * 100).toFixed(0)}%"></div>
                </div>
                <div class="flex justify-between text-xs text-[#8b949e]">
                    <span>Stage ${j.stages_completed || 0}/${j.stages_total || 1}</span>
                    <span>${(j.progress * 100).toFixed(0)}%</span>
                </div>` : ''}
                <div class="mt-2 flex gap-2">
                    <button onclick="cancelJob('${j.id || j.job_id}')" class="text-xs px-2 py-1 bg-[#da3633] hover:bg-[#f85149] rounded">Cancel</button>
                </div>
            </div>
        `).join('');
    }

    // History
    if (!history.length) {
        historyContainer.innerHTML = '<div class="text-[#8b949e] text-sm">No job history</div>';
    } else {
        historyContainer.innerHTML = history.map(j => {
            const jobId = j.id || j.job_id || 'unknown';
            return `
            <div class="card p-3 flex items-center gap-3">
                <div class="flex-1 min-w-0">
                    <div class="flex items-center gap-2 mb-1">
                        <span class="font-mono text-xs text-[#8b949e]">${jobId.substring(0, 12)}</span>
                        ${statusBadge(j.status)}
                    </div>
                    <div class="font-mono text-sm text-[#e6edf3] truncate" title="${(j.sql || '').replace(/"/g, '&quot;')}">${j.sql || ''}</div>
                    ${j.error ? `<div class="text-xs text-red-400 mt-1">${j.error}</div>` : ''}
                </div>
                <div class="text-xs text-[#8b949e] whitespace-nowrap">${timeAgo(j.created_at || j.completed_at)}</div>
                <div class="flex gap-1">
                    ${j.status === 'completed' ? `<button onclick="viewJobResult('${jobId}')" class="text-xs px-2 py-1 bg-[#21262d] hover:bg-[#30363d] rounded">View</button>` : ''}
                    ${j.status === 'failed' ? `<button onclick="retryJob('${jobId}')" class="text-xs px-2 py-1 bg-[#21262d] hover:bg-[#30363d] rounded">Retry</button>` : ''}
                </div>
            </div>`;
        }).join('');
    }
}

function updateLocalStatus(local) {
    const statusEl = document.getElementById('local-status');
    const btnStart = document.getElementById('btn-start-local');
    const btnStop = document.getElementById('btn-stop-local');

    if (local.running) {
        statusEl.textContent = 'Running';
        statusEl.className = 'text-xs text-green-400';
        btnStart.classList.add('hidden');
        btnStop.classList.remove('hidden');
    } else {
        statusEl.textContent = 'Stopped';
        statusEl.className = 'text-xs text-[#8b949e]';
        btnStart.classList.remove('hidden');
        btnStop.classList.add('hidden');
    }
}

// ─────────────────────────────────────────────────────────
// Scheduler Connect / Disconnect
// ─────────────────────────────────────────────────────────

async function connectScheduler() {
    const host = document.getElementById('scheduler-host').value || 'localhost';
    const port = document.getElementById('scheduler-port').value || '8814';

    try {
        const res = await fetch('/api/cluster/connect', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ host, port: parseInt(port) }),
        });
        const data = await res.json();

        if (data.connected) {
            tuskToast(`Connected to scheduler at ${data.address}`, 'success');
            refreshCluster();
            startAutoRefresh();
        } else {
            tuskToast(data.error || 'Failed to connect', 'error');
        }
    } catch (e) {
        tuskToast('Connection failed: ' + e.message, 'error');
    }
}

async function disconnectScheduler() {
    try {
        await fetch('/api/cluster/disconnect', { method: 'POST' });
        tuskToast('Disconnected from scheduler', 'info');
        stopAutoRefresh();
        refreshCluster();
    } catch (e) {
        tuskToast('Disconnect failed: ' + e.message, 'error');
    }
}

// ─────────────────────────────────────────────────────────
// Local Cluster Start / Stop
// ─────────────────────────────────────────────────────────

async function startLocalCluster() {
    const workers = document.getElementById('local-workers').value || '3';

    const btn = document.getElementById('btn-start-local');
    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader" class="w-4 h-4 animate-spin"></i> Starting...';

    try {
        const res = await fetch('/api/cluster/local/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ workers: parseInt(workers) }),
        });
        const data = await res.json();

        if (data.started) {
            if (data.warning) {
                tuskToast(`Cluster started but scheduler may still be loading. Retrying connection...`, 'warning');
                // Retry connection a few times
                let connected = false;
                for (let i = 0; i < 5; i++) {
                    await new Promise(r => setTimeout(r, 2000));
                    try {
                        const cRes = await fetch('/api/cluster/connect', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ host: 'localhost', port: 8814 }),
                        });
                        const cData = await cRes.json();
                        if (cData.connected) { connected = true; break; }
                    } catch (e) {}
                }
                if (connected) {
                    tuskToast(`Connected to scheduler`, 'success');
                } else {
                    tuskToast(`Scheduler not responding. Try clicking Connect manually.`, 'warning');
                }
            } else {
                tuskToast(`Local cluster started with ${data.workers} workers`, 'success');
            }
            startAutoRefresh();
            refreshCluster();
        } else {
            tuskToast(data.error || 'Failed to start local cluster', 'error');
        }
    } catch (e) {
        tuskToast('Failed to start: ' + e.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i data-lucide="play" class="w-4 h-4"></i> Start Local Cluster';
        lucide.createIcons();
    }
}

async function stopLocalCluster() {
    const btn = document.getElementById('btn-stop-local');
    btn.disabled = true;

    try {
        const res = await fetch('/api/cluster/local/stop', { method: 'POST' });
        const data = await res.json();

        if (data.stopped) {
            tuskToast('Local cluster stopped', 'info');
            stopAutoRefresh();
            refreshCluster();
        } else {
            tuskToast(data.error || 'Failed to stop', 'error');
        }
    } catch (e) {
        tuskToast('Failed to stop: ' + e.message, 'error');
    } finally {
        btn.disabled = false;
    }
}

// ─────────────────────────────────────────────────────────
// Job Submission
// ─────────────────────────────────────────────────────────

async function submitJob() {
    const input = document.getElementById('job-sql');
    const sql = input.value.trim();
    if (!sql) {
        tuskToast('Enter a SQL query to execute', 'warning');
        return;
    }

    try {
        const res = await fetch('/api/cluster/jobs', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql }),
        });
        const data = await res.json();

        if (data.job_id) {
            tuskToast(`Job submitted: ${data.job_id.substring(0, 12)}`, 'success');
            input.value = '';
            refreshCluster();
        } else {
            tuskToast(data.error || 'Failed to submit job', 'error');
        }
    } catch (e) {
        tuskToast('Submit failed: ' + e.message, 'error');
    }
}

// ─────────────────────────────────────────────────────────
// Job Actions
// ─────────────────────────────────────────────────────────

async function cancelJob(jobId) {
    try {
        const res = await fetch(`/api/cluster/jobs/${jobId}/cancel`, { method: 'POST' });
        const data = await res.json();
        if (data.cancelled) {
            tuskToast('Job cancelled', 'info');
            refreshCluster();
        } else {
            tuskToast(data.error || 'Cancel failed', 'error');
        }
    } catch (e) {
        tuskToast('Cancel failed: ' + e.message, 'error');
    }
}

async function retryJob(jobId) {
    try {
        const res = await fetch(`/api/cluster/jobs/${jobId}/retry`, { method: 'POST' });
        const data = await res.json();
        if (data.job_id) {
            tuskToast(`Job retried: ${data.job_id.substring(0, 12)}`, 'success');
            refreshCluster();
        } else {
            tuskToast(data.error || 'Retry failed', 'error');
        }
    } catch (e) {
        tuskToast('Retry failed: ' + e.message, 'error');
    }
}

async function viewJobResult(jobId) {
    try {
        const res = await fetch(`/api/cluster/jobs/${jobId}/result`);
        const data = await res.json();

        if (data.error) {
            tuskToast(data.error, 'error');
            return;
        }

        // Simple display in a modal-like overlay
        const cols = data.columns || [];
        const rows = data.rows || [];

        let html = `<div class="fixed inset-0 z-50 flex items-center justify-center">
            <div class="absolute inset-0 bg-black/50" onclick="this.parentElement.remove()"></div>
            <div class="relative card p-6 max-w-4xl w-full mx-4 max-h-[80vh] overflow-auto shadow-xl">
                <div class="flex items-center justify-between mb-4">
                    <h3 class="text-lg font-semibold">Job Result: ${jobId.substring(0, 12)}</h3>
                    <button onclick="this.closest('.fixed').remove()" class="text-[#8b949e] hover:text-white">&times;</button>
                </div>
                <div class="text-xs text-[#8b949e] mb-3">${rows.length} rows, ${cols.length} columns</div>
                <div class="overflow-auto">
                    <table class="w-full text-sm">
                        <thead><tr>${cols.map(c => `<th class="text-left px-2 py-1 border-b border-[#30363d] text-[#8b949e] font-medium">${c}</th>`).join('')}</tr></thead>
                        <tbody>${rows.slice(0, 100).map(r => `<tr class="hover:bg-[#161b22]">${cols.map((_, i) => `<td class="px-2 py-1 border-b border-[#21262d] font-mono">${r[i] != null ? r[i] : '<span class="text-[#484f58]">NULL</span>'}</td>`).join('')}</tr>`).join('')}</tbody>
                    </table>
                </div>
                ${rows.length > 100 ? '<div class="text-xs text-[#8b949e] mt-2">Showing first 100 rows</div>' : ''}
            </div>
        </div>`;

        document.body.insertAdjacentHTML('beforeend', html);
    } catch (e) {
        tuskToast('Failed to load result: ' + e.message, 'error');
    }
}

// ─────────────────────────────────────────────────────────
// Auto-refresh
// ─────────────────────────────────────────────────────────

function startAutoRefresh() {
    stopAutoRefresh();
    refreshInterval = setInterval(refreshCluster, 5000);
}

function stopAutoRefresh() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
        refreshInterval = null;
    }
}

// ─────────────────────────────────────────────────────────
// Init
// ─────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
    // Initial refresh
    await refreshCluster();

    // Start auto-refresh if connected
    try {
        const res = await fetch('/api/cluster/status');
        const status = await res.json();
        if (status.scheduler_online) {
            startAutoRefresh();
        }
    } catch (e) {
        // ignore
    }

    // Submit on Enter in SQL input
    const sqlInput = document.getElementById('job-sql');
    if (sqlInput) {
        sqlInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                submitJob();
            }
        });
    }
});
