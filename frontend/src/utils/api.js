/**
 * API client v9.1 — UA Dashboard Fondazione Telethon
 * FIX: safe() wrapper garantisce che ogni risposta array sia sempre un array
 * anche se il backend ritorna {detail:...} o null per errori non gestiti.
 */
const BASE = import.meta.env.VITE_API_URL || ''

async function get(path) {
  const r = await fetch(`${BASE}${path}`)
  if (!r.ok) {
    const text = await r.text().catch(() => '')
    throw new Error(text?.slice(0, 300) || `HTTP ${r.status}`)
  }
  return r.json()
}

async function post(path, body) {
  const r = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) {
    const text = await r.text().catch(() => '')
    throw new Error(text?.slice(0, 300) || `HTTP ${r.status}`)
  }
  return r.json()
}

function qs(p = {}) {
  const clean = Object.fromEntries(
    Object.entries(p).filter(([, v]) => v !== '' && v != null)
  )
  const s = new URLSearchParams(clean).toString()
  return s ? `?${s}` : ''
}

// FIX: garantisce che la risposta sia sempre un array
function safeArray(promise) {
  return promise.then(data => {
    if (Array.isArray(data)) return data
    if (data == null) return []
    // Se è un oggetto con 'detail' è un errore del backend
    if (typeof data === 'object' && data.detail) throw new Error(data.detail)
    return []
  })
}

export const api = {
  // ── Sistema ───────────────────────────────────────────────────
  wake:    ()     => get(`/wake`),
  health:  ()     => get(`/health`),
  anni:    ()     => safeArray(get(`/kpi/saving/anni`)),
  filtri:  (p={}) => get(`/filtri/disponibili${qs(p)}`),

  // ── Saving KPI ────────────────────────────────────────────────
  riepilogo:    (p={}) => get(`/kpi/saving/riepilogo${qs(p)}`),
  mensile:      (p={}) => safeArray(get(`/kpi/saving/mensile${qs(p)}`)),
  mensileArea:  (p={}) => safeArray(get(`/kpi/saving/mensile-con-area${qs(p)}`)),
  perCdc:       (p={}) => safeArray(get(`/kpi/saving/per-cdc${qs(p)}`)),
  perBuyer:     (p={}) => safeArray(get(`/kpi/saving/per-buyer${qs(p)}`)),
  perAlfa:      (p={}) => safeArray(get(`/kpi/saving/per-alfa-documento${qs(p)}`)),
  perMacro:     (p={}) => safeArray(get(`/kpi/saving/per-macro-categoria${qs(p)}`)),
  perCommessa:  (p={}) => safeArray(get(`/kpi/saving/per-commessa${qs(p)}`)),
  perCategoria: (p={}) => safeArray(get(`/kpi/saving/per-categoria${qs(p)}`)),
  topFornitori: (p={}) => safeArray(get(`/kpi/saving/top-fornitori${qs(p)}`)),
  pareto:       (p={}) => safeArray(get(`/kpi/saving/pareto-fornitori${qs(p)}`)),
  concentration:(p={}) => get(`/kpi/saving/concentration-index${qs(p)}`),
  valute:       (p={}) => safeArray(get(`/kpi/saving/valute${qs(p)}`)),

  // Alias legacy
  savingPareto:       (p={}) => safeArray(get(`/kpi/saving/pareto-fornitori${qs(p)}`)),
  savingTopFornitori: (p={}) => safeArray(get(`/kpi/saving/top-fornitori${qs(p)}`)),
  savingAlfaDoc:      (p={}) => safeArray(get(`/kpi/saving/per-alfa-documento${qs(p)}`)),

  // ── YoY ───────────────────────────────────────────────────────
  yoy:     (p={}) => get(`/kpi/saving/yoy-granulare${qs(p)}`),
  yoyCdc:  (p={}) => safeArray(get(`/kpi/saving/yoy-cdc${qs(p)}`)),

  // ── Tempi ─────────────────────────────────────────────────────
  tempiRiepilogo:     () => get(`/kpi/tempi/riepilogo`),
  tempiMensile:       () => safeArray(get(`/kpi/tempi/mensile`)),
  tempiDist:          () => safeArray(get(`/kpi/tempi/distribuzione`)),
  tempiDistribuzione: () => safeArray(get(`/kpi/tempi/distribuzione`)),

  // ── NC ────────────────────────────────────────────────────────
  ncRiepilogo:    () => get(`/kpi/nc/riepilogo`),
  ncMensile:      () => safeArray(get(`/kpi/nc/mensile`)),
  ncTopFornitori: () => safeArray(get(`/kpi/nc/top-fornitori`)),
  ncPerTipo:      () => safeArray(get(`/kpi/nc/per-tipo`)),

  // ── Risorse ───────────────────────────────────────────────────
  risorseRiepilogo:  ()     => get(`/kpi/risorse/riepilogo`),
  risorsePerRisorsa: (p={}) => safeArray(get(`/kpi/risorse/per-risorsa${qs(p)}`)),
  risorseMensile:    (p={}) => safeArray(get(`/kpi/risorse/mensile${qs(p)}`)),

  // ── Upload ────────────────────────────────────────────────────
  // FIX: uploadLog garantito come array
  uploadLog: () => safeArray(get(`/upload/log`)),
  deleteUpload: (id) => fetch(`${BASE}/upload/${id}`, { method: 'DELETE' }).then(r => r.json()),

  inspectFile: async (file) => {
    const fd = new FormData()
    fd.append('file', file)
    const r = await fetch(`${BASE}/upload/inspect`, { method: 'POST', body: fd })
    if (!r.ok) {
      const text = await r.text().catch(() => '')
      throw new Error(text?.slice(0, 300) || `Errore ispezione: HTTP ${r.status}`)
    }
    return r.json()
  },

  uploadSaving: async (file, cdcOverride) => {
    const fd = new FormData()
    fd.append('file', file)
    const url = cdcOverride
      ? `${BASE}/upload/auto?cdc_override=${encodeURIComponent(cdcOverride)}`
      : `${BASE}/upload/auto`
    const r = await fetch(url, { method: 'POST', body: fd })
    const data = await r.json().catch(() => ({}))
    if (!r.ok) throw new Error(data?.detail || `HTTP ${r.status}`)
    return data
  },

  uploadRisorse: async (file) => {
    const fd = new FormData()
    fd.append('file', file)
    const r = await fetch(`${BASE}/upload/auto`, { method: 'POST', body: fd })
    const data = await r.json().catch(() => ({}))
    if (!r.ok) throw new Error(data?.detail || `HTTP ${r.status}`)
    return data
  },

  uploadTempi: async (file) => {
    const fd = new FormData()
    fd.append('file', file)
    const r = await fetch(`${BASE}/upload/auto`, { method: 'POST', body: fd })
    const data = await r.json().catch(() => ({}))
    if (!r.ok) throw new Error(data?.detail || `HTTP ${r.status}`)
    return data
  },

  uploadNc: async (file) => {
    const fd = new FormData()
    fd.append('file', file)
    const r = await fetch(`${BASE}/upload/auto`, { method: 'POST', body: fd })
    const data = await r.json().catch(() => ({}))
    if (!r.ok) throw new Error(data?.detail || `HTTP ${r.status}`)
    return data
  },

  // ── Export ────────────────────────────────────────────────────
  exportExcel: async (body) => {
    const r = await fetch(`${BASE}/export/custom/excel`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!r.ok) throw new Error(`Export failed: ${r.status}`)
    const blob = await r.blob()
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `report_${body?.filtri?.anno || 'tutti'}.xlsx`
    a.click()
    URL.revokeObjectURL(url)
  },
}
