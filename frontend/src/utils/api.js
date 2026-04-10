/**
 * API client v9 — UA Dashboard Fondazione Telethon
 * Tutti gli endpoint sono allineati al backend v9.
 */
const BASE = import.meta.env.VITE_API_URL || ''

async function get(path) {
  const r = await fetch(`${BASE}${path}`)
  if (!r.ok) {
    const text = await r.text().catch(() => '')
    // Errori domain-aware invece di "Failed to fetch" generico
    const msg = text?.slice(0, 300) || `HTTP ${r.status}`
    throw new Error(msg)
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

export const api = {
  // ── Sistema ───────────────────────────────────────────────────
  wake:    ()     => get(`/wake`),
  health:  ()     => get(`/health`),
  anni:    ()     => get(`/kpi/saving/anni`),
  filtri:  (p={}) => get(`/filtri/disponibili${qs(p)}`),

  // ── Saving KPI ────────────────────────────────────────────────
  riepilogo:   (p={}) => get(`/kpi/saving/riepilogo${qs(p)}`),
  mensile:     (p={}) => get(`/kpi/saving/mensile${qs(p)}`),
  mensileArea: (p={}) => get(`/kpi/saving/mensile-con-area${qs(p)}`),
  perCdc:      (p={}) => get(`/kpi/saving/per-cdc${qs(p)}`),
  perBuyer:    (p={}) => get(`/kpi/saving/per-buyer${qs(p)}`),
  perAlfa:     (p={}) => get(`/kpi/saving/per-alfa-documento${qs(p)}`),
  perMacro:    (p={}) => get(`/kpi/saving/per-macro-categoria${qs(p)}`),
  perCommessa: (p={}) => get(`/kpi/saving/per-commessa${qs(p)}`),
  perCategoria:(p={}) => get(`/kpi/saving/per-categoria${qs(p)}`),
  topFornitori:(p={}) => get(`/kpi/saving/top-fornitori${qs(p)}`),
  pareto:      (p={}) => get(`/kpi/saving/pareto-fornitori${qs(p)}`),
  valute:      (p={}) => get(`/kpi/saving/valute${qs(p)}`),

  // Alias legacy (usati in Fornitori.jsx e AlfaDoc.jsx)
  savingPareto:       (p={}) => get(`/kpi/saving/pareto-fornitori${qs(p)}`),
  savingTopFornitori: (p={}) => get(`/kpi/saving/top-fornitori${qs(p)}`),
  savingAlfaDoc:      (p={}) => get(`/kpi/saving/per-alfa-documento${qs(p)}`),

  // ── YoY ───────────────────────────────────────────────────────
  yoy:     (p={}) => get(`/kpi/saving/yoy-granulare${qs(p)}`),
  yoyCdc:  (p={}) => get(`/kpi/saving/yoy-cdc${qs(p)}`),

  // ── Tempi ─────────────────────────────────────────────────────
  tempiRiepilogo:    () => get(`/kpi/tempi/riepilogo`),
  tempiMensile:      () => get(`/kpi/tempi/mensile`),
  tempiDist:         () => get(`/kpi/tempi/distribuzione`),
  tempiDistribuzione:() => get(`/kpi/tempi/distribuzione`),

  // ── NC ────────────────────────────────────────────────────────
  ncRiepilogo:    () => get(`/kpi/nc/riepilogo`),
  ncMensile:      () => get(`/kpi/nc/mensile`),
  ncTopFornitori: () => get(`/kpi/nc/top-fornitori`),
  ncPerTipo:      () => get(`/kpi/nc/per-tipo`),

  // ── Risorse ───────────────────────────────────────────────────
  risorseRiepilogo: ()     => get(`/kpi/risorse/riepilogo`),
  risorsePerRisorsa:(p={}) => get(`/kpi/risorse/per-risorsa${qs(p)}`),
  risorseMensile:   (p={}) => get(`/kpi/risorse/mensile${qs(p)}`),

  // ── Upload ────────────────────────────────────────────────────
  uploadLog:     ()    => get(`/upload/log`),
  deleteUpload:  (id)  => fetch(`${BASE}/upload/${id}`, { method: 'DELETE' }).then(r => r.json()),

  /** Ispeziona file senza importarlo (smart preview) */
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

  /** Importa file saving — stessa pipeline del preview */
  uploadSaving: async (file, cdcOverride) => {
    const fd = new FormData()
    fd.append('file', file)
    const url = cdcOverride
      ? `${BASE}/upload/saving?cdc_override=${encodeURIComponent(cdcOverride)}`
      : `${BASE}/upload/saving`
    const r = await fetch(url, { method: 'POST', body: fd })
    const data = await r.json()
    if (!r.ok) throw new Error(data?.detail || `HTTP ${r.status}`)
    return data
  },

  uploadRisorse: async (file) => {
    const fd = new FormData()
    fd.append('file', file)
    const r = await fetch(`${BASE}/upload/risorse`, { method: 'POST', body: fd })
    const data = await r.json()
    if (!r.ok) throw new Error(data?.detail || `HTTP ${r.status}`)
    return data
  },

  uploadTempi: async (file) => {
    const fd = new FormData()
    fd.append('file', file)
    const r = await fetch(`${BASE}/upload/tempi`, { method: 'POST', body: fd })
    const data = await r.json()
    if (!r.ok) throw new Error(data?.detail || `HTTP ${r.status}`)
    return data
  },

  uploadNc: async (file) => {
    const fd = new FormData()
    fd.append('file', file)
    const r = await fetch(`${BASE}/upload/nc`, { method: 'POST', body: fd })
    const data = await r.json()
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
