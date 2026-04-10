/**
 * API client — tutti i metodi usati dai componenti.
 * Ogni metodo corrisponde esattamente a un endpoint del backend.
 */
const BASE = import.meta.env.VITE_API_URL || ''

async function get(path) {
  const r = await fetch(`${BASE}${path}`)
  if (!r.ok) throw new Error(`${r.status}: ${await r.text().then(t => t.slice(0, 200))}`)
  return r.json()
}

async function post(path, body) {
  const r = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) throw new Error(`${r.status}: ${await r.text().then(t => t.slice(0, 200))}`)
  return r.json()
}

function qs(p = {}) {
  const clean = Object.fromEntries(
    Object.entries(p).filter(([, v]) => v !== '' && v != null && v !== undefined)
  )
  const s = new URLSearchParams(clean).toString()
  return s ? `?${s}` : ''
}

export const api = {
  // ── Meta ──────────────────────────────────
  anni:           ()     => get(`/kpi/saving/anni`),
  filtri:         (p={}) => get(`/filtri/disponibili${qs(p)}`),
  wake:           ()     => get(`/wake`),

  // ── Saving KPI ────────────────────────────
  riepilogo:      (p={}) => get(`/kpi/saving/riepilogo${qs(p)}`),
  mensile:        (p={}) => get(`/kpi/saving/mensile${qs(p)}`),
  mensileArea:    (p={}) => get(`/kpi/saving/mensile-con-area${qs(p)}`),
  perCdc:         (p={}) => get(`/kpi/saving/per-cdc${qs(p)}`),
  perBuyer:       (p={}) => get(`/kpi/saving/per-buyer${qs(p)}`),
  perAlfa:        (p={}) => get(`/kpi/saving/per-alfa-documento${qs(p)}`),
  perMacro:       (p={}) => get(`/kpi/saving/per-macro-categoria${qs(p)}`),
  perCommessa:    (p={}) => get(`/kpi/saving/per-commessa${qs(p)}`),
  perCategoria:   (p={}) => get(`/kpi/saving/per-categoria${qs(p)}`),
  topFornitori:   (p={}) => get(`/kpi/saving/top-fornitori${qs(p)}`),
  pareto:         (p={}) => get(`/kpi/saving/pareto-fornitori${qs(p)}`),
  valute:         (p={}) => get(`/kpi/saving/valute${qs(p)}`),

  // Alias per compatibilità con Fornitori.jsx e AlfaDoc.jsx
  savingPareto:        (p={}) => get(`/kpi/saving/pareto-fornitori${qs(p)}`),
  savingTopFornitori:  (p={}) => get(`/kpi/saving/top-fornitori${qs(p)}`),
  savingAlfaDoc:       (p={}) => get(`/kpi/saving/per-alfa-documento${qs(p)}`),

  // ── YoY ───────────────────────────────────
  yoy:            (p={}) => get(`/kpi/saving/yoy-granulare${qs(p)}`),
  yoyCdc:         (p={}) => get(`/kpi/saving/yoy-cdc${qs(p)}`),

  // ── Tempi ─────────────────────────────────
  tempiRiepilogo:    ()     => get(`/kpi/tempi/riepilogo`),
  tempiMensile:      ()     => get(`/kpi/tempi/mensile`),
  tempiDist:         ()     => get(`/kpi/tempi/distribuzione`),
  tempiDistribuzione:()     => get(`/kpi/tempi/distribuzione`),  // alias

  // ── NC ────────────────────────────────────
  ncRiepilogo:    ()     => get(`/kpi/nc/riepilogo`),
  ncMensile:      ()     => get(`/kpi/nc/mensile`),
  ncTopFornitori: ()     => get(`/kpi/nc/top-fornitori`),
  ncPerTipo:      ()     => get(`/kpi/nc/per-tipo`),

  // ── Upload ────────────────────────────────
  uploadLog:      ()     => get(`/upload/log`),
  deleteUpload:   (id)   => fetch(`${BASE}/upload/${id}`, { method: 'DELETE' }).then(r => r.json()),

  // ── Export ────────────────────────────────
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
