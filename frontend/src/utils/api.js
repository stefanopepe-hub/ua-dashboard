const BASE = import.meta.env.VITE_API_URL || ''

export async function apiFetch(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  })
  if (!res.ok) throw new Error(`API error ${res.status}: ${await res.text()}`)
  return res.json()
}

function clean(p) {
  return Object.fromEntries(Object.entries(p).filter(([,v]) => v !== '' && v != null))
}

export const api = {
  // Meta
  anniDisponibili:      ()     => apiFetch('/kpi/saving/anni'),
  filtriDisponibili:    (p={}) => apiFetch('/filtri/disponibili?' + new URLSearchParams(clean(p))),
  periodoDisponibile:   (p={}) => apiFetch('/kpi/saving/periodo-disponibile?' + new URLSearchParams(clean(p))),

  // Saving KPI
  savingRiepilogo:      (p={}) => apiFetch('/kpi/saving/riepilogo?'           + new URLSearchParams(clean(p))),
  savingMensile:        (p={}) => apiFetch('/kpi/saving/mensile?'              + new URLSearchParams(clean(p))),
  savingMensileArea:    (p={}) => apiFetch('/kpi/saving/mensile-con-area?'     + new URLSearchParams(clean(p))),
  savingPerCdc:         (p={}) => apiFetch('/kpi/saving/per-cdc?'              + new URLSearchParams(clean(p))),
  savingPerBuyer:       (p={}) => apiFetch('/kpi/saving/per-buyer?'            + new URLSearchParams(clean(p))),
  savingAlfaDoc:        (p={}) => apiFetch('/kpi/saving/per-alfa-documento?'   + new URLSearchParams(clean(p))),
  savingMacroCategoria: (p={}) => apiFetch('/kpi/saving/per-macro-categoria?'  + new URLSearchParams(clean(p))),
  savingCommessa:       (p={}) => apiFetch('/kpi/saving/per-commessa?'         + new URLSearchParams(clean(p))),
  savingCategorie:      (p={}) => apiFetch('/kpi/saving/per-categoria?'        + new URLSearchParams(clean(p))),
  savingTopFornitori:   (p={}) => apiFetch('/kpi/saving/top-fornitori?'        + new URLSearchParams(clean(p))),
  savingPareto:         (p={}) => apiFetch('/kpi/saving/pareto-fornitori?'     + new URLSearchParams(clean(p))),
  savingValute:         (p={}) => apiFetch('/kpi/saving/valute?'               + new URLSearchParams(clean(p))),

  // YoY — usa sempre il confronto omogeneo
  savingYoy:            (p={}) => apiFetch('/kpi/saving/yoy-omogeneo?'         + new URLSearchParams(clean(p))),
  savingYoyGranulare:   (p={}) => apiFetch('/kpi/saving/yoy-granulare?'        + new URLSearchParams(clean(p))),
  savingYoyCdc:         (p={}) => apiFetch('/kpi/saving/yoy-cdc?'              + new URLSearchParams(clean(p))),

  // Tempi
  tempiRiepilogo:       ()     => apiFetch('/kpi/tempi/riepilogo'),
  tempiMensile:         ()     => apiFetch('/kpi/tempi/mensile'),
  tempiDistribuzione:   ()     => apiFetch('/kpi/tempi/distribuzione'),

  // NC
  ncRiepilogo:          ()     => apiFetch('/kpi/nc/riepilogo'),
  ncMensile:            ()     => apiFetch('/kpi/nc/mensile'),
  ncTopFornitori:       ()     => apiFetch('/kpi/nc/top-fornitori'),
  ncPerTipo:            ()     => apiFetch('/kpi/nc/per-tipo'),

  // Report builder
  buildReport: (body) => apiFetch('/report/build', {
    method: 'POST',
    body: JSON.stringify(body),
  }),

  // Upload
  uploadLog:    ()    => apiFetch('/upload/log'),
  deleteUpload: (id)  => apiFetch(`/upload/${id}`, { method: 'DELETE' }),

  // Export URLs
  exportCustomExcel: (body) => fetch(`${BASE}/export/custom/excel`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }).then(r => r.blob()).then(b => {
    const url = URL.createObjectURL(b)
    const a = document.createElement('a')
    a.href = url; a.download = 'report.xlsx'; a.click()
    URL.revokeObjectURL(url)
  }),
  exportCustomPdf: (body) => fetch(`${BASE}/export/custom/pdf`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }).then(r => r.blob()).then(b => {
    const url = URL.createObjectURL(b)
    const a = document.createElement('a')
    a.href = url; a.download = 'report.pdf'; a.click()
    URL.revokeObjectURL(url)
  }),
}
