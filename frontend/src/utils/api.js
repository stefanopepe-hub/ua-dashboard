const BASE = import.meta.env.VITE_API_URL || ''

export async function apiFetch(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  })
  if (!res.ok) throw new Error(`API error ${res.status}: ${await res.text()}`)
  return res.json()
}

export const api = {
  // Saving
  savingRiepilogo:  (p={}) => apiFetch('/kpi/saving/riepilogo?'   + new URLSearchParams(clean(p))),
  savingMensile:    (p={}) => apiFetch('/kpi/saving/mensile?'      + new URLSearchParams(clean(p))),
  savingPerCdc:     (p={}) => apiFetch('/kpi/saving/per-cdc?'      + new URLSearchParams(clean(p))),
  savingPerBuyer:   (p={}) => apiFetch('/kpi/saving/per-buyer?'    + new URLSearchParams(clean(p))),
  savingTopFornitori:(p={})=> apiFetch('/kpi/saving/top-fornitori?'+ new URLSearchParams(clean(p))),
  savingPareto:     (p={}) => apiFetch('/kpi/saving/pareto-fornitori?'+new URLSearchParams(clean(p))),
  savingCategorie:  (p={}) => apiFetch('/kpi/saving/per-categoria?'+ new URLSearchParams(clean(p))),
  savingValute:     (p={}) => apiFetch('/kpi/saving/valute?'       + new URLSearchParams(clean(p))),
  // YoY
  savingYoy:        (p={}) => apiFetch('/kpi/saving/yoy?'          + new URLSearchParams(clean(p))),
  savingYoyCdc:     (p={}) => apiFetch('/kpi/saving/yoy-cdc?'      + new URLSearchParams(clean(p))),
  // Tempi
  tempiRiepilogo:   ()     => apiFetch('/kpi/tempi/riepilogo'),
  tempiMensile:     ()     => apiFetch('/kpi/tempi/mensile'),
  tempiDistribuzione:()    => apiFetch('/kpi/tempi/distribuzione'),
  // NC
  ncRiepilogo:      ()     => apiFetch('/kpi/nc/riepilogo'),
  ncMensile:        ()     => apiFetch('/kpi/nc/mensile'),
  ncTopFornitori:   ()     => apiFetch('/kpi/nc/top-fornitori'),
  ncPerTipo:        ()     => apiFetch('/kpi/nc/per-tipo'),
  // Anni disponibili
  anniDisponibili: () => apiFetch('/kpi/saving/anni'),
  // Upload
  uploadLog:        ()     => apiFetch('/upload/log'),
  deleteUpload:     (id)   => apiFetch(`/upload/${id}`, { method: 'DELETE' }),
  // Export (returns URL to open)
  exportSavingExcel:(p={}) => `${BASE}/export/saving/excel?${new URLSearchParams(clean(p))}`,
  exportTempiExcel: ()     => `${BASE}/export/tempi/excel`,
  exportNcExcel:    ()     => `${BASE}/export/nc/excel`,
  exportReportPdf:  (p={}) => `${BASE}/export/report/pdf?${new URLSearchParams(clean(p))}`,
}

function clean(p) {
  return Object.fromEntries(Object.entries(p).filter(([,v]) => v !== '' && v != null))
}
