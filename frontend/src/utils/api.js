const BASE = import.meta.env.VITE_API_URL || ''

async function apiFetch(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`API ${res.status}: ${text.slice(0, 200)}`)
  }
  return res.json()
}

function params(p = {}) {
  const clean = Object.fromEntries(Object.entries(p).filter(([, v]) => v !== '' && v != null))
  return new URLSearchParams(clean).toString()
}

export const api = {
  // Meta
  anni:              ()     => apiFetch('/kpi/saving/anni'),
  filtri:            (p={}) => apiFetch(`/filtri/disponibili?${params(p)}`),

  // KPI Saving
  riepilogo:         (p={}) => apiFetch(`/kpi/saving/riepilogo?${params(p)}`),
  mensile:           (p={}) => apiFetch(`/kpi/saving/mensile?${params(p)}`),
  mensileArea:       (p={}) => apiFetch(`/kpi/saving/mensile-con-area?${params(p)}`),
  perCdc:            (p={}) => apiFetch(`/kpi/saving/per-cdc?${params(p)}`),
  perBuyer:          (p={}) => apiFetch(`/kpi/saving/per-buyer?${params(p)}`),
  perAlfa:           (p={}) => apiFetch(`/kpi/saving/per-alfa-documento?${params(p)}`),
  perMacro:          (p={}) => apiFetch(`/kpi/saving/per-macro-categoria?${params(p)}`),
  perCommessa:       (p={}) => apiFetch(`/kpi/saving/per-commessa?${params(p)}`),
  perCategoria:      (p={}) => apiFetch(`/kpi/saving/per-categoria?${params(p)}`),
  topFornitori:      (p={}) => apiFetch(`/kpi/saving/top-fornitori?${params(p)}`),
  pareto:            (p={}) => apiFetch(`/kpi/saving/pareto-fornitori?${params(p)}`),
  valute:            (p={}) => apiFetch(`/kpi/saving/valute?${params(p)}`),

  // YoY
  yoy:               (p={}) => apiFetch(`/kpi/saving/yoy-granulare?${params(p)}`),
  yoyCdc:            (p={}) => apiFetch(`/kpi/saving/yoy-cdc?${params(p)}`),

  // Tempi
  tempiRiepilogo:    ()     => apiFetch('/kpi/tempi/riepilogo'),
  tempiMensile:      ()     => apiFetch('/kpi/tempi/mensile'),
  tempiDist:         ()     => apiFetch('/kpi/tempi/distribuzione'),

  // NC
  ncRiepilogo:       ()     => apiFetch('/kpi/nc/riepilogo'),
  ncMensile:         ()     => apiFetch('/kpi/nc/mensile'),
  ncTopFornitori:    ()     => apiFetch('/kpi/nc/top-fornitori'),
  ncPerTipo:         ()     => apiFetch('/kpi/nc/per-tipo'),

  // Upload
  uploadLog:         ()     => apiFetch('/upload/log'),
  deleteUpload:      (id)   => apiFetch(`/upload/${id}`, { method: 'DELETE' }),

  // Export
  exportExcel: async (body) => {
    const res = await fetch(`${BASE}/export/custom/excel`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    const blob = await res.blob()
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = 'report.xlsx'; a.click()
    URL.revokeObjectURL(url)
  },
}
