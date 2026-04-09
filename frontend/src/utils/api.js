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
  savingRiepilogo: (params = {}) => apiFetch('/kpi/saving/riepilogo?' + new URLSearchParams(params)),
  savingMensile: (params = {}) => apiFetch('/kpi/saving/mensile?' + new URLSearchParams(params)),
  savingPerCdc: (params = {}) => apiFetch('/kpi/saving/per-cdc?' + new URLSearchParams(params)),
  savingPerBuyer: (params = {}) => apiFetch('/kpi/saving/per-buyer?' + new URLSearchParams(params)),
  savingTopFornitori: (params = {}) => apiFetch('/kpi/saving/top-fornitori?' + new URLSearchParams(params)),
  savingPareto: (params = {}) => apiFetch('/kpi/saving/pareto-fornitori?' + new URLSearchParams(params)),
  savingCategorie: (params = {}) => apiFetch('/kpi/saving/per-categoria?' + new URLSearchParams(params)),
  savingValute: (params = {}) => apiFetch('/kpi/saving/valute?' + new URLSearchParams(params)),
  // Tempi
  tempiRiepilogo: () => apiFetch('/kpi/tempi/riepilogo'),
  tempiMensile: () => apiFetch('/kpi/tempi/mensile'),
  tempiDistribuzione: () => apiFetch('/kpi/tempi/distribuzione'),
  // NC
  ncRiepilogo: () => apiFetch('/kpi/nc/riepilogo'),
  ncMensile: () => apiFetch('/kpi/nc/mensile'),
  ncTopFornitori: () => apiFetch('/kpi/nc/top-fornitori'),
  ncPerTipo: () => apiFetch('/kpi/nc/per-tipo'),
  // Upload
  uploadLog: () => apiFetch('/upload/log'),
  deleteUpload: (id) => apiFetch(`/upload/${id}`, { method: 'DELETE' }),
}
