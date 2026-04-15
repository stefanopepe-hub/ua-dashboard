/**
 * fmt.js — Formattatori e palette colori enterprise
 * Fondazione Telethon ETS — UA Dashboard
 */

// ── Formatters ─────────────────────────────────────────────────────────────

const EUR = new Intl.NumberFormat('it-IT', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })
const EUR2 = new Intl.NumberFormat('it-IT', { style: 'currency', currency: 'EUR', minimumFractionDigits: 2, maximumFractionDigits: 2 })
const NUM = new Intl.NumberFormat('it-IT')

export function fmtEur(v) {
  if (v == null || isNaN(v)) return '—'
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return `€${(v / 1_000_000).toLocaleString('it-IT', { maximumFractionDigits: 2 })}M`
  if (abs >= 1_000)     return `€${(v / 1_000).toLocaleString('it-IT', { maximumFractionDigits: 1 })}K`
  return EUR2.format(v)
}

export function fmtNum(v) {
  if (v == null || isNaN(v)) return '—'
  return NUM.format(Math.round(v))
}

export function fmtPct(v) {
  if (v == null || isNaN(v)) return '—'
  return `${Number(v).toFixed(2)}%`
}

export function fmtDays(v) {
  if (v == null || isNaN(v)) return '—'
  return `${Number(v).toFixed(1)} gg`
}

export function fmtDate(v) {
  if (!v) return '—'
  try {
    return new Date(v).toLocaleDateString('it-IT', { day: '2-digit', month: 'short', year: 'numeric' })
  } catch { return v }
}

// ── Palette colori enterprise ───────────────────────────────────────────────

export const COLORS = {
  blue:   '#0057A8',   // Telethon brand blue
  red:    '#D81E1E',   // Telethon brand red
  green:  '#16a34a',
  orange: '#f59e0b',
  purple: '#7c3aed',
  teal:   '#0891b2',
  gray:   '#9ca3af',
  yellow: '#eab308',
}

export const CDC_COLORS = {
  GD:        '#0057A8',
  TIGEM:     '#D81E1E',
  TIGET:     '#16a34a',
  FT:        '#f59e0b',
  STRUTTURA: '#7c3aed',
}

export const CHART_PALETTE = [
  '#0057A8', '#D81E1E', '#16a34a', '#f59e0b',
  '#7c3aed', '#0891b2', '#9ca3af', '#eab308',
  '#ec4899', '#14b8a6',
]

export const SPEND_BUCKET_COLORS = {
  'Materiali di Consumo': '#0057A8',
  'Servizi':              '#f59e0b',
  'Strumentazione':       '#16a34a',
  'Non Classificato':     '#9ca3af',
}

export function shortMese(v) {
  const m = ['Gen','Feb','Mar','Apr','Mag','Giu','Lug','Ago','Set','Ott','Nov','Dic']
  if (!v) return '—'
  const n = typeof v === 'string' ? parseInt(v.split('-')[1]) : v
  return m[n - 1] || String(v)
}
