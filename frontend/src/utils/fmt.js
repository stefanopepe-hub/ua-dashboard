// fmt.js — Formattazione numeri, date, colori. Unica fonte di verità.

export function fmtEur(v, decimali = 0) {
  if (v == null || isNaN(v)) return '—'
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return `€${(v / 1_000_000).toFixed(2)}M`
  if (abs >= 1_000)     return `€${(v / 1_000).toFixed(1)}K`
  return `€${Number(v).toFixed(decimali)}`
}

export function fmtEurFull(v) {
  if (v == null || isNaN(v)) return '—'
  return new Intl.NumberFormat('it-IT', {
    style: 'currency', currency: 'EUR', maximumFractionDigits: 0
  }).format(v)
}

export function fmtPct(v) {
  if (v == null || isNaN(v)) return '—'
  return `${Number(v).toFixed(2)}%`
}

export function fmtPct1(v) {
  if (v == null || isNaN(v)) return '—'
  return `${Number(v).toFixed(1)}%`
}

export function fmtNum(v) {
  if (v == null || isNaN(v)) return '—'
  return Number(v).toLocaleString('it-IT')
}

export function fmtDays(v) {
  if (v == null || isNaN(v)) return '—'
  return `${Number(v).toFixed(1)} gg`
}

export function shortMese(ym) {
  if (!ym) return '—'
  const MESI = ['Gen','Feb','Mar','Apr','Mag','Giu','Lug','Ago','Set','Ott','Nov','Dic']
  const parts = String(ym).split('-')
  if (parts.length === 2) {
    const m = parseInt(parts[1], 10) - 1
    return MESI[m] ?? ym
  }
  return ym
}

export function fmtDate(iso) {
  if (!iso) return '—'
  try {
    return new Date(iso).toLocaleDateString('it-IT')
  } catch {
    return iso
  }
}

// ─── Color palettes ───────────────────────────────────────────────
export const COLORS = {
  blue:   '#0057A8',
  red:    '#D81E1E',
  green:  '#15803d',
  orange: '#ea580c',
  teal:   '#0891b2',
  purple: '#7c3aed',
  amber:  '#d97706',
  gray:   '#9ca3af',
  // light variants
  blueLight:  '#93c5fd',
  greenLight: '#86efac',
  redLight:   '#fca5a5',
}

export const CDC_COLORS = {
  GD:        '#0057A8',
  TIGEM:     '#D81E1E',
  TIGET:     '#15803d',
  FT:        '#ea580c',
  STRUTTURA: '#7c3aed',
}

export const CDC_ORDER = ['GD', 'TIGEM', 'TIGET', 'FT', 'STRUTTURA']

// Chart palette — per serie ordinate
export const CHART_PALETTE = [
  '#0057A8', '#D81E1E', '#15803d', '#ea580c', '#7c3aed',
  '#0891b2', '#d97706', '#9ca3af', '#1d4ed8', '#dc2626',
]
