// Formattazione numeri — unica fonte di verità

export function fmtEur(v, decimali = 0) {
  if (v == null || isNaN(v)) return '—'
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return `€${(v / 1_000_000).toFixed(2)}M`
  if (abs >= 1_000)     return `€${(v / 1_000).toFixed(1)}K`
  return `€${v.toFixed(decimali)}`
}

export function fmtEurFull(v) {
  if (v == null || isNaN(v)) return '—'
  return new Intl.NumberFormat('it-IT', { style:'currency', currency:'EUR', maximumFractionDigits: 0 }).format(v)
}

export function fmtPct(v) {
  if (v == null || isNaN(v)) return '—'
  return `${Number(v).toFixed(2)}%`
}

export function fmtNum(v) {
  if (v == null || isNaN(v)) return '—'
  return Number(v).toLocaleString('it-IT')
}

export function fmtDays(v) {
  if (v == null || isNaN(v)) return '—'
  return `${Number(v).toFixed(1)} gg`
}

export const COLORS = {
  blue:   '#0057A8',
  red:    '#D81E1E',
  green:  '#16a34a',
  orange: '#ea580c',
  teal:   '#0891b2',
  purple: '#7c3aed',
  gray:   '#6b7280',
}

export const CDC_COLORS = {
  GD:        '#0057A8',
  TIGEM:     '#D81E1E',
  TIGET:     '#16a34a',
  FT:        '#ea580c',
  STRUTTURA: '#7c3aed',
  Terapie:   '#0891b2',
}

export function shortMese(ym) {
  // Converte "2025-03" -> "Mar"
  if (!ym) return '—'
  const MESI = ['Gen','Feb','Mar','Apr','Mag','Giu','Lug','Ago','Set','Ott','Nov','Dic']
  const parts = String(ym).split('-')
  if (parts.length === 2) {
    const m = parseInt(parts[1]) - 1
    return MESI[m] || ym
  }
  return ym
}
