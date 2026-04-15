/** fmt.js — Formattatori e palette enterprise */

const EUR2 = new Intl.NumberFormat('it-IT',{style:'currency',currency:'EUR',minimumFractionDigits:2,maximumFractionDigits:2})
const NUM  = new Intl.NumberFormat('it-IT')

export function fmtEur(v) {
  if (v==null||isNaN(v)) return '—'
  const abs=Math.abs(v)
  if (abs>=1_000_000) return `€${(v/1_000_000).toLocaleString('it-IT',{maximumFractionDigits:2})}M`
  if (abs>=1_000)     return `€${(v/1_000).toLocaleString('it-IT',{maximumFractionDigits:1})}K`
  return EUR2.format(v)
}
export function fmtNum(v)  { if(v==null||isNaN(v))return'—'; return NUM.format(Math.round(v)) }
export function fmtPct(v)  { if(v==null||isNaN(v))return'—'; return `${Number(v).toFixed(2)}%` }
export function fmtDays(v) { if(v==null||isNaN(v))return'—'; return `${Number(v).toFixed(1)} gg` }
export function fmtDate(v) { if(!v)return'—'; try{return new Date(v).toLocaleDateString('it-IT',{day:'2-digit',month:'short',year:'numeric'})}catch{return v} }

const _MESI = ['Gen','Feb','Mar','Apr','Mag','Giu','Lug','Ago','Set','Ott','Nov','Dic']
export function shortMese(v) {
  if (!v) return '—'
  const n = typeof v==='string' ? parseInt(v.split('-')[1]) : v
  return _MESI[n-1] || String(v)
}

export const COLORS = {
  blue:   '#0057A8', red:    '#D81E1E', green:  '#15803d',
  orange: '#ea580c', purple: '#7c3aed', teal:   '#0891b2',
  gray:   '#9ca3af', yellow: '#eab308',
}

export const CDC_COLORS = {
  GD: '#0057A8', TIGEM: '#D81E1E', TIGET: '#15803d', FT: '#ea580c', STRUTTURA: '#7c3aed',
}

export const CHART_PALETTE = [
  '#0057A8','#D81E1E','#15803d','#ea580c','#7c3aed','#0891b2','#9ca3af','#eab308',
]
