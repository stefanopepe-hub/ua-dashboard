export const fmtEur = (v, decimals = 0) => {
  if (v == null || isNaN(v)) return '—'
  return new Intl.NumberFormat('it-IT', {
    style: 'currency',
    currency: 'EUR',
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(v)
}

export const fmtPct = (v, decimals = 1) => {
  if (v == null || isNaN(v)) return '—'
  return `${Number(v).toFixed(decimals)}%`
}

export const fmtNum = (v) => {
  if (v == null || isNaN(v)) return '—'
  return new Intl.NumberFormat('it-IT').format(v)
}

export const fmtDays = (v) => {
  if (v == null || isNaN(v)) return '—'
  return `${Number(v).toFixed(1)} gg`
}

export const MESI_IT = {
  '01': 'Gen', '02': 'Feb', '03': 'Mar', '04': 'Apr',
  '05': 'Mag', '06': 'Giu', '07': 'Lug', '08': 'Ago',
  '09': 'Set', '10': 'Ott', '11': 'Nov', '12': 'Dic',
}

export const shortMese = (ym) => {
  if (!ym) return ''
  const [, m] = ym.split('-')
  return MESI_IT[m] || ym
}

// Telethon brand colors
export const COLORS = {
  blue: '#0057A8',
  red: '#D81E1E',
  green: '#16a34a',
  orange: '#ea580c',
  purple: '#7c3aed',
  teal: '#0891b2',
  gray: '#6b7280',
}

export const CDC_COLORS = {
  GD: '#0057A8',
  TIGEM: '#D81E1E',
  FT: '#7c3aed',
  STRUTTURA: '#0891b2',
  Terapie: '#ea580c',
  TIGET: '#16a34a',
}
