import { AlertCircle, Loader2 } from 'lucide-react'
import { fmtEur, fmtPct, fmtNum, COLORS } from '../utils/fmt'

// ── KPI Card ─────────────────────────────────────────────────────
export function KpiCard({ label, value, sub, color = 'blue', icon }) {
  const bg = { blue:'bg-blue-50', green:'bg-green-50', orange:'bg-orange-50',
                red:'bg-red-50', purple:'bg-purple-50', gray:'bg-gray-50' }[color] || 'bg-blue-50'
  const tc = { blue:'text-telethon-blue', green:'text-green-700', orange:'text-orange-600',
                red:'text-telethon-red', purple:'text-purple-700', gray:'text-gray-600' }[color] || 'text-telethon-blue'
  return (
    <div className="kpi-card">
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs font-bold text-gray-500 uppercase tracking-wide">{label}</span>
        {icon && <div className={`${bg} ${tc} p-1.5 rounded-lg`}>{icon}</div>}
      </div>
      <div className="text-2xl font-bold text-gray-900 leading-tight">{value}</div>
      {sub && <div className="text-xs text-gray-500 mt-0.5">{sub}</div>}
    </div>
  )
}

// ── Delta badge YoY ──────────────────────────────────────────────
export function DeltaBadge({ value, suffix = '%', label = '' }) {
  if (value == null) return <span className="text-xs text-gray-400">—</span>
  const up = value >= 0
  return (
    <div className="flex items-center gap-1 mt-0.5">
      <span className={`text-xs font-bold ${up ? 'text-green-600' : 'text-red-500'}`}>
        {up ? '▲' : '▼'} {Math.abs(value).toFixed(1)}{suffix}
      </span>
      {label && <span className="text-xs text-gray-400">{label}</span>}
    </div>
  )
}

// ── Filtri ───────────────────────────────────────────────────────
export function FilterBar({ anno, setAnno, strRic, setStrRic, cdc, setCdc, anni = [] }) {
  return (
    <div className="flex flex-wrap gap-3">
      <select value={anno} onChange={e => setAnno(e.target.value)}
        className="filter-select font-semibold text-telethon-blue border-2 border-telethon-blue">
        <option value="">Tutti gli anni</option>
        {anni.map(a => <option key={a} value={String(a)}>{a}</option>)}
      </select>

      {setStrRic && (
        <select value={strRic} onChange={e => setStrRic(e.target.value)} className="filter-select">
          <option value="">Ricerca + Struttura</option>
          <option value="RICERCA">Solo Ricerca</option>
          <option value="STRUTTURA">Solo Struttura</option>
        </select>
      )}

      {setCdc && (
        <select value={cdc} onChange={e => setCdc(e.target.value)} className="filter-select">
          <option value="">Tutti i CDC</option>
          {['GD','TIGEM','TIGET','FT','STRUTTURA'].map(c =>
            <option key={c} value={c}>{c}</option>)}
        </select>
      )}
    </div>
  )
}

export function GranSelect({ value, onChange }) {
  const opts = [
    { value:'mensile',    label:'Mensile' },
    { value:'bimestrale', label:'Bimestrale' },
    { value:'quarter',    label:'Trimestrale' },
    { value:'semestrale', label:'Semestrale' },
    { value:'annuale',    label:'Annuale' },
  ]
  return (
    <select value={value} onChange={e => onChange(e.target.value)}
      className="filter-select font-semibold text-telethon-blue border-2 border-telethon-blue">
      {opts.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
  )
}

// ── Loading / Error ───────────────────────────────────────────────
export function LoadingBox() {
  return (
    <div className="flex items-center justify-center py-12 gap-3 text-gray-400">
      <Loader2 className="h-5 w-5 animate-spin" />
      <span className="text-sm">Caricamento…</span>
    </div>
  )
}

export function ErrorBox({ message }) {
  return (
    <div className="flex items-start gap-3 bg-red-50 border border-red-100 rounded-xl px-4 py-3 text-sm text-red-700">
      <AlertCircle className="h-4 w-4 mt-0.5 flex-shrink-0" />
      <span>{message}</span>
    </div>
  )
}

// ── Section title ─────────────────────────────────────────────────
export function SectionTitle({ children }) {
  return <h3 className="text-xs font-bold text-gray-500 uppercase tracking-widest mb-4">{children}</h3>
}

// ── Badge ─────────────────────────────────────────────────────────
export function Badge({ color = 'gray', children }) {
  const cls = {
    blue:   'bg-blue-100 text-blue-700',
    green:  'bg-green-100 text-green-700',
    red:    'bg-red-100 text-red-700',
    orange: 'bg-orange-100 text-orange-700',
    gray:   'bg-gray-100 text-gray-600',
  }[color] || 'bg-gray-100 text-gray-600'
  return <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${cls}`}>{children}</span>
}

// ── Tabella generica ─────────────────────────────────────────────
export function DataTable({ columns, rows, maxRows }) {
  const data = maxRows ? rows.slice(0, maxRows) : rows
  if (!data?.length) return <p className="text-sm text-gray-400 py-4 text-center">Nessun dato</p>
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-gray-100">
          {columns.map(c => (
            <th key={c.key} className={`py-1.5 px-2 text-xs font-bold text-gray-500 uppercase
              ${c.align === 'left' ? 'text-left' : 'text-right'}`}>{c.label}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => (
          <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
            {columns.map(c => (
              <td key={c.key} className={`py-1.5 px-2 ${c.align === 'left' ? 'text-left' : 'text-right'} tabular-nums`}>
                {c.render ? c.render(row[c.key], row) : (row[c.key] ?? '—')}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  )
}
