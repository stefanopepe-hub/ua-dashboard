/**
 * Design System — UA Dashboard Fondazione Telethon
 * Unica fonte di verità per tutti i componenti UI.
 */
import { AlertCircle, Loader2, Info, TrendingUp, TrendingDown, Minus } from 'lucide-react'
import { fmtEur, fmtPct, fmtNum } from '../utils/fmt'

// ─────────────────────────────────────────────────────────────────
// KPI CARD — 3 varianti: default, large (executive), compact
// ─────────────────────────────────────────────────────────────────

export function KpiCard({
  label, value, sub, delta, deltaLabel, color = 'blue',
  icon, size = 'default', accent = true
}) {
  const accentCls = {
    blue:   'kpi-card-accent-blue',
    green:  'kpi-card-accent-green',
    red:    'kpi-card-accent-red',
    orange: 'kpi-card-accent-orange',
    gray:   'kpi-card-accent-gray',
  }[color] || 'kpi-card-accent-gray'

  const labelCls = {
    blue:   'text-telethon-blue',
    green:  'text-green-600',
    red:    'text-red-600',
    orange: 'text-orange-600',
    gray:   'text-gray-400',
  }[color] || 'text-gray-400'

  if (size === 'large') {
    return (
      <div className={`kpi-card ${accent ? accentCls : ''}`}>
        <div className="flex items-start justify-between mb-3">
          <span className={`text-[11px] font-bold uppercase tracking-widest ${labelCls}`}>{label}</span>
          {icon && <div className={`p-2 rounded-xl bg-opacity-10 ${
            color === 'blue' ? 'bg-blue-50' : color === 'green' ? 'bg-green-50' : 'bg-gray-50'
          }`}>{icon}</div>}
        </div>
        <div className="text-3xl font-bold text-gray-900 tracking-tight">{value}</div>
        {delta != null && <DeltaBadge value={delta} label={deltaLabel} size="md" />}
        {sub && !delta && <p className="text-xs text-gray-400 mt-1.5">{sub}</p>}
      </div>
    )
  }

  if (size === 'compact') {
    return (
      <div className="card-sm">
        <div className={`text-[10px] font-bold uppercase tracking-wider mb-1 ${labelCls}`}>{label}</div>
        <div className="text-lg font-bold text-gray-900">{value}</div>
        {sub && <div className="text-xs text-gray-400">{sub}</div>}
      </div>
    )
  }

  return (
    <div className={`kpi-card ${accent ? accentCls : ''}`}>
      <span className={`text-[11px] font-bold uppercase tracking-widest ${labelCls} block mb-1`}>{label}</span>
      <div className="text-2xl font-bold text-gray-900 tracking-tight">{value}</div>
      {delta != null && <DeltaBadge value={delta} label={deltaLabel} />}
      {sub && !delta && <p className="text-xs text-gray-400 mt-1">{sub}</p>}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────
// DELTA BADGE — variazione YoY
// ─────────────────────────────────────────────────────────────────

export function DeltaBadge({ value, suffix = '%', label = '', size = 'sm' }) {
  if (value == null) return <span className="text-xs text-gray-300 mt-1 block">—</span>
  const up = value > 0
  const zero = value === 0
  const cls = zero ? 'text-gray-400' : up ? 'text-green-600' : 'text-red-500'
  const Icon = zero ? Minus : up ? TrendingUp : TrendingDown
  const textSize = size === 'md' ? 'text-sm' : 'text-xs'
  return (
    <div className={`flex items-center gap-1 mt-1 ${textSize} ${cls} font-semibold`}>
      <Icon className="h-3 w-3" />
      <span>{zero ? '—' : `${up ? '+' : ''}${Math.abs(value).toFixed(1)}${suffix}`}</span>
      {label && <span className="text-gray-400 font-normal">{label}</span>}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────
// FILTER BAR
// ─────────────────────────────────────────────────────────────────

export function FilterBar({ anno, setAnno, strRic, setStrRic, cdc, setCdc, anni = [] }) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <select value={anno} onChange={e => setAnno(e.target.value)}
        className="filter-select font-semibold text-telethon-blue border-2 border-telethon-blue/30 hover:border-telethon-blue">
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
          {['GD', 'TIGEM', 'TIGET', 'FT', 'STRUTTURA'].map(c => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
      )}
    </div>
  )
}

export function GranSelect({ value, onChange }) {
  return (
    <select value={value} onChange={e => onChange(e.target.value)}
      className="filter-select font-semibold text-telethon-blue border-2 border-telethon-blue/30">
      <option value="mensile">Mensile</option>
      <option value="bimestrale">Bimestrale</option>
      <option value="quarter">Trimestrale</option>
      <option value="semestrale">Semestrale</option>
      <option value="annuale">Annuale</option>
    </select>
  )
}

// ─────────────────────────────────────────────────────────────────
// STATES — Loading, Error, Empty
// ─────────────────────────────────────────────────────────────────

export function LoadingBox({ rows = 3 }) {
  return (
    <div className="space-y-3 py-4">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="skeleton h-8 w-full" style={{ opacity: 1 - i * 0.2 }} />
      ))}
    </div>
  )
}

export function LoadingSpinner({ size = 'md' }) {
  const sz = size === 'sm' ? 'h-4 w-4' : size === 'lg' ? 'h-8 w-8' : 'h-5 w-5'
  return (
    <div className="flex items-center justify-center py-12">
      <Loader2 className={`${sz} animate-spin text-telethon-blue`} />
    </div>
  )
}

export function ErrorBox({ message, title }) {
  // Messaggi di errore specifici e leggibili
  const display = typeof message === 'string'
    ? message.replace(/^(\d{3}): /, '').slice(0, 200)
    : 'Errore imprevisto'
  return (
    <div className="alert-error">
      <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5" />
      <div>
        {title && <div className="font-semibold mb-0.5">{title}</div>}
        <div>{display}</div>
      </div>
    </div>
  )
}

export function EmptyState({ title = 'Nessun dato', message, icon }) {
  return (
    <div className="flex flex-col items-center justify-center py-12 text-center">
      {icon && <div className="mb-3 text-gray-300">{icon}</div>}
      <div className="text-sm font-semibold text-gray-500">{title}</div>
      {message && <div className="text-xs text-gray-400 mt-1 max-w-xs">{message}</div>}
    </div>
  )
}

export function InfoBox({ children }) {
  return (
    <div className="alert-info">
      <Info className="h-4 w-4 flex-shrink-0 mt-0.5" />
      <div>{children}</div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────
// SKELETON CARDS
// ─────────────────────────────────────────────────────────────────

export function SkeletonKpiGrid({ count = 4 }) {
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="kpi-card">
          <div className="skeleton h-3 w-20 mb-3" />
          <div className="skeleton h-8 w-28 mb-2" />
          <div className="skeleton h-3 w-16" />
        </div>
      ))}
    </div>
  )
}

export function SkeletonChart({ height = 260 }) {
  return (
    <div className="card">
      <div className="skeleton h-3 w-48 mb-5" />
      <div className="skeleton rounded-xl" style={{ height }} />
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────
// PAGE HEADER
// ─────────────────────────────────────────────────────────────────

export function PageHeader({ title, subtitle, actions }) {
  return (
    <div className="flex items-start justify-between mb-6">
      <div>
        <h1 className="page-title">{title}</h1>
        {subtitle && <p className="page-subtitle">{subtitle}</p>}
      </div>
      {actions && <div className="flex items-center gap-2 flex-shrink-0">{actions}</div>}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────
// SECTION TITLE
// ─────────────────────────────────────────────────────────────────

export function SectionTitle({ children, sub }) {
  return (
    <div className="mb-4">
      <div className="section-title">{children}</div>
      {sub && <p className="text-xs text-gray-400 -mt-3">{sub}</p>}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────
// BADGE
// ─────────────────────────────────────────────────────────────────

export function Badge({ color = 'gray', children, dot = false }) {
  const cls = {
    blue:   'badge-blue',
    green:  'badge-green',
    red:    'badge-red',
    orange: 'badge-orange',
    gray:   'badge-gray',
    amber:  'badge-amber',
  }[color] || 'badge-gray'
  const dotCls = {
    blue: 'bg-[#0057A8]', green: 'bg-green-500', red: 'bg-red-500',
    orange: 'bg-orange-500', gray: 'bg-gray-400', amber: 'bg-amber-500',
  }[color] || 'bg-gray-400'
  return (
    <span className={`badge ${cls}`}>
      {dot && <span className={`inline-block w-1.5 h-1.5 rounded-full mr-1.5 ${dotCls}`} />}
      {children}
    </span>
  )
}

// ─────────────────────────────────────────────────────────────────
// DATA TABLE — con paginazione opzionale
// ─────────────────────────────────────────────────────────────────

export function DataTable({ columns, rows, maxRows, emptyMessage, rowKey }) {
  const data = maxRows ? rows.slice(0, maxRows) : rows
  if (!data?.length) {
    return (
      <p className="text-sm text-gray-400 text-center py-8">
        {emptyMessage || 'Nessun dato disponibile'}
      </p>
    )
  }
  return (
    <div className="overflow-x-auto">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map(c => (
              <th key={c.key} className={c.align === 'left' ? '' : 'right'}>{c.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={rowKey ? rowKey(row, i) : (row?.id ?? i)}>
              {columns.map(c => (
                <td key={c.key} className={`${c.align === 'left' ? '' : 'text-right'} ${c.mono ? 'tabular' : ''}`}>
                  {c.render ? c.render(row[c.key], row) : (row[c.key] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────
// CHART WRAPPER — gestisce loading/error/empty uniformemente
// ─────────────────────────────────────────────────────────────────

export function ChartCard({
  title, subtitle, loading, error, empty, emptyMessage,
  children, height = 260, actions
}) {
  return (
    <div className="card">
      <div className="flex items-start justify-between mb-1">
        <div>
          <SectionTitle>{title}</SectionTitle>
          {subtitle && <p className="text-xs text-gray-400 -mt-3 mb-4">{subtitle}</p>}
        </div>
        {actions && <div className="flex-shrink-0">{actions}</div>}
      </div>
      {loading ? (
        <LoadingBox rows={2} />
      ) : error ? (
        <ErrorBox message={error} />
      ) : empty ? (
        <EmptyState message={emptyMessage || 'Nessun dato per il periodo selezionato'} />
      ) : (
        children
      )}
    </div>
  )
}
