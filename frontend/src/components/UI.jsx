import { clsx } from 'clsx'

export function Spinner({ size = 'md' }) {
  const sz = { sm: 'h-4 w-4', md: 'h-8 w-8', lg: 'h-12 w-12' }[size]
  return (
    <div className={`animate-spin rounded-full border-2 border-telethon-blue border-t-transparent ${sz}`} />
  )
}

export function LoadingBox() {
  return (
    <div className="flex items-center justify-center h-40">
      <Spinner />
    </div>
  )
}

export function ErrorBox({ message }) {
  return (
    <div className="rounded-lg bg-red-50 border border-red-200 p-4 text-sm text-red-700">
      {message}
    </div>
  )
}

export function KpiCard({ label, value, sub, trend, color = 'blue', icon }) {
  const colors = {
    blue: 'bg-telethon-blue',
    red: 'bg-telethon-red',
    green: 'bg-green-600',
    orange: 'bg-orange-500',
    purple: 'bg-purple-600',
  }
  return (
    <div className="kpi-card">
      <div className="flex items-start justify-between">
        <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">{label}</span>
        {icon && (
          <span className={`${colors[color]} text-white rounded-lg p-1.5 text-sm`}>{icon}</span>
        )}
      </div>
      <div className="text-2xl font-bold text-gray-900 mt-1">{value}</div>
      {sub && <div className="text-xs text-gray-500 mt-0.5">{sub}</div>}
      {trend != null && (
        <div className={clsx('text-xs font-medium mt-1', trend >= 0 ? 'text-green-600' : 'text-red-600')}>
          {trend >= 0 ? '↑' : '↓'} {Math.abs(trend).toFixed(1)}% vs mese prec.
        </div>
      )}
    </div>
  )
}

export function SectionTitle({ children }) {
  return <h2 className="section-title">{children}</h2>
}

export function Badge({ children, color = 'blue' }) {
  const cls = {
    blue: 'bg-blue-100 text-blue-700',
    green: 'bg-green-100 text-green-700',
    red: 'bg-red-100 text-red-700',
    orange: 'bg-orange-100 text-orange-700',
    gray: 'bg-gray-100 text-gray-600',
  }[color]
  return <span className={`badge ${cls}`}>{children}</span>
}

export function FilterBar({ anno, setAnno, strRic, setStrRic, cdc, setCdc }) {
  return (
    <div className="flex flex-wrap gap-3 mb-6">
      <select
        className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white focus:outline-none focus:ring-2 focus:ring-telethon-blue"
        value={anno}
        onChange={(e) => setAnno(e.target.value)}
      >
        <option value="">Tutti gli anni</option>
        <option value="2025">2025</option>
        <option value="2024">2024</option>
        <option value="2023">2023</option>
      </select>
      {setStrRic && (
        <select
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white focus:outline-none focus:ring-2 focus:ring-telethon-blue"
          value={strRic}
          onChange={(e) => setStrRic(e.target.value)}
        >
          <option value="">Ricerca + Struttura</option>
          <option value="RICERCA">Solo Ricerca</option>
          <option value="STRUTTURA">Solo Struttura</option>
        </select>
      )}
      {setCdc && (
        <select
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white focus:outline-none focus:ring-2 focus:ring-telethon-blue"
          value={cdc}
          onChange={(e) => setCdc(e.target.value)}
        >
          <option value="">Tutti i CDC</option>
          <option value="GD">GD</option>
          <option value="TIGEM">TIGEM</option>
          <option value="FT">FT</option>
          <option value="STRUTTURA">Struttura</option>
          <option value="Terapie">Terapie</option>
          <option value="TIGET">TIGET</option>
        </select>
      )}
    </div>
  )
}

export function DataTable({ columns, rows, maxRows = 15 }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-100">
            {columns.map((col) => (
              <th key={col.key} className="text-left py-2 px-3 text-xs font-semibold text-gray-500 uppercase tracking-wide whitespace-nowrap">
                {col.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, maxRows).map((row, i) => (
            <tr key={i} className="border-b border-gray-50 hover:bg-gray-50 transition-colors">
              {columns.map((col) => (
                <td key={col.key} className="py-2 px-3 text-gray-700 whitespace-nowrap">
                  {col.render ? col.render(row[col.key], row) : row[col.key]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
