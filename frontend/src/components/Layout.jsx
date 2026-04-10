import { useEffect, useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import {
  LayoutDashboard, TrendingUp, Clock, AlertTriangle,
  Building2, FileText, Upload, Database, ChevronRight,
} from 'lucide-react'

const NAV = [
  {
    section: 'Executive',
    items: [
      { to: '/',          icon: LayoutDashboard, label: 'Dashboard' },
    ],
  },
  {
    section: 'Analytics',
    items: [
      { to: '/saving',    icon: TrendingUp,  label: 'Saving & Ordini' },
      { to: '/fornitori', icon: Building2,   label: 'Fornitori' },
      { to: '/alfa-doc',  icon: FileText,    label: 'Tipologie Doc.' },
    ],
  },
  {
    section: 'Operativo',
    items: [
      { to: '/tempi',     icon: Clock,           label: 'Tempi Attraversamento' },
      { to: '/nc',        icon: AlertTriangle,   label: 'Non Conformità' },
    ],
  },
  {
    section: 'Dati',
    items: [
      { to: '/upload',    icon: Upload,   label: 'Carica Dati' },
      { to: '/quality',   icon: Database, label: 'Data Quality' },
    ],
  },
]

export default function Layout({ children }) {
  const [backendStatus, setBackendStatus] = useState('waking') // waking | ok | error
  const location = useLocation()

  useEffect(() => {
    const BASE = import.meta.env.VITE_API_URL || ''
    fetch(`${BASE}/wake`, { signal: AbortSignal.timeout(15000) })
      .then(() => setBackendStatus('ok'))
      .catch(() => setBackendStatus('error'))
  }, [])

  // Titolo pagina corrente
  const currentNav = NAV.flatMap(s => s.items).find(i =>
    i.to === '/' ? location.pathname === '/' : location.pathname.startsWith(i.to)
  )

  return (
    <div className="flex h-screen bg-[#f4f6f9] overflow-hidden">
      {/* ── Sidebar ── */}
      <aside className="w-60 flex-shrink-0 bg-white border-r border-gray-100 flex flex-col shadow-sm">
        {/* Logo */}
        <div className="px-5 py-5">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 bg-[#0057A8] rounded-xl flex items-center justify-center flex-shrink-0">
              <span className="text-white text-xs font-bold tracking-tight">T</span>
            </div>
            <div className="min-w-0">
              <div className="text-[11px] font-bold text-gray-900 leading-tight truncate">Fondazione Telethon</div>
              <div className="text-[10px] text-gray-400 font-medium">Ufficio Acquisti</div>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav className="flex-1 px-3 overflow-y-auto pb-4">
          {NAV.map(({ section, items }) => (
            <div key={section}>
              <div className="nav-section-label">{section}</div>
              {items.map(({ to, icon: Icon, label }) => (
                <NavLink key={to} to={to} end={to === '/'}
                  className={({ isActive }) =>
                    `nav-item ${isActive ? 'nav-item-active' : 'nav-item-idle'}`}>
                  <Icon className="h-4 w-4 flex-shrink-0" />
                  <span className="truncate">{label}</span>
                </NavLink>
              ))}
            </div>
          ))}
        </nav>

        {/* Backend status */}
        <div className="px-5 py-4 border-t border-gray-50">
          <div className="flex items-center gap-2">
            <div className={`h-1.5 w-1.5 rounded-full flex-shrink-0 ${
              backendStatus === 'ok'     ? 'bg-green-400' :
              backendStatus === 'error'  ? 'bg-red-400' :
              'bg-amber-400 animate-pulse'
            }`} />
            <span className={`text-[10px] font-medium ${
              backendStatus === 'ok'     ? 'text-green-600' :
              backendStatus === 'error'  ? 'text-red-500' :
              'text-amber-500'
            }`}>
              {backendStatus === 'ok'    ? 'Connesso' :
               backendStatus === 'error' ? 'Non raggiungibile' :
               'Connessione in corso…'}
            </span>
          </div>
          <p className="text-[10px] text-gray-300 mt-0.5 font-medium">v8 · Uso interno</p>
        </div>
      </aside>

      {/* ── Main ── */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top bar */}
        <header className="h-14 bg-white border-b border-gray-100 px-6 flex items-center justify-between flex-shrink-0 shadow-sm">
          <div className="flex items-center gap-2 text-sm text-gray-400">
            <span className="font-medium text-gray-900">{currentNav?.label || 'Dashboard'}</span>
          </div>
          {backendStatus === 'waking' && (
            <div className="flex items-center gap-2 text-xs text-amber-600 bg-amber-50 border border-amber-100 px-3 py-1.5 rounded-lg">
              <div className="h-3 w-3 border-2 border-amber-400 border-t-transparent rounded-full animate-spin flex-shrink-0" />
              Backend in avvio (free tier — ~30s)
            </div>
          )}
        </header>

        {/* Page content */}
        <main className="flex-1 overflow-y-auto">
          <div className="p-6 max-w-screen-2xl mx-auto">
            {children}
          </div>
        </main>
      </div>
    </div>
  )
}
