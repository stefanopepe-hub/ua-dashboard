import { useEffect, useState, useCallback } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import {
  LayoutDashboard, TrendingUp, Clock, AlertTriangle,
  Building2, FileText, Upload, Database, Users,
  Activity, ChevronRight,
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
      { to: '/risorse',   icon: Users,       label: 'Risorse' },
      { to: '/tempi',     icon: Clock,       label: 'Tempi Attraversamento' },
      { to: '/nc',        icon: AlertTriangle, label: 'Non Conformità' },
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

// Stati possibili: 'starting' | 'ok' | 'degraded' | 'unreachable'
const STATUS_CONFIG = {
  starting:    { dot: 'bg-amber-400 animate-pulse', text: 'text-amber-500', label: 'Avvio in corso…' },
  ok:          { dot: 'bg-green-400',               text: 'text-green-600', label: 'Connesso' },
  degraded:    { dot: 'bg-amber-400',               text: 'text-amber-600', label: 'Parzialmente degradato' },
  unreachable: { dot: 'bg-red-400',                 text: 'text-red-500',  label: 'Verifica connessione…' },
}

export default function Layout({ children }) {
  const [status, setStatus] = useState('starting')
  const [retries, setRetries] = useState(0)
  const location = useLocation()

  const checkHealth = useCallback(async () => {
    const BASE = import.meta.env.VITE_API_URL || ''
    try {
      // Timeout generoso per Render free tier (cold start ~30s)
      const ctrl = new AbortController()
      const timer = setTimeout(() => ctrl.abort(), 60000)
      const r = await fetch(`${BASE}/health`, { signal: ctrl.signal })
      clearTimeout(timer)
      if (!r.ok) {
        setStatus('degraded')
        return
      }
      const data = await r.json()
      setStatus(data.database === 'reachable' ? 'ok' : 'degraded')
    } catch (e) {
      // AbortError = timeout, non unreachable
      if (e.name === 'AbortError') {
        setStatus('starting')
      } else {
        setStatus('unreachable')
      }
      // Riprova dopo 30s, max 5 volte
      if (retries < 5) {
        setTimeout(() => {
          setRetries(r => r + 1)
          checkHealth()
        }, 30000)
      }
    }
  }, [retries])

  useEffect(() => {
    checkHealth()
  }, [])

  const sc = STATUS_CONFIG[status]
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

        {/* Health status */}
        <div className="px-5 py-4 border-t border-gray-50">
          <button
            onClick={checkHealth}
            className="flex items-center gap-2 w-full hover:opacity-70 transition-opacity"
            title="Clicca per aggiornare lo stato"
          >
            <div className={`h-1.5 w-1.5 rounded-full flex-shrink-0 ${sc.dot}`} />
            <span className={`text-[10px] font-medium ${sc.text}`}>{sc.label}</span>
          </button>
          {status === 'starting' && (
            <p className="text-[9px] text-gray-300 mt-0.5">
              Render free tier: attendi fino a 60s al primo avvio
            </p>
          )}
          <p className="text-[10px] text-gray-300 mt-0.5 font-medium">v9 · Uso interno</p>
        </div>
      </aside>

      {/* ── Main ── */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top bar */}
        <header className="h-14 bg-white border-b border-gray-100 px-6 flex items-center justify-between flex-shrink-0 shadow-sm">
          <div className="flex items-center gap-2 text-sm">
            <span className="font-semibold text-gray-900">{currentNav?.label || 'Dashboard'}</span>
          </div>
          {status === 'starting' && (
            <div className="flex items-center gap-2 text-xs text-amber-600 bg-amber-50 border border-amber-100 px-3 py-1.5 rounded-lg">
              <div className="h-3 w-3 border-2 border-amber-400 border-t-transparent rounded-full animate-spin flex-shrink-0" />
              Backend in avvio — le analisi saranno disponibili a breve
            </div>
          )}
        </header>

        {/* Content */}
        <main className="flex-1 overflow-y-auto">
          <div className="p-6 max-w-screen-2xl mx-auto">
            {children}
          </div>
        </main>
      </div>
    </div>
  )
}
