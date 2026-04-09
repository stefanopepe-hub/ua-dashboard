import { NavLink } from 'react-router-dom'
import {
  LayoutDashboard, TrendingUp, Clock, AlertTriangle,
  Building2, Upload, ChevronRight,
} from 'lucide-react'

const nav = [
  { to: '/', icon: LayoutDashboard, label: 'Riepilogo' },
  { to: '/saving', icon: TrendingUp, label: 'Saving & Ordini' },
  { to: '/tempi', icon: Clock, label: 'Tempi Attraversamento' },
  { to: '/nc', icon: AlertTriangle, label: 'Non Conformità' },
  { to: '/fornitori', icon: Building2, label: 'Fornitori' },
  { to: '/upload', icon: Upload, label: 'Carica Dati' },
]

export default function Layout({ children }) {
  return (
    <div className="flex h-screen bg-gray-50 overflow-hidden">
      {/* Sidebar */}
      <aside className="w-60 flex-shrink-0 bg-white border-r border-gray-100 flex flex-col">
        {/* Logo */}
        <div className="px-5 py-5 border-b border-gray-100">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 bg-telethon-blue rounded-lg flex items-center justify-center">
              <span className="text-white text-xs font-bold">T</span>
            </div>
            <div>
              <div className="text-xs font-bold text-gray-900 leading-tight">Fondazione Telethon</div>
              <div className="text-[10px] text-gray-400">Ufficio Acquisti</div>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav className="flex-1 px-3 py-4 space-y-0.5 overflow-y-auto">
          {nav.map(({ to, icon: Icon, label }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-telethon-lightblue text-telethon-blue'
                    : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'
                }`
              }
            >
              <Icon className="h-4 w-4 flex-shrink-0" />
              {label}
            </NavLink>
          ))}
        </nav>

        {/* Footer */}
        <div className="px-5 py-3 border-t border-gray-100">
          <p className="text-[10px] text-gray-400">Dashboard KPI — Uso interno</p>
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 overflow-y-auto">
        <div className="p-6 max-w-7xl mx-auto">
          {children}
        </div>
      </main>
    </div>
  )
}
