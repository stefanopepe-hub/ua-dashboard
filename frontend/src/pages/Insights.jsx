/**
 * Insights.jsx — Auto-Insights Engine
 * Fondazione Telethon ETS — UA Dashboard
 *
 * Mostra insight testuali generati automaticamente dall'analisi dei dati:
 * saving rate, concentrazione fornitori, trend YoY, efficienza negoziale, etc.
 */
import { useState, useEffect, useCallback } from 'react'
import {
  TrendingUp, TrendingDown, AlertTriangle, CheckCircle2,
  Info, RefreshCw, Zap, BarChart3, ShoppingCart,
  Users, Activity, Target, Lightbulb,
} from 'lucide-react'
import { api } from '../utils/api'
import { useAnni } from '../hooks/useAnni'

// ── Helpers ──────────────────────────────────────────────────────

const CATEGORY_META = {
  saving:      { label: 'Saving',          icon: Target,    color: '#0057A8' },
  fornitori:   { label: 'Fornitori',       icon: ShoppingCart, color: '#7c3aed' },
  efficienza:  { label: 'Efficienza',      icon: Activity,  color: '#0891b2' },
  budget:      { label: 'Budget & Spesa',  icon: BarChart3, color: '#059669' },
  trend:       { label: 'Trend YoY',       icon: TrendingUp, color: '#d97706' },
}

const TYPE_META = {
  positive: {
    bg: 'bg-green-50', border: 'border-green-200',
    badge: 'bg-green-100 text-green-700',
    icon: CheckCircle2, iconColor: 'text-green-500',
    label: 'Positivo',
  },
  warning: {
    bg: 'bg-amber-50', border: 'border-amber-200',
    badge: 'bg-amber-100 text-amber-700',
    icon: AlertTriangle, iconColor: 'text-amber-500',
    label: 'Attenzione',
  },
  alert: {
    bg: 'bg-red-50', border: 'border-red-200',
    badge: 'bg-red-100 text-red-700',
    icon: AlertTriangle, iconColor: 'text-red-500',
    label: 'Critico',
  },
  info: {
    bg: 'bg-blue-50', border: 'border-blue-200',
    badge: 'bg-blue-100 text-blue-700',
    icon: Info, iconColor: 'text-blue-500',
    label: 'Info',
  },
}

function InsightCard({ insight }) {
  const tm = TYPE_META[insight.type] || TYPE_META.info
  const TypeIcon = tm.icon
  const cm = CATEGORY_META[insight.category] || CATEGORY_META.budget
  const CatIcon = cm.icon

  return (
    <div className={`rounded-2xl border p-5 ${tm.bg} ${tm.border} transition-all hover:shadow-md`}>
      <div className="flex items-start gap-3">
        <div className={`flex-shrink-0 mt-0.5 ${tm.iconColor}`}>
          <TypeIcon className="h-5 w-5" />
        </div>
        <div className="flex-1 min-w-0">
          {/* Header */}
          <div className="flex items-center gap-2 flex-wrap mb-1">
            <span className={`text-[10px] font-bold uppercase tracking-wider px-2 py-0.5 rounded-full ${tm.badge}`}>
              {tm.label}
            </span>
            <span className="flex items-center gap-1 text-[10px] text-gray-500 font-medium">
              <CatIcon className="h-3 w-3" style={{ color: cm.color }} />
              {cm.label}
            </span>
          </div>

          {/* Titolo */}
          <h3 className="text-sm font-semibold text-gray-900 leading-snug mb-1.5">
            {insight.title}
          </h3>

          {/* Body */}
          <p className="text-xs text-gray-600 leading-relaxed">
            {insight.body}
          </p>

          {/* Metriche */}
          {(insight.metric || insight.delta) && (
            <div className="flex items-center gap-3 mt-3">
              {insight.metric && (
                <span className="text-base font-bold text-gray-900">
                  {insight.metric}
                </span>
              )}
              {insight.delta && (
                <span className={`text-sm font-semibold flex items-center gap-0.5 ${
                  insight.delta.startsWith('+') ? 'text-green-600' :
                  insight.delta.startsWith('-') ? 'text-red-500' : 'text-gray-500'
                }`}>
                  {insight.delta.startsWith('+')
                    ? <TrendingUp className="h-3.5 w-3.5" />
                    : insight.delta.startsWith('-')
                      ? <TrendingDown className="h-3.5 w-3.5" />
                      : null}
                  {insight.delta} vs anno prec.
                </span>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function InsightSkeleton() {
  return (
    <div className="rounded-2xl border border-gray-100 bg-gray-50 p-5 animate-pulse">
      <div className="flex items-start gap-3">
        <div className="w-5 h-5 bg-gray-200 rounded-full flex-shrink-0 mt-0.5" />
        <div className="flex-1 space-y-2">
          <div className="flex gap-2">
            <div className="h-4 w-16 bg-gray-200 rounded-full" />
            <div className="h-4 w-12 bg-gray-200 rounded-full" />
          </div>
          <div className="h-4 w-3/4 bg-gray-200 rounded" />
          <div className="h-3 w-full bg-gray-200 rounded" />
          <div className="h-3 w-5/6 bg-gray-200 rounded" />
        </div>
      </div>
    </div>
  )
}

function SummaryBar({ insights }) {
  const counts = { positive: 0, warning: 0, alert: 0, info: 0 }
  insights.forEach(i => { counts[i.type] = (counts[i.type] || 0) + 1 })
  return (
    <div className="flex flex-wrap gap-3">
      {counts.positive > 0 && (
        <div className="flex items-center gap-1.5 text-sm font-medium text-green-700 bg-green-50 border border-green-200 px-3 py-1.5 rounded-full">
          <CheckCircle2 className="h-4 w-4" />
          {counts.positive} positivi
        </div>
      )}
      {counts.warning > 0 && (
        <div className="flex items-center gap-1.5 text-sm font-medium text-amber-700 bg-amber-50 border border-amber-200 px-3 py-1.5 rounded-full">
          <AlertTriangle className="h-4 w-4" />
          {counts.warning} da monitorare
        </div>
      )}
      {counts.alert > 0 && (
        <div className="flex items-center gap-1.5 text-sm font-medium text-red-700 bg-red-50 border border-red-200 px-3 py-1.5 rounded-full">
          <AlertTriangle className="h-4 w-4" />
          {counts.alert} critici
        </div>
      )}
      {counts.info > 0 && (
        <div className="flex items-center gap-1.5 text-sm font-medium text-blue-700 bg-blue-50 border border-blue-200 px-3 py-1.5 rounded-full">
          <Info className="h-4 w-4" />
          {counts.info} informativi
        </div>
      )}
    </div>
  )
}

export default function Insights() {
  const { anni } = useAnni()
  const [anno, setAnno]       = useState('')
  const [strRic, setStrRic]   = useState('')
  const [filter, setFilter]   = useState('all')
  const [insights, setInsights] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState(null)
  const [lastRefresh, setLastRefresh] = useState(null)

  const load = useCallback(async () => {
    setLoading(true); setError(null)
    try {
      const data = await api.insights({ anno: anno || undefined, str_ric: strRic || undefined })
      setInsights(Array.isArray(data) ? data : [])
      setLastRefresh(new Date())
    } catch (e) {
      setError(e.message || 'Errore nel caricamento degli insight')
      setInsights([])
    } finally {
      setLoading(false)
    }
  }, [anno, strRic])

  // Carica alla prima render e quando cambiano i filtri
  useEffect(() => {
    if (anni.length > 0 && !anno) {
      setAnno(String(Math.max(...anni)))
    }
  }, [anni, anno])

  useEffect(() => {
    load()
  }, [load])

  // Filtra per categoria
  const categories = ['all', ...Object.keys(CATEGORY_META)]
  const filtered = filter === 'all'
    ? insights
    : insights.filter(i => i.category === filter)

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <Zap className="h-5 w-5 text-telethon-blue" />
            <h1 className="text-2xl font-bold text-gray-900 tracking-tight">
              Auto-Insights
            </h1>
          </div>
          <p className="text-sm text-gray-400">
            Analisi automatica dei dati — insight generati in tempo reale dai KPI procurement
          </p>
          {lastRefresh && (
            <p className="text-xs text-gray-300 mt-0.5">
              Aggiornato: {lastRefresh.toLocaleTimeString('it-IT')}
            </p>
          )}
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="btn-ghost text-xs flex items-center gap-1.5 h-8"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${loading ? 'animate-spin' : ''}`} />
          Aggiorna
        </button>
      </div>

      {/* Filtri */}
      <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-4">
        <div className="flex flex-wrap gap-3 items-center">
          {/* Anno */}
          <select
            value={anno}
            onChange={e => setAnno(e.target.value)}
            className="h-8 text-sm font-semibold text-telethon-blue border-telethon-blue rounded-xl px-3 pr-7 bg-blue-50"
          >
            {anni.map(a => (
              <option key={a} value={String(a)}>{a}</option>
            ))}
          </select>

          {/* Area */}
          <select
            value={strRic}
            onChange={e => setStrRic(e.target.value)}
            className="h-8 text-sm rounded-xl px-3 pr-7 border border-gray-200"
          >
            <option value="">Ricerca + Struttura</option>
            <option value="RICERCA">Solo Ricerca</option>
            <option value="STRUTTURA">Solo Struttura</option>
          </select>

          <div className="w-px h-5 bg-gray-200 mx-1 hidden sm:block" />

          {/* Filtro categoria */}
          <div className="flex flex-wrap gap-1.5">
            {categories.map(cat => {
              const meta = CATEGORY_META[cat]
              const CatIcon = meta?.icon || Lightbulb
              return (
                <button
                  key={cat}
                  onClick={() => setFilter(cat)}
                  className={`h-7 px-3 text-xs font-medium rounded-full border transition-all flex items-center gap-1 ${
                    filter === cat
                      ? 'bg-telethon-blue text-white border-telethon-blue'
                      : 'bg-white text-gray-500 border-gray-200 hover:border-telethon-blue hover:text-telethon-blue'
                  }`}
                >
                  {cat === 'all' ? (
                    <>
                      <Lightbulb className="h-3 w-3" /> Tutti
                    </>
                  ) : (
                    <>
                      <CatIcon className="h-3 w-3" />
                      {meta.label}
                    </>
                  )}
                </button>
              )
            })}
          </div>
        </div>
      </div>

      {/* Summary bar */}
      {!loading && insights.length > 0 && (
        <SummaryBar insights={insights} />
      )}

      {/* Insights grid */}
      {loading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <InsightSkeleton key={i} />
          ))}
        </div>
      ) : error ? (
        <div className="bg-red-50 border border-red-200 rounded-2xl p-6 text-center">
          <AlertTriangle className="h-8 w-8 text-red-400 mx-auto mb-2" />
          <p className="text-sm font-medium text-red-700">{error}</p>
          <button onClick={load} className="mt-3 text-xs text-red-600 underline">
            Riprova
          </button>
        </div>
      ) : filtered.length === 0 ? (
        <div className="bg-gray-50 border border-gray-200 rounded-2xl p-10 text-center">
          <Lightbulb className="h-10 w-10 text-gray-300 mx-auto mb-3" />
          <p className="text-sm font-medium text-gray-500">
            {insights.length === 0
              ? 'Nessun dato disponibile. Carica dei file Excel per generare gli insight.'
              : 'Nessun insight per questa categoria.'}
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {filtered.map((insight, i) => (
            <InsightCard key={i} insight={insight} />
          ))}
        </div>
      )}

      {/* Footer note */}
      {!loading && insights.length > 0 && (
        <p className="text-xs text-gray-300 text-right">
          {filtered.length} insight visualizzati
          {filter !== 'all' ? ` (filtro: ${CATEGORY_META[filter]?.label})` : ''}
          {' · '}
          Gli insight sono calcolati in tempo reale sui dati importati.
        </p>
      )}
    </div>
  )
}
