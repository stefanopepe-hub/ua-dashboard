/**
 * Risorse.jsx — Enterprise Resource Analytics v3.0
 * Fondazione Telethon ETS — UA Dashboard
 */
import { useState, useEffect, useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, CHART_PALETTE } from '../utils/fmt'
import { LoadingBox, ErrorBox, SectionTitle, PageHeader, FilterBar } from '../components/UI'
import { Users, TrendingUp, Award, BarChart2, ChevronUp, ChevronDown, Minus } from 'lucide-react'

function initials(name = '') {
  const parts = name.trim().split(/\s+/)
  return ((parts[0]?.[0] || '') + (parts[1]?.[0] || '')).toUpperCase() || '?'
}
function avatarBg(idx) { return CHART_PALETTE[idx % CHART_PALETTE.length] }

function TrendIcon({ value }) {
  if (value == null || isNaN(value)) return <Minus className="h-3.5 w-3.5 text-gray-300" />
  if (value > 0) return <ChevronUp className="h-3.5 w-3.5 text-green-500" />
  if (value < 0) return <ChevronDown className="h-3.5 w-3.5 text-red-400" />
  return <Minus className="h-3.5 w-3.5 text-gray-300" />
}

function TableSkeleton({ rows = 5 }) {
  return (
    <div className="space-y-2 py-2">
      {[...Array(rows)].map((_, i) => (
        <div key={i} className="flex items-center gap-3 px-4 py-3 rounded-xl bg-gray-50 animate-pulse">
          <div className="w-8 h-8 rounded-full bg-gray-200 flex-shrink-0" />
          <div className="flex-1 space-y-1.5">
            <div className="h-3 bg-gray-200 rounded w-32" />
            <div className="h-2 bg-gray-100 rounded w-20" />
          </div>
          <div className="h-3 bg-gray-200 rounded w-16" />
        </div>
      ))}
    </div>
  )
}

function PodiumCard({ rank, risorsa, saving, pratiche, perc_saving, colorIdx }) {
  const bg = avatarBg(colorIdx)
  const medals = ['🥇', '🥈', '🥉']
  const ringCls = rank === 1
    ? 'ring-2 ring-yellow-400 ring-offset-2'
    : rank === 2 ? 'ring-2 ring-gray-300 ring-offset-2'
    : 'ring-2 ring-amber-600/40 ring-offset-2'
  return (
    <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-5 flex flex-col items-center gap-3 hover:shadow-md transition-shadow">
      <div className={`w-14 h-14 rounded-full flex items-center justify-center text-white font-bold text-lg flex-shrink-0 ${ringCls}`}
        style={{ background: bg }}>
        {initials(risorsa)}
      </div>
      <div className="text-center">
        <div className="text-xs font-bold text-gray-400 mb-0.5">{medals[rank - 1]} #{rank} Top Performer</div>
        <div className="font-bold text-gray-900 text-sm leading-tight">{risorsa}</div>
      </div>
      <div className="relative w-16 h-16">
        <svg className="w-16 h-16 -rotate-90" viewBox="0 0 56 56">
          <circle cx="28" cy="28" r="22" fill="none" stroke="#f3f4f6" strokeWidth="5" />
          <circle cx="28" cy="28" r="22" fill="none" stroke={bg} strokeWidth="5"
            strokeDasharray={`${2 * Math.PI * 22}`}
            strokeDashoffset={`${2 * Math.PI * 22 * (1 - Math.min((perc_saving || 0) / 30, 1))}`}
            strokeLinecap="round" />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-xs font-extrabold text-gray-800">{fmtPct(perc_saving)}</span>
        </div>
      </div>
      <div className="w-full grid grid-cols-2 gap-2 text-center border-t border-gray-50 pt-3">
        <div>
          <div className="text-base font-extrabold text-green-600">{fmtEur(saving)}</div>
          <div className="text-xs text-gray-400">Saving</div>
        </div>
        <div>
          <div className="text-base font-extrabold text-gray-900">{fmtNum(pratiche)}</div>
          <div className="text-xs text-gray-400">Pratiche</div>
        </div>
      </div>
    </div>
  )
}

export default function Risorse() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]     = useState('')
  const [strRic, setStrRic] = useState('')

  useEffect(() => { if (!anno && defaultAnno) setAnno(String(defaultAnno)) }, [defaultAnno])

  const ready = !!anno

  const { data: riepilogo, loading: lRiep, error: eRiep } = useKpi(
    () => ready ? api.risorseRiepilogo() : Promise.resolve(null), [anno]
  )
  const { data: perRisorsa, loading: lPer, error: ePer } = useKpi(
    () => ready ? api.risorsePerRisorsa({ anno, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )
  const { data: mensile, loading: lMens } = useKpi(
    () => ready ? api.risorseMensile({ anno, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )

  const sorted = useMemo(() =>
    [...(perRisorsa || [])].sort((a, b) => (b.saving_generato || 0) - (a.saving_generato || 0)),
    [perRisorsa]
  )
  const top3       = sorted.slice(0, 3)
  const totPratiche = sorted.reduce((s, r) => s + (r.pratiche_gestite || 0), 0)
  const totSaving   = sorted.reduce((s, r) => s + (r.saving_generato || 0), 0)
  const nRisorse    = riepilogo?.n_risorse ?? sorted.length
  const mediaPer    = nRisorse > 0 ? Math.round(totPratiche / nRisorse) : 0

  const risorseNames = useMemo(() => {
    const names = new Set()
    ;(mensile || []).forEach(r => { if (r.risorsa) names.add(r.risorsa) })
    return [...names].slice(0, 8)
  }, [mensile])

  const monthlyChart = useMemo(() => {
    const map = {}
    ;(mensile || []).forEach(row => {
      const key = row.mese || row.mese_label || '?'
      if (!map[key]) map[key] = { name: key }
      if (row.risorsa && risorseNames.includes(row.risorsa))
        map[key][row.risorsa] = Math.round((row.saving_generato || 0) / 1000)
    })
    return Object.values(map).sort((a, b) => a.name.localeCompare(b.name))
  }, [mensile, risorseNames])

  const loading = lRiep || lPer
  const error   = eRiep || ePer

  return (
    <div className="space-y-6">
      <PageHeader
        title="Performance Team"
        subtitle={`Analisi risorse, saving e pratiche gestite${ready ? ` — ${anno}` : ''}`}
        badge={
          <span className="badge badge-blue">
            <Users className="h-3 w-3 inline mr-1" />{nRisorse} risorse
          </span>
        }
      />
      <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic} anni={anni} />
      {error && <ErrorBox message={error} />}

      {!ready ? (
        <div className="text-sm text-gray-400 py-4">Seleziona un anno per visualizzare i dati.</div>
      ) : (
        <>
          {/* 4 KPI headline cards */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            {[
              { label: 'Pratiche Gestite', value: fmtNum(totPratiche), Icon: BarChart2, iconCls: 'bg-blue-50 text-telethon-blue', sub: 'totale periodo' },
              { label: 'Saving Generato',  value: fmtEur(totSaving),   Icon: TrendingUp, iconCls: 'bg-green-50 text-green-600', valueCls: 'text-green-700', sub: 'risparmio netto' },
              { label: 'Risorse Attive',   value: fmtNum(nRisorse),    Icon: Users, iconCls: 'bg-purple-50 text-purple-600', sub: 'buyer attivi' },
              { label: 'Media per Risorsa',value: fmtNum(mediaPer),    Icon: Award, iconCls: 'bg-orange-50 text-orange-500', sub: 'pratiche / buyer' },
            ].map(({ label, value, Icon, iconCls, valueCls, sub }) => (
              <div key={label} className="bg-white rounded-2xl shadow-sm border border-gray-100 p-5">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs font-bold text-gray-400 uppercase tracking-wider">{label}</span>
                  <div className={`w-8 h-8 rounded-xl flex items-center justify-center ${iconCls}`}>
                    <Icon className="h-4 w-4" />
                  </div>
                </div>
                {loading
                  ? <div className="skeleton h-8 w-24 rounded mt-1" />
                  : <div className={`text-3xl font-extrabold tracking-tight ${valueCls || 'text-gray-900'}`}>{value}</div>
                }
                <p className="text-xs text-gray-400 mt-1">{sub}</p>
              </div>
            ))}
          </div>

          {/* Top-3 podium */}
          {!loading && top3.length > 0 && (
            <div>
              <SectionTitle sub="I tre buyer con saving più alto nel periodo selezionato">
                Top 3 Performer — {anno}
              </SectionTitle>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                {top3.map((r, i) => (
                  <PodiumCard key={r.risorsa || i} rank={i + 1}
                    risorsa={r.risorsa || r.utente || '—'}
                    saving={r.saving_generato} pratiche={r.pratiche_gestite}
                    perc_saving={r.perc_saving} colorIdx={i} />
                ))}
              </div>
            </div>
          )}

          {/* Team Performance Table */}
          <div className="bg-white rounded-2xl shadow-sm border border-gray-100 overflow-hidden">
            <div className="px-6 py-4 border-b border-gray-100">
              <h3 className="section-title">Team Performance — {anno}</h3>
              <p className="text-xs text-gray-400 mt-0.5">Ordinato per saving generato decrescente</p>
            </div>
            {loading ? (
              <div className="p-4"><TableSkeleton /></div>
            ) : sorted.length === 0 ? (
              <div className="text-center py-12 text-gray-400">
                <div className="text-4xl mb-3">👤</div>
                <div className="font-semibold text-gray-500">Nessuna risorsa trovata</div>
                <div className="text-xs mt-1">Carica i dati saving per visualizzare le risorse.</div>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-100 bg-gray-50/60">
                      <th className="py-3 px-4 text-xs font-bold text-gray-500 uppercase tracking-wider text-left w-8">#</th>
                      <th className="py-3 px-4 text-xs font-bold text-gray-500 uppercase tracking-wider text-left">Risorsa</th>
                      <th className="py-3 px-4 text-xs font-bold text-gray-500 uppercase tracking-wider text-left hidden md:table-cell">Responsabile</th>
                      <th className="py-3 px-4 text-xs font-bold text-gray-500 uppercase tracking-wider text-right hidden md:table-cell">Pratiche</th>
                      <th className="py-3 px-4 text-xs font-bold text-gray-500 uppercase tracking-wider text-right">Saving (€)</th>
                      <th className="py-3 px-4 text-xs font-bold text-gray-500 uppercase tracking-wider text-right hidden lg:table-cell">Neg.</th>
                      <th className="py-3 px-4 text-xs font-bold text-gray-500 uppercase tracking-wider text-right hidden lg:table-cell">% Saving</th>
                      <th className="py-3 px-4 text-xs font-bold text-gray-500 uppercase tracking-wider text-center">Trend</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {sorted.map((r, i) => {
                      const name  = r.risorsa || r.utente || '—'
                      const isTop = i === 0
                      const isBot = i === sorted.length - 1 && sorted.length > 1
                      const rowBg = isTop ? 'bg-green-50/60 hover:bg-green-50'
                        : isBot ? 'bg-orange-50/40 hover:bg-orange-50'
                        : 'hover:bg-gray-50/80'
                      const savAmt = r.saving_generato || 0
                      const savCls = savAmt > 50000
                        ? 'text-green-700 bg-green-50 border border-green-100'
                        : savAmt > 10000
                          ? 'text-blue-700 bg-blue-50 border border-blue-100'
                          : 'text-gray-600 bg-gray-50 border border-gray-100'
                      return (
                        <tr key={name} className={`transition-colors ${rowBg}`}>
                          <td className="py-3 px-4 text-xs font-bold text-gray-400 tabular-nums">{isTop ? '🥇' : i + 1}</td>
                          <td className="py-3 px-4">
                            <div className="flex items-center gap-2.5">
                              <div className="w-8 h-8 rounded-full flex items-center justify-center text-white font-bold text-xs flex-shrink-0"
                                style={{ background: avatarBg(i) }}>
                                {initials(name)}
                              </div>
                              <span className="font-semibold text-gray-900 text-sm">{name}</span>
                              {isTop && <span className="hidden sm:inline text-xs px-1.5 py-0.5 rounded-full bg-green-100 text-green-700 font-semibold">Top</span>}
                            </div>
                          </td>
                          <td className="py-3 px-4 text-xs text-gray-500 hidden md:table-cell">{r.responsabile || '—'}</td>
                          <td className="py-3 px-4 text-right tabular-nums font-semibold text-gray-800 hidden md:table-cell">{fmtNum(r.pratiche_gestite)}</td>
                          <td className="py-3 px-4 text-right">
                            <span className={`inline-block tabular-nums text-xs font-bold px-2.5 py-1 rounded-full ${savCls}`}>{fmtEur(r.saving_generato)}</span>
                          </td>
                          <td className="py-3 px-4 text-right tabular-nums text-gray-600 hidden lg:table-cell">{fmtNum(r.negoziazioni_concluse ?? 0)}</td>
                          <td className="py-3 px-4 text-right tabular-nums hidden lg:table-cell">
                            <span className={`font-semibold ${(r.perc_saving || 0) >= 10 ? 'text-green-600' : 'text-gray-600'}`}>{fmtPct(r.perc_saving)}</span>
                          </td>
                          <td className="py-3 px-4 text-center">
                            <div className="flex items-center justify-center"><TrendIcon value={null} /></div>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Monthly stacked chart */}
          <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-6">
            <SectionTitle sub="Saving mensile in €K per buyer (stacked)">
              Saving Mensile per Risorsa — {anno}
            </SectionTitle>
            {lMens ? <LoadingBox rows={3} /> : monthlyChart.length === 0 ? (
              <div className="text-center py-10 text-gray-400 text-sm">Nessun dato mensile disponibile</div>
            ) : (
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={monthlyChart} barCategoryGap="30%" margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" vertical={false} />
                  <XAxis dataKey="name" tick={{ fontSize: 11, fill: '#9ca3af' }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fontSize: 11, fill: '#9ca3af' }} tickFormatter={v => `€${v}K`} axisLine={false} tickLine={false} />
                  <Tooltip formatter={(v, n) => [`€${v}K`, n]}
                    contentStyle={{ borderRadius: 12, border: 'none', boxShadow: '0 4px 24px rgba(0,0,0,.08)', fontSize: 12 }} />
                  <Legend wrapperStyle={{ fontSize: 11, paddingTop: 12 }} />
                  {risorseNames.map((name, i) => (
                    <Bar key={name} dataKey={name} stackId="a" fill={avatarBg(i)}
                      radius={i === risorseNames.length - 1 ? [3, 3, 0, 0] : [0, 0, 0, 0]} />
                  ))}
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
        </>
      )}
    </div>
  )
}
