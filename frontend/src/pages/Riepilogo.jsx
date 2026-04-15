/**
 * Riepilogo.jsx — Executive Dashboard v3.0
 * Fondazione Telethon ETS — UA Acquisti
 */
import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import {
  ComposedChart, Bar, Line, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend, Cell,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, COLORS, CDC_COLORS } from '../utils/fmt'
import {
  KpiCardGradient, StatCard, FilterBar, GranSelect, DeltaBadge,
  LoadingBox, ErrorBox, SectionTitle, SectionHeader, ProgressBar, Badge,
} from '../components/UI'
import {
  TrendingUp, TrendingDown, Minus, Download, Printer,
  BarChart2, Users, Clock, AlertCircle, ArrowRight,
  DollarSign, ShoppingCart, Target, Activity,
} from 'lucide-react'

// ── Helpers ─────────────────────────────────────────────────────────────────

const AVATAR_COLORS = [
  '#0057A8', '#D81E1E', '#15803d', '#ea580c', '#7c3aed', '#0891b2',
]

function initials(name) {
  if (!name) return '?'
  const parts = name.trim().split(/\s+/)
  if (parts.length === 1) return name.slice(0, 2).toUpperCase()
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase()
}

function savingTier(perc) {
  if (perc >= 15) return { label: 'Top', cls: 'chip-green' }
  if (perc >= 8)  return { label: 'OK',  cls: 'chip-blue' }
  return               { label: '—',   cls: 'chip-gray' }
}

// ── Custom Tooltip ───────────────────────────────────────────────────────────

function YoYTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-white border border-gray-100 rounded-xl shadow-lg p-3 text-xs min-w-[190px]">
      <div className="font-bold text-gray-700 mb-2 pb-1.5 border-b border-gray-100">{label}</div>
      {payload.map((p, i) => (
        <div key={i} className="flex items-center justify-between gap-4 py-0.5">
          <span className="flex items-center gap-1.5 text-gray-500">
            <span className="w-2 h-2 rounded-full inline-block flex-shrink-0"
              style={{ background: p.color || p.stroke }} />
            {p.name}
          </span>
          <span className="font-bold tabular-nums text-gray-800">
            {p.unit === '%'
              ? `${Number(p.value).toFixed(1)}%`
              : `€${p.value}K`}
          </span>
        </div>
      ))}
    </div>
  )
}

function CdcTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  return (
    <div className="bg-white border border-gray-100 rounded-xl shadow-lg p-3 text-xs min-w-[170px]">
      <div className="font-bold text-gray-700 mb-2">{label}</div>
      <div className="space-y-0.5">
        <div className="flex justify-between gap-4">
          <span className="text-gray-400">Listino</span>
          <span className="font-bold tabular-nums">€{d.listino}K</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-gray-400">Impegnato</span>
          <span className="font-bold tabular-nums text-telethon-blue">€{d.impegnato}K</span>
        </div>
        <div className="flex justify-between gap-4 pt-1 border-t border-gray-100">
          <span className="text-gray-400">Saving</span>
          <span className="font-bold tabular-nums text-green-600">
            €{d.saving}K &nbsp;({d.perc_saving?.toFixed(1)}%)
          </span>
        </div>
      </div>
    </div>
  )
}

// ── Buyer Avatar Card ────────────────────────────────────────────────────────

function BuyerRow({ buyer, rank, idx }) {
  const tier = savingTier(buyer.perc_saving || 0)
  const color = AVATAR_COLORS[idx % AVATAR_COLORS.length]
  const shortName = (buyer.utente || '').split(' ').slice(-2).join(' ')
  return (
    <div className="flex items-center gap-3 py-2.5 border-b border-gray-50 last:border-0
      hover:bg-gray-50/60 rounded-lg px-2 -mx-2 transition-colors">
      {/* Rank */}
      <span className={`text-xs font-bold w-5 text-center flex-shrink-0
        ${rank === 1 ? 'text-amber-500' : rank === 2 ? 'text-gray-400' : rank === 3 ? 'text-orange-400' : 'text-gray-300'}`}>
        {rank <= 3 ? ['🥇','🥈','🥉'][rank - 1] : rank}
      </span>
      {/* Avatar */}
      <div className="w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0
        text-white text-xs font-bold"
        style={{ background: color }}>
        {initials(buyer.utente)}
      </div>
      {/* Name + stats */}
      <div className="flex-1 min-w-0">
        <div className="text-xs font-semibold text-gray-800 truncate">{shortName}</div>
        <div className="text-xs text-gray-400">{fmtNum(buyer.n_ord || buyer.n_righe)} ordini</div>
      </div>
      {/* Saving */}
      <div className="text-right flex-shrink-0">
        <div className="text-sm font-bold text-green-700 tabular-nums">{fmtEur(buyer.saving)}</div>
        <span className={`chip ${tier.cls} text-xs`}>{fmtPct(buyer.perc_saving)}</span>
      </div>
    </div>
  )
}

// ── Main Component ───────────────────────────────────────────────────────────

export default function Riepilogo() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]   = useState('')
  const [strRic, setStrRic] = useState('')
  const [gran, setGran]   = useState('mensile')

  useEffect(() => { if (!anno && defaultAnno) setAnno(String(defaultAnno)) }, [defaultAnno])

  const annoInt = parseInt(anno) || new Date().getFullYear()
  const ap      = annoInt - 1
  const ready   = !!anno

  // ── Data fetching ──
  const { data: yoy,     loading: lYoy,  error: eYoy } = useKpi(
    () => ready ? api.yoy({ anno: annoInt, granularita: gran, str_ric: strRic }) : Promise.resolve(null),
    [anno, gran, strRic]
  )
  const { data: cdcData, loading: lCdc } = useKpi(
    () => ready ? api.perCdc({ anno, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )
  const { data: areaData } = useKpi(
    () => ready ? api.mensileArea({ anno, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )
  const { data: tempiR } = useKpi(() => api.tempiRiepilogo(), [])
  const { data: ncR }    = useKpi(() => api.ncRiepilogo(), [])
  const { data: buyers } = useKpi(
    () => ready ? api.perBuyer({ anno, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )
  const { data: buckets } = useKpi(
    () => ready ? api.perMacro({ anno, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )

  // ── KPI headline ──
  const hl    = yoy?.kpi_headline || {}
  const kc    = hl.corrente   || {}
  const kp    = hl.precedente || {}
  const delta = hl.delta      || {}
  const chart = yoy?.chart_data || []

  // ── Chart data ──
  const chartSav = chart.map(d => ({
    name:     d.label,
    parziale: d.parziale,
    curr:     Math.round((d[`saving_${annoInt}`]     || 0) / 1000),
    prev:     Math.round((d[`saving_${ap}`]          || 0) / 1000),
    rate:     parseFloat((d[`perc_saving_${annoInt}`] || 0).toFixed(1)),
  }))

  const chartImp = chart.map(d => ({
    name:     d.label,
    parziale: d.parziale,
    curr:     Math.round((d[`impegnato_${annoInt}`] || 0) / 1000),
    prev:     Math.round((d[`impegnato_${ap}`]      || 0) / 1000),
  }))

  const cdcBar = (cdcData || []).filter(d => d.cdc).map(d => ({
    name:        d.cdc,
    listino:     Math.round((d.listino   || 0) / 1000),
    impegnato:   Math.round((d.impegnato || 0) / 1000),
    saving:      Math.round((d.saving    || 0) / 1000),
    perc_saving: d.perc_saving || 0,
    fill:        CDC_COLORS[d.cdc] || COLORS.gray,
  }))

  const areaChart = (areaData || []).map(d => ({
    name:      d.label || d.mese || '',
    Ricerca:   Math.round((d.ric_saving || 0) / 1000),
    Struttura: Math.round((d.str_saving || 0) / 1000),
  }))

  const buyerList  = (buyers  || []).slice(0, 6)
  const bucketList = (buckets || []).slice(0, 8)
  const maxBucket  = bucketList[0]?.impegnato || 1

  const hasData = kc.listino != null
  const hasCdc  = cdcBar.length > 0
  const hasArea = areaChart.some(d => d.Ricerca > 0 || d.Struttura > 0)
  const hasSav  = chartSav.some(d => d.curr > 0 || d.prev > 0)

  // ── Render ──
  return (
    <div className="space-y-7">

      {/* ── Page Header ── */}
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-extrabold text-gray-900 tracking-tight">
            Dashboard Ufficio Acquisti
          </h1>
          <p className="text-sm text-gray-400 mt-0.5">
            Report KPI — Fondazione Telethon ETS &nbsp;·&nbsp; Anno {anno || '—'}
          </p>
        </div>
        <div className="flex gap-2">
          <button onClick={() => window.print()} className="btn-ghost text-xs">
            <Printer className="h-3.5 w-3.5"/> Stampa
          </button>
          <button
            onClick={() => api.exportExcel({
              filtri: { anno, str_ric: strRic },
              sezioni: ['riepilogo','mensile','cdc','top_fornitori','alfa_documento'],
            })}
            className="btn-outline text-xs"
          >
            <Download className="h-3.5 w-3.5"/> Excel
          </button>
        </div>
      </div>

      {/* ── Filtri ── */}
      <div className="flex flex-wrap gap-3 items-center">
        <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic} anni={anni} />
        <GranSelect value={gran} onChange={setGran} />
      </div>

      {/* ── Alert / Nota ── */}
      {eYoy && <ErrorBox message={eYoy} />}
      {yoy?.nota && (
        <div className="bg-blue-50 border border-blue-100 rounded-xl px-4 py-3 text-xs text-blue-800">
          ℹ️ {yoy.nota}
        </div>
      )}

      {/* ── Stato iniziale ── */}
      {!ready && (
        <div className="card-premium text-center py-12 text-gray-400">
          <BarChart2 className="h-10 w-10 mx-auto mb-3 text-gray-200" />
          <div className="font-semibold text-gray-500">Seleziona un anno</div>
          <div className="text-xs mt-1">Scegli l'anno dal filtro in alto per visualizzare i dati.</div>
        </div>
      )}

      {/* ── KPI Hero Section ── */}
      {ready && (lYoy ? (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="skeleton rounded-2xl h-28" />
          ))}
        </div>
      ) : hasData && (
        <>
          {/* Gradient hero cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <KpiCardGradient
              label="Listino Totale"
              value={fmtEur(kc.listino)}
              sub={`Precedente: ${fmtEur(kp.listino)}`}
              delta={delta.listino}
              icon={DollarSign}
              gradient="from-slate-600 to-slate-800"
            />
            <KpiCardGradient
              label="Impegnato"
              value={fmtEur(kc.impegnato)}
              sub={`Precedente: ${fmtEur(kp.impegnato)}`}
              delta={delta.impegnato}
              icon={ShoppingCart}
              gradient="from-blue-600 to-blue-800"
            />
            <KpiCardGradient
              label="Saving Generato"
              value={fmtEur(kc.saving)}
              sub={`${fmtPct(kc.perc_saving)} sul listino`}
              delta={delta.saving}
              icon={Target}
              gradient="from-green-600 to-green-800"
            />
            <KpiCardGradient
              label="% Saving"
              value={fmtPct(kc.perc_saving)}
              sub={`+${(delta.perc_saving || 0).toFixed(1)} pp vs ${ap}`}
              delta={delta.perc_saving}
              icon={Activity}
              gradient="from-violet-600 to-violet-800"
            />
          </div>

          {/* Secondary stat cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              title="Documenti"
              value={fmtNum(kc.n_righe)}
              change={null}
              changeLabel="totale dataset"
              icon={BarChart2}
              color="blue"
            />
            <StatCard
              title="Fornitori Albo"
              value={fmtPct(kc.perc_albo)}
              change={null}
              changeLabel={`${fmtNum(kc.n_albo)} accreditati`}
              icon={Users}
              color="green"
            />
            <StatCard
              title="Tempo Medio Ordine"
              value={tempiR?.avg_total_days > 0 ? `${Number(tempiR.avg_total_days).toFixed(1)} gg` : '—'}
              change={null}
              changeLabel="dalla creazione"
              icon={Clock}
              color="orange"
            />
            <StatCard
              title="Non Conformità"
              value={ncR?.n_nc > 0 ? fmtPct(ncR.perc_nc) : '—'}
              change={null}
              changeLabel={ncR?.n_nc > 0 ? `${fmtNum(ncR.n_nc)} NC` : 'nessuna NC'}
              icon={AlertCircle}
              color={ncR?.n_nc > 0 ? 'red' : 'gray'}
            />
          </div>
        </>
      ))}

      {/* ── YoY Charts ── */}
      {ready && !lYoy && hasSav && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">

          {/* Saving YoY + rate line */}
          <div className="card-premium">
            <SectionTitle sub={`Barre = €K · Linea = % saving ${anno}`}>
              Saving {anno} vs {ap}
            </SectionTitle>
            <ResponsiveContainer width="100%" height={230}>
              <ComposedChart data={chartSav} margin={{ top: 4, right: 24, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" vertical={false} />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                <YAxis yAxisId="left" tick={{ fontSize: 10 }} tickFormatter={v => `€${v}K`}
                  axisLine={false} tickLine={false} />
                <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 10 }}
                  tickFormatter={v => `${v}%`} axisLine={false} tickLine={false} domain={[0, 'auto']} />
                <Tooltip content={<YoYTooltip />} />
                <Legend wrapperStyle={{ fontSize: 11 }} iconType="circle" iconSize={8} />
                <Bar yAxisId="left" dataKey="curr" name={anno} radius={[4,4,0,0]}>
                  {chartSav.map((d, i) => (
                    <Cell key={i}
                      fill={d.parziale ? '#86efac' : COLORS.green}
                      opacity={d.parziale ? 0.65 : 1}
                    />
                  ))}
                </Bar>
                <Bar yAxisId="left" dataKey="prev" name={String(ap)}
                  fill="#d1fae5" radius={[4,4,0,0]} />
                <Line yAxisId="right" dataKey="rate" name="% saving"
                  unit="%" type="monotone"
                  stroke="#f59e0b" strokeWidth={2.5} dot={false}
                  activeDot={{ r: 4, fill: '#f59e0b' }} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>

          {/* Impegnato YoY */}
          <div className="card-premium">
            <SectionTitle sub="Spesa effettiva negoziata (€K)">
              Impegnato {anno} vs {ap}
            </SectionTitle>
            <ResponsiveContainer width="100%" height={230}>
              <ComposedChart data={chartImp} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" vertical={false} />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                <YAxis yAxisId="left" tick={{ fontSize: 10 }} tickFormatter={v => `€${v}K`}
                  axisLine={false} tickLine={false} />
                <Tooltip content={<YoYTooltip />} />
                <Legend wrapperStyle={{ fontSize: 11 }} iconType="circle" iconSize={8} />
                <Bar yAxisId="left" dataKey="curr" name={anno} radius={[4,4,0,0]}>
                  {chartImp.map((d, i) => (
                    <Cell key={i}
                      fill={d.parziale ? '#93c5fd' : COLORS.blue}
                      opacity={d.parziale ? 0.65 : 1}
                    />
                  ))}
                </Bar>
                <Bar yAxisId="left" dataKey="prev" name={String(ap)}
                  fill="#dbeafe" radius={[4,4,0,0]} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* ── CDC + Area Trend ── */}
      {ready && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">

          {/* CDC Horizontal bars */}
          <div className="card-premium">
            <SectionTitle sub={`Listino vs Impegnato per centro di costo — ${anno} (€K)`}>
              Performance per CDC
            </SectionTitle>
            {lCdc ? <LoadingBox /> : !hasCdc ? (
              <p className="text-sm text-gray-400 py-6 text-center">Nessun dato CDC</p>
            ) : (
              <ResponsiveContainer width="100%" height={Math.max(200, hasCdc ? cdcBar.length * 60 + 50 : 200)}>
                <ComposedChart
                  data={cdcBar} layout="vertical"
                  margin={{ top: 4, right: 70, left: 80, bottom: 0 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                  <XAxis type="number" tick={{ fontSize: 10 }} tickFormatter={v => `€${v}K`}
                    axisLine={false} tickLine={false} />
                  <YAxis dataKey="name" type="category" tick={{ fontSize: 11, fontWeight: 600 }}
                    width={78} axisLine={false} tickLine={false} />
                  <Tooltip content={<CdcTooltip />} />
                  <Legend wrapperStyle={{ fontSize: 11 }} iconType="circle" iconSize={8} />
                  <Bar dataKey="listino" name="Listino" fill="#e5e7eb" radius={[0,4,4,0]} barSize={10} />
                  <Bar dataKey="impegnato" name="Impegnato" barSize={10} radius={[0,4,4,0]}>
                    {cdcBar.map((d, i) => <Cell key={i} fill={d.fill} />)}
                  </Bar>
                </ComposedChart>
              </ResponsiveContainer>
            )}
            {/* Saving chips per CDC */}
            {hasCdc && !lCdc && (
              <div className="mt-3 pt-3 border-t border-gray-50 flex flex-wrap gap-2">
                {cdcBar.map(d => (
                  <span key={d.name} className="inline-flex items-center gap-1.5 text-xs
                    bg-gray-50 border border-gray-100 rounded-lg px-2.5 py-1">
                    <span className="w-2 h-2 rounded-full"
                      style={{ background: d.fill }} />
                    <span className="font-semibold text-gray-700">{d.name}</span>
                    <span className="font-bold text-green-600">{fmtPct(d.perc_saving)}</span>
                  </span>
                ))}
              </div>
            )}
          </div>

          {/* Stacked Area: Ricerca vs Struttura */}
          <div className="card-premium">
            <SectionTitle sub={`Saving mensile per area — ${anno} (€K)`}>
              Trend Area Ricerca vs Struttura
            </SectionTitle>
            {!hasArea ? (
              <p className="text-sm text-gray-400 py-6 text-center">Nessun dato area disponibile</p>
            ) : (
              <ResponsiveContainer width="100%" height={260}>
                <AreaChart data={areaChart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                  <defs>
                    <linearGradient id="gradRic" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%"  stopColor={COLORS.blue}  stopOpacity={0.25} />
                      <stop offset="95%" stopColor={COLORS.blue}  stopOpacity={0.02} />
                    </linearGradient>
                    <linearGradient id="gradStr" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%"  stopColor={COLORS.teal}  stopOpacity={0.25} />
                      <stop offset="95%" stopColor={COLORS.teal}  stopOpacity={0.02} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" vertical={false} />
                  <XAxis dataKey="name" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fontSize: 10 }} tickFormatter={v => `€${v}K`}
                    axisLine={false} tickLine={false} />
                  <Tooltip content={<YoYTooltip />} />
                  <Legend wrapperStyle={{ fontSize: 11 }} iconType="circle" iconSize={8} />
                  <Area type="monotone" dataKey="Ricerca"
                    stroke={COLORS.blue} strokeWidth={2.5}
                    fill="url(#gradRic)" dot={false}
                    activeDot={{ r: 4 }} stackId="1" />
                  <Area type="monotone" dataKey="Struttura"
                    stroke={COLORS.teal} strokeWidth={2.5}
                    fill="url(#gradStr)" dot={false}
                    activeDot={{ r: 4 }} stackId="1" />
                </AreaChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
      )}

      {/* ── Buyer + Macro Categoria ── */}
      {ready && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">

          {/* Buyer performance */}
          <div className="card-premium">
            <div className="flex items-center justify-between mb-4">
              <div>
                <h3 className="section-title">Top Buyer — {anno}</h3>
                <p className="text-xs text-gray-400 -mt-2">Saving generato per risorsa</p>
              </div>
              <Link to="/risorse"
                className="inline-flex items-center gap-1 text-xs font-semibold text-telethon-blue
                  hover:underline flex-shrink-0">
                Dettaglio <ArrowRight className="h-3.5 w-3.5" />
              </Link>
            </div>
            {!buyerList.length ? (
              <p className="text-sm text-gray-400 py-6 text-center">Nessun dato buyer</p>
            ) : (
              <div>
                {buyerList.map((b, i) => (
                  <BuyerRow key={i} buyer={b} rank={i + 1} idx={i} />
                ))}
              </div>
            )}
          </div>

          {/* Macro Categoria — Progress bars */}
          <div className="card-premium">
            <SectionTitle sub={`Impegnato per macro categoria — ${anno}`}>
              Spesa per Categoria
            </SectionTitle>
            {!bucketList.length ? (
              <p className="text-sm text-gray-400 py-6 text-center">Nessun dato categoria</p>
            ) : (
              <div className="space-y-3">
                {bucketList.map((b, i) => {
                  const pct = Math.min(100, ((b.impegnato || 0) / maxBucket) * 100)
                  const colorList = ['blue','green','orange','red','purple','teal','blue','green']
                  return (
                    <div key={i}>
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs font-medium text-gray-700 truncate max-w-[180px]"
                          title={b.macro_categoria}>
                          {b.macro_categoria || '—'}
                        </span>
                        <div className="flex items-center gap-2 flex-shrink-0 ml-2">
                          {b.perc_saving > 0 && (
                            <span className="chip chip-green text-xs">
                              {fmtPct(b.perc_saving)}
                            </span>
                          )}
                          <span className="text-xs font-bold tabular-nums text-gray-600">
                            {fmtEur(b.impegnato)}
                          </span>
                        </div>
                      </div>
                      <ProgressBar value={pct} max={100} color={colorList[i % colorList.length]} />
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>
      )}

    </div>
  )
}
