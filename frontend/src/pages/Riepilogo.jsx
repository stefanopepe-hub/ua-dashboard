import { useState, useEffect } from 'react'
import {
  BarChart, Bar, LineChart, Line, ComposedChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend, Cell, PieChart, Pie,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, fmtDays, COLORS, CDC_COLORS } from '../utils/fmt'
import {
  KpiCard, FilterBar, GranSelect, DeltaBadge,
  LoadingBox, ErrorBox, SectionTitle,
} from '../components/UI'
import { TrendingUp, TrendingDown, Minus, Download, Printer } from 'lucide-react'

const BUCKET_COLORS = {
  'Materiali di Consumo': '#3b82f6',
  'Servizi':              '#f59e0b',
  'Strumentazione':       '#10b981',
  'Non Classificato':     '#9ca3af',
}

function DeltaArrow({ value }) {
  if (value == null) return <span className="text-gray-300">—</span>
  if (value > 0) return <span className="text-green-600 flex items-center gap-0.5"><TrendingUp className="h-3 w-3"/>+{Math.abs(value).toFixed(1)}%</span>
  if (value < 0) return <span className="text-red-500 flex items-center gap-0.5"><TrendingDown className="h-3 w-3"/>-{Math.abs(value).toFixed(1)}%</span>
  return <span className="text-gray-400"><Minus className="h-3 w-3 inline"/>0%</span>
}

export default function Riepilogo() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]   = useState('')
  const [strRic, setStrRic] = useState('')
  const [gran, setGran]   = useState('mensile')

  useEffect(() => { if (!anno && defaultAnno) setAnno(String(defaultAnno)) }, [defaultAnno])

  const annoInt = parseInt(anno) || new Date().getFullYear()
  const ap = annoInt - 1
  const ready = !!anno

  const { data: yoy,     loading: lYoy,  error: eYoy } = useKpi(
    () => ready ? api.yoy({ anno: annoInt, granularita: gran, str_ric: strRic }) : Promise.resolve(null),
    [anno, gran, strRic]
  )
  const { data: cdcData, loading: lCdc } = useKpi(
    () => api.perCdc({ anno, str_ric: strRic }), [anno, strRic]
  )
  const { data: areaData } = useKpi(
    () => api.mensileArea({ anno, str_ric: strRic }), [anno, strRic]
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

  const hl    = yoy?.kpi_headline || {}
  const kc    = hl.corrente   || {}
  const kp    = hl.precedente || {}
  const delta = hl.delta      || {}
  const chart = yoy?.chart_data || []

  const mkChart = (keyFn) => chart.map(d => ({
    name: d.label, parziale: d.parziale,
    curr: keyFn(d, annoInt), prev: keyFn(d, ap),
  }))
  const chartSav = mkChart((d, a) => Math.round((d[`saving_${a}`]    || 0) / 1000))
  const chartImp = mkChart((d, a) => Math.round((d[`impegnato_${a}`] || 0) / 1000))

  const cdcBar = (cdcData || []).filter(d => d.cdc).map(d => ({
    name: d.cdc,
    Listino:   Math.round((d.listino   || 0) / 1000),
    Impegnato: Math.round((d.impegnato || 0) / 1000),
    fill: CDC_COLORS[d.cdc] || COLORS.gray,
  }))

  const buyerBar = (buyers || []).slice(0, 6).map(d => ({
    name: (d.utente || '').split(' ').slice(-1)[0] || d.utente,
    'Saving €K': Math.round((d.saving || 0) / 1000),
    fill: COLORS.blue,
  }))

  const bucketPie = (buckets || []).slice(0, 8).map(d => ({
    name:  d.macro_categoria || 'Altro',
    value: Math.round((d.impegnato || 0) / 1000),
  }))

  const areaChart = (areaData || []).map(d => ({
    name: d.label || d.mese || '',
    Ricerca:   Math.round((d[`ric_saving`]  || d.ric_saving   || 0) / 1000),
    Struttura: Math.round((d[`str_saving`]  || d.str_saving   || 0) / 1000),
  }))

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dashboard Ufficio Acquisti</h1>
          <p className="text-sm text-gray-400 mt-0.5">Report KPI — Fondazione Telethon ETS</p>
        </div>
        <div className="flex gap-2">
          <button onClick={() => window.print()} className="btn-ghost text-xs">
            <Printer className="h-3.5 w-3.5"/> Stampa
          </button>
          <button onClick={() => api.exportExcel({ filtri: { anno, str_ric: strRic }, sezioni: ['riepilogo','mensile','cdc','top_fornitori','alfa_documento'] })}
            className="btn-outline text-xs">
            <Download className="h-3.5 w-3.5"/> Excel
          </button>
        </div>
      </div>

      {/* Filtri */}
      <div className="flex flex-wrap gap-3 items-center">
        <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic} anni={anni} />
        <GranSelect value={gran} onChange={setGran} />
      </div>

      {eYoy && (
        <div className="bg-amber-50 border border-amber-100 rounded-xl px-4 py-3 text-xs text-amber-700">
          ⚠️ {eYoy.includes('fetch') ? 'Connessione al server in corso...' : eYoy.slice(0, 150)}
        </div>
      )}
      {yoy?.nota && (
        <div className="bg-blue-50 border border-blue-100 rounded-xl px-4 py-3 text-xs text-blue-800">
          ℹ️ {yoy.nota}
        </div>
      )}

      {/* KPI principali */}
      {!ready ? (
        <div className="text-sm text-gray-400 py-4">Seleziona un anno per visualizzare i dati.</div>
      ) : lYoy ? <LoadingBox /> : kc.listino != null && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="kpi-card border-l-4 border-gray-300">
              <span className="kpi-label text-gray-400">Listino</span>
              <div className="kpi-value text-gray-700">{fmtEur(kc.listino)}</div>
              <div className="text-xs text-gray-400 mt-1">prezzo di partenza</div>
            </div>
            <div className="kpi-card border-l-4 border-telethon-blue">
              <span className="kpi-label text-telethon-blue">Impegnato</span>
              <div className="kpi-value">{fmtEur(kc.impegnato)}</div>
              <div className="text-xs text-gray-400 mt-1">quanto paghiamo</div>
            </div>
            <div className="kpi-card border-l-4 border-green-500">
              <span className="kpi-label text-green-600">Saving</span>
              <div className="kpi-value text-green-700">{fmtEur(kc.saving)}</div>
              <div className="flex items-center gap-2 mt-1">
                <span className="text-sm font-bold text-green-600">{fmtPct(kc.perc_saving)}</span>
                <DeltaBadge value={delta.perc_saving} suffix=" pp" label={`vs ${ap}`} />
              </div>
            </div>
            <div className="kpi-card border-l-4 border-telethon-red">
              <span className="kpi-label text-telethon-red">% Negoziati</span>
              <div className="kpi-value">{fmtPct(kc.perc_negoziati)}</div>
              <div className="text-xs text-gray-400 mt-1">{fmtNum(kc.n_negoziati)} / {fmtNum(kc.n_doc_neg)} negoziabili</div>
            </div>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <KpiCard label="N° Documenti"  value={fmtNum(kc.n_righe)}   sub="totale dataset"/>
            <KpiCard label="Accreditati"   value={fmtPct(kc.perc_albo)} sub={`${fmtNum(kc.n_albo)} su albo`} color="green"/>
            {tempiR?.avg_total_days > 0 && (
              <KpiCard label="Tempo Medio Ordine" value={`${tempiR.avg_total_days} gg`} sub="dalla creazione" color="orange"/>
            )}
            {ncR?.n_nc > 0 && (
              <KpiCard label="% Non Conformità" value={fmtPct(ncR.perc_nc)} sub={`${fmtNum(ncR.n_nc)} NC / ${fmtNum(ncR.n_totale)}`} color="red"/>
            )}
          </div>
        </>
      )}

      {/* YoY Saving + Impegnato */}
      {ready && !lYoy && chartSav.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="card">
            <SectionTitle>Saving — {anno} vs {ap} (€K)</SectionTitle>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={chartSav} barGap={2} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{ fontSize: 11 }}/>
                <YAxis tick={{ fontSize: 11 }} tickFormatter={v => `€${v}K`}/>
                <Tooltip formatter={(v, n) => [`€${v}K`, n]}/>
                <Legend wrapperStyle={{ fontSize: 11 }}/>
                <Bar dataKey="curr" name={anno}    fill={COLORS.green} radius={[3,3,0,0]}/>
                <Bar dataKey="prev" name={String(ap)} fill="#86efac" radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="card">
            <SectionTitle>Impegnato — {anno} vs {ap} (€K)</SectionTitle>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={chartImp} barGap={2} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{ fontSize: 11 }}/>
                <YAxis tick={{ fontSize: 11 }} tickFormatter={v => `€${v}K`}/>
                <Tooltip formatter={(v, n) => [`€${v}K`, n]}/>
                <Legend wrapperStyle={{ fontSize: 11 }}/>
                <Bar dataKey="curr" name={anno}    fill={COLORS.blue} radius={[3,3,0,0]}/>
                <Bar dataKey="prev" name={String(ap)} fill="#93c5fd" radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* CDC + Area */}
      {ready && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="card">
            <SectionTitle>Listino vs Impegnato per CDC — {anno} (€K)</SectionTitle>
            {lCdc ? <LoadingBox/> : cdcBar.length === 0
              ? <p className="text-sm text-gray-400 py-6 text-center">Nessun dato</p>
              : (
              <ResponsiveContainer width="100%" height={Math.max(200, cdcBar.length * 52 + 60)}>
                <BarChart data={cdcBar} layout="vertical" barGap={3} margin={{ top: 4, right: 16, left: 80, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                  <XAxis type="number" tick={{ fontSize: 11 }} tickFormatter={v => `€${v}K`}/>
                  <YAxis dataKey="name" type="category" tick={{ fontSize: 11 }} width={80}/>
                  <Tooltip formatter={(v, n) => [`€${v}K`, n]}/>
                  <Legend wrapperStyle={{ fontSize: 11 }}/>
                  <Bar dataKey="Listino"   fill="#e5e7eb" radius={[0,3,3,0]}/>
                  <Bar dataKey="Impegnato" fill={COLORS.blue} radius={[0,3,3,0]}>
                    {cdcBar.map((e, i) => <Cell key={i} fill={e.fill}/>)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>

          <div className="card">
            <SectionTitle>Saving per Area — {anno} (€K)</SectionTitle>
            {areaChart.length === 0
              ? <p className="text-sm text-gray-400 py-6 text-center">Nessun dato area disponibile</p>
              : (
              <ResponsiveContainer width="100%" height={Math.max(200, cdcBar.length * 52 + 60)}>
                <BarChart data={areaChart} barGap={3} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                  <XAxis dataKey="name" tick={{ fontSize: 11 }}/>
                  <YAxis tick={{ fontSize: 11 }} tickFormatter={v => `€${v}K`}/>
                  <Tooltip formatter={(v, n) => [`€${v}K`, n]}/>
                  <Legend wrapperStyle={{ fontSize: 11 }}/>
                  <Bar dataKey="Ricerca"   fill={COLORS.blue}   radius={[3,3,0,0]}/>
                  <Bar dataKey="Struttura" fill={COLORS.teal}   radius={[3,3,0,0]}/>
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
      )}

      {/* Buyer + Macro Categoria */}
      {ready && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="card">
            <SectionTitle>Saving per Buyer — {anno} (€K)</SectionTitle>
            {buyerBar.length === 0
              ? <p className="text-sm text-gray-400 py-6 text-center">Nessun dato buyer</p>
              : (
              <ResponsiveContainer width="100%" height={Math.max(180, buyerBar.length * 38 + 60)}>
                <BarChart data={buyerBar} layout="vertical" margin={{ top: 4, right: 16, left: 90, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                  <XAxis type="number" tick={{ fontSize: 11 }} tickFormatter={v => `€${v}K`}/>
                  <YAxis dataKey="name" type="category" tick={{ fontSize: 11 }} width={90}/>
                  <Tooltip formatter={v => [`€${v}K`, 'Saving']}/>
                  <Bar dataKey="Saving €K" fill={COLORS.blue} radius={[0,3,3,0]}/>
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>

          <div className="card">
            <SectionTitle>Spesa per Macro Categoria — {anno}</SectionTitle>
            {bucketPie.length === 0
              ? <p className="text-sm text-gray-400 py-6 text-center">Nessun dato macro categoria</p>
              : (
              <div className="flex flex-col gap-2">
                {(buckets || []).slice(0, 8).map((b, i) => (
                  <div key={i} className="flex items-center justify-between text-xs">
                    <span className="text-gray-700 font-medium truncate max-w-[160px]" title={b.macro_categoria}>
                      {b.macro_categoria || '—'}
                    </span>
                    <div className="flex items-center gap-3">
                      <div className="w-32 bg-gray-100 rounded-full h-1.5">
                        <div className="h-1.5 rounded-full bg-telethon-blue"
                          style={{ width: `${Math.min(100, (b.impegnato / (buckets[0]?.impegnato || 1)) * 100)}%` }}/>
                      </div>
                      <span className="tabular-nums text-gray-500 w-16 text-right">{fmtEur(b.impegnato)}</span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
