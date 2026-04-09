import { useState, useEffect } from 'react'
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis,
  CartesianGrid, Tooltip, ResponsiveContainer, Legend, Cell,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, fmtDays, COLORS, CDC_COLORS } from '../utils/fmt'
import { KpiCard, FilterBar, LoadingBox, ErrorBox, SectionTitle } from '../components/UI'
import { DeltaBadge, YoyComment } from '../components/YoyChart'
import { ExportBar } from '../components/PrintButton'
import { Clock, AlertTriangle, Info, AlertCircle } from 'lucide-react'

const GRANULARITA_OPTIONS = [
  { value: 'mensile',    label: 'Mensile' },
  { value: 'bimestrale', label: 'Bimestrale' },
  { value: 'quarter',    label: 'Trimestrale (Q)' },
  { value: 'semestrale', label: 'Semestrale' },
  { value: 'annuale',    label: 'Annuale' },
]

// Barra parziale con pattern tratteggiato
const CustomBar = (props) => {
  const { x, y, width, height, parziale, fill } = props
  if (!height || height <= 0) return null
  if (parziale) {
    return (
      <g>
        <defs>
          <pattern id="parziale-pattern" patternUnits="userSpaceOnUse" width="6" height="6">
            <path d="M-1,1 l2,-2 M0,6 l6,-6 M5,7 l2,-2" stroke={fill} strokeWidth="1.5" opacity="0.6"/>
          </pattern>
        </defs>
        <rect x={x} y={y} width={width} height={height} fill={`url(#parziale-pattern)`} stroke={fill} strokeWidth={1} rx={2}/>
      </g>
    )
  }
  return <rect x={x} y={y} width={width} height={height} fill={fill} rx={2}/>
}

export default function Riepilogo() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]           = useState('')
  const [strRic, setStrRic]       = useState('')
  const [granularita, setGran]    = useState('mensile')
  const [note, setNote]           = useState('')

  useEffect(() => { if (!anno && defaultAnno) setAnno(defaultAnno) }, [defaultAnno])

  const annoInt = parseInt(anno) || new Date().getFullYear()
  const ap = annoInt - 1

  const { data: yoy, loading: lYoy, error: eYoy } = useKpi(
    () => anno ? api.savingYoyGranulare({ anno: annoInt, granularita, str_ric: strRic }) : Promise.resolve(null),
    [anno, granularita, strRic]
  )
  const { data: cdcData }    = useKpi(() => api.savingPerCdc({ anno, str_ric: strRic }), [anno, strRic])
  const { data: mensileArea }= useKpi(() => api.savingMensileArea({ anno, str_ric: strRic }), [anno, strRic])
  const { data: tempiR }     = useKpi(() => api.tempiRiepilogo(), [])
  const { data: ncR }        = useKpi(() => api.ncRiepilogo(), [])

  const hl    = yoy?.kpi_headline || {}
  const kc    = hl.corrente  || {}
  const kp    = hl.precedente || {}
  const delta = hl.delta     || {}
  const chart = yoy?.chart_data || []

  // Grafici — costruiti dai dati granulari
  const mkChart = (keyFn) =>
    chart.map(d => ({
      name:     d.label,
      parziale: d.parziale,
      curr:     keyFn(d, annoInt),
      prev:     keyFn(d, ap),
    }))

  const chartSaving  = mkChart((d,a) => Math.round((d[`saving_${a}`]||0)/1000))
  const chartImp     = mkChart((d,a) => Math.round((d[`impegnato_${a}`]||0)/1000))
  const chartPct     = mkChart((d,a) => d[`perc_saving_${a}`]||0)

  const cdcChart = (cdcData||[]).filter(d=>d.cdc).map(d=>({
    name: d.cdc,
    'Impegnato €K': Math.round(d.impegnato/1000),
    'Saving €K':    Math.round(d.saving/1000),
    fill: CDC_COLORS[d.cdc]||COLORS.gray,
  }))

  const autoComment = (() => {
    if (!yoy || !kc.saving) return ''
    const d = delta.saving
    const base = `Saving ${hl.label_curr}: ${fmtEur(kc.saving)}`
    if (d != null && kp.saving) {
      const trend = d>=0 ? 'in crescita' : 'in calo'
      return `${base} — ${trend} del ${Math.abs(d).toFixed(1)}% rispetto allo stesso periodo ${ap} (${fmtEur(kp.saving)}).`
    }
    return `${base}. Dati anno precedente non disponibili per il confronto.`
  })()

  // Tooltip custom con nota parziale
  const CustomTooltip = ({ active, payload, label }) => {
    if (!active || !payload?.length) return null
    const d = chart.find(c => c.label === label)
    return (
      <div className="bg-white border border-gray-200 rounded-lg shadow-lg p-3 text-xs">
        <p className="font-semibold text-gray-700 mb-1">{label}{d?.parziale ? ' ⚠️ parziale' : ''}</p>
        {payload.map((p,i) => (
          <p key={i} style={{color:p.color}}>
            {p.name}: €{p.value?.toLocaleString('it-IT')}K
          </p>
        ))}
        {d?.parziale && d?.parziale_label && (
          <p className="text-orange-500 mt-1">{d.parziale_label}</p>
        )}
      </div>
    )
  }

  return (
    <div className="space-y-6 print:space-y-4">
      {/* Header */}
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dashboard Ufficio Acquisti</h1>
          <p className="text-sm text-gray-500 mt-0.5">Report KPI — Fondazione Telethon ETS</p>
        </div>
        <ExportBar anno={anno} strRic={strRic} note={note} />
      </div>

      {/* Filtri */}
      <div className="flex flex-wrap gap-3 mb-2">
        <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic} anni={anni} />
        <select
          className="text-sm border-2 border-telethon-blue rounded-lg px-3 py-1.5 bg-white font-semibold text-telethon-blue focus:outline-none"
          value={granularita}
          onChange={e => setGran(e.target.value)}
        >
          {GRANULARITA_OPTIONS.map(o => (
            <option key={o.value} value={o.value}>{o.value === granularita ? `📊 ${o.label}` : o.label}</option>
          ))}
        </select>
      </div>

      {eYoy && <ErrorBox message={eYoy} />}

      {/* Nota periodo */}
      {yoy?.nota && (
        <div className="flex items-start gap-2 bg-amber-50 border border-amber-200 rounded-lg px-4 py-2.5 text-xs text-amber-800">
          <Info className="h-3.5 w-3.5 mt-0.5 flex-shrink-0"/>
          <span>{yoy.nota} KPI headline: <strong>{hl.label_curr}</strong> vs <strong>{hl.label_prev}</strong>. Le barre tratteggiate indicano periodi parziali.</span>
        </div>
      )}

      {/* KPI Cards */}
      {lYoy ? <LoadingBox /> : kc.impegnato != null && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          {[
            { label:'Impegnato', value: fmtEur(kc.impegnato), delta: delta.impegnato, suffix:'%' },
            { label:'Saving',    value: fmtEur(kc.saving),    delta: delta.saving,    suffix:'%' },
            { label:'% Saving',  value: fmtPct(kc.perc_saving), delta: delta.perc_saving, suffix:' pp' },
          ].map(k => (
            <div key={k.label} className="kpi-card">
              <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">{k.label}</span>
              <div className="text-2xl font-bold text-gray-900 mt-1">{k.value}</div>
              <DeltaBadge value={k.delta} suffix={k.suffix} label={`vs ${ap}`} />
            </div>
          ))}
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Righe Totali</span>
            <div className="text-2xl font-bold text-gray-900 mt-1">{fmtNum(kc.n_righe)}</div>
            <span className="text-xs text-gray-400">tutti i documenti</span>
          </div>
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">% Negoziati</span>
            <div className="text-2xl font-bold text-gray-900 mt-1">{fmtPct(kc.perc_negoziati)}</div>
            <DeltaBadge value={delta.perc_negoziati} suffix=" pp" label={`vs ${ap}`} />
          </div>
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">% Albo</span>
            <div className="text-2xl font-bold text-gray-900 mt-1">{fmtPct(kc.perc_albo)}</div>
            <span className="text-xs text-gray-400">{fmtNum(kc.n_albo)} accreditati</span>
          </div>
        </div>
      )}

      {/* Tempi + NC */}
      {(tempiR || ncR) && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {tempiR && <>
            <KpiCard label="Tempo Medio Ordine" value={fmtDays(tempiR.avg_total_days)} sub="dalla creazione" color="blue" icon={<Clock className="h-3.5 w-3.5"/>}/>
            <KpiCard label="Bottleneck Acquisti" value={fmtPct(tempiR.perc_bottleneck_purchasing)} sub="ordini con ritardo UA" color="orange"/>
          </>}
          {ncR && <>
            <KpiCard label="% Non Conformità" value={fmtPct(ncR.perc_nc)} sub={`${fmtNum(ncR.n_nc)} NC / ${fmtNum(ncR.n_totale)}`} color="red" icon={<AlertTriangle className="h-3.5 w-3.5"/>}/>
            <KpiCard label="Delta Medio Fattura" value={fmtDays(ncR.avg_delta_giorni)} sub="origine → fattura" color="purple"/>
          </>}
        </div>
      )}

      {/* Commento */}
      {autoComment && <YoyComment autoText={autoComment} note={note} onNoteChange={setNote} />}

      {/* GRAFICI SAVING */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <SectionTitle>Saving {anno} vs {ap} (€K) — {GRANULARITA_OPTIONS.find(o=>o.value===granularita)?.label}</SectionTitle>
          {chart.some(d=>d.parziale) && (
            <span className="flex items-center gap-1 text-xs text-amber-600">
              <AlertCircle className="h-3 w-3"/> Tratteggiato = periodo parziale
            </span>
          )}
        </div>
        {lYoy ? <LoadingBox/> : (
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={chartSaving} margin={{top:4,right:8,left:0,bottom:0}} barGap={2}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{fontSize:11}}/>
              <YAxis tick={{fontSize:11}}/>
              <Tooltip content={<CustomTooltip/>}/>
              <Legend wrapperStyle={{fontSize:11}}/>
              <Bar dataKey="curr" name={String(anno)} fill={COLORS.blue} radius={[3,3,0,0]}
                shape={(p) => <CustomBar {...p} parziale={chart[chartSaving.indexOf(chartSaving.find(d=>d.name===p.name))]?.parziale} fill={COLORS.blue}/>}/>
              <Bar dataKey="prev" name={String(ap)} fill="#93c5fd" radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* GRAFICI IMPEGNATO + % SAVING */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>Impegnato {anno} vs {ap} (€K)</SectionTitle>
          {lYoy ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={chartImp} margin={{top:4,right:8,left:0,bottom:0}} barGap={2}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{fontSize:11}}/>
                <YAxis tick={{fontSize:11}}/>
                <Tooltip content={<CustomTooltip/>}/>
                <Legend wrapperStyle={{fontSize:11}}/>
                <Bar dataKey="curr" name={String(anno)} fill={COLORS.blue} radius={[3,3,0,0]}/>
                <Bar dataKey="prev" name={String(ap)} fill="#93c5fd" radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
        <div className="card">
          <SectionTitle>% Saving {anno} vs {ap}</SectionTitle>
          {lYoy ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={chartPct} margin={{top:4,right:8,left:0,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{fontSize:11}}/>
                <YAxis tick={{fontSize:11}} unit="%"/>
                <Tooltip formatter={(v,n)=>[fmtPct(v),n]}/>
                <Legend wrapperStyle={{fontSize:11}}/>
                <Line type="monotone" dataKey="curr" name={String(anno)}
                  stroke={COLORS.blue} strokeWidth={2.5} dot={{r:4}} connectNulls={false}/>
                <Line type="monotone" dataKey="prev" name={String(ap)}
                  stroke="#93c5fd" strokeWidth={1.5} strokeDasharray="5 3" dot={{r:2}}/>
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* CDC — barre orizzontali */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>Impegnato per CDC — {anno} (€K)</SectionTitle>
          {!cdcData ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={cdcChart} layout="vertical" margin={{top:4,right:24,left:64,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:10}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={64}/>
                <Tooltip formatter={(v)=>[`€${v}K`,'Impegnato']}/>
                <Bar dataKey="Impegnato €K" radius={[0,3,3,0]}>
                  {cdcChart.map((e,i)=><Cell key={i} fill={e.fill}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
        <div className="card">
          <SectionTitle>Saving per CDC — {anno} (€K)</SectionTitle>
          {!cdcData ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={cdcChart} layout="vertical" margin={{top:4,right:24,left:64,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:10}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={64}/>
                <Tooltip formatter={(v)=>[`€${v}K`,'Saving']}/>
                <Bar dataKey="Saving €K" radius={[0,3,3,0]}>
                  {cdcChart.map((e,i)=><Cell key={i} fill={e.fill}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Saving per area (stacked) */}
      {mensileArea && mensileArea.length > 0 && (
        <div className="card">
          <SectionTitle>Saving Mensile per Area — {anno} (€K)</SectionTitle>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart
              data={mensileArea.map(d=>({
                name: d.label || d.mese,
                'Ricerca':   Math.round((d.ric_saving||0)/1000),
                'Struttura': Math.round((d.str_saving||0)/1000),
              }))}
              margin={{top:4,right:8,left:0,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{fontSize:11}}/>
              <YAxis tick={{fontSize:11}}/>
              <Tooltip formatter={(v,n)=>[`€${v}K`,n]}/>
              <Legend wrapperStyle={{fontSize:11}}/>
              <Bar dataKey="Ricerca"   fill={COLORS.blue}  radius={[0,0,0,0]} stackId="a"/>
              <Bar dataKey="Struttura" fill={COLORS.teal}  radius={[3,3,0,0]} stackId="a"/>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  )
}
