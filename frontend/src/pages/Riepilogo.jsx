import { useState, useEffect } from 'react'
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, Cell } from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, fmtDays, COLORS, CDC_COLORS } from '../utils/fmt'
import { KpiCard, FilterBar, GranSelect, DeltaBadge, LoadingBox, ErrorBox, SectionTitle } from '../components/UI'
import { Info, TrendingDown, TrendingUp, Clock, AlertTriangle, Printer, FileText, Download } from 'lucide-react'

export default function Riepilogo() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]   = useState('')
  const [strRic, setStrRic] = useState('')
  const [gran, setGran]   = useState('mensile')

  useEffect(() => { if (!anno && defaultAnno) setAnno(defaultAnno) }, [defaultAnno])

  const annoInt = parseInt(anno) || new Date().getFullYear()
  const ap = annoInt - 1

  const { data: yoy,     loading: lYoy,  error: eYoy  } = useKpi(() => anno ? api.yoy({ anno: annoInt, granularita: gran, str_ric: strRic }) : Promise.resolve(null), [anno, gran, strRic])
  const { data: cdcData, loading: lCdc               } = useKpi(() => api.perCdc({ anno, str_ric: strRic }), [anno, strRic])
  const { data: areaData                             } = useKpi(() => api.mensileArea({ anno, str_ric: strRic }), [anno, strRic])
  const { data: tempiR                               } = useKpi(() => api.tempiRiepilogo(), [])
  const { data: ncR                                  } = useKpi(() => api.ncRiepilogo(), [])

  const hl    = yoy?.kpi_headline || {}
  const kc    = hl.corrente   || {}
  const kp    = hl.precedente || {}
  const delta = hl.delta      || {}
  const chart = yoy?.chart_data || []

  // Grafici YoY
  const mkChart = (keyFn) => chart.map(d => ({
    name: d.label, parziale: d.parziale,
    curr: keyFn(d, annoInt), prev: keyFn(d, ap),
  }))
  const chartSav = mkChart((d,a) => Math.round((d[`saving_${a}`]||0)/1000))
  const chartImp = mkChart((d,a) => Math.round((d[`impegnato_${a}`]||0)/1000))
  const chartPct = mkChart((d,a) => d[`perc_saving_${a}`]||0)
  const chartLst = mkChart((d,a) => Math.round((d[`listino_${a}`]||0)/1000))

  // CDC
  const cdcBar = (cdcData||[]).filter(d=>d.cdc).map(d=>({
    name: d.cdc,
    'Listino €K':   Math.round((d.listino||0)/1000),
    'Impegnato €K': Math.round((d.impegnato||0)/1000),
    'Saving €K':    Math.round((d.saving||0)/1000),
    fill: CDC_COLORS[d.cdc]||COLORS.gray,
  }))

  // Commento automatico
  const nota = yoy?.nota || ''
  const autoComment = kc.saving ? (
    `Saving ${hl.label_curr}: ${fmtEur(kc.saving)} ` +
    (delta.saving != null && kp.saving
      ? `— ${delta.saving>=0?'▲ in crescita':'▼ in calo'} del ${Math.abs(delta.saving).toFixed(1)}% ` +
        `vs stesso periodo ${ap} (${fmtEur(kp.saving)}). ` +
        `% Saving: ${fmtPct(kc.perc_saving)} `+
        (delta.perc_saving!=null ? `(${delta.perc_saving>=0?'+':''}${delta.perc_saving.toFixed(1)} pp vs ${ap}).` : '.')
      : '.')
  ) : ''

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dashboard Ufficio Acquisti</h1>
          <p className="text-sm text-gray-500 mt-0.5">Report KPI — Fondazione Telethon ETS</p>
        </div>
        <div className="flex gap-2">
          <button onClick={() => window.print()} className="btn-outline"><Printer className="h-4 w-4"/> Stampa</button>
          <button onClick={() => api.exportExcel({filtri:{anno,str_ric:strRic},sezioni:['riepilogo','mensile','cdc','alfa_documento','top_fornitori']})}
            className="btn-outline"><Download className="h-4 w-4"/> Excel</button>
        </div>
      </div>

      {/* Filtri */}
      <div className="flex flex-wrap gap-3">
        <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic} anni={anni}/>
        <GranSelect value={gran} onChange={setGran}/>
      </div>

      {eYoy && <ErrorBox message={eYoy}/>}

      {/* Nota periodo */}
      {nota && (
        <div className="flex items-start gap-2 bg-amber-50 border border-amber-100 rounded-xl px-4 py-3 text-xs text-amber-800">
          <Info className="h-3.5 w-3.5 mt-0.5 flex-shrink-0"/>
          <span>{nota} <strong>KPI headline: {hl.label_curr} vs {hl.label_prev}</strong>. Barre tratteggiate = periodi parziali.</span>
        </div>
      )}

      {/* KPI Cards principali */}
      {lYoy ? <LoadingBox/> : kc.listino != null && (
        <div className="space-y-3">
          {/* Prima riga: i 3 numeri chiave con impatto visivo */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* LISTINO */}
            <div className="kpi-card border-l-4 border-gray-300">
              <span className="text-xs font-bold text-gray-400 uppercase tracking-wide">Listino (prezzo di partenza)</span>
              <div className="text-3xl font-bold text-gray-700 mt-1">{fmtEur(kc.listino)}</div>
              <DeltaBadge value={delta.listino} label={`vs ${ap}`}/>
              <p className="text-xs text-gray-400 mt-1">Quanto avremmo pagato senza negoziazione</p>
            </div>
            {/* IMPEGNATO */}
            <div className="kpi-card border-l-4 border-telethon-blue">
              <span className="text-xs font-bold text-telethon-blue uppercase tracking-wide">Impegnato (quanto paghiamo)</span>
              <div className="text-3xl font-bold text-gray-900 mt-1">{fmtEur(kc.impegnato)}</div>
              <DeltaBadge value={delta.impegnato} label={`vs ${ap}`}/>
              <p className="text-xs text-gray-400 mt-1">Quanto paghiamo effettivamente</p>
            </div>
            {/* SAVING */}
            <div className="kpi-card border-l-4 border-green-500">
              <span className="text-xs font-bold text-green-600 uppercase tracking-wide">Saving (il nostro lavoro)</span>
              <div className="text-3xl font-bold text-green-700 mt-1">{fmtEur(kc.saving)}</div>
              <DeltaBadge value={delta.saving} label={`vs ${ap}`}/>
              <div className="flex items-center gap-2 mt-1">
                <span className="text-lg font-bold text-green-600">{fmtPct(kc.perc_saving)}</span>
                <DeltaBadge value={delta.perc_saving} suffix=" pp" label={`vs ${ap}`}/>
              </div>
            </div>
          </div>

          {/* Seconda riga: metriche operative */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <KpiCard label="N° Ordini Totali" value={fmtNum(kc.n_righe)} sub="tutti i documenti"/>
            <KpiCard label="Doc. Negoziabili" value={fmtNum(kc.n_doc_neg)} sub="OS/OSP/OPR/ORN/ORD/PS"/>
            <KpiCard label="% Negoziati" value={fmtPct(kc.perc_negoziati)}
              sub={<DeltaBadge value={delta.perc_negoziati} suffix=" pp" label={`vs ${ap}`}/>}/>
            <KpiCard label="% Albo Fornitori" value={fmtPct(kc.perc_albo)} sub={`${fmtNum(kc.n_albo)} accreditati`}/>
          </div>
        </div>
      )}

      {/* Tempi + NC */}
      {(tempiR?.avg_total_days || ncR?.n_nc > 0) && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {tempiR?.avg_total_days && <>
            <KpiCard label="Tempo Medio Ordine" value={fmtDays(tempiR.avg_total_days)} sub="dalla creazione" color="blue" icon={<Clock className="h-3.5 w-3.5"/>}/>
            <KpiCard label="Bottleneck Acquisti" value={fmtPct(tempiR.perc_bottleneck_purchasing)} sub="ordini con ritardo UA" color="orange"/>
          </>}
          {ncR && <>
            <KpiCard label="% Non Conformità" value={fmtPct(ncR.perc_nc)} sub={`${fmtNum(ncR.n_nc)} NC / ${fmtNum(ncR.n_totale)}`} color="red" icon={<AlertTriangle className="h-3.5 w-3.5"/>}/>
            <KpiCard label="Delta Medio Fattura" value={fmtDays(ncR.avg_delta_giorni)} sub="origine → fattura" color="purple"/>
          </>}
        </div>
      )}

      {/* Commento automatico */}
      {autoComment && (
        <div className="bg-blue-50 border border-blue-100 rounded-xl px-4 py-3 text-sm text-blue-800">
          💬 {autoComment}
        </div>
      )}

      {/* Grafico Saving YoY */}
      <div className="card">
        <SectionTitle>Saving — {anno} vs {ap} (€K) — {gran.charAt(0).toUpperCase()+gran.slice(1)}</SectionTitle>
        {lYoy ? <LoadingBox/> : (
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={chartSav} margin={{top:4,right:8,left:0,bottom:0}} barGap={2}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{fontSize:11}}/>
              <YAxis tick={{fontSize:11}}/>
              <Tooltip formatter={(v,n)=>[`€${v}K`,n]}/>
              <Legend wrapperStyle={{fontSize:11}}/>
              <Bar dataKey="curr" name={String(anno)} fill={COLORS.green} radius={[3,3,0,0]}/>
              <Bar dataKey="prev" name={String(ap)} fill="#86efac" radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Listino vs Impegnato */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>Listino vs Impegnato — {anno} (€K)</SectionTitle>
          {lYoy ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart
                data={cdcBar}
                layout="vertical"
                margin={{top:4,right:24,left:72,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:10}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={72}/>
                <Tooltip formatter={(v,n)=>[`€${v}K`,n]}/>
                <Legend wrapperStyle={{fontSize:11}}/>
                <Bar dataKey="Listino €K" fill="#d1d5db" radius={[0,3,3,0]}/>
                <Bar dataKey="Impegnato €K" fill={COLORS.blue} radius={[0,3,3,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        <div className="card">
          <SectionTitle>Saving per CDC — {anno} (€K)</SectionTitle>
          {lCdc ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={cdcBar} layout="vertical" margin={{top:4,right:24,left:72,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:10}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={72}/>
                <Tooltip formatter={(v)=>[`€${v}K`,'Saving']}/>
                <Bar dataKey="Saving €K" radius={[0,3,3,0]}>
                  {cdcBar.map((e,i)=><Cell key={i} fill={e.fill}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* % Saving mensile + Saving per area */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>% Saving — {anno} vs {ap}</SectionTitle>
          {lYoy ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={chartPct} margin={{top:4,right:8,left:0,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{fontSize:11}}/>
                <YAxis tick={{fontSize:11}} unit="%"/>
                <Tooltip formatter={(v,n)=>[fmtPct(v),n]}/>
                <Legend wrapperStyle={{fontSize:11}}/>
                <Line type="monotone" dataKey="curr" name={String(anno)} stroke={COLORS.green} strokeWidth={2.5} dot={{r:3}}/>
                <Line type="monotone" dataKey="prev" name={String(ap)} stroke="#86efac" strokeWidth={1.5} strokeDasharray="5 3" dot={{r:2}}/>
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>

        <div className="card">
          <SectionTitle>Saving per Area — {anno} (€K)</SectionTitle>
          {!areaData ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart
                data={(areaData||[]).map(d=>({
                  name: d.label||d.mese,
                  'Ricerca':   Math.round((d.ric_saving||0)/1000),
                  'Struttura': Math.round((d.str_saving||0)/1000),
                }))}
                margin={{top:4,right:8,left:0,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{fontSize:11}}/>
                <YAxis tick={{fontSize:11}}/>
                <Tooltip formatter={(v,n)=>[`€${v}K`,n]}/>
                <Legend wrapperStyle={{fontSize:11}}/>
                <Bar dataKey="Ricerca"   fill={COLORS.blue} stackId="a"/>
                <Bar dataKey="Struttura" fill={COLORS.teal} stackId="a" radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>
    </div>
  )
}
