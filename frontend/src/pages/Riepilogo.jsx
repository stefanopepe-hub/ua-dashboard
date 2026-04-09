import { useState, useEffect } from 'react'
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, fmtDays, COLORS, CDC_COLORS } from '../utils/fmt'
import { KpiCard, FilterBar, LoadingBox, ErrorBox, SectionTitle } from '../components/UI'
import { YoyBarChart, YoyLineChart, YoyComment, DeltaBadge } from '../components/YoyChart'
import { ExportBar } from '../components/PrintButton'
import { Clock, AlertTriangle, ShoppingCart } from 'lucide-react'

export default function Riepilogo() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno] = useState('')
  const [strRic, setStrRic] = useState('')
  const [note, setNote] = useState('')

  // Imposta anno di default appena gli anni sono disponibili
  useEffect(() => { if (!anno && defaultAnno) setAnno(defaultAnno) }, [defaultAnno])

  const annoInt = parseInt(anno) || new Date().getFullYear()
  const ap = annoInt - 1
  const params = { anno, str_ric: strRic }

  const { data: riepilogo, loading: l1, error: e1 } = useKpi(() => api.savingRiepilogo(params), [anno, strRic])
  const { data: yoy, loading: l2 } = useKpi(() => anno ? api.savingYoy({ anno: annoInt, str_ric: strRic }) : Promise.resolve(null), [anno, strRic])
  const { data: cdc, loading: l3 } = useKpi(() => api.savingPerCdc({ anno }), [anno])
  const { data: tempiR } = useKpi(() => api.tempiRiepilogo(), [])
  const { data: ncR } = useKpi(() => api.ncRiepilogo(), [])

  const chart = yoy?.chart_data || []
  const delta = yoy?.delta || {}
  const kc = yoy?.kpi_corrente || {}
  const kp = yoy?.kpi_precedente || {}

  const cdcChart = (cdc || []).filter(d => d.cdc)
    .map(d => ({ name: d.cdc, value: Math.round(d.impegnato / 1000) }))

  const autoComment = (() => {
    if (!yoy || !kc.saving) return ''
    const d = delta.saving
    const base = `Saving ${anno}: ${fmtEur(kc.saving)}`
    if (d != null) return `${base} — ${d >= 0 ? 'in crescita' : 'in calo'} del ${Math.abs(d).toFixed(1)}% rispetto al ${ap} (${fmtEur(kp.saving || 0)}).`
    return `${base}. Dati ${ap} non ancora caricati.`
  })()

  return (
    <div className="space-y-6 print:space-y-4">
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dashboard Ufficio Acquisti</h1>
          <p className="text-sm text-gray-500 mt-0.5">Report KPI — Fondazione Telethon ETS</p>
        </div>
        <ExportBar anno={anno} strRic={strRic} note={note} />
      </div>

      <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic} anni={anni} />
      {e1 && <ErrorBox message={e1} />}

      {/* KPI Cards con delta YoY */}
      {l1 ? <LoadingBox /> : riepilogo && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Impegnato</span>
            <div className="text-2xl font-bold text-gray-900 mt-1">{(riepilogo.impegnato/1e6).toFixed(2)} M€</div>
            <DeltaBadge value={delta.impegnato} label={`vs ${ap}`} />
          </div>
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Saving</span>
            <div className="text-2xl font-bold text-gray-900 mt-1">{(riepilogo.saving/1000).toFixed(1)} K€</div>
            <DeltaBadge value={delta.saving} label={`vs ${ap}`} />
          </div>
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">% Saving</span>
            <div className="text-2xl font-bold text-gray-900 mt-1">{fmtPct(riepilogo.perc_saving)}</div>
            <DeltaBadge value={delta.perc_saving} suffix=" pp" label={`vs ${ap}`} />
          </div>
          <KpiCard label="Ordini OS/OSP/PS" value={fmtNum(riepilogo.n_ordini)} sub="totale periodo" color="blue" icon={<ShoppingCart className="h-3.5 w-3.5"/>}/>
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">% Negoziati</span>
            <div className="text-2xl font-bold text-gray-900 mt-1">{fmtPct(riepilogo.perc_negoziati)}</div>
            <DeltaBadge value={delta.n_ordini} label={`vs ${ap}`} />
          </div>
          <KpiCard label="% Albo Fornitori" value={fmtPct(riepilogo.perc_albo)} sub="ordini accreditati" color="purple"/>
        </div>
      )}

      {(tempiR || ncR) && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {tempiR && <>
            <KpiCard label="Tempo Medio Ordine" value={fmtDays(tempiR.avg_total_days)} sub="dalla creazione" color="blue" icon={<Clock className="h-3.5 w-3.5"/>}/>
            <KpiCard label="Bottleneck Acquisti" value={fmtPct(tempiR.perc_bottleneck_purchasing)} sub="ordini con ritardo UA" color="orange"/>
          </>}
          {ncR && <>
            <KpiCard label="% Non Conformità" value={fmtPct(ncR.perc_nc)} sub={`${fmtNum(ncR.n_nc)} NC su ${fmtNum(ncR.n_totale)}`} color="red" icon={<AlertTriangle className="h-3.5 w-3.5"/>}/>
            <KpiCard label="Delta Medio Fattura" value={fmtDays(ncR.avg_delta_giorni)} sub="origine → fattura" color="purple"/>
          </>}
        </div>
      )}

      {autoComment && <YoyComment autoText={autoComment} note={note} onNoteChange={setNote} />}

      <div className="card">
        <SectionTitle>Saving Mensile — {anno} vs {ap} (€K)</SectionTitle>
        {l2 ? <LoadingBox /> : (
          <YoyBarChart
            data={chart.map(d => ({...d, [`${anno} €K`]: Math.round((d[`saving_${annoInt}`]||0)/1000), [`${ap} €K`]: Math.round((d[`saving_${ap}`]||0)/1000)}))}
            dataKey1={`${anno} €K`} dataKey2={`${ap} €K`}
            label1={String(anno)} label2={String(ap)} formatter={v=>`€${v}K`}
          />
        )}
      </div>

      <div className="card">
        <SectionTitle>% Saving Mensile — {anno} vs {ap}</SectionTitle>
        {l2 ? <LoadingBox /> : (
          <YoyLineChart
            data={chart.map(d => ({...d, [`% ${anno}`]: d[`perc_saving_${annoInt}`]||0, [`% ${ap}`]: d[`perc_saving_${ap}`]||0}))}
            dataKey1={`% ${anno}`} dataKey2={`% ${ap}`}
            label1={String(anno)} label2={String(ap)} formatter={v=>fmtPct(v)} unit="%"
          />
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="card lg:col-span-2">
          <SectionTitle>Impegnato Mensile — {anno} vs {ap} (€K)</SectionTitle>
          {l2 ? <LoadingBox /> : (
            <YoyBarChart
              data={chart.map(d => ({...d, [`${anno} €K`]: Math.round((d[`impegnato_${annoInt}`]||0)/1000), [`${ap} €K`]: Math.round((d[`impegnato_${ap}`]||0)/1000)}))}
              dataKey1={`${anno} €K`} dataKey2={`${ap} €K`}
              label1={String(anno)} label2={String(ap)} formatter={v=>`€${v}K`}
            />
          )}
        </div>
        <div className="card">
          <SectionTitle>Impegnato per CDC</SectionTitle>
          {l3 ? <LoadingBox /> : (
            <ResponsiveContainer width="100%" height={240}>
              <PieChart>
                <Pie data={cdcChart} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={75}
                  label={({name,percent})=>`${name} ${(percent*100).toFixed(0)}%`} labelLine={false}>
                  {cdcChart.map((e,i)=><Cell key={i} fill={CDC_COLORS[e.name]||COLORS.gray}/>)}
                </Pie>
                <Tooltip formatter={v=>[`€${v}K`]}/>
              </PieChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>
    </div>
  )
}
