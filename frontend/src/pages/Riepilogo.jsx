import { useState } from 'react'
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend, PieChart, Pie, Cell,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, fmtDays, shortMese, COLORS, CDC_COLORS } from '../utils/fmt'
import { KpiCard, FilterBar, LoadingBox, ErrorBox, SectionTitle } from '../components/UI'
import { Euro, TrendingUp, Users, Clock, AlertTriangle, ShoppingCart } from 'lucide-react'

export default function Riepilogo() {
  const [anno, setAnno] = useState('2025')
  const [strRic, setStrRic] = useState('')

  const params = Object.fromEntries(Object.entries({ anno, str_ric: strRic }).filter(([, v]) => v))

  const { data: riepilogo, loading: l1, error: e1 } = useKpi(() => api.savingRiepilogo(params), [anno, strRic])
  const { data: mensile, loading: l2 } = useKpi(() => api.savingMensile(params), [anno, strRic])
  const { data: cdc, loading: l3 } = useKpi(() => api.savingPerCdc({ anno }), [anno])
  const { data: tempiR } = useKpi(() => api.tempiRiepilogo(), [])
  const { data: ncR } = useKpi(() => api.ncRiepilogo(), [])

  const mensileChart = (mensile || []).map((m) => ({
    name: shortMese(m.mese),
    'Saving €K': Math.round(m.saving / 1000),
    '% Saving': m.perc_saving,
  }))

  const cdcChart = (cdc || [])
    .filter((d) => d.cdc)
    .map((d) => ({ name: d.cdc, value: Math.round(d.impegnato / 1000) }))

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dashboard Ufficio Acquisti</h1>
          <p className="text-sm text-gray-500 mt-0.5">Report KPI — Fondazione Telethon ETS</p>
        </div>
        <span className="text-xs font-semibold text-telethon-blue">RISERVATO – USO INTERNO</span>
      </div>

      <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic} />
      {e1 && <ErrorBox message={e1} />}

      {l1 ? <LoadingBox /> : riepilogo && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <KpiCard label="Impegnato" value={(riepilogo.impegnato/1e6).toFixed(2)+' M€'} sub="totale periodo" color="blue" icon={<Euro className="h-3.5 w-3.5"/>} />
          <KpiCard label="Saving Generato" value={(riepilogo.saving/1000).toFixed(1)+' K€'} sub="risparmio netto" color="green" icon={<TrendingUp className="h-3.5 w-3.5"/>} />
          <KpiCard label="% Saving" value={fmtPct(riepilogo.perc_saving)} sub="su impegnato" color="green" />
          <KpiCard label="Ordini OS/OSP/PS" value={fmtNum(riepilogo.n_ordini)} sub="totale periodo" color="blue" icon={<ShoppingCart className="h-3.5 w-3.5"/>} />
          <KpiCard label="% Ord. Negoziati" value={fmtPct(riepilogo.perc_negoziati)} sub={fmtNum(riepilogo.n_negoziati)+' negoziati'} color="orange" icon={<Users className="h-3.5 w-3.5"/>} />
          <KpiCard label="% Albo Fornitori" value={fmtPct(riepilogo.perc_albo)} sub="ordini accreditati" color="purple" />
        </div>
      )}

      {(tempiR || ncR) && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {tempiR && <>
            <KpiCard label="Tempo Medio Ordine" value={fmtDays(tempiR.avg_total_days)} sub="dalla creazione" color="blue" icon={<Clock className="h-3.5 w-3.5"/>} />
            <KpiCard label="Bottleneck Acquisti" value={fmtPct(tempiR.perc_bottleneck_purchasing)} sub="ordini con ritardo UA" color="orange" />
          </>}
          {ncR && <>
            <KpiCard label="% Non Conformità" value={fmtPct(ncR.perc_nc)} sub={fmtNum(ncR.n_nc)+' NC su '+fmtNum(ncR.n_totale)} color="red" icon={<AlertTriangle className="h-3.5 w-3.5"/>} />
            <KpiCard label="Delta Medio Fattura" value={fmtDays(ncR.avg_delta_giorni)} sub="origine → fattura" color="purple" />
          </>}
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="card lg:col-span-2">
          <SectionTitle>Saving Mensile (€K)</SectionTitle>
          {l2 ? <LoadingBox /> : (
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={mensileChart} margin={{top:4,right:8,left:0,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                <XAxis dataKey="name" tick={{fontSize:11}} />
                <YAxis tick={{fontSize:11}} />
                <Tooltip formatter={(v,n) => [n==='% Saving' ? fmtPct(v) : `€${v}K`, n]} />
                <Bar dataKey="Saving €K" fill={COLORS.blue} radius={[3,3,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
        <div className="card">
          <SectionTitle>Impegnato per CDC (€K)</SectionTitle>
          {l3 ? <LoadingBox /> : (
            <ResponsiveContainer width="100%" height={240}>
              <PieChart>
                <Pie data={cdcChart} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={75}
                  label={({name,percent})=>`${name} ${(percent*100).toFixed(0)}%`} labelLine={false}>
                  {cdcChart.map((e,i)=><Cell key={i} fill={CDC_COLORS[e.name]||COLORS.gray}/>)}
                </Pie>
                <Tooltip formatter={(v)=>[`€${v}K`]}/>
              </PieChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      <div className="card">
        <SectionTitle>% Saving Mensile</SectionTitle>
        {l2 ? <LoadingBox /> : (
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={mensileChart} margin={{top:4,right:8,left:0,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{fontSize:11}}/>
              <YAxis tick={{fontSize:11}} unit="%"/>
              <Tooltip formatter={(v)=>[fmtPct(v),'% Saving']}/>
              <Line type="monotone" dataKey="% Saving" stroke={COLORS.green} strokeWidth={2} dot={{r:3}}/>
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}
