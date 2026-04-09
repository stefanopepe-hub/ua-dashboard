import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { fmtDays, fmtNum, fmtPct, shortMese, COLORS } from '../utils/fmt'
import { KpiCard, LoadingBox, ErrorBox, SectionTitle, DataTable } from '../components/UI'

const BOTTLENECK_COLORS = { PURCHASING: COLORS.red, AUTO: COLORS.blue, OTHER: COLORS.gray }

export default function Tempi() {
  const {data:riepilogo,loading:l1,error:e1} = useKpi(()=>api.tempiRiepilogo(),[])
  const {data:mensile,loading:l2,error:e2} = useKpi(()=>api.tempiMensile(),[])
  const {data:dist,loading:l3} = useKpi(()=>api.tempiDistribuzione(),[])

  const mensileChart = (mensile||[]).map(m=>({
    name: shortMese(m.mese),
    Purchasing: m.avg_purchasing,
    Automatico: m.avg_auto,
    Altro: m.avg_other,
    'Tot. Giorni': m.avg_total,
    'N° Ordini': m.n_ordini,
  }))

  const bottleneckChart = (mensile||[]).map(m=>({
    name: shortMese(m.mese),
    PURCHASING: m.n_bottleneck_purchasing,
    AUTO: m.n_bottleneck_auto,
    OTHER: m.n_bottleneck_other,
  }))

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Tempi di Attraversamento Ordini</h1>

      {(e1||e2) && <ErrorBox message={e1||e2}/>}

      {/* KPI headline */}
      {l1 ? <LoadingBox/> : riepilogo && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <KpiCard label="Tempo Medio Totale" value={fmtDays(riepilogo.avg_total_days)} sub="per ordine" color="blue"/>
          <KpiCard label="Tempo UA" value={fmtDays(riepilogo.avg_purchasing)} sub="fase acquisti" color="orange"/>
          <KpiCard label="Tempo Automatico" value={fmtDays(riepilogo.avg_auto)} sub="fase sistema" color="purple"/>
          <KpiCard label="Tempo Altro" value={fmtDays(riepilogo.avg_other)} sub="altre fasi" color="blue"/>
          <KpiCard label="Bottleneck UA" value={fmtPct(riepilogo.perc_bottleneck_purchasing)} sub="ordini con ritardo UA" color="red"/>
          <KpiCard label="N° Ordini Analizzati" value={fmtNum(riepilogo.n_ordini)} color="blue"/>
        </div>
      )}

      {/* Andamento mensile tempo medio per fase */}
      <div className="card">
        <SectionTitle>Tempo Medio per Fase — Andamento Mensile (giorni)</SectionTitle>
        {l2 ? <LoadingBox/> : (
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={mensileChart} margin={{top:4,right:8,left:0,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{fontSize:11}}/>
              <YAxis tick={{fontSize:11}} unit=" gg"/>
              <Tooltip formatter={(v,n)=>[fmtDays(v),n]}/>
              <Legend wrapperStyle={{fontSize:11}}/>
              <Bar dataKey="Purchasing" stackId="a" fill={COLORS.red} radius={[0,0,0,0]}/>
              <Bar dataKey="Automatico" stackId="a" fill={COLORS.blue}/>
              <Bar dataKey="Altro" stackId="a" fill={COLORS.gray} radius={[2,2,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Trend totale */}
      <div className="card">
        <SectionTitle>Trend Tempo Medio Totale (giorni)</SectionTitle>
        {l2 ? <LoadingBox/> : (
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={mensileChart} margin={{top:4,right:8,left:0,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{fontSize:11}}/>
              <YAxis tick={{fontSize:11}} unit=" gg"/>
              <Tooltip formatter={(v)=>[fmtDays(v),'Giorni totali']}/>
              <Line type="monotone" dataKey="Tot. Giorni" stroke={COLORS.blue} strokeWidth={2} dot={{r:3}}/>
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Bottleneck mensile */}
        <div className="card">
          <SectionTitle>Bottleneck per Mese (N° Ordini)</SectionTitle>
          {l2 ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={bottleneckChart} margin={{top:4,right:8,left:0,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{fontSize:11}}/>
                <YAxis tick={{fontSize:11}}/>
                <Tooltip/>
                <Legend wrapperStyle={{fontSize:11}}/>
                <Bar dataKey="PURCHASING" fill={COLORS.red} radius={[2,2,0,0]}/>
                <Bar dataKey="AUTO" fill={COLORS.blue} radius={[2,2,0,0]}/>
                <Bar dataKey="OTHER" fill={COLORS.gray} radius={[2,2,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Distribuzione fasce */}
        <div className="card">
          <SectionTitle>Distribuzione per Fascia Temporale</SectionTitle>
          {l3 ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={dist||[]} layout="vertical" margin={{top:4,right:16,left:48,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:11}}/>
                <YAxis dataKey="fascia" type="category" tick={{fontSize:11}} width={56}/>
                <Tooltip formatter={(v)=>[fmtNum(v),'Ordini']}/>
                <Bar dataKey="n_ordini" fill={COLORS.blue} radius={[0,3,3,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Tabella dettaglio mensile */}
      <div className="card">
        <SectionTitle>Dettaglio Mensile</SectionTitle>
        {l2 ? <LoadingBox/> : (
          <DataTable
            columns={[
              {key:'name',label:'Mese'},
              {key:'Tot. Giorni',label:'Tempo Tot.',render:v=>fmtDays(v)},
              {key:'Purchasing',label:'Fase UA',render:v=>fmtDays(v)},
              {key:'Automatico',label:'Fase Auto',render:v=>fmtDays(v)},
              {key:'Altro',label:'Altro',render:v=>fmtDays(v)},
              {key:'N° Ordini',label:'N° Ordini',render:v=>fmtNum(v)},
            ]}
            rows={mensileChart}
          />
        )}
      </div>
    </div>
  )
}
