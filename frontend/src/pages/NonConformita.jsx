import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { fmtPct, fmtNum, fmtDays, shortMese, COLORS } from '../utils/fmt'
import { KpiCard, LoadingBox, ErrorBox, SectionTitle, DataTable, Badge } from '../components/UI'

export default function NonConformita() {
  const {data:riepilogo,loading:l1,error:e1} = useKpi(()=>api.ncRiepilogo(),[])
  const {data:mensile,loading:l2} = useKpi(()=>api.ncMensile(),[])
  const {data:topForn,loading:l3} = useKpi(()=>api.ncTopFornitori(),[])
  const {data:perTipo,loading:l4} = useKpi(()=>api.ncPerTipo(),[])

  const mensileChart = (mensile||[]).map(m=>({
    name: shortMese(m.mese),
    'N° NC': m.n_nc,
    'N° Totale': m.n_totale,
    '% NC': m.perc_nc,
    'Delta Gg': m.avg_delta_giorni,
  }))

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Non Conformità</h1>

      {e1 && <ErrorBox message={e1}/>}

      {l1 ? <LoadingBox/> : riepilogo && (
        <div className="grid grid-cols-2 md:grid-cols-2 lg:grid-cols-5 gap-4">
          <KpiCard label="Documenti Analizzati" value={fmtNum(riepilogo.n_totale)} color="blue"/>
          <KpiCard label="Non Conformità" value={fmtNum(riepilogo.n_nc)} sub="totale NC rilevate" color="red"/>
          <KpiCard label="% NC" value={fmtPct(riepilogo.perc_nc)} sub="sul totale" color="red"/>
          <KpiCard label="Delta Medio" value={fmtDays(riepilogo.avg_delta_giorni)} sub="origine → fattura" color="orange"/>
          <KpiCard label="Delta Medio NC" value={fmtDays(riepilogo.avg_delta_nc)} sub="solo documenti NC" color="purple"/>
        </div>
      )}

      <div className="card">
        <SectionTitle>Trend Mensile Non Conformità</SectionTitle>
        {l2 ? <LoadingBox/> : (
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={mensileChart} margin={{top:4,right:8,left:0,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{fontSize:11}}/>
              <YAxis tick={{fontSize:11}}/>
              <Tooltip/>
              <Legend wrapperStyle={{fontSize:11}}/>
              <Bar dataKey="N° Totale" fill="#e5e7eb" radius={[2,2,0,0]}/>
              <Bar dataKey="N° NC" fill={COLORS.red} radius={[2,2,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* % NC mensile */}
        <div className="card">
          <SectionTitle>% NC Mensile</SectionTitle>
          {l2 ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={mensileChart} margin={{top:4,right:8,left:0,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{fontSize:11}}/>
                <YAxis tick={{fontSize:11}} unit="%"/>
                <Tooltip formatter={(v)=>[fmtPct(v),'% NC']}/>
                <Line type="monotone" dataKey="% NC" stroke={COLORS.red} strokeWidth={2} dot={{r:3}}/>
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Delta giorni mensile */}
        <div className="card">
          <SectionTitle>Delta Medio Giorni (origine → fattura)</SectionTitle>
          {l2 ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={mensileChart} margin={{top:4,right:8,left:0,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{fontSize:11}}/>
                <YAxis tick={{fontSize:11}} unit=" gg"/>
                <Tooltip formatter={(v)=>[fmtDays(v),'Delta gg']}/>
                <Line type="monotone" dataKey="Delta Gg" stroke={COLORS.orange} strokeWidth={2} dot={{r:3}}/>
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* NC per tipo origine */}
      <div className="card">
        <SectionTitle>NC per Tipo Origine</SectionTitle>
        {l4 ? <LoadingBox/> : (
          <DataTable
            columns={[
              {key:'tipo',label:'Tipo Origine'},
              {key:'n_totale',label:'N° Totale',render:v=>fmtNum(v)},
              {key:'n_nc',label:'N° NC',render:v=>fmtNum(v)},
              {key:'perc_nc',label:'% NC',render:v=><span className={v>10?'text-red-600 font-semibold':''}>{fmtPct(v)}</span>},
              {key:'avg_delta_giorni',label:'Delta Medio',render:v=>fmtDays(v)},
            ]}
            rows={perTipo||[]}
          />
        )}
      </div>

      {/* Top fornitori NC */}
      <div className="card">
        <SectionTitle>Top 10 Fornitori per Non Conformità</SectionTitle>
        {l3 ? <LoadingBox/> : (
          <DataTable
            columns={[
              {key:'ragione_sociale',label:'Fornitore'},
              {key:'n_totale',label:'N° Documenti',render:v=>fmtNum(v)},
              {key:'n_nc',label:'N° NC',render:v=>fmtNum(v)},
              {key:'perc_nc',label:'% NC',render:(v)=>(
                <Badge color={v>20?'red':v>10?'orange':'gray'}>{fmtPct(v)}</Badge>
              )},
              {key:'avg_delta',label:'Delta Medio',render:v=>fmtDays(v)},
            ]}
            rows={topForn||[]}
          />
        )}
      </div>
    </div>
  )
}
