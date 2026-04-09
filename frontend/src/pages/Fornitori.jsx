import { useState } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, COLORS } from '../utils/fmt'
import { LoadingBox, ErrorBox, SectionTitle, DataTable, Badge, KpiCard } from '../components/UI'

export default function Fornitori() {
  const [anno, setAnno] = useState('2025')
  const [strRic, setStrRic] = useState('')

  const {data:pareto,loading:l1,error:e1} = useKpi(()=>api.savingPareto({anno}),[anno])
  const {data:topFornitori,loading:l2} = useKpi(()=>api.savingTopFornitori({anno,per:'impegnato',limit:20,str_ric:strRic}),[anno,strRic])

  // Calcola soglia 80%
  const soglia80 = pareto ? pareto.find(r=>r.cum_perc>=80)?.rank : null
  const soglia50 = pareto ? pareto.find(r=>r.cum_perc>=50)?.rank : null
  const totFornitori = pareto?.length

  const paretoChart = (pareto||[]).slice(0,50).map(r=>({
    rank: r.rank,
    '% Cumulata': r.cum_perc,
  }))

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Analisi Fornitori</h1>
        <div className="flex gap-3">
          <select className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white" value={anno} onChange={e=>setAnno(e.target.value)}>
            <option value="2025">2025</option>
            <option value="2024">2024</option>
            <option value="2023">2023</option>
          </select>
          <select className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white" value={strRic} onChange={e=>setStrRic(e.target.value)}>
            <option value="">Tutti</option>
            <option value="RICERCA">Ricerca</option>
            <option value="STRUTTURA">Struttura</option>
          </select>
        </div>
      </div>

      {e1 && <ErrorBox message={e1}/>}

      {/* Pareto KPI */}
      {pareto && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KpiCard label="Fornitori Totali" value={fmtNum(totFornitori)} sub="nel periodo" color="blue"/>
          <KpiCard label="Coprono 50% spesa" value={fmtNum(soglia50)} sub="fornitori" color="orange"/>
          <KpiCard label="Coprono 80% spesa" value={fmtNum(soglia80)} sub="fornitori (regola Pareto)" color="red"/>
          <KpiCard label="Tail fornitori" value={fmtNum(totFornitori - soglia80)} sub="coprono il 20% restante" color="gray"/>
        </div>
      )}

      {/* Curva Pareto */}
      <div className="card">
        <SectionTitle>Curva Pareto — Concentrazione della Spesa</SectionTitle>
        <p className="text-xs text-gray-500 mb-3">Percentuale cumulata della spesa in funzione del numero di fornitori (ordinati per volume decrescente)</p>
        {l1 ? <LoadingBox/> : (
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={paretoChart} margin={{top:4,right:8,left:0,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="rank" tick={{fontSize:11}} label={{value:'N° Fornitori',position:'insideBottom',offset:-2,fontSize:11}}/>
              <YAxis tick={{fontSize:11}} unit="%" domain={[0,100]}/>
              <Tooltip formatter={(v)=>[fmtPct(v),'Spesa cumulata']} labelFormatter={v=>`Fornitore #${v}`}/>
              <Line type="monotone" dataKey="% Cumulata" stroke={COLORS.blue} strokeWidth={2} dot={false}/>
            </LineChart>
          </ResponsiveContainer>
        )}
        {soglia80 && (
          <p className="text-xs text-gray-500 mt-2">
            ⚡ <strong>{soglia80} fornitori</strong> ({fmtPct(soglia80/totFornitori*100)} del parco) coprono l'80% della spesa totale.
          </p>
        )}
      </div>

      {/* Top 20 fornitori per volume */}
      <div className="card">
        <SectionTitle>Top 20 Fornitori per Volume Acquistato</SectionTitle>
        {l2 ? <LoadingBox/> : (
          <DataTable
            columns={[
              {key:'ragione_sociale',label:'Fornitore'},
              {key:'impegnato',label:'Impegnato',render:v=>fmtEur(v)},
              {key:'saving',label:'Saving',render:v=>fmtEur(v)},
              {key:'perc_saving',label:'% Saving',render:v=>fmtPct(v)},
              {key:'n_ordini',label:'N° Ordini',render:v=>fmtNum(v)},
              {key:'albo',label:'Albo',render:v=><Badge color={v?'green':'gray'}>{v?'✓ SI':'NO'}</Badge>},
            ]}
            rows={topFornitori||[]}
            maxRows={20}
          />
        )}
      </div>
    </div>
  )
}
