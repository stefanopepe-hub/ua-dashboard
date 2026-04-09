import { useState } from 'react'
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, shortMese, COLORS, CDC_COLORS } from '../utils/fmt'
import { KpiCard, FilterBar, LoadingBox, ErrorBox, SectionTitle, DataTable, Badge } from '../components/UI'

export default function Saving() {
  const [anno, setAnno] = useState('2025')
  const [strRic, setStrRic] = useState('')
  const [cdc, setCdc] = useState('')
  const [topPer, setTopPer] = useState('saving')

  const params = Object.fromEntries(Object.entries({anno,str_ric:strRic,cdc}).filter(([,v])=>v))

  const {data:riepilogo,loading:l1,error:e1} = useKpi(()=>api.savingRiepilogo(params),[anno,strRic,cdc])
  const {data:mensile,loading:l2} = useKpi(()=>api.savingMensile(params),[anno,strRic,cdc])
  const {data:byCdc,loading:l3} = useKpi(()=>api.savingPerCdc({anno}),[anno])
  const {data:byBuyer,loading:l4} = useKpi(()=>api.savingPerBuyer({anno,str_ric:strRic}),[anno,strRic])
  const {data:categorie,loading:l5} = useKpi(()=>api.savingCategorie({anno,str_ric:strRic}),[anno,strRic])
  const {data:valute,loading:l6} = useKpi(()=>api.savingValute({anno}),[anno])
  const {data:topFornitori,loading:l7} = useKpi(()=>api.savingTopFornitori({anno,per:topPer,str_ric:strRic}),[anno,topPer,strRic])

  const mensileChart = (mensile||[]).map(m=>({
    name: shortMese(m.mese),
    'Impegnato €K': Math.round(m.impegnato/1000),
    'Saving €K': Math.round(m.saving/1000),
    '% Saving': m.perc_saving,
    'Ord. Totali': m.n_ordini,
    'Negoziati': m.n_negoziati,
  }))

  const cdcChart = (byCdc||[]).filter(d=>d.cdc).map(d=>({
    name: d.cdc,
    'Impegnato €K': Math.round(d.impegnato/1000),
    'Saving €K': Math.round(d.saving/1000),
    fill: CDC_COLORS[d.cdc]||COLORS.gray,
  }))

  const buyerChart = (byBuyer||[]).slice(0,8).map(d=>({
    name: d.utente?.split(' ').pop()||d.utente,
    'Saving €K': Math.round(d.saving/1000),
    '% Saving': d.perc_saving,
  }))

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Saving & Ordini</h1>

      <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic} cdc={cdc} setCdc={setCdc}/>
      {e1 && <ErrorBox message={e1}/>}

      {/* KPI headline */}
      {l1 ? <LoadingBox/> : riepilogo && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <KpiCard label="Impegnato" value={(riepilogo.impegnato/1e6).toFixed(2)+' M€'} color="blue"/>
          <KpiCard label="Saving" value={(riepilogo.saving/1000).toFixed(1)+' K€'} color="green"/>
          <KpiCard label="% Saving" value={fmtPct(riepilogo.perc_saving)} color="green"/>
          <KpiCard label="Ordini OS/OSP/PS" value={fmtNum(riepilogo.n_ordini)} color="blue"/>
          <KpiCard label="Negoziati" value={fmtNum(riepilogo.n_negoziati)} sub={fmtPct(riepilogo.perc_negoziati)} color="orange"/>
          <KpiCard label="Albo Fornitori" value={fmtPct(riepilogo.perc_albo)} sub={fmtNum(riepilogo.n_fornitori_albo)+' ordini'} color="purple"/>
        </div>
      )}

      {/* Andamento mensile impegnato + saving */}
      <div className="card">
        <SectionTitle>Andamento Mensile — Impegnato vs Saving (€K)</SectionTitle>
        {l2 ? <LoadingBox/> : (
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={mensileChart} margin={{top:4,right:8,left:0,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{fontSize:11}}/>
              <YAxis tick={{fontSize:11}}/>
              <Tooltip formatter={(v,n)=>[`€${v}K`,n]}/>
              <Legend wrapperStyle={{fontSize:11}}/>
              <Bar dataKey="Impegnato €K" fill="#bfdbfe" radius={[2,2,0,0]}/>
              <Bar dataKey="Saving €K" fill={COLORS.blue} radius={[2,2,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Ordini negoziati mensili */}
      <div className="card">
        <SectionTitle>Ordini Totali vs Negoziati per Mese</SectionTitle>
        {l2 ? <LoadingBox/> : (
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={mensileChart} margin={{top:4,right:8,left:0,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{fontSize:11}}/>
              <YAxis tick={{fontSize:11}}/>
              <Tooltip/>
              <Legend wrapperStyle={{fontSize:11}}/>
              <Bar dataKey="Ord. Totali" fill="#e5e7eb" radius={[2,2,0,0]}/>
              <Bar dataKey="Negoziati" fill={COLORS.orange} radius={[2,2,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Breakdown CDC */}
        <div className="card">
          <SectionTitle>Saving per Centro di Costo</SectionTitle>
          {l3 ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={cdcChart} layout="vertical" margin={{top:4,right:16,left:40,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:11}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={60}/>
                <Tooltip formatter={(v)=>[`€${v}K`]}/>
                <Bar dataKey="Saving €K" radius={[0,3,3,0]}>
                  {(cdcChart||[]).map((e,i)=>(
                    <rect key={i} fill={e.fill}/>
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Buyer performance */}
        <div className="card">
          <SectionTitle>Saving per Buyer (top 8)</SectionTitle>
          {l4 ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={buyerChart} layout="vertical" margin={{top:4,right:16,left:60,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:11}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={70}/>
                <Tooltip formatter={(v,n)=>[n==='% Saving'?fmtPct(v):`€${v}K`,n]}/>
                <Bar dataKey="Saving €K" fill={COLORS.blue} radius={[0,3,3,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Top fornitori */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <SectionTitle>Top 10 Fornitori</SectionTitle>
          <div className="flex gap-2">
            {['saving','impegnato'].map(k=>(
              <button key={k} onClick={()=>setTopPer(k)}
                className={`text-xs px-3 py-1 rounded-full border transition-colors ${topPer===k?'bg-telethon-blue text-white border-telethon-blue':'border-gray-200 text-gray-600 hover:bg-gray-50'}`}>
                per {k}
              </button>
            ))}
          </div>
        </div>
        {l7 ? <LoadingBox/> : (
          <DataTable
            columns={[
              {key:'ragione_sociale',label:'Fornitore'},
              {key:'impegnato',label:'Impegnato',render:v=>fmtEur(v)},
              {key:'saving',label:'Saving',render:v=>fmtEur(v)},
              {key:'perc_saving',label:'% Saving',render:v=>fmtPct(v)},
              {key:'n_ordini',label:'N° Ordini',render:v=>fmtNum(v)},
              {key:'albo',label:'Albo',render:v=><Badge color={v?'green':'gray'}>{v?'SI':'NO'}</Badge>},
            ]}
            rows={topFornitori||[]}
          />
        )}
      </div>

      {/* Categorie merceologiche */}
      <div className="card">
        <SectionTitle>Saving per Categoria Merceologica</SectionTitle>
        {l5 ? <LoadingBox/> : (
          <DataTable
            columns={[
              {key:'desc_gruppo_merceol',label:'Categoria'},
              {key:'impegnato',label:'Impegnato',render:v=>fmtEur(v)},
              {key:'saving',label:'Saving',render:v=>fmtEur(v)},
              {key:'perc_saving',label:'% Saving',render:v=>fmtPct(v)},
              {key:'n_ordini',label:'N° Ordini',render:v=>fmtNum(v)},
              {key:'perc_negoziati',label:'% Neg.',render:v=>fmtPct(v)},
            ]}
            rows={categorie||[]}
          />
        )}
      </div>

      {/* Valute */}
      <div className="card">
        <SectionTitle>Esposizione Valutaria</SectionTitle>
        {l6 ? <LoadingBox/> : (
          <DataTable
            columns={[
              {key:'valuta',label:'Valuta'},
              {key:'impegnato_eur',label:'Importo €',render:v=>fmtEur(v)},
              {key:'perc',label:'% sul totale',render:v=>fmtPct(v)},
              {key:'n_ordini',label:'N° Ordini',render:v=>fmtNum(v)},
            ]}
            rows={valute||[]}
          />
        )}
      </div>
    </div>
  )
}
