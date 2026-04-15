/**
 * Risorse.jsx — Focus Risorsa stile PPT Dashboard_UA_2025 slide 7
 * Ordini lavorati / Negoziati / Saving / % Negoziati per buyer
 */
import { useState, useEffect } from 'react'
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
         ResponsiveContainer, Legend, Cell } from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, COLORS } from '../utils/fmt'
import { KpiCard, LoadingBox, ErrorBox, SectionTitle, PageHeader,
         FilterBar, StatRow, DataTable, Badge } from '../components/UI'
import { User, TrendingUp, Award } from 'lucide-react'

const AREA_COLORS = { RICERCA: '#0057A8', STRUTTURA: '#7c3aed' }

function RisorseCard({ buyer, kpi, monthly, selected, onClick }) {
  if (!buyer) return null
  const area = kpi?.area
  return (
    <div onClick={onClick}
      className={`card cursor-pointer transition-all hover:shadow-md ${selected?'ring-2 ring-telethon-blue ring-offset-1':''}`}>
      <div className="flex items-center gap-3 mb-4">
        <div className="w-10 h-10 rounded-full flex items-center justify-center text-white font-bold text-sm flex-shrink-0"
          style={{ background: AREA_COLORS[area] || COLORS.gray }}>
          {(buyer.split(' ')[0]||'').charAt(0)}{(buyer.split(' ')[1]||'').charAt(0)}
        </div>
        <div>
          <div className="font-bold text-gray-900 text-sm">{buyer}</div>
          <div className="text-xs text-gray-400">{area || '—'}</div>
        </div>
      </div>
      <div className="grid grid-cols-3 gap-2 text-center">
        <div>
          <div className="text-lg font-extrabold text-gray-900">{fmtNum(kpi?.n_righe)}</div>
          <div className="text-xs text-gray-400">Ordini</div>
        </div>
        <div>
          <div className="text-lg font-extrabold text-green-600">{fmtEur(kpi?.saving)}</div>
          <div className="text-xs text-gray-400">Saving</div>
        </div>
        <div>
          <div className="text-lg font-extrabold">{fmtPct(kpi?.perc_negoziati)}</div>
          <div className="text-xs text-gray-400">Neg.</div>
        </div>
      </div>
    </div>
  )
}

export default function Risorse() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]     = useState('')
  const [strRic, setStrRic] = useState('')
  const [selected, setSelected] = useState(null)

  useEffect(() => { if (!anno && defaultAnno) setAnno(String(defaultAnno)) }, [defaultAnno])

  const annoInt = parseInt(anno) || new Date().getFullYear()
  const ap = annoInt - 1
  const ready = !!anno

  const { data: buyers, loading: lB, error: eB } = useKpi(
    () => ready ? api.perBuyer({ anno, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )
  const { data: buyersPrev } = useKpi(
    () => ready ? api.perBuyer({ anno: String(ap), str_ric: strRic }) : Promise.resolve([]),
    [ap, strRic]
  )
  const { data: mensile } = useKpi(
    () => ready ? api.mensile({ anno, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )

  // Filtra solo UA
  const uaBuyers = (buyers || []).filter(b => b.is_ua && b.utente)
  const prevMap  = Object.fromEntries((buyersPrev || []).map(b => [b.utente, b]))

  const sel = selected || (uaBuyers[0]?.utente)
  const selKpi  = uaBuyers.find(b => b.utente === sel)
  const selPrev = prevMap[sel]

  // Saving per buyer — ranking chart
  const rankChart = [...uaBuyers]
    .sort((a,b) => (b.saving||0)-(a.saving||0))
    .map(b => ({
      name: (b.utente||'').split(' ').slice(-1)[0],
      full: b.utente,
      saving: Math.round((b.saving||0)/1000),
      ordini: b.n_righe||0,
      fill: AREA_COLORS[b.area] || COLORS.gray,
    }))

  const delta = (c, p) => p ? round((c-p)/Math.abs(p)*100,1) : null
  const round = (v,d) => Math.round(v*Math.pow(10,d))/Math.pow(10,d)

  return (
    <div className="space-y-6">
      <PageHeader
        title="Performance Team"
        subtitle={`Saving e KPI per buyer | ${ready?anno:'—'}`}
        badge={<span className="badge badge-blue">{uaBuyers.length} buyer UA</span>}
      />

      <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic} anni={anni}/>

      {eB && <ErrorBox message={eB}/>}
      {!ready ? <div className="text-sm text-gray-400 py-4">Seleziona un anno.</div>
      : lB ? <LoadingBox rows={4}/> : (
        <>
          {/* Grid card per ogni buyer */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {uaBuyers.map(b => (
              <RisorseCard key={b.utente} buyer={b.utente} kpi={b}
                selected={sel===b.utente} onClick={()=>setSelected(b.utente)}/>
            ))}
          </div>

          {/* Focus risorsa selezionata — stile PPT slide 7 */}
          {selKpi && (
            <div className="card">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-12 h-12 rounded-full flex items-center justify-center text-white font-bold flex-shrink-0"
                  style={{ background: AREA_COLORS[selKpi.area] || COLORS.blue }}>
                  <User className="h-6 w-6"/>
                </div>
                <div>
                  <h2 className="text-xl font-bold text-gray-900">{sel}</h2>
                  <div className="text-sm text-gray-400">{selKpi.area} | Dettaglio saving e negoziazione | {anno}</div>
                </div>
              </div>

              {/* 6 KPI headline — esattamente come il PPT */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-6">
                {[
                  { icon:'📋', label:'Ordini Lavorati', value:fmtNum(selKpi.n_righe), sub:'totale periodo', color:'gray' },
                  { icon:'🤝', label:'Ordini Negoziati', value:fmtNum(selKpi.n_negoziati), sub:'OS/OSP/PS neg.', color:'blue' },
                  { icon:'📊', label:'% Ordini Neg.', value:fmtPct(selKpi.perc_negoziati), sub:`su ${fmtNum(selKpi.n_negotiable)} negoziabili`, color:'blue' },
                  { icon:'💼', label:'Impegnato', value:fmtEur(selKpi.impegnato), sub:'periodo', color:'default' },
                  { icon:'💰', label:'Saving Totale', value:fmtEur(selKpi.saving), sub:'risparmio netto', color:'green' },
                  { icon:'✨', label:'% Saving', value:fmtPct(selKpi.perc_saving), sub:'su impegnato', color:'green' },
                ].map((k,i) => (
                  <div key={i} className="kpi-card border-l-4 border-l-gray-200 text-center">
                    <div className="text-xl mb-1">{k.icon}</div>
                    <div className="kpi-label text-gray-400 text-center">{k.label}</div>
                    <div className={`kpi-value mt-1 text-center ${k.color==='green'?'text-green-600':k.color==='blue'?'text-telethon-blue':''}`}>
                      {k.value}
                    </div>
                    <div className="text-xs text-gray-400 mt-1">{k.sub}</div>
                    {selPrev && (
                      <div className="mt-1 text-center">
                        {k.label==='Saving Totale'&&selPrev.saving!=null && (
                          <span className={`text-xs font-semibold ${selKpi.saving>selPrev.saving?'text-green-500':'text-red-500'}`}>
                            vs {fmtEur(selPrev.saving)} {ap}
                          </span>
                        )}
                        {k.label==='% Saving'&&selPrev.perc_saving!=null && (
                          <span className={`text-xs font-semibold ${selKpi.perc_saving>selPrev.perc_saving?'text-green-500':'text-red-500'}`}>
                            vs {fmtPct(selPrev.perc_saving)} {ap}
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                ))}
              </div>

              {/* Ranking comparativo */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div>
                  <SectionTitle>Ranking Saving — {anno} (€K)</SectionTitle>
                  <ResponsiveContainer width="100%" height={Math.max(180, rankChart.length*38+60)}>
                    <BarChart data={rankChart} layout="vertical" margin={{top:4,right:16,left:100,bottom:0}}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                      <XAxis type="number" tick={{fontSize:11}} tickFormatter={v=>`€${v}K`}/>
                      <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={100}/>
                      <Tooltip formatter={(v,n)=>[`€${v}K`,'Saving']}/>
                      <Bar dataKey="saving" radius={[0,3,3,0]}>
                        {rankChart.map((e,i)=><Cell key={i} fill={e.full===sel?e.fill:'#d1d5db'}/>)}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div>
                  <SectionTitle>Confronto Buyer — {anno} vs {ap}</SectionTitle>
                  <DataTable sortable
                    columns={[
                      {key:'utente',label:'Buyer',render:v=><span className="font-medium">{v}</span>},
                      {key:'saving',label:`Saving ${anno}`,align:'right',render:v=><span className="text-green-600 font-semibold">{fmtEur(v)}</span>},
                      {key:'saving_prev',label:`Saving ${ap}`,align:'right',render:v=>fmtEur(v)},
                      {key:'delta',label:'Δ',align:'right',render:v=>v==null?'—':<span className={`font-semibold ${v>0?'text-green-600':'text-red-500'}`}>{v>0?'+':''}{v?.toFixed(1)}%</span>},
                    ]}
                    rows={uaBuyers.map(b=>({
                      ...b,
                      saving_prev: prevMap[b.utente]?.saving,
                      delta: prevMap[b.utente]?.saving
                        ? round((b.saving-prevMap[b.utente].saving)/Math.abs(prevMap[b.utente].saving)*100,1)
                        : null,
                    }))}
                  />
                </div>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}
