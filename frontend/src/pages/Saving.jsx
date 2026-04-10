import { useState, useEffect } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, Cell } from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, COLORS, CDC_COLORS } from '../utils/fmt'
import { KpiCard, FilterBar, GranSelect, DeltaBadge, LoadingBox, ErrorBox, SectionTitle, DataTable, Badge } from '../components/UI'
import { Download } from 'lucide-react'

export default function Saving() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]     = useState('')
  const [strRic, setStrRic] = useState('')
  const [cdc, setCdc]       = useState('')
  const [gran, setGran]     = useState('mensile')
  const [topPer, setTopPer] = useState('saving')
  const [note, setNote]     = useState('')

  useEffect(() => { if (!anno && defaultAnno) setAnno(defaultAnno) }, [defaultAnno])

  const annoInt = parseInt(anno) || new Date().getFullYear()
  const ap = annoInt - 1

  const { data: yoy,    loading: lYoy,  error: eYoy  } = useKpi(() => anno ? api.yoy({ anno: annoInt, granularita: gran, str_ric: strRic, cdc }) : Promise.resolve(null), [anno, gran, strRic, cdc])
  const { data: byCdc,  loading: lCdc               } = useKpi(() => anno ? api.yoyCdc({ anno: annoInt }) : Promise.resolve([]), [anno])
  const { data: byBuyer, loading: lBuyer             } = useKpi(() => api.perBuyer({ anno, str_ric: strRic, cdc }), [anno, strRic, cdc])
  const { data: topForn, loading: lForn              } = useKpi(() => api.topFornitori({ anno, per: topPer, str_ric: strRic, cdc }), [anno, topPer, strRic, cdc])
  const { data: categ,  loading: lCat               } = useKpi(() => api.perCategoria({ anno, str_ric: strRic, cdc }), [anno, strRic, cdc])
  const { data: valute, loading: lVal               } = useKpi(() => api.valute({ anno }), [anno])

  const hl    = yoy?.kpi_headline || {}
  const kc    = hl.corrente   || {}
  const kp    = hl.precedente || {}
  const delta = hl.delta      || {}
  const chart = yoy?.chart_data || []

  const chartSav = chart.map(d => ({
    name: d.label,
    [String(anno)]: Math.round((d[`saving_${annoInt}`]||0)/1000),
    [String(ap)]:   Math.round((d[`saving_${ap}`]||0)/1000),
  }))
  const chartImp = chart.map(d => ({
    name: d.label,
    [String(anno)]: Math.round((d[`impegnato_${annoInt}`]||0)/1000),
    [String(ap)]:   Math.round((d[`impegnato_${ap}`]||0)/1000),
  }))
  const chartLst = chart.map(d => ({
    name: d.label,
    [String(anno)]: Math.round((d[`listino_${annoInt}`]||0)/1000),
    [String(ap)]:   Math.round((d[`listino_${ap}`]||0)/1000),
  }))

  // CDC YoY
  const cdcChart = (byCdc||[]).map(d => ({
    name: d.cdc,
    [`Sav. ${anno}`]: Math.round((d[`saving_${annoInt}`]||0)/1000),
    [`Sav. ${ap}`]:   Math.round((d[`saving_${ap}`]||0)/1000),
    fill: CDC_COLORS[d.cdc]||COLORS.gray,
  }))

  const buyerChart = (byBuyer||[]).slice(0,8).map(d => ({
    name: (d.utente||'').split(' ').pop() || d.utente,
    'Saving €K': Math.round((d.saving||0)/1000),
  }))

  const autoComment = kc.saving ? (
    `Saving ${hl.label_curr}: ${fmtEur(kc.saving)}` +
    (delta.saving != null && kp.saving
      ? ` — ${delta.saving>=0?'▲ in crescita':'▼ in calo'} del ${Math.abs(delta.saving).toFixed(1)}% vs ${ap} (${fmtEur(kp.saving)}). % Saving: ${fmtPct(kc.perc_saving)} (${delta.perc_saving!=null?(delta.perc_saving>=0?'+':'')+(delta.perc_saving||0).toFixed(1)+' pp vs '+ap:''}).`
      : '.')
  ) : ''

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between flex-wrap gap-3">
        <h1 className="text-2xl font-bold text-gray-900">Saving & Ordini</h1>
        <button onClick={() => api.exportExcel({filtri:{anno,str_ric:strRic,cdc},sezioni:['riepilogo','mensile','cdc','alfa_documento','top_fornitori']})}
          className="btn-outline"><Download className="h-4 w-4"/> Excel</button>
      </div>

      <div className="flex flex-wrap gap-3">
        <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic}
          cdc={cdc} setCdc={setCdc} anni={anni}/>
        <GranSelect value={gran} onChange={setGran}/>
      </div>

      {eYoy && <ErrorBox message={eYoy}/>}

      {yoy?.nota && (
        <div className="bg-amber-50 border border-amber-100 rounded-xl px-4 py-3 text-xs text-amber-800">
          ℹ️ {yoy.nota} <strong>KPI: {hl.label_curr} vs {hl.label_prev}</strong>
        </div>
      )}

      {/* KPI */}
      {lYoy ? <LoadingBox/> : kc.listino != null && (
        <div className="space-y-3">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="kpi-card border-l-4 border-gray-300">
              <span className="text-xs font-bold text-gray-400 uppercase tracking-wide">Listino</span>
              <div className="text-2xl font-bold text-gray-700 mt-1">{fmtEur(kc.listino)}</div>
              <DeltaBadge value={delta.listino} label={`vs ${ap}`}/>
              <p className="text-xs text-gray-400 mt-1">prezzo di partenza</p>
            </div>
            <div className="kpi-card border-l-4 border-telethon-blue">
              <span className="text-xs font-bold text-telethon-blue uppercase tracking-wide">Impegnato</span>
              <div className="text-2xl font-bold text-gray-900 mt-1">{fmtEur(kc.impegnato)}</div>
              <DeltaBadge value={delta.impegnato} label={`vs ${ap}`}/>
              <p className="text-xs text-gray-400 mt-1">quanto paghiamo</p>
            </div>
            <div className="kpi-card border-l-4 border-green-500">
              <span className="text-xs font-bold text-green-600 uppercase tracking-wide">Saving</span>
              <div className="text-2xl font-bold text-green-700 mt-1">{fmtEur(kc.saving)}</div>
              <div className="flex items-center gap-2">
                <span className="text-base font-bold text-green-600">{fmtPct(kc.perc_saving)}</span>
                <DeltaBadge value={delta.perc_saving} suffix=" pp" label={`vs ${ap}`}/>
              </div>
            </div>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <KpiCard label="N° Ordini" value={fmtNum(kc.n_righe)} sub="tutti i documenti"/>
            <KpiCard label="Negoziabili" value={fmtNum(kc.n_doc_neg)} sub="OS/OSP/OPR/ORN/ORD"/>
            <KpiCard label="% Negoziati" value={fmtPct(kc.perc_negoziati)}
              sub={<DeltaBadge value={delta.perc_negoziati} suffix=" pp" label={`vs ${ap}`}/>}/>
            <KpiCard label="% Albo" value={fmtPct(kc.perc_albo)} sub={`${fmtNum(kc.n_albo)} accreditati`}/>
          </div>
        </div>
      )}

      {/* Commento */}
      {autoComment && (
        <div className="bg-blue-50 border border-blue-100 rounded-xl px-4 py-3 text-sm text-blue-800">
          💬 {autoComment}
          <textarea value={note} onChange={e=>setNote(e.target.value)}
            placeholder="Aggiungi nota o commento per il report…"
            className="w-full mt-2 text-xs border border-blue-100 rounded-lg p-2 bg-white text-gray-700 resize-none focus:outline-none"
            rows={2}/>
        </div>
      )}

      {/* Saving YoY */}
      <div className="card">
        <SectionTitle>Saving — {anno} vs {ap} (€K)</SectionTitle>
        {lYoy ? <LoadingBox/> : (
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={chartSav} margin={{top:4,right:8,left:0,bottom:0}} barGap={2}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{fontSize:11}}/>
              <YAxis tick={{fontSize:11}}/>
              <Tooltip formatter={(v,n)=>[`€${v}K`,n]}/>
              <Legend wrapperStyle={{fontSize:11}}/>
              <Bar dataKey={String(anno)} fill={COLORS.green} radius={[3,3,0,0]}/>
              <Bar dataKey={String(ap)} fill="#86efac" radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>Impegnato — {anno} vs {ap} (€K)</SectionTitle>
          {lYoy ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={chartImp} margin={{top:4,right:8,left:0,bottom:0}} barGap={2}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{fontSize:11}}/>
                <YAxis tick={{fontSize:11}}/>
                <Tooltip formatter={(v,n)=>[`€${v}K`,n]}/>
                <Legend wrapperStyle={{fontSize:11}}/>
                <Bar dataKey={String(anno)} fill={COLORS.blue} radius={[3,3,0,0]}/>
                <Bar dataKey={String(ap)} fill="#93c5fd" radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Saving CDC */}
        <div className="card">
          <SectionTitle>Saving per CDC — {anno} vs {ap} (€K)</SectionTitle>
          {lCdc ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={cdcChart} layout="vertical" margin={{top:4,right:24,left:72,bottom:0}} barGap={2}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:11}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={72}/>
                <Tooltip formatter={(v,n)=>[`€${v}K`,n]}/>
                <Legend wrapperStyle={{fontSize:11}}/>
                <Bar dataKey={`Sav. ${anno}`} fill={COLORS.blue} radius={[0,3,3,0]}/>
                <Bar dataKey={`Sav. ${ap}`} fill="#93c5fd" radius={[0,3,3,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Buyer */}
        <div className="card">
          <SectionTitle>Saving per Buyer — {anno}</SectionTitle>
          {lBuyer ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={buyerChart} layout="vertical" margin={{top:4,right:16,left:80,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:11}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={80}/>
                <Tooltip formatter={(v)=>[`€${v}K`,'Saving']}/>
                <Bar dataKey="Saving €K" fill={COLORS.blue} radius={[0,3,3,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Top fornitori */}
        <div className="card">
          <div className="flex items-center justify-between mb-3">
            <SectionTitle>Top 10 Fornitori — {anno}</SectionTitle>
            <div className="flex gap-1.5">
              {['saving','impegnato'].map(k=>(
                <button key={k} onClick={()=>setTopPer(k)}
                  className={`text-xs px-2.5 py-1 rounded-full border transition-colors
                    ${topPer===k?'bg-telethon-blue text-white border-telethon-blue':'border-gray-200 text-gray-600'}`}>
                  {k}
                </button>
              ))}
            </div>
          </div>
          {lForn ? <LoadingBox/> : (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="text-left py-1.5 px-2 font-semibold text-gray-500 uppercase">Fornitore</th>
                  <th className="text-right py-1.5 px-2 font-semibold text-gray-500 uppercase">Saving</th>
                  <th className="text-right py-1.5 px-2 font-semibold text-gray-500 uppercase">% Sav.</th>
                  <th className="text-center py-1.5 px-2 font-semibold text-gray-500 uppercase">Albo</th>
                </tr>
              </thead>
              <tbody>
                {(topForn||[]).map((r,i)=>(
                  <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-1.5 px-2 font-medium max-w-[140px]">
                      <span className="block truncate" title={r.ragione_sociale}>{r.ragione_sociale}</span>
                    </td>
                    <td className="py-1.5 px-2 text-right tabular-nums text-green-700">{fmtEur(r.saving)}</td>
                    <td className="py-1.5 px-2 text-right tabular-nums">{fmtPct(r.perc_saving)}</td>
                    <td className="py-1.5 px-2 text-center">
                      <Badge color={r.albo?'green':'gray'}>{r.albo?'SI':'NO'}</Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* Categorie + Valute */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>Top Categorie Merceologiche — {anno}</SectionTitle>
          {lCat ? <LoadingBox/> : (
            <DataTable
              columns={[
                {key:'desc_gruppo_merceol',label:'Categoria',align:'left'},
                {key:'impegnato',label:'Impegnato',render:v=>fmtEur(v)},
                {key:'saving',label:'Saving',render:v=>fmtEur(v)},
                {key:'perc_saving',label:'% Sav.',render:v=>fmtPct(v)},
              ]}
              rows={categ||[]} maxRows={10}
            />
          )}
        </div>
        <div className="card">
          <SectionTitle>Esposizione Valutaria — {anno}</SectionTitle>
          {lVal ? <LoadingBox/> : (
            <DataTable
              columns={[
                {key:'valuta',label:'Valuta',align:'left'},
                {key:'impegnato_eur',label:'Impegnato €',render:v=>fmtEur(v)},
                {key:'perc',label:'%',render:v=>fmtPct(v)},
                {key:'n_ordini',label:'N°',render:v=>fmtNum(v)},
              ]}
              rows={valute||[]}
            />
          )}
        </div>
      </div>
    </div>
  )
}
