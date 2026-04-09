import { useState } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, COLORS, CDC_COLORS } from '../utils/fmt'
import { KpiCard, FilterBar, LoadingBox, ErrorBox, SectionTitle, DataTable, Badge } from '../components/UI'
import { YoyBarChart, YoyLineChart, YoyComment, DeltaBadge } from '../components/YoyChart'
import { ExportBar } from '../components/PrintButton'

export default function Saving() {
  const [anno, setAnno] = useState('2025')
  const [strRic, setStrRic] = useState('')
  const [cdc, setCdc] = useState('')
  const [topPer, setTopPer] = useState('saving')
  const [note, setNote] = useState('')

  const annoInt = parseInt(anno) || 2025
  const ap = annoInt - 1
  const params = { anno, str_ric: strRic, cdc }

  const { data: riepilogo, loading: l1, error: e1 } = useKpi(() => api.savingRiepilogo(params), [anno, strRic, cdc])
  const { data: yoy, loading: l2 } = useKpi(() => api.savingYoy({ anno: annoInt, str_ric: strRic, cdc }), [anno, strRic, cdc])
  const { data: yoyCdc, loading: l3 } = useKpi(() => api.savingYoyCdc({ anno: annoInt }), [anno])
  const { data: byBuyer, loading: l4 } = useKpi(() => api.savingPerBuyer({ anno, str_ric: strRic, cdc }), [anno, strRic, cdc])
  const { data: categorie, loading: l5 } = useKpi(() => api.savingCategorie({ anno, str_ric: strRic, cdc }), [anno, strRic, cdc])
  const { data: valute, loading: l6 } = useKpi(() => api.savingValute({ anno }), [anno])
  const { data: topFornitori, loading: l7 } = useKpi(() => api.savingTopFornitori({ anno, per: topPer, str_ric: strRic, cdc }), [anno, topPer, strRic, cdc])

  const chart = yoy?.chart_data || []
  const delta = yoy?.delta || {}
  const kc = yoy?.kpi_corrente || {}
  const kp = yoy?.kpi_precedente || {}

  const autoComment = (() => {
    if (!yoy) return ''
    const d = delta.saving
    const base = `Saving ${anno}: ${fmtEur(kc.saving||0)}`
    if (d != null) return `${base} — ${d >= 0 ? '▲ in crescita' : '▼ in calo'} del ${Math.abs(d).toFixed(1)}% rispetto al ${ap} (${fmtEur(kp.saving||0)}). % saving: ${fmtPct(kc.perc_saving||0)} (${delta.perc_saving>=0?'+':''}${delta.perc_saving?.toFixed(1)||'n/d'} pp vs ${ap}).`
    return `${base}. Dati ${ap} non disponibili.`
  })()

  // CDC YoY chart
  const cdcChart = (yoyCdc || []).map(d => ({
    name: d.cdc,
    [`${anno} €K`]: Math.round((d[`saving_${annoInt}`]||0)/1000),
    [`${ap} €K`]: Math.round((d[`saving_${ap}`]||0)/1000),
  }))

  const buyerChart = (byBuyer || []).slice(0, 8).map(d => ({
    name: d.utente?.split(' ').pop() || d.utente,
    'Saving €K': Math.round(d.saving / 1000),
    '% Saving': d.perc_saving,
  }))

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between flex-wrap gap-3">
        <h1 className="text-2xl font-bold text-gray-900">Saving & Ordini</h1>
        <ExportBar anno={anno} strRic={strRic} cdc={cdc} note={note} />
      </div>

      <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic} cdc={cdc} setCdc={setCdc} />
      {e1 && <ErrorBox message={e1} />}

      {/* KPI */}
      {l1 ? <LoadingBox /> : riepilogo && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Impegnato</span>
            <div className="text-2xl font-bold mt-1">{(riepilogo.impegnato/1e6).toFixed(2)} M€</div>
            <DeltaBadge value={delta.impegnato} label={`vs ${ap}`} />
          </div>
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Saving</span>
            <div className="text-2xl font-bold mt-1">{(riepilogo.saving/1000).toFixed(1)} K€</div>
            <DeltaBadge value={delta.saving} label={`vs ${ap}`} />
          </div>
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">% Saving</span>
            <div className="text-2xl font-bold mt-1">{fmtPct(riepilogo.perc_saving)}</div>
            <DeltaBadge value={delta.perc_saving} suffix=" pp" label={`vs ${ap}`} />
          </div>
          <KpiCard label="Ordini" value={fmtNum(riepilogo.n_ordini)} sub="OS/OSP/PS" color="blue" />
          <div className="kpi-card">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">% Negoziati</span>
            <div className="text-2xl font-bold mt-1">{fmtPct(riepilogo.perc_negoziati)}</div>
            <DeltaBadge value={delta.n_ordini} label={`vs ${ap}`} />
          </div>
          <KpiCard label="Albo Fornitori" value={fmtPct(riepilogo.perc_albo)} sub={fmtNum(riepilogo.n_fornitori_albo)+' ordini'} color="purple" />
        </div>
      )}

      {/* Commento */}
      {yoy && <YoyComment autoText={autoComment} note={note} onNoteChange={setNote} />}

      {/* Saving YoY mensile */}
      <div className="card">
        <SectionTitle>Saving Mensile — {anno} vs {ap} (€K)</SectionTitle>
        {l2 ? <LoadingBox /> : (
          <YoyBarChart
            data={chart.map(d => ({
              ...d,
              [`${anno} €K`]: Math.round((d[`saving_${annoInt}`]||0)/1000),
              [`${ap} €K`]:   Math.round((d[`saving_${ap}`]||0)/1000),
            }))}
            dataKey1={`${anno} €K`} dataKey2={`${ap} €K`}
            label1={String(anno)} label2={String(ap)}
            formatter={v => `€${v}K`}
          />
        )}
      </div>

      {/* Impegnato + % saving YoY */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>Impegnato Mensile — {anno} vs {ap} (€K)</SectionTitle>
          {l2 ? <LoadingBox /> : (
            <YoyBarChart
              data={chart.map(d => ({
                ...d,
                [`${anno} €K`]: Math.round((d[`impegnato_${annoInt}`]||0)/1000),
                [`${ap} €K`]:   Math.round((d[`impegnato_${ap}`]||0)/1000),
              }))}
              dataKey1={`${anno} €K`} dataKey2={`${ap} €K`}
              label1={String(anno)} label2={String(ap)}
              formatter={v => `€${v}K`} height={200}
            />
          )}
        </div>
        <div className="card">
          <SectionTitle>% Saving — {anno} vs {ap}</SectionTitle>
          {l2 ? <LoadingBox /> : (
            <YoyLineChart
              data={chart.map(d => ({
                ...d,
                [`% ${anno}`]: d[`perc_saving_${annoInt}`]||0,
                [`% ${ap}`]:   d[`perc_saving_${ap}`]||0,
              }))}
              dataKey1={`% ${anno}`} dataKey2={`% ${ap}`}
              label1={String(anno)} label2={String(ap)}
              formatter={v => fmtPct(v)} unit="%" height={200}
            />
          )}
        </div>
      </div>

      {/* Ordini negoziati YoY */}
      <div className="card">
        <SectionTitle>Ordini Negoziati — {anno} vs {ap}</SectionTitle>
        {l2 ? <LoadingBox /> : (
          <YoyBarChart
            data={chart.map(d => ({
              ...d,
              [`Neg. ${anno}`]: d[`n_negoziati_${annoInt}`]||0,
              [`Neg. ${ap}`]:   d[`n_negoziati_${ap}`]||0,
            }))}
            dataKey1={`Neg. ${anno}`} dataKey2={`Neg. ${ap}`}
            label1={String(anno)} label2={String(ap)}
          />
        )}
      </div>

      {/* CDC YoY */}
      <div className="card">
        <SectionTitle>Saving per CDC — {anno} vs {ap} (€K)</SectionTitle>
        {l3 ? <LoadingBox /> : (
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={cdcChart} layout="vertical" margin={{top:4,right:16,left:56,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis type="number" tick={{fontSize:11}}/>
              <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={64}/>
              <Tooltip formatter={v=>[`€${v}K`]}/>
              <Legend wrapperStyle={{fontSize:11}}/>
              <Bar dataKey={`${anno} €K`} fill={COLORS.blue} radius={[0,3,3,0]}/>
              <Bar dataKey={`${ap} €K`} fill="#93c5fd" radius={[0,3,3,0]}/>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Buyer + Top fornitori */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>Saving per Buyer (top 8)</SectionTitle>
          {l4 ? <LoadingBox /> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={buyerChart} layout="vertical" margin={{top:4,right:16,left:64,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:11}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11}} width={72}/>
                <Tooltip formatter={(v,n) => [n==='% Saving'?fmtPct(v):`€${v}K`,n]}/>
                <Bar dataKey="Saving €K" fill={COLORS.blue} radius={[0,3,3,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
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
          {l7 ? <LoadingBox /> : (
            <DataTable
              columns={[
                {key:'ragione_sociale',label:'Fornitore'},
                {key:'saving',label:'Saving',render:v=>fmtEur(v)},
                {key:'perc_saving',label:'% Sav.',render:v=>fmtPct(v)},
                {key:'albo',label:'Albo',render:v=><Badge color={v?'green':'gray'}>{v?'SI':'NO'}</Badge>},
              ]}
              rows={topFornitori||[]} maxRows={10}
            />
          )}
        </div>
      </div>

      {/* Categorie + Valute */}
      <div className="card">
        <SectionTitle>Saving per Categoria Merceologica</SectionTitle>
        {l5 ? <LoadingBox /> : (
          <DataTable
            columns={[
              {key:'desc_gruppo_merceol',label:'Categoria'},
              {key:'impegnato',label:'Impegnato',render:v=>fmtEur(v)},
              {key:'saving',label:'Saving',render:v=>fmtEur(v)},
              {key:'perc_saving',label:'% Saving',render:v=>fmtPct(v)},
              {key:'perc_negoziati',label:'% Neg.',render:v=>fmtPct(v)},
            ]}
            rows={categorie||[]}
          />
        )}
      </div>
      <div className="card">
        <SectionTitle>Esposizione Valutaria</SectionTitle>
        {l6 ? <LoadingBox /> : (
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
