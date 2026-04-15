import { useState, useEffect } from 'react'
import {
  BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend, ComposedChart, Area,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, COLORS, CDC_COLORS } from '../utils/fmt'
import {
  KpiCard, FilterBar, GranSelect, DeltaBadge,
  LoadingBox, ErrorBox, SectionTitle, DataTable, Badge,
} from '../components/UI'
import { Download, AlertTriangle, ChevronDown } from 'lucide-react'

/* ── Esposizione Valutaria — collapsible panel ────────────────────────── */
function EsposizioneValutaria({ anno, ready }) {
  const [open, setOpen] = useState(false)
  const { data: esp, loading } = useKpi(
    () => ready ? api.valuteEsposizione({ anno }) : Promise.resolve(null),
    [anno]
  )
  const valute  = esp?.valute || []
  const foreign = valute.filter(v => v.is_foreign)
  if (!ready || (!loading && !foreign.length)) return null

  return (
    <div className="bg-white rounded-2xl shadow-sm border border-gray-100 overflow-hidden">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-6 py-4 text-left hover:bg-gray-50/60 transition-colors"
      >
        <div>
          <div className="flex items-center gap-2">
            <h3 className="section-title mb-0">Esposizione Valutaria — {anno}</h3>
            {!loading && foreign.length > 0 && (
              <span className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-full bg-amber-50 border border-amber-200 text-amber-700 font-semibold">
                <AlertTriangle className="h-3 w-3" />{foreign.length} valute estere
              </span>
            )}
          </div>
          {!loading && esp && (
            <p className="text-xs text-gray-400 mt-0.5">
              Esposizione estera: {fmtEur(esp.esposizione_estera_eur)} ({fmtPct(esp.perc_esposizione_estera)} del totale)
            </p>
          )}
        </div>
        <ChevronDown className={`h-4 w-4 text-gray-400 transition-transform ${open ? 'rotate-180' : ''}`} />
      </button>

      {open && (
        <div className="border-t border-gray-100">
          {loading ? (
            <div className="p-6"><LoadingBox rows={4} /></div>
          ) : !foreign.length ? (
            <div className="text-center py-8 text-gray-400 text-sm">Nessun ordine in valuta estera</div>
          ) : (
            <>
              {/* Summary */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 px-6 py-4 bg-amber-50/30 border-b border-amber-100/60">
                {[
                  { label: 'Totale EUR', value: fmtEur(esp.totale_eur), cls: 'text-gray-900' },
                  { label: 'Valute', value: fmtNum(esp.n_valute), cls: 'text-gray-900' },
                  { label: 'Ordini Forex', value: fmtNum(foreign.reduce((s,v) => s + (v.n_ordini||0), 0)), cls: 'text-amber-700' },
                  { label: 'Esposizione Estera', value: fmtEur(esp.esposizione_estera_eur), cls: 'text-amber-700',
                    sub: `${fmtPct(esp.perc_esposizione_estera)} del totale` },
                ].map(({label, value, cls, sub}) => (
                  <div key={label}>
                    <div className="text-xs text-gray-500 font-semibold uppercase tracking-wider mb-1">{label}</div>
                    <div className={`text-lg font-extrabold ${cls}`}>{value}</div>
                    {sub && <div className="text-xs text-gray-400">{sub}</div>}
                  </div>
                ))}
              </div>
              {/* Detail table */}
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-100 bg-gray-50/60">
                      {['Valuta','N. Ordini','Importo Originale','Cambio Medio','Controvalore EUR','% Totale','Rischio'].map((h,i) => (
                        <th key={h} className={`py-3 px-4 text-xs font-bold text-gray-500 uppercase tracking-wider
                          ${i > 0 ? 'text-right' : 'text-left'} ${i===6 ? 'text-center' : ''}
                          ${i===2||i===3 ? 'hidden lg:table-cell' : ''}`}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {valute.map((v, i) => (
                      <tr key={v.valuta||i} className={`transition-colors ${v.is_foreign ? 'hover:bg-amber-50/30' : 'opacity-60 hover:bg-gray-50/60'}`}>
                        <td className="py-3 px-4 font-bold text-sm text-gray-900">{v.valuta || '—'}</td>
                        <td className="py-3 px-4 text-right tabular-nums text-gray-700">{fmtNum(v.n_ordini)}</td>
                        <td className="py-3 px-4 text-right tabular-nums text-gray-600 hidden lg:table-cell">
                          {v.importo_originale != null
                            ? `${Number(v.importo_originale).toLocaleString('it-IT',{maximumFractionDigits:0})} ${v.valuta}`
                            : '—'}
                        </td>
                        <td className="py-3 px-4 text-right tabular-nums text-gray-600 hidden lg:table-cell">
                          {v.cambio_medio != null ? Number(v.cambio_medio).toFixed(4) : '—'}
                        </td>
                        <td className="py-3 px-4 text-right tabular-nums font-semibold text-gray-800">{fmtEur(v.impegnato_eur)}</td>
                        <td className="py-3 px-4 text-right tabular-nums">
                          <span className={`font-semibold ${v.is_foreign ? 'text-amber-700' : 'text-gray-500'}`}>{fmtPct(v.perc_su_totale_eur)}</span>
                        </td>
                        <td className="py-3 px-4 text-center">
                          {v.is_foreign
                            ? <span className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-full bg-amber-50 border border-amber-200 text-amber-700 font-semibold whitespace-nowrap">
                                <AlertTriangle className="h-3 w-3" />Rischio cambio
                              </span>
                            : <span className="text-xs text-gray-300">—</span>}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}

export default function Saving() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]     = useState('')
  const [strRic, setStrRic] = useState('')
  const [cdc, setCdc]       = useState('')
  const [gran, setGran]     = useState('mensile')
  const [topPer, setTopPer] = useState('saving')
  const [note, setNote]     = useState('')

  useEffect(() => {
    if (!anno && defaultAnno) setAnno(String(defaultAnno))
  }, [defaultAnno])

  const annoInt = anno ? parseInt(anno, 10) : 0
  const ap      = annoInt ? annoInt - 1 : 0
  const ready   = annoInt > 0

  const { data: yoy,         loading: lYoy,   error: eYoy } = useKpi(
    () => ready ? api.yoy({ anno: annoInt, granularita: gran, str_ric: strRic, cdc }) : Promise.resolve(null),
    [anno, gran, strRic, cdc]
  )
  const { data: byCdc,       loading: lCdc   } = useKpi(
    () => ready ? api.yoyCdc({ anno: annoInt }) : Promise.resolve([]),
    [anno]
  )
  const { data: byBuyer,     loading: lBuyer } = useKpi(
    () => ready ? api.perBuyer({ anno, str_ric: strRic, cdc }) : Promise.resolve([]),
    [anno, strRic, cdc]
  )
  const { data: topForn,     loading: lForn  } = useKpi(
    () => ready ? api.topFornitori({ anno, per: topPer, str_ric: strRic, cdc }) : Promise.resolve([]),
    [anno, topPer, strRic, cdc]
  )
  // FIX: perCategoria ritorna desc_gruppo_merceol — mappato correttamente sotto
  const { data: categ,       loading: lCat   } = useKpi(
    () => ready ? api.perCategoria({ anno, str_ric: strRic, cdc }) : Promise.resolve([]),
    [anno, strRic, cdc]
  )
  const { data: valute,      loading: lVal   } = useKpi(
    () => ready ? api.valute({ anno }) : Promise.resolve([]),
    [anno]
  )
  const { data: pareto,      loading: lPareto } = useKpi(
    () => ready ? api.pareto({ anno, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )
  const { data: concentration } = useKpi(
    () => ready ? api.concentration({ anno, str_ric: strRic }) : Promise.resolve({}),
    [anno, strRic]
  )

  const hl    = yoy?.kpi_headline || {}
  const kc    = hl.corrente   || {}
  const kp    = hl.precedente || {}
  const delta = hl.delta      || {}
  const chart = yoy?.chart_data || []

  const chartSav = chart.map(d => ({
    name: d.label,
    [anno]: Math.round((d[`saving_${annoInt}`]    || 0) / 1000),
    [ap]:   Math.round((d[`saving_${ap}`]         || 0) / 1000),
  }))
  const chartImp = chart.map(d => ({
    name: d.label,
    [anno]: Math.round((d[`impegnato_${annoInt}`] || 0) / 1000),
    [ap]:   Math.round((d[`impegnato_${ap}`]      || 0) / 1000),
  }))

  const cdcChart = (byCdc || []).filter(d => d.cdc).map(d => ({
    name:      d.cdc,
    [`${anno}`]: Math.round((d[`saving_${annoInt}`] || 0) / 1000),
    [`${ap}`]:   Math.round((d[`saving_${ap}`]      || 0) / 1000),
    fill: CDC_COLORS[d.cdc] || COLORS.gray,
  }))

  const buyerChart = (byBuyer || []).slice(0, 8).map(d => ({
    name:        (d.utente || '').split(' ').pop() || d.utente,
    'Saving €K': Math.round((d.saving || 0) / 1000),
  }))

  const savingOnListino    = kc.listino ? (kc.saving    / kc.listino) * 100 : null
  const impegnatoOnListino = kc.listino ? (kc.impegnato / kc.listino) * 100 : null

  const autoComment = ready && kc.saving
    ? `Saving ${hl.label_curr || anno}: ${fmtEur(kc.saving)}` +
      (delta.saving != null && kp.saving
        ? ` — ${delta.saving >= 0 ? '▲ in crescita' : '▼ in calo'} del ${Math.abs(delta.saving).toFixed(1)}%` +
          ` vs ${ap} (${fmtEur(kp.saving)}). % Saving: ${fmtPct(kc.perc_saving)}` +
          (delta.perc_saving != null ? ` (${delta.perc_saving >= 0 ? '+' : ''}${delta.perc_saving.toFixed(1)} pp).` : '.')
        : '.')
    : ''

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between flex-wrap gap-3">
        <h1 className="text-2xl font-bold text-gray-900">Saving & Ordini</h1>
        <button
          onClick={() => api.exportExcel({ filtri: { anno, str_ric: strRic, cdc }, sezioni: ['riepilogo','mensile','cdc','alfa_documento','top_fornitori'] })}
          className="btn-outline">
          <Download className="h-4 w-4"/> Excel
        </button>
      </div>

      <div className="flex flex-wrap gap-3">
        <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic}
          cdc={cdc} setCdc={setCdc} anni={anni}/>
        <GranSelect value={gran} onChange={setGran}/>
      </div>

      {eYoy && (
        <div className="flex items-center gap-2 bg-amber-50 border border-amber-100 rounded-xl px-4 py-3 text-xs text-amber-700">
          <span>⚠️</span>
          <span>
            {eYoy.includes('fetch') || eYoy.includes('network')
              ? 'Il server sta avviando. Attendi qualche secondo e ricarica la pagina.'
              : `Errore: ${eYoy.slice(0, 120)}`}
          </span>
        </div>
      )}
      {yoy?.nota && (
        <div className="bg-amber-50 border border-amber-100 rounded-xl px-4 py-3 text-xs text-amber-800">
          ℹ️ {yoy.nota} <strong>KPI: {hl.label_curr} vs {hl.label_prev}</strong>
        </div>
      )}

      {/* KPI Cards */}
      {!ready ? (
        <div className="text-sm text-gray-400 py-4">Seleziona un anno per visualizzare i dati.</div>
      ) : lYoy ? <LoadingBox/> : kc.listino != null && (
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
            <KpiCard label="N° Ordini"     value={fmtNum(kc.n_righe)}        sub="tutti i documenti"/>
            <KpiCard label="Negoziabili"   value={fmtNum(kc.n_doc_neg)}      sub="OS/OSP/OPR/ORN/ORD"/>
            <KpiCard label="% Negoziati"   value={fmtPct(kc.perc_negoziati)} sub={<DeltaBadge value={delta.perc_negoziati} suffix=" pp" label={`vs ${ap}`}/>}/>
            <KpiCard label="% Albo"        value={fmtPct(kc.perc_albo)}      sub={`${fmtNum(kc.n_albo)} accreditati`}/>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <KpiCard label="Saving/Listino"     value={fmtPct(savingOnListino)}    sub="efficienza economica" color="green"/>
            <KpiCard label="Impegnato/Listino"  value={fmtPct(impegnatoOnListino)} sub="copertura spend"      color="blue"/>
            <KpiCard label="Quota Top 5"        value={fmtPct(concentration?.share_top_5)} sub="concentrazione fornitori" color="orange"/>
            <KpiCard label="HHI Fornitori"      value={fmtNum(concentration?.hhi)} sub={concentration?.hhi_interpretation || 'indice concentrazione'} color="gray"/>
          </div>
        </div>
      )}

      {autoComment && (
        <div className="bg-blue-50 border border-blue-100 rounded-xl px-4 py-3 text-sm text-blue-800">
          💬 {autoComment}
          <textarea value={note} onChange={e => setNote(e.target.value)}
            placeholder="Aggiungi una nota per il report…"
            className="w-full mt-2 text-xs border border-blue-100 rounded-lg p-2 bg-white text-gray-700 resize-none focus:outline-none"
            rows={2}/>
        </div>
      )}

      {/* Saving YoY */}
      <div className="card">
        <SectionTitle>Saving — {anno} vs {ap} (€K)</SectionTitle>
        {lYoy ? <LoadingBox/> : chartSav.length === 0 ? (
          <p className="text-sm text-gray-400 text-center py-8">Seleziona un anno</p>
        ) : (
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={chartSav} barGap={2} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="name" tick={{ fontSize: 11 }}/>
              <YAxis tick={{ fontSize: 11 }} tickFormatter={v => `€${v}K`}/>
              <Tooltip formatter={(v, n) => [`€${v}K`, n]}/>
              <Legend wrapperStyle={{ fontSize: 11 }}/>
              <Bar dataKey={anno}     fill={COLORS.green} radius={[3,3,0,0]}/>
              <Bar dataKey={String(ap)} fill="#86efac"   radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Impegnato YoY */}
        <div className="card">
          <SectionTitle>Impegnato — {anno} vs {ap} (€K)</SectionTitle>
          {lYoy ? <LoadingBox/> : chartImp.length === 0 ? (
            <p className="text-sm text-gray-400 text-center py-8">Nessun dato</p>
          ) : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={chartImp} barGap={2} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis dataKey="name" tick={{ fontSize: 11 }}/>
                <YAxis tick={{ fontSize: 11 }} tickFormatter={v => `€${v}K`}/>
                <Tooltip formatter={(v, n) => [`€${v}K`, n]}/>
                <Legend wrapperStyle={{ fontSize: 11 }}/>
                <Bar dataKey={anno}     fill={COLORS.blue} radius={[3,3,0,0]}/>
                <Bar dataKey={String(ap)} fill="#93c5fd" radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* CDC YoY */}
        <div className="card">
          <SectionTitle>Saving per CDC — {anno} vs {ap} (€K)</SectionTitle>
          {lCdc ? <LoadingBox/> : cdcChart.length === 0 ? (
            <p className="text-sm text-gray-400 text-center py-8">Nessun dato</p>
          ) : (
            // FIX: height dinamico per evitare grafici schiacciati/allungati
            <ResponsiveContainer width="100%" height={Math.max(200, cdcChart.length * 52 + 60)}>
              <BarChart data={cdcChart} layout="vertical" barGap={2}
                margin={{ top: 4, right: 24, left: 72, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{ fontSize: 11 }} tickFormatter={v => `€${v}K`}/>
                <YAxis dataKey="name" type="category" tick={{ fontSize: 11 }} width={72}/>
                <Tooltip formatter={(v, n) => [`€${v}K`, n]}/>
                <Legend wrapperStyle={{ fontSize: 11 }}/>
                <Bar dataKey={anno}       fill={COLORS.blue} radius={[0,3,3,0]}/>
                <Bar dataKey={String(ap)} fill="#93c5fd"     radius={[0,3,3,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Buyer */}
        <div className="card">
          <SectionTitle>Saving per Buyer — {anno}</SectionTitle>
          {lBuyer ? <LoadingBox/> : buyerChart.length === 0 ? (
            <p className="text-sm text-gray-400 text-center py-8">Nessun dato</p>
          ) : (
            <ResponsiveContainer width="100%" height={Math.max(180, buyerChart.length * 36 + 60)}>
              <BarChart data={buyerChart} layout="vertical" margin={{ top: 4, right: 16, left: 80, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{ fontSize: 11 }} tickFormatter={v => `€${v}K`}/>
                <YAxis dataKey="name" type="category" tick={{ fontSize: 11 }} width={80}/>
                <Tooltip formatter={v => [`€${v}K`, 'Saving']}/>
                <Bar dataKey="Saving €K" fill={COLORS.blue} radius={[0,3,3,0]}/>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Top Fornitori */}
        <div className="card">
          <div className="flex items-center justify-between mb-3">
            <SectionTitle>Top 10 Fornitori — {anno}</SectionTitle>
            <div className="flex gap-1.5">
              {['saving','impegnato'].map(k => (
                <button key={k} onClick={() => setTopPer(k)}
                  className={`text-xs px-2.5 py-1 rounded-full border transition-colors
                    ${topPer === k ? 'bg-telethon-blue text-white border-telethon-blue' : 'border-gray-200 text-gray-600'}`}>
                  {k}
                </button>
              ))}
            </div>
          </div>
          {lForn ? <LoadingBox/> : (topForn || []).length === 0 ? (
            <p className="text-sm text-gray-400 text-center py-8">Nessun dato</p>
          ) : (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="text-left py-1.5 px-2 text-gray-500 font-semibold uppercase">Fornitore</th>
                  <th className="text-right py-1.5 px-2 text-gray-500 font-semibold uppercase">Saving</th>
                  <th className="text-right py-1.5 px-2 text-gray-500 font-semibold uppercase">% Sav.</th>
                  <th className="text-center py-1.5 px-2 text-gray-500 font-semibold uppercase">Albo</th>
                </tr>
              </thead>
              <tbody>
                {(topForn || []).map((r, i) => (
                  <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-1.5 px-2 font-medium max-w-[160px]">
                      <span className="block truncate" title={r.ragione_sociale}>{r.ragione_sociale || '—'}</span>
                    </td>
                    <td className="py-1.5 px-2 text-right tabular-nums text-green-700">{fmtEur(r.saving)}</td>
                    <td className="py-1.5 px-2 text-right tabular-nums">{fmtPct(r.perc_saving)}</td>
                    <td className="py-1.5 px-2 text-center">
                      <Badge color={r.albo ? 'green' : 'gray'}>{r.albo ? 'SI' : 'NO'}</Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* Pareto */}
      <div className="card">
        <SectionTitle>Pareto Fornitori — {anno}</SectionTitle>
        {lPareto ? <LoadingBox/> : (pareto || []).length === 0 ? (
          <p className="text-sm text-gray-400 text-center py-8">Nessun dato</p>
        ) : (
          <ResponsiveContainer width="100%" height={260}>
            <ComposedChart data={(pareto || []).slice(0, 20)} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis dataKey="rank" tick={{ fontSize: 11 }}/>
              <YAxis yAxisId="left" tick={{ fontSize: 11 }} tickFormatter={v => fmtEur(v)}/>
              <YAxis yAxisId="right" orientation="right" domain={[0, 100]} tick={{ fontSize: 11 }} unit="%"/>
              <Tooltip formatter={(value, name) => {
                if (name === 'Impegnato') return [fmtEur(value), name]
                if (name === 'Cumulata') return [`${Number(value).toFixed(2)}%`, name]
                return [value, name]
              }}/>
              <Legend wrapperStyle={{ fontSize: 11 }}/>
              <Bar yAxisId="left"  dataKey="imp_impegnato_eur" name="Impegnato" fill={COLORS.blue} radius={[3,3,0,0]}/>
              <Area yAxisId="right" type="monotone" dataKey="cum_perc" name="Cumulata" stroke={COLORS.orange} fill="#fed7aa"/>
            </ComposedChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Categorie + Valute */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>Top Categorie Merceologiche — {anno}</SectionTitle>
          {lCat ? <LoadingBox/> : (
            // FIX: la colonna chiave è desc_gruppo_merceol, mostriamo come 'Categoria'
            <DataTable
              columns={[
                { key: 'desc_gruppo_merceol', label: 'Categoria',  align: 'left',
                  render: v => <span className="font-medium">{v || '—'}</span> },
                { key: 'impegnato',           label: 'Impegnato',  render: v => fmtEur(v) },
                { key: 'saving',              label: 'Saving',     render: v => fmtEur(v) },
                { key: 'perc_saving',         label: '% Sav.',     render: v => fmtPct(v) },
              ]}
              rows={categ || []} maxRows={10}
              emptyMessage="Nessun dato disponibile — verifica che il file contenga la colonna gruppo merceologico"
            />
          )}
        </div>
        <div className="card">
          <SectionTitle>Esposizione Valutaria — {anno}</SectionTitle>
          {lVal ? <LoadingBox/> : (
            <DataTable
              columns={[
                { key: 'valuta',        label: 'Valuta',     align: 'left' },
                { key: 'impegnato_eur', label: 'Impegnato €', render: v => fmtEur(v) },
                { key: 'perc',          label: '%',           render: v => fmtPct(v) },
                { key: 'n_ordini',      label: 'N°',          render: v => fmtNum(v) },
              ]}
              rows={valute || []}
              emptyMessage="Nessun dato valutario disponibile"
            />
          )}
        </div>

        {/* Esposizione Valutaria — nuova analisi rischio cambio */}
        <EsposizioneValutaria anno={anno} ready={ready} />
      </div>
    </div>
  )
}
