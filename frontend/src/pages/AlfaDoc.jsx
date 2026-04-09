import { useState, useEffect } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell, Legend,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, COLORS } from '../utils/fmt'
import { FilterBar, LoadingBox, ErrorBox, SectionTitle, KpiCard } from '../components/UI'

const ALFA_COLORS = {
  OPR: '#0057A8', ORN: '#1d4ed8', OS:  '#16a34a', OSP: '#15803d',
  ORD: '#6b7280', OSD: '#9ca3af', OSDP01: '#d1d5db', PS: '#ea580c',
  DDT: '#7c3aed', default: '#6b7280',
}

const ALFA_DESC = {
  OPR:    'Ordine di Prodotto/Ricerca',
  ORN:    'Ordine a Ricevimento Note',
  OS:     'Ordine Standard',
  OSP:    'Ordine Standard Parziale',
  ORD:    'Ordine',
  OSD:    'Ordine Su Domanda',
  OSDP01: 'Ordine Su Domanda (P01)',
  PS:     'Piano di Spedizione',
  DDT:    'Documento di Trasporto',
}

// Tipi considerati negoziabili
const DOC_NEG = new Set(['OS','OSP','PS','OPR','ORN','ORD'])

export default function AlfaDoc() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]   = useState('')
  const [strRic, setStrRic] = useState('')
  const [cdc, setCdc]     = useState('')
  useEffect(() => { if (!anno && defaultAnno) setAnno(defaultAnno) }, [defaultAnno])

  const params = { anno, str_ric: strRic, cdc }
  const { data, loading, error } = useKpi(() => api.savingAlfaDoc(params), [anno, strRic, cdc])

  // Totali
  const tot = {
    imp:  (data||[]).reduce((s,d) => s + d.impegnato, 0),
    sav:  (data||[]).reduce((s,d) => s + d.saving, 0),
    rig:  (data||[]).reduce((s,d) => s + d.n_righe, 0),
    neg:  (data||[]).reduce((s,d) => s + d.n_negoziati, 0),
    neg_doc: (data||[]).reduce((s,d) => s + d.n_doc_neg, 0),
  }

  const chartImp = (data||[]).map(d => ({
    name:          d.alfa_documento,
    'Impegnato €K': Math.round(d.impegnato/1000),
    fill:           ALFA_COLORS[d.alfa_documento] || ALFA_COLORS.default,
  }))
  const chartSav = (data||[]).filter(d => d.saving > 0).map(d => ({
    name:       d.alfa_documento,
    'Saving €K': Math.round(d.saving/1000),
    fill:        ALFA_COLORS[d.alfa_documento] || ALFA_COLORS.default,
  }))
  const chartNeg = (data||[]).filter(d => d.n_doc_neg > 0).map(d => ({
    name:        d.alfa_documento,
    'Negoziati': d.n_negoziati,
    'Totale':    d.n_doc_neg,
    pct:         d.perc_negoziati,
  }))

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Tipologie Documentali</h1>
        <p className="text-sm text-gray-500 mt-0.5">
          Analisi per tipo documento Alyante — OPR, ORN, OS, OSP, ORD, OSD, PS…
        </p>
      </div>

      <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic}
        cdc={cdc} setCdc={setCdc} anni={anni} />
      {error && <ErrorBox message={error} />}

      {/* KPI totali */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard label="Impegnato Totale" value={fmtEur(tot.imp)} sub="tutti i tipi doc" color="blue"/>
        <KpiCard label="Saving Totale"    value={fmtEur(tot.sav)} sub={fmtPct(tot.imp ? tot.sav/tot.imp*100 : 0)} color="green"/>
        <KpiCard label="N° Righe Totali"  value={fmtNum(tot.rig)} color="blue"/>
        <KpiCard label="N° Negoziati"     value={fmtNum(tot.neg)} sub={`su ${fmtNum(tot.neg_doc)} negoziabili`} color="orange"/>
      </div>

      {/* Grafici */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>Impegnato per Tipo Documento (€K)</SectionTitle>
          {loading ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={chartImp} layout="vertical" margin={{top:4,right:24,left:56,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:11}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11, fontFamily:'monospace'}} width={56}/>
                <Tooltip formatter={(v)=>[`€${v}K`,'Impegnato']}/>
                <Bar dataKey="Impegnato €K" radius={[0,3,3,0]}>
                  {chartImp.map((e,i) => <Cell key={i} fill={e.fill}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        <div className="card">
          <SectionTitle>Saving per Tipo Documento (€K)</SectionTitle>
          {loading ? <LoadingBox/> : (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={chartSav} layout="vertical" margin={{top:4,right:24,left:56,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
                <XAxis type="number" tick={{fontSize:11}}/>
                <YAxis dataKey="name" type="category" tick={{fontSize:11, fontFamily:'monospace'}} width={56}/>
                <Tooltip formatter={(v)=>[`€${v}K`,'Saving']}/>
                <Bar dataKey="Saving €K" radius={[0,3,3,0]}>
                  {chartSav.map((e,i) => <Cell key={i} fill={e.fill}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Negoziati per tipo */}
      <div className="card">
        <SectionTitle>Negoziati vs Totale per Tipo Documento (tipi negoziabili)</SectionTitle>
        {loading ? <LoadingBox/> : (
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={chartNeg} layout="vertical" margin={{top:4,right:24,left:56,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
              <XAxis type="number" tick={{fontSize:11}}/>
              <YAxis dataKey="name" type="category" tick={{fontSize:11, fontFamily:'monospace'}} width={56}/>
              <Tooltip formatter={(v,n) => [fmtNum(v), n]}/>
              <Legend wrapperStyle={{fontSize:11}}/>
              <Bar dataKey="Totale"    fill="#e5e7eb" radius={[0,3,3,0]}/>
              <Bar dataKey="Negoziati" fill={COLORS.orange} radius={[0,3,3,0]}/>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Tabella dettaglio */}
      <div className="card">
        <SectionTitle>Dettaglio per Tipologia Documentale</SectionTitle>
        {loading ? <LoadingBox/> : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100">
                {['Tipo','Descrizione','N° Righe','Impegnato','Saving','% Saving','Doc. Neg.','Negoziati','% Neg.','Albo'].map(h => (
                  <th key={h} className="text-right first:text-left py-2 px-3 text-xs font-semibold text-gray-500 uppercase tracking-wide whitespace-nowrap">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(data||[]).map((r,i) => (
                <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                  <td className="py-2 px-3">
                    <span className="font-mono font-bold text-sm"
                      style={{color: ALFA_COLORS[r.alfa_documento]||ALFA_COLORS.default}}>
                      {r.alfa_documento}
                    </span>
                    {DOC_NEG.has(r.alfa_documento) && (
                      <span className="ml-1 text-[10px] text-blue-500 font-medium">NEG.</span>
                    )}
                  </td>
                  <td className="py-2 px-3 text-gray-500 text-xs">{ALFA_DESC[r.alfa_documento]||'—'}</td>
                  <td className="py-2 px-3 text-right tabular-nums">{fmtNum(r.n_righe)}</td>
                  <td className="py-2 px-3 text-right tabular-nums font-medium">{fmtEur(r.impegnato)}</td>
                  <td className="py-2 px-3 text-right tabular-nums text-green-700">{fmtEur(r.saving)}</td>
                  <td className="py-2 px-3 text-right tabular-nums">
                    <span className={r.perc_saving > 10 ? 'text-green-600 font-semibold' : ''}>
                      {fmtPct(r.perc_saving)}
                    </span>
                  </td>
                  <td className="py-2 px-3 text-right tabular-nums">{fmtNum(r.n_doc_neg)}</td>
                  <td className="py-2 px-3 text-right tabular-nums">{fmtNum(r.n_negoziati)}</td>
                  <td className="py-2 px-3 text-right tabular-nums">
                    <span className={r.perc_negoziati > 50 ? 'text-blue-600 font-semibold' : ''}>
                      {r.n_doc_neg > 0 ? fmtPct(r.perc_negoziati) : '—'}
                    </span>
                  </td>
                  <td className="py-2 px-3 text-right tabular-nums">{fmtNum(r.n_albo)}</td>
                </tr>
              ))}
              {/* Totali */}
              <tr className="border-t-2 border-gray-200 bg-gray-50 font-semibold">
                <td className="py-2 px-3 text-xs font-bold text-gray-700">TOTALE</td>
                <td className="py-2 px-3"/>
                <td className="py-2 px-3 text-right">{fmtNum(tot.rig)}</td>
                <td className="py-2 px-3 text-right">{fmtEur(tot.imp)}</td>
                <td className="py-2 px-3 text-right text-green-700">{fmtEur(tot.sav)}</td>
                <td className="py-2 px-3 text-right">{fmtPct(tot.imp ? tot.sav/tot.imp*100 : 0)}</td>
                <td className="py-2 px-3 text-right">{fmtNum(tot.neg_doc)}</td>
                <td className="py-2 px-3 text-right">{fmtNum(tot.neg)}</td>
                <td className="py-2 px-3 text-right">{fmtPct(tot.neg_doc ? tot.neg/tot.neg_doc*100 : 0)}</td>
                <td className="py-2 px-3"/>
              </tr>
            </tbody>
          </table>
        )}
      </div>

      {/* Legenda */}
      <div className="card">
        <SectionTitle>Legenda Tipologie</SectionTitle>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
          {Object.entries(ALFA_DESC).map(([alfa, desc]) => (
            <div key={alfa} className="flex items-center gap-3 py-1">
              <span className="w-12 font-mono font-bold text-sm text-right flex-shrink-0"
                style={{color:ALFA_COLORS[alfa]||ALFA_COLORS.default}}>{alfa}</span>
              <span className="text-xs text-gray-600">{desc}</span>
              {DOC_NEG.has(alfa) && <span className="text-[10px] text-blue-500 font-medium bg-blue-50 px-1 rounded">negoziabile</span>}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
