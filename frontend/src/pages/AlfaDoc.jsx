import { useState, useEffect } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, Legend } from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, COLORS } from '../utils/fmt'
import { FilterBar, LoadingBox, ErrorBox, SectionTitle, KpiCard } from '../components/UI'

const ALFA_COLORI = {
  OPR: '#0057A8', ORN: '#2563eb', OS: '#16a34a',
  OSP: '#15803d', ORD: '#6b7280', OSD: '#9ca3af',
  OSDP01: '#d1d5db', PS: '#ea580c',
}
const ALFA_DESC = {
  OPR:    'Ordine di Prodotto / Ricerca',
  ORN:    'Ordine a Ricevimento Note',
  OS:     'Ordine Standard',
  OSP:    'Ordine Standard Parziale',
  ORD:    'Ordine',
  OSD:    'Ordine Su Domanda',
  OSDP01: 'Ordine Su Domanda (variante)',
  PS:     'Piano di Spedizione',
}
const DOC_NEG = new Set(['OS','OSP','PS','OPR','ORN','ORD'])

export default function AlfaDoc() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]     = useState('')
  const [strRic, setStrRic] = useState('')
  const [cdc, setCdc]       = useState('')

  useEffect(() => {
    if (!anno && defaultAnno) setAnno(String(defaultAnno))
  }, [defaultAnno])

  const { data, loading, error } = useKpi(
    () => anno ? api.perAlfa({ anno, str_ric: strRic, cdc }) : Promise.resolve([]),
    [anno, strRic, cdc]
  )

  const rows = data || []
  const tot = {
    listino:  rows.reduce((s, d) => s + (d.listino  || 0), 0),
    impegnato:rows.reduce((s, d) => s + (d.impegnato|| 0), 0),
    saving:   rows.reduce((s, d) => s + (d.saving   || 0), 0),
    righe:    rows.reduce((s, d) => s + (d.n_righe  || 0), 0),
    neg:      rows.reduce((s, d) => s + (d.n_negoziati || 0), 0),
    doc_neg:  rows.reduce((s, d) => s + (d.n_doc_neg|| 0), 0),
  }

  const colorOf = a => ALFA_COLORI[a] || '#94a3b8'

  const chartImp = rows.map(d => ({ name: d.alfa_documento, 'Listino €K': Math.round((d.listino||0)/1000), 'Impegnato €K': Math.round((d.impegnato||0)/1000) }))
  const chartSav = rows.filter(d => (d.saving||0) > 0).map(d => ({ name: d.alfa_documento, 'Saving €K': Math.round((d.saving||0)/1000) }))
  const chartNeg = rows.filter(d => DOC_NEG.has(d.alfa_documento) && (d.n_doc_neg||0) > 0).map(d => ({
    name: d.alfa_documento, 'Totale': d.n_doc_neg||0, 'Negoziati': d.n_negoziati||0,
  }))

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Tipologie Documentali</h1>
        <p className="text-sm text-gray-500 mt-0.5">
          Analisi per tipo documento Alyante — OPR, ORN, OS, OSP, ORD, OSD, PS
        </p>
      </div>

      <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic}
        cdc={cdc} setCdc={setCdc} anni={anni} />
      {error && <ErrorBox message={error} />}

      {/* KPI */}
      {loading ? <LoadingBox /> : (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KpiCard label="LISTINO TOTALE"   value={fmtEur(tot.listino)}   sub="prezzo di partenza" color="gray" />
          <KpiCard label="IMPEGNATO TOTALE" value={fmtEur(tot.impegnato)} sub="quanto paghiamo" color="blue" />
          <KpiCard label="SAVING TOTALE"    value={fmtEur(tot.saving)}    sub={fmtPct(tot.listino ? tot.saving/tot.listino*100 : 0)} color="green" />
          <KpiCard label="NEGOZIATI"        value={fmtNum(tot.neg)}       sub={`su ${fmtNum(tot.doc_neg)} negoziabili`} color="orange" />
        </div>
      )}

      {/* Grafici */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <SectionTitle>Listino vs Impegnato per Tipo — {anno} (€K)</SectionTitle>
          {loading ? <LoadingBox /> : (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={chartImp} layout="vertical" margin={{ top: 4, right: 24, left: 64, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                <XAxis type="number" tick={{ fontSize: 11 }} />
                <YAxis dataKey="name" type="category" tick={{ fontSize: 11, fontFamily: 'monospace' }} width={64} />
                <Tooltip formatter={(v, n) => [`€${v}K`, n]} />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                <Bar dataKey="Listino €K" fill="#d1d5db" radius={[0, 3, 3, 0]} />
                <Bar dataKey="Impegnato €K" fill={COLORS.blue} radius={[0, 3, 3, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        <div className="card">
          <SectionTitle>Saving per Tipo Documento — {anno} (€K)</SectionTitle>
          {loading ? <LoadingBox /> : chartSav.length === 0 ? (
            <p className="text-sm text-gray-400 text-center py-8">Nessun saving per il periodo</p>
          ) : (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={chartSav} layout="vertical" margin={{ top: 4, right: 24, left: 64, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                <XAxis type="number" tick={{ fontSize: 11 }} />
                <YAxis dataKey="name" type="category" tick={{ fontSize: 11, fontFamily: 'monospace' }} width={64} />
                <Tooltip formatter={(v) => [`€${v}K`, 'Saving']} />
                <Bar dataKey="Saving €K" radius={[0, 3, 3, 0]}>
                  {chartSav.map((e, i) => <Cell key={i} fill={colorOf(e.name)} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Negoziati */}
      {chartNeg.length > 0 && (
        <div className="card">
          <SectionTitle>Negoziati vs Totale Negoziabili — {anno}</SectionTitle>
          <p className="text-xs text-gray-400 mb-3">Tipi su cui si negozia: OS, OSP, OPR, ORN, ORD, PS</p>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={chartNeg} layout="vertical" margin={{ top: 4, right: 24, left: 64, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis dataKey="name" type="category" tick={{ fontSize: 11, fontFamily: 'monospace' }} width={64} />
              <Tooltip formatter={(v, n) => [fmtNum(v), n]} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="Totale" fill="#e5e7eb" radius={[0, 3, 3, 0]} />
              <Bar dataKey="Negoziati" fill={COLORS.orange} radius={[0, 3, 3, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Tabella */}
      <div className="card">
        <SectionTitle>Dettaglio per Tipologia Documentale — {anno}</SectionTitle>
        {loading ? <LoadingBox /> : rows.length === 0 ? (
          <p className="text-sm text-gray-400 text-center py-8">Nessun dato per il periodo</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b-2 border-gray-100">
                  {['TIPO','DESCRIZIONE','N° RIGHE','LISTINO','IMPEGNATO','SAVING','% SAVING','NEG.BILI','NEGOZ.','% NEG.'].map(h => (
                    <th key={h} className={`py-2 px-3 text-xs font-bold text-gray-500 uppercase tracking-wide
                      ${['TIPO','DESCRIZIONE'].includes(h) ? 'text-left' : 'text-right'}`}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => (
                  <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2.5 px-3">
                      <div className="flex items-center gap-1.5">
                        <span className="font-mono font-bold text-sm" style={{ color: colorOf(r.alfa_documento) }}>
                          {r.alfa_documento}
                        </span>
                        {DOC_NEG.has(r.alfa_documento) && (
                          <span className="text-[9px] font-bold text-blue-500 bg-blue-50 border border-blue-100 px-1 rounded">NEG.</span>
                        )}
                      </div>
                    </td>
                    <td className="py-2.5 px-3 text-gray-500 text-xs">{ALFA_DESC[r.alfa_documento] || '—'}</td>
                    <td className="py-2.5 px-3 text-right tabular-nums">{fmtNum(r.n_righe)}</td>
                    <td className="py-2.5 px-3 text-right tabular-nums text-gray-500">{fmtEur(r.listino)}</td>
                    <td className="py-2.5 px-3 text-right tabular-nums font-medium">{fmtEur(r.impegnato)}</td>
                    <td className="py-2.5 px-3 text-right tabular-nums text-green-700">{r.saving > 0 ? fmtEur(r.saving) : '—'}</td>
                    <td className="py-2.5 px-3 text-right tabular-nums">
                      {r.saving > 0 ? <span className={r.perc_saving > 10 ? 'text-green-600 font-semibold' : ''}>{fmtPct(r.perc_saving)}</span> : '—'}
                    </td>
                    <td className="py-2.5 px-3 text-right tabular-nums text-gray-400">{DOC_NEG.has(r.alfa_documento) ? fmtNum(r.n_doc_neg) : '—'}</td>
                    <td className="py-2.5 px-3 text-right tabular-nums">{DOC_NEG.has(r.alfa_documento) ? fmtNum(r.n_negoziati) : '—'}</td>
                    <td className="py-2.5 px-3 text-right tabular-nums">
                      {DOC_NEG.has(r.alfa_documento) && r.n_doc_neg > 0
                        ? <span className={r.perc_negoziati > 50 ? 'text-blue-600 font-semibold' : ''}>{fmtPct(r.perc_negoziati)}</span>
                        : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="border-t-2 border-gray-200 bg-gray-50 font-semibold">
                  <td className="py-2.5 px-3 text-xs font-bold text-gray-700 uppercase" colSpan={2}>Totale</td>
                  <td className="py-2.5 px-3 text-right">{fmtNum(tot.righe)}</td>
                  <td className="py-2.5 px-3 text-right text-gray-500">{fmtEur(tot.listino)}</td>
                  <td className="py-2.5 px-3 text-right">{fmtEur(tot.impegnato)}</td>
                  <td className="py-2.5 px-3 text-right text-green-700">{fmtEur(tot.saving)}</td>
                  <td className="py-2.5 px-3 text-right">{fmtPct(tot.listino ? tot.saving/tot.listino*100 : 0)}</td>
                  <td className="py-2.5 px-3 text-right">{fmtNum(tot.doc_neg)}</td>
                  <td className="py-2.5 px-3 text-right">{fmtNum(tot.neg)}</td>
                  <td className="py-2.5 px-3 text-right">{fmtPct(tot.doc_neg ? tot.neg/tot.doc_neg*100 : 0)}</td>
                </tr>
              </tfoot>
            </table>
          </div>
        )}
      </div>

      {/* Legenda */}
      <div className="card">
        <SectionTitle>Legenda Codici Documento</SectionTitle>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-2">
          {Object.entries(ALFA_DESC).map(([alfa, desc]) => (
            <div key={alfa} className="flex items-center gap-3 py-1.5 border-b border-gray-50">
              <span className="w-14 font-mono font-bold text-sm text-right flex-shrink-0" style={{ color: colorOf(alfa) }}>{alfa}</span>
              <span className="text-xs text-gray-600 flex-1">{desc}</span>
              {DOC_NEG.has(alfa) && (
                <span className="text-[9px] font-bold text-blue-500 bg-blue-50 border border-blue-100 px-1 rounded flex-shrink-0">NEG.</span>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
