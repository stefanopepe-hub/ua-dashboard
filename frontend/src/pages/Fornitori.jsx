import { useState, useEffect } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, COLORS } from '../utils/fmt'
import { LoadingBox, ErrorBox, SectionTitle, KpiCard } from '../components/UI'

export default function Fornitori() {
  const { anni, defaultAnno } = useAnni()
  const [anno, setAnno]     = useState('')
  const [strRic, setStrRic] = useState('')

  useEffect(() => {
    if (!anno && defaultAnno) setAnno(String(defaultAnno))
  }, [defaultAnno])

  const ready = !!anno

  const { data: pareto,        loading: l1, error: e1 } = useKpi(
    () => ready ? api.pareto({ anno, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )
  const { data: topFornitori,  loading: l2, error: e2 } = useKpi(
    () => ready ? api.topFornitori({ anno, per: 'impegnato', limit: 20, str_ric: strRic }) : Promise.resolve([]),
    [anno, strRic]
  )

  const error = e1 || e2
  const soglia80    = pareto?.find(r => r.cum_perc >= 80)?.rank
  const soglia50    = pareto?.find(r => r.cum_perc >= 50)?.rank
  const totFornitori = pareto?.length || 0
  const paretoChart  = (pareto || []).slice(0, 80).map(r => ({
    rank: r.rank,
    '% Cumulata': r.cum_perc,
  }))

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-2xl font-bold text-gray-900">Analisi Fornitori</h1>
        <div className="flex gap-3">
          <select value={anno} onChange={e => setAnno(e.target.value)}
            className="filter-select font-semibold text-telethon-blue border-2 border-telethon-blue">
            <option value="">Seleziona anno…</option>
            {anni.map(a => <option key={a} value={String(a)}>{a}</option>)}
          </select>
          <select value={strRic} onChange={e => setStrRic(e.target.value)} className="filter-select">
            <option value="">Ricerca + Struttura</option>
            <option value="RICERCA">Solo Ricerca</option>
            <option value="STRUTTURA">Solo Struttura</option>
          </select>
        </div>
      </div>

      {!ready && (
        <div className="bg-blue-50 border border-blue-100 rounded-xl px-4 py-3 text-sm text-blue-700">
          Seleziona un anno per visualizzare l'analisi fornitori.
        </div>
      )}

      {error && <ErrorBox message={error} />}

      {/* KPI Pareto */}
      {ready && totFornitori > 0 && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KpiCard label="FORNITORI TOTALI"   value={fmtNum(totFornitori)} sub="nel periodo" color="blue" />
          <KpiCard label="COPRONO 50% SPESA"  value={fmtNum(soglia50)}    sub="fornitori" color="orange" />
          <KpiCard label="COPRONO 80% SPESA"  value={fmtNum(soglia80)}    sub="regola Pareto" color="red" />
          <KpiCard label="TAIL FORNITORI"     value={fmtNum(totFornitori - (soglia80 || 0))} sub="restante 20%" color="gray" />
        </div>
      )}

      {/* Curva Pareto */}
      <div className="card">
        <SectionTitle>Curva Pareto — Concentrazione della Spesa — {anno}</SectionTitle>
        <p className="text-xs text-gray-500 mb-3">
          % cumulata dell'impegnato in funzione del numero di fornitori (ordinati per volume decrescente)
        </p>
        {!ready ? null : l1 ? <LoadingBox /> : paretoChart.length === 0 ? (
          <p className="text-sm text-gray-400 text-center py-8">Nessun dato per il periodo selezionato</p>
        ) : (
          <>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={paretoChart} margin={{ top: 4, right: 8, left: 0, bottom: 16 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                <XAxis dataKey="rank" tick={{ fontSize: 11 }}
                  label={{ value: 'N° Fornitori', position: 'insideBottom', offset: -8, fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} unit="%" domain={[0, 100]} />
                <Tooltip formatter={v => [fmtPct(v), 'Spesa cumulata']} labelFormatter={v => `Fornitore #${v}`} />
                <Line type="monotone" dataKey="% Cumulata" stroke={COLORS.blue} strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
            {soglia80 && (
              <p className="text-xs text-gray-500 mt-2">
                ⚡ <strong>{soglia80} fornitori</strong> ({fmtPct(soglia80 / totFornitori * 100)} del parco)
                coprono l'80% della spesa totale.
              </p>
            )}
          </>
        )}
      </div>

      {/* Top 20 */}
      <div className="card">
        <SectionTitle>Top 20 Fornitori per Volume Acquistato — {anno}</SectionTitle>
        {!ready ? null : l2 ? <LoadingBox /> : (topFornitori || []).length === 0 ? (
          <p className="text-sm text-gray-400 text-center py-8">Nessun dato disponibile</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100">
                {['#', 'Fornitore', 'Listino', 'Impegnato', 'Saving', '% Saving', 'N°', 'Albo'].map(h => (
                  <th key={h} className={`py-2 px-3 text-xs font-semibold text-gray-500 uppercase
                    ${['#','Fornitore'].includes(h) ? 'text-left' : 'text-right'}`}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(topFornitori || []).map((r, i) => (
                <tr key={i} className="border-b border-gray-50 hover:bg-gray-50 transition-colors">
                  <td className="py-2 px-3 text-gray-400 text-xs">{i + 1}</td>
                  <td className="py-2.5 px-3 font-medium" style={{minWidth:'200px', maxWidth:'300px'}}>
                    <span className="block truncate" title={r.ragione_sociale}>{r.ragione_sociale}</span>
                  </td>
                  <td className="py-2.5 px-3 text-right tabular-nums text-gray-500 whitespace-nowrap">{fmtEur(r.listino)}</td>
                  <td className="py-2.5 px-3 text-right tabular-nums font-medium whitespace-nowrap">{fmtEur(r.impegnato)}</td>
                  <td className="py-2.5 px-3 text-right tabular-nums text-green-700 whitespace-nowrap">{fmtEur(r.saving)}</td>
                  <td className="py-2.5 px-3 text-right tabular-nums whitespace-nowrap">{fmtPct(r.perc_saving)}</td>
                  <td className="py-2.5 px-3 text-right whitespace-nowrap">{fmtNum(r.n_righe)}</td>
                  <td className="py-2.5 px-3 text-center whitespace-nowrap">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold
                      ${r.albo ? 'bg-green-50 text-green-700 border border-green-200' : 'bg-gray-50 text-gray-500 border border-gray-200'}`}>
                      {r.albo ? '✓ Accreditato' : 'Non accreditato'}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
