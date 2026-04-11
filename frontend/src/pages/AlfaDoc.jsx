import { useState, useEffect } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell, Legend, LineChart, Line,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtPct, fmtNum, COLORS } from '../utils/fmt'
import {
  FilterBar, LoadingBox, ErrorBox, SectionTitle, KpiCard,
  ChartCard, PageHeader, Badge, DataTable, EmptyState,
} from '../components/UI'

// ── Label business CORRETTE (Fondazione Telethon ETS) ────────────
const ALFA_LABELS = {
  ORN:    'Ordine Ricerca',
  ORD:    'Ordine Diretto Ricerca',
  OPR:    'Ordine Previsionale Ricerca',
  PS:     'Procedura Straordinaria',
  OS:     'Ordine Struttura',
  OSP:    'Ordine Previsionale Struttura',
  OSD:    'Ordine Diretto Struttura',
  OSDP01: 'Ordine Diretto Struttura (variante)',
}

const ALFA_AREA = {
  ORN: 'RICERCA', ORD: 'RICERCA', OPR: 'RICERCA', PS: 'RICERCA',
  OS: 'STRUTTURA', OSP: 'STRUTTURA', OSD: 'STRUTTURA', OSDP01: 'STRUTTURA',
}

const ALFA_COLORS = {
  ORN: '#0057A8', ORD: '#2563eb', OPR: '#60a5fa',
  PS:  '#7c3aed',
  OS:  '#D81E1E', OSP: '#ef4444', OSD: '#fca5a5', OSDP01: '#fecaca',
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
    listino:   rows.reduce((s, d) => s + (d.listino   || 0), 0),
    impegnato: rows.reduce((s, d) => s + (d.impegnato || 0), 0),
    saving:    rows.reduce((s, d) => s + (d.saving    || 0), 0),
    righe:     rows.reduce((s, d) => s + (d.n_righe   || 0), 0),
    neg:       rows.reduce((s, d) => s + (d.n_negoziati || 0), 0),
    doc_neg:   rows.reduce((s, d) => s + (d.n_doc_neg || 0), 0),
  }

  const colorOf = a => ALFA_COLORS[a] || '#94a3b8'
  const labelOf = a => ALFA_LABELS[a] || a

  // Grafici
  const chartLstImp = rows.map(d => ({
    name: d.alfa_documento,
    label: labelOf(d.alfa_documento),
    'Listino €K':   Math.round((d.listino   || 0) / 1000),
    'Impegnato €K': Math.round((d.impegnato || 0) / 1000),
  }))

  const chartSav = rows
    .filter(d => (d.saving || 0) > 0)
    .map(d => ({
      name:        d.alfa_documento,
      label:       labelOf(d.alfa_documento),
      'Saving €K': Math.round((d.saving || 0) / 1000),
    }))

  const chartNeg = rows
    .filter(d => DOC_NEG.has(d.alfa_documento) && (d.n_doc_neg || 0) > 0)
    .map(d => ({
      name:        d.alfa_documento,
      label:       labelOf(d.alfa_documento),
      'Totale':    d.n_doc_neg || 0,
      'Negoziati': d.n_negoziati || 0,
    }))

  // Custom tooltip per recharts
  const CustomTooltip = ({ active, payload, label }) => {
    if (!active || !payload?.length) return null
    const alfaCode = label
    return (
      <div className="bg-white border border-gray-100 rounded-xl p-3 shadow-lg text-xs">
        <div className="font-bold text-gray-900 mb-1">{alfaCode}</div>
        <div className="text-gray-500 mb-2">{labelOf(alfaCode)}</div>
        {payload.map(p => (
          <div key={p.dataKey} className="flex items-center justify-between gap-4">
            <span style={{ color: p.color }}>{p.dataKey}</span>
            <span className="font-semibold">€{p.value}K</span>
          </div>
        ))}
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Tipologie Documentali"
        subtitle="Analisi per tipo documento Alyante — con le definizioni business corrette"
      />

      <FilterBar anno={anno} setAnno={setAnno} strRic={strRic} setStrRic={setStrRic}
        cdc={cdc} setCdc={setCdc} anni={anni} />

      {error && <ErrorBox message={error} />}

      {/* KPI */}
      {loading ? <LoadingBox /> : (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KpiCard label="Listino Totale"   value={fmtEur(tot.listino)}   color="gray"   sub="prezzo di partenza" />
          <KpiCard label="Impegnato Totale" value={fmtEur(tot.impegnato)} color="blue"   sub="quanto paghiamo" />
          <KpiCard label="Saving Totale"    value={fmtEur(tot.saving)}    color="green"
            sub={`${fmtPct(tot.listino ? tot.saving/tot.listino*100 : 0)}`} />
          <KpiCard label="% Negoziati"
            value={fmtPct(tot.doc_neg ? tot.neg/tot.doc_neg*100 : 0)}
            color="orange"
            sub={`${fmtNum(tot.neg)} / ${fmtNum(tot.doc_neg)} negoziabili`} />
        </div>
      )}

      {/* Grafici */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <ChartCard title="Listino vs Impegnato per Tipo" subtitle="€K" loading={loading} empty={!rows.length}>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartLstImp} layout="vertical" margin={{ top: 4, right: 24, left: 80, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis dataKey="name" type="category" tick={{ fontSize: 11, fontFamily: 'monospace', fontWeight: 600 }} width={80} />
              <Tooltip content={<CustomTooltip />} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="Listino €K"   fill="#d1d5db" radius={[0, 3, 3, 0]} />
              <Bar dataKey="Impegnato €K" fill={COLORS.blue} radius={[0, 3, 3, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Saving per Tipo Documento" subtitle="€K" loading={loading} empty={!chartSav.length}>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartSav} layout="vertical" margin={{ top: 4, right: 24, left: 80, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis dataKey="name" type="category" tick={{ fontSize: 11, fontFamily: 'monospace', fontWeight: 600 }} width={80} />
              <Tooltip content={<CustomTooltip />} />
              <Bar dataKey="Saving €K" radius={[0, 3, 3, 0]}>
                {chartSav.map((e, i) => <Cell key={i} fill={colorOf(e.name)} />)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Negoziati */}
      {chartNeg.length > 0 && (
        <ChartCard title="Negoziati vs Totale Negoziabili" subtitle="Solo tipi su cui si può negoziare" loading={loading}>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={chartNeg} layout="vertical" margin={{ top: 4, right: 24, left: 80, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis dataKey="name" type="category" tick={{ fontSize: 11, fontFamily: 'monospace', fontWeight: 600 }} width={80} />
              <Tooltip formatter={(v, n) => [fmtNum(v), n]} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="Totale"    fill="#e5e7eb" radius={[0, 3, 3, 0]} />
              <Bar dataKey="Negoziati" fill={COLORS.orange} radius={[0, 3, 3, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* Tabella dettaglio — full width, no truncation */}
      <div className="card">
        <SectionTitle>Dettaglio per Tipologia Documentale — {anno}</SectionTitle>
        {loading ? <LoadingBox /> : rows.length === 0 ? (
          <EmptyState title="Nessun dato" message="Seleziona un anno con dati caricati" />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b-2 border-gray-100">
                  <th className="text-left py-3 px-3 text-xs font-bold text-gray-400 uppercase whitespace-nowrap">Codice</th>
                  <th className="text-left py-3 px-3 text-xs font-bold text-gray-400 uppercase whitespace-nowrap">Descrizione Business</th>
                  <th className="text-left py-3 px-3 text-xs font-bold text-gray-400 uppercase whitespace-nowrap">Area</th>
                  <th className="text-right py-3 px-3 text-xs font-bold text-gray-400 uppercase whitespace-nowrap">N° Doc.</th>
                  <th className="text-right py-3 px-3 text-xs font-bold text-gray-400 uppercase whitespace-nowrap">Listino</th>
                  <th className="text-right py-3 px-3 text-xs font-bold text-gray-400 uppercase whitespace-nowrap">Impegnato</th>
                  <th className="text-right py-3 px-3 text-xs font-bold text-gray-400 uppercase whitespace-nowrap">Saving</th>
                  <th className="text-right py-3 px-3 text-xs font-bold text-gray-400 uppercase whitespace-nowrap">% Sav.</th>
                  <th className="text-right py-3 px-3 text-xs font-bold text-gray-400 uppercase whitespace-nowrap">Negoz.</th>
                  <th className="text-right py-3 px-3 text-xs font-bold text-gray-400 uppercase whitespace-nowrap">% Neg.</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => {
                  const area = ALFA_AREA[r.alfa_documento]
                  return (
                    <tr key={i} className="border-b border-gray-50 hover:bg-gray-50/80 transition-colors">
                      <td className="py-2.5 px-3">
                        <div className="flex items-center gap-2">
                          <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: colorOf(r.alfa_documento) }} />
                          <span className="font-mono font-bold text-sm">{r.alfa_documento}</span>
                          {DOC_NEG.has(r.alfa_documento) && (
                            <span className="text-[9px] font-bold text-blue-500 bg-blue-50 border border-blue-100 px-1 rounded">NEG.</span>
                          )}
                        </div>
                      </td>
                      <td className="py-2.5 px-3 text-gray-700">{labelOf(r.alfa_documento)}</td>
                      <td className="py-2.5 px-3">
                        {area && (
                          <Badge color={area === 'RICERCA' ? 'blue' : 'orange'}>{area}</Badge>
                        )}
                      </td>
                      <td className="py-2.5 px-3 text-right tabular-nums">{fmtNum(r.n_righe)}</td>
                      <td className="py-2.5 px-3 text-right tabular-nums text-gray-500">{fmtEur(r.listino)}</td>
                      <td className="py-2.5 px-3 text-right tabular-nums font-medium">{fmtEur(r.impegnato)}</td>
                      <td className="py-2.5 px-3 text-right tabular-nums text-green-700">
                        {(r.saving || 0) > 0 ? fmtEur(r.saving) : '—'}
                      </td>
                      <td className="py-2.5 px-3 text-right tabular-nums">
                        {(r.saving || 0) > 0 ? (
                          <span className={r.perc_saving > 10 ? 'text-green-600 font-semibold' : ''}>
                            {fmtPct(r.perc_saving)}
                          </span>
                        ) : '—'}
                      </td>
                      <td className="py-2.5 px-3 text-right tabular-nums">
                        {DOC_NEG.has(r.alfa_documento) ? fmtNum(r.n_negoziati) : '—'}
                      </td>
                      <td className="py-2.5 px-3 text-right tabular-nums">
                        {DOC_NEG.has(r.alfa_documento) && r.n_doc_neg > 0 ? (
                          <span className={r.perc_negoziati > 50 ? 'text-blue-600 font-semibold' : ''}>
                            {fmtPct(r.perc_negoziati)}
                          </span>
                        ) : '—'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
              <tfoot>
                <tr className="border-t-2 border-gray-200 bg-gray-50 font-semibold">
                  <td className="py-3 px-3 text-xs font-bold text-gray-700 uppercase" colSpan={3}>Totale</td>
                  <td className="py-3 px-3 text-right tabular-nums">{fmtNum(tot.righe)}</td>
                  <td className="py-3 px-3 text-right tabular-nums text-gray-500">{fmtEur(tot.listino)}</td>
                  <td className="py-3 px-3 text-right tabular-nums">{fmtEur(tot.impegnato)}</td>
                  <td className="py-3 px-3 text-right tabular-nums text-green-700">{fmtEur(tot.saving)}</td>
                  <td className="py-3 px-3 text-right tabular-nums">
                    {fmtPct(tot.listino ? tot.saving/tot.listino*100 : 0)}
                  </td>
                  <td className="py-3 px-3 text-right tabular-nums">{fmtNum(tot.neg)}</td>
                  <td className="py-3 px-3 text-right tabular-nums">
                    {fmtPct(tot.doc_neg ? tot.neg/tot.doc_neg*100 : 0)}
                  </td>
                </tr>
              </tfoot>
            </table>
          </div>
        )}
      </div>

      {/* Legenda business */}
      <div className="card">
        <SectionTitle>Legenda Codici Documento — Definizioni Business</SectionTitle>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2">
          {Object.entries(ALFA_LABELS).map(([code, label]) => (
            <div key={code} className="flex items-center gap-3 py-2 border-b border-gray-50">
              <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: colorOf(code) }} />
              <span className="font-mono font-bold text-sm w-16 flex-shrink-0">{code}</span>
              <span className="text-sm text-gray-700">{label}</span>
              <Badge color={ALFA_AREA[code] === 'RICERCA' ? 'blue' : 'orange'} className="ml-auto flex-shrink-0">
                {ALFA_AREA[code]}
              </Badge>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
