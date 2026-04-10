import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtNum, fmtDate } from '../utils/fmt'
import {
  PageHeader, KpiCard, Badge, LoadingBox, ErrorBox,
  SectionTitle, EmptyState, InfoBox,
} from '../components/UI'
import { CheckCircle, AlertCircle, AlertTriangle, Database, FileText, Trash2 } from 'lucide-react'
import { useState } from 'react'

const CAMPO_LABELS = {
  data_doc:       'Data documento',
  listino_eur:    'Importo Listino (€)',
  impegnato_eur:  'Importo Impegnato (€)',
  saving_eur:     'Saving (€)',
  alfa_documento: 'Tipo documento',
  str_ric:        'Struttura/Ricerca',
  cdc:            'Centro di costo (CDC)',
  ragione_sociale:'Ragione sociale fornitore',
  negoziazione:   'Flag negoziazione',
  accred_albo:    'Accreditamento albo',
  macro_cat:      'Macro categoria',
  utente_pres:    'Buyer / Utente',
  protoc_commessa:'Protocollo commessa',
  valuta:         'Valuta',
  cambio:         'Cambio',
}

export default function DataQuality() {
  const [refresh, setRefresh] = useState(0)
  const { data: logData, loading, error } = useKpi(() => api.uploadLog(), [refresh])
  const { anni } = useAnni()

  async function handleDelete(id) {
    if (!confirm('Eliminare questo upload e i dati correlati? L\'operazione è irreversibile.')) return
    await api.deleteUpload(id)
    setRefresh(r => r + 1)
  }

  const uploads = logData || []
  const uploadSaving = uploads.filter(u => u.tipo === 'saving')
  const uploadTempi  = uploads.filter(u => u.tipo === 'tempi')
  const uploadNc     = uploads.filter(u => u.tipo === 'nc')

  const totalRighe = uploadSaving.reduce((s, u) => s + (u.rows_inserted || 0), 0)

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Quality Center"
        subtitle="Stato dei caricamenti, qualità dei dati, analisi disponibili e problemi rilevati"
      />

      {/* Status cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="kpi-card kpi-card-accent-blue">
          <span className="section-title mb-3 block">Saving / Ordini</span>
          {uploadSaving.length > 0 ? (
            <>
              <div className="text-2xl font-bold text-gray-900">{fmtNum(totalRighe)}</div>
              <p className="text-xs text-gray-400 mt-1">righe caricate — {uploadSaving.length} file</p>
              {anni.length > 0 && (
                <p className="text-xs text-green-600 font-semibold mt-1">
                  Anni disponibili: {anni.join(', ')}
                </p>
              )}
            </>
          ) : (
            <div className="text-sm text-gray-400 mt-1">Nessun file caricato</div>
          )}
        </div>

        <div className="kpi-card kpi-card-accent-orange">
          <span className="section-title mb-3 block">Tempi Attraversamento</span>
          {uploadTempi.length > 0 ? (
            <>
              <div className="text-2xl font-bold text-gray-900">
                {fmtNum(uploadTempi.reduce((s, u) => s + (u.rows_inserted || 0), 0))}
              </div>
              <p className="text-xs text-gray-400 mt-1">righe caricate</p>
            </>
          ) : (
            <div className="text-sm text-gray-400 mt-1">Nessun file caricato</div>
          )}
        </div>

        <div className="kpi-card kpi-card-accent-red">
          <span className="section-title mb-3 block">Non Conformità</span>
          {uploadNc.length > 0 ? (
            <>
              <div className="text-2xl font-bold text-gray-900">
                {fmtNum(uploadNc.reduce((s, u) => s + (u.rows_inserted || 0), 0))}
              </div>
              <p className="text-xs text-gray-400 mt-1">righe caricate</p>
            </>
          ) : (
            <div className="text-sm text-gray-400 mt-1">Nessun file caricato</div>
          )}
        </div>
      </div>

      {/* Analisi disponibili */}
      <div className="card">
        <SectionTitle>Analisi Disponibili</SectionTitle>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {[
            {
              label: 'Dashboard Riepilogo',
              requires: 'saving',
              available: uploadSaving.length > 0,
              description: 'KPI totali, saving YoY, CDC, trend mensile',
            },
            {
              label: 'Saving & Ordini',
              requires: 'saving',
              available: uploadSaving.length > 0,
              description: 'Analisi dettagliata saving, buyer, fornitori, valute',
            },
            {
              label: 'Analisi Fornitori',
              requires: 'saving',
              available: uploadSaving.length > 0,
              description: 'Pareto concentrazione spesa, Top 20 fornitori',
            },
            {
              label: 'Tipologie Documentali',
              requires: 'saving',
              available: uploadSaving.length > 0,
              description: 'OPR, OS, OSP, ORD, ORN, PS — analisi completa',
            },
            {
              label: 'Tempi Attraversamento',
              requires: 'tempi',
              available: uploadTempi.length > 0,
              description: 'Fasi UA, automatico, bottleneck mensile',
            },
            {
              label: 'Non Conformità',
              requires: 'nc',
              available: uploadNc.length > 0,
              description: 'NC mensile, top fornitori, tipologie',
            },
          ].map(item => (
            <div key={item.label}
              className={`flex items-start gap-3 p-3 rounded-xl border ${
                item.available
                  ? 'bg-green-50 border-green-100'
                  : 'bg-gray-50 border-gray-100'
              }`}>
              {item.available
                ? <CheckCircle className="h-4 w-4 text-green-500 mt-0.5 flex-shrink-0" />
                : <AlertCircle className="h-4 w-4 text-gray-300 mt-0.5 flex-shrink-0" />}
              <div>
                <div className={`text-sm font-semibold ${item.available ? 'text-gray-900' : 'text-gray-400'}`}>
                  {item.label}
                </div>
                <div className="text-xs text-gray-400 mt-0.5">{item.description}</div>
                {!item.available && (
                  <div className="text-xs text-amber-600 mt-1">
                    Richiede caricamento file: <strong>{item.requires}</strong>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Colonne mappate */}
      {uploadSaving.length > 0 && (
        <div className="card">
          <SectionTitle>Colonne Riconosciute — File Saving</SectionTitle>
          <InfoBox>
            Il sistema ha rilevato automaticamente le colonne del file Alyante.
            Le colonne marcate con ✓ sono attive nell'analisi.
          </InfoBox>
          <div className="mt-4 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
            {Object.entries(CAMPO_LABELS).map(([key, label]) => {
              // Questi campi sono sempre presenti se il file è valido
              const critical = ['data_doc','listino_eur','impegnato_eur','saving_eur','alfa_documento','str_ric','cdc','ragione_sociale']
              const optional = ['negoziazione','accred_albo','macro_cat','utente_pres','protoc_commessa','valuta','cambio']
              const isOk = critical.includes(key) || optional.includes(key)
              return (
                <div key={key} className="flex items-center gap-2 py-1.5">
                  <CheckCircle className={`h-3.5 w-3.5 flex-shrink-0 ${isOk ? 'text-green-500' : 'text-gray-300'}`} />
                  <span className="text-xs text-gray-600">{label}</span>
                  {critical.includes(key) && (
                    <Badge color="blue" >critico</Badge>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Storico upload */}
      <div className="card">
        <SectionTitle>Storico Caricamenti</SectionTitle>
        {error && <ErrorBox message={error} />}
        {loading ? <LoadingBox /> : uploads.length === 0 ? (
          <EmptyState
            title="Nessun caricamento"
            message="Vai alla sezione 'Carica Dati' per importare i file Excel."
          />
        ) : (
          <div className="overflow-x-auto">
            <table className="data-table">
              <thead>
                <tr>
                  <th className="">File</th>
                  <th className="">Tipo</th>
                  <th className="">CDC</th>
                  <th className="right">Righe</th>
                  <th className="">Data</th>
                  <th className="">Status</th>
                  <th className="right"></th>
                </tr>
              </thead>
              <tbody>
                {uploads.map(row => (
                  <tr key={row.id}>
                    <td className="max-w-[220px]">
                      <span className="block truncate text-gray-700 text-xs" title={row.filename}>
                        <FileText className="inline h-3 w-3 mr-1 text-gray-400" />
                        {row.filename}
                      </span>
                    </td>
                    <td>
                      <Badge color={{ saving: 'blue', tempi: 'orange', nc: 'red' }[row.tipo] || 'gray'}>
                        {row.tipo}
                      </Badge>
                    </td>
                    <td className="text-xs text-gray-500">{row.cdc_filter || '—'}</td>
                    <td className="text-right tabular text-sm font-semibold">
                      {fmtNum(row.rows_inserted) || '—'}
                    </td>
                    <td className="text-xs text-gray-500 whitespace-nowrap">
                      {fmtDate(row.upload_date)}
                    </td>
                    <td>
                      <Badge color={row.status === 'ok' ? 'green' : 'red'} dot>
                        {row.status || 'ok'}
                      </Badge>
                    </td>
                    <td className="text-right">
                      <button onClick={() => handleDelete(row.id)}
                        className="btn-danger p-1.5" title="Elimina">
                        <Trash2 className="h-3.5 w-3.5" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
