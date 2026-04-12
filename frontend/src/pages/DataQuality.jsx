/**
 * DataQuality.jsx — Enterprise Data Quality Center v9.1
 * FIX: safeArray garantisce che uploads sia sempre un array
 */
import { useState } from 'react'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtNum, fmtDate } from '../utils/fmt'
import {
  PageHeader, KpiCard, Badge, SectionTitle,
  EmptyState, InfoBox, ErrorBox, LoadingBox,
} from '../components/UI'
import {
  CheckCircle, AlertCircle, AlertTriangle,
  Database, FileText, Trash2, RefreshCw, ChevronDown, ChevronUp,
} from 'lucide-react'

function ConfBadge({ score }) {
  if (!score && score !== 0) return null
  const pct = Math.round(score * 100)
  const [cls, label] =
    pct >= 85 ? ['badge-green', 'Alta']  :
    pct >= 60 ? ['badge-amber', 'Media'] :
                ['badge-red',   'Bassa']
  return <span className={`badge ${cls} font-mono text-[10px]`}>{pct}% {label}</span>
}

function AnalyticsPill({ label, available }) {
  return (
    <span className={`inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-full font-medium ${
      available
        ? 'bg-green-50 text-green-700 border border-green-200'
        : 'bg-gray-50 text-gray-400 border border-gray-200 line-through'
    }`}>
      {available ? <CheckCircle className="h-2.5 w-2.5"/> : <AlertCircle className="h-2.5 w-2.5"/>}
      {label}
    </span>
  )
}

function UploadRow({ row, onDelete }) {
  const [open, setOpen] = useState(false)

  // FIX: garantisce array anche se il campo è null/stringa
  const safeArr = v => Array.isArray(v) ? v : []
  const available  = safeArr(row.available_analyses)
  const blocked    = safeArr(row.blocked_analyses)
  const warnings   = safeArr(row.warnings)
  const blockCrit  = blocked.filter(b => b?.severity === 'critical')
  const blockOpt   = blocked.filter(b => b?.severity !== 'critical')

  return (
    <>
      <tr className="border-b border-gray-50 hover:bg-gray-50/60 transition-colors">
        <td className="py-2.5 px-3">
          <div className="flex items-center gap-2">
            <FileText className="h-3.5 w-3.5 text-gray-300 flex-shrink-0"/>
            <span className="text-xs text-gray-700 font-medium truncate max-w-[180px]"
                  title={row.filename}>{row.filename}</span>
          </div>
        </td>
        <td className="py-2.5 px-3">
          <Badge color={{
            savings: 'blue', saving: 'blue', risorse: 'green',
            nc: 'red', tempi: 'orange', non_conformita: 'red',
          }[row.family_detected || row.tipo] || 'gray'}>
            {row.family_detected || row.tipo || '—'}
          </Badge>
        </td>
        <td className="py-2.5 px-3 text-xs text-gray-500 font-mono">
          {row.sheet_used ? `"${row.sheet_used}"` : '—'}
          {row.header_row != null && <span className="text-gray-300 ml-1">[R{row.header_row}]</span>}
        </td>
        <td className="py-2.5 px-3"><ConfBadge score={row.mapping_score}/></td>
        <td className="py-2.5 px-3 text-right tabular-nums text-sm font-semibold">
          {row.rows_inserted != null ? fmtNum(row.rows_inserted) : '—'}
        </td>
        <td className="py-2.5 px-3 text-xs text-gray-400 whitespace-nowrap">
          {fmtDate(row.upload_date)}
        </td>
        <td className="py-2.5 px-3">
          <Badge color={row.status === 'ok' || !row.status ? 'green' : 'red'} dot>
            {row.status || 'ok'}
          </Badge>
        </td>
        <td className="py-2.5 px-3 text-right">
          <div className="flex items-center justify-end gap-1">
            <button onClick={() => setOpen(o => !o)} className="btn-ghost p-1.5">
              {open ? <ChevronUp className="h-3.5 w-3.5"/> : <ChevronDown className="h-3.5 w-3.5"/>}
            </button>
            <button onClick={() => onDelete(row.id)} className="btn-danger p-1.5">
              <Trash2 className="h-3.5 w-3.5"/>
            </button>
          </div>
        </td>
      </tr>

      {open && (
        <tr className="bg-gray-50/80 border-b border-gray-100">
          <td colSpan={8} className="px-4 py-4">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2 flex items-center gap-1">
                  <CheckCircle className="h-3 w-3 text-green-500"/>
                  Analisi attive ({available.length})
                </div>
                <div className="flex flex-wrap gap-1">
                  {available.length > 0
                    ? available.map(a => <AnalyticsPill key={a} label={a} available/>)
                    : <span className="text-xs text-gray-400">Nessuna</span>}
                </div>
              </div>
              {blockCrit.length > 0 && (
                <div>
                  <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2 flex items-center gap-1">
                    <AlertCircle className="h-3 w-3 text-red-500"/>
                    Bloccate ({blockCrit.length})
                  </div>
                  <div className="space-y-1">
                    {blockCrit.slice(0, 5).map((b, i) => (
                      <div key={i} className="text-xs text-red-700 bg-red-50 border border-red-100 rounded-lg px-2 py-1">
                        <span className="font-medium">{b.analysis}:</span>{' '}
                        <span className="text-red-600">{b.reason}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              <div>
                {blockOpt.length > 0 && (
                  <>
                    <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2 flex items-center gap-1">
                      <AlertTriangle className="h-3 w-3 text-amber-500"/>
                      Parziali ({blockOpt.length})
                    </div>
                    <div className="space-y-1 mb-3">
                      {blockOpt.slice(0, 3).map((b, i) => (
                        <div key={i} className="text-xs text-amber-700">
                          <span className="font-medium">{b.analysis}:</span> {b.reason}
                        </div>
                      ))}
                    </div>
                  </>
                )}
                {warnings.length > 0 && (
                  <>
                    <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-1">Avvisi</div>
                    {warnings.slice(0, 3).map((w, i) => (
                      <div key={i} className="text-xs text-gray-500">{w}</div>
                    ))}
                  </>
                )}
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

export default function DataQuality() {
  const [refresh, setRefresh] = useState(0)
  const { data: logData, loading, error } = useKpi(() => api.uploadLog(), [refresh])
  const { anni } = useAnni()

  async function handleDelete(id) {
    if (!confirm('Eliminare questo upload e i dati correlati?')) return
    await api.deleteUpload(id)
    setRefresh(r => r + 1)
  }

  // FIX: garantisce sempre array
  const uploads = Array.isArray(logData) ? logData : []
  const saving  = uploads.filter(u => u.tipo === 'savings' || u.tipo === 'saving')
  const risorse = uploads.filter(
  u =>
    u.tipo === 'risorse' ||
    u.target_domain === 'risorse'
)
  const nc      = uploads.filter(u => u.tipo === 'nc' || u.tipo === 'non_conformita')
  const tempi   = uploads.filter(u => u.tipo === 'tempi')

  const totalRows  = saving.reduce((s, u) => s + (u.rows_inserted || 0), 0)
  const allOk      = uploads.every(u => !u.status || u.status === 'ok' || u.status === 'partial')

  const ANALISI_CATALOG = [
    { label: 'Dashboard Riepilogo',   available: saving.length > 0 },
    { label: 'Saving & Ordini',       available: saving.length > 0 },
    { label: 'Analisi Fornitori',     available: saving.length > 0 },
    { label: 'Tipologie Documentali', available: saving.length > 0 },
    { label: 'Confronto YoY',         available: anni.length >= 2,
      note: anni.length < 2 ? `Hai ${anni.length} anno/i, ne servono 2` : undefined },
    { label: 'Tempi Attraversamento', available: tempi.length > 0,
      note: tempi.length === 0 ? 'Carica file tempi' : undefined },
    { label: 'Non Conformità',        available: nc.length > 0,
      note: nc.length === 0 ? 'Carica file NC' : undefined },
    { label: 'Analytics Risorse',     available: risorse.length > 0,
      note: risorse.length === 0 ? 'Carica file risorse' : undefined },
  ]

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Quality Center"
        subtitle="Stato dei caricamenti, colonne riconosciute, analisi disponibili"
        actions={
          <button onClick={() => setRefresh(r => r + 1)} className="btn-ghost text-xs">
            <RefreshCw className="h-3.5 w-3.5"/> Aggiorna
          </button>
        }
      />

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard label="File Caricati"    value={fmtNum(uploads.length)}        color="blue"/>
        <KpiCard label="Righe Saving"     value={fmtNum(totalRows)}             color="blue" sub={`${saving.length} file`}/>
        <KpiCard label="Anni Disponibili" value={anni.length > 0 ? anni.join(', ') : '—'}
          color={anni.length >= 2 ? 'green' : 'orange'}
          sub={anni.length >= 2 ? 'YoY abilitato' : 'Carica 2° anno per YoY'}/>
        <KpiCard label="Stato Sistema"    value={allOk ? 'Operativo' : 'Attenzione'}
          color={allOk ? 'green' : 'orange'}/>
      </div>

      <div className="card">
        <SectionTitle>Analisi Disponibili</SectionTitle>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {ANALISI_CATALOG.map(item => (
            <div key={item.label}
              className={`flex items-start gap-3 p-3 rounded-xl border ${
                item.available ? 'bg-green-50/70 border-green-100' : 'bg-gray-50 border-gray-100'
              }`}>
              {item.available
                ? <CheckCircle className="h-4 w-4 text-green-500 mt-0.5 flex-shrink-0"/>
                : <AlertCircle className="h-4 w-4 text-gray-300 mt-0.5 flex-shrink-0"/>}
              <div>
                <div className={`text-sm font-semibold ${item.available ? 'text-gray-900' : 'text-gray-400'}`}>
                  {item.label}
                </div>
                {!item.available && item.note && (
                  <div className="text-xs text-amber-600 mt-0.5">{item.note}</div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {anni.length > 0 && (
        <div className={`card border-l-4 ${anni.length >= 2 ? 'border-l-green-400' : 'border-l-amber-400'}`}>
          <div className="flex items-start gap-3">
            <Database className={`h-5 w-5 mt-0.5 flex-shrink-0 ${anni.length >= 2 ? 'text-green-500' : 'text-amber-500'}`}/>
            <div>
              <div className="text-sm font-semibold text-gray-900 mb-0.5">
                {anni.length >= 2 ? '✅ Confronto Year-over-Year abilitato' : '⚠️ Un solo anno disponibile'}
              </div>
              <div className="text-xs text-gray-500">
                {anni.length >= 2
                  ? `Anni disponibili: ${anni.join(', ')} — YoY attivo su tutte le pagine analytics.`
                  : `Anno disponibile: ${anni[0]}. Carica il file saving ${anni[0]+1} per YoY.`}
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="card">
        <SectionTitle>Storico Caricamenti — Dettaglio Mapping</SectionTitle>
        <InfoBox>
          Clicca sulla freccia di ogni upload per vedere le analisi abilitate e quelle bloccate.
        </InfoBox>
        {error && <ErrorBox message={error}/>}
        {loading ? <LoadingBox/> : uploads.length === 0 ? (
          <EmptyState
            title="Nessun file caricato"
            message="Vai in 'Carica Dati' per importare i file Excel."
          />
        ) : (
          <div className="overflow-x-auto mt-4">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b-2 border-gray-100">
                  {['File','Famiglia','Foglio','Confidenza','Righe','Data','Stato',''].map(h => (
                    <th key={h} className={`py-2 px-3 text-xs font-bold text-gray-400 uppercase ${h === 'Righe' ? 'text-right' : 'text-left'}`}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {uploads.map(row => (
                  <UploadRow key={row.id} row={row} onDelete={handleDelete}/>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
