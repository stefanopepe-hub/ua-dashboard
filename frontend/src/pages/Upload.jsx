import { useState, useRef } from 'react'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { Badge } from '../components/UI'
import {
  Upload as UploadIcon, Trash2, CheckCircle, AlertCircle,
  Loader2, Download, AlertTriangle, Info, ChevronDown, ChevronUp,
} from 'lucide-react'

const CDC_OPTIONS = ['GD','TIGEM','TIGET','FT','STRUTTURA']

const CONFIDENCE_CONFIG = {
  high:   { cls: 'bg-green-100 text-green-700',  label: 'Alta', icon: CheckCircle },
  medium: { cls: 'bg-amber-100 text-amber-700',  label: 'Media', icon: AlertTriangle },
  low:    { cls: 'bg-red-100 text-red-700',      label: 'Bassa', icon: AlertCircle },
}

// ── Smart Upload Card (preview + import unificati) ─────────────
function SmartUploadCard({ tipo, label, color, desc, onSuccess }) {
  const [file, setFile]         = useState(null)
  const [cdc, setCdc]           = useState('')
  const [status, setStatus]     = useState(null) // null|inspecting|ready|importing|ok|error
  const [inspection, setInspect]= useState(null) // risultato /inspect
  const [msg, setMsg]           = useState('')
  const [showDetail, setShowDetail] = useState(false)
  const inputRef = useRef()

  const borderCls = { blue:'border-telethon-blue', orange:'border-orange-400', red:'border-telethon-red' }[color]
  const bgCls     = { blue:'bg-telethon-lightblue', orange:'bg-orange-50', red:'bg-red-50' }[color]
  const textCls   = { blue:'text-telethon-blue', orange:'text-orange-600', red:'text-telethon-red' }[color]

  async function handleSelect(e) {
    const f = e.target.files?.[0]
    if (!f) return
    setFile(f)
    setStatus('inspecting')
    setMsg('')
    setInspect(null)
    try {
      const result = await api.inspectFile(f)
      setInspect(result)
      setStatus('ready')
    } catch (err) {
      setStatus('error')
      setMsg(`Ispezione fallita: ${err.message}`)
    }
  }

  function handleRemove() {
    setFile(null)
    setStatus(null)
    setMsg('')
    setInspect(null)
    if (inputRef.current) inputRef.current.value = ''
  }

  async function handleImport() {
    if (!file || status === 'importing') return
    setStatus('importing')
    setMsg('')
    try {
      let data
      if (tipo === 'saving') data = await api.uploadSaving(file, cdc || null)
      else if (tipo === 'risorse') data = await api.uploadRisorse(file)
      else if (tipo === 'tempi') data = await api.uploadTempi(file)
      else if (tipo === 'nc') data = await api.uploadNc(file)

      const righe = data.rows_inserted ?? data.rows ?? 0
      setStatus('ok')
      setMsg(`✓ ${righe.toLocaleString('it-IT')} righe importate`)
      if (data.warnings?.length > 0) {
        setMsg(prev => prev + `. Avvisi: ${data.warnings[0]}`)
      }
      setFile(null)
      setInspect(null)
      if (inputRef.current) inputRef.current.value = ''
      onSuccess?.()
    } catch (e) {
      setStatus('error')
      setMsg(e.message)
    }
  }

  const conf = inspection?.overall_confidence
  const confConf = CONFIDENCE_CONFIG[conf] || null

  return (
    <div className={`bg-white rounded-2xl border border-gray-100 shadow-sm p-5 border-l-4 ${borderCls}`}>
      <div className="font-semibold text-gray-900 text-sm mb-0.5">{label}</div>
      <div className="text-xs text-gray-400 mb-3">{desc}</div>

      {tipo === 'saving' && (
        <div className="mb-3">
          <label className="text-xs text-gray-500 block mb-1">CDC (opzionale — sovrascrive il file):</label>
          <select value={cdc} onChange={e => setCdc(e.target.value)}
            className="w-full text-xs border border-gray-200 rounded-xl px-2.5 py-1.5 bg-white">
            <option value="">Rileva automaticamente dal file</option>
            {CDC_OPTIONS.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
      )}

      {/* File selezionato */}
      {file && (
        <div className="bg-gray-50 rounded-xl px-3 py-2 mb-3 text-xs">
          <div className="flex items-center justify-between">
            <span className="text-gray-700 truncate max-w-[180px]" title={file.name}>{file.name}</span>
            <div className="flex items-center gap-2 ml-2">
              <span className="text-gray-400">{(file.size / 1024).toFixed(0)} KB</span>
              {status !== 'importing' && (
                <button onClick={handleRemove} className="text-gray-400 hover:text-red-500 transition-colors">
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              )}
            </div>
          </div>

          {/* Inspection result */}
          {status === 'inspecting' && (
            <div className="flex items-center gap-1.5 mt-2 text-gray-500">
              <Loader2 className="h-3 w-3 animate-spin" /> Analisi in corso…
            </div>
          )}

          {inspection && status === 'ready' && (
            <div className="mt-2 space-y-1.5">
              {/* Family + confidence */}
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-gray-600 font-medium">{inspection.family_label}</span>
                {confConf && (
                  <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold ${confConf.cls}`}>
                    Confidenza {confConf.label} ({Math.round(inspection.overall_score * 100)}%)
                  </span>
                )}
              </div>

              {/* Available analyses */}
              {inspection.available_analyses?.length > 0 && (
                <div className="text-green-600 flex items-start gap-1">
                  <CheckCircle className="h-3 w-3 mt-0.5 flex-shrink-0" />
                  <span>Analisi disponibili: {inspection.available_analyses.slice(0, 3).join(', ')}{inspection.available_analyses.length > 3 ? ` +${inspection.available_analyses.length - 3}` : ''}</span>
                </div>
              )}

              {/* Warnings */}
              {inspection.warnings?.slice(0, 2).map((w, i) => (
                <div key={i} className="text-amber-600 flex items-start gap-1">
                  <AlertTriangle className="h-3 w-3 mt-0.5 flex-shrink-0" />
                  <span>{w}</span>
                </div>
              ))}

              {/* Blocked */}
              {inspection.blocked_analyses?.filter(b => b.severity === 'critical').length > 0 && (
                <div className="text-red-600 flex items-start gap-1">
                  <AlertCircle className="h-3 w-3 mt-0.5 flex-shrink-0" />
                  <span>{inspection.blocked_analyses.filter(b => b.severity === 'critical').length} analisi bloccate</span>
                </div>
              )}

              {/* Toggle detail */}
              <button onClick={() => setShowDetail(d => !d)}
                className="text-gray-400 hover:text-gray-600 flex items-center gap-1 text-[10px]">
                {showDetail ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
                {showDetail ? 'Nascondi dettagli' : `Vedi ${inspection.mapped_fields?.length || 0} colonne rilevate`}
              </button>

              {showDetail && (
                <div className="bg-white border border-gray-100 rounded-lg p-2 mt-1 max-h-40 overflow-y-auto">
                  {inspection.mapped_fields?.map(f => (
                    <div key={f.canonical} className="flex items-center gap-2 py-0.5 text-[10px]">
                      <div className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                        f.confidence >= 0.9 ? 'bg-green-400' :
                        f.confidence >= 0.7 ? 'bg-amber-400' : 'bg-red-400'
                      }`} />
                      <span className="text-gray-500 w-24 truncate" title={f.canonical}>{f.canonical}</span>
                      <span className="text-gray-400">←</span>
                      <span className="text-gray-700 font-medium truncate" title={f.source_column}>{f.source_column}</span>
                      <span className="text-gray-300 ml-auto">{Math.round(f.confidence * 100)}%</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Messaggio risultato */}
      {msg && (
        <div className={`flex items-start gap-2 text-xs mb-3 ${status === 'ok' ? 'text-green-700' : 'text-red-700'}`}>
          {status === 'ok'
            ? <CheckCircle className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />
            : <AlertCircle className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />}
          <span className="break-words">{msg}</span>
        </div>
      )}

      {/* Input file + bottoni */}
      <input ref={inputRef} type="file" accept=".xlsx,.xls" className="hidden" onChange={handleSelect} />

      <div className="flex items-center gap-2 flex-wrap">
        <button
          onClick={() => { if (inputRef.current) { inputRef.current.value = ''; inputRef.current.click() } }}
          disabled={status === 'importing' || status === 'inspecting'}
          className={`btn-ghost text-xs ${bgCls} ${textCls} hover:opacity-80`}>
          <UploadIcon className="h-3.5 w-3.5" />
          {file ? 'Cambia file' : 'Seleziona file Excel'}
        </button>

        {/* Importa — appare solo con file pronto */}
        {file && status === 'ready' && !inspection?.is_blocked && (
          <button onClick={handleImport} disabled={status === 'importing'}
            className="btn-primary text-xs">
            <CheckCircle className="h-3.5 w-3.5" />
            Importa
          </button>
        )}

        {/* File bloccato */}
        {file && status === 'ready' && inspection?.is_blocked && (
          <span className="text-xs text-red-600 flex items-center gap-1">
            <AlertCircle className="h-3.5 w-3.5" />
            File non importabile — campi critici mancanti
          </span>
        )}

        {status === 'importing' && (
          <span className="text-xs text-gray-500 flex items-center gap-1">
            <Loader2 className="h-3.5 w-3.5 animate-spin" /> Importazione…
          </span>
        )}
      </div>
    </div>
  )
}

// ── Export Panel ──────────────────────────────────────────────
function ExportPanel({ anni }) {
  const [anno, setAnno] = useState('')
  const [cdc, setCdc]   = useState('')

  return (
    <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-5">
      <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
        <Download className="h-4 w-4 text-telethon-blue" /> Export Dati
      </h3>
      <div className="flex flex-wrap gap-3 mb-4">
        <select value={anno} onChange={e => setAnno(e.target.value)} className="filter-select">
          <option value="">Tutti gli anni</option>
          {(anni || []).map(a => <option key={a} value={String(a)}>{a}</option>)}
        </select>
        <select value={cdc} onChange={e => setCdc(e.target.value)} className="filter-select">
          <option value="">Tutti i CDC</option>
          {CDC_OPTIONS.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
      </div>
      <button
        onClick={() => api.exportExcel({ filtri: { anno, cdc }, sezioni: ['riepilogo','mensile','cdc','alfa_documento','top_fornitori'] })}
        className="btn-secondary text-xs">
        <Download className="h-3.5 w-3.5" /> Saving Excel
      </button>
    </div>
  )
}

// ── Pagina principale ─────────────────────────────────────────
export default function Upload() {
  const [refresh, setRefresh] = useState(0)
  const { data: logData, loading } = useKpi(() => api.uploadLog(), [refresh])
  const { data: anniData } = useKpi(() => api.anni(), [])
  const anni = (anniData || []).map(r => r.anno)

  async function handleDelete(id) {
    if (!confirm('Eliminare questo upload? L\'operazione è irreversibile.')) return
    await api.deleteUpload(id)
    setRefresh(r => r + 1)
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="page-title">Carica Dati</h1>
        <p className="page-subtitle">
          Il sistema analizza automaticamente il file e rileva colonne e tipologia.
          Dopo l'analisi puoi vedere cosa verrà importato prima di confermare.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <SmartUploadCard tipo="saving"  label="File Saving / Ordini" color="blue"
          desc="Estratto Alyante con ordini, importi, saving. Il sistema rileva automaticamente il foglio e le colonne."
          onSuccess={() => setRefresh(r => r + 1)} />
        <SmartUploadCard tipo="risorse" label="File Risorse / Team" color="orange"
          desc="File con dati Risorsa, Pratiche Gestite, Mese, Saving Generato, ecc. Rilevamento automatico."
          onSuccess={() => setRefresh(r => r + 1)} />
        <SmartUploadCard tipo="tempi"   label="Tempi Attraversamento" color="blue"
          desc="File con colonne Year_Month, Total_Days, Days_Purchasing, ecc."
          onSuccess={() => setRefresh(r => r + 1)} />
        <SmartUploadCard tipo="nc"      label="Non Conformità" color="red"
          desc="File con dati Non Conformità, Fornitore, Data Origine, Delta Giorni."
          onSuccess={() => setRefresh(r => r + 1)} />
      </div>

      <ExportPanel anni={anni} />

      {/* Storico */}
      <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-5">
        <h3 className="section-title">Storico Caricamenti</h3>
        {loading ? (
          <div className="flex items-center gap-2 text-sm text-gray-400 py-4">
            <Loader2 className="h-4 w-4 animate-spin" /> Caricamento…
          </div>
        ) : (logData || []).length === 0 ? (
          <p className="text-sm text-gray-400 py-4">Nessun caricamento effettuato</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="data-table">
              <thead>
                <tr>
                  <th>File</th>
                  <th>Tipo</th>
                  <th>Famiglia</th>
                  <th>Confidenza</th>
                  <th className="right">Righe</th>
                  <th>Data</th>
                  <th>Status</th>
                  <th className="right"></th>
                </tr>
              </thead>
              <tbody>
                {(logData || []).map(row => (
                  <tr key={row.id}>
                    <td className="max-w-[200px]">
                      <span className="block truncate text-xs text-gray-700" title={row.filename}>
                        {row.filename}
                      </span>
                    </td>
                    <td>
                      <Badge color={{ saving:'blue', tempi:'orange', nc:'red', risorse:'green' }[row.tipo] || 'gray'}>
                        {row.tipo}
                      </Badge>
                    </td>
                    <td className="text-xs text-gray-500">
                      {row.family_detected || '—'}
                    </td>
                    <td>
                      {row.mapping_confidence ? (
                        <span className={`badge ${
                          row.mapping_confidence === 'high' ? 'badge-green' :
                          row.mapping_confidence === 'medium' ? 'badge-amber' : 'badge-red'
                        }`}>
                          {row.mapping_confidence}
                          {row.mapping_score ? ` (${Math.round(row.mapping_score * 100)}%)` : ''}
                        </span>
                      ) : '—'}
                    </td>
                    <td className="text-right tabular font-medium">
                      {row.rows_inserted != null ? row.rows_inserted.toLocaleString('it-IT') : '—'}
                    </td>
                    <td className="text-xs text-gray-500 whitespace-nowrap">
                      {new Date(row.upload_date).toLocaleString('it-IT')}
                    </td>
                    <td>
                      <Badge color={row.status === 'ok' ? 'green' : 'red'} dot>{row.status || 'ok'}</Badge>
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
