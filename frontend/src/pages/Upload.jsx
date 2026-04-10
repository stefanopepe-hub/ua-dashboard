import { useState, useRef } from 'react'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { Badge } from '../components/UI'
import {
  Upload as UploadIcon, Trash2, CheckCircle,
  AlertCircle, Loader2, Download,
} from 'lucide-react'

const BASE = () => import.meta.env.VITE_API_URL || ''
const CDC_OPTIONS = ['GD','TIGEM','TIGET','FT','STRUTTURA','Terapie']

// ── Card di upload per un singolo tipo di file ─────────────────
function UploadCard({ tipo, label, color, desc, onSuccess }) {
  const [file, setFile]     = useState(null)
  const [cdc, setCdc]       = useState('')
  const [status, setStatus] = useState(null)   // null | uploading | ok | error
  const [msg, setMsg]       = useState('')
  const inputRef = useRef()

  const borderCls = { blue:'border-telethon-blue', orange:'border-orange-400', red:'border-telethon-red' }[color]
  const bgCls     = { blue:'bg-telethon-lightblue', orange:'bg-orange-50', red:'bg-red-50' }[color]
  const textCls   = { blue:'text-telethon-blue', orange:'text-orange-600', red:'text-telethon-red' }[color]

  function handleSelect(e) {
    const f = e.target.files?.[0]
    if (!f) return
    setFile(f)
    setStatus(null)
    setMsg('')
  }

  function handleRemove() {
    setFile(null)
    setStatus(null)
    setMsg('')
    if (inputRef.current) inputRef.current.value = ''
  }

  async function handleImport() {
    if (!file || status === 'uploading') return
    setStatus('uploading')
    setMsg('')

    const fd = new FormData()
    fd.append('file', file)

    const url = tipo === 'saving' && cdc
      ? `${BASE()}/upload/saving?cdc_override=${encodeURIComponent(cdc)}`
      : `${BASE()}/upload/${tipo}`

    try {
      const res  = await fetch(url, { method: 'POST', body: fd })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`)

      const righe = data.rows_inserted ?? data.rows ?? 0
      setStatus('ok')
      setMsg(`✓ ${righe.toLocaleString('it-IT')} righe importate${data.sheet_used ? ` — foglio "${data.sheet_used}"` : ''}`)
      setFile(null)
      if (inputRef.current) inputRef.current.value = ''
      onSuccess?.()
    } catch (e) {
      setStatus('error')
      setMsg(e.message)
    }
  }

  return (
    <div className={`bg-white rounded-xl border border-gray-100 shadow-sm p-5 border-l-4 ${borderCls}`}>
      <div className="font-semibold text-gray-900 text-sm mb-0.5">{label}</div>
      <div className="text-xs text-gray-500 mb-3">{desc}</div>

      {tipo === 'saving' && (
        <div className="mb-3">
          <label className="text-xs text-gray-500 block mb-1">CDC (opzionale):</label>
          <select value={cdc} onChange={e => setCdc(e.target.value)}
            className="w-full text-xs border border-gray-200 rounded-lg px-2.5 py-1.5 bg-white">
            <option value="">Rileva automaticamente dal file</option>
            {CDC_OPTIONS.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
      )}

      {/* File selezionato */}
      {file && (
        <div className="flex items-center justify-between bg-gray-50 rounded-lg px-3 py-2 mb-3 text-xs">
          <span className="text-gray-700 truncate max-w-[180px]" title={file.name}>{file.name}</span>
          <div className="flex items-center gap-2 ml-2 flex-shrink-0">
            <span className="text-gray-400">{(file.size / 1024).toFixed(0)} KB</span>
            <button onClick={handleRemove} className="text-gray-400 hover:text-red-500 transition-colors">
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          </div>
        </div>
      )}

      {/* Messaggio risultato */}
      {msg && (
        <div className={`flex items-start gap-2 text-xs mb-3 ${status === 'ok' ? 'text-green-700' : 'text-red-700'}`}>
          {status === 'ok'
            ? <CheckCircle className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />
            : <AlertCircle className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />}
          <span>{msg}</span>
        </div>
      )}

      {/* Bottoni */}
      <input
        ref={inputRef}
        type="file"
        accept=".xlsx,.xls"
        className="hidden"
        onChange={handleSelect}
      />

      <div className="flex items-center gap-2">
        {/* Seleziona file */}
        <button
          onClick={() => { if (inputRef.current) { inputRef.current.value = ''; inputRef.current.click() } }}
          disabled={status === 'uploading'}
          className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-medium
            ${bgCls} ${textCls} hover:opacity-80 disabled:opacity-40 transition-colors`}>
          <UploadIcon className="h-3.5 w-3.5" />
          {file ? 'Cambia file' : 'Seleziona file Excel'}
        </button>

        {/* IMPORTA — appare solo quando c'è un file selezionato */}
        {file && (
          <button
            onClick={handleImport}
            disabled={status === 'uploading'}
            className="flex items-center gap-1.5 px-4 py-2 rounded-lg text-xs font-bold
              bg-telethon-blue text-white hover:opacity-90 disabled:opacity-50 transition-colors">
            {status === 'uploading'
              ? <><Loader2 className="h-3.5 w-3.5 animate-spin" /> Importazione…</>
              : <><CheckCircle className="h-3.5 w-3.5" /> Importa</>
            }
          </button>
        )}
      </div>
    </div>
  )
}

// ── Export panel ──────────────────────────────────────────────
function ExportPanel({ anni }) {
  const [anno, setAnno] = useState('')
  const [cdc, setCdc]   = useState('')

  function dl(path) {
    const p = new URLSearchParams(
      Object.fromEntries(Object.entries({ anno, cdc }).filter(([, v]) => v))
    )
    window.open(`${BASE()}${path}?${p}`, '_blank')
  }

  return (
    <div className="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
      <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
        <Download className="h-4 w-4 text-telethon-blue" /> Export Dati
      </h3>
      <div className="flex flex-wrap gap-3 mb-4">
        <select value={anno} onChange={e => setAnno(e.target.value)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white">
          <option value="">Tutti gli anni</option>
          {(anni || []).map(a => <option key={a} value={String(a)}>{a}</option>)}
        </select>
        <select value={cdc} onChange={e => setCdc(e.target.value)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white">
          <option value="">Tutti i CDC</option>
          {CDC_OPTIONS.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
      </div>
      <div className="flex flex-wrap gap-2">
        {[
          { label: 'Saving Excel',  color: 'blue',   path: '/export/custom/excel' },
        ].map(btn => (
          <button key={btn.label} onClick={() => dl(btn.path)}
            className="flex items-center gap-1.5 px-3 py-2 text-xs font-medium rounded-lg
              bg-telethon-lightblue text-telethon-blue hover:opacity-80">
            <Download className="h-3.5 w-3.5" /> {btn.label}
          </button>
        ))}
      </div>
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
    if (!confirm('Eliminare questo upload? L\'operazione non è reversibile.')) return
    await api.deleteUpload(id)
    setRefresh(r => r + 1)
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Carica Dati</h1>
        <p className="text-sm text-gray-500 mt-1">
          Seleziona un file Excel, poi clicca <strong>Importa</strong>.
          Il sistema rileva automaticamente il foglio e le colonne —
          non dipende dal nome del file o del foglio.
        </p>
        <div className="mt-2 bg-amber-50 border border-amber-100 rounded-lg px-4 py-2 text-xs text-amber-700">
          <strong>Nota:</strong> se il file è aperto in Excel, salvalo e chiudilo prima di caricarlo.
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <UploadCard tipo="saving" label="File Saving / Ordini" color="blue"
          desc="Estratto Alyante con ordini, importi, saving. Qualsiasi foglio — rilevamento automatico."
          onSuccess={() => setRefresh(r => r + 1)} />
        <UploadCard tipo="tempi" label="Tempi Attraversamento" color="orange"
          desc="File con colonne Protocol, Year_Month, Days_Purchasing… Qualsiasi foglio."
          onSuccess={() => setRefresh(r => r + 1)} />
        <UploadCard tipo="nc" label="Non Conformità" color="red"
          desc="File con colonne Fornitore, Data Origine, Non Conformità. Qualsiasi foglio."
          onSuccess={() => setRefresh(r => r + 1)} />
      </div>

      <ExportPanel anni={anni} />

      <div className="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
        <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-4">
          Storico Caricamenti
        </h3>
        {loading ? (
          <div className="flex items-center gap-2 text-sm text-gray-400 py-4">
            <Loader2 className="h-4 w-4 animate-spin" /> Caricamento…
          </div>
        ) : (logData || []).length === 0 ? (
          <p className="text-sm text-gray-400 py-4">Nessun caricamento effettuato</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100">
                {['File','Tipo','CDC','Data','Righe','Status',''].map(h => (
                  <th key={h} className="text-left py-2 px-3 text-xs font-semibold text-gray-500 uppercase">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(logData || []).map(row => (
                <tr key={row.id} className="border-b border-gray-50 hover:bg-gray-50">
                  <td className="py-2 px-3 text-gray-700 max-w-[220px] truncate" title={row.filename}>
                    {row.filename}
                  </td>
                  <td className="py-2 px-3">
                    <Badge color={{ saving:'blue', tempi:'orange', nc:'red' }[row.tipo] || 'gray'}>
                      {row.tipo}
                    </Badge>
                  </td>
                  <td className="py-2 px-3 text-gray-500 text-xs">{row.cdc_filter || '—'}</td>
                  <td className="py-2 px-3 text-gray-500 text-xs whitespace-nowrap">
                    {new Date(row.upload_date).toLocaleString('it-IT')}
                  </td>
                  <td className="py-2 px-3 tabular-nums font-medium">
                    {row.rows_inserted != null ? row.rows_inserted.toLocaleString('it-IT') : '—'}
                  </td>
                  <td className="py-2 px-3">
                    <Badge color={row.status === 'ok' ? 'green' : 'red'}>{row.status}</Badge>
                  </td>
                  <td className="py-2 px-3">
                    <button onClick={() => handleDelete(row.id)}
                      className="text-gray-300 hover:text-red-500 transition-colors" title="Elimina">
                      <Trash2 className="h-4 w-4" />
                    </button>
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
