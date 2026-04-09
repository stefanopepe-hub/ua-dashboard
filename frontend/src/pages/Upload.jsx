import { useState, useRef } from 'react'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { LoadingBox, ErrorBox, SectionTitle, Badge } from '../components/UI'
import { Upload as UploadIcon, Trash2, CheckCircle, AlertCircle, FileSpreadsheet } from 'lucide-react'

const UPLOAD_CONFIG = [
  {
    key: 'saving',
    label: 'File Saving / Ordini',
    endpoint: '/upload/saving',
    desc: 'Excel con foglio "Final saving 2025" — estratto mensile da Alyante',
    foglio: 'Final saving 2025',
    color: 'blue',
  },
  {
    key: 'tempi',
    label: 'File Tempi Attraversamento',
    endpoint: '/upload/tempi',
    desc: 'Excel con colonne Protocol, Year_Month, Days_Purchasing…',
    foglio: 'Foglio unico',
    color: 'orange',
  },
  {
    key: 'nc',
    label: 'File Non Conformità',
    endpoint: '/upload/nc',
    desc: 'Excel NonConformita_Ricerca_Semplificato con colonna "Non Conformità"',
    foglio: 'Foglio unico',
    color: 'red',
  },
]

function UploadCard({ config, onSuccess }) {
  const [status, setStatus] = useState(null) // null | 'uploading' | 'ok' | 'error'
  const [msg, setMsg] = useState('')
  const inputRef = useRef()
  const BASE = import.meta.env.VITE_API_URL || ''

  async function handleFile(file) {
    if (!file) return
    setStatus('uploading')
    setMsg('')
    const fd = new FormData()
    fd.append('file', file)
    try {
      const res = await fetch(`${BASE}${config.endpoint}`, { method: 'POST', body: fd })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Errore upload')
      setStatus('ok')
      setMsg(`${data.rows.toLocaleString('it-IT')} righe caricate`)
      onSuccess?.()
    } catch (e) {
      setStatus('error')
      setMsg(e.message)
    }
  }

  const borderColor = { blue: 'border-telethon-blue', orange: 'border-orange-400', red: 'border-telethon-red' }[config.color]
  const bgColor = { blue: 'bg-telethon-lightblue', orange: 'bg-orange-50', red: 'bg-red-50' }[config.color]
  const textColor = { blue: 'text-telethon-blue', orange: 'text-orange-600', red: 'text-telethon-red' }[config.color]

  return (
    <div className={`card border-l-4 ${borderColor}`}>
      <div className="flex items-start gap-3">
        <FileSpreadsheet className={`h-5 w-5 mt-0.5 flex-shrink-0 ${textColor}`}/>
        <div className="flex-1 min-w-0">
          <div className="font-semibold text-gray-900 text-sm">{config.label}</div>
          <div className="text-xs text-gray-500 mt-0.5">{config.desc}</div>
          <div className="text-xs text-gray-400 mt-0.5">Foglio atteso: <code className="bg-gray-100 px-1 rounded">{config.foglio}</code></div>
        </div>
      </div>

      <div className="mt-4">
        <input ref={inputRef} type="file" accept=".xlsx,.xls" className="hidden"
          onChange={e=>handleFile(e.target.files[0])}/>
        <button
          onClick={()=>inputRef.current?.click()}
          disabled={status==='uploading'}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${bgColor} ${textColor} hover:opacity-80 disabled:opacity-50`}>
          <UploadIcon className="h-4 w-4"/>
          {status==='uploading' ? 'Caricamento…' : 'Seleziona file Excel'}
        </button>

        {status==='ok' && (
          <div className="flex items-center gap-2 mt-2 text-sm text-green-700">
            <CheckCircle className="h-4 w-4"/> {msg}
          </div>
        )}
        {status==='error' && (
          <div className="flex items-center gap-2 mt-2 text-sm text-red-700">
            <AlertCircle className="h-4 w-4"/> {msg}
          </div>
        )}
      </div>
    </div>
  )
}

export default function Upload() {
  const [refresh, setRefresh] = useState(0)
  const {data:log,loading} = useKpi(()=>api.uploadLog(),[refresh])
  const BASE = import.meta.env.VITE_API_URL || ''

  async function handleDelete(id) {
    if (!confirm('Eliminare questo upload e tutti i dati correlati?')) return
    await api.deleteUpload(id)
    setRefresh(r=>r+1)
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Carica Dati</h1>
        <p className="text-sm text-gray-500 mt-0.5">Carica i file Excel mensili estratti da Alyante. I dati vengono aggiunti al database senza sovrascrivere quelli esistenti.</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {UPLOAD_CONFIG.map(cfg=>(
          <UploadCard key={cfg.key} config={cfg} onSuccess={()=>setRefresh(r=>r+1)}/>
        ))}
      </div>

      {/* Log uploads */}
      <div className="card">
        <SectionTitle>Storico Caricamenti</SectionTitle>
        {loading ? <LoadingBox/> : (log||[]).length === 0 ? (
          <p className="text-sm text-gray-400 text-center py-8">Nessun caricamento ancora effettuato</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100">
                  {['File','Tipo','Data','Righe','Status',''].map(h=>(
                    <th key={h} className="text-left py-2 px-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(log||[]).map(row=>(
                  <tr key={row.id} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 px-3 text-gray-700 max-w-xs truncate">{row.filename}</td>
                    <td className="py-2 px-3"><Badge color={{saving:'blue',tempi:'orange',nc:'red'}[row.tipo]||'gray'}>{row.tipo}</Badge></td>
                    <td className="py-2 px-3 text-gray-500 whitespace-nowrap">{new Date(row.upload_date).toLocaleString('it-IT')}</td>
                    <td className="py-2 px-3">{row.rows_inserted?.toLocaleString('it-IT') || '—'}</td>
                    <td className="py-2 px-3"><Badge color={row.status==='ok'?'green':'red'}>{row.status}</Badge></td>
                    <td className="py-2 px-3">
                      <button onClick={()=>handleDelete(row.id)} className="text-gray-400 hover:text-red-600 transition-colors">
                        <Trash2 className="h-4 w-4"/>
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
