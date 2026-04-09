import { useState, useRef } from 'react'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { LoadingBox, Badge } from '../components/UI'
import { Upload as UploadIcon, Trash2, CheckCircle, AlertCircle, FileSpreadsheet, Download, Eye } from 'lucide-react'

const CDC_OPTIONS = ['GD', 'TIGEM', 'FT', 'STRUTTURA', 'Terapie', 'TIGET']
const BASE = () => import.meta.env.VITE_API_URL || ''

const UPLOAD_CONFIG = [
  { key:'saving', label:'File Saving / Ordini', endpoint:'/upload/saving', color:'blue',
    desc:'Estratto Alyante con ordini, importi, saving. Qualsiasi foglio — rilevamento automatico.' },
  { key:'tempi', label:'Tempi Attraversamento', endpoint:'/upload/tempi', color:'orange',
    desc:'File con colonne Protocol, Year_Month, Days_Purchasing… Qualsiasi foglio.' },
  { key:'nc', label:'Non Conformità', endpoint:'/upload/nc', color:'red',
    desc:'File con colonne Fornitore, Data Origine, Non Conformità. Qualsiasi foglio.' },
]

function UploadCard({ config, onSuccess }) {
  const [status, setStatus] = useState(null)
  const [msg, setMsg] = useState('')
  const [cdc, setCdc] = useState('')
  const [preview, setPreview] = useState(null)
  const inputRef = useRef()

  async function handleFile(file) {
    if (!file) return

    // Preview first for saving files
    if (config.key === 'saving') {
      const fd = new FormData()
      fd.append('file', file)
      try {
        const res = await fetch(`${BASE()}/upload/preview`, { method:'POST', body:fd })
        const data = await res.json()
        setPreview(data)
      } catch(e) { setPreview(null) }
    }

    setStatus('uploading')
    setMsg('')
    const fd = new FormData()
    fd.append('file', file)
    const url = cdc && config.key === 'saving'
      ? `${BASE()}${config.endpoint}?cdc_override=${encodeURIComponent(cdc)}`
      : `${BASE()}${config.endpoint}`
    try {
      const res = await fetch(url, { method:'POST', body:fd })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Errore upload')
      setStatus('ok')
      setMsg(`${Number(data.rows).toLocaleString('it-IT')} righe caricate${data.sheet_detected ? ` (foglio: "${data.sheet_detected}")` : ''}`)
      onSuccess?.()
    } catch(e) {
      setStatus('error')
      setMsg(e.message)
    }
  }

  const border = {blue:'border-telethon-blue',orange:'border-orange-400',red:'border-telethon-red'}[config.color]
  const bg = {blue:'bg-telethon-lightblue',orange:'bg-orange-50',red:'bg-red-50'}[config.color]
  const text = {blue:'text-telethon-blue',orange:'text-orange-600',red:'text-telethon-red'}[config.color]

  return (
    <div className={`bg-white rounded-xl border border-gray-100 shadow-sm p-5 border-l-4 ${border}`}>
      <div className="flex items-start gap-3">
        <FileSpreadsheet className={`h-5 w-5 mt-0.5 flex-shrink-0 ${text}`}/>
        <div className="flex-1">
          <div className="font-semibold text-gray-900 text-sm">{config.label}</div>
          <div className="text-xs text-gray-500 mt-0.5">{config.desc}</div>
        </div>
      </div>

      {config.key === 'saving' && (
        <div className="mt-3">
          <label className="text-xs text-gray-500 font-medium">CDC (opzionale — sovrascrive il CDC nel file):</label>
          <select className="mt-1 w-full text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white"
            value={cdc} onChange={e=>setCdc(e.target.value)}>
            <option value="">Rileva automaticamente dal file</option>
            {CDC_OPTIONS.map(c=><option key={c} value={c}>{c}</option>)}
          </select>
        </div>
      )}

      <div className="mt-4">
        <input ref={inputRef} type="file" accept=".xlsx,.xls" className="hidden"
          onChange={e=>handleFile(e.target.files[0])}/>
        <button onClick={()=>inputRef.current?.click()} disabled={status==='uploading'}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${bg} ${text} hover:opacity-80 disabled:opacity-50`}>
          <UploadIcon className="h-4 w-4"/>
          {status==='uploading' ? 'Caricamento…' : 'Seleziona file Excel'}
        </button>

        {preview && status !== 'ok' && (
          <div className="mt-2 text-xs text-gray-500 bg-gray-50 rounded-lg px-3 py-2">
            <span className="font-medium">Rilevato:</span> foglio &ldquo;{preview.detected_sheet}&rdquo;
            {' '}· tipo <span className="font-medium">{preview.detected_type || '?'}</span>
            {' '}· {preview.sheets?.find(s=>s.name===preview.detected_sheet)?.rows?.toLocaleString('it-IT')} righe
          </div>
        )}
        {status==='ok' && <div className="flex items-center gap-2 mt-2 text-sm text-green-700"><CheckCircle className="h-4 w-4"/>{msg}</div>}
        {status==='error' && <div className="flex items-center gap-2 mt-2 text-sm text-red-700"><AlertCircle className="h-4 w-4"/>{msg}</div>}
      </div>
    </div>
  )
}

function ExportPanel() {
  const [anno, setAnno] = useState('2025')
  const [cdc, setCdc] = useState('')
  const base = BASE()

  const dl = (path) => {
    const params = new URLSearchParams(Object.fromEntries(Object.entries({anno,cdc}).filter(([,v])=>v)))
    window.open(`${base}${path}?${params}`, '_blank')
  }

  return (
    <div className="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
      <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
        <Download className="h-4 w-4 text-telethon-blue"/> Export Dati
      </h3>
      <div className="flex flex-wrap gap-3 mb-4">
        <select className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white" value={anno} onChange={e=>setAnno(e.target.value)}>
          <option value="">Tutti gli anni</option>
          <option value="2025">2025</option>
          <option value="2024">2024</option>
        </select>
        <select className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white" value={cdc} onChange={e=>setCdc(e.target.value)}>
          <option value="">Tutti i CDC</option>
          {CDC_OPTIONS.map(c=><option key={c} value={c}>{c}</option>)}
        </select>
      </div>
      <div className="flex flex-wrap gap-2">
        <button onClick={()=>dl('/export/saving/excel')}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium bg-telethon-lightblue text-telethon-blue rounded-lg hover:opacity-80">
          <Download className="h-4 w-4"/> Saving Excel
        </button>
        <button onClick={()=>dl('/export/tempi/excel')}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium bg-orange-50 text-orange-600 rounded-lg hover:opacity-80">
          <Download className="h-4 w-4"/> Tempi Excel
        </button>
        <button onClick={()=>dl('/export/nc/excel')}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium bg-red-50 text-red-600 rounded-lg hover:opacity-80">
          <Download className="h-4 w-4"/> NC Excel
        </button>
      </div>
    </div>
  )
}

export default function Upload() {
  const [refresh, setRefresh] = useState(0)
  const {data:log, loading} = useKpi(()=>api.uploadLog(),[refresh])

  async function handleDelete(id) {
    if (!confirm('Eliminare questo upload e tutti i dati correlati?')) return
    await api.deleteUpload(id)
    setRefresh(r=>r+1)
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Carica Dati</h1>
        <p className="text-sm text-gray-500 mt-0.5">
          Il sistema rileva automaticamente il foglio e le colonne — non dipende dal nome del file o del foglio.
          Puoi caricare più file dello stesso tipo (es. un file per CDC): i dati vengono aggregati.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {UPLOAD_CONFIG.map(cfg=>(
          <UploadCard key={cfg.key} config={cfg} onSuccess={()=>setRefresh(r=>r+1)}/>
        ))}
      </div>

      <ExportPanel/>

      <div className="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
        <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-4">Storico Caricamenti</h3>
        {loading ? <LoadingBox/> : (log||[]).length===0 ? (
          <p className="text-sm text-gray-400 text-center py-8">Nessun caricamento effettuato</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100">
                  {['File','Tipo','CDC','Data','Righe','Status',''].map(h=>(
                    <th key={h} className="text-left py-2 px-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(log||[]).map(row=>(
                  <tr key={row.id} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 px-3 text-gray-700 max-w-xs truncate">{row.filename}</td>
                    <td className="py-2 px-3"><Badge color={{saving:'blue',tempi:'orange',nc:'red'}[row.tipo]||'gray'}>{row.tipo}</Badge></td>
                    <td className="py-2 px-3 text-gray-500">{row.cdc_filter||'—'}</td>
                    <td className="py-2 px-3 text-gray-500 whitespace-nowrap">{new Date(row.upload_date).toLocaleString('it-IT')}</td>
                    <td className="py-2 px-3">{row.rows_inserted?.toLocaleString('it-IT')||'—'}</td>
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
