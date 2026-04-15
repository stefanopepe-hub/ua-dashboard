/**
 * UI.jsx — Design System Enterprise
 * Fondazione Telethon ETS — UA Dashboard v2.1
 */
import { useState } from 'react'
import { AlertCircle, TrendingUp, TrendingDown, Minus, Info, RefreshCw } from 'lucide-react'

export function KpiCard({ label, value, sub, color='default', delta, large=false }) {
  const b = {blue:'border-l-telethon-blue',green:'border-l-green-500',red:'border-l-telethon-red',
    orange:'border-l-orange-500',purple:'border-l-purple-600',teal:'border-l-teal-500',
    gray:'border-l-gray-300',default:'border-l-gray-200'}
  return (
    <div className={`kpi-card border-l-4 ${b[color]||b.default}`}>
      <div className="kpi-label text-gray-400">{label}</div>
      <div className={`${large?'text-4xl font-extrabold':'kpi-value'} mt-1`}>{value}</div>
      {delta!=null && <div className="mt-1"><DeltaBadge value={delta}/></div>}
      {sub && <div className="text-xs text-gray-400 mt-1 leading-tight">{sub}</div>}
    </div>
  )
}

export function KpiCardSolid({ label, value, sub, bg='#0057A8', fg='#fff' }) {
  return (
    <div className="rounded-2xl p-5 flex flex-col gap-1" style={{background:bg}}>
      <div className="text-xs font-bold uppercase tracking-wider opacity-75" style={{color:fg}}>{label}</div>
      <div className="text-3xl font-extrabold tracking-tight" style={{color:fg}}>{value}</div>
      {sub && <div className="text-xs opacity-70 mt-0.5" style={{color:fg}}>{sub}</div>}
    </div>
  )
}

export function DeltaBadge({ value, suffix='', label='' }) {
  if (value==null||isNaN(value)) return null
  const pos=value>0, zero=value===0
  return (
    <span className={`inline-flex items-center gap-0.5 text-xs font-semibold ${zero?'text-gray-400':pos?'text-green-600':'text-red-500'}`}>
      {zero?<Minus className="h-3 w-3"/>:pos?<TrendingUp className="h-3 w-3"/>:<TrendingDown className="h-3 w-3"/>}
      {pos?'+':''}{value.toFixed(1)}{suffix}
      {label&&<span className="text-gray-400 font-normal ml-0.5">{label}</span>}
    </span>
  )
}

export function SectionTitle({ children, sub, action }) {
  return (
    <div className="flex items-start justify-between mb-4 gap-2">
      <div>
        <h3 className="section-title">{children}</h3>
        {sub&&<p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
      </div>
      {action&&<div className="flex-shrink-0">{action}</div>}
    </div>
  )
}

export function PageHeader({ title, subtitle, badge, actions }) {
  return (
    <div className="flex items-start justify-between flex-wrap gap-3 mb-2">
      <div>
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-bold text-gray-900 tracking-tight">{title}</h1>
          {badge}
        </div>
        {subtitle&&<p className="text-sm text-gray-400 mt-0.5">{subtitle}</p>}
      </div>
      {actions&&<div className="flex gap-2 flex-wrap">{actions}</div>}
    </div>
  )
}

export function FilterBar({ anno, setAnno, strRic, setStrRic, cdc, setCdc, anni=[] }) {
  return (
    <div className="flex flex-wrap gap-2 items-center">
      <select value={anno} onChange={e=>setAnno(e.target.value)}
        className="h-8 text-sm font-semibold text-telethon-blue border-telethon-blue rounded-xl px-3 pr-7 bg-blue-50">
        {anni.map(a=><option key={a} value={String(a)}>{a}</option>)}
      </select>
      <select value={strRic||''} onChange={e=>setStrRic(e.target.value)} className="h-8 text-sm rounded-xl px-3 pr-7 border-gray-200">
        <option value="">Ricerca + Struttura</option>
        <option value="RICERCA">Solo Ricerca</option>
        <option value="STRUTTURA">Solo Struttura</option>
      </select>
      {setCdc&&(
        <select value={cdc||''} onChange={e=>setCdc(e.target.value)} className="h-8 text-sm rounded-xl px-3 pr-7 border-gray-200">
          <option value="">Tutti i CDC</option>
          {['GD','TIGEM','TIGET','FT','STRUTTURA'].map(c=><option key={c} value={c}>{c}</option>)}
        </select>
      )}
    </div>
  )
}

export function GranSelect({ value, onChange }) {
  return (
    <select value={value} onChange={e=>onChange(e.target.value)} className="h-8 text-sm rounded-xl px-3 pr-7 border-gray-200">
      <option value="mensile">Mensile</option>
      <option value="quarter">Trimestrale</option>
      <option value="semestrale">Semestrale</option>
      <option value="annuale">Annuale</option>
    </select>
  )
}

export function Badge({ children, color='gray', dot=false }) {
  const cls={blue:'badge-blue',green:'badge-green',red:'badge-red',orange:'badge-orange',
    gray:'badge-gray',amber:'badge-amber',purple:'badge-purple',teal:'badge-teal'}
  return (
    <span className={`badge ${cls[color]||cls.gray}`}>
      {dot&&<span className={`w-1.5 h-1.5 rounded-full inline-block ${color==='green'?'bg-green-500':color==='red'?'bg-red-500':'bg-gray-400'}`}/>}
      {children}
    </span>
  )
}

export function LoadingBox({ rows=3 }) {
  return (
    <div className="space-y-2 py-2">
      {[...Array(rows)].map((_,i)=>(
        <div key={i} className="skeleton h-4 rounded" style={{width:`${75+i*5}%`,opacity:1-i*.2}}/>
      ))}
    </div>
  )
}

export function LoadingSpinner({ size='md' }) {
  const s=size==='lg'?'h-8 w-8':'h-5 w-5'
  return (
    <div className="flex items-center justify-center py-8">
      <div className={`${s} border-2 border-telethon-blue border-t-transparent rounded-full animate-spin`}/>
    </div>
  )
}

export function ErrorBox({ message }) {
  if (!message) return null
  const isNet = typeof message==='string'&&(message.includes('fetch')||message.includes('network'))
  return (
    <div className="flex items-start gap-2 bg-amber-50 border border-amber-100 rounded-xl px-4 py-3 text-xs text-amber-700">
      <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5"/>
      <span>{isNet?'Server in avvio — attendi qualche secondo e ricarica.':String(message).slice(0,150)}</span>
    </div>
  )
}

export function InfoBox({ children }) {
  return (
    <div className="flex items-start gap-2 bg-blue-50 border border-blue-100 rounded-xl px-4 py-3 text-xs text-blue-700">
      <Info className="h-3.5 w-3.5 flex-shrink-0 mt-0.5"/><span>{children}</span>
    </div>
  )
}

export function EmptyState({ title='Nessun dato', message='Carica i file Excel per visualizzare i dati.' }) {
  return (
    <div className="text-center py-12 text-gray-400">
      <div className="text-4xl mb-3">📊</div>
      <div className="font-semibold text-gray-500">{title}</div>
      <div className="text-xs mt-1 max-w-xs mx-auto">{message}</div>
    </div>
  )
}

export function DataTable({ columns, rows=[], maxRows, emptyMessage, sortable=false }) {
  const [sort, setSort] = useState(null)
  const [dir, setDir]   = useState('desc')
  let data = [...(rows||[])]
  if (sortable&&sort) data.sort((a,b)=>{const va=a[sort]??0,vb=b[sort]??0;return dir==='desc'?vb-va:va-vb})
  if (maxRows) data=data.slice(0,maxRows)
  if (!data.length) return <p className="text-sm text-gray-400 py-6 text-center">{emptyMessage||'Nessun dato'}</p>
  return (
    <div className="overflow-x-auto">
      <table className="data-table">
        <thead>
          <tr>{columns.map(col=>(
            <th key={col.key}
              className={`${col.align==='right'?'right':''} ${sortable&&col.sortable!==false?'cursor-pointer hover:text-telethon-blue select-none':''}`}
              onClick={()=>{if(!sortable||col.sortable===false)return;sort===col.key?setDir(d=>d==='desc'?'asc':'desc'):(setSort(col.key),setDir('desc'))}}>
              {col.label}{sortable&&sort===col.key&&<span className="ml-1">{dir==='desc'?'↓':'↑'}</span>}
            </th>
          ))}</tr>
        </thead>
        <tbody>{data.map((row,i)=>(
          <tr key={i}>{columns.map(col=>(
            <td key={col.key} className={col.align==='right'?'text-right tabular-nums':''}>
              {col.render?col.render(row[col.key],row):(row[col.key]??'—')}
            </td>
          ))}</tr>
        ))}</tbody>
      </table>
    </div>
  )
}

export function ChartCard({ title, sub, children, height=240, empty, emptyMsg }) {
  return (
    <div className="card">
      {title&&<SectionTitle>{title}</SectionTitle>}
      {sub&&<p className="text-xs text-gray-400 -mt-3 mb-3">{sub}</p>}
      {empty?<div className="flex items-center justify-center text-gray-300 text-sm" style={{height}}>{emptyMsg||'Nessun dato'}</div>
            :<div style={{height}}>{children}</div>}
    </div>
  )
}

export function StatRow({ label, value, sub, color }) {
  const c={blue:'text-telethon-blue',green:'text-green-600',orange:'text-orange-500',red:'text-red-500',gray:'text-gray-500'}
  return (
    <div className="flex items-center justify-between py-2 border-b border-gray-50 last:border-0">
      <span className="text-xs text-gray-500">{label}</span>
      <div className="text-right">
        <span className={`text-sm font-bold ${c[color]||'text-gray-900'}`}>{value}</span>
        {sub&&<span className="text-xs text-gray-400 ml-1">{sub}</span>}
      </div>
    </div>
  )
}

export function DataQualityLight({ score }) {
  const col=score>=80?'bg-green-500':score>=50?'bg-amber-500':'bg-red-500'
  const lbl=score>=80?'Dati OK':score>=50?'Attenzione':'Verifica necessaria'
  return (
    <div className="flex items-center gap-1.5">
      <div className={`w-2 h-2 rounded-full ${col}`}/>
      <span className="text-xs text-gray-400">{lbl}</span>
    </div>
  )
}
