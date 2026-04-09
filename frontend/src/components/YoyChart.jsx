/**
 * Componenti riutilizzabili per grafici YoY.
 * Usati in Riepilogo, Saving, Tempi.
 */
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis,
  CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { COLORS } from '../utils/fmt'

const CURR_COLOR = COLORS.blue
const PREV_COLOR = '#93c5fd' // blue-300 — tono più chiaro per anno precedente

/**
 * Barre affiancate per due anni.
 * dataKey1 = chiave anno corrente, dataKey2 = chiave anno precedente
 */
export function YoyBarChart({ data, dataKey1, dataKey2, label1, label2, formatter, height=240 }) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{top:4,right:8,left:0,bottom:0}}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
        <XAxis dataKey="mese" tick={{fontSize:11}}/>
        <YAxis tick={{fontSize:11}}/>
        <Tooltip formatter={(v,n) => [formatter ? formatter(v) : v, n]}/>
        <Legend wrapperStyle={{fontSize:11}}/>
        <Bar dataKey={dataKey1} name={label1} fill={CURR_COLOR} radius={[2,2,0,0]}/>
        <Bar dataKey={dataKey2} name={label2} fill={PREV_COLOR} radius={[2,2,0,0]}/>
      </BarChart>
    </ResponsiveContainer>
  )
}

/**
 * Due linee per confronto trend % su due anni
 */
export function YoyLineChart({ data, dataKey1, dataKey2, label1, label2, formatter, unit='', height=200 }) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data} margin={{top:4,right:8,left:0,bottom:0}}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/>
        <XAxis dataKey="mese" tick={{fontSize:11}}/>
        <YAxis tick={{fontSize:11}} unit={unit}/>
        <Tooltip formatter={(v,n) => [formatter ? formatter(v) : v+unit, n]}/>
        <Legend wrapperStyle={{fontSize:11}}/>
        <Line type="monotone" dataKey={dataKey1} name={label1}
          stroke={CURR_COLOR} strokeWidth={2.5} dot={{r:3}}/>
        <Line type="monotone" dataKey={dataKey2} name={label2}
          stroke={PREV_COLOR} strokeWidth={1.5} strokeDasharray="5 3" dot={{r:2}}/>
      </LineChart>
    </ResponsiveContainer>
  )
}

/**
 * Delta badge: mostra variazione percentuale
 */
export function DeltaBadge({ value, suffix='%', label='' }) {
  if (value == null) return null
  const positive = value >= 0
  return (
    <span className={`inline-flex items-center gap-0.5 text-xs font-semibold ${positive ? 'text-green-600' : 'text-red-600'}`}>
      {positive ? '▲' : '▼'} {Math.abs(value).toFixed(1)}{suffix}
      {label && <span className="text-gray-400 font-normal ml-1">{label}</span>}
    </span>
  )
}

/**
 * Box commento automatico + nota libera
 */
export function YoyComment({ autoText, note, onNoteChange }) {
  return (
    <div className="bg-blue-50 border border-blue-100 rounded-lg p-4 space-y-2">
      <p className="text-sm text-blue-800">💬 {autoText}</p>
      <div>
        <label className="text-xs text-gray-500 font-medium">📝 Aggiungi nota:</label>
        <textarea
          className="mt-1 w-full text-sm border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-telethon-blue resize-none"
          rows={2}
          placeholder="Inserisci un commento o spiegazione per questo periodo…"
          value={note}
          onChange={e => onNoteChange(e.target.value)}
        />
      </div>
    </div>
  )
}
