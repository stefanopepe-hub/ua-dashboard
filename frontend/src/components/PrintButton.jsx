import { useState } from 'react'
import { Printer, FileText, Download } from 'lucide-react'
import { api } from '../utils/api'

/**
 * Componente bottoni export: stampa rapida + PDF formattato + Excel
 */
export function ExportBar({ anno, strRic, cdc, note, showExcel=true }) {
  const [pdfLoading, setPdfLoading] = useState(false)

  function handlePrint() {
    window.print()
  }

  function handlePdf() {
    setPdfLoading(true)
    const url = api.exportReportPdf({ anno, str_ric: strRic, cdc, note })
    const a = document.createElement('a')
    a.href = url
    a.download = `report_acquisti_${anno}.pdf`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    setTimeout(() => setPdfLoading(false), 2000)
  }

  function handleExcel() {
    window.open(api.exportSavingExcel({ anno, str_ric: strRic, cdc }), '_blank')
  }

  return (
    <div className="flex flex-wrap gap-2 print:hidden">
      <button onClick={handlePrint}
        className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border border-gray-200 rounded-lg text-gray-600 hover:bg-gray-50 transition-colors">
        <Printer className="h-3.5 w-3.5"/> Stampa rapida
      </button>
      <button onClick={handlePdf} disabled={pdfLoading}
        className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-telethon-blue text-white rounded-lg hover:opacity-90 disabled:opacity-50 transition-colors">
        <FileText className="h-3.5 w-3.5"/>
        {pdfLoading ? 'Generazione…' : 'Report PDF'}
      </button>
      {showExcel && (
        <button onClick={handleExcel}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border border-gray-200 rounded-lg text-gray-600 hover:bg-gray-50 transition-colors">
          <Download className="h-3.5 w-3.5"/> Excel
        </button>
      )}
    </div>
  )
}
