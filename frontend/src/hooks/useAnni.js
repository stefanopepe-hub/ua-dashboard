import { useKpi } from './useKpi'
import { api } from '../utils/api'

export function useAnni() {
  const { data, loading } = useKpi(() => api.anniDisponibili(), [])
  const anni = (data || []).map(d => d.anno)
  // default = anno più recente disponibile
  const defaultAnno = anni.length > 0 ? String(anni[0]) : String(new Date().getFullYear())
  return { anni, loading, defaultAnno }
}
