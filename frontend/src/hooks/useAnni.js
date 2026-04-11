/**
 * hooks/useAnni.js — Hook anni disponibili con React Query
 */
import { useQuery } from '@tanstack/react-query'
import { api } from '../utils/api'

export function useAnni() {
  const query = useQuery({
    queryKey: ['anni'],
    queryFn: api.anni,
    staleTime: 10 * 60 * 1000,  // 10 minuti
    retry: 2,
    refetchOnWindowFocus: false,
  })

  const anni = (query.data || []).map(r => r.anno)
  const defaultAnno = anni[0] ?? null

  return {
    anni,
    defaultAnno,
    loading: query.isLoading,
    error: query.error?.message ?? null,
  }
}
