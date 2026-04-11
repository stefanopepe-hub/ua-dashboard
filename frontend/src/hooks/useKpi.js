/**
 * hooks/useKpi.js — React Query hook per analytics KPI
 * 
 * Enterprise upgrade rispetto al polling semplice:
 * - cache automatica (staleTime 5min)
 * - retry automatico (3 tentativi con backoff)
 * - error boundary friendly
 * - no refetch inutile su navigazione
 * - stato separato per ogni widget
 */
import { useQuery, useQueryClient } from '@tanstack/react-query'

/**
 * Hook KPI generico con React Query.
 * 
 * @param {Function} fetchFn - funzione async che ritorna i dati
 * @param {Array} deps - dipendenze per la cache key
 * @param {Object} options - opzioni React Query override
 */
export function useKpi(fetchFn, deps = [], options = {}) {
  const key = ['kpi', ...deps.map(d => String(d ?? ''))]

  const query = useQuery({
    queryKey: key,
    queryFn: fetchFn,
    staleTime: 5 * 60 * 1000,      // 5 minuti — non refetch se dati freschi
    gcTime: 10 * 60 * 1000,        // 10 minuti in cache dopo unmount
    retry: 2,
    retryDelay: attempt => Math.min(1000 * 2 ** attempt, 10000),
    refetchOnWindowFocus: false,   // non refetch quando torna in focus
    ...options,
  })

  return {
    data:    query.data ?? null,
    loading: query.isLoading || query.isFetching,
    error:   query.error?.message ?? null,
    refetch: query.refetch,
    isStale: query.isStale,
  }
}

/**
 * Hook per invalidare la cache dopo un upload.
 * Chiama invalidateKpi() dopo un import riuscito.
 */
export function useKpiInvalidation() {
  const client = useQueryClient()
  return {
    invalidateKpi: (prefix = 'kpi') => {
      client.invalidateQueries({ queryKey: [prefix] })
    },
    invalidateAll: () => {
      client.invalidateQueries()
    },
  }
}
