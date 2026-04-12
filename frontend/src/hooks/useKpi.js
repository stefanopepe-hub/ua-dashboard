/**
 * hooks/useKpi.js — KPI data hook
 *
 * Approccio diretto senza React Query per evitare cache key collision.
 * Ogni chiamata useKpi è indipendente e ha il suo stato locale.
 */
import { useState, useEffect, useRef } from 'react'

export function useKpi(fetchFn, deps = [], options = {}) {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)
  const counter = useRef(0)

  useEffect(() => {
    let cancelled = false
    const id = ++counter.current
    setLoading(true)
    setError(null)

    fetchFn()
      .then(result => {
        if (cancelled || id !== counter.current) return
        setData(result ?? null)
        setLoading(false)
      })
      .catch(err => {
        if (cancelled || id !== counter.current) return
        setError(err?.message ?? String(err))
        setLoading(false)
      })

    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)

  const refetch = () => { counter.current++; setLoading(true); setError(null)
    fetchFn().then(r => { setData(r ?? null); setLoading(false) })
             .catch(e => { setError(e?.message ?? String(e)); setLoading(false) }) }

  return { data, loading, error, refetch }
}

// Kept for backward compatibility — no-op without React Query
export function useKpiInvalidation() {
  return {
    invalidateKpi: () => {},
    invalidateAll: () => {},
  }
}
