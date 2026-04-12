/**
 * hooks/useAnni.js — Hook anni disponibili
 */
import { useState, useEffect } from 'react'
import { api } from '../utils/api'

export function useAnni() {
  const [anni,    setAnni]    = useState([])
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)

  useEffect(() => {
    let cancelled = false
    api.anni()
      .then(data => {
        if (cancelled) return
        setAnni((data || []).map(r => r.anno))
        setLoading(false)
      })
      .catch(err => {
        if (cancelled) return
        setError(err?.message ?? String(err))
        setLoading(false)
      })
    return () => { cancelled = true }
  }, [])

  const defaultAnno = anni[0] ?? null
  return { anni, defaultAnno, loading, error }
}
