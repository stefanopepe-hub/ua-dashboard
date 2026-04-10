import { useState, useEffect, useRef } from 'react'

export function useKpi(fetcher, deps = []) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)
  const cancel = useRef(false)

  useEffect(() => {
    cancel.current = false
    setLoading(true)
    setError(null)
    fetcher()
      .then(d  => { if (!cancel.current) { setData(d);  setLoading(false) } })
      .catch(e => { if (!cancel.current) { setError(e.message); setLoading(false) } })
    return () => { cancel.current = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)

  return { data, loading, error }
}
