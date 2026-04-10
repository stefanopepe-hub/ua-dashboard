import { useState, useEffect } from 'react'
import { api } from '../utils/api'

export function useAnni() {
  const [anni, setAnni] = useState([])
  const [defaultAnno, setDefault] = useState('')

  useEffect(() => {
    api.anni()
      .then(rows => {
        const list = rows.map(r => r.anno)
        setAnni(list)
        if (list.length > 0) setDefault(String(list[0]))
      })
      .catch(() => {})
  }, [])

  return { anni, defaultAnno }
}
