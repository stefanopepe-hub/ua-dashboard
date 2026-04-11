import { Routes, Route } from 'react-router-dom'
import { Suspense, lazy } from 'react'
import Layout from './components/Layout'
import { ErrorBoundary } from './components/ErrorBoundary'
import { LoadingSpinner } from './components/UI'

// Lazy loading delle pagine
const Riepilogo     = lazy(() => import('./pages/Riepilogo'))
const Saving        = lazy(() => import('./pages/Saving'))
const Tempi         = lazy(() => import('./pages/Tempi'))
const NonConformita = lazy(() => import('./pages/NonConformita'))
const Fornitori     = lazy(() => import('./pages/Fornitori'))
const AlfaDoc       = lazy(() => import('./pages/AlfaDoc'))
const Upload        = lazy(() => import('./pages/Upload'))
const DataQuality   = lazy(() => import('./pages/DataQuality'))
const Risorse       = lazy(() => import('./pages/Risorse'))

// Wrapper: ogni pagina è isolata da error boundary
function Page({ component: Comp, title }) {
  return (
    <ErrorBoundary title={`Errore nella pagina: ${title}`}>
      <Suspense fallback={<LoadingSpinner size="lg" />}>
        <Comp />
      </Suspense>
    </ErrorBoundary>
  )
}

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/"          element={<Page component={Riepilogo}     title="Dashboard" />} />
        <Route path="/saving"    element={<Page component={Saving}        title="Saving & Ordini" />} />
        <Route path="/tempi"     element={<Page component={Tempi}         title="Tempi Attraversamento" />} />
        <Route path="/nc"        element={<Page component={NonConformita} title="Non Conformità" />} />
        <Route path="/fornitori" element={<Page component={Fornitori}     title="Fornitori" />} />
        <Route path="/alfa-doc"  element={<Page component={AlfaDoc}       title="Tipologie Doc." />} />
        <Route path="/upload"    element={<Page component={Upload}        title="Carica Dati" />} />
        <Route path="/quality"   element={<Page component={DataQuality}   title="Data Quality" />} />
        <Route path="/risorse"   element={<Page component={Risorse}       title="Risorse" />} />
      </Routes>
    </Layout>
  )
}
