import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Riepilogo from './pages/Riepilogo'
import Saving from './pages/Saving'
import Tempi from './pages/Tempi'
import NonConformita from './pages/NonConformita'
import Fornitori from './pages/Fornitori'
import AlfaDoc from './pages/AlfaDoc'
import Upload from './pages/Upload'
import DataQuality from './pages/DataQuality'

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/"          element={<Riepilogo />} />
        <Route path="/saving"    element={<Saving />} />
        <Route path="/tempi"     element={<Tempi />} />
        <Route path="/nc"        element={<NonConformita />} />
        <Route path="/fornitori" element={<Fornitori />} />
        <Route path="/alfa-doc"  element={<AlfaDoc />} />
        <Route path="/upload"    element={<Upload />} />
        <Route path="/quality"   element={<DataQuality />} />
      </Routes>
    </Layout>
  )
}
