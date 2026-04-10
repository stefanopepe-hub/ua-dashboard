import { useState } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { useAnni } from '../hooks/useAnni'
import { api } from '../utils/api'
import { fmtEur, fmtNum, fmtPct, fmtDays, COLORS } from '../utils/fmt'
import {
  PageHeader, KpiCard, ChartCard, SectionTitle, Badge,
  LoadingBox, EmptyState, DataTable, InfoBox,
} from '../components/UI'

export default function Risorse() {
  const { anni } = useAnni()
  const [anno, setAnno] = useState('')

  const { data: riepilogo, loading: l1, error: e1 } = useKpi(() => api.risorseRiepilogo(), [])
  const { data: perRisorsa, loading: l2 } = useKpi(
    () => api.risorsePerRisorsa({ anno }), [anno]
  )
  const { data: mensile, loading: l3 } = useKpi(
    () => api.risorseMensile({ anno }), [anno]
  )

  const noData = !l1 && riepilogo && !riepilogo.available

  const mensileChart = (mensile || []).map(m => ({
    name: m.mese?.slice(0, 7) || m.mese,
    'Pratiche': m.pratiche_totali || 0,
    'Saving €K': Math.round((m.saving_totale || 0) / 1000),
    'Risorse': m.n_risorse_attive || 0,
  }))

  if (noData || e1) {
    return (
      <div className="space-y-6">
        <PageHeader
          title="Analytics Risorse"
          subtitle="Workload, performance e saving per buyer e team"
        />
        <div className="card">
          <EmptyState
            title="Dati risorse non disponibili"
            message={riepilogo?.reason || e1 || 'Carica il file analytics risorse dalla sezione Carica Dati.'}
          />
          <div className="mt-4">
            <InfoBox>
              Il file risorse deve contenere colonne come: <strong>Risorsa</strong>, <strong>Mese</strong>,
              <strong>Pratiche Gestite</strong>, <strong>Saving Generato</strong>, ecc.
              Il sistema rileva automaticamente il tipo file.
            </InfoBox>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Analytics Risorse"
        subtitle="Workload, performance e saving per buyer e team"
        actions={
          <select value={anno} onChange={e => setAnno(e.target.value)} className="filter-select">
            <option value="">Tutti gli anni</option>
            {anni.map(a => <option key={a} value={String(a)}>{a}</option>)}
          </select>
        }
      />

      {/* KPI overview */}
      {l1 ? <LoadingBox /> : riepilogo?.available && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KpiCard label="N° Risorse"         value={fmtNum(riepilogo.n_risorse)}              color="blue" />
          <KpiCard label="Record Totali"       value={fmtNum(riepilogo.n_record)}               color="gray" />
          <KpiCard label="Media Pratiche/Res." value={fmtNum(riepilogo.avg_pratiche_gestite)}   color="orange" />
          <KpiCard label="Saving Generato"     value={fmtEur(riepilogo.tot_saving_generato)}    color="green" />
        </div>
      )}

      {/* Trend mensile */}
      <ChartCard
        title="Trend Mensile — Pratiche e Saving"
        subtitle="Andamento mensile del workload e saving generato dal team"
        loading={l3}
        empty={mensileChart.length === 0}
        emptyMessage="Nessun dato mensile disponibile per il periodo selezionato"
      >
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={mensileChart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
            <XAxis dataKey="name" tick={{ fontSize: 11 }} />
            <YAxis yAxisId="left" tick={{ fontSize: 11 }} />
            <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Bar yAxisId="left" dataKey="Pratiche" fill={COLORS.blue} radius={[3, 3, 0, 0]} />
            <Bar yAxisId="right" dataKey="Saving €K" fill={COLORS.green} radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Tabella per risorsa */}
      <ChartCard
        title="Performance per Risorsa"
        subtitle="Dettaglio workload e saving per buyer / operatore"
        loading={l2}
        empty={!perRisorsa?.length}
        emptyMessage="Nessun dato per il periodo selezionato"
      >
        <DataTable
          columns={[
            { key: 'risorsa',               label: 'Risorsa',        align: 'left' },
            { key: 'struttura',             label: 'Struttura',      align: 'left',
              render: v => v ? <Badge color="blue">{v}</Badge> : '—' },
            { key: 'pratiche_gestite',      label: 'Pratiche',       mono: true, render: v => fmtNum(v) },
            { key: 'pratiche_aperte',       label: 'Aperte',         mono: true, render: v => fmtNum(v) },
            { key: 'pratiche_chiuse',       label: 'Chiuse',         mono: true, render: v => fmtNum(v) },
            { key: 'saving_generato',       label: 'Saving',         mono: true,
              render: v => <span className="text-green-700 font-semibold">{fmtEur(v)}</span> },
            { key: 'negoziazioni_concluse', label: 'Negoz.',         mono: true, render: v => fmtNum(v) },
            { key: 'tempo_medio_giorni',    label: 'Tempo Medio',    mono: true, render: v => fmtDays(v) },
            { key: 'efficienza',            label: 'Efficienza',
              render: v => v ? <span className={v >= 80 ? 'text-green-600 font-semibold' : ''}>{fmtPct(v)}</span> : '—' },
          ]}
          rows={perRisorsa || []}
        />
      </ChartCard>
    </div>
  )
}
