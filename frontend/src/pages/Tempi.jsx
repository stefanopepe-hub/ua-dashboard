import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { fmtDays, fmtNum, fmtPct, shortMese, COLORS } from '../utils/fmt'
import {
  KpiCard, LoadingBox, SectionTitle, ChartCard,
  DataTable, PageHeader, EmptyState,
} from '../components/UI'

export default function Tempi() {
  const { data: riepilogo, loading: l1, error: e1 } = useKpi(() => api.tempiRiepilogo(), [])
  const { data: mensile,   loading: l2, error: e2 } = useKpi(() => api.tempiMensile(), [])
  const { data: dist,      loading: l3             } = useKpi(() => api.tempiDist(), [])

  const mensileChart = (mensile || []).map(m => ({
    name:         shortMese(m.mese),
    'UA':         m.avg_purchasing ?? 0,
    'Automatico': m.avg_auto ?? 0,
    'Altro':      m.avg_other ?? 0,
    'Totale gg':  m.avg_total ?? 0,
    'N° Ordini':  m.n_ordini ?? 0,
  }))

  const bottleneckChart = (mensile || []).map(m => ({
    name:        shortMese(m.mese),
    PURCHASING:  m.n_bottleneck_purchasing ?? 0,
    AUTO:        m.n_bottleneck_auto ?? 0,
    OTHER:       m.n_bottleneck_other ?? 0,
  }))

  const hasData = !l1 && riepilogo && riepilogo.n_ordini > 0

  return (
    <div className="space-y-6">
      <PageHeader
        title="Tempi di Attraversamento"
        subtitle="Analisi dei tempi per fase di elaborazione degli ordini"
      />

      {/* KPI */}
      {l1 ? (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          {[...Array(6)].map((_, i) => (
            <div key={i} className="kpi-card"><div className="skeleton h-16 rounded-lg" /></div>
          ))}
        </div>
      ) : !hasData ? (
        <div className="card">
          <EmptyState
            title="Dati tempi non disponibili"
            message="Carica il file Tempi Attraversamento dalla sezione 'Carica Dati' per visualizzare questa analisi."
          />
        </div>
      ) : (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <KpiCard label="Tempo Medio Tot."  value={fmtDays(riepilogo.avg_total_days)} color="blue"  sub="per ordine" />
          <KpiCard label="Fase Acquisti"     value={fmtDays(riepilogo.avg_purchasing)} color="orange" sub="UA" />
          <KpiCard label="Fase Automatica"   value={fmtDays(riepilogo.avg_auto)}       color="blue"   sub="sistema" />
          <KpiCard label="Fase Altro"        value={fmtDays(riepilogo.avg_other)}      color="gray"   sub="altre fasi" />
          <KpiCard label="Bottleneck UA"     value={fmtPct(riepilogo.perc_bottleneck_purchasing)} color="red" sub="ordini con ritardo" />
          <KpiCard label="N° Ordini"         value={fmtNum(riepilogo.n_ordini)}        color="blue" />
        </div>
      )}

      {/* Tempo per fase */}
      <ChartCard
        title="Tempo Medio per Fase (giorni)"
        subtitle="Composizione mensile del tempo di attraversamento per fase"
        loading={l2}
        empty={mensileChart.length === 0}
        emptyMessage="Nessun dato disponibile — carica il file Tempi Attraversamento"
      >
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={mensileChart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
            <XAxis dataKey="name" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} unit=" gg" />
            <Tooltip formatter={(v, n) => [fmtDays(v), n]} />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Bar dataKey="UA"         stackId="a" fill={COLORS.red} />
            <Bar dataKey="Automatico" stackId="a" fill={COLORS.blue} />
            <Bar dataKey="Altro"      stackId="a" fill={COLORS.gray} radius={[2, 2, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Trend totale */}
      <ChartCard
        title="Andamento Tempo Medio Totale"
        subtitle="Trend mensile del tempo complessivo di attraversamento"
        loading={l2}
        empty={mensileChart.length === 0}
      >
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={mensileChart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
            <XAxis dataKey="name" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} unit=" gg" />
            <Tooltip formatter={v => [fmtDays(v), 'Giorni totali']} />
            <Line type="monotone" dataKey="Totale gg" stroke={COLORS.blue} strokeWidth={2.5} dot={{ r: 3 }} />
          </LineChart>
        </ResponsiveContainer>
      </ChartCard>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Bottleneck */}
        <ChartCard
          title="Bottleneck per Mese"
          subtitle="N° ordini con ritardo per fase"
          loading={l2}
          empty={bottleneckChart.every(r => !r.PURCHASING && !r.AUTO)}
        >
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={bottleneckChart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis dataKey="name" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="PURCHASING" fill={COLORS.red}  radius={[2, 2, 0, 0]} />
              <Bar dataKey="AUTO"       fill={COLORS.blue} radius={[2, 2, 0, 0]} />
              <Bar dataKey="OTHER"      fill={COLORS.gray} radius={[2, 2, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Distribuzione fasce */}
        <ChartCard
          title="Distribuzione per Fascia di Tempo"
          subtitle="N° ordini per range di giorni totali"
          loading={l3}
          empty={!dist?.length}
        >
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={dist || []} layout="vertical" margin={{ top: 4, right: 16, left: 48, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis dataKey="fascia" type="category" tick={{ fontSize: 11 }} width={56} />
              <Tooltip formatter={v => [fmtNum(v), 'Ordini']} />
              <Bar dataKey="n_ordini" fill={COLORS.blue} radius={[0, 3, 3, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Tabella dettaglio */}
      <ChartCard
        title="Dettaglio Mensile per Fase"
        loading={l2}
        empty={mensileChart.length === 0}
      >
        <DataTable
          columns={[
            { key: 'name',       label: 'Mese',       align: 'left' },
            { key: 'Totale gg',  label: 'Tot. Giorni', render: v => fmtDays(v), mono: true },
            { key: 'UA',         label: 'Fase UA',     render: v => fmtDays(v), mono: true },
            { key: 'Automatico', label: 'Fase Auto',   render: v => fmtDays(v), mono: true },
            { key: 'Altro',      label: 'Altro',       render: v => fmtDays(v), mono: true },
            { key: 'N° Ordini',  label: 'N° Ordini',   render: v => fmtNum(v),  mono: true },
          ]}
          rows={mensileChart}
        />
      </ChartCard>
    </div>
  )
}
