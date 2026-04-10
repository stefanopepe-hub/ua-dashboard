import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { useKpi } from '../hooks/useKpi'
import { api } from '../utils/api'
import { fmtPct, fmtNum, fmtDays, shortMese, COLORS } from '../utils/fmt'
import {
  KpiCard, LoadingBox, ErrorBox, SectionTitle,
  DataTable, Badge, ChartCard, PageHeader, EmptyState,
} from '../components/UI'

export default function NonConformita() {
  const { data: riepilogo, loading: l1, error: e1 } = useKpi(() => api.ncRiepilogo(), [])
  const { data: mensile,   loading: l2              } = useKpi(() => api.ncMensile(), [])
  const { data: topForn,   loading: l3              } = useKpi(() => api.ncTopFornitori(), [])
  const { data: perTipo,   loading: l4              } = useKpi(() => api.ncPerTipo(), [])

  const mensileChart = (mensile || []).map(m => ({
    name:         shortMese(m.mese),
    'N° NC':      m.n_nc ?? 0,
    'N° Totale':  m.n_totale ?? 0,
    '% NC':       m.perc_nc ?? 0,
    'Delta Gg':   m.avg_delta ?? m.avg_delta_giorni ?? 0,
  }))

  const hasNoData = !l1 && !riepilogo?.n_totale

  return (
    <div className="space-y-6">
      <PageHeader
        title="Non Conformità"
        subtitle="Analisi delle non conformità sui documenti di acquisto"
      />

      {e1 && <ErrorBox message={e1} title="Errore caricamento dati" />}

      {/* KPI */}
      {l1 ? (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="kpi-card"><div className="skeleton h-16 rounded-lg" /></div>
          ))}
        </div>
      ) : riepilogo && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KpiCard label="Documenti Analizzati" value={fmtNum(riepilogo.n_totale)}  color="blue" />
          <KpiCard label="Non Conformità"        value={fmtNum(riepilogo.n_nc)}     color="red"
            sub="totale NC rilevate" />
          <KpiCard label="% NC"                  value={fmtPct(riepilogo.perc_nc)}  color="red"
            sub="sul totale documenti" />
          <KpiCard label="Delta Medio"           value={fmtDays(riepilogo.avg_delta_giorni)}
            color="orange" sub="origine → fattura" />
        </div>
      )}

      {/* Andamento mensile */}
      <ChartCard
        title="Andamento Mensile — Non Conformità"
        subtitle="Documenti totali vs documenti non conformi per mese"
        loading={l2}
        empty={mensileChart.length === 0}
        emptyMessage="Carica il file Non Conformità per visualizzare questa analisi"
      >
        <ResponsiveContainer width="100%" height={240}>
          <BarChart data={mensileChart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
            <XAxis dataKey="name" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Bar dataKey="N° Totale" fill="#e5e7eb" radius={[2, 2, 0, 0]} />
            <Bar dataKey="N° NC"     fill={COLORS.red} radius={[2, 2, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* % NC mensile */}
        <ChartCard
          title="% Non Conformità Mensile"
          subtitle="Percentuale di documenti NC per mese"
          loading={l2}
          empty={mensileChart.length === 0}
        >
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={mensileChart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis dataKey="name" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} unit="%" />
              <Tooltip formatter={v => [fmtPct(v), '% NC']} />
              <Line type="monotone" dataKey="% NC" stroke={COLORS.red} strokeWidth={2} dot={{ r: 3 }} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Delta giorni mensile */}
        <ChartCard
          title="Delta Medio Giorni — Origine → Fattura"
          subtitle="Numero medio di giorni tra data origine e prima fattura"
          loading={l2}
          empty={mensileChart.length === 0}
        >
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={mensileChart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis dataKey="name" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} unit=" gg" />
              <Tooltip formatter={v => [fmtDays(v), 'Delta gg']} />
              <Line type="monotone" dataKey="Delta Gg" stroke={COLORS.orange} strokeWidth={2} dot={{ r: 3 }} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* NC per tipo origine */}
      <ChartCard
        title="Non Conformità per Tipo Origine"
        subtitle="Distribuzione delle NC per categoria"
        loading={l4}
        empty={!perTipo?.length}
        emptyMessage="Nessun dato per tipo origine disponibile"
      >
        <DataTable
          columns={[
            { key: 'tipo',       label: 'Tipo Origine', align: 'left' },
            { key: 'n_totale',   label: 'N° Tot.',      render: v => fmtNum(v), mono: true },
            { key: 'n_nc',       label: 'N° NC',        render: v => fmtNum(v), mono: true },
            { key: 'perc_nc',    label: '% NC',
              render: v => <span className={v > 10 ? 'text-red-600 font-semibold' : ''}>{fmtPct(v)}</span> },
            { key: 'avg_delta',  label: 'Delta Medio',  render: v => fmtDays(v) },
          ]}
          rows={perTipo || []}
        />
      </ChartCard>

      {/* Top fornitori NC */}
      <ChartCard
        title="Top 10 Fornitori per N° Non Conformità"
        subtitle="Fornitori con maggior numero di NC rilevate"
        loading={l3}
        empty={!topForn?.length}
        emptyMessage="Nessun dato fornitori disponibile"
      >
        <DataTable
          columns={[
            { key: 'ragione_sociale', label: 'Fornitore', align: 'left' },
            { key: 'n_totale',        label: 'N° Doc.',   render: v => fmtNum(v), mono: true },
            { key: 'n_nc',            label: 'N° NC',     render: v => fmtNum(v), mono: true },
            { key: 'perc_nc',         label: '% NC',
              render: v => <Badge color={v > 20 ? 'red' : v > 10 ? 'orange' : 'gray'}>{fmtPct(v)}</Badge> },
            { key: 'avg_delta',       label: 'Delta Gg',  render: v => fmtDays(v) },
          ]}
          rows={topForn || []}
        />
      </ChartCard>
    </div>
  )
}
