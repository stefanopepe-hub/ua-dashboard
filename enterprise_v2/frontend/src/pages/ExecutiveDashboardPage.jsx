import { useEffect, useState } from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  CartesianGrid,
  Tooltip,
  XAxis,
  YAxis,
  LineChart,
  Line,
} from "recharts";
import Section from "../components/Section.jsx";
import KpiCard from "../components/KpiCard.jsx";
import { API_BASE } from "../config.js";

async function fetchJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  return res.json();
}

function euro(value) {
  if (value == null) return "-";
  return `€${Number(value).toLocaleString("it-IT")}`;
}

export default function ExecutiveDashboardPage() {
  const [state, setState] = useState({
    savingSummary: null,
    cycleSummary: null,
    ncSummary: null,
    resourcesSummary: null,
    yoy: [],
  });

  useEffect(() => {
    Promise.all([
      fetchJson("/analytics/saving/summary"),
      fetchJson("/analytics/cycle/summary"),
      fetchJson("/analytics/nc/summary"),
      fetchJson("/analytics/resources/summary"),
      fetchJson("/analytics/saving/yoy"),
    ]).then(([savingSummary, cycleSummary, ncSummary, resourcesSummary, yoy]) => {
      setState({
        savingSummary: savingSummary.data,
        cycleSummary: cycleSummary.data,
        ncSummary: ncSummary.data,
        resourcesSummary: resourcesSummary.data,
        yoy: yoy.data || [],
      });
    });
  }, []);

  const cards = [
    { title: "Saving", value: euro(state.savingSummary?.saving_amount), subtitle: "Saving complessivo", accent: "#2563eb" },
    { title: "Impegnato", value: euro(state.savingSummary?.committed_amount), subtitle: "Volume impegnato", accent: "#0f766e" },
    { title: "Tempo medio", value: state.cycleSummary ? `${state.cycleSummary.avg_total_days} gg` : "-", subtitle: "Attraversamento", accent: "#ea580c" },
    { title: "Non conformità aperte", value: state.ncSummary?.open_cases ?? "-", subtitle: "Casi aperti", accent: "#dc2626" },
    { title: "Team size", value: state.resourcesSummary?.team_size ?? "-", subtitle: "Risorse attive", accent: "#7c3aed" },
    { title: "Documenti gestiti", value: state.resourcesSummary?.documents_handled ?? "-", subtitle: "Capacità operativa", accent: "#0891b2" },
  ];

  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 16, marginBottom: 18 }}>
        {cards.map((card) => <KpiCard key={card.title} {...card} />)}
      </div>

      <Section title="Saving YoY" subtitle="Andamento saving e impegnato per anno">
        <div style={{ width: "100%", height: 320 }}>
          <ResponsiveContainer>
            <BarChart data={state.yoy}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="year" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="saving_amount" name="Saving" />
              <Bar dataKey="committed_amount" name="Impegnato" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Section>
    </>
  );
}
