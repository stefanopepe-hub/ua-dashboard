import { useEffect, useState } from "react";
import { ResponsiveContainer, BarChart, Bar, CartesianGrid, Tooltip, XAxis, YAxis, LineChart, Line } from "recharts";
import Section from "../components/Section.jsx";
import KpiCard from "../components/KpiCard.jsx";
import DataTable from "../components/DataTable.jsx";
import { API_BASE } from "../config.js";

async function fetchJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  return res.json();
}

function euro(value) {
  return `€${Number(value || 0).toLocaleString("it-IT")}`;
}

export default function ResourcesPage() {
  const [data, setData] = useState({ summary: null, resources: [], trend: [] });

  useEffect(() => {
    Promise.all([
      fetchJson("/analytics/resources/summary"),
      fetchJson("/analytics/resources/list"),
      fetchJson("/analytics/resources/monthly-trend")
    ]).then(([summary, resources, trend]) => {
      setData({
        summary: summary.data,
        resources: resources.data || [],
        trend: trend.data || []
      });
    });
  }, []);

  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 16, marginBottom: 18 }}>
        <KpiCard title="Team size" value={data.summary?.team_size ?? "-"} subtitle="Risorse attive" accent="#7c3aed" />
        <KpiCard title="Documenti gestiti" value={data.summary?.documents_handled ?? "-"} subtitle="Volume operativo" accent="#2563eb" />
        <KpiCard title="Saving generato" value={euro(data.summary?.saving_generated)} subtitle="Performance team" accent="#16a34a" />
        <KpiCard title="Tempo medio" value={data.summary ? `${data.summary.avg_cycle_time_days} gg` : "-"} subtitle="Attraversamento" accent="#ea580c" />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 18 }}>
        <Section title="Team">
          <DataTable
            columns={[
              { key: "resource_name", label: "Risorsa" },
              { key: "documents_handled", label: "Pratiche" },
              { key: "saving_generated", label: "Saving" },
              { key: "backlog", label: "Backlog" },
            ]}
            rows={data.resources.map((row) => ({ ...row, saving_generated: euro(row.saving_generated) }))}
          />
        </Section>
        <Section title="Trend mensile">
          <div style={{ width: "100%", height: 320 }}>
            <ResponsiveContainer>
              <LineChart data={data.trend}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="month" />
                <YAxis />
                <Tooltip />
                <Line type="monotone" dataKey="documents_handled" name="Documenti" />
                <Line type="monotone" dataKey="saving_generated" name="Saving" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Section>
      </div>
    </>
  );
}
