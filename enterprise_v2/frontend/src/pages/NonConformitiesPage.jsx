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

export default function NonConformitiesPage() {
  const [data, setData] = useState({ summary: null, suppliers: [], types: [], trend: [] });

  useEffect(() => {
    Promise.all([
      fetchJson("/analytics/nc/summary"),
      fetchJson("/analytics/nc/top-suppliers"),
      fetchJson("/analytics/nc/types"),
      fetchJson("/analytics/nc/monthly-trend")
    ]).then(([summary, suppliers, types, trend]) => {
      setData({
        summary: summary.data,
        suppliers: suppliers.data || [],
        types: types.data || [],
        trend: trend.data || []
      });
    });
  }, []);

  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 16, marginBottom: 18 }}>
        <KpiCard title="Record" value={data.summary?.records ?? "-"} subtitle="NC analizzate" accent="#2563eb" />
        <KpiCard title="Aperte" value={data.summary?.open_cases ?? "-"} subtitle="Casi aperti" accent="#dc2626" />
        <KpiCard title="Chiuse" value={data.summary?.closed_cases ?? "-"} subtitle="Casi chiusi" accent="#16a34a" />
        <KpiCard title="Tempo chiusura" value={data.summary ? `${data.summary.avg_closure_days} gg` : "-"} subtitle="Media" accent="#ea580c" />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 18 }}>
        <Section title="Top fornitori NC">
          <DataTable
            columns={[
              { key: "supplier_name", label: "Fornitore" },
              { key: "nc_count", label: "NC" },
            ]}
            rows={data.suppliers}
          />
        </Section>
        <Section title="Tipologie NC">
          <div style={{ width: "100%", height: 320 }}>
            <ResponsiveContainer>
              <BarChart data={data.types}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="type" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="count" name="Conteggio" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Section>
      </div>

      <Section title="Trend mensile">
        <div style={{ width: "100%", height: 320 }}>
          <ResponsiveContainer>
            <LineChart data={data.trend}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="month" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="count" name="NC" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Section>
    </>
  );
}
