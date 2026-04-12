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

export default function CycleTimesPage() {
  const [data, setData] = useState({ summary: null, bottlenecks: [], trend: [] });

  useEffect(() => {
    Promise.all([
      fetchJson("/analytics/cycle/summary"),
      fetchJson("/analytics/cycle/bottlenecks"),
      fetchJson("/analytics/cycle/monthly-trend")
    ]).then(([summary, bottlenecks, trend]) => {
      setData({
        summary: summary.data,
        bottlenecks: bottlenecks.data || [],
        trend: trend.data || []
      });
    });
  }, []);

  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 16, marginBottom: 18 }}>
        <KpiCard title="Record" value={data.summary?.records ?? "-"} subtitle="Passaggi" accent="#2563eb" />
        <KpiCard title="Tempo medio totale" value={data.summary ? `${data.summary.avg_total_days} gg` : "-"} subtitle="Total days" accent="#ea580c" />
        <KpiCard title="Tempo UA" value={data.summary ? `${data.summary.avg_purchasing_days} gg` : "-"} subtitle="Purchasing" accent="#0f766e" />
        <KpiCard title="Tempo automatico" value={data.summary ? `${data.summary.avg_automatic_days} gg` : "-"} subtitle="Auto" accent="#7c3aed" />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 18 }}>
        <Section title="Bottleneck">
          <div style={{ width: "100%", height: 320 }}>
            <ResponsiveContainer>
              <BarChart data={data.bottlenecks}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="stage" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="avg_days" name="Giorni medi" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Section>
        <Section title="Trend mensile">
          <div style={{ width: "100%", height: 320 }}>
            <ResponsiveContainer>
              <LineChart data={data.trend}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="month" />
                <YAxis />
                <Tooltip />
                <Line type="monotone" dataKey="avg_days" name="Tempo medio" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Section>
      </div>
    </>
  );
}
