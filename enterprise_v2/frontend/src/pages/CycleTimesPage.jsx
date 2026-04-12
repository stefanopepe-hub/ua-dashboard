import { useEffect, useState } from "react";
import Section from "../components/Section.jsx";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

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
      <Section title="Riepilogo Tempi">
        <pre>{JSON.stringify(data.summary, null, 2)}</pre>
      </Section>
      <Section title="Bottleneck">
        <pre>{JSON.stringify(data.bottlenecks, null, 2)}</pre>
      </Section>
      <Section title="Trend Mensile">
        <pre>{JSON.stringify(data.trend, null, 2)}</pre>
      </Section>
    </>
  );
}
