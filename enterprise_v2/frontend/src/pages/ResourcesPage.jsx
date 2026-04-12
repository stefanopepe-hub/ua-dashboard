import { useEffect, useState } from "react";
import Section from "../components/Section.jsx";

import { API_BASE } from "../config.js";

async function fetchJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  return res.json();
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
      <Section title="Riepilogo Risorse">
        <pre>{JSON.stringify(data.summary, null, 2)}</pre>
      </Section>
      <Section title="Team">
        <pre>{JSON.stringify(data.resources, null, 2)}</pre>
      </Section>
      <Section title="Trend Mensile">
        <pre>{JSON.stringify(data.trend, null, 2)}</pre>
      </Section>
    </>
  );
}
