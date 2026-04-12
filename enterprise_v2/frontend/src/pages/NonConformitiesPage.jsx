import { useEffect, useState } from "react";
import Section from "../components/Section.jsx";

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
      <Section title="Riepilogo Non Conformità">
        <pre>{JSON.stringify(data.summary, null, 2)}</pre>
      </Section>
      <Section title="Top Fornitori NC">
        <pre>{JSON.stringify(data.suppliers, null, 2)}</pre>
      </Section>
      <Section title="Tipologie NC">
        <pre>{JSON.stringify(data.types, null, 2)}</pre>
      </Section>
      <Section title="Trend Mensile">
        <pre>{JSON.stringify(data.trend, null, 2)}</pre>
      </Section>
    </>
  );
}
