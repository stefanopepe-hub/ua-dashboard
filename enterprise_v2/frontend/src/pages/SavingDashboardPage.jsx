import { useEffect, useState } from "react";
import Section from "../components/Section.jsx";

import { API_BASE } from "../config.js";

async function fetchJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  return res.json();
}

export default function SavingDashboardPage() {
  const [data, setData] = useState({
    summary: null,
    topSuppliers: [],
    documentTypes: [],
    cdc: [],
    buyers: [],
    protocols: [],
    yoy: [],
  });

  useEffect(() => {
    Promise.all([
      fetchJson("/analytics/saving/summary"),
      fetchJson("/analytics/saving/top-suppliers"),
      fetchJson("/analytics/saving/document-types"),
      fetchJson("/analytics/saving/cdc"),
      fetchJson("/analytics/saving/buyers"),
      fetchJson("/analytics/saving/protocols"),
      fetchJson("/analytics/saving/yoy"),
    ]).then(([summary, topSuppliers, documentTypes, cdc, buyers, protocols, yoy]) => {
      setData({
        summary: summary.data,
        topSuppliers: topSuppliers.data || [],
        documentTypes: documentTypes.data || [],
        cdc: cdc.data || [],
        buyers: buyers.data || [],
        protocols: protocols.data || [],
        yoy: yoy.data || [],
      });
    });
  }, []);

  return (
    <>
      <Section title="KPI Saving & Ordini">
        <pre>{JSON.stringify(data.summary, null, 2)}</pre>
      </Section>

      <Section title="Top Fornitori">
        <pre>{JSON.stringify(data.topSuppliers, null, 2)}</pre>
      </Section>

      <Section title="Tipologie Documentali">
        <pre>{JSON.stringify(data.documentTypes, null, 2)}</pre>
      </Section>

      <Section title="CDC">
        <pre>{JSON.stringify(data.cdc, null, 2)}</pre>
      </Section>

      <Section title="Buyer">
        <pre>{JSON.stringify(data.buyers, null, 2)}</pre>
      </Section>

      <Section title="Protocolli">
        <pre>{JSON.stringify(data.protocols, null, 2)}</pre>
      </Section>

      <Section title="YoY">
        <pre>{JSON.stringify(data.yoy, null, 2)}</pre>
      </Section>
    </>
  );
}
