import { useEffect, useState } from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  CartesianGrid,
  Tooltip,
  XAxis,
  YAxis,
  PieChart,
  Pie,
  Cell,
  LineChart,
  Line,
} from "recharts";
import Section from "../components/Section.jsx";
import KpiCard from "../components/KpiCard.jsx";
import DataTable from "../components/DataTable.jsx";
import { API_BASE } from "../config.js";

async function fetchJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  return res.json();
}

function euro(value) {
  if (value == null) return "-";
  return `€${Number(value).toLocaleString("it-IT")}`;
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

  const summaryCards = [
    { title: "Listino", value: euro(data.summary?.list_amount), subtitle: "Valore listino", accent: "#94a3b8" },
    { title: "Impegnato", value: euro(data.summary?.committed_amount), subtitle: "Valore impegnato", accent: "#2563eb" },
    { title: "Saving", value: euro(data.summary?.saving_amount), subtitle: `${data.summary?.saving_percent ?? "-"}%`, accent: "#16a34a" },
    { title: "Fornitori", value: data.summary?.suppliers_count ?? "-", subtitle: "Parco fornitori", accent: "#7c3aed" },
  ];

  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 16, marginBottom: 18 }}>
        {summaryCards.map((card) => <KpiCard key={card.title} {...card} />)}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1.2fr 1fr", gap: 18 }}>
        <Section title="Top Fornitori" subtitle="Primi fornitori per volume acquistato">
          <DataTable
            columns={[
              { key: "supplier_name", label: "Fornitore" },
              { key: "committed_amount", label: "Impegnato" },
              { key: "accredited", label: "Albo" },
            ]}
            rows={data.topSuppliers.map((row) => ({
              ...row,
              committed_amount: euro(row.committed_amount),
              accredited: row.accredited ? "Sì" : "No",
            }))}
          />
        </Section>

        <Section title="CDC" subtitle="Distribuzione per centro di costo">
          <div style={{ width: "100%", height: 320 }}>
            <ResponsiveContainer>
              <BarChart data={data.cdc}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="cdc" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="committed_amount" name="Impegnato" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Section>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 18 }}>
        <Section title="Tipologie Documentali" subtitle="Distribuzione documenti">
          <div style={{ width: "100%", height: 320 }}>
            <ResponsiveContainer>
              <PieChart>
                <Pie data={data.documentTypes} dataKey="count" nameKey="code" outerRadius={110}>
                  {data.documentTypes.map((entry, index) => <Cell key={index} />)}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </Section>

        <Section title="Saving YoY" subtitle="Trend storico">
          <div style={{ width: "100%", height: 320 }}>
            <ResponsiveContainer>
              <LineChart data={data.yoy}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="year" />
                <YAxis />
                <Tooltip />
                <Line type="monotone" dataKey="saving_amount" name="Saving" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Section>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 18 }}>
        <Section title="Buyer">
          <DataTable
            columns={[
              { key: "buyer_name", label: "Buyer" },
              { key: "committed_amount", label: "Impegnato" },
            ]}
            rows={data.buyers.map((row) => ({ ...row, committed_amount: euro(row.committed_amount) }))}
          />
        </Section>

        <Section title="Protocolli">
          <DataTable
            columns={[
              { key: "protocol", label: "Protocollo" },
              { key: "committed_amount", label: "Impegnato" },
            ]}
            rows={data.protocols.map((row) => ({ ...row, committed_amount: euro(row.committed_amount) }))}
          />
        </Section>
      </div>
    </>
  );
}
