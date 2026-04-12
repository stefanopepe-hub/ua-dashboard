import { useEffect, useState } from "react";
import { ResponsiveContainer, BarChart, Bar, CartesianGrid, Tooltip, XAxis, YAxis } from "recharts";
import Section from "../components/Section.jsx";
import DataTable from "../components/DataTable.jsx";
import { API_BASE } from "../config.js";

async function fetchJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  return res.json();
}

export default function DocumentTypesPage() {
  const [rows, setRows] = useState([]);

  useEffect(() => {
    fetchJson("/analytics/saving/document-types").then((res) => setRows(res.data || []));
  }, []);

  return (
    <>
      <Section title="Tipologie Documentali" subtitle="Volumi e numerosità per tipologia">
        <div style={{ width: "100%", height: 320 }}>
          <ResponsiveContainer>
            <BarChart data={rows}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="code" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="count" name="Conteggio" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Section>

      <Section title="Dettaglio tipologie">
        <DataTable
          columns={[
            { key: "code", label: "Codice" },
            { key: "label", label: "Descrizione" },
            { key: "count", label: "Conteggio" },
            { key: "committed_amount", label: "Impegnato" },
          ]}
          rows={rows}
        />
      </Section>
    </>
  );
}
