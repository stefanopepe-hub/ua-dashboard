import { useEffect, useState } from "react";
import Section from "../components/Section.jsx";
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

export default function SuppliersPage() {
  const [rows, setRows] = useState([]);

  useEffect(() => {
    fetchJson("/analytics/saving/top-suppliers").then((res) => setRows(res.data || []));
  }, []);

  return (
    <Section title="Fornitori" subtitle="Vista sintetica dei principali fornitori">
      <DataTable
        columns={[
          { key: "supplier_name", label: "Fornitore" },
          { key: "committed_amount", label: "Impegnato" },
          { key: "accredited", label: "Albo" },
        ]}
        rows={rows.map((row) => ({
          ...row,
          committed_amount: euro(row.committed_amount),
          accredited: row.accredited ? "Sì" : "No",
        }))}
      />
    </Section>
  );
}
