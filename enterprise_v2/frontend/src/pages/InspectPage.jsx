import { useState } from "react";
import Section from "../components/Section.jsx";
import DataTable from "../components/DataTable.jsx";
import KpiCard from "../components/KpiCard.jsx";
import { API_BASE } from "../config.js";

export default function InspectPage() {
  const [health, setHealth] = useState(null);
  const [inspection, setInspection] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const checkHealth = async () => {
    setError("");
    const res = await fetch(`${API_BASE}/health`);
    const data = await res.json();
    setHealth(data);
  };

  const handleFile = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    setLoading(true);
    setError("");
    setInspection(null);

    try {
      const formData = new FormData();
      formData.append("file", file);
      const res = await fetch(`${API_BASE}/inspect-excel`, {
        method: "POST",
        body: formData,
      });
      const data = await res.json();
      if (!data.ok && data.error_message) {
        setError(data.error_message);
      }
      setInspection(data);
    } catch (e) {
      setError(`Errore di collegamento al backend: ${e}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
      <Section title="Upload & Inspect" subtitle="Carica un file Excel e verifica famiglia, foglio selezionato, mapping e readiness">
        <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 12, flexWrap: "wrap" }}>
          <button onClick={checkHealth} style={{ padding: "10px 14px", borderRadius: 10, cursor: "pointer" }}>
            Verifica backend
          </button>
          <input type="file" accept=".xlsx,.xls" onChange={handleFile} />
        </div>
        {health && <pre style={{ background: "#f8fafc", padding: 12, borderRadius: 12 }}>{JSON.stringify(health, null, 2)}</pre>}
        {loading && <p>Ispezione file in corso...</p>}
        {error && <p style={{ color: "crimson" }}>{error}</p>}
      </Section>

      {inspection && (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 16, marginBottom: 18 }}>
            <KpiCard title="Famiglia file" value={inspection.file_family || "-"} subtitle="Classifier" accent="#2563eb" />
            <KpiCard title="Confidence" value={inspection.confidence_score ?? "-"} subtitle="0 - 1" accent="#16a34a" />
            <KpiCard title="Foglio" value={inspection.selected_sheet || "-"} subtitle="Best sheet" accent="#7c3aed" />
            <KpiCard title="Header row" value={inspection.selected_header_row ?? "-"} subtitle="Detected header" accent="#ea580c" />
          </div>

          <Section title="Campi mappati">
            <DataTable
              columns={[
                { key: "field", label: "Campo canonico" },
                { key: "column", label: "Colonna sorgente" },
              ]}
              rows={Object.entries(inspection.mapped_fields || {}).map(([field, column]) => ({ field, column }))}
            />
          </Section>

          <Section title="Readiness">
            <pre style={{ background: "#f8fafc", padding: 12, borderRadius: 12 }}>{JSON.stringify(inspection.readiness || {}, null, 2)}</pre>
          </Section>

          <Section title="Dettaglio sheets">
            <pre style={{ whiteSpace: "pre-wrap", background: "#f8fafc", padding: 12, borderRadius: 12 }}>
              {JSON.stringify(inspection.sheets || [], null, 2)}
            </pre>
          </Section>
        </>
      )}
    </>
  );
}
