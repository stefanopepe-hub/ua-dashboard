import { useState } from "react";
import Section from "../components/Section.jsx";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

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
      <Section title="Health Check">
        <button onClick={checkHealth}>Verifica backend</button>
        {health && <pre style={{ marginTop: 12 }}>{JSON.stringify(health, null, 2)}</pre>}
      </Section>

      <Section title="Inspect Excel">
        <input type="file" accept=".xlsx,.xls" onChange={handleFile} />
        {loading && <p>Ispezione file in corso...</p>}
        {error && <p style={{ color: "crimson" }}>{error}</p>}
      </Section>

      {inspection && (
        <>
          <Section title="Riepilogo ispezione">
            <pre>{JSON.stringify({
              file_name: inspection.file_name,
              file_family: inspection.file_family,
              confidence_score: inspection.confidence_score,
              selected_sheet: inspection.selected_sheet,
              selected_header_row: inspection.selected_header_row
            }, null, 2)}</pre>
          </Section>

          <Section title="Campi mappati">
            <pre>{JSON.stringify(inspection.mapped_fields || {}, null, 2)}</pre>
          </Section>

          <Section title="Readiness">
            <pre>{JSON.stringify(inspection.readiness || {}, null, 2)}</pre>
          </Section>

          <Section title="Dettaglio sheets">
            <pre style={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(inspection.sheets || [], null, 2)}</pre>
          </Section>
        </>
      )}
    </>
  );
}
