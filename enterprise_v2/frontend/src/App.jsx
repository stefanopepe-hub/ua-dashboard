import { useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

function Section({ title, children }) {
  return (
    <div style={{ border: "1px solid #ddd", borderRadius: 12, padding: 16, marginBottom: 16, background: "#fff" }}>
      <h2 style={{ marginTop: 0 }}>{title}</h2>
      {children}
    </div>
  );
}

function KeyValue({ label, value }) {
  return (
    <div style={{ marginBottom: 8 }}>
      <strong>{label}: </strong>
      <span>{String(value ?? "")}</span>
    </div>
  );
}

export default function App() {
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
    <div style={{ fontFamily: "Arial, sans-serif", padding: 24, background: "#f6f7fb", minHeight: "100vh" }}>
      <h1>Telethon Enterprise V2</h1>
      <p>Nuova piattaforma enterprise separata dall'app attuale.</p>

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
          <Section title="Riepilogo">
            <KeyValue label="File" value={inspection.file_name} />
            <KeyValue label="Famiglia rilevata" value={inspection.file_family} />
            <KeyValue label="Confidence score" value={inspection.confidence_score} />
            <KeyValue label="Foglio selezionato" value={inspection.selected_sheet} />
            <KeyValue label="Header row" value={inspection.selected_header_row} />
          </Section>

          <Section title="Campi mappati">
            <pre>{JSON.stringify(inspection.mapped_fields || {}, null, 2)}</pre>
          </Section>

          <Section title="Readiness">
            <pre>{JSON.stringify(inspection.readiness || {}, null, 2)}</pre>
          </Section>

          <Section title="Fogli del workbook">
            <pre>{JSON.stringify(inspection.sheet_names || [], null, 2)}</pre>
          </Section>

          <Section title="Dettaglio sheets">
            <pre style={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(inspection.sheets || [], null, 2)}</pre>
          </Section>
        </>
      )}
    </div>
  );
}
