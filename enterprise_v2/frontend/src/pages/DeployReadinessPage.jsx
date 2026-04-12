import Section from "../components/Section.jsx";

export default function DeployReadinessPage() {
  return (
    <>
      <Section title="Deploy Readiness">
        <ul>
          <li>Backend Dockerfile presente</li>
          <li>Frontend package.json presente</li>
          <li>Frontend Dockerfile presente</li>
          <li>Variabile VITE_API_BASE_URL da configurare su Render</li>
          <li>Health endpoint backend disponibile su /health</li>
          <li>Frontend collegato al backend via fetch</li>
        </ul>
      </Section>

      <Section title="Prossimi passaggi Render">
        <ol>
          <li>Creare un nuovo servizio backend separato puntando a enterprise_v2/backend</li>
          <li>Creare un nuovo servizio frontend separato puntando a enterprise_v2/frontend</li>
          <li>Impostare VITE_API_BASE_URL nel frontend</li>
          <li>Verificare /health del backend</li>
          <li>Aprire il frontend e testare Upload & Inspect e tutte le dashboard demo</li>
        </ol>
      </Section>
    </>
  );
}
