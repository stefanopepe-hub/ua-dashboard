import { useState } from "react";
import InspectPage from "./pages/InspectPage.jsx";
import SavingDashboardPage from "./pages/SavingDashboardPage.jsx";
import ResourcesPage from "./pages/ResourcesPage.jsx";
import CycleTimesPage from "./pages/CycleTimesPage.jsx";
import NonConformitiesPage from "./pages/NonConformitiesPage.jsx";
import DeployReadinessPage from "./pages/DeployReadinessPage.jsx";

const NAV_ITEMS = [
  ["inspect", "Upload & Inspect"],
  ["saving", "Saving Dashboard"],
  ["resources", "Risorse"],
  ["cycle", "Tempi Attraversamento"],
  ["nc", "Non Conformità"],
  ["deploy", "Deploy Readiness"],
];

export default function App() {
  const [page, setPage] = useState("inspect");

  const renderPage = () => {
    if (page === "inspect") return <InspectPage />;
    if (page === "saving") return <SavingDashboardPage />;
    if (page === "resources") return <ResourcesPage />;
    if (page === "cycle") return <CycleTimesPage />;
    if (page === "nc") return <NonConformitiesPage />;
    return <DeployReadinessPage />;
  };

  return (
    <div style={{ display: "grid", gridTemplateColumns: "260px 1fr", minHeight: "100vh", background: "#f8fafc" }}>
      <aside style={{ background: "#0f172a", color: "#fff", padding: 24 }}>
        <h1 style={{ fontSize: 22, marginTop: 0 }}>Telethon Enterprise V2</h1>
        <p style={{ color: "#cbd5e1", fontSize: 14, lineHeight: 1.5 }}>
          Procurement Intelligence Platform
        </p>
        <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 24 }}>
          {NAV_ITEMS.map(([key, label]) => (
            <button
              key={key}
              onClick={() => setPage(key)}
              style={{
                textAlign: "left",
                padding: "12px 14px",
                borderRadius: 12,
                border: "1px solid rgba(255,255,255,0.08)",
                background: page === key ? "#1d4ed8" : "#111827",
                color: "#fff",
                cursor: "pointer"
              }}
            >
              {label}
            </button>
          ))}
        </div>
      </aside>

      <main style={{ padding: 28 }}>
        <div style={{ marginBottom: 20 }}>
          <h2 style={{ margin: 0, color: "#0f172a" }}>Enterprise MVP Accelerato</h2>
          <p style={{ color: "#475569" }}>
            Nuova piattaforma separata dall'app attuale, orientata a import intelligente e analytics operative.
          </p>
        </div>
        {renderPage()}
      </main>
    </div>
  );
}
