import { useState } from "react";
import InspectPage from "./pages/InspectPage.jsx";
import SavingDashboardPage from "./pages/SavingDashboardPage.jsx";
import ResourcesPage from "./pages/ResourcesPage.jsx";
import CycleTimesPage from "./pages/CycleTimesPage.jsx";
import NonConformitiesPage from "./pages/NonConformitiesPage.jsx";

export default function App() {
  const [page, setPage] = useState("inspect");
  const buttonStyle = { padding: "8px 12px" };

  return (
    <div style={{ fontFamily: "Arial, sans-serif", padding: 24, background: "#f6f7fb", minHeight: "100vh" }}>
      <h1>Telethon Enterprise V2</h1>
      <p>Nuova piattaforma enterprise separata dall'app attuale.</p>

      <div style={{ display: "flex", gap: 12, marginBottom: 20, flexWrap: "wrap" }}>
        <button style={buttonStyle} onClick={() => setPage("inspect")}>Upload & Inspect</button>
        <button style={buttonStyle} onClick={() => setPage("saving")}>Saving Dashboard</button>
        <button style={buttonStyle} onClick={() => setPage("resources")}>Risorse</button>
        <button style={buttonStyle} onClick={() => setPage("cycle")}>Tempi Attraversamento</button>
        <button style={buttonStyle} onClick={() => setPage("nc")}>Non Conformità</button>
      </div>

      {page === "inspect" && <InspectPage />}
      {page === "saving" && <SavingDashboardPage />}
      {page === "resources" && <ResourcesPage />}
      {page === "cycle" && <CycleTimesPage />}
      {page === "nc" && <NonConformitiesPage />}
    </div>
  );
}
