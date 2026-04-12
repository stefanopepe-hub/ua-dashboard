import { useState } from "react";
import InspectPage from "./pages/InspectPage.jsx";
import SavingDashboardPage from "./pages/SavingDashboardPage.jsx";

export default function App() {
  const [page, setPage] = useState("inspect");

  return (
    <div style={{ fontFamily: "Arial, sans-serif", padding: 24, background: "#f6f7fb", minHeight: "100vh" }}>
      <h1>Telethon Enterprise V2</h1>
      <p>Nuova piattaforma enterprise separata dall'app attuale.</p>

      <div style={{ display: "flex", gap: 12, marginBottom: 20 }}>
        <button onClick={() => setPage("inspect")}>Upload & Inspect</button>
        <button onClick={() => setPage("saving")}>Saving Dashboard</button>
      </div>

      {page === "inspect" ? <InspectPage /> : <SavingDashboardPage />}
    </div>
  );
}
