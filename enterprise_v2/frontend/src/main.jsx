import React from "react";
import ReactDOM from "react-dom/client";

function App() {
  return (
    <div style={{ fontFamily: "Arial, sans-serif", padding: "24px" }}>
      <h1>Telethon Enterprise V2</h1>
      <p>Nuova piattaforma enterprise separata dall'app attuale.</p>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
