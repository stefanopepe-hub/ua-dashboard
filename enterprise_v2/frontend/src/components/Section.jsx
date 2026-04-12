export default function Section({ title, subtitle, children, right }) {
  return (
    <div
      style={{
        border: "1px solid #e2e8f0",
        borderRadius: 20,
        padding: 20,
        marginBottom: 18,
        background: "#ffffff",
        boxShadow: "0 10px 30px rgba(15, 23, 42, 0.06)"
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12, marginBottom: 14 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 22, color: "#0f172a" }}>{title}</h2>
          {subtitle ? <p style={{ margin: "6px 0 0 0", color: "#64748b" }}>{subtitle}</p> : null}
        </div>
        {right || null}
      </div>
      {children}
    </div>
  );
}
