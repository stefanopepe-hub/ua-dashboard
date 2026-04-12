export default function KpiCard({ title, value, subtitle, accent = "#2563eb" }) {
  return (
    <div
      style={{
        background: "#fff",
        border: "1px solid #e2e8f0",
        borderLeft: `6px solid ${accent}`,
        borderRadius: 18,
        padding: 18,
        minHeight: 120,
        boxShadow: "0 8px 24px rgba(15, 23, 42, 0.05)"
      }}
    >
      <div style={{ fontSize: 13, letterSpacing: 0.3, textTransform: "uppercase", color: "#64748b", fontWeight: 700 }}>
        {title}
      </div>
      <div style={{ fontSize: 34, fontWeight: 800, color: "#0f172a", marginTop: 10 }}>
        {value}
      </div>
      {subtitle ? <div style={{ color: "#64748b", marginTop: 8 }}>{subtitle}</div> : null}
    </div>
  );
}
