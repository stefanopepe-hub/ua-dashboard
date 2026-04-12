export default function Section({ title, children }) {
  return (
    <div
      style={{
        border: "1px solid #e5e7eb",
        borderRadius: 16,
        padding: 18,
        marginBottom: 18,
        background: "#ffffff",
        boxShadow: "0 6px 18px rgba(15, 23, 42, 0.06)"
      }}
    >
      <h2 style={{ marginTop: 0, fontSize: 20, color: "#0f172a" }}>{title}</h2>
      {children}
    </div>
  );
}
