export default function DataTable({ columns, rows }) {
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th
                key={column.key}
                style={{
                  textAlign: "left",
                  padding: "12px 10px",
                  borderBottom: "1px solid #e2e8f0",
                  color: "#64748b",
                  fontSize: 13,
                  textTransform: "uppercase",
                  whiteSpace: "nowrap"
                }}
              >
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index}>
              {columns.map((column) => (
                <td
                  key={column.key}
                  style={{
                    padding: "12px 10px",
                    borderBottom: "1px solid #f1f5f9",
                    color: "#0f172a",
                    whiteSpace: "nowrap"
                  }}
                >
                  {String(row[column.key] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
