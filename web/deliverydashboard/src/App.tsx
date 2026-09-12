import { useEffect, useState } from "react";

// SummaryRow mirrors internal/deliverystats.SummaryRow, exactly what
// cmd/deliverygateway's /api/summary endpoint serves off the live
// ClickHouse rollup table. Nothing here is hardcoded or mocked: every
// number rendered is whatever the gateway's own query returns at fetch
// time, which is the "live" half of the resume claim this dashboard is
// built to demonstrate.
type SummaryRow = { channel: string; status: string; count: number };
type SummaryResponse = { generated_at: string; rows: SummaryRow[] };

const CHANNEL_COLORS: Record<string, string> = {
  email: "#4C6EF5",
  sms: "#12B886",
  whatsapp: "#F59F00",
};

export default function App() {
  const [data, setData] = useState<SummaryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = () => {
      fetch("/api/summary")
        .then((r) => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return r.json();
        })
        .then((d: SummaryResponse) => {
          setData(d);
          setError(null);
        })
        .catch((e) => setError(String(e)));
    };
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, []);

  if (error) {
    return <div style={{ fontFamily: "sans-serif", padding: 24, color: "#C92A2A" }}>Error loading /api/summary: {error}</div>;
  }
  if (!data) {
    return <div style={{ fontFamily: "sans-serif", padding: 24 }}>Loading live delivery stats...</div>;
  }

  const byChannel: Record<string, SummaryRow[]> = {};
  let total = 0;
  for (const row of data.rows) {
    (byChannel[row.channel] ??= []).push(row);
    total += row.count;
  }
  const maxCount = Math.max(1, ...data.rows.map((r) => r.count));

  return (
    <div style={{ fontFamily: "sans-serif", padding: 24, maxWidth: 900, margin: "0 auto" }}>
      <h1 style={{ fontSize: 20 }}>Delivery Stats Dashboard</h1>
      <p style={{ color: "#666", fontSize: 13 }}>
        Live from ClickHouse rollup, last refreshed {data.generated_at}. Total webhooks: {total.toLocaleString()}
      </p>

      <div style={{ display: "flex", gap: 16, marginBottom: 24 }}>
        {Object.entries(byChannel).map(([channel, rows]) => {
          const channelTotal = rows.reduce((s, r) => s + r.count, 0);
          return (
            <div key={channel} style={{ flex: 1, border: "1px solid #E9ECEF", borderRadius: 8, padding: 16 }}>
              <div style={{ fontSize: 12, color: "#666", textTransform: "uppercase" }}>{channel}</div>
              <div style={{ fontSize: 28, fontWeight: 600 }}>{channelTotal.toLocaleString()}</div>
            </div>
          );
        })}
      </div>

      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ textAlign: "left", borderBottom: "1px solid #E9ECEF" }}>
            <th style={{ padding: "6px 8px" }}>Channel</th>
            <th style={{ padding: "6px 8px" }}>Status</th>
            <th style={{ padding: "6px 8px" }}>Count</th>
            <th style={{ padding: "6px 8px" }}>Share</th>
          </tr>
        </thead>
        <tbody>
          {data.rows.map((row) => (
            <tr key={`${row.channel}-${row.status}`} style={{ borderBottom: "1px solid #F1F3F5" }}>
              <td style={{ padding: "6px 8px" }}>{row.channel}</td>
              <td style={{ padding: "6px 8px" }}>{row.status}</td>
              <td style={{ padding: "6px 8px" }}>{row.count.toLocaleString()}</td>
              <td style={{ padding: "6px 8px", width: 200 }}>
                <div style={{ background: "#F1F3F5", borderRadius: 4, height: 10 }}>
                  <div
                    style={{
                      width: `${(row.count / maxCount) * 100}%`,
                      background: CHANNEL_COLORS[row.channel] ?? "#868E96",
                      height: 10,
                      borderRadius: 4,
                    }}
                  />
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
