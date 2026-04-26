import React, { useState, useEffect, useRef, useCallback } from "react";

const API = import.meta.env.VITE_API_BASE || "/api";

const COLORS = {
  ADDED:    { bg: "rgba(40,180,40,0.30)", border: "#1a8a1a" },
  DELETED:  { bg: "rgba(220,40,40,0.30)", border: "#a31919" },
  MODIFIED: { bg: "rgba(220,200,40,0.35)", border: "#9a8a14" },
};

export default function App() {
  const [runId, setRunId] = useState(null);
  const [meta, setMeta] = useState(null);
  const [tab, setTab] = useState("viewer");
  const [pageNum, setPageNum] = useState(1);
  const [busy, setBusy] = useState(false);

  const onUpload = async (e) => {
    e.preventDefault();
    const form = new FormData(e.target);
    setBusy(true);
    try {
      const resp = await fetch(`${API}/compare`, { method: "POST", body: form });
      if (!resp.ok) throw new Error(`compare failed: ${resp.status}`);
      const data = await resp.json();
      setRunId(data.run_id);
      const meta = await (await fetch(`${API}/runs/${data.run_id}`)).json();
      setMeta(meta);
      setPageNum(1);
    } catch (err) {
      alert(err.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ fontFamily: "system-ui, sans-serif", padding: "1rem", maxWidth: 1600, margin: "0 auto" }}>
      <header style={{ borderBottom: "2px solid #333", paddingBottom: "0.5rem", marginBottom: "1rem" }}>
        <h1 style={{ margin: 0 }}>Spec-Diff</h1>
        <p style={{ margin: "0.3rem 0 0", color: "#666" }}>
          Compare two versions of a supplier specification document.
        </p>
      </header>

      {!runId && (
        <form onSubmit={onUpload} style={{ display: "flex", gap: "1rem", alignItems: "end", flexWrap: "wrap" }}>
          <label>Previous version<br/>
            <input type="file" name="base" accept="application/pdf" required />
          </label>
          <label>Current version<br/>
            <input type="file" name="target" accept="application/pdf" required />
          </label>
          <label>
            <input type="checkbox" name="use_llm" value="true" defaultChecked /> Use LLM for summary
          </label>
          <button disabled={busy} style={{ padding: "0.5rem 1rem" }}>
            {busy ? "Comparing… (~30-60 sec)" : "Compare"}
          </button>
        </form>
      )}

      {runId && meta && (
        <>
          <StatsBar meta={meta} />
          <Tabs tab={tab} setTab={setTab} />
          {tab === "viewer" && (
            <SideBySide
              runId={runId}
              meta={meta}
              pageNum={pageNum}
              setPageNum={setPageNum}
            />
          )}
          {tab === "summary" && <SummaryTable runId={runId} />}
          {tab === "query"   && <QueryPanel  runId={runId} />}
          {tab === "tables"  && <TablesList  runId={runId} />}
          <button onClick={() => { setRunId(null); setMeta(null); }} style={{ marginTop: "1rem", padding: "0.4rem 1rem" }}>
            ← Start over (compare new files)
          </button>
        </>
      )}
    </div>
  );
}

function StatsBar({ meta }) {
  const s = meta.stats || {};
  return (
    <div style={{ display: "flex", gap: "1rem", marginBottom: "0.75rem", flexWrap: "wrap" }}>
      <Tag label={`Added: ${s.ADDED || 0}`}    color="#1a8a1a" />
      <Tag label={`Deleted: ${s.DELETED || 0}`}  color="#a31919" />
      <Tag label={`Modified: ${s.MODIFIED || 0}`} color="#9a8a14" />
      <Tag label={`Unchanged: ${s.UNCHANGED || 0}`} color="#666" />
      <Tag label={`Coverage: base ${meta.coverage.base.toFixed(1)}% / target ${meta.coverage.target.toFixed(1)}%`} color="#444" />
    </div>
  );
}

function Tag({ label, color }) {
  return (
    <span style={{
      backgroundColor: color, color: "white",
      padding: "0.25rem 0.6rem", borderRadius: 6, fontSize: 13
    }}>{label}</span>
  );
}

function Tabs({ tab, setTab }) {
  const items = [
    ["viewer",  "Side-by-side viewer"],
    ["summary", "Summary table"],
    ["query",   "Ask a question"],
    ["tables",  "Compare tables"],
  ];
  return (
    <nav style={{ display: "flex", gap: "0.5rem", borderBottom: "1px solid #ccc", marginBottom: "1rem" }}>
      {items.map(([k, label]) => (
        <button
          key={k}
          onClick={() => setTab(k)}
          style={{
            padding: "0.5rem 1rem",
            background: tab === k ? "#222" : "#eee",
            color: tab === k ? "white" : "#333",
            border: "none",
            borderRadius: "6px 6px 0 0",
            cursor: "pointer",
          }}
        >{label}</button>
      ))}
    </nav>
  );
}

function SideBySide({ runId, meta, pageNum, setPageNum }) {
  const maxPages = Math.max(meta.n_pages_base, meta.n_pages_target);
  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", marginBottom: "0.5rem" }}>
        <button onClick={() => setPageNum(Math.max(1, pageNum - 1))} disabled={pageNum <= 1}>◀ prev</button>
        <span>Page {pageNum} / {maxPages}</span>
        <button onClick={() => setPageNum(Math.min(maxPages, pageNum + 1))} disabled={pageNum >= maxPages}>next ▶</button>
        <span style={{ marginLeft: 24, fontSize: 12, color: "#666" }}>
          <span style={{ background: COLORS.ADDED.bg,    padding: "0 6px" }}>added</span> &nbsp;
          <span style={{ background: COLORS.DELETED.bg,  padding: "0 6px" }}>deleted</span> &nbsp;
          <span style={{ background: COLORS.MODIFIED.bg, padding: "0 6px" }}>modified</span>
        </span>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.75rem" }}>
        <PageView runId={runId} side="base"   pageNum={Math.min(pageNum, meta.n_pages_base)}   label={meta.base_label} />
        <PageView runId={runId} side="target" pageNum={Math.min(pageNum, meta.n_pages_target)} label={meta.target_label} />
      </div>
    </div>
  );
}

function PageView({ runId, side, pageNum, label }) {
  const [overlay, setOverlay] = useState({ regions: [] });
  const imgRef = useRef(null);

  useEffect(() => {
    fetch(`${API}/runs/${runId}/overlay/${side}/${pageNum}`)
      .then((r) => r.json())
      .then(setOverlay)
      .catch(() => setOverlay({ regions: [] }));
  }, [runId, side, pageNum]);

  return (
    <div>
      <div style={{ fontSize: 13, color: "#333", marginBottom: 4 }}>{label} — page {pageNum}</div>
      <div style={{ position: "relative", border: "1px solid #999", background: "#fafafa" }}>
        <img
          ref={imgRef}
          src={`${API}/runs/${runId}/pages/${side}/${pageNum}`}
          style={{ display: "block", width: "100%", height: "auto" }}
          alt={`${side} page ${pageNum}`}
        />
        {overlay.regions.map((r, i) => {
          const [x0, y0, x1, y1] = r.bbox;
          const c = COLORS[r.change_type] || COLORS.MODIFIED;
          return (
            <div
              key={i}
              title={`${r.change_type} ${r.stable_key || ""} (${r.block_type})`}
              style={{
                position: "absolute",
                left:   `${(x0 / 612) * 100}%`,
                top:    `${(y0 / 792) * 100}%`,
                width:  `${((x1 - x0) / 612) * 100}%`,
                height: `${((y1 - y0) / 792) * 100}%`,
                background: c.bg,
                outline:    `1px solid ${c.border}`,
              }}
            />
          );
        })}
      </div>
    </div>
  );
}

function SummaryTable({ runId }) {
  const [rows, setRows] = useState(null);
  useEffect(() => {
    fetch(`${API}/runs/${runId}/summary`).then((r) => r.json()).then((d) => setRows(d.summary || []));
  }, [runId]);
  if (rows === null) return <div>Loading…</div>;
  if (rows.length === 0) return <div>No summary rows produced.</div>;

  return (
    <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 14 }}>
      <thead>
        <tr style={{ background: "#222", color: "white" }}>
          <th style={th}>Feature</th>
          <th style={th}>Change</th>
          <th style={th}>Seek Clarification</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i} style={{ background: i % 2 ? "#f6f6f6" : "white" }}>
            <td style={td}>{r.feature}</td>
            <td style={td}>{r.change}</td>
            <td style={{ ...td, color: r.seek_clarification === "None" ? "#999" : "#a04" }}>
              {r.seek_clarification}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

const th = { textAlign: "left", padding: "0.5rem 0.75rem", borderBottom: "1px solid #444" };
const td = { padding: "0.5rem 0.75rem", borderBottom: "1px solid #ddd", verticalAlign: "top" };

function QueryPanel({ runId }) {
  const [q, setQ] = useState("");
  const [results, setResults] = useState(null);
  const [busy, setBusy] = useState(false);

  const ask = async () => {
    if (!q.trim()) return;
    setBusy(true);
    try {
      const r = await fetch(`${API}/runs/${runId}/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: q }),
      });
      const data = await r.json();
      setResults(data.rows || []);
    } finally { setBusy(false); }
  };

  return (
    <div>
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.75rem" }}>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && ask()}
          placeholder="e.g., what changed in Sasquatch package on Big Bend?"
          style={{ flex: 1, padding: "0.5rem", fontSize: 14 }}
        />
        <button onClick={ask} disabled={busy} style={{ padding: "0.5rem 1rem" }}>
          {busy ? "…" : "Ask"}
        </button>
      </div>
      {results && (
        <div>
          <div style={{ marginBottom: 6, color: "#666" }}>{results.length} matches</div>
          {results.slice(0, 50).map((r, i) => <QueryResult key={i} r={r} />)}
        </div>
      )}
    </div>
  );
}

function QueryResult({ r }) {
  const c = COLORS[r.change_type] || COLORS.MODIFIED;
  return (
    <div style={{
      borderLeft: `4px solid ${c.border}`,
      background: "#fbfbfb",
      padding: "0.5rem 0.75rem",
      marginBottom: "0.5rem",
      fontSize: 13,
    }}>
      <div style={{ fontWeight: 600 }}>
        <span style={{ background: c.bg, padding: "0 6px", marginRight: 6 }}>{r.change_type}</span>
        {r.stable_key && <code>{r.stable_key}</code>}
        <span style={{ color: "#666", marginLeft: 8 }}>page {r.page} · {r.block_type}</span>
      </div>
      {r.before && <div style={{ color: "#a31919" }}>– {r.before.slice(0, 240)}</div>}
      {r.after  && <div style={{ color: "#1a8a1a" }}>+ {r.after.slice(0, 240)}</div>}
      {r.field_changes && r.field_changes.length > 0 && (
        <div style={{ marginTop: 4 }}>
          {r.field_changes.map((fc, i) => (
            <div key={i} style={{ fontFamily: "monospace", fontSize: 12 }}>
              <strong>{fc.field}:</strong> {String(fc.before).slice(0, 80)} → {String(fc.after).slice(0, 80)}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function TablesList({ runId }) {
  const [data, setData] = useState(null);
  const [baseSel, setBaseSel] = useState("");
  const [targetSel, setTargetSel] = useState("");
  const [diff, setDiff] = useState(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    fetch(`${API}/runs/${runId}/tables`).then((r) => r.json()).then(setData);
  }, [runId]);

  const compare = async () => {
    if (!baseSel || !targetSel) return;
    setBusy(true);
    try {
      const r = await fetch(`${API}/runs/${runId}/compare-tables`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ base_header_query: baseSel, target_header_query: targetSel }),
      });
      setDiff(await r.json());
    } finally { setBusy(false); }
  };

  if (!data) return <div>Loading tables…</div>;

  return (
    <div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem", marginBottom: "1rem" }}>
        <div>
          <h3>Previous version tables</h3>
          <select value={baseSel} onChange={(e) => setBaseSel(e.target.value)} style={{ width: "100%", padding: "0.4rem" }}>
            <option value="">— pick a table —</option>
            {(data.base || []).map((t) => (
              <option key={t.id} value={t.header_preview}>
                p{t.page_first} · {t.n_columns}c × {t.n_rows}r · {t.header_preview}
              </option>
            ))}
          </select>
        </div>
        <div>
          <h3>Current version tables</h3>
          <select value={targetSel} onChange={(e) => setTargetSel(e.target.value)} style={{ width: "100%", padding: "0.4rem" }}>
            <option value="">— pick a table —</option>
            {(data.target || []).map((t) => (
              <option key={t.id} value={t.header_preview}>
                p{t.page_first} · {t.n_columns}c × {t.n_rows}r · {t.header_preview}
              </option>
            ))}
          </select>
        </div>
      </div>
      <button onClick={compare} disabled={busy || !baseSel || !targetSel} style={{ padding: "0.5rem 1rem" }}>
        {busy ? "Comparing…" : "Compare these tables"}
      </button>
      {diff && diff.row_diffs && (
        <div style={{ marginTop: "1rem" }}>
          <p>{diff.row_diffs.length} row changes</p>
          {diff.row_diffs.slice(0, 100).map((rd, i) => (
            <div key={i} style={{ fontSize: 12, padding: "0.3rem", borderBottom: "1px solid #eee" }}>
              <span style={{ background: COLORS[rd.change_type]?.bg, padding: "0 6px", marginRight: 6 }}>{rd.change_type}</span>
              <code>{rd.key || "—"}</code>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
