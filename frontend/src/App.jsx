import React, { useEffect, useRef, useState } from "react";

const API = import.meta.env.VITE_API_BASE || "/api";

const COLORS = {
  ADDED: {
    bg: "rgba(31, 160, 70, 0.24)",
    border: "#16813a",
    text: "#0f6a2f",
    chip: "#1f9f46",
  },
  DELETED: {
    bg: "rgba(218, 54, 54, 0.22)",
    border: "#b42323",
    text: "#9f1d1d",
    chip: "#c93333",
  },
  MODIFIED: {
    bg: "rgba(218, 185, 42, 0.30)",
    border: "#9a7a10",
    text: "#765c08",
    chip: "#a8870f",
  },
};

const PROCESS_STEPS = [
  "Uploading PDFs",
  "Rendering pages",
  "Extracting text and tables",
  "Aligning semantic blocks",
  "Preparing highlights",
  "Building summary",
];

const shellStyle = {
  minHeight: "100vh",
  background: "#f7f4ee",
  color: "#1f2933",
  fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif",
};

const pageStyle = {
  maxWidth: 1680,
  margin: "0 auto",
  padding: "18px 20px 28px",
};

const panelStyle = {
  background: "#fffefa",
  border: "1px solid #ddd7ca",
  borderRadius: 8,
  boxShadow: "0 1px 2px rgba(20, 20, 20, 0.05)",
};

export default function App() {
  const [runId, setRunId] = useState(null);
  const [meta, setMeta] = useState(null);
  const [tab, setTab] = useState("viewer");
  const [pageNum, setPageNum] = useState(1);
  const [busy, setBusy] = useState(false);
  const [progress, setProgress] = useState(0);
  const [stepIndex, setStepIndex] = useState(0);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!busy) return;

    setProgress(8);
    setStepIndex(0);

    const timer = setInterval(() => {
      setProgress((p) => {
        const next = Math.min(92, p + Math.max(1, Math.round((95 - p) / 10)));
        const idx = Math.min(PROCESS_STEPS.length - 1, Math.floor(next / 18));
        setStepIndex(idx);
        return next;
      });
    }, 1100);

    return () => clearInterval(timer);
  }, [busy]);

  const onUpload = async (e) => {
    e.preventDefault();
    const form = new FormData(e.target);

    setBusy(true);
    setError("");
    setRunId(null);
    setMeta(null);
    setPageNum(1);
    setTab("viewer");

    try {
      const resp = await fetch(`${API}/compare`, { method: "POST", body: form });
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `Compare failed with status ${resp.status}`);
      }

      const data = await resp.json();
      setProgress(96);
      setStepIndex(PROCESS_STEPS.length - 1);

      const metaResp = await fetch(`${API}/runs/${data.run_id}`);
      if (!metaResp.ok) throw new Error(`Could not load run metadata: ${metaResp.status}`);

      const nextMeta = await metaResp.json();
      setRunId(data.run_id);
      setMeta(nextMeta);
      setProgress(100);
    } catch (err) {
      setError(err.message || "Comparison failed");
    } finally {
      setBusy(false);
    }
  };

  const startOver = () => {
    setRunId(null);
    setMeta(null);
    setPageNum(1);
    setTab("viewer");
    setError("");
    setProgress(0);
  };

  return (
    <div style={shellStyle}>
      <div style={pageStyle}>
        <header style={{ marginBottom: 16 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", gap: 16 }}>
            <div>
              <h1 style={{ margin: 0, fontSize: 34, letterSpacing: 0, lineHeight: 1.05 }}>Spec-Diff</h1>
              <p style={{ margin: "6px 0 0", color: "#667085", fontSize: 15 }}>
                Compare supplier specification PDFs with semantic diffs, visual highlights, summaries, and table queries.
              </p>
            </div>
            {runId && (
              <button
                onClick={startOver}
                style={{
                  border: "1px solid #c9c2b6",
                  background: "#fffefa",
                  borderRadius: 6,
                  padding: "8px 12px",
                  cursor: "pointer",
                  color: "#344054",
                  fontWeight: 600,
                }}
              >
                Start over
              </button>
            )}
          </div>
        </header>

        {!runId && (
          <section style={{ ...panelStyle, padding: 18, marginBottom: 16 }}>
            <form onSubmit={onUpload}>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "minmax(220px, 1fr) minmax(220px, 1fr) auto auto",
                  gap: 14,
                  alignItems: "end",
                }}
              >
                <FileInput label="Previous version" name="base" />
                <FileInput label="Current version" name="target" />
                <label style={{ display: "flex", gap: 8, alignItems: "center", color: "#475467", fontSize: 14 }}>
                  <input type="checkbox" name="use_llm" value="true" defaultChecked />
                  LLM summary
                </label>
                <button
                  disabled={busy}
                  style={{
                    height: 40,
                    border: "none",
                    borderRadius: 6,
                    background: busy ? "#98a2b3" : "#1f2937",
                    color: "white",
                    padding: "0 18px",
                    cursor: busy ? "default" : "pointer",
                    fontWeight: 700,
                  }}
                >
                  {busy ? "Processing" : "Compare"}
                </button>
              </div>
            </form>

            {busy && <ProcessingState progress={progress} step={PROCESS_STEPS[stepIndex]} />}
            {error && (
              <div
                style={{
                  marginTop: 14,
                  border: "1px solid #f0b4b4",
                  background: "#fff1f1",
                  color: "#9f1d1d",
                  borderRadius: 6,
                  padding: 12,
                  fontSize: 14,
                }}
              >
                {error}
              </div>
            )}
          </section>
        )}

        {runId && meta && (
          <>
            <StatsBar meta={meta} />
            <Tabs tab={tab} setTab={setTab} />

            <main style={{ ...panelStyle, padding: 12 }}>
              {tab === "viewer" && (
                <SideBySide
                  runId={runId}
                  meta={meta}
                  pageNum={pageNum}
                  setPageNum={setPageNum}
                />
              )}
              {tab === "summary" && <SummaryTable runId={runId} />}
              {tab === "query" && <QueryPanel runId={runId} />}
              {tab === "tables" && <TablesList runId={runId} />}
            </main>
          </>
        )}
      </div>
    </div>
  );
}

function FileInput({ label, name }) {
  return (
    <label style={{ display: "block" }}>
      <span style={{ display: "block", marginBottom: 6, color: "#344054", fontSize: 13, fontWeight: 700 }}>
        {label}
      </span>
      <input
        type="file"
        name={name}
        accept="application/pdf"
        required
        style={{
          width: "100%",
          boxSizing: "border-box",
          border: "1px solid #d0d5dd",
          borderRadius: 6,
          padding: 8,
          background: "white",
          color: "#344054",
        }}
      />
    </label>
  );
}

function ProcessingState({ progress, step }) {
  return (
    <div style={{ marginTop: 16 }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6, color: "#475467", fontSize: 13 }}>
        <span>{step}</span>
        <span>{progress}%</span>
      </div>
      <div style={{ height: 8, background: "#ebe4d8", borderRadius: 999, overflow: "hidden" }}>
        <div
          style={{
            width: `${progress}%`,
            height: "100%",
            background: "#2f5f4f",
            transition: "width 400ms ease",
          }}
        />
      </div>
      <div style={{ display: "flex", gap: 8, marginTop: 10, flexWrap: "wrap" }}>
        {PROCESS_STEPS.map((s, i) => (
          <span
            key={s}
            style={{
              fontSize: 12,
              color: i <= PROCESS_STEPS.indexOf(step) ? "#2f5f4f" : "#98a2b3",
              background: i <= PROCESS_STEPS.indexOf(step) ? "#e7f0ea" : "#f2f0eb",
              border: "1px solid #ded8cd",
              padding: "3px 8px",
              borderRadius: 999,
            }}
          >
            {s}
          </span>
        ))}
      </div>
    </div>
  );
}

function StatsBar({ meta }) {
  const s = meta.stats || {};

  return (
    <section
      style={{
        display: "flex",
        gap: 10,
        marginBottom: 12,
        flexWrap: "wrap",
        alignItems: "center",
      }}
    >
      <Tag label={`Added: ${s.ADDED || 0}`} color={COLORS.ADDED.chip} />
      <Tag label={`Deleted: ${s.DELETED || 0}`} color={COLORS.DELETED.chip} />
      <Tag label={`Modified: ${s.MODIFIED || 0}`} color={COLORS.MODIFIED.chip} />
      <Tag label={`Unchanged: ${s.UNCHANGED || 0}`} color="#667085" />
      <Tag
        label={`Coverage: base ${meta.coverage.base.toFixed(1)}% / target ${meta.coverage.target.toFixed(1)}%`}
        color="#475467"
      />
      <Tag label={`Pages: ${meta.n_pages_base} / ${meta.n_pages_target}`} color="#2f5f4f" />
    </section>
  );
}

function Tag({ label, color }) {
  return (
    <span
      style={{
        backgroundColor: color,
        color: "white",
        padding: "5px 10px",
        borderRadius: 999,
        fontSize: 13,
        fontWeight: 700,
      }}
    >
      {label}
    </span>
  );
}

function Tabs({ tab, setTab }) {
  const items = [
    ["viewer", "Side-by-side viewer"],
    ["summary", "Summary table"],
    ["query", "Ask a question"],
    ["tables", "Compare tables"],
  ];

  return (
    <nav
      style={{
        display: "flex",
        gap: 6,
        borderBottom: "1px solid #d8d0c3",
        marginBottom: 12,
        overflowX: "auto",
      }}
    >
      {items.map(([key, label]) => {
        const active = tab === key;
        return (
          <button
            key={key}
            onClick={() => setTab(key)}
            style={{
              padding: "10px 14px",
              background: active ? "#1f2937" : "#ebe7df",
              color: active ? "white" : "#344054",
              border: active ? "1px solid #1f2937" : "1px solid #d8d0c3",
              borderBottom: active ? "1px solid #1f2937" : "none",
              borderRadius: "7px 7px 0 0",
              cursor: "pointer",
              fontWeight: 700,
              whiteSpace: "nowrap",
            }}
          >
            {label}
          </button>
        );
      })}
    </nav>
  );
}

function SideBySide({ runId, meta, pageNum, setPageNum }) {
  const maxPages = Math.max(meta.n_pages_base, meta.n_pages_target);

  return (
    <div>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          marginBottom: 12,
          flexWrap: "wrap",
        }}
      >
        <button
          onClick={() => setPageNum(Math.max(1, pageNum - 1))}
          disabled={pageNum <= 1}
          style={navButtonStyle(pageNum <= 1)}
        >
          ◀ prev
        </button>
        <span style={{ fontSize: 18, fontWeight: 800, minWidth: 110 }}>
          Page {pageNum} / {maxPages}
        </span>
        <button
          onClick={() => setPageNum(Math.min(maxPages, pageNum + 1))}
          disabled={pageNum >= maxPages}
          style={navButtonStyle(pageNum >= maxPages)}
        >
          next ▶
        </button>
        <Legend />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
        <PageView
          runId={runId}
          side="base"
          pageNum={pageNum}
          totalPages={meta.n_pages_base}
          label={meta.base_label}
        />
        <PageView
          runId={runId}
          side="target"
          pageNum={pageNum}
          totalPages={meta.n_pages_target}
          label={meta.target_label}
        />
      </div>
    </div>
  );
}

function navButtonStyle(disabled) {
  return {
    border: "1px solid #b7b0a5",
    background: disabled ? "#efede8" : "#fffefa",
    color: disabled ? "#98a2b3" : "#1f2937",
    borderRadius: 6,
    padding: "7px 12px",
    cursor: disabled ? "default" : "pointer",
    fontWeight: 800,
  };
}

function Legend() {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, marginLeft: 8, flexWrap: "wrap" }}>
      <LegendChip label="added" color={COLORS.ADDED.bg} border={COLORS.ADDED.border} />
      <LegendChip label="deleted" color={COLORS.DELETED.bg} border={COLORS.DELETED.border} />
      <LegendChip label="modified" color={COLORS.MODIFIED.bg} border={COLORS.MODIFIED.border} />
    </div>
  );
}

function LegendChip({ label, color, border }) {
  return (
    <span
      style={{
        background: color,
        border: `1px solid ${border}`,
        color: "#344054",
        padding: "2px 9px",
        borderRadius: 999,
        fontSize: 13,
        fontWeight: 700,
      }}
    >
      {label}
    </span>
  );
}

function PageView({ runId, side, pageNum, totalPages, label }) {
  const [overlay, setOverlay] = useState({ regions: [] });
  const [imageState, setImageState] = useState("idle");
  const imgRef = useRef(null);

  const pageExists = pageNum >= 1 && pageNum <= totalPages;

  useEffect(() => {
    setImageState(pageExists ? "loading" : "idle");

    if (!pageExists) {
      setOverlay({ regions: [] });
      return;
    }

    fetch(`${API}/runs/${runId}/overlay/${side}/${pageNum}`)
      .then((r) => r.json())
      .then(setOverlay)
      .catch(() => setOverlay({ regions: [] }));
  }, [runId, side, pageNum, pageExists]);

  return (
    <div>
      <div style={{ fontSize: 14, color: "#344054", marginBottom: 6, fontWeight: 700 }}>
        {label} — {pageExists ? `page ${pageNum}` : "no page"}
      </div>

      <div
        style={{
          position: "relative",
          border: "1px solid #b7b0a5",
          background: "#f9f7f2",
          minHeight: 520,
          overflow: "hidden",
        }}
      >
        {!pageExists ? (
          <EmptyPage pageNum={pageNum} />
        ) : (
          <>
            {imageState === "loading" && (
              <div
                style={{
                  position: "absolute",
                  inset: 0,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  color: "#667085",
                  background: "#f9f7f2",
                  zIndex: 1,
                  fontWeight: 700,
                }}
              >
                Loading page {pageNum}
              </div>
            )}

            <img
              key={`${side}-${pageNum}`}
              ref={imgRef}
              src={`${API}/runs/${runId}/pages/${side}/${pageNum}`}
              onLoad={() => setImageState("ready")}
              onError={() => setImageState("error")}
              style={{ display: "block", width: "100%", height: "auto" }}
              alt={`${side} page ${pageNum}`}
            />

            {imageState === "error" && (
              <div
                style={{
                  position: "absolute",
                  inset: 0,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  color: COLORS.DELETED.text,
                  background: "#fff1f1",
                  zIndex: 2,
                  fontWeight: 700,
                }}
              >
                Could not load page {pageNum}
              </div>
            )}

            {overlay.regions.map((r, i) => {
              const [x0, y0, x1, y1] = r.bbox;
              const c = COLORS[r.change_type] || COLORS.MODIFIED;
              const pageWidth = r.page_width || overlay.page_width || 612;
              const pageHeight = r.page_height || overlay.page_height || 792;

              return (
                <div
                  key={i}
                  title={`${r.change_type} ${r.stable_key || ""} (${r.block_type})`}
                  style={{
                    position: "absolute",
                    left: `${(x0 / pageWidth) * 100}%`,
                    top: `${(y0 / pageHeight) * 100}%`,
                    width: `${((x1 - x0) / pageWidth) * 100}%`,
                    height: `${((y1 - y0) / pageHeight) * 100}%`,
                    background: c.bg,
                    outline: `1px solid ${c.border}`,
                    pointerEvents: "auto",
                    mixBlendMode: "multiply",
                  }}
                />
              );
            })}
          </>
        )}
      </div>
    </div>
  );
}

function EmptyPage({ pageNum }) {
  return (
    <div
      style={{
        minHeight: 520,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "#667085",
        fontSize: 14,
        background: "#f2eee6",
        fontWeight: 700,
      }}
    >
      This document has no page {pageNum}
    </div>
  );
}

function SummaryTable({ runId }) {
  const [rows, setRows] = useState(null);

  useEffect(() => {
    fetch(`${API}/runs/${runId}/summary`)
      .then((r) => r.json())
      .then((d) => setRows(d.summary || []));
  }, [runId]);

  if (rows === null) return <SoftLoading label="Loading summary" />;
  if (rows.length === 0) return <EmptyState label="No summary rows produced." />;

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 14 }}>
        <thead>
          <tr style={{ background: "#1f2937", color: "white" }}>
            <th style={th}>Feature</th>
            <th style={th}>Change</th>
            <th style={th}>Seek Clarification</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} style={{ background: i % 2 ? "#fbfaf7" : "white" }}>
              <td style={td}>{r.feature}</td>
              <td style={td}>{r.change}</td>
              <td style={{ ...td, color: r.seek_clarification === "None" ? "#98a2b3" : COLORS.DELETED.text }}>
                {r.seek_clarification}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const th = {
  textAlign: "left",
  padding: "10px 12px",
  borderBottom: "1px solid #384250",
  whiteSpace: "nowrap",
};

const td = {
  padding: "10px 12px",
  borderBottom: "1px solid #e5dfd4",
  verticalAlign: "top",
};

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
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && ask()}
          placeholder="Ask about a feature, package, table, code, or section"
          style={{
            flex: 1,
            padding: "10px 12px",
            fontSize: 14,
            border: "1px solid #c9c2b6",
            borderRadius: 6,
            background: "white",
          }}
        />
        <button
          onClick={ask}
          disabled={busy}
          style={{
            border: "none",
            borderRadius: 6,
            background: busy ? "#98a2b3" : "#1f2937",
            color: "white",
            padding: "0 18px",
            fontWeight: 800,
            cursor: busy ? "default" : "pointer",
          }}
        >
          {busy ? "Searching" : "Ask"}
        </button>
      </div>

      {results && (
        <div>
          <div style={{ marginBottom: 8, color: "#667085", fontWeight: 700 }}>
            {results.length} matches
          </div>
          {results.length === 0 && <EmptyState label="No matching changes found." />}
          {results.slice(0, 50).map((r, i) => <QueryResult key={i} r={r} />)}
        </div>
      )}
    </div>
  );
}

function QueryResult({ r }) {
  const c = COLORS[r.change_type] || COLORS.MODIFIED;

  return (
    <div
      style={{
        borderLeft: `4px solid ${c.border}`,
        background: "#fffefa",
        padding: "10px 12px",
        marginBottom: 8,
        fontSize: 13,
        borderRadius: 6,
        boxShadow: "0 1px 1px rgba(20, 20, 20, 0.04)",
      }}
    >
      <div style={{ fontWeight: 800, marginBottom: 5 }}>
        <span style={{ background: c.bg, color: c.text, padding: "1px 7px", marginRight: 6, borderRadius: 999 }}>
          {r.change_type}
        </span>
        {r.stable_key && <code>{r.stable_key}</code>}
        <span style={{ color: "#667085", marginLeft: 8 }}>
          page {r.page} · {r.block_type}
        </span>
      </div>
      {r.before && <div style={{ color: COLORS.DELETED.text }}>- {r.before.slice(0, 260)}</div>}
      {r.after && <div style={{ color: COLORS.ADDED.text }}>+ {r.after.slice(0, 260)}</div>}
      {r.field_changes && r.field_changes.length > 0 && (
        <div style={{ marginTop: 6 }}>
          {r.field_changes.map((fc, i) => (
            <div key={i} style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace", fontSize: 12 }}>
              <strong>{fc.field}:</strong> {String(fc.before).slice(0, 90)} → {String(fc.after).slice(0, 90)}
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
    fetch(`${API}/runs/${runId}/tables`)
      .then((r) => r.json())
      .then(setData);
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
    } finally {
      setBusy(false);
    }
  };

  if (!data) return <SoftLoading label="Loading tables" />;

  return (
    <div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 14 }}>
        <TablePicker
          title="Previous version tables"
          value={baseSel}
          onChange={setBaseSel}
          tables={data.base || []}
        />
        <TablePicker
          title="Current version tables"
          value={targetSel}
          onChange={setTargetSel}
          tables={data.target || []}
        />
      </div>

      <button
        onClick={compare}
        disabled={busy || !baseSel || !targetSel}
        style={{
          border: "none",
          borderRadius: 6,
          background: busy || !baseSel || !targetSel ? "#98a2b3" : "#1f2937",
          color: "white",
          padding: "9px 14px",
          fontWeight: 800,
          cursor: busy || !baseSel || !targetSel ? "default" : "pointer",
        }}
      >
        {busy ? "Comparing" : "Compare these tables"}
      </button>

      {diff && diff.error && (
        <div style={{ marginTop: 12, color: COLORS.DELETED.text, fontWeight: 700 }}>
          {diff.error}
        </div>
      )}

      {diff && diff.row_diffs && (
        <div style={{ marginTop: 14 }}>
          <div style={{ marginBottom: 8, color: "#667085", fontWeight: 700 }}>
            {diff.row_diffs.length} row changes
          </div>
          {diff.row_diffs.length === 0 && <EmptyState label="No row-level differences found." />}
          {diff.row_diffs.slice(0, 100).map((rd, i) => (
            <div
              key={i}
              style={{
                fontSize: 13,
                padding: "8px 10px",
                borderBottom: "1px solid #e5dfd4",
                background: i % 2 ? "#fbfaf7" : "white",
              }}
            >
              <span
                style={{
                  background: COLORS[rd.change_type]?.bg,
                  color: COLORS[rd.change_type]?.text,
                  padding: "1px 7px",
                  marginRight: 6,
                  borderRadius: 999,
                  fontWeight: 800,
                }}
              >
                {rd.change_type}
              </span>
              <code>{rd.key || "-"}</code>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function TablePicker({ title, value, onChange, tables }) {
  return (
    <div>
      <h3 style={{ margin: "0 0 8px", fontSize: 15 }}>{title}</h3>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        style={{
          width: "100%",
          padding: "9px 10px",
          border: "1px solid #c9c2b6",
          borderRadius: 6,
          background: "white",
          color: "#344054",
        }}
      >
        <option value="">Pick a table</option>
        {tables.map((t) => (
          <option key={t.id} value={t.header_preview}>
            p{t.page_first} · {t.n_columns}c x {t.n_rows}r · {t.header_preview}
          </option>
        ))}
      </select>
    </div>
  );
}

function SoftLoading({ label }) {
  return (
    <div style={{ padding: 20, color: "#667085", fontWeight: 700 }}>
      {label}
    </div>
  );
}

function EmptyState({ label }) {
  return (
    <div
      style={{
        padding: 18,
        border: "1px dashed #c9c2b6",
        borderRadius: 8,
        color: "#667085",
        background: "#fbfaf7",
        fontWeight: 700,
      }}
    >
      {label}
    </div>
  );
}
