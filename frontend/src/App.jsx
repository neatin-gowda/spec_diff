import React, { useEffect, useMemo, useRef, useState } from "react";

const API = import.meta.env.VITE_API_BASE || "/api";

const BRAND = {
  name: "DocuLens AI Agent",
  subtitle: "Document comparison with semantic review, visual evidence, citations, and reports.",
};

const COLORS = {
  ADDED: { bg: "rgba(31,160,70,.18)", border: "#1e8a47", text: "#176c38", chip: "#e8f6ed" },
  DELETED: { bg: "rgba(218,54,54,.16)", border: "#bb3030", text: "#9f2525", chip: "#fff0f0" },
  MODIFIED: { bg: "rgba(218,185,42,.22)", border: "#9a7a10", text: "#735c11", chip: "#fff8d8" },
  MATCH: { bg: "#eef4ff", border: "#5d6b98", text: "#344054", chip: "#eef4ff" },
};

const css = `
  * { box-sizing: border-box; }
  body { margin: 0; }
  button, input, select { font: inherit; }
  code { background: #f2eee6; border: 1px solid #e0d8ca; border-radius: 5px; padding: 1px 5px; }
`;

const shellStyle = {
  minHeight: "100vh",
  background: "#f7f3eb",
  color: "#202936",
  fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif",
};

const pageStyle = {
  maxWidth: 1720,
  margin: "0 auto",
  padding: "18px 22px 32px",
};

const panelStyle = {
  background: "#fffdf8",
  border: "1px solid #ded6c8",
  borderRadius: 10,
  boxShadow: "0 1px 3px rgba(31,41,55,.08)",
};

async function readResponseError(resp) {
  try {
    const text = await resp.text();
    if (!text) return `Request failed with status ${resp.status}`;
    try {
      const parsed = JSON.parse(text);
      return parsed.detail || parsed.error || text;
    } catch {
      return text;
    }
  } catch {
    return `Request failed with status ${resp.status}`;
  }
}

function friendlyFetchError(err) {
  const msg = String(err?.message || "");
  if (msg.toLowerCase().includes("failed to fetch")) {
    return "The app could not reach the comparison service. Please confirm the backend is running.";
  }
  return msg || "Something went wrong while processing the documents.";
}

export default function App() {
  const [runId, setRunId] = useState(null);
  const [meta, setMeta] = useState(null);
  const [tab, setTab] = useState("viewer");
  const [pageNum, setPageNum] = useState(1);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!runId || !busy) return;

    let cancelled = false;

    const poll = async () => {
      try {
        const resp = await fetch(`${API}/runs/${runId}`);
        if (!resp.ok) throw new Error(await readResponseError(resp));

        const data = await resp.json();
        if (cancelled) return;

        setMeta(data);

        if (data.status === "complete") {
          setBusy(false);
          setTab("viewer");
          return;
        }

        if (data.status === "failed") {
          setBusy(false);
          setError(data.error || data.status_message || "Comparison failed.");
          return;
        }

        setTimeout(poll, 1200);
      } catch (err) {
        if (cancelled) return;
        setBusy(false);
        setError(friendlyFetchError(err));
      }
    };

    poll();

    return () => {
      cancelled = true;
    };
  }, [runId, busy]);

  const onUpload = async (e) => {
    e.preventDefault();

    const form = new FormData(e.currentTarget);
    const base = form.get("base");
    const target = form.get("target");

    if (!base || !target || !base.name || !target.name) {
      setError("Please select both PDF documents before starting.");
      return;
    }

    setBusy(true);
    setError("");
    setRunId(null);
    setPageNum(1);
    setTab("viewer");
    setMeta({
      status: "uploading",
      status_message: "Uploading documents",
      progress: 3,
      stats: {},
      coverage: {},
      n_pages_base: 0,
      n_pages_target: 0,
    });

    try {
      const resp = await fetch(`${API}/compare`, { method: "POST", body: form });
      if (!resp.ok) throw new Error(await readResponseError(resp));

      const data = await resp.json();

      setRunId(data.run_id);
      setMeta({
        run_id: data.run_id,
        status: data.status,
        status_message: data.status_message,
        progress: data.progress,
        stats: {},
        coverage: {},
        n_pages_base: 0,
        n_pages_target: 0,
      });
    } catch (err) {
      setBusy(false);
      setError(friendlyFetchError(err));
    }
  };

  const startOver = () => {
    setRunId(null);
    setMeta(null);
    setPageNum(1);
    setTab("viewer");
    setError("");
    setBusy(false);
  };

  const downloadReport = () => {
    if (runId) window.location.href = `${API}/runs/${runId}/report.pdf`;
  };

  const isComplete = meta?.status === "complete";

  return (
    <div style={shellStyle}>
      <style>{css}</style>
      <div style={pageStyle}>
        <Header runId={isComplete ? runId : null} onStartOver={startOver} onDownloadReport={downloadReport} />

        {!isComplete && (
          <section style={{ ...panelStyle, padding: 22, marginBottom: 16 }}>
            <UploadPanel onUpload={onUpload} busy={busy} />
            {busy && meta && (
              <ProcessingState
                progress={meta.progress || 0}
                message={meta.status_message || "Working"}
                status={meta.status || "running"}
              />
            )}
            {error && <ErrorBox message={error} />}
          </section>
        )}

        {isComplete && runId && meta && (
          <>
            <StatsBar meta={meta} />
            <Tabs tab={tab} setTab={setTab} />

            <main style={{ ...panelStyle, padding: 12 }}>
              {tab === "viewer" && (
                <SideBySide runId={runId} meta={meta} pageNum={pageNum} setPageNum={setPageNum} />
              )}
              {tab === "report" && <ReviewReport runId={runId} />}
              {tab === "query" && <QueryPanel runId={runId} />}
              {tab === "tables" && <TablesList runId={runId} />}
            </main>
          </>
        )}
      </div>
    </div>
  );
}

function Header({ runId, onStartOver, onDownloadReport }) {
  return (
    <header style={{ marginBottom: 18 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 16 }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 4 }}>
            <div
              style={{
                width: 34,
                height: 34,
                borderRadius: 8,
                background: "#1f2937",
                color: "white",
                display: "grid",
                placeItems: "center",
                fontSize: 14,
                fontWeight: 700,
              }}
            >
              AI
            </div>
            <h1 style={{ margin: 0, fontSize: 30, letterSpacing: 0, lineHeight: 1.08, fontWeight: 700 }}>
              {BRAND.name}
            </h1>
          </div>
          <p style={{ margin: "6px 0 0", color: "#667085", fontSize: 15 }}>{BRAND.subtitle}</p>
        </div>

        {runId && (
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "flex-end" }}>
            <button onClick={onDownloadReport} style={primaryButtonStyle()}>
              Download report
            </button>
            <button onClick={onStartOver} style={secondaryButtonStyle()}>
              New comparison
            </button>
          </div>
        )}
      </div>
    </header>
  );
}

function UploadPanel({ onUpload, busy }) {
  return (
    <form onSubmit={onUpload}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "minmax(260px, 1fr) minmax(260px, 1fr) 210px",
          gap: 16,
          alignItems: "stretch",
        }}
      >
        <FileInput label="Baseline document" helper="Previous, approved, or reference PDF" name="base" disabled={busy} />
        <FileInput label="Revised document" helper="Latest, proposed, or updated PDF" name="target" disabled={busy} />

        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <label
            style={{
              display: "flex",
              gap: 8,
              alignItems: "center",
              color: "#475467",
              fontSize: 14,
              background: "#fbfaf6",
              border: "1px solid #ded6c8",
              borderRadius: 8,
              padding: "10px 12px",
              fontWeight: 600,
            }}
          >
            <input type="checkbox" name="use_llm" value="true" defaultChecked disabled={busy} />
            AI review summary
          </label>

          <button disabled={busy} style={primaryButtonStyle(busy, { height: 44 })}>
            {busy ? "Processing" : "Compare documents"}
          </button>

          <div style={{ color: "#667085", fontSize: 12, lineHeight: 1.35 }}>
            Turn off AI review summary for a faster first check.
          </div>
        </div>
      </div>

      <div style={{ marginTop: 16, display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 10 }}>
        <Capability label="Semantic review" detail="Finds meaningful content changes, not layout noise." />
        <Capability label="Visual evidence" detail="Highlights additions, removals, and modifications." />
        <Capability label="Business report" detail="Creates a PDF report with citations and review items." />
      </div>
    </form>
  );
}

function FileInput({ label, helper, name, disabled }) {
  const [fileName, setFileName] = useState("");
  const inputRef = useRef(null);

  const openPicker = () => {
    if (!disabled) inputRef.current?.click();
  };

  return (
    <div
      onClick={openPicker}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") openPicker();
      }}
      role="button"
      tabIndex={disabled ? -1 : 0}
      style={{
        border: "1px dashed #b9ae9e",
        borderRadius: 10,
        background: "#fbfaf6",
        padding: 16,
        minHeight: 126,
        cursor: disabled ? "default" : "pointer",
        outline: "none",
      }}
    >
      <input
        ref={inputRef}
        type="file"
        name={name}
        accept="application/pdf"
        required
        disabled={disabled}
        onClick={(e) => e.stopPropagation()}
        onChange={(e) => setFileName(e.target.files?.[0]?.name || "")}
        style={{ position: "absolute", width: 1, height: 1, opacity: 0, pointerEvents: "none" }}
      />

      <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
        <div>
          <div style={{ color: "#1f2937", fontSize: 15, fontWeight: 650 }}>{label}</div>
          <div style={{ marginTop: 4, color: "#667085", fontSize: 12 }}>{helper}</div>
        </div>
        <span
          style={{
            background: "#eee8dd",
            color: "#344054",
            border: "1px solid #d8d0c3",
            borderRadius: 999,
            padding: "4px 9px",
            fontSize: 12,
            fontWeight: 650,
            height: 24,
          }}
        >
          PDF
        </span>
      </div>

      <div
        style={{
          marginTop: 18,
          border: "1px solid #d0c7b8",
          borderRadius: 8,
          padding: "10px 11px",
          background: "white",
          color: fileName ? "#2f5f4f" : "#667085",
          fontSize: 14,
          fontWeight: 600,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {fileName || "Select PDF"}
      </div>
    </div>
  );
}

function Capability({ label, detail }) {
  return (
    <div style={{ background: "#fbfaf6", border: "1px solid #e0d8ca", borderRadius: 8, padding: 11 }}>
      <div style={{ fontSize: 13, fontWeight: 650, color: "#344054" }}>{label}</div>
      <div style={{ marginTop: 4, fontSize: 12, color: "#667085", lineHeight: 1.35 }}>{detail}</div>
    </div>
  );
}

function ProcessingState({ progress, message, status }) {
  const safeProgress = Math.max(0, Math.min(100, Number(progress) || 0));

  return (
    <div style={{ marginTop: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 7, color: "#475467", fontSize: 13 }}>
        <span style={{ fontWeight: 650 }}>{message}</span>
        <span>{safeProgress}%</span>
      </div>
      <div style={{ height: 8, background: "#e8dfd2", borderRadius: 999, overflow: "hidden" }}>
        <div
          style={{
            width: `${safeProgress}%`,
            height: "100%",
            background: status === "failed" ? COLORS.DELETED.border : "#2f5f4f",
            transition: "width 450ms ease, background 250ms ease",
          }}
        />
      </div>
      <p style={{ margin: "10px 0 0", color: "#667085", fontSize: 13 }}>
        Results will appear automatically when processing completes.
      </p>
    </div>
  );
}

function ErrorBox({ message }) {
  return (
    <div
      style={{
        marginTop: 16,
        border: "1px solid #f0b4b4",
        background: "#fff5f5",
        color: "#9f1d1d",
        borderRadius: 8,
        padding: 13,
        fontSize: 14,
        fontWeight: 600,
        lineHeight: 1.45,
      }}
    >
      {message}
    </div>
  );
}

function StatsBar({ meta }) {
  const s = meta.stats || {};

  return (
    <section style={{ ...panelStyle, padding: 12, display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap", alignItems: "center" }}>
      <StatChip label="Added" value={s.ADDED || 0} tone="added" />
      <StatChip label="Deleted" value={s.DELETED || 0} tone="deleted" />
      <StatChip label="Modified" value={s.MODIFIED || 0} tone="modified" />
      <StatChip label="Unchanged" value={s.UNCHANGED || 0} />
      <StatChip label="Coverage" value={`${safePercent(meta.coverage?.base)} / ${safePercent(meta.coverage?.target)}`} />
      <StatChip label="Pages" value={`${meta.n_pages_base} / ${meta.n_pages_target}`} />
    </section>
  );
}

function StatChip({ label, value, tone }) {
  const toneStyle =
    tone === "added"
      ? { borderColor: "#c8e6d2", background: "#f1faf4", color: COLORS.ADDED.text }
      : tone === "deleted"
        ? { borderColor: "#f2caca", background: "#fff6f6", color: COLORS.DELETED.text }
        : tone === "modified"
          ? { borderColor: "#eadb8d", background: "#fffaf0", color: COLORS.MODIFIED.text }
          : { borderColor: "#d8d0c3", background: "#fbfaf6", color: "#475467" };

  return (
    <span style={{ display: "inline-flex", alignItems: "baseline", gap: 6, border: "1px solid", borderRadius: 999, padding: "5px 10px", fontSize: 13, ...toneStyle }}>
      <span>{label}</span>
      <strong style={{ fontWeight: 650 }}>{value}</strong>
    </span>
  );
}

function safePercent(value) {
  return typeof value === "number" ? `${value.toFixed(1)}%` : "-";
}

function Tabs({ tab, setTab }) {
  const items = [
    ["viewer", "Visual review"],
    ["report", "Review report"],
    ["query", "Ask agent"],
    ["tables", "Table compare"],
  ];

  return (
    <nav style={{ display: "flex", gap: 4, borderBottom: "1px solid #d8d0c3", marginBottom: 12, overflowX: "auto" }}>
      {items.map(([key, label]) => {
        const active = tab === key;
        return (
          <button
            key={key}
            onClick={() => setTab(key)}
            style={{
              padding: "10px 14px",
              background: active ? "#1f2937" : "transparent",
              color: active ? "white" : "#344054",
              border: active ? "1px solid #1f2937" : "1px solid transparent",
              borderRadius: "8px 8px 0 0",
              cursor: "pointer",
              fontWeight: 600,
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
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12, flexWrap: "wrap" }}>
        <button onClick={() => setPageNum(Math.max(1, pageNum - 1))} disabled={pageNum <= 1} style={navButtonStyle(pageNum <= 1)}>
          Prev
        </button>
        <span style={{ fontSize: 17, fontWeight: 650, minWidth: 100 }}>Page {pageNum} / {maxPages}</span>
        <button onClick={() => setPageNum(Math.min(maxPages, pageNum + 1))} disabled={pageNum >= maxPages} style={navButtonStyle(pageNum >= maxPages)}>
          Next
        </button>
        <Legend />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
        <PageView runId={runId} side="base" pageNum={pageNum} totalPages={meta.n_pages_base} label="Baseline document" docName={meta.base_label} />
        <PageView runId={runId} side="target" pageNum={pageNum} totalPages={meta.n_pages_target} label="Revised document" docName={meta.target_label} />
      </div>
    </div>
  );
}

function navButtonStyle(disabled) {
  return {
    border: "1px solid #c9c0b0",
    background: disabled ? "#f1ece3" : "#fffdf8",
    color: disabled ? "#98a2b3" : "#344054",
    borderRadius: 7,
    padding: "7px 12px",
    cursor: disabled ? "default" : "pointer",
    fontWeight: 600,
  };
}

function Legend() {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 7, marginLeft: 6, flexWrap: "wrap" }}>
      <LegendChip label="added" color={COLORS.ADDED.bg} border={COLORS.ADDED.border} />
      <LegendChip label="deleted" color={COLORS.DELETED.bg} border={COLORS.DELETED.border} />
      <LegendChip label="modified" color={COLORS.MODIFIED.bg} border={COLORS.MODIFIED.border} />
    </div>
  );
}

function LegendChip({ label, color, border }) {
  return (
    <span style={{ background: color, border: `1px solid ${border}`, color: "#344054", padding: "2px 8px", borderRadius: 999, fontSize: 12, fontWeight: 600 }}>
      {label}
    </span>
  );
}

function PageView({ runId, side, pageNum, totalPages, label, docName }) {
  const [overlay, setOverlay] = useState({ regions: [] });
  const [imageState, setImageState] = useState("idle");
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
      <div style={{ marginBottom: 7 }}>
        <div style={{ fontSize: 13, color: "#667085", fontWeight: 600 }}>{label}</div>
        <div style={{ fontSize: 14, color: "#344054", fontWeight: 650 }}>
          {docName} - {pageExists ? `page ${pageNum}` : "no page"}
        </div>
      </div>

      <div style={{ position: "relative", border: "1px solid #b7ae9f", background: "#f9f6ef", minHeight: 520, overflow: "hidden" }}>
        {!pageExists ? (
          <EmptyPage pageNum={pageNum} />
        ) : (
          <>
            {imageState === "loading" && (
              <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", color: "#667085", background: "#f9f6ef", zIndex: 1, fontWeight: 600 }}>
                Loading page {pageNum}
              </div>
            )}

            <img
              key={`${side}-${pageNum}`}
              src={`${API}/runs/${runId}/pages/${side}/${pageNum}`}
              onLoad={() => setImageState("ready")}
              onError={() => setImageState("error")}
              style={{ display: "block", width: "100%", height: "auto" }}
              alt={`${side} page ${pageNum}`}
            />

            {imageState === "error" && (
              <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", color: COLORS.DELETED.text, background: "#fff5f5", zIndex: 2, fontWeight: 600 }}>
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
    <div style={{ minHeight: 520, display: "flex", alignItems: "center", justifyContent: "center", color: "#667085", fontSize: 14, background: "#f0ebe2", fontWeight: 600 }}>
      This document has no page {pageNum}
    </div>
  );
}

function ReviewReport({ runId }) {
  const [rows, setRows] = useState(null);
  const [filter, setFilter] = useState("ALL");

  useEffect(() => {
    fetch(`${API}/runs/${runId}/summary`)
      .then((r) => r.json())
      .then((d) => setRows(d.summary || []))
      .catch(() => setRows([]));
  }, [runId]);

  const filtered = useMemo(() => {
    if (!rows) return [];
    return rows.filter((r) => {
      const needsReview = r.needs_review || (r.seek_clarification && r.seek_clarification !== "None");
      if (filter === "REVIEW") return needsReview;
      if (filter === "ALL") return true;
      return r.change_type === filter;
    });
  }, [rows, filter]);

  if (rows === null) return <SoftLoading label="Loading review report" />;
  if (rows.length === 0) return <EmptyState label="No review rows were produced." />;

  const reviewCount = rows.filter((r) => r.needs_review || (r.seek_clarification && r.seek_clarification !== "None")).length;
  const avgConfidence = average(rows.map((r) => r.confidence).filter((v) => typeof v === "number"));

  return (
    <div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 10, marginBottom: 14 }}>
        <MetricCard label="Review items" value={rows.length} />
        <MetricCard label="Needs review" value={reviewCount} />
        <MetricCard label="Average confidence" value={avgConfidence == null ? "-" : `${Math.round(avgConfidence * 100)}%`} />
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap" }}>
        {[
          ["ALL", "All changes"],
          ["ADDED", "Added"],
          ["DELETED", "Deleted"],
          ["MODIFIED", "Modified"],
          ["REVIEW", "Needs review"],
        ].map(([value, label]) => (
          <button key={value} onClick={() => setFilter(value)} style={filterButtonStyle(filter === value)}>
            {label}
          </button>
        ))}
      </div>

      {filtered.length === 0 ? (
        <EmptyState label={`No rows match the "${filterLabel(filter)}" filter.`} />
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 14 }}>
            <thead>
              <tr style={{ background: "#263241", color: "white" }}>
                <th style={th}>Area / Item</th>
                <th style={th}>Change</th>
                <th style={th}>Evidence</th>
                <th style={th}>Confidence</th>
                <th style={th}>Review</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => {
                const color = COLORS[r.change_type] || COLORS.MODIFIED;
                const needsReview = r.needs_review || (r.seek_clarification && r.seek_clarification !== "None");

                return (
                  <tr key={i} style={{ background: i % 2 ? "#fbfaf7" : "white" }}>
                    <td style={td}>
                      <div style={{ fontWeight: 650 }}>{r.area || "Document"}</div>
                      <div style={{ marginTop: 4, color: "#475467" }}>{r.item || r.feature}</div>
                      {r.stable_key && <code style={{ display: "inline-block", marginTop: 5 }}>Key {r.stable_key}</code>}
                    </td>
                    <td style={td}>
                      <ChangeBadge type={r.change_type || "MODIFIED"} />
                      <div style={{ marginTop: 6 }}>{r.change}</div>
                    </td>
                    <td style={td}>
                      <div>{r.citation || "-"}</div>
                      {r.before && <div style={{ marginTop: 6, color: COLORS.DELETED.text }}>Before: {trim(r.before, 140)}</div>}
                      {r.after && <div style={{ marginTop: 3, color: COLORS.ADDED.text }}>After: {trim(r.after, 140)}</div>}
                    </td>
                    <td style={td}>
                      <Confidence value={r.confidence} />
                      <div style={{ marginTop: 6, color: "#667085" }}>{r.impact || "medium"} impact</div>
                    </td>
                    <td style={td}>
                      {needsReview ? (
                        <div style={{ color: COLORS.DELETED.text, fontWeight: 650 }}>
                          {r.seek_clarification && r.seek_clarification !== "None" ? r.seek_clarification : r.review_reason || "Review recommended"}
                        </div>
                      ) : (
                        <span style={{ color: "#667085" }}>No clarification needed</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function QueryPanel({ runId }) {
  const [q, setQ] = useState("");
  const [answer, setAnswer] = useState("");
  const [results, setResults] = useState(null);
  const [busy, setBusy] = useState(false);

  const ask = async () => {
    if (!q.trim()) return;
    setBusy(true);
    setAnswer("");
    setResults(null);

    try {
      const r = await fetch(`${API}/runs/${runId}/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: q }),
      });

      if (!r.ok) throw new Error(await readResponseError(r));

      const data = await r.json();
      setAnswer(data.answer || `I found ${data.count || 0} matching changes.`);
      setResults(data.rows || []);
    } catch (err) {
      setAnswer(friendlyFetchError(err));
      setResults([]);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <div style={{ background: "#fbfaf6", border: "1px solid #ded6c8", borderRadius: 8, padding: 12, marginBottom: 12 }}>
        <div style={{ fontWeight: 650, marginBottom: 6 }}>Ask about the comparison</div>
        <div style={{ color: "#667085", fontSize: 13, marginBottom: 10 }}>
          Ask about a section, requirement, table row, code, part number, value, or cell-level change.
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && ask()}
            placeholder="Example: Compare PCB 205 from old file with PCB 203 from new file"
            style={{ flex: 1, padding: "10px 12px", fontSize: 14, border: "1px solid #c9c0b0", borderRadius: 7, background: "white" }}
          />
          <button onClick={ask} disabled={busy} style={primaryButtonStyle(busy)}>
            {busy ? "Searching" : "Ask"}
          </button>
        </div>
      </div>

      {answer && (
        <div style={{ background: "#fffdf8", border: "1px solid #d8d0c3", borderLeft: "4px solid #2f5f4f", borderRadius: 8, padding: 12, marginBottom: 12, color: "#344054", fontWeight: 600 }}>
          {answer}
        </div>
      )}

      {results && (
        <div>
          <div style={{ marginBottom: 8, color: "#667085", fontWeight: 600 }}>
            {results.length} supporting result{results.length === 1 ? "" : "s"}
          </div>
          {results.length === 0 && <EmptyState label="No matching changes found." />}
          {results.slice(0, 50).map((r, i) => <QueryResult key={i} r={r} />)}
        </div>
      )}
    </div>
  );
}

function QueryResult({ r }) {
  if (r.type === "table_row_comparison") {
    return <TableRowComparisonResult result={r} />;
  }

  if (r.type === "table_row") {
    return <TableRowLookupResult result={r} />;
  }

  const c = COLORS[r.change_type] || COLORS.MODIFIED;

  return (
    <div style={{ borderLeft: `4px solid ${c.border}`, background: "#fffdf8", padding: "10px 12px", marginBottom: 8, fontSize: 13, borderRadius: 7, boxShadow: "0 1px 1px rgba(20,20,20,.04)" }}>
      <div style={{ fontWeight: 650, marginBottom: 5 }}>
        <ChangeBadge type={r.change_type || "MODIFIED"} />
        {r.stable_key && <code style={{ marginLeft: 6 }}>{r.stable_key}</code>}
        <span style={{ color: "#667085", marginLeft: 8 }}>{r.citation || `page ${r.page} - ${r.block_type}`}</span>
      </div>
      {r.before && <div style={{ color: COLORS.DELETED.text }}>Before: {trim(r.before, 260)}</div>}
      {r.after && <div style={{ color: COLORS.ADDED.text }}>After: {trim(r.after, 260)}</div>}
      {r.field_changes?.length > 0 && <FieldDiffTable rows={r.field_changes} />}
    </div>
  );
}

function TableRowComparisonResult({ result }) {
  const rows = result.field_changes || [];

  return (
    <div style={{ background: "#fffdf8", border: "1px solid #d8d0c3", borderLeft: `4px solid ${COLORS.MODIFIED.border}`, borderRadius: 8, padding: 12, marginBottom: 10 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
        <div>
          <div style={{ fontWeight: 650, color: "#1f2937" }}>
            Table row comparison: <code>{result.row_key}</code>
          </div>
          <div style={{ marginTop: 4, color: "#667085", fontSize: 13 }}>{result.citation}</div>
        </div>
        <Confidence value={result.confidence} />
      </div>

      <div style={{ marginTop: 10, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <DefinitionBox title="Baseline row" value={result.definition?.base || result.before} />
        <DefinitionBox title="Revised row" value={result.definition?.target || result.after} />
      </div>

      {rows.length > 0 ? (
        <FieldDiffTable rows={rows} />
      ) : (
        <div style={{ marginTop: 10 }}>
          <EmptyState label="No cell-level differences were found for these selected rows." />
        </div>
      )}
    </div>
  );
}

function TableRowLookupResult({ result }) {
  return (
    <div style={{ background: "#fffdf8", border: "1px solid #d8d0c3", borderLeft: `4px solid ${COLORS.MATCH.border}`, borderRadius: 8, padding: 12, marginBottom: 10 }}>
      <div style={{ fontWeight: 650 }}>
        {result.side === "base" ? "Baseline" : "Revised"} table row: <code>{result.row_key}</code>
      </div>
      <div style={{ marginTop: 4, color: "#667085", fontSize: 13 }}>{result.citation}</div>
      <DefinitionBox title="Row definition" value={result.definition} />
      <ValuesTable values={result.values} />
    </div>
  );
}

function TablesList({ runId }) {
  const [data, setData] = useState(null);
  const [baseTableId, setBaseTableId] = useState("");
  const [targetTableId, setTargetTableId] = useState("");
  const [baseRowKey, setBaseRowKey] = useState("");
  const [targetRowKey, setTargetRowKey] = useState("");
  const [diff, setDiff] = useState(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    fetch(`${API}/runs/${runId}/tables`)
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData({ base: [], target: [] }));
  }, [runId]);

  const baseTables = data?.base || [];
  const targetTables = data?.target || [];
  const baseTable = baseTables.find((t) => t.id === baseTableId);
  const targetTable = targetTables.find((t) => t.id === targetTableId);

  const compare = async () => {
    if (!baseTableId || !targetTableId) return;
    setBusy(true);
    setDiff(null);

    try {
      const r = await fetch(`${API}/runs/${runId}/compare-tables`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          base_table_id: baseTableId,
          target_table_id: targetTableId,
          base_row_key: baseRowKey.trim() || null,
          target_row_key: targetRowKey.trim() || null,
        }),
      });

      if (!r.ok) throw new Error(await readResponseError(r));
      setDiff(await r.json());
    } catch (err) {
      setDiff({ error: friendlyFetchError(err) });
    } finally {
      setBusy(false);
    }
  };

  if (!data) return <SoftLoading label="Loading detected tables" />;

  return (
    <div>
      <div style={{ background: "#fbfaf6", border: "1px solid #ded6c8", borderRadius: 8, padding: 12, marginBottom: 14 }}>
        <div style={{ fontWeight: 650, marginBottom: 4 }}>Compare detected tables</div>
        <div style={{ color: "#667085", fontSize: 13 }}>
          Select tables from both documents. Optionally enter row keys such as a code, part number, PCB number, package code, or any phrase from the row.
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 14 }}>
        <TablePicker title="Baseline document table" value={baseTableId} onChange={setBaseTableId} tables={baseTables} />
        <TablePicker title="Revised document table" value={targetTableId} onChange={setTargetTableId} tables={targetTables} />
      </div>

      {(baseTable || targetTable) && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 14 }}>
          <TableInfo table={baseTable} emptyLabel="Select a baseline table to inspect detected rows." />
          <TableInfo table={targetTable} emptyLabel="Select a revised table to inspect detected rows." />
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr auto", gap: 10, alignItems: "end", marginBottom: 14 }}>
        <RowKeyInput label="Baseline row key" value={baseRowKey} onChange={setBaseRowKey} table={baseTable} placeholder="Optional, e.g. 205 or 44Q" />
        <RowKeyInput label="Revised row key" value={targetRowKey} onChange={setTargetRowKey} table={targetTable} placeholder="Optional, e.g. 203 or 44Q" />
        <button onClick={compare} disabled={busy || !baseTableId || !targetTableId} style={primaryButtonStyle(busy || !baseTableId || !targetTableId, { height: 40 })}>
          {busy ? "Comparing" : "Compare"}
        </button>
      </div>

      {diff?.error && <ErrorBox message={diff.error} />}

      {diff && !diff.error && (
        <TableCompareResult diff={diff} />
      )}
    </div>
  );
}

function TablePicker({ title, value, onChange, tables }) {
  return (
    <div>
      <label style={{ display: "block", marginBottom: 7, fontSize: 13, color: "#344054", fontWeight: 650 }}>{title}</label>
      <select value={value} onChange={(e) => onChange(e.target.value)} style={inputStyle}>
        <option value="">Select a detected table</option>
        {tables.map((t) => (
          <option key={t.id} value={t.id}>
            p{t.page_first} - {t.n_columns}c x {t.n_rows}r - {t.header_preview || t.area || "table"}
          </option>
        ))}
      </select>
    </div>
  );
}

function TableInfo({ table, emptyLabel }) {
  if (!table) return <EmptyState label={emptyLabel} />;

  return (
    <div style={{ background: "#fffdf8", border: "1px solid #ded6c8", borderRadius: 8, padding: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
        <div>
          <div style={{ fontWeight: 650 }}>{table.area || "Detected table"}</div>
          <div style={{ marginTop: 4, color: "#667085", fontSize: 13 }}>
            Page {table.page_first} · {table.n_columns} columns · {table.n_rows} rows
          </div>
        </div>
        <code>{table.id?.slice(0, 8)}</code>
      </div>

      <div style={{ marginTop: 10, color: "#475467", fontSize: 13 }}>
        <strong style={{ fontWeight: 650 }}>Headers:</strong> {table.header?.slice(0, 8).join(" | ") || "No headers detected"}
      </div>

      {table.row_keys?.length > 0 && (
        <div style={{ marginTop: 10, display: "flex", gap: 6, flexWrap: "wrap" }}>
          {table.row_keys.slice(0, 18).map((key, i) => (
            <span key={`${key}-${i}`} style={{ border: "1px solid #d8d0c3", borderRadius: 999, padding: "2px 7px", fontSize: 12, color: "#475467", background: "#fbfaf6" }}>
              {trim(key, 28)}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

function RowKeyInput({ label, value, onChange, table, placeholder }) {
  const listId = `${label.replace(/\s+/g, "-").toLowerCase()}-keys`;

  return (
    <div>
      <label style={{ display: "block", marginBottom: 7, fontSize: 13, color: "#344054", fontWeight: 650 }}>{label}</label>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        list={listId}
        placeholder={placeholder}
        style={inputStyle}
      />
      <datalist id={listId}>
        {(table?.row_keys || []).slice(0, 150).map((key, i) => (
          <option key={`${key}-${i}`} value={key} />
        ))}
      </datalist>
    </div>
  );
}

function TableCompareResult({ diff }) {
  const counts = diff.counts || {};
  const rowDiffs = diff.row_diffs || [];

  return (
    <div style={{ marginTop: 12 }}>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 12 }}>
        <TableResultCard label="Baseline table" table={diff.base_table} />
        <TableResultCard label="Revised table" table={diff.target_table} />
      </div>

      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
        <StatChip label="Added rows" value={counts.ADDED || 0} tone="added" />
        <StatChip label="Deleted rows" value={counts.DELETED || 0} tone="deleted" />
        <StatChip label="Modified rows" value={counts.MODIFIED || 0} tone="modified" />
      </div>

      <HeaderAlignment alignment={diff.header_alignment || []} />

      <div style={{ marginTop: 14 }}>
        <div style={{ marginBottom: 8, color: "#344054", fontWeight: 650 }}>
          Row comparison results
        </div>
        {rowDiffs.length === 0 ? (
          <EmptyState label="Tables were compared, but no row-level differences were found." />
        ) : (
          rowDiffs.slice(0, 120).map((row, i) => <TableRowDiff key={i} row={row} />)
        )}
      </div>
    </div>
  );
}

function TableResultCard({ label, table }) {
  return (
    <div style={{ background: "#fbfaf6", border: "1px solid #ded6c8", borderRadius: 8, padding: 12 }}>
      <div style={{ color: "#667085", fontSize: 12, fontWeight: 650 }}>{label}</div>
      <div style={{ marginTop: 4, fontWeight: 650 }}>{table?.area || "Table"}</div>
      <div style={{ marginTop: 4, color: "#667085", fontSize: 13 }}>
        Page {table?.page_first || "-"} · {table?.n_columns || 0} columns · {table?.n_rows || 0} rows
      </div>
    </div>
  );
}

function HeaderAlignment({ alignment }) {
  if (!alignment.length) return null;

  return (
    <div style={{ background: "#fffdf8", border: "1px solid #ded6c8", borderRadius: 8, padding: 12 }}>
      <div style={{ fontWeight: 650, marginBottom: 8 }}>Column alignment</div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
        {alignment.slice(0, 40).map((item, i) => {
          const type = item.status === "matched" ? "MATCH" : item.status === "base_only" ? "DELETED" : "ADDED";
          return (
            <span key={i} style={{ border: `1px solid ${COLORS[type].border}`, background: COLORS[type].chip, color: COLORS[type].text, borderRadius: 999, padding: "3px 8px", fontSize: 12 }}>
              {item.base_col || "new"} {item.target_col && item.base_col !== item.target_col ? `-> ${item.target_col}` : item.target_col && !item.base_col ? item.target_col : ""}
            </span>
          );
        })}
      </div>
    </div>
  );
}

function TableRowDiff({ row }) {
  const type = row.change_type || "MODIFIED";
  const fieldDiffs = row.field_diffs || [];

  return (
    <div style={{ background: "#fffdf8", border: "1px solid #ded6c8", borderLeft: `4px solid ${(COLORS[type] || COLORS.MODIFIED).border}`, borderRadius: 8, padding: 12, marginBottom: 10 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 10, flexWrap: "wrap" }}>
        <div>
          <ChangeBadge type={type} />
          <span style={{ marginLeft: 8, fontWeight: 650 }}>{row.key || row.definition || "row"}</span>
        </div>
        <span style={{ color: "#667085", fontSize: 13 }}>Match {Math.round((row.match_score || 0) * 100)}%</span>
      </div>

      <div style={{ marginTop: 8, color: "#475467", fontSize: 13 }}>{row.definition}</div>

      <div style={{ marginTop: 10, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <DefinitionBox title="Baseline row" value={row.base_row?.definition || row.base_row?.text} />
        <DefinitionBox title="Revised row" value={row.target_row?.definition || row.target_row?.text} />
      </div>

      {fieldDiffs.length > 0 && <FieldDiffTable rows={fieldDiffs} />}
    </div>
  );
}

function FieldDiffTable({ rows }) {
  if (!rows?.length) return null;

  return (
    <div style={{ overflowX: "auto", marginTop: 10 }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ background: "#f2eee6", color: "#344054" }}>
            <th style={smallTh}>Field</th>
            <th style={smallTh}>Before</th>
            <th style={smallTh}>After</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              <td style={smallTd}>{r.field || "-"}</td>
              <td style={{ ...smallTd, color: COLORS.DELETED.text }}>{displayCell(r.before)}</td>
              <td style={{ ...smallTd, color: COLORS.ADDED.text }}>{displayCell(r.after)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ValuesTable({ values }) {
  const entries = Object.entries(values || {});
  if (!entries.length) return null;

  return (
    <div style={{ overflowX: "auto", marginTop: 10 }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <tbody>
          {entries.slice(0, 30).map(([key, value]) => (
            <tr key={key}>
              <td style={{ ...smallTd, width: "28%", color: "#667085" }}>{key}</td>
              <td style={smallTd}>{displayCell(value)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DefinitionBox({ title, value }) {
  return (
    <div style={{ background: "#fbfaf6", border: "1px solid #e0d8ca", borderRadius: 8, padding: 10 }}>
      <div style={{ fontSize: 12, color: "#667085", fontWeight: 650, marginBottom: 4 }}>{title}</div>
      <div style={{ fontSize: 13, color: "#344054", lineHeight: 1.4 }}>{value || "-"}</div>
    </div>
  );
}

function ChangeBadge({ type }) {
  const c = COLORS[type] || COLORS.MODIFIED;
  return (
    <span style={{ display: "inline-block", background: c.chip, color: c.text, border: `1px solid ${c.border}`, padding: "2px 8px", borderRadius: 999, fontWeight: 650, fontSize: 12 }}>
      {type}
    </span>
  );
}

function filterLabel(filter) {
  if (filter === "ALL") return "All changes";
  if (filter === "REVIEW") return "Needs review";
  return filter.toLowerCase();
}

function MetricCard({ label, value }) {
  return (
    <div style={{ background: "#fbfaf6", border: "1px solid #ded6c8", borderRadius: 8, padding: 12 }}>
      <div style={{ fontSize: 12, color: "#667085", fontWeight: 600 }}>{label}</div>
      <div style={{ marginTop: 4, fontSize: 22, color: "#1f2937", fontWeight: 650 }}>{value}</div>
    </div>
  );
}

function Confidence({ value }) {
  if (typeof value !== "number") return <span>-</span>;
  const pct = Math.round(value * 100);
  const color = pct >= 80 ? COLORS.ADDED.text : pct >= 65 ? COLORS.MODIFIED.text : COLORS.DELETED.text;
  return <span style={{ color, fontWeight: 650 }}>{pct}%</span>;
}

function average(values) {
  if (!values.length) return null;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

const inputStyle = {
  width: "100%",
  padding: "10px 11px",
  border: "1px solid #c9c0b0",
  borderRadius: 7,
  background: "white",
  color: "#344054",
};

const th = {
  textAlign: "left",
  padding: "10px 12px",
  borderBottom: "1px solid #384250",
  whiteSpace: "nowrap",
  fontWeight: 650,
};

const td = {
  padding: "10px 12px",
  borderBottom: "1px solid #e5dfd4",
  verticalAlign: "top",
};

const smallTh = {
  textAlign: "left",
  padding: "8px 9px",
  borderBottom: "1px solid #ded6c8",
  fontWeight: 650,
};

const smallTd = {
  padding: "8px 9px",
  borderBottom: "1px solid #eee7dc",
  verticalAlign: "top",
};

function SoftLoading({ label }) {
  return <div style={{ padding: 20, color: "#667085", fontWeight: 600 }}>{label}</div>;
}

function EmptyState({ label }) {
  return (
    <div style={{ padding: 18, border: "1px dashed #c9c0b0", borderRadius: 8, color: "#667085", background: "#fbfaf7", fontWeight: 600 }}>
      {label}
    </div>
  );
}

function filterButtonStyle(active) {
  return {
    border: `1px solid ${active ? "#1f2937" : "#c9c0b0"}`,
    background: active ? "#1f2937" : "#fffdf8",
    color: active ? "white" : "#344054",
    borderRadius: 999,
    padding: "7px 11px",
    cursor: "pointer",
    fontWeight: 600,
  };
}

function primaryButtonStyle(disabled = false, extra = {}) {
  return {
    border: "none",
    borderRadius: 7,
    background: disabled ? "#98a2b3" : "#1f2937",
    color: "white",
    padding: "9px 14px",
    fontWeight: 600,
    cursor: disabled ? "default" : "pointer",
    ...extra,
  };
}

function secondaryButtonStyle(extra = {}) {
  return {
    border: "1px solid #c9c0b0",
    borderRadius: 7,
    background: "#fffdf8",
    color: "#344054",
    padding: "9px 13px",
    fontWeight: 600,
    cursor: "pointer",
    ...extra,
  };
}

function displayCell(value) {
  if (value === null || value === undefined || value === "") return "-";
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function trim(value, limit) {
  if (!value) return "";
  const text = String(value).replace(/\s+/g, " ").trim();
  return text.length <= limit ? text : `${text.slice(0, limit - 1)}...`;
}
