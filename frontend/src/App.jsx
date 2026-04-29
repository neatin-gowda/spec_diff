import React, { useEffect, useMemo, useRef, useState } from "react";

const API = import.meta.env.VITE_API_BASE || "/api";

const BRAND = {
  name: "DocuLens AI Agent",
  subtitle: "Document comparison with semantic review, visual evidence, citations, and reports.",
};

const FILE_ACCEPT = ".pdf,.png,.jpg,.jpeg,.tif,.tiff,.bmp,.webp,.doc,.docx,.xls,.xlsx,.xlsm,.xlsb,.csv,.tsv";

const COLORS = {
  ADDED: { bg: "rgba(31,160,70,.16)", border: "#1e8a47", text: "#176c38", chip: "#eef8f1" },
  DELETED: { bg: "rgba(218,54,54,.14)", border: "#bb3030", text: "#9f2525", chip: "#fff2f2" },
  MODIFIED: { bg: "rgba(218,185,42,.20)", border: "#9a7a10", text: "#735c11", chip: "#fff8df" },
  UNCHANGED: { bg: "rgba(96,108,128,.12)", border: "#98a2b3", text: "#475467", chip: "#f2f4f7" },
  MATCH: { bg: "#eef4ff", border: "#6b7da8", text: "#344054", chip: "#eef4ff" },
};

const css = `
  * { box-sizing: border-box; }
  html, body, #root { min-height: 100%; }
  body { margin: 0; overflow-x: hidden; }
  button, input, select, textarea { font: inherit; }
  button { transition: background .15s ease, border-color .15s ease, color .15s ease, opacity .15s ease, transform .15s ease; }
  button:not(:disabled):hover { transform: translateY(-1px); }
  button:disabled { transform: none; }
  code { background: #f6f1e8; border: 1px solid #e2d8c8; border-radius: 5px; padding: 1px 5px; }
  .dl-scrollbar::-webkit-scrollbar { height: 10px; width: 10px; }
  .dl-scrollbar::-webkit-scrollbar-thumb { background: #c9c0b0; border-radius: 999px; }
  .dl-scrollbar::-webkit-scrollbar-track { background: #f2ece2; }
  .progress-track {
    height: 7px;
    background: #e9e2d7;
    border-radius: 999px;
    overflow: hidden;
    position: relative;
  }
  .progress-fill {
    height: 100%;
    min-width: 7%;
    border-radius: 999px;
    overflow: hidden;
    position: relative;
    background: linear-gradient(90deg, #2f5f4f 0%, #3f8067 48%, #2f5f4f 100%);
    transition: width 450ms ease, background 250ms ease;
  }
  .progress-fill::after {
    content: "";
    position: absolute;
    inset: 0;
    transform: translateX(-100%);
    background: linear-gradient(90deg, transparent, rgba(255,255,255,.45), transparent);
    animation: progress-shimmer 1.45s ease-in-out infinite;
  }
  .progress-fill.failed {
    background: #bb3030;
  }
  .progress-fill.failed::after {
    display: none;
  }
  @keyframes progress-shimmer {
    100% { transform: translateX(100%); }
  }
  .grid-safe {
    min-width: 0;
  }
  .viewer-grid {
    align-items: start;
    min-width: 0;
  }
  .viewer-grid > * {
    min-width: 0;
  }
  .doc-viewer-shell {
    min-width: 0;
  }
  .doc-frame {
    position: relative;
    border: 1px solid #b7ae9f;
    background: #f9f6ef;
    min-height: 520px;
    overflow: visible;
  }
  .doc-frame.native {
    background: #f7f2e9;
  }
  .native-page {
    width: 100%;
    min-width: 0;
    min-height: 520px;
    padding: 14px;
    color: #1f2937;
  }
  .native-page.document {
    max-width: 980px;
    margin: 0 auto;
    background: #fffdf8;
    box-shadow: 0 1px 4px rgba(31,41,55,.08);
  }
  .native-page.spreadsheet {
    min-width: 100%;
    background: #fffdf8;
  }
  .native-block {
    max-width: 100%;
    overflow-wrap: anywhere;
  }
  .native-token {
    border-radius: 4px;
    padding: 0 2px;
  }
  .native-token-delete,
  .native-token-replace-base {
    color: #9f2525;
    background: rgba(218,54,54,.16);
    text-decoration: line-through;
    text-decoration-thickness: 1px;
  }
  .native-token-insert,
  .native-token-replace-target {
    color: #176c38;
    background: rgba(31,160,70,.16);
    font-weight: 600;
  }
  .native-table-wrap {
    max-width: 100%;
    overflow-x: auto;
    border: 1px solid #e9dfd0;
    border-radius: 6px;
    background: #fffdf8;
  }
  .native-table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
  }
  .native-table.spreadsheet {
    table-layout: auto;
    min-width: 720px;
  }
  .native-table th,
  .native-table td {
    overflow-wrap: anywhere;
    vertical-align: top;
  }
  .table-selected-stack {
    display: grid;
    grid-template-columns: 1fr;
    gap: 14px;
    margin-bottom: 14px;
    min-width: 0;
  }
  .table-preview-shell {
    max-width: 100%;
    min-width: 0;
    overflow: hidden;
  }
  .table-scroll-frame {
    max-width: 100%;
    overflow-x: auto;
    overflow-y: hidden;
    border: 1px solid #eee7dc;
    border-radius: 8px;
  }
  .cell-wrap {
    white-space: normal;
    overflow-wrap: anywhere;
    word-break: normal;
  }
  .cell-truncate {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  @media (max-width: 1200px) {
    .table-picker-grid, .table-config-grid {
      grid-template-columns: 1fr !important;
    }
  }
  @media (max-width: 760px) {
    .table-action-grid {
      grid-template-columns: 1fr !important;
    }
    .table-action-grid button {
      width: 100%;
    }
  }
  @media (max-width: 980px) {
    .upload-grid, .viewer-grid, .two-grid, .report-metrics, .table-picker-grid, .table-config-grid {
      grid-template-columns: 1fr !important;
    }
    .header-actions { justify-content: flex-start !important; }
  }
`;

const shellStyle = {
  minHeight: "100vh",
  background: "#f8f5ef",
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
  borderRadius: 8,
  boxShadow: "0 1px 3px rgba(31,41,55,.08)",
};

export default function App() {
  const [workspace, setWorkspace] = useState("home");
  const [runId, setRunId] = useState(null);
  const [meta, setMeta] = useState(null);
  const [tab, setTab] = useState("viewer");
  const [pageNum, setPageNum] = useState(1);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [extractRunId, setExtractRunId] = useState(null);
  const [extractMeta, setExtractMeta] = useState(null);
  const [extractBusy, setExtractBusy] = useState(false);
  const [extractError, setExtractError] = useState("");
  const [extractTab, setExtractTab] = useState("overview");
  const [jobError, setJobError] = useState("");

  const resetAll = () => {
    setWorkspace("home");
    setRunId(null);
    setMeta(null);
    setPageNum(1);
    setTab("viewer");
    setError("");
    setBusy(false);
    setExtractRunId(null);
    setExtractMeta(null);
    setExtractBusy(false);
    setExtractError("");
    setExtractTab("overview");
  };

  const goWorkspace = (nextWorkspace) => {
    if (nextWorkspace === "home") {
      resetAll();
    } else {
      setWorkspace(nextWorkspace);
      setError("");
      setExtractError("");
      setJobError("");
    }

    if (typeof window !== "undefined" && window.history?.pushState) {
      window.history.pushState({ doculensWorkspace: nextWorkspace }, "", window.location.href);
    }
  };

  useEffect(() => {
    if (typeof window === "undefined" || !window.history?.replaceState) return undefined;

    window.history.replaceState({ doculensWorkspace: "home" }, "", window.location.href);
    const onPopState = () => resetAll();
    window.addEventListener("popstate", onPopState);

    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    if (!runId || !busy) return;

    let cancelled = false;
    let timer = null;

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
          setError(normalizeErrorMessage(data.error || data.status_message || "Comparison failed."));
          return;
        }

        timer = setTimeout(poll, 1000);
      } catch (err) {
        if (cancelled) return;
        setBusy(false);
        setError(friendlyFetchError(err));
      }
    };

    poll();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [runId, busy]);

  useEffect(() => {
    if (!extractRunId || !extractBusy) return;

    let cancelled = false;
    let timer = null;

    const poll = async () => {
      try {
        const resp = await fetch(`${API}/extract-runs/${extractRunId}`);
        if (!resp.ok) throw new Error(await readResponseError(resp));

        const data = await resp.json();
        if (cancelled) return;

        setExtractMeta(data);

        if (data.status === "complete") {
          setExtractBusy(false);
          setExtractTab("overview");
          return;
        }

        if (data.status === "failed") {
          setExtractBusy(false);
          setExtractError(normalizeErrorMessage(data.error || data.status_message || "Extraction failed."));
          return;
        }

        timer = setTimeout(poll, 1000);
      } catch (err) {
        if (cancelled) return;
        setExtractBusy(false);
        setExtractError(friendlyFetchError(err));
      }
    };

    poll();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [extractRunId, extractBusy]);

  const onUpload = async (e) => {
    e.preventDefault();

    const form = new FormData(e.currentTarget);
    const base = form.get("base");
    const target = form.get("target");

    if (!base || !target || !base.name || !target.name) {
      setError("Please select both documents before starting.");
      return;
    }

    setWorkspace("compare");
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
      setBusy(false);
      setMeta({
        run_id: data.run_id,
        status: data.status,
        status_message: data.status_message || "Starting comparison",
        progress: data.progress || 5,
        stats: {},
        coverage: {},
        n_pages_base: 0,
        n_pages_target: 0,
      });
      setWorkspace("jobs");
    } catch (err) {
      setBusy(false);
      setError(friendlyFetchError(err));
    }
  };

  const onExtractUpload = async (e) => {
    e.preventDefault();

    const form = new FormData(e.currentTarget);
    const documents = form.getAll("document").filter((file) => file && file.name);

    if (!documents.length) {
      setExtractError("Please select at least one document, spreadsheet, PDF, or image before starting extraction.");
      return;
    }

    setWorkspace("extract");
    setExtractBusy(true);
    setExtractError("");
    setExtractRunId(null);
    setExtractTab("overview");
    setExtractMeta({
      status: "uploading",
      status_message: "Uploading document",
      progress: 3,
      summary: {},
    });

    try {
      const resp = await fetch(`${API}/extract`, { method: "POST", body: form });
      if (!resp.ok) throw new Error(await readResponseError(resp));

      const data = await resp.json();
      setExtractRunId(data.run_id);
      setExtractBusy(false);
      setExtractMeta({
        run_id: data.run_id,
        status: data.status,
        status_message: data.status_message || "Starting extraction",
        progress: data.progress || 5,
        summary: {},
      });
      setWorkspace("jobs");
    } catch (err) {
      setExtractBusy(false);
      setExtractError(friendlyFetchError(err));
    }
  };

  const openJob = async (job) => {
    setJobError("");
    try {
      if (job.kind === "extraction") {
        const resp = await fetch(`${API}/extract-runs/${job.run_id}`);
        if (!resp.ok) throw new Error(await readResponseError(resp));
        const data = await resp.json();
        setExtractRunId(job.run_id);
        setExtractMeta(data);
        setExtractBusy(data.status !== "complete" && data.status !== "failed");
        setExtractTab("overview");
        setWorkspace("extract");
        return;
      }

      const resp = await fetch(`${API}/runs/${job.run_id}`);
      if (!resp.ok) throw new Error(await readResponseError(resp));
      const data = await resp.json();
      setRunId(job.run_id);
      setMeta(data);
      setBusy(data.status !== "complete" && data.status !== "failed");
      setTab("viewer");
      setPageNum(1);
      setWorkspace("compare");
    } catch (err) {
      setJobError(friendlyFetchError(err));
    }
  };

  const downloadReport = () => {
    if (runId) window.location.href = `${API}/runs/${runId}/report.pdf`;
  };

  const isComplete = meta?.status === "complete";
  const isExtractComplete = extractMeta?.status === "complete";

  return (
    <div style={shellStyle}>
      <style>{css}</style>
      <div style={pageStyle}>
        <Header
          runId={workspace === "compare" && isComplete ? runId : null}
          workspace={workspace}
          onStartOver={() => goWorkspace("home")}
          onJobs={() => goWorkspace("jobs")}
          onDownloadReport={downloadReport}
        />

        {workspace === "home" && (
          <LandingPage
            onExtract={() => goWorkspace("extract")}
            onCompare={() => goWorkspace("compare")}
            onJobs={() => goWorkspace("jobs")}
          />
        )}

        {workspace === "jobs" && (
          <JobsDashboard onOpenJob={openJob} error={jobError} />
        )}

        {workspace === "compare" && !isComplete && (
          <section style={{ ...panelStyle, padding: 22, marginBottom: 16 }}>
            <UploadPanel onUpload={onUpload} busy={busy} onBack={() => goWorkspace("home")} />
            {busy && meta && (
              <ProcessingState
                progress={meta.progress || 0}
                message={meta.status_message || "Processing documents"}
                status={meta.status || "running"}
              />
            )}
            {error && <ErrorBox message={error} />}
          </section>
        )}

        {workspace === "extract" && !isExtractComplete && (
          <section style={{ ...panelStyle, padding: 22, marginBottom: 16 }}>
            <ExtractUploadPanel onUpload={onExtractUpload} busy={extractBusy} onBack={() => goWorkspace("home")} />
            {extractBusy && extractMeta && (
              <ProcessingState
                progress={extractMeta.progress || 0}
                message={extractMeta.status_message || "Extracting document"}
                status={extractMeta.status || "running"}
              />
            )}
            {extractError && <ErrorBox message={extractError} />}
          </section>
        )}

        {workspace === "compare" && isComplete && runId && meta && (
          <>
            <StatsBar meta={meta} />
            <Tabs tab={tab} setTab={setTab} />

            <main style={{ ...panelStyle, padding: 12 }}>
              {tab === "viewer" && <SideBySide runId={runId} meta={meta} pageNum={pageNum} setPageNum={setPageNum} />}
              {tab === "report" && <ReviewReport runId={runId} />}
              {tab === "query" && <QueryPanel runId={runId} />}
              {tab === "tables" && <TablesWorkspace runId={runId} />}
            </main>
          </>
        )}

        {workspace === "extract" && isExtractComplete && extractRunId && extractMeta && (
          <ExtractionWorkspace
            runId={extractRunId}
            meta={extractMeta}
            tab={extractTab}
            setTab={setExtractTab}
          />
        )}
      </div>
    </div>
  );
}

function Header({ runId, workspace, onStartOver, onJobs, onDownloadReport }) {
  return (
    <header style={{ marginBottom: 18 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 16, flexWrap: "wrap" }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 4 }}>
            <div
              style={{
                width: 30,
                height: 30,
                borderRadius: 7,
                background: "#1f2937",
                color: "white",
                display: "grid",
                placeItems: "center",
                fontSize: 12,
                fontWeight: 650,
              }}
            >
              AI
            </div>
            <h1 style={{ margin: 0, fontSize: 26, letterSpacing: 0, lineHeight: 1.1, fontWeight: 600 }}>
              {BRAND.name}
            </h1>
          </div>
          <p style={{ margin: "6px 0 0", color: "#667085", fontSize: 14 }}>{BRAND.subtitle}</p>
        </div>

        {(runId || workspace !== "home") && (
          <div className="header-actions" style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "flex-end" }}>
            {runId && (
              <button onClick={onDownloadReport} style={primaryButtonStyle()}>
                Export PDF report
              </button>
            )}
            {workspace !== "jobs" && (
              <button onClick={onJobs} style={secondaryButtonStyle()}>
                Job status
              </button>
            )}
            <button onClick={onStartOver} style={secondaryButtonStyle()}>
              New workflow
            </button>
          </div>
        )}
      </div>
    </header>
  );
}

function LandingPage({ onExtract, onCompare, onJobs }) {
  return (
    <section style={{ ...panelStyle, padding: 22 }}>
      <div style={{ marginBottom: 18 }}>
        <h2 style={{ margin: 0, fontSize: 22, fontWeight: 600 }}>Choose a workspace</h2>
        <p style={{ margin: "7px 0 0", color: "#667085", fontSize: 14 }}>
          Use extraction when you want to inspect one or more files. Use comparison when you want to review old vs revised versions.
        </p>
      </div>

      <div className="two-grid" style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 14 }}>
        <WorkspaceCard
          title="Extract documents"
          description="Upload PDFs, images, Word files, spreadsheets, xlsb workbooks, CSV, or TSV and review extracted text, tables, image/OCR content, coverage, JSON, and optional AI analysis."
          action="Start extraction"
          onClick={onExtract}
        />
        <WorkspaceCard
          title="Compare documents"
          description="Upload baseline and revised files, then use the existing side-by-side review, semantic diff, table workspace, Ask Agent, and reports."
          action="Start comparison"
          onClick={onCompare}
        />
        <WorkspaceCard
          title="Job status"
          description="Review queued, running, completed, or failed extraction and comparison jobs without blocking the upload page."
          action="Open jobs"
          onClick={onJobs}
        />
      </div>
    </section>
  );
}

function WorkspaceCard({ title, description, action, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        textAlign: "left",
        border: "1px solid #ded6c8",
        background: "#fbfaf6",
        borderRadius: 8,
        padding: 18,
        cursor: "pointer",
        color: "#202936",
      }}
    >
      <div style={{ fontSize: 17, fontWeight: 650, marginBottom: 7 }}>{title}</div>
      <div style={{ color: "#667085", fontSize: 14, lineHeight: 1.45, minHeight: 62 }}>{description}</div>
      <div style={{ marginTop: 16, color: "#2f5f4f", fontWeight: 650 }}>{action}</div>
    </button>
  );
}

function JobsDashboard({ onOpenJob, error }) {
  const [state, setState] = useState({ loading: true, error: "", jobs: [] });

  const loadJobs = async () => {
    try {
      const resp = await fetch(`${API}/jobs?limit=80`);
      if (!resp.ok) throw new Error(await readResponseError(resp));
      const data = await resp.json();
      setState({ loading: false, error: "", jobs: data.jobs || [] });
    } catch (err) {
      setState({ loading: false, error: friendlyFetchError(err), jobs: [] });
    }
  };

  useEffect(() => {
    let cancelled = false;
    let timer = null;

    const poll = async () => {
      if (cancelled) return;
      await loadJobs();
      if (!cancelled) timer = setTimeout(poll, 1800);
    };

    poll();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, []);

  const jobs = state.jobs || [];

  return (
    <section style={{ ...panelStyle, padding: 18 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap", marginBottom: 14 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 20, fontWeight: 600 }}>Job status</h2>
          <p style={{ margin: "5px 0 0", color: "#667085", fontSize: 13 }}>
            Uploads start a background job. You can start another workflow and return here to open completed results.
          </p>
        </div>
        <button type="button" onClick={loadJobs} style={secondaryButtonStyle()}>
          Refresh
        </button>
      </div>

      {error && <ErrorBox message={error} />}
      {state.error && <ErrorBox message={state.error} />}
      {state.loading && !jobs.length ? (
        <SoftLoading label="Loading jobs" />
      ) : jobs.length === 0 ? (
        <EmptyState label="No jobs are available in this backend session yet." />
      ) : (
        <div className="dl-scrollbar" style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13, minWidth: 860 }}>
            <thead>
              <tr style={{ background: "#1f2937", color: "white" }}>
                <th style={th}>Workflow</th>
                <th style={th}>Documents</th>
                <th style={th}>Status</th>
                <th style={th}>Progress</th>
                <th style={th}>Pages</th>
                <th style={th}>Action</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => {
                const complete = job.status === "complete";
                const failed = job.status === "failed";
                return (
                  <tr key={job.run_id}>
                    <td style={td}>
                      <strong style={{ fontWeight: 650 }}>{job.kind === "extraction" ? "Extraction" : "Comparison"}</strong>
                      <div style={{ color: "#667085", marginTop: 4 }}>{trim(job.run_id, 18)}</div>
                    </td>
                    <td style={td}>
                      {job.kind === "extraction" ? (
                        <div>{job.label || "Uploaded document"}</div>
                      ) : (
                        <div>{job.base_label || "Baseline"} → {job.target_label || "Revised"}</div>
                      )}
                      <div style={{ color: "#667085", marginTop: 4 }}>
                        {[job.source_format, job.base_format, job.target_format].filter(Boolean).join(" / ")}
                      </div>
                    </td>
                    <td style={td}>
                      <JobStatusBadge status={job.status} />
                      {job.status_message && <div style={{ color: "#667085", marginTop: 5 }}>{job.status_message}</div>}
                      {failed && job.error && <div style={{ color: COLORS.DELETED.text, marginTop: 5 }}>{trim(job.error, 160)}</div>}
                    </td>
                    <td style={td}>
                      <ProgressMini value={job.progress || 0} failed={failed} />
                    </td>
                    <td style={td}>
                      {job.kind === "extraction"
                        ? (job.n_pages || "-")
                        : `${job.n_pages_base || "-"} / ${job.n_pages_target || "-"}`}
                    </td>
                    <td style={td}>
                      <button
                        type="button"
                        onClick={() => onOpenJob(job)}
                        disabled={!complete}
                        style={complete ? primaryButtonStyle(false, { height: 36 }) : secondaryButtonStyle({ height: 36, opacity: 0.55, cursor: "default" })}
                      >
                        {complete ? "Open result" : failed ? "Failed" : "Processing"}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function UploadPanel({ onUpload, busy, onBack }) {
  return (
    <form onSubmit={onUpload}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", marginBottom: 14 }}>
        <div>
          <div style={{ fontWeight: 650 }}>Compare two documents</div>
          <div style={{ color: "#667085", fontSize: 13, marginTop: 3 }}>Keep the existing side-by-side visual review, table workspace, Ask Agent, and reports.</div>
        </div>
        <button type="button" onClick={onBack} disabled={busy} style={secondaryButtonStyle(busy ? { opacity: 0.65, cursor: "default" } : {})}>
          Back to workspaces
        </button>
      </div>

      <div
        className="upload-grid"
        style={{
          display: "grid",
          gridTemplateColumns: "minmax(260px, 1fr) minmax(260px, 1fr) 210px",
          gap: 16,
          alignItems: "stretch",
        }}
      >
        <FileInput label="Baseline document" helper="Previous, approved, or reference file" name="base" disabled={busy} />
        <FileInput label="Revised document" helper="Latest, proposed, or updated file" name="target" disabled={busy} />

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
        <Capability label="Semantic review" detail="Finds meaningful content changes, not layout-only differences." />
        <Capability label="Visual evidence" detail="Renders uploaded files as PDFs for side-by-side review." />
        <Capability label="Business report" detail="Creates a downloadable PDF report with citations and review items." />
      </div>
    </form>
  );
}

function ExtractUploadPanel({ onUpload, busy, onBack }) {
  return (
    <form onSubmit={onUpload}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", marginBottom: 14 }}>
        <div>
          <div style={{ fontWeight: 650 }}>Extract documents</div>
          <div style={{ color: "#667085", fontSize: 13, marginTop: 3 }}>Upload one or more files and review extracted text, tables, OCR content, and structured JSON output.</div>
        </div>
        <button type="button" onClick={onBack} disabled={busy} style={secondaryButtonStyle(busy ? { opacity: 0.65, cursor: "default" } : {})}>
          Back to workspaces
        </button>
      </div>

      <div
        className="upload-grid"
        style={{
          display: "grid",
          gridTemplateColumns: "minmax(280px, 1fr) 230px",
          gap: 16,
          alignItems: "stretch",
        }}
      >
        <FileInput
          label="Document or image"
          helper="PDF, image, Word, Excel, xlsb, CSV, or TSV. Multiple files can be extracted together."
          name="document"
          disabled={busy}
          multiple
        />

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
            <input type="checkbox" name="use_ai" value="true" disabled={busy} />
            Optional AI analysis
          </label>

          <button disabled={busy} style={primaryButtonStyle(busy, { height: 44 })}>
            {busy ? "Extracting" : "Extract content"}
          </button>

          <div style={{ color: "#667085", fontSize: 12, lineHeight: 1.35 }}>
            Extraction runs deterministically first. AI only reviews extracted evidence when enabled.
          </div>
        </div>
      </div>

      <div style={{ marginTop: 16, display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 10 }}>
        <Capability label="Text and structure" detail="Extracts headings, paragraphs, lists, key-values, and page-level content." />
        <Capability label="Tables" detail="Detects tables, headers, rows, cells, sample values, and table quality signals." />
        <Capability label="Images and OCR" detail="Uses OCR fallback for scanned pages and image-based content." />
      </div>
    </form>
  );
}

function FileInput({ label, helper, name, disabled, multiple = false }) {
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
        accept={FILE_ACCEPT}
        multiple={multiple}
        required
        disabled={disabled}
        onClick={(e) => e.stopPropagation()}
        onChange={(e) => {
          const files = Array.from(e.target.files || []);
          setFileName(files.length > 1 ? `${files.length} files selected` : files[0]?.name || "");
        }}
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
          PDF IMG DOC XLS CSV
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
        {fileName || "Select a file"}
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
  const width = Math.max(7, safeProgress);

  return (
    <div style={{ marginTop: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 7, color: "#475467", fontSize: 13 }}>
        <span style={{ fontWeight: 600 }}>{message}</span>
        <span>{safeProgress}%</span>
      </div>
      <div className="progress-track">
        <div
          className={`progress-fill ${status === "failed" ? "failed" : ""}`}
          style={{
            width: `${width}%`,
          }}
        />
      </div>
      <p style={{ margin: "10px 0 0", color: "#667085", fontSize: 13 }}>
        The comparison is still running. This view updates automatically as the backend reports progress.
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
        whiteSpace: "pre-wrap",
      }}
    >
      {normalizeErrorMessage(message)}
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
    ["tables", "Table workspace"],
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

function ExtractionWorkspace({ runId, meta, tab, setTab }) {
  return (
    <>
      <ExtractionStats meta={meta} />
      <ExtractionTabs tab={tab} setTab={setTab} />
      <main style={{ ...panelStyle, padding: 12 }}>
        {tab === "overview" && <ExtractionOverview runId={runId} meta={meta} />}
        {tab === "tables" && <ExtractionTables runId={runId} />}
        {tab === "text" && <ExtractionBlocks runId={runId} />}
        {tab === "json" && <ExtractionJsonPreview runId={runId} meta={meta} />}
        {tab === "preview" && <ExtractionPreview runId={runId} meta={meta} />}
      </main>
    </>
  );
}

function ExtractionStats({ meta }) {
  const summary = meta.summary || {};
  return (
    <section style={{ ...panelStyle, padding: 12, display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap", alignItems: "center" }}>
      <StatChip label="Format" value={(meta.source_format || "-").toUpperCase()} />
      <StatChip label="Documents" value={meta.documents?.length || summary.document_count || 1} />
      <StatChip label="Coverage" value={typeof meta.coverage === "number" ? `${meta.coverage.toFixed(1)}%` : "-"} />
      <StatChip label="Quality" value={summary.quality || "-"} />
      <StatChip label="Tables" value={summary.table_count || 0} />
      <StatChip label="Blocks" value={Object.values(summary.block_counts || {}).reduce((a, b) => a + Number(b || 0), 0)} />
      <StatChip label="Pages" value={meta.n_pages || meta.native_pages || 0} />
    </section>
  );
}

function ExtractionTabs({ tab, setTab }) {
  const items = [
    ["overview", "Extraction overview"],
    ["tables", "Extracted tables"],
    ["text", "Text blocks"],
    ["json", "Structured JSON"],
    ["preview", "Preview"],
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

function ExtractionOverview({ runId, meta }) {
  const summary = meta.summary || {};
  const ai = meta.ai_analysis;
  const aiResult = ai?.result || null;

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap", marginBottom: 12 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 18, fontWeight: 650 }}>{meta.label || "Extracted document"}</h2>
          <p style={{ margin: "6px 0 0", color: "#667085", fontSize: 13 }}>{summary.message || "Extraction complete."}</p>
        </div>
        <button onClick={() => { window.location.href = `${API}/extract-runs/${runId}/json`; }} style={primaryButtonStyle(false)}>
          Download JSON
        </button>
      </div>

      <div className="report-metrics" style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 10, marginBottom: 12 }}>
        <MetricCard label="Extraction coverage" value={typeof meta.coverage === "number" ? `${meta.coverage.toFixed(1)}%` : "-"} />
        <MetricCard label="Tables detected" value={summary.table_count || 0} />
        <MetricCard label="Table rows" value={summary.table_row_count || 0} />
        <MetricCard label="Image/OCR blocks" value={summary.figure_count || 0} />
      </div>

      <div style={{ ...panelStyle, padding: 14, boxShadow: "none", marginBottom: 12 }}>
        <div style={{ fontWeight: 650, marginBottom: 8 }}>Block breakdown</div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          {Object.entries(summary.block_counts || {}).map(([key, value]) => (
            <StatChip key={key} label={key.replace("_", " ")} value={value} />
          ))}
          {Object.keys(summary.block_counts || {}).length === 0 && <span style={{ color: "#667085" }}>No block statistics available.</span>}
        </div>
      </div>

      {ai && (
        <div style={{ ...panelStyle, padding: 14, boxShadow: "none" }}>
          <div style={{ fontWeight: 650, marginBottom: 8 }}>
            AI-assisted analysis {ai.available ? "- available" : "- unavailable"}
          </div>
          {!ai.available && <div style={{ color: COLORS.DELETED.text }}>{ai.error || "AI analysis was not generated."}</div>}
          {aiResult && (
            <div style={{ color: "#344054", lineHeight: 1.5 }}>
              <p style={{ marginTop: 0 }}>{aiResult.executive_summary || "AI analysis completed."}</p>
              {Array.isArray(aiResult.key_items) && aiResult.key_items.length > 0 && (
                <GenericRowsTable
                  columns={["Item"]}
                  rows={aiResult.key_items.slice(0, 20).map((item) => ({ Item: typeof item === "string" ? item : JSON.stringify(item) }))}
                />
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function ExtractionTables({ runId }) {
  const [state, setState] = useState({ loading: true, error: "", tables: [] });

  useEffect(() => {
    let cancelled = false;
    setState({ loading: true, error: "", tables: [] });
    fetch(`${API}/extract-runs/${runId}/tables?include_rows=true`)
      .then(async (resp) => {
        if (!resp.ok) throw new Error(await readResponseError(resp));
        return resp.json();
      })
      .then((data) => {
        if (!cancelled) setState({ loading: false, error: "", tables: data.tables || [] });
      })
      .catch((err) => {
        if (!cancelled) setState({ loading: false, error: friendlyFetchError(err), tables: [] });
      });
    return () => { cancelled = true; };
  }, [runId]);

  if (state.loading) return <SoftLoading label="Loading extracted tables..." />;
  if (state.error) return <ErrorBox message={state.error} />;
  if (!state.tables.length) return <EmptyState label="No tables were detected in this document." />;

  return (
    <div style={{ display: "grid", gap: 12 }}>
      {state.tables.map((table) => (
        <div key={table.id} style={{ ...panelStyle, padding: 12, boxShadow: "none" }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap", marginBottom: 8 }}>
            <div>
              <div style={{ fontWeight: 650 }}>{table.display_name || table.title || "Detected table"}</div>
              <div style={{ color: "#667085", fontSize: 13, marginTop: 3 }}>
                {table.page_label} · {table.n_columns} columns · {table.n_rows} rows · header quality {Math.round((table.header_quality || 0) * 100)}%
                {table.extraction_confidence ? ` · extraction ${Math.round(table.extraction_confidence * 100)}%` : ""}
              </div>
            </div>
            <code>{String(table.id || "").slice(0, 8)}</code>
          </div>
          {Array.isArray(table.quality_warnings) && table.quality_warnings.length > 0 && (
            <div style={{ color: "#8a5a00", fontSize: 13, marginBottom: 8 }}>
              Review note: {table.quality_warnings.slice(0, 2).join(" ")}
            </div>
          )}
          <div style={{ color: "#475467", fontSize: 13, marginBottom: 8 }}>
            Columns: {(table.columns || []).slice(0, 12).join(" | ") || "No columns detected"}
          </div>
          <TablePreview columns={table.columns || []} rows={table.rows || table.row_preview || []} />
        </div>
      ))}
    </div>
  );
}

function ExtractionBlocks({ runId }) {
  const [state, setState] = useState({ loading: true, error: "", blocks: [] });

  useEffect(() => {
    let cancelled = false;
    setState({ loading: true, error: "", blocks: [] });
    fetch(`${API}/extract-runs/${runId}/blocks?limit=1000`)
      .then(async (resp) => {
        if (!resp.ok) throw new Error(await readResponseError(resp));
        return resp.json();
      })
      .then((data) => {
        if (!cancelled) setState({ loading: false, error: "", blocks: data.blocks || [] });
      })
      .catch((err) => {
        if (!cancelled) setState({ loading: false, error: friendlyFetchError(err), blocks: [] });
      });
    return () => { cancelled = true; };
  }, [runId]);

  if (state.loading) return <SoftLoading label="Loading extracted text blocks..." />;
  if (state.error) return <ErrorBox message={state.error} />;

  const rows = state.blocks
    .filter((block) => block.text || block.type === "table")
    .slice(0, 500)
    .map((block) => ({
      Page: block.page_number,
      Type: block.type,
      Path: block.path,
      Text: trim(block.text || JSON.stringify(block.payload || {}), 700),
    }));

  return rows.length ? (
    <GenericRowsTable columns={["Page", "Type", "Path", "Text"]} rows={rows} />
  ) : (
    <EmptyState label="No extracted text blocks were returned." />
  );
}

function ExtractionJsonPreview({ runId, meta }) {
  const [state, setState] = useState({ loading: true, error: "", data: null });
  const [page, setPage] = useState(1);
  const total = meta.n_pages || meta.native_pages || 1;

  useEffect(() => {
    let cancelled = false;
    setState({ loading: true, error: "", data: null });
    fetchStructuredExtraction(runId)
      .then((data) => {
        if (!cancelled) setState({ loading: false, error: "", data });
      })
      .catch((err) => {
        if (!cancelled) setState({ loading: false, error: friendlyFetchError(err), data: null });
      });
    return () => { cancelled = true; };
  }, [runId]);

  if (state.loading) return <SoftLoading label="Building structured JSON preview..." />;
  if (state.error) return <ErrorBox message={state.error} />;

  const data = state.data || {};
  const fields = data.semantic_fields || [];
  const tables = data.tables || [];
  const pages = data.pages || [];
  const businessDocs = data.business_structure?.documents || [];
  const documentSummary = data.document_summary || {};
  const quality = documentSummary.extraction_quality || {};

  return (
    <div className="two-grid" style={{ display: "grid", gridTemplateColumns: "minmax(0, .95fr) minmax(0, 1.05fr)", gap: 14, alignItems: "start" }}>
      <div style={{ minWidth: 0 }}>
        <div style={{ display: "flex", gap: 10, alignItems: "center", marginBottom: 10, flexWrap: "wrap" }}>
          <button disabled={page <= 1} onClick={() => setPage(Math.max(1, page - 1))} style={navButtonStyle(page <= 1)}>Prev</button>
          <strong>Actual page {page} / {total}</strong>
          <button disabled={page >= total} onClick={() => setPage(Math.min(total, page + 1))} style={navButtonStyle(page >= total)}>Next</button>
        </div>
        <div className="doc-frame" style={{ display: "flex", justifyContent: "center", padding: 10 }}>
          <img
            alt={`Actual document page ${page}`}
            src={`${API}/extract-runs/${runId}/pages/${page}`}
            style={{ maxWidth: "100%", height: "auto", display: "block" }}
          />
        </div>
      </div>

      <div style={{ minWidth: 0, display: "grid", gap: 12 }}>
        <div style={{ ...panelStyle, padding: 12, boxShadow: "none" }}>
          <div style={{ fontWeight: 650, marginBottom: 8 }}>Business extraction summary</div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", color: "#344054", fontSize: 13 }}>
            <span style={softPillStyle}>Document: {documentSummary.label || meta.label || "uploaded file"}</span>
            <span style={softPillStyle}>Type: {documentSummary.source_type || meta.source_format || "document"}</span>
            <span style={softPillStyle}>Template: {documentSummary.detected_template || "generic document"}</span>
            <span style={softPillStyle}>Quality: {quality.grade || "not rated"}</span>
            {Number.isFinite(quality.score) && <span style={softPillStyle}>Score: {Math.round(quality.score * 100)}%</span>}
            {documentSummary.detected_language && <span style={softPillStyle}>Script: {documentSummary.detected_language}</span>}
          </div>
          {Array.isArray(quality.warnings) && quality.warnings.length > 0 && (
            <div style={{ color: "#8a5a00", fontSize: 13, marginTop: 8, lineHeight: 1.4 }}>
              {quality.warnings.slice(0, 3).map((w) => w.message || w).join(" ")}
            </div>
          )}
        </div>

	      <div style={{ ...panelStyle, padding: 12, boxShadow: "none" }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center", marginBottom: 8 }}>
            <div>
              <div style={{ fontWeight: 650 }}>Readable extraction JSON</div>
              <div style={{ color: "#667085", fontSize: 13, marginTop: 3 }}>
	                {fields.length} field(s), {tables.length} table(s), {data.sections?.length || 0} section(s), {pages.length} page block(s)
              </div>
            </div>
            <button onClick={() => { window.location.href = `${API}/extract-runs/${runId}/json`; }} style={secondaryButtonStyle()}>
              Download JSON
            </button>
          </div>

          {fields.length > 0 ? (
            <GenericRowsTable
              columns={["field", "value", "page", "source", "citation"]}
              rows={fields.slice(0, 80)}
            />
          ) : (
            <EmptyState label="No key-value fields were detected. Check extracted tables and text blocks." />
          )}
        </div>

        {tables.length > 0 && (
          <div style={{ ...panelStyle, padding: 12, boxShadow: "none" }}>
            <div style={{ fontWeight: 650, marginBottom: 8 }}>Extracted business tables</div>
            <GenericRowsTable
              columns={["title", "page", "area", "row_count", "columns"]}
              rows={tables.slice(0, 30).map((table) => ({
                title: table.title,
                page: table.page,
                area: table.area,
                row_count: table.row_count,
                columns: (table.columns || []).join(" | "),
              }))}
            />
          </div>
        )}

        {businessDocs.length > 0 && (
          <div style={{ ...panelStyle, padding: 12, boxShadow: "none" }}>
            <div style={{ fontWeight: 650, marginBottom: 8 }}>Business structure preview</div>
            <BusinessStructurePreview documents={businessDocs} />
          </div>
        )}

        <div style={{ ...panelStyle, padding: 12, boxShadow: "none" }}>
          <div style={{ fontWeight: 650, marginBottom: 8 }}>JSON payload preview</div>
          <pre className="dl-scrollbar" style={{ margin: 0, maxHeight: 360, overflow: "auto", background: "#fbfaf6", border: "1px solid #e0d8ca", borderRadius: 8, padding: 12, fontSize: 12, lineHeight: 1.45, whiteSpace: "pre-wrap" }}>
            {JSON.stringify(data, null, 2)}
          </pre>
        </div>
      </div>
    </div>
  );
}

function ExtractionPreview({ runId, meta }) {
  const [page, setPage] = useState(1);
  const total = meta.n_pages || 1;

  return (
    <div>
      <div style={{ display: "flex", gap: 10, alignItems: "center", marginBottom: 10 }}>
        <button disabled={page <= 1} onClick={() => setPage(Math.max(1, page - 1))} style={navButtonStyle(page <= 1)}>Prev</button>
        <strong>Page {page} / {total}</strong>
        <button disabled={page >= total} onClick={() => setPage(Math.min(total, page + 1))} style={navButtonStyle(page >= total)}>Next</button>
      </div>
      <div className="doc-frame" style={{ display: "flex", justifyContent: "center", padding: 10 }}>
        <img
          alt={`Extracted preview page ${page}`}
          src={`${API}/extract-runs/${runId}/pages/${page}`}
          style={{ maxWidth: "100%", height: "auto", display: "block" }}
        />
      </div>
    </div>
  );
}

function BusinessStructurePreview({ documents }) {
  return (
    <div style={{ display: "grid", gap: 10 }}>
      {documents.slice(0, 4).map((doc) => (
        <div key={doc.document_index || doc.label} style={{ border: "1px solid #e0d8ca", borderRadius: 8, background: "#fffdf8", padding: 10 }}>
          <div style={{ fontWeight: 650, marginBottom: 8 }}>
            {doc.label || `Document ${doc.document_index || ""}`}
          </div>
          <div style={{ display: "grid", gap: 8 }}>
            {(doc.sections || []).slice(0, 8).map((section, idx) => (
              <BusinessSectionCard key={`${section.path || section.title}-${idx}`} section={section} />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function BusinessSectionCard({ section }) {
  const fields = section.fields || [];
  const inlineRecords = section.inline_records || [];
  const tables = section.tables || [];
  const content = section.content || [];

  return (
    <details open={false} style={{ border: "1px solid #e9dfd0", borderRadius: 7, background: "#fbfaf6", padding: 9 }}>
      <summary style={{ cursor: "pointer", fontWeight: 650, color: "#344054" }}>
        {section.title || "Section"} <span style={{ color: "#667085", fontWeight: 500 }}>p.{section.page || "-"}</span>
      </summary>
      <div style={{ marginTop: 9, display: "grid", gap: 8 }}>
        {fields.length > 0 && (
          <div>
            <div style={{ fontSize: 12, color: "#667085", fontWeight: 650, marginBottom: 4 }}>Extracted fields</div>
            <GenericRowsTable columns={["field", "value", "page"]} rows={fields.slice(0, 12)} />
          </div>
        )}

        {inlineRecords.length > 0 && (
          <div>
            <div style={{ fontSize: 12, color: "#667085", fontWeight: 650, marginBottom: 4 }}>Inline records</div>
            <GenericRowsTable
              columns={inferColumns(inlineRecords.map((r) => r.values || r))}
              rows={inlineRecords.slice(0, 10).map((r) => r.values || r)}
            />
          </div>
        )}

        {tables.length > 0 && (
          <div>
            <div style={{ fontSize: 12, color: "#667085", fontWeight: 650, marginBottom: 4 }}>Related tables</div>
            {tables.slice(0, 4).map((table, idx) => (
              <div key={`${table.title}-${idx}`} style={{ marginBottom: 8 }}>
                <div style={{ fontSize: 12, color: "#344054", fontWeight: 650, marginBottom: 4 }}>
                  {table.title || "Detected table"} · {table.row_count || 0} rows
                </div>
                <TablePreview columns={table.columns || []} rows={table.sample_rows || []} />
              </div>
            ))}
          </div>
        )}

        {content.length > 0 && (
          <div>
            <div style={{ fontSize: 12, color: "#667085", fontWeight: 650, marginBottom: 4 }}>Related content</div>
            <ul style={{ margin: 0, paddingLeft: 18, color: "#344054", fontSize: 13, lineHeight: 1.45 }}>
              {content.slice(0, 8).map((item, idx) => (
                <li key={idx}>{trim(item.text, 220)}</li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </details>
  );
}

function SideBySide({ runId, meta, pageNum, setPageNum }) {
  const basePages = meta.base_format && meta.base_format !== "pdf" ? meta.base_native_pages || meta.n_pages_base || 1 : meta.n_pages_base || 1;
  const targetPages = meta.target_format && meta.target_format !== "pdf" ? meta.target_native_pages || meta.n_pages_target || 1 : meta.n_pages_target || 1;
  const maxPages = Math.max(basePages, targetPages);
  const [basePage, setBasePage] = useState(pageNum);
  const [targetPage, setTargetPage] = useState(pageNum);

  useEffect(() => {
    setBasePage(pageNum);
    setTargetPage(pageNum);
  }, [runId, pageNum]);

  const goBoth = (nextPage) => {
    const safePage = Math.max(1, Math.min(maxPages, nextPage));
    setPageNum(safePage);
    setBasePage(safePage);
    setTargetPage(safePage);
  };

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12, flexWrap: "wrap" }}>
        <button onClick={() => goBoth(pageNum - 1)} disabled={pageNum <= 1} style={navButtonStyle(pageNum <= 1)}>
          Prev both
        </button>
        <span style={{ fontSize: 17, fontWeight: 650, minWidth: 100 }}>Page {pageNum} / {maxPages}</span>
        <button onClick={() => goBoth(pageNum + 1)} disabled={pageNum >= maxPages} style={navButtonStyle(pageNum >= maxPages)}>
          Next both
        </button>
        <Legend />
      </div>

      <div className="viewer-grid" style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr)", gap: 14 }}>
        <PageView
          runId={runId}
          side="base"
          pageNum={basePage}
          setPageNum={setBasePage}
          totalPages={basePages}
          label="Baseline document"
          docName={meta.base_label}
          format={meta.base_format}
        />
        <PageView
          runId={runId}
          side="target"
          pageNum={targetPage}
          setPageNum={setTargetPage}
          totalPages={targetPages}
          label="Revised document"
          docName={meta.target_label}
          format={meta.target_format}
        />
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

function smallNavButtonStyle(disabled) {
  return {
    border: "1px solid #c9c0b0",
    background: disabled ? "#f1ece3" : "#fffdf8",
    color: disabled ? "#98a2b3" : "#344054",
    borderRadius: 6,
    padding: "5px 8px",
    cursor: disabled ? "default" : "pointer",
    fontWeight: 600,
    fontSize: 12,
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

function PageView({ runId, side, pageNum, setPageNum, totalPages, label, docName, format }) {
  const [overlay, setOverlay] = useState({ regions: [] });
  const [nativePage, setNativePage] = useState(null);
  const [imageState, setImageState] = useState("idle");
  const pageExists = pageNum >= 1 && pageNum <= totalPages;
  const useNativeViewer = format && format !== "pdf";

  useEffect(() => {
    setImageState(pageExists && !useNativeViewer ? "loading" : "idle");

    if (!pageExists) {
      setOverlay({ regions: [] });
      setNativePage(null);
      return;
    }

    if (useNativeViewer) {
      setOverlay({ regions: [] });
      fetch(`${API}/runs/${runId}/native-page/${side}/${pageNum}`)
        .then((r) => r.json())
        .then(setNativePage)
        .catch(() => setNativePage({ items: [] }));
      return;
    }

    setNativePage(null);
    fetch(`${API}/runs/${runId}/overlay/${side}/${pageNum}`)
      .then((r) => r.json())
      .then(setOverlay)
      .catch(() => setOverlay({ regions: [] }));
  }, [runId, side, pageNum, pageExists, useNativeViewer]);

  return (
    <div className="doc-viewer-shell">
      <div style={{ marginBottom: 7, display: "flex", justifyContent: "space-between", gap: 10, alignItems: "flex-end", flexWrap: "wrap" }}>
        <div>
          <div style={{ fontSize: 13, color: "#667085", fontWeight: 600 }}>{label}</div>
          <div style={{ fontSize: 14, color: "#344054", fontWeight: 650 }}>
            {docName} - {pageExists ? `page ${pageNum}` : "no page"}
            {format && <span style={{ color: "#667085", fontSize: 11, marginLeft: 6, textTransform: "uppercase" }}>{format}</span>}
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <button
            type="button"
            onClick={() => setPageNum(Math.max(1, pageNum - 1))}
            disabled={pageNum <= 1}
            style={smallNavButtonStyle(pageNum <= 1)}
            title={`Previous ${label}`}
          >
            Prev
          </button>
          <span style={{ color: "#667085", fontSize: 12, minWidth: 46, textAlign: "center" }}>
            {pageNum}/{totalPages || 1}
          </span>
          <button
            type="button"
            onClick={() => setPageNum(Math.min(totalPages || 1, pageNum + 1))}
            disabled={pageNum >= (totalPages || 1)}
            style={smallNavButtonStyle(pageNum >= (totalPages || 1))}
            title={`Next ${label}`}
          >
            Next
          </button>
        </div>
      </div>

      <div className={`doc-frame dl-scrollbar ${useNativeViewer ? "native" : ""}`}>
        {!pageExists ? (
          <EmptyPage pageNum={pageNum} />
        ) : useNativeViewer ? (
          <NativePageView page={nativePage} side={side} />
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

            {(overlay.regions || []).map((r, i) => {
              const [x0, y0, x1, y1] = r.bbox || [0, 0, 0, 0];
              const c = COLORS[r.change_type] || COLORS.MODIFIED;
              const pageWidth = r.page_width || overlay.page_width || 612;
              const pageHeight = r.page_height || overlay.page_height || 792;

              return (
                <div
                  key={i}
                  title={`${r.change_type || "change"} ${r.stable_key || ""} (${r.block_type || "block"})`}
                  style={{
                    position: "absolute",
                    left: `${(x0 / pageWidth) * 100}%`,
                    top: `${(y0 / pageHeight) * 100}%`,
                    width: `${Math.max(0.15, ((x1 - x0) / pageWidth) * 100)}%`,
                    height: `${Math.max(0.15, ((y1 - y0) / pageHeight) * 100)}%`,
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
    <div style={{ minHeight: 520, display: "grid", placeItems: "center", color: "#667085", fontWeight: 600 }}>
      No page {pageNum} in this document.
    </div>
  );
}

function NativePageView({ page, side }) {
  if (!page) {
    return (
      <div style={{ minHeight: 520, display: "grid", placeItems: "center", color: "#667085", fontWeight: 600 }}>
        Loading structured page
      </div>
    );
  }

  const items = page.items || [];
  const viewerType = page.viewer_type || (page.format === "spreadsheet" ? "spreadsheet" : "document");

  if (!items.length) {
    return (
      <div style={{ minHeight: 520, display: "grid", placeItems: "center", color: "#667085", fontWeight: 600 }}>
        No structured content on this page.
      </div>
    );
  }

  return (
    <div className={`native-page ${viewerType}`} dir="auto">
      {items.map((item) => (
        <NativeItem key={item.id} item={item} viewerType={viewerType} side={side || page.side} />
      ))}
    </div>
  );
}

function NativeItem({ item, viewerType, side }) {
  const highlight = nativeHighlightStyle(item.highlight);

  if (item.type === "table" && !item.payload?.layout_table) {
    return <NativeTable item={item} viewerType={viewerType} />;
  }

  const isHeading = item.type === "section" || item.type === "heading";

  return (
    <div
      className="native-block"
      dir="auto"
      style={{
        ...highlight,
        marginBottom: isHeading ? 10 : 8,
        padding: isHeading ? "7px 9px" : "6px 8px",
        borderRadius: 6,
        fontSize: isHeading ? 14 : 13,
        fontWeight: isHeading ? 650 : 400,
        lineHeight: 1.45,
      }}
      title={item.change_type}
    >
      <NativeTokenText item={item} side={side} />
    </div>
  );
}

function NativeTokenText({ item, side }) {
  const tokens = item.token_diff || [];
  const hasTokenDiff = item.highlight === "modified" && Array.isArray(tokens) && tokens.some((t) => t.op && t.op !== "equal");

  if (!hasTokenDiff) {
    return item.text || item.payload?.text || item.payload?.layout_text || item.path || "-";
  }

  return (
    <span>
      {tokens.map((token, idx) => {
        const op = token.op;
        if (op === "delete" && side !== "base") return null;
        if (op === "insert" && side === "base") return null;
        const text =
          op === "equal"
            ? token.text_a
            : side === "base"
              ? token.text_a
              : token.text_b;

        if (!text) return null;

        let cls = "";
        if (op === "delete") cls = "native-token-delete";
        if (op === "insert") cls = "native-token-insert";
        if (op === "replace") cls = side === "base" ? "native-token-replace-base" : "native-token-replace-target";

        return (
          <React.Fragment key={idx}>
            {idx > 0 ? " " : ""}
            <span className={`native-token ${cls}`}>{text}</span>
          </React.Fragment>
        );
      })}
    </span>
  );
}

function NativeTable({ item, viewerType }) {
  const header = item.header || [];
  const rows = item.rows || [];
  const title = item.payload?.table_title || item.text || "Table";
  const isSpreadsheet = viewerType === "spreadsheet";

  return (
    <div className="native-block" dir="auto" style={{ ...nativeHighlightStyle(item.highlight), marginBottom: 14, padding: 10, borderRadius: 7 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "baseline", flexWrap: "wrap", marginBottom: 7 }}>
        <div style={{ fontSize: 14, fontWeight: 650, color: "#344054" }}>{title}</div>
        <div style={{ fontSize: 11, color: "#667085" }}>{rows.length} row{rows.length === 1 ? "" : "s"}</div>
      </div>
      <div className="native-table-wrap dl-scrollbar">
        <table className={`native-table ${isSpreadsheet ? "spreadsheet" : ""}`} style={{ fontSize: isSpreadsheet ? 12 : 12 }}>
          <thead>
            <tr style={{ background: "#f2eee6", color: "#344054" }}>
              {header.map((col) => (
                <th key={col} dir="auto" style={smallTh}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const rowStyle = nativeHighlightStyle(row.highlight, true);
              return (
                <tr key={row.id} title={row.change_type} style={{ background: rowStyle.background }}>
                  {header.map((col) => (
                    <td key={col} dir="auto" style={{ ...smallTd, borderLeft: rowStyle.borderLeft }}>
                      {displayCell(row.values?.[col])}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function nativeHighlightStyle(kind, compact = false) {
  if (kind === "added") {
    return {
      background: compact ? COLORS.ADDED.bg : "rgba(31,160,70,.08)",
      border: compact ? undefined : `1px solid ${COLORS.ADDED.border}`,
      borderLeft: `3px solid ${COLORS.ADDED.border}`,
    };
  }
  if (kind === "deleted") {
    return {
      background: compact ? COLORS.DELETED.bg : "rgba(218,54,54,.08)",
      border: compact ? undefined : `1px solid ${COLORS.DELETED.border}`,
      borderLeft: `3px solid ${COLORS.DELETED.border}`,
    };
  }
  if (kind === "modified") {
    return {
      background: compact ? "rgba(218,185,42,.10)" : "rgba(218,185,42,.08)",
      border: compact ? undefined : `1px solid ${COLORS.MODIFIED.border}`,
      borderLeft: `3px solid ${COLORS.MODIFIED.border}`,
    };
  }
  return {
    background: compact ? "transparent" : "#fffdf8",
    border: compact ? undefined : "1px solid transparent",
    borderLeft: "3px solid transparent",
  };
}

function ReviewReport({ runId }) {
  const [rows, setRows] = useState(null);
  const [filter, setFilter] = useState("ALL");
  const [error, setError] = useState("");

  useEffect(() => {
    setRows(null);
    setError("");

    fetch(`${API}/runs/${runId}/summary`)
      .then(async (r) => {
        if (!r.ok) throw new Error(await readResponseError(r));
        return r.json();
      })
      .then((data) => setRows(Array.isArray(data) ? data : data.rows || data.summary || []))
      .catch((err) => setError(friendlyFetchError(err)));
  }, [runId]);

  const filteredRows = useMemo(() => {
    const list = rows || [];
    if (filter === "ALL") return list;
    if (filter === "REVIEW") return list.filter((r) => needsReview(r));
    return list.filter((r) => rowChangeType(r) === filter);
  }, [rows, filter]);

  const avgConfidence = average((rows || []).map((r) => normalizeConfidence(r.confidence)).filter((v) => typeof v === "number"));
  const reviewCount = (rows || []).filter(needsReview).length;
  const filterCounts = useMemo(() => {
    const list = rows || [];
    return {
      ALL: list.length,
      ADDED: list.filter((r) => rowChangeType(r) === "ADDED").length,
      DELETED: list.filter((r) => rowChangeType(r) === "DELETED").length,
      MODIFIED: list.filter((r) => rowChangeType(r) === "MODIFIED").length,
      REVIEW: list.filter(needsReview).length,
    };
  }, [rows]);
  const keyInsights = useMemo(() => {
    const list = (rows || []).filter((row) => row.change || row.description || row.before || row.after);
    const priority = [...list].sort((a, b) => {
      const ai = impactRank(a.impact) + (needsReview(a) ? 2 : 0) + (normalizeConfidence(a.confidence) || 0);
      const bi = impactRank(b.impact) + (needsReview(b) ? 2 : 0) + (normalizeConfidence(b.confidence) || 0);
      return bi - ai;
    });
    return priority.slice(0, 6);
  }, [rows]);

  if (error) return <ErrorBox message={error} />;
  if (!rows) return <SoftLoading label="Building review report" />;

  return (
    <div>
      <div className="report-metrics" style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 10, marginBottom: 14 }}>
        <MetricCard label="Review items" value={rows.length} />
        <MetricCard label="Needs review" value={reviewCount} />
        <MetricCard label="Avg confidence" value={avgConfidence == null ? "-" : `${Math.round(avgConfidence * 100)}%`} />
        <MetricCard label="Report" value="PDF ready" />
      </div>

      {keyInsights.length > 0 && (
        <div style={{ background: "#fbfaf6", border: "1px solid #ded6c8", borderRadius: 8, padding: 12, marginBottom: 12 }}>
          <div style={{ fontWeight: 650, color: "#344054", marginBottom: 8 }}>Important changes</div>
          <div style={{ display: "grid", gap: 8 }}>
            {keyInsights.map((row, i) => (
              <div key={i} style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: 8, alignItems: "start" }}>
                <ChangeBadge type={rowChangeType(row)} />
                <div>
                  <span style={{ fontWeight: 650 }}>{trim(row.feature || row.item || row.area || "Document item", 120)}: </span>
                  <span>{trim(row.change || row.description || row.before || row.after || "Change detected.", 260)}</span>
                  {row.citation && <span style={{ color: "#667085" }}> ({friendlyCitation(row.citation)})</span>}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
        {["ALL", "ADDED", "DELETED", "MODIFIED", "REVIEW"].map((key) => (
          <button key={key} onClick={() => setFilter(key)} style={filterButtonStyle(filter === key)}>
            {filterLabel(key)} {filterCounts[key] ?? 0}
          </button>
        ))}
      </div>

      {filteredRows.length === 0 ? (
        <EmptyState label="No review items match this filter." />
      ) : (
        <div className="dl-scrollbar" style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13, minWidth: 980 }}>
            <thead>
              <tr style={{ background: "#1f2937", color: "white" }}>
                <th style={th}>Area / Item</th>
                <th style={th}>Change</th>
                <th style={th}>Evidence</th>
                <th style={th}>Confidence</th>
                <th style={th}>Review</th>
              </tr>
            </thead>
            <tbody>
              {filteredRows.map((row, i) => (
                <tr key={i}>
                  <td style={{ ...td, width: "24%" }}>
                    <strong style={{ fontWeight: 650 }}>{row.feature || row.area || row.item || row.path || "Document change"}</strong>
                    <div style={{ color: "#667085", marginTop: 6 }}>{trim(row.before || row.after || row.text || "", 180)}</div>
                  </td>
                  <td style={{ ...td, width: "22%" }}>
                    <ChangeBadge type={rowChangeType(row)} />
                    <div style={{ marginTop: 7 }}>{trim(row.change || row.description || "", 240)}</div>
                  </td>
                  <td style={{ ...td, width: "28%" }}>
                    <div>{friendlyCitation(row.citation || row.evidence || "-")}</div>
                    {row.before && <div style={{ color: COLORS.DELETED.text, marginTop: 7 }}>Before: {trim(row.before, 180)}</div>}
                    {row.after && <div style={{ color: COLORS.ADDED.text, marginTop: 4 }}>After: {trim(row.after, 180)}</div>}
                  </td>
                  <td style={{ ...td, width: "11%" }}>
                    <Confidence value={normalizeConfidence(row.confidence)} />
                    {row.impact && <div style={{ color: "#667085", marginTop: 4 }}>{row.impact}</div>}
                  </td>
                  <td style={{ ...td, width: "15%", color: needsReview(row) ? COLORS.DELETED.text : "#475467", fontWeight: needsReview(row) ? 650 : 400 }}>
                    {row.seek_clarification || row.review || row.recommendation || (needsReview(row) ? "Review recommended." : "No action suggested.")}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

const DEFAULT_AI_SUMMARY_PROMPT = "Summarize key changes as a table with columns Feature, Change, Seek Clarification. Use only the extracted comparison evidence.";

const AI_PROMPT_PRESETS = [
  {
    label: "Key changes table",
    prompt: DEFAULT_AI_SUMMARY_PROMPT,
  },
  {
    label: "Executive summary",
    prompt: "Write a concise executive summary of the most important document changes. Group related changes and include evidence references where useful.",
  },
  {
    label: "High-risk changes",
    prompt: "Identify high-risk changes such as dates, prices, obligations, requirements, removals, availability, or table cell changes. Explain why each item may need review.",
  },
  {
    label: "Clarification list",
    prompt: "List the changes that should be checked with the relevant owner or team. For each item, explain the exact clarification question to ask.",
  },
];

const FAST_QUERY_PRESETS = [
  {
    label: "Evidence summary",
    prompt: "Summarize the key changes with citations",
  },
  {
    label: "Removed content",
    prompt: "List content that was deleted or removed with page evidence",
  },
  {
    label: "Table changes",
    prompt: "Show table row and cell changes",
  },
  {
    label: "Numbers and dates",
    prompt: "Show changes involving numbers, dates, prices, percentages, or codes",
  },
];

function QueryPanel({ runId }) {
  const [q, setQ] = useState(DEFAULT_AI_SUMMARY_PROMPT);
  const [mode, setMode] = useState("ai");
  const [response, setResponse] = useState(null);
  const [busy, setBusy] = useState(false);
  const [downloadBusy, setDownloadBusy] = useState(false);

  const ask = async () => {
    const effectiveQuestion = q.trim() || (mode === "ai" ? DEFAULT_AI_SUMMARY_PROMPT : "");
    if (!effectiveQuestion) return;

    setBusy(true);
    setResponse(null);

    try {
      const r = await fetch(`${API}/runs/${runId}/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: effectiveQuestion, mode, response_language: "source" }),
      });

      if (!r.ok) throw new Error(await readResponseError(r));

      const data = await r.json();
      setResponse(data);
    } catch (err) {
      setResponse({ answer: friendlyFetchError(err), rows: [] });
    } finally {
      setBusy(false);
    }
  };

  const rows = response?.rows || [];
  const columns = response?.columns || inferColumns(rows);
  const responseConfidence = normalizeConfidence(response?.confidence);
  const canDownloadAiSummary = response?.mode === "ai" && (Boolean(response?.answer) || rows.length > 0);

  const selectMode = (nextMode) => {
    setMode(nextMode);
    setResponse(null);
    if (nextMode === "ai" && !q.trim()) {
      setQ(DEFAULT_AI_SUMMARY_PROMPT);
    }
  };

  const downloadAiSummary = async () => {
    if (!canDownloadAiSummary || downloadBusy) return;
    setDownloadBusy(true);

    try {
      const r = await fetch(`${API}/runs/${runId}/ai-summary.pdf`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: "AI Summary",
          answer: response?.answer || "",
          columns,
          rows,
          confidence: response?.confidence ?? null,
        }),
      });

      if (!r.ok) throw new Error(await readResponseError(r));

      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `ai_summary_${runId}.pdf`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      const message = friendlyFetchError(err);
      setResponse((prev) => ({
        ...(prev || {}),
        ai_error:
          message === "Not Found"
            ? "AI summary PDF export is not available on the current backend revision. Redeploy the backend with the latest api.py, then try again."
            : message,
      }));
    } finally {
      setDownloadBusy(false);
    }
  };

  return (
    <div>
      <div style={{ background: "#fbfaf6", border: "1px solid #ded6c8", borderRadius: 8, padding: 12, marginBottom: 12 }}>
        <div style={{ fontWeight: 650, marginBottom: 6 }}>Ask about the comparison</div>
        <div style={{ color: "#667085", fontSize: 13, marginBottom: 10 }}>
          Use fast query for exact evidence lookup, or AI Summarization for a business-ready answer from extracted and ranked comparison evidence.
        </div>

        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 10 }}>
          <button
            type="button"
            onClick={() => selectMode("fast")}
            disabled={busy}
            style={modeButtonStyle(mode === "fast", busy)}
          >
            Natural language query
          </button>
          <button
            type="button"
            onClick={() => selectMode("ai")}
            disabled={busy}
            style={modeButtonStyle(mode === "ai", busy)}
          >
            AI Summarization
          </button>
        </div>

        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 10 }}>
          {(mode === "ai" ? AI_PROMPT_PRESETS : FAST_QUERY_PRESETS).map((preset) => (
            <button
              key={preset.label}
              type="button"
              onClick={() => setQ(preset.prompt)}
              disabled={busy}
              style={presetButtonStyle(busy)}
              title={preset.prompt}
            >
              {preset.label}
            </button>
          ))}
        </div>

        {mode === "ai" && (
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 10 }}>
            <span style={{ color: "#667085", fontSize: 12 }}>
              Tip: for PCV/code review, ask for the exact baseline and revised values, for example: compare PCV 133456 with PCV 225376.
            </span>
          </div>
        )}

        <div style={{ display: "flex", gap: 8 }}>
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && ask()}
            placeholder={
              mode === "ai"
                ? DEFAULT_AI_SUMMARY_PROMPT
                : "Example: Find changes for PCV 205 or summarize key changes"
            }
            style={{ ...inputStyle, flex: 1 }}
          />
          <button onClick={ask} disabled={busy} style={primaryButtonStyle(busy)}>
            {busy ? (mode === "ai" ? "Summarizing" : "Searching") : "Ask"}
          </button>
        </div>
      </div>

      {response?.answer && (
        <div dir="auto" style={{ background: "#fffdf8", border: "1px solid #d8d0c3", borderLeft: "4px solid #2f5f4f", borderRadius: 8, padding: 12, marginBottom: 12, color: "#344054", lineHeight: 1.45 }}>
          {response.mode && (
            <div style={{ display: "flex", gap: 10, alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", marginBottom: 6 }}>
              <div style={{ color: "#667085", fontSize: 12, fontWeight: 650 }}>
                {response.mode === "ai" ? "AI Summarization" : "Natural language query"}
                {response.mode === "ai" && response.ai_called === true ? " - Successful" : ""}
                {response.mode === "ai" && response.ai_unavailable ? " - Unavailable" : ""}
                {response.mode === "ai" && typeof responseConfidence === "number" ? ` | Confidence ${Math.round(responseConfidence * 100)}%` : ""}
              </div>
              {canDownloadAiSummary && (
                <button
                  type="button"
                  onClick={downloadAiSummary}
                  disabled={downloadBusy}
                  style={secondaryButtonStyle(downloadBusy ? { opacity: 0.65, cursor: "default" } : {})}
                >
                  {downloadBusy ? "Preparing PDF" : "Download AI summary"}
                </button>
              )}
            </div>
          )}
          {response.answer}
          {response.ai_error && (
            <div style={{ marginTop: 8, color: COLORS.DELETED.text, fontSize: 12, fontWeight: 600 }}>
              {response.ai_error}
            </div>
          )}
        </div>
      )}

      {response && rows.length === 0 && <EmptyState label="No supporting results were found." />}

      {rows.length > 0 && columns.length > 0 ? (
        <GenericRowsTable columns={columns} rows={rows} />
      ) : (
        rows.slice(0, 50).map((r, i) => <QueryResult key={i} r={r} />)
      )}
    </div>
  );
}

function QueryResult({ r }) {
  const c = COLORS[r.change_type] || COLORS.MODIFIED;

  return (
    <div style={{ borderLeft: `4px solid ${c.border}`, background: "#fffdf8", padding: "10px 12px", marginBottom: 8, fontSize: 13, borderRadius: 7, boxShadow: "0 1px 1px rgba(20,20,20,.04)" }}>
      <div style={{ fontWeight: 650, marginBottom: 5 }}>
        <ChangeBadge type={rowChangeType(r)} />
        {r.stable_key && <code style={{ marginLeft: 6 }}>{r.stable_key}</code>}
        <span style={{ color: "#667085", marginLeft: 8 }}>{r.citation || `page ${r.page || "-"} - ${r.block_type || "block"}`}</span>
      </div>
      {r.before && <div style={{ color: COLORS.DELETED.text }}>Before: {trim(r.before, 260)}</div>}
      {r.after && <div style={{ color: COLORS.ADDED.text }}>After: {trim(r.after, 260)}</div>}
      {r.field_changes?.length > 0 && <FieldDiffTable rows={r.field_changes} />}
    </div>
  );
}

function TablesWorkspace({ runId }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const [baseTableId, setBaseTableId] = useState("");
  const [targetTableId, setTargetTableId] = useState("");
  const [baseRowColumns, setBaseRowColumns] = useState([]);
  const [targetRowColumns, setTargetRowColumns] = useState([]);
  const [baseValueColumns, setBaseValueColumns] = useState([]);
  const [targetValueColumns, setTargetValueColumns] = useState([]);
  const [rowFilter, setRowFilter] = useState("");
  const [baseView, setBaseView] = useState(null);
  const [targetView, setTargetView] = useState(null);
  const [viewBusy, setViewBusy] = useState(false);
  const [diff, setDiff] = useState(null);
  const [compareBusy, setCompareBusy] = useState(false);
  const [exportBusy, setExportBusy] = useState(false);
  const [useTableAi, setUseTableAi] = useState(false);
  const [tableQuestion, setTableQuestion] = useState("Summarize selected table changes, including value changes and header-only changes, with clarification questions.");

  useEffect(() => {
    setData(null);
    setError("");

    fetch(`${API}/runs/${runId}/tables?include_rows=true`)
      .then(async (r) => {
        if (!r.ok) throw new Error(await readResponseError(r));
        return r.json();
      })
      .then((payload) => setData(payload))
      .catch((err) => {
        setError(friendlyFetchError(err));
        setData({ base: [], target: [] });
      });
  }, [runId]);

  const baseTables = data?.base || [];
  const targetTables = data?.target || [];
  const baseTable = baseTables.find((t) => t.id === baseTableId);
  const targetTable = targetTables.find((t) => t.id === targetTableId);

  useEffect(() => {
    setDiff(null);
    setBaseView(null);
    if (!baseTable) {
      setBaseRowColumns([]);
      setBaseValueColumns([]);
      return;
    }
    setBaseRowColumns(defaultRowColumns(baseTable));
    setBaseValueColumns(defaultValueColumns(baseTable));
  }, [baseTableId]);

  useEffect(() => {
    setDiff(null);
    setTargetView(null);
    if (!targetTable) {
      setTargetRowColumns([]);
      setTargetValueColumns([]);
      return;
    }
    setTargetRowColumns(defaultRowColumns(targetTable));
    setTargetValueColumns(defaultValueColumns(targetTable));
  }, [targetTableId]);

  useEffect(() => {
    let cancelled = false;
    const timer = setTimeout(async () => {
      if (!baseTable && !targetTable) return;

      setViewBusy(true);
      setError("");

      try {
        const [basePayload, targetPayload] = await Promise.all([
          baseTable
            ? fetchTableView(runId, "base", baseTable.id, unique([...baseRowColumns, ...baseValueColumns.filter((c) => !baseRowColumns.includes(c))]), rowFilter)
            : Promise.resolve(null),
          targetTable
            ? fetchTableView(runId, "target", targetTable.id, unique([...targetRowColumns, ...targetValueColumns.filter((c) => !targetRowColumns.includes(c))]), rowFilter)
            : Promise.resolve(null),
        ]);

        if (cancelled) return;

        setBaseView(basePayload);
        setTargetView(targetPayload);
      } catch (err) {
        if (!cancelled) setError(friendlyFetchError(err));
      } finally {
        if (!cancelled) setViewBusy(false);
      }
    }, 250);

    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [
    runId,
    baseTableId,
    targetTableId,
    baseRowColumns.join("|"),
    targetRowColumns.join("|"),
    baseValueColumns.join("|"),
    targetValueColumns.join("|"),
    rowFilter,
  ]);

  const tableComparePayload = (overrides = {}) => ({
    base_table_id: baseTableId,
    target_table_id: targetTableId,
    base_row_columns: baseRowColumns,
    target_row_columns: targetRowColumns,
    base_value_columns: baseValueColumns.filter((c) => !baseRowColumns.includes(c)),
    target_value_columns: targetValueColumns.filter((c) => !targetRowColumns.includes(c)),
    row_filter: rowFilter.trim() || null,
    use_ai: overrides.use_ai ?? useTableAi,
    question: tableQuestion.trim() || null,
    limit: 200,
  });

  const compare = async (overrides = {}) => {
    if (!baseTableId || !targetTableId) return;

    setCompareBusy(true);
    setDiff(null);
    setError("");

    try {
      const r = await fetch(`${API}/runs/${runId}/compare-table-columns`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(tableComparePayload(overrides)),
      });

      if (!r.ok) throw new Error(await readResponseError(r));
      setDiff(await r.json());
    } catch (err) {
      setError(friendlyFetchError(err));
    } finally {
      setCompareBusy(false);
    }
  };

  const exportTablePdf = async () => {
    if (!baseTableId || !targetTableId || exportBusy) return;

    setExportBusy(true);
    setError("");

    try {
      const r = await fetch(`${API}/runs/${runId}/table-report.pdf`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...tableComparePayload(), use_ai: false }),
      });

      if (!r.ok) throw new Error(await readResponseError(r));

      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `table_comparison_${runId}.pdf`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      setError(friendlyFetchError(err));
    } finally {
      setExportBusy(false);
    }
  };

  if (!data) return <SoftLoading label="Loading detected tables" />;

  return (
    <div>
      <div style={{ background: "#fbfaf6", border: "1px solid #ded6c8", borderRadius: 8, padding: 12, marginBottom: 14 }}>
        <div style={{ fontWeight: 650, marginBottom: 4 }}>Table workspace</div>
        <div style={{ color: "#667085", fontSize: 13, lineHeight: 1.45 }}>
          Select tables by page and topic, choose the row/feature columns and value columns, preview the selected rows, then compare only those table slices.
        </div>
      </div>

      {error && <ErrorBox message={error} />}

      <div className="table-picker-grid" style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr)", gap: 14, marginBottom: 14 }}>
        <TablePicker title="Baseline document table" value={baseTableId} onChange={setBaseTableId} tables={baseTables} />
        <TablePicker title="Revised document table" value={targetTableId} onChange={setTargetTableId} tables={targetTables} />
      </div>

      <div className="table-picker-grid" style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr)", gap: 14, marginBottom: 14 }}>
        <TableInfo table={baseTable} emptyLabel="Select a baseline table to inspect its title, page, columns, and rows." />
        <TableInfo table={targetTable} emptyLabel="Select a revised table to inspect its title, page, columns, and rows." />
      </div>

      <RowFilterSuggestions baseTable={baseTable} targetTable={targetTable} onSelect={setRowFilter} />

      <div className="table-config-grid" style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr)", gap: 14, marginBottom: 14 }}>
        <ColumnConfig
          title="Baseline columns"
          table={baseTable}
          rowColumns={baseRowColumns}
          setRowColumns={(cols) => {
            const clean = unique(cols);
            setBaseRowColumns(clean);
            setBaseValueColumns((prev) => prev.filter((c) => !clean.includes(c)));
          }}
          valueColumns={baseValueColumns}
          setValueColumns={(cols) => {
            const clean = unique(cols).filter((c) => !baseRowColumns.includes(c));
            setBaseValueColumns(clean);
          }}
        />
        <ColumnConfig
          title="Revised columns"
          table={targetTable}
          rowColumns={targetRowColumns}
          setRowColumns={(cols) => {
            const clean = unique(cols);
            setTargetRowColumns(clean);
            setTargetValueColumns((prev) => prev.filter((c) => !clean.includes(c)));
          }}
          valueColumns={targetValueColumns}
          setValueColumns={(cols) => {
            const clean = unique(cols).filter((c) => !targetRowColumns.includes(c));
            setTargetValueColumns(clean);
          }}
        />
      </div>

      <div style={{ background: "#fbfaf6", border: "1px solid #ded6c8", borderRadius: 8, padding: 12, marginBottom: 14 }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center", flexWrap: "wrap", marginBottom: 8 }}>
          <label style={{ display: "flex", alignItems: "center", gap: 8, fontWeight: 650, color: "#344054" }}>
            <input
              type="checkbox"
              checked={useTableAi}
              onChange={(e) => setUseTableAi(e.target.checked)}
            />
            Include AI insight for this selected table slice
          </label>
          <button
            type="button"
            onClick={() => {
              setUseTableAi(true);
              compare({ use_ai: true });
            }}
            disabled={compareBusy || !baseTableId || !targetTableId}
            style={secondaryButtonStyle(compareBusy || !baseTableId || !targetTableId ? { height: 36, opacity: 0.65, cursor: "default" } : { height: 36 })}
          >
            {compareBusy ? "Running" : "Run table AI insight"}
          </button>
        </div>
        <input
          value={tableQuestion}
          onChange={(e) => setTableQuestion(e.target.value)}
          disabled={!useTableAi}
          placeholder="Example: summarize changed values, renamed headers, missing rows, and review questions"
          style={{
            ...inputStyle,
            opacity: useTableAi ? 1 : 0.65,
            background: useTableAi ? "#fffdf8" : "#f4efe6",
          }}
        />
        <div style={{ color: "#667085", fontSize: 12, marginTop: 6 }}>
          AI is optional and receives only the selected table metadata, selected columns, aligned rows, and detected cell/header changes.
        </div>
      </div>

      <div className="table-action-grid" style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) auto auto", gap: 10, alignItems: "end", marginBottom: 14 }}>
        <div>
          <label style={labelStyle}>Find rows in selected tables</label>
          <input
            value={rowFilter}
            onChange={(e) => setRowFilter(e.target.value)}
            placeholder="Optional: type a feature, code, package, PCV, or phrase to narrow the rows"
            style={inputStyle}
          />
          <div style={{ color: "#667085", fontSize: 12, marginTop: 5 }}>
            Leave blank to compare all rows in the selected table slice.
          </div>
        </div>
        <button
          onClick={() => compare()}
          disabled={compareBusy || !baseTableId || !targetTableId}
          style={primaryButtonStyle(compareBusy || !baseTableId || !targetTableId, { height: 40 })}
        >
          {compareBusy ? "Comparing" : "Apply & compare"}
        </button>
        <button
          onClick={exportTablePdf}
          disabled={exportBusy || !baseTableId || !targetTableId}
          style={secondaryButtonStyle(exportBusy || !baseTableId || !targetTableId ? { height: 40, opacity: 0.65, cursor: "default" } : { height: 40 })}
        >
          {exportBusy ? "Exporting" : "Export PDF"}
        </button>
      </div>

      {viewBusy && <SoftLoading label="Rendering selected table values" />}

      <div className="table-selected-stack">
        <SelectedTableView title="Baseline selected view" view={baseView} />
        <SelectedTableView title="Revised selected view" view={targetView} />
      </div>

      {diff && <TableColumnCompareResult diff={diff} />}
    </div>
  );
}

async function fetchTableView(runId, side, tableId, columns, rowFilter) {
  const r = await fetch(`${API}/runs/${runId}/table-view`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      side,
      table_id: tableId,
      columns,
      row_filter: rowFilter.trim() || null,
      limit: 300,
    }),
  });

  if (!r.ok) throw new Error(await readResponseError(r));
  return r.json();
}

function TablePicker({ title, value, onChange, tables }) {
  return (
    <div>
      <label style={labelStyle}>{title}</label>
      <select value={value} onChange={(e) => onChange(e.target.value)} style={inputStyle}>
        <option value="">Select a detected table</option>
        {tables.map((t) => (
          <option key={t.id} value={t.id}>
            {t.display_name || `Page ${t.page_first || "-"} - ${t.title || t.header_preview || "Detected table"}`}
          </option>
        ))}
      </select>
    </div>
  );
}

function TableInfo({ table, emptyLabel }) {
  if (!table) return <EmptyState label={emptyLabel} />;

  const columns = table.columns || [];

  return (
    <div className="table-preview-shell" style={{ background: "#fffdf8", border: "1px solid #ded6c8", borderRadius: 8, padding: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
        <div style={{ minWidth: 0 }}>
          <div className="cell-truncate" title={table.title || table.area || "Detected table"} style={{ fontWeight: 650 }}>
            {table.title || table.area || "Detected table"}
          </div>
          <div style={{ marginTop: 4, color: "#667085", fontSize: 13 }}>
            {table.page_label || `Page ${table.page_first || "-"}`} · {table.n_columns || columns.length} columns · {table.n_rows || 0} rows
            {table.extraction_confidence ? ` · extraction ${Math.round(table.extraction_confidence * 100)}%` : ""}
          </div>
        </div>
        {table.id && <code>{table.id.slice(0, 8)}</code>}
      </div>

      {table.context && (
        <div style={{ marginTop: 10, color: "#667085", fontSize: 13, lineHeight: 1.4 }}>
          {table.context}
        </div>
      )}

      <div className="cell-wrap" style={{ marginTop: 10, color: "#475467", fontSize: 13 }}>
        <strong style={{ fontWeight: 650 }}>Columns:</strong> {columns.slice(0, 14).join(" | ") || "No columns detected"}
      </div>

      {Array.isArray(table.column_details) && table.column_details.length > 0 && (
        <div style={{ marginTop: 10, display: "flex", gap: 6, flexWrap: "wrap" }}>
          {table.column_details.slice(0, 14).map((col) => (
            <span key={col.name} title={`${col.name}${col.sample_values?.[0] ? `: ${col.sample_values[0]}` : ""}`} style={{ border: "1px solid #d8d0c3", borderRadius: 999, padding: "2px 7px", fontSize: 12, color: "#475467", background: "#fbfaf6", maxWidth: 260, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {col.name}
              {col.semantic_role ? ` · ${col.semantic_role}` : ""}
              {col.sample_values?.[0] ? `: ${trim(col.sample_values[0], 24)}` : ""}
            </span>
          ))}
        </div>
      )}

      {Array.isArray(table.quality_warnings) && table.quality_warnings.length > 0 && (
        <div style={{ marginTop: 10, color: "#8a5a00", fontSize: 13, lineHeight: 1.35 }}>
          {table.quality_warnings.slice(0, 2).join(" ")}
        </div>
      )}

      {Array.isArray(table.row_preview) && table.row_preview.length > 0 && (
        <TablePreview columns={columns.slice(0, 6)} rows={table.row_preview.slice(0, 5)} />
      )}
    </div>
  );
}

function RowFilterSuggestions({ baseTable, targetTable, onSelect }) {
  const suggestions = unique([
    ...((baseTable?.row_keys || []).filter(Boolean)),
    ...((targetTable?.row_keys || []).filter(Boolean)),
  ]).slice(0, 18);

  if (!suggestions.length) return null;

  return (
    <div style={{ background: "#fbfaf6", border: "1px solid #ded6c8", borderRadius: 8, padding: 10, marginBottom: 14 }}>
      <div style={{ color: "#344054", fontSize: 13, fontWeight: 650, marginBottom: 7 }}>Quick row filters</div>
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        {suggestions.map((key) => (
          <button
            key={key}
            type="button"
            onClick={() => onSelect(key)}
            title={key}
            style={{
              border: "1px solid #d8d0c3",
              background: "#fffdf8",
              color: "#344054",
              borderRadius: 999,
              padding: "4px 8px",
              fontSize: 12,
              cursor: "pointer",
              maxWidth: 240,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {trim(key, 38)}
          </button>
        ))}
      </div>
    </div>
  );
}

function ColumnConfig({ title, table, rowColumns, setRowColumns, valueColumns, setValueColumns }) {
  if (!table) return <EmptyState label={`${title}: select a table first.`} />;

  const columns = table.columns || [];

  return (
    <div className="table-preview-shell" style={{ background: "#fffdf8", border: "1px solid #ded6c8", borderRadius: 8, padding: 12 }}>
      <div style={{ fontWeight: 650, marginBottom: 10 }}>{title}</div>

      <MultiSelect
        label="Row / feature columns"
        helper="Used to identify and align rows, such as Feature, Item, Order Code, Package, or PCV."
        options={columns}
        selected={rowColumns}
        onChange={setRowColumns}
      />

      <div style={{ height: 12 }} />

      <MultiSelect
        label="Value columns"
        helper="Values to render and compare. Row/feature columns are excluded here to avoid duplicate output."
        options={columns.filter((c) => !rowColumns.includes(c))}
        selected={valueColumns}
        onChange={setValueColumns}
      />
    </div>
  );
}

function MultiSelect({ label, helper, options, selected, onChange }) {
  const toggle = (option) => {
    if (selected.includes(option)) onChange(selected.filter((x) => x !== option));
    else onChange([...selected, option]);
  };

  const selectAll = () => onChange(options);
  const clear = () => onChange([]);

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "baseline", marginBottom: 6 }}>
        <div>
          <div style={{ fontSize: 13, color: "#344054", fontWeight: 650 }}>{label}</div>
          <div style={{ fontSize: 12, color: "#667085" }}>{helper}</div>
        </div>
        <div style={{ display: "flex", gap: 6 }}>
          <button type="button" onClick={selectAll} style={miniButtonStyle}>All</button>
          <button type="button" onClick={clear} style={miniButtonStyle}>Clear</button>
        </div>
      </div>

      <div className="dl-scrollbar" style={{ maxHeight: 150, overflow: "auto", border: "1px solid #e0d8ca", borderRadius: 8, padding: 8, background: "#fbfaf6", minWidth: 0 }}>
        {options.length === 0 ? (
          <div style={{ color: "#667085", fontSize: 13 }}>No columns available.</div>
        ) : (
          options.map((option) => (
            <label key={option} title={option} style={{ display: "flex", gap: 8, alignItems: "center", padding: "5px 4px", fontSize: 13, color: "#344054", minWidth: 0 }}>
              <input type="checkbox" checked={selected.includes(option)} onChange={() => toggle(option)} />
              <span className="cell-truncate">{option}</span>
            </label>
          ))
        )}
      </div>
    </div>
  );
}

function TablePreview({ columns, rows }) {
  if (!columns.length || !rows.length) return null;

  const minWidth = tableMinWidth(columns.length, 420, 920);

  return (
    <div className="dl-scrollbar table-scroll-frame" style={{ marginTop: 12 }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, minWidth }}>
        <thead>
          <tr style={{ background: "#f2eee6" }}>
            {columns.map((col) => <th key={col} title={col} style={smallTh}>{col}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {columns.map((col) => <td key={col} style={smallTd}>{displayCell(row.values?.[col])}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SelectedTableView({ title, view }) {
  if (!view) return <EmptyState label={`${title}: select a table and columns to render values.`} />;

  return (
    <div className="table-preview-shell" style={{ background: "#fffdf8", border: "1px solid #ded6c8", borderRadius: 8, padding: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 10, flexWrap: "wrap", marginBottom: 10 }}>
        <div style={{ minWidth: 0 }}>
          <div style={{ fontWeight: 650 }}>{title}</div>
          <div className="cell-wrap" style={{ color: "#667085", fontSize: 13, marginTop: 3 }}>
            {view.title || view.table?.display_name || "Selected table"} · showing {view.count || 0} of {view.total_rows || 0} row(s)
          </div>
        </div>
      </div>

      <RenderedRowsTable columns={view.columns || []} rows={view.rows || []} />
    </div>
  );
}

function RenderedRowsTable({ columns, rows }) {
  if (!columns.length) return <EmptyState label="No columns selected." />;
  if (!rows.length) return <EmptyState label="No rows match the selected table/filter." />;

  const minWidth = tableMinWidth(columns.length + 1, 560, 1280);

  return (
    <div className="dl-scrollbar table-scroll-frame">
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13, minWidth }}>
        <thead>
          <tr style={{ background: "#f2eee6", color: "#344054" }}>
            <th style={smallTh}>Row</th>
            {columns.map((col) => <th key={col} title={col} style={smallTh}>{col}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              <td style={{ ...smallTd, color: "#667085", minWidth: 160 }}>{row.row_key || row.definition || `Row ${i + 1}`}</td>
              {columns.map((col) => (
                <td key={col} style={smallTd}>{displayCell(row.values?.[col])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TableColumnCompareResult({ diff }) {
  const counts = diff.counts || {};
  const rows = diff.rows || diff.row_diffs || [];
  const alignment = diff.value_column_alignment || diff.header_alignment || [];

  return (
    <div style={{ marginTop: 14 }}>
      {diff.answer && (
        <div style={{ background: "#fffdf8", border: "1px solid #d8d0c3", borderLeft: "4px solid #2f5f4f", borderRadius: 8, padding: 12, marginBottom: 12, color: "#344054" }}>
          {diff.answer}
        </div>
      )}

      {diff.review_summary && (
        <div style={{ background: "#fbfaf6", border: "1px solid #ded6c8", borderRadius: 8, padding: 12, marginBottom: 12 }}>
          <div style={{ fontWeight: 650, marginBottom: 5 }}>Selected table review</div>
          <div style={{ color: "#475467", fontSize: 13, lineHeight: 1.45 }}>{diff.review_summary}</div>
        </div>
      )}

      {diff.ai_review && (
        <div style={{ background: "#fffdf8", border: "1px solid #d8d0c3", borderLeft: `4px solid ${diff.ai_review.available ? "#2f5f4f" : COLORS.DELETED.border}`, borderRadius: 8, padding: 12, marginBottom: 12 }}>
          <div style={{ fontWeight: 650, marginBottom: 5 }}>
            Selected table AI insight {diff.ai_review.available ? "- successful" : "- unavailable"}
            {typeof normalizeConfidence(diff.ai_review.confidence) === "number" ? ` | Confidence ${Math.round(normalizeConfidence(diff.ai_review.confidence) * 100)}%` : ""}
          </div>
          {diff.ai_review.available ? (
            <>
              {diff.ai_review.answer && <div dir="auto" style={{ color: "#344054", lineHeight: 1.45, marginBottom: 10 }}>{diff.ai_review.answer}</div>}
              {Array.isArray(diff.ai_review.rows) && diff.ai_review.rows.length > 0 && (
                <GenericRowsTable columns={diff.ai_review.columns?.length ? diff.ai_review.columns : inferColumns(diff.ai_review.rows)} rows={diff.ai_review.rows} />
              )}
            </>
          ) : (
            <div style={{ color: COLORS.DELETED.text }}>{diff.ai_review.error || "AI review was not generated."}</div>
          )}
        </div>
      )}

      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
        <StatChip label="Added rows" value={counts.ADDED || counts.added || 0} tone="added" />
        <StatChip label="Deleted rows" value={counts.DELETED || counts.deleted || 0} tone="deleted" />
        <StatChip label="Modified rows" value={counts.MODIFIED || counts.modified || 0} tone="modified" />
        <StatChip label="Compared changes" value={rows.length} />
      </div>

      {alignment.length > 0 && <ColumnAlignment alignment={alignment} />}

      {Array.isArray(diff.header_insights) && diff.header_insights.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 650, marginBottom: 8 }}>Header and meaning checks</div>
          <GenericRowsTable columns={diff.header_insight_columns || ["Baseline Header", "Revised Header", "Header Match", "Observation", "Seek Clarification"]} rows={diff.header_insights} />
        </div>
      )}

      {Array.isArray(diff.review_rows) && diff.review_rows.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <GenericRowsTable columns={diff.review_columns || ["Feature", "Change", "Seek Clarification"]} rows={diff.review_rows} />
        </div>
      )}

      <div className="table-selected-stack" style={{ marginTop: 14 }}>
        <SelectedTableView title="Baseline compared values" view={diff.base_preview} />
        <SelectedTableView title="Revised compared values" view={diff.target_preview} />
      </div>

      {rows.length === 0 ? (
        <EmptyState label="No row-level differences were found for the selected columns." />
      ) : (
        <div style={{ marginTop: 14 }}>
          {rows.slice(0, 200).map((row, i) => <TableColumnRowDiff key={i} row={row} />)}
        </div>
      )}
    </div>
  );
}

function ColumnAlignment({ alignment }) {
  return (
    <div style={{ background: "#fffdf8", border: "1px solid #ded6c8", borderRadius: 8, padding: 12 }}>
      <div style={{ fontWeight: 650, marginBottom: 8 }}>Selected column alignment</div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
        {alignment.slice(0, 60).map((item, i) => {
          const type = item.status === "matched" || item.status === "selected_pair" ? "MATCH" : item.status === "base_only" ? "DELETED" : "ADDED";
          return (
            <span key={i} style={{ border: `1px solid ${COLORS[type].border}`, background: COLORS[type].chip, color: COLORS[type].text, borderRadius: 999, padding: "3px 8px", fontSize: 12 }}>
              {item.base_col || "new"} {item.target_col ? `-> ${item.target_col}` : ""}
            </span>
          );
        })}
      </div>
    </div>
  );
}

function TableColumnRowDiff({ row }) {
  const type = row.change_type || row.status || "MODIFIED";
  const diffs = row.field_diffs || row.cell_diffs || row.value_diffs || row.diffs || [];

  return (
    <div style={{ background: "#fffdf8", border: "1px solid #ded6c8", borderLeft: `4px solid ${(COLORS[type] || COLORS.MODIFIED).border}`, borderRadius: 8, padding: 12, marginBottom: 10 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 10, flexWrap: "wrap" }}>
        <div>
          <ChangeBadge type={type} />
          <span style={{ marginLeft: 8, fontWeight: 650 }}>
            {row.row_key?.base || row.row_key?.target || row.key || row.definition || "row"}
          </span>
        </div>
        {typeof row.match_score === "number" && (
          <span style={{ color: "#667085", fontSize: 13 }}>Match {Math.round(row.match_score * 100)}%</span>
        )}
      </div>

      <div className="two-grid" style={{ marginTop: 10, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <DefinitionBox title="Baseline row" value={row.row_definition?.base || row.base_row?.definition || row.before} />
        <DefinitionBox title="Revised row" value={row.row_definition?.target || row.target_row?.definition || row.after} />
      </div>

      {diffs.length > 0 ? (
        <FieldDiffTable rows={diffs} />
      ) : (
        <div style={{ marginTop: 10 }}>
          <ValuesSideBySide base={row.base_row?.values || row.base_values} target={row.target_row?.values || row.target_values} />
        </div>
      )}
    </div>
  );
}

function ValuesSideBySide({ base, target }) {
  return (
    <div className="two-grid" style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
      <ValuesTable title="Baseline values" values={base} />
      <ValuesTable title="Revised values" values={target} />
    </div>
  );
}

function GenericRowsTable({ columns, rows }) {
  return (
    <div className="dl-scrollbar" style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13, minWidth: 780 }}>
        <thead>
          <tr style={{ background: "#1f2937", color: "white" }}>
            {columns.map((col) => <th key={col} dir="auto" style={th}>{col}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 200).map((row, i) => (
            <tr key={i}>
              {columns.map((col) => <td key={col} dir="auto" style={td}>{displayCell(row[col])}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function FieldDiffTable({ rows }) {
  if (!rows?.length) return null;

  return (
    <div className="dl-scrollbar" style={{ overflowX: "auto", marginTop: 10 }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13, minWidth: 640 }}>
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
              <td style={smallTd}>{r.field || r.column || r.name || "-"}</td>
              <td style={{ ...smallTd, color: COLORS.DELETED.text }}>{displayCell(r.before ?? r.base ?? r.old)}</td>
              <td style={{ ...smallTd, color: COLORS.ADDED.text }}>{displayCell(r.after ?? r.target ?? r.new)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ValuesTable({ title, values }) {
  const entries = Object.entries(values || {}).filter(([k]) => !["text", "definition"].includes(k));
  if (!entries.length) return <DefinitionBox title={title} value="-" />;

  return (
    <div style={{ background: "#fbfaf6", border: "1px solid #e0d8ca", borderRadius: 8, padding: 10 }}>
      <div style={{ fontSize: 12, color: "#667085", fontWeight: 650, marginBottom: 6 }}>{title}</div>
      <div className="dl-scrollbar" style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <tbody>
            {entries.slice(0, 40).map(([key, value]) => (
              <tr key={key}>
                <td style={{ ...smallTd, width: "32%", color: "#667085" }}>{key}</td>
                <td style={smallTd}>{displayCell(value)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function DefinitionBox({ title, value }) {
  return (
    <div style={{ background: "#fbfaf6", border: "1px solid #e0d8ca", borderRadius: 8, padding: 10 }}>
      <div style={{ fontSize: 12, color: "#667085", fontWeight: 650, marginBottom: 4 }}>{title}</div>
      <div style={{ fontSize: 13, color: "#344054", lineHeight: 1.4 }}>{displayCell(value)}</div>
    </div>
  );
}

function ChangeBadge({ type }) {
  const normalized = String(type || "MODIFIED").toUpperCase();
  const c = COLORS[normalized] || COLORS.MODIFIED;

  return (
    <span style={{ display: "inline-block", background: c.chip, color: c.text, border: `1px solid ${c.border}`, padding: "2px 8px", borderRadius: 999, fontWeight: 650, fontSize: 12 }}>
      {normalized}
    </span>
  );
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

function JobStatusBadge({ status }) {
  const value = String(status || "queued").toLowerCase();
  const tone =
    value === "complete" ? COLORS.ADDED :
    value === "failed" ? COLORS.DELETED :
    value === "running" ? COLORS.MODIFIED :
    COLORS.UNCHANGED;
  return (
    <span style={{ display: "inline-block", background: tone.chip, color: tone.text, border: `1px solid ${tone.border}`, padding: "2px 8px", borderRadius: 999, fontWeight: 650, fontSize: 12 }}>
      {value}
    </span>
  );
}

function ProgressMini({ value, failed = false }) {
  const pct = Math.max(0, Math.min(100, Number(value) || 0));
  return (
    <div>
      <div className="progress-track" style={{ height: 6, minWidth: 140 }}>
        <div className={`progress-fill ${failed ? "failed" : ""}`} style={{ width: `${failed ? 100 : pct}%` }} />
      </div>
      <div style={{ marginTop: 5, color: "#667085", fontSize: 12 }}>{failed ? "failed" : `${pct}%`}</div>
    </div>
  );
}

async function readResponseError(resp) {
  try {
    const text = await resp.text();
    if (!text) return `Request failed with status ${resp.status}`;

    try {
      const parsed = JSON.parse(text);
      return normalizeErrorMessage(parsed.detail || parsed.error || parsed.message || parsed);
    } catch {
      return text;
    }
  } catch {
    return `Request failed with status ${resp.status}`;
  }
}

function friendlyFetchError(err) {
  const message = normalizeErrorMessage(err);
  if (message.toLowerCase().includes("failed to fetch")) {
    return "The app could not reach the comparison service. Please confirm the backend is running and the API URL is correct.";
  }
  return message || "Something went wrong while processing the documents.";
}

async function fetchStructuredExtraction(runId) {
  const structuredResp = await fetch(`${API}/extract-runs/${runId}/structured-json`);
  if (structuredResp.ok) return structuredResp.json();

  const jsonResp = await fetch(`${API}/extract-runs/${runId}/json`);
  if (!jsonResp.ok) throw new Error(await readResponseError(structuredResp));

  const payload = await jsonResp.json();
  return normalizeStructuredExtractionPayload(payload);
}

function normalizeStructuredExtractionPayload(payload) {
  if (payload?.structured_json) return payload.structured_json;

  const blocks = payload?.blocks || [];
  const tables = payload?.tables || [];
  const semanticFields = [];

  blocks.forEach((block) => {
    const text = block.text || block.payload?.text || "";
    const match = String(text).match(/^\s*([^:：]{2,80})\s*[:：]\s*(.{1,300})$/);
    if (match) {
      semanticFields.push({
        field: match[1].trim(),
        value: match[2].trim(),
        page: block.page_number,
        source: block.type,
        citation: `p.${block.page_number || "-"} - ${block.path || "document"}`,
      });
    }

    inferTextAttributes(text).forEach((item) => {
      semanticFields.push({
        ...item,
        page: block.page_number,
        source: block.type,
        citation: `p.${block.page_number || "-"} - ${block.path || "document"}`,
      });
    });
  });

  tables.slice(0, 40).forEach((table) => {
    (table.rows || []).slice(0, 50).forEach((row) => {
      Object.entries(row || {}).forEach(([key, value]) => {
        if (!value || String(key).startsWith("__")) return;
        semanticFields.push({
          field: key,
          value,
          page: table.page_first || table.page_number,
          source: "table",
          table: table.display_name || table.title,
          citation: `${table.page_label || "page"} - ${table.title || "table"}`,
        });
      });
    });
  });

  return {
    run_id: payload?.run_id,
    documents: payload?.documents || [],
    summary: payload?.summary || {},
    coverage: payload?.coverage,
    semantic_fields: semanticFields.slice(0, 220),
    business_structure: buildBusinessStructureFromBlocks(blocks, tables, semanticFields),
    sections: blocks.filter((b) => ["section", "heading"].includes(b.type)).slice(0, 200),
    tables,
    text_blocks: blocks.filter((b) => ["paragraph", "list_item", "kv_pair", "figure"].includes(b.type)).slice(0, 500),
    ai_analysis: payload?.ai_analysis,
  };
}

function buildBusinessStructureFromBlocks(blocks, tables, semanticFields) {
  const docs = [{ document_index: 1, label: "Extracted document", sections: [] }];
  let current = null;

  blocks
    .slice()
    .sort((a, b) => (a.page_number || 1) - (b.page_number || 1) || (a.sequence || 0) - (b.sequence || 0))
    .forEach((block) => {
      if (["section", "heading"].includes(block.type)) {
        current = {
          title: block.text || block.path || `Page ${block.page_number || 1}`,
          page: block.page_number || 1,
          path: block.path,
          content: [],
          fields: [],
          inline_records: [],
          tables: [],
        };
        docs[0].sections.push(current);
        return;
      }

      if (!current || current.page !== (block.page_number || 1)) {
        current = {
          title: `Page ${block.page_number || 1}`,
          page: block.page_number || 1,
          path: `/page_${block.page_number || 1}`,
          content: [],
          fields: [],
          inline_records: [],
          tables: [],
        };
        docs[0].sections.push(current);
      }

      if (["paragraph", "list_item", "kv_pair", "figure"].includes(block.type)) {
        const text = block.text || block.payload?.text || "";
        const itemFields = semanticFields.filter((f) => f.page === block.page_number && f.citation?.includes(block.path || "__no_path__"));
        const inline = inlineRecordFromText(text);
        current.content.push({ type: block.type, page: block.page_number, path: block.path, text, fields: itemFields });
        current.fields.push(...itemFields);
        if (inline) current.inline_records.push({ ...inline, page: block.page_number, citation: `p.${block.page_number || "-"} - ${block.path || "document"}` });
      }
    });

  tables.forEach((table) => {
    const page = table.page_first || table.page_number || 1;
    let section = docs[0].sections.find((s) => s.page === page);
    if (!section) {
      section = { title: `Page ${page}`, page, path: `/page_${page}`, content: [], fields: [], inline_records: [], tables: [] };
      docs[0].sections.push(section);
    }
    section.tables.push({
      title: table.display_name || table.title || "Detected table",
      page_label: table.page_label,
      columns: table.columns || [],
      row_count: table.n_rows || 0,
      sample_rows: (table.rows || table.row_preview || []).slice(0, 8),
    });
  });

  return { documents: docs, section_count: docs[0].sections.length };
}

function inlineRecordFromText(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  const cells = raw.includes("|")
    ? raw.split("|").map((x) => x.trim()).filter(Boolean)
    : raw.split(/\s{3,}/).map((x) => x.trim()).filter(Boolean);
  if (cells.length < 2) return null;
  return {
    record_type: "inline_row",
    columns: cells.map((_, idx) => `Column ${idx + 1}`),
    values: Object.fromEntries(cells.map((value, idx) => [`Column ${idx + 1}`, value])),
    text: raw,
  };
}

function inferTextAttributes(text) {
  const source = String(text || "");
  const patterns = [
    ["color", /\b(?:colou?r|shade)\s*(?:is|=|:)?\s*([A-Za-z][A-Za-z\s/-]{2,40})/gi],
    ["size", /\b(?:size|dimension)\s*(?:is|=|:)?\s*([A-Z0-9][A-Z0-9\s./x-]{0,40})/gi],
    ["quantity", /\b(?:qty|quantity|count|units?)\s*(?:is|=|:)?\s*(\d[\d,]*(?:\.\d+)?)/gi],
    ["price", /([$€£]\s?\d[\d,]*(?:\.\d+)?)/g],
    ["percentage", /\b(\d+(?:\.\d+)?%)\b/g],
    ["date", /\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})\b/g],
    ["code", /\b([A-Z]{1,8}[- ]?\d{2,12}[A-Z]?)\b/gi],
  ];
  const out = [];
  const seen = new Set();

  patterns.forEach(([field, rx]) => {
    for (const match of source.matchAll(rx)) {
      const value = String(match[1] || "").replace(/\s+/g, " ").trim();
      const key = `${field}:${value.toLowerCase()}`;
      if (!value || seen.has(key)) continue;
      seen.add(key);
      out.push({ field, value });
    }
  });

  return out;
}

function normalizeErrorMessage(value) {
  if (!value) return "";
  if (typeof value === "string") return value;
  if (value instanceof Error) return normalizeErrorMessage(value.message);
  if (Array.isArray(value)) return value.map(normalizeErrorMessage).filter(Boolean).join("\n");
  if (typeof value === "object") {
    if (value.detail) return normalizeErrorMessage(value.detail);
    if (value.error) return normalizeErrorMessage(value.error);
    if (value.message) return normalizeErrorMessage(value.message);
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

function defaultRowColumns(table) {
  const columns = table?.columns || [];
  const suggested = table?.suggested_row_columns || [];
  const picked = suggested.filter((c) => columns.includes(c));
  return picked.length ? picked : columns.slice(0, 1);
}

function defaultValueColumns(table) {
  const columns = table?.columns || [];
  const suggested = table?.suggested_value_columns || [];
  const rowCols = defaultRowColumns(table);
  const picked = suggested.filter((c) => columns.includes(c) && !rowCols.includes(c));
  if (picked.length) return picked.slice(0, 12);
  return columns.filter((c) => !rowCols.includes(c)).slice(0, 12);
}

function inferColumns(rows) {
  if (!rows?.length) return [];
  const keys = new Set();
  rows.slice(0, 20).forEach((row) => {
    if (row && typeof row === "object" && !Array.isArray(row)) {
      Object.keys(row).forEach((key) => {
        if (!["payload", "raw"].includes(key)) keys.add(key);
      });
    }
  });
  return Array.from(keys).slice(0, 12);
}

function displayCell(value) {
  if (value === null || value === undefined || value === "") return "-";
  if (Array.isArray(value)) return value.map(displayCell).join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function tableMinWidth(columnCount, min = 560, max = 1280) {
  const count = Math.max(1, Number(columnCount) || 1);
  return Math.min(max, Math.max(min, 180 + count * 180));
}

function trim(value, limit) {
  if (!value) return "";
  const text = String(value).replace(/\s+/g, " ").trim();
  return text.length <= limit ? text : `${text.slice(0, limit - 1)}...`;
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function filterLabel(filter) {
  if (filter === "ALL") return "All changes";
  if (filter === "REVIEW") return "Needs review";
  if (filter === "ADDED") return "Added";
  if (filter === "DELETED") return "Deleted";
  if (filter === "MODIFIED") return "Modified";
  return filter.toLowerCase();
}

function friendlyCitation(value) {
  const text = String(value || "-");
  return text
    .replace(/\bbase\s*p\.?\s*(\d+)/gi, "Baseline page $1")
    .replace(/\btarget\s*p\.?\s*(\d+)/gi, "Revised page $1")
    .replace(/\bbaseline\s*p\.?\s*(\d+)/gi, "Baseline page $1")
    .replace(/\brevised\s*p\.?\s*(\d+)/gi, "Revised page $1")
    .replace(/\s*->\s*/g, " → ");
}

function impactRank(value) {
  const text = String(value || "").toLowerCase();
  if (text.includes("high")) return 3;
  if (text.includes("medium")) return 2;
  if (text.includes("low")) return 1;
  return 0;
}

function rowChangeType(row) {
  const raw = String(row?.change_type || row?.changeType || row?.status || "").toUpperCase();
  if (["ADDED", "DELETED", "MODIFIED", "UNCHANGED", "MATCH"].includes(raw)) return raw;

  const text = `${row?.type || ""} ${row?.change || ""} ${row?.description || ""} ${row?.review || ""}`.toUpperCase();
  if (text.includes("ADDED") || text.includes("NEW CONTENT") || text.includes("INTRODUCED")) return "ADDED";
  if (text.includes("DELETED") || text.includes("REMOVED") || text.includes("DROPPED")) return "DELETED";
  if (text.includes("MODIFIED") || text.includes("CHANGED") || text.includes("UPDATED") || text.includes("REVISED")) return "MODIFIED";
  return raw || "MODIFIED";
}

function needsReview(row) {
  const text = `${row.seek_clarification || ""} ${row.review || ""} ${row.recommendation || ""}`.toLowerCase();
  const confidence = normalizeConfidence(row.confidence);
  return text.includes("review") || text.includes("clarif") || text.includes("confirm") || (typeof confidence === "number" && confidence < 0.8);
}

function normalizeConfidence(value) {
  if (typeof value !== "number") return null;
  return value > 1 ? value / 100 : value;
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

const labelStyle = {
  display: "block",
  marginBottom: 7,
  fontSize: 13,
  color: "#344054",
  fontWeight: 650,
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
  verticalAlign: "top",
  whiteSpace: "normal",
  overflowWrap: "anywhere",
};

const smallTd = {
  padding: "8px 9px",
  borderBottom: "1px solid #eee7dc",
  verticalAlign: "top",
  whiteSpace: "normal",
  overflowWrap: "anywhere",
  lineHeight: 1.35,
};

const miniButtonStyle = {
  border: "1px solid #c9c0b0",
  background: "#fffdf8",
  color: "#344054",
  borderRadius: 6,
  padding: "3px 7px",
  cursor: "pointer",
  fontSize: 12,
  fontWeight: 600,
};

const softPillStyle = {
  border: "1px solid #ded6c8",
  background: "#fbfaf6",
  color: "#344054",
  borderRadius: 999,
  padding: "4px 8px",
  fontSize: 12,
  fontWeight: 600,
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

function modeButtonStyle(active, disabled = false) {
  return {
    border: `1px solid ${active ? "#1f2937" : "#c9c0b0"}`,
    background: active ? "#1f2937" : "#fffdf8",
    color: active ? "white" : "#344054",
    borderRadius: 999,
    padding: "7px 12px",
    cursor: disabled ? "default" : "pointer",
    fontWeight: 600,
    opacity: disabled ? 0.7 : 1,
  };
}

function presetButtonStyle(disabled = false) {
  return {
    border: "1px solid #d8d0c3",
    background: "#fffdf8",
    color: "#344054",
    borderRadius: 999,
    padding: "6px 10px",
    cursor: disabled ? "default" : "pointer",
    fontWeight: 550,
    fontSize: 12,
    opacity: disabled ? 0.65 : 1,
  };
}

function primaryButtonStyle(disabled = false, extra = {}) {
  return {
    border: "none",
    borderRadius: 6,
    background: disabled ? "#98a2b3" : "#1f2937",
    color: "white",
    padding: "9px 14px",
    fontWeight: 550,
    cursor: disabled ? "default" : "pointer",
    ...extra,
  };
}

function secondaryButtonStyle(extra = {}) {
  return {
    border: "1px solid #c9c0b0",
    borderRadius: 6,
    background: "#fffdf8",
    color: "#344054",
    padding: "9px 13px",
    fontWeight: 550,
    cursor: "pointer",
    ...extra,
  };
}
