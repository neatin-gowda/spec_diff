-- =========================================================================
-- spec_diff schema (Postgres 15+ with pgvector)
-- Designed to store ANY supplier's structured spec document, plus diffs.
-- =========================================================================

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---------------------------------------------------------------------
-- A "document family" = a recurring supplier publication
-- (e.g., "Ford Bronco Model Spec", "Ford F-150 Order Guide", "GM Tahoe MPF")
-- The template_profile is auto-discovered on first ingest, then reused.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS document_family (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    supplier         TEXT NOT NULL,
    family_name      TEXT NOT NULL,
    template_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    template_version INT NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (supplier, family_name)
);

-- ---------------------------------------------------------------------
-- One row per uploaded PDF.
-- raw_pdf_blob_uri points to Azure Blob Storage.
-- page_images_prefix is the blob prefix where rendered page images live.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spec_document (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    family_id           UUID NOT NULL REFERENCES document_family(id),
    label               TEXT NOT NULL,             -- e.g. "2024_MPF_Model_Spec"
    version_tag         TEXT,                      -- e.g. "2024MY"
    raw_pdf_blob_uri    TEXT NOT NULL,
    page_images_prefix  TEXT NOT NULL,
    page_count          INT NOT NULL,
    sha256              CHAR(64) NOT NULL,
    extracted_at        TIMESTAMPTZ,
    coverage_pct        NUMERIC(5,2),              -- extraction coverage
    uploaded_by         TEXT,
    uploaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (family_id, sha256)
);

CREATE INDEX IF NOT EXISTS idx_spec_document_family ON spec_document (family_id);
CREATE INDEX IF NOT EXISTS idx_spec_document_sha ON spec_document (sha256);

-- ---------------------------------------------------------------------
-- A "block" = the smallest semantically meaningful unit
-- (section heading, table row, paragraph, list item, key/value pair).
-- block_type is one of: section | heading | paragraph | table | table_row
--                      | list_item | kv_pair | figure | note
-- path is a slash-separated logical path inside the doc tree.
-- stable_key is the natural identifier when one exists.
-- payload is the structured body of the block.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS doc_block (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_id     UUID NOT NULL REFERENCES spec_document(id) ON DELETE CASCADE,
    parent_id       UUID REFERENCES doc_block(id) ON DELETE CASCADE,
    block_type      TEXT NOT NULL,
    path            TEXT NOT NULL,
    stable_key      TEXT,
    page_number     INT NOT NULL,
    bbox            NUMERIC[],                     -- [x0,y0,x1,y1] on page
    text            TEXT,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_hash    CHAR(64) NOT NULL,
    embedding       vector(1536),
    sequence        INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_block_doc         ON doc_block (document_id);
CREATE INDEX IF NOT EXISTS idx_block_path        ON doc_block (document_id, path);
CREATE INDEX IF NOT EXISTS idx_block_key         ON doc_block (document_id, stable_key)
                                                  WHERE stable_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_block_payload_gin ON doc_block USING gin (payload jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_block_text_trgm   ON doc_block USING gin (text gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_block_embedding   ON doc_block USING hnsw (embedding vector_cosine_ops);

-- =========================================================================
-- NORMALIZED TABLE STORAGE
-- =========================================================================
-- Why this exists:
-- doc_block.payload can store table JSON, but accurate table comparison and
-- natural-language table queries need first-class tables, columns, rows,
-- and cells. This lets the agent answer:
--   "Compare PCV 205 in the old document with PCV 203 in the new document"
-- and lets the UI render selected columns immediately.
-- =========================================================================

-- ---------------------------------------------------------------------
-- One row per detected logical table.
-- A table may span multiple pages.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS doc_table (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_id          UUID NOT NULL REFERENCES spec_document(id) ON DELETE CASCADE,
    block_id             UUID REFERENCES doc_block(id) ON DELETE SET NULL,

    table_index          INT NOT NULL,
    title                TEXT,                       -- business-facing table name
    context              TEXT,                       -- section path / nearby heading text
    page_start           INT NOT NULL,
    page_end             INT NOT NULL,
    pages                INT[] NOT NULL DEFAULT '{}',
    bbox_by_page         JSONB NOT NULL DEFAULT '{}'::jsonb,

    header_source        TEXT,                       -- normal | vertical | inferred | mixed
    extraction_strategy  TEXT,                       -- pdfplumber lines/text/camelot/etc
    extraction_confidence NUMERIC(4,3),
    stitched_from        INT NOT NULL DEFAULT 1,

    column_count         INT NOT NULL DEFAULT 0,
    row_count            INT NOT NULL DEFAULT 0,
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (document_id, table_index)
);

CREATE INDEX IF NOT EXISTS idx_doc_table_doc           ON doc_table (document_id);
CREATE INDEX IF NOT EXISTS idx_doc_table_page          ON doc_table (document_id, page_start, page_end);
CREATE INDEX IF NOT EXISTS idx_doc_table_title_trgm    ON doc_table USING gin (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_doc_table_context_trgm  ON doc_table USING gin (context gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_doc_table_metadata_gin  ON doc_table USING gin (metadata jsonb_path_ops);

-- ---------------------------------------------------------------------
-- One row per logical column in a detected table.
-- semantic_role helps the agent understand whether a column is the feature
-- column, a PCV/value column, a code column, a price/status column, etc.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS doc_table_column (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_id             UUID NOT NULL REFERENCES doc_table(id) ON DELETE CASCADE,

    column_index         INT NOT NULL,
    header_text          TEXT NOT NULL,
    normalized_header    TEXT NOT NULL,
    header_source        TEXT,                       -- normal | vertical | inferred
    semantic_role        TEXT,                       -- row_label | value | pcv | code | amount | status | date | unknown
    value_type_hint      TEXT,                       -- text | number | symbol | date | currency | mixed | blank
    sample_values        JSONB NOT NULL DEFAULT '[]'::jsonb,
    confidence           NUMERIC(4,3),
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,

    UNIQUE (table_id, column_index)
);

CREATE INDEX IF NOT EXISTS idx_table_column_table         ON doc_table_column (table_id);
CREATE INDEX IF NOT EXISTS idx_table_column_header_trgm   ON doc_table_column USING gin (header_text gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_table_column_norm          ON doc_table_column (table_id, normalized_header);
CREATE INDEX IF NOT EXISTS idx_table_column_role          ON doc_table_column (semantic_role);

-- ---------------------------------------------------------------------
-- One row per logical row in a detected table.
-- row_key/row_label make row lookup fast and explainable.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS doc_table_row (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_id         UUID NOT NULL REFERENCES doc_table(id) ON DELETE CASCADE,
    block_id         UUID REFERENCES doc_block(id) ON DELETE SET NULL,

    row_index        INT NOT NULL,
    page_number      INT NOT NULL,
    bbox             NUMERIC[],
    stable_key       TEXT,
    row_label        TEXT,
    row_text         TEXT,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,

    UNIQUE (table_id, row_index)
);

CREATE INDEX IF NOT EXISTS idx_table_row_table       ON doc_table_row (table_id);
CREATE INDEX IF NOT EXISTS idx_table_row_key         ON doc_table_row (table_id, stable_key)
                                                      WHERE stable_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_table_row_label_trgm  ON doc_table_row USING gin (row_label gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_table_row_text_trgm   ON doc_table_row USING gin (row_text gin_trgm_ops);

-- ---------------------------------------------------------------------
-- One row per table cell.
-- This is the most important table for exact custom comparison.
-- It allows:
--   * selected-column rendering
--   * cell-level diff
--   * NL queries over row/column/value
--   * PCV-to-PCV comparisons
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS doc_table_cell (
    id                  BIGSERIAL PRIMARY KEY,
    table_id             UUID NOT NULL REFERENCES doc_table(id) ON DELETE CASCADE,
    row_id               UUID NOT NULL REFERENCES doc_table_row(id) ON DELETE CASCADE,
    column_id            UUID NOT NULL REFERENCES doc_table_column(id) ON DELETE CASCADE,

    row_index            INT NOT NULL,
    column_index         INT NOT NULL,
    raw_value            TEXT,
    normalized_value     TEXT,
    value_type           TEXT,                       -- text | number | symbol | date | currency | blank | mixed
    bbox                 NUMERIC[],
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,

    UNIQUE (table_id, row_index, column_index)
);

CREATE INDEX IF NOT EXISTS idx_table_cell_table          ON doc_table_cell (table_id);
CREATE INDEX IF NOT EXISTS idx_table_cell_row            ON doc_table_cell (row_id);
CREATE INDEX IF NOT EXISTS idx_table_cell_column         ON doc_table_cell (column_id);
CREATE INDEX IF NOT EXISTS idx_table_cell_value_trgm     ON doc_table_cell USING gin (raw_value gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_table_cell_normalized     ON doc_table_cell (table_id, column_index, normalized_value);

-- ---------------------------------------------------------------------
-- A comparison_run = one diff between two documents of the same family.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS comparison_run (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    family_id       UUID NOT NULL REFERENCES document_family(id),
    base_doc_id     UUID NOT NULL REFERENCES spec_document(id),
    target_doc_id   UUID NOT NULL REFERENCES spec_document(id),
    status          TEXT NOT NULL DEFAULT 'pending', -- pending|running|complete|failed
    summary_json    JSONB,
    stats           JSONB,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    error           TEXT,
    UNIQUE (base_doc_id, target_doc_id)
);

CREATE INDEX IF NOT EXISTS idx_comparison_family ON comparison_run (family_id);
CREATE INDEX IF NOT EXISTS idx_comparison_docs   ON comparison_run (base_doc_id, target_doc_id);

-- ---------------------------------------------------------------------
-- One row per block-pair decision in a comparison_run.
-- change_type: ADDED | DELETED | MODIFIED | UNCHANGED
-- For ADDED, base_block_id is null. For DELETED, target_block_id is null.
-- field_diffs is an array of {path, before, after} for table cells / kv pairs.
-- token_diff is a JSON list of {op: equal|insert|delete|replace, text} for prose.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS block_diff (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES comparison_run(id) ON DELETE CASCADE,
    base_block_id   UUID REFERENCES doc_block(id),
    target_block_id UUID REFERENCES doc_block(id),
    change_type     TEXT NOT NULL,
    similarity      NUMERIC(4,3),
    field_diffs     JSONB,
    token_diff      JSONB,
    impact_score    NUMERIC(4,3),
    CHECK (change_type IN ('ADDED','DELETED','MODIFIED','UNCHANGED'))
);

CREATE INDEX IF NOT EXISTS idx_diff_run         ON block_diff (run_id);
CREATE INDEX IF NOT EXISTS idx_diff_run_change  ON block_diff (run_id, change_type);
CREATE INDEX IF NOT EXISTS idx_diff_field_gin   ON block_diff USING gin (field_diffs jsonb_path_ops);

-- ---------------------------------------------------------------------
-- Stores table-level or selected-column comparison requests/results.
-- This lets the UI/report/agent reuse a user-selected table comparison.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS table_comparison_result (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id               UUID NOT NULL REFERENCES comparison_run(id) ON DELETE CASCADE,

    base_table_id         UUID REFERENCES doc_table(id) ON DELETE SET NULL,
    target_table_id       UUID REFERENCES doc_table(id) ON DELETE SET NULL,

    base_row_columns      JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_row_columns    JSONB NOT NULL DEFAULT '[]'::jsonb,
    base_value_columns    JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_value_columns  JSONB NOT NULL DEFAULT '[]'::jsonb,
    row_filter            TEXT,

    counts                JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_summary        TEXT,
    result_json           JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_by            TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_table_cmp_run ON table_comparison_result (run_id);
CREATE INDEX IF NOT EXISTS idx_table_cmp_tables ON table_comparison_result (base_table_id, target_table_id);
CREATE INDEX IF NOT EXISTS idx_table_cmp_result_gin ON table_comparison_result USING gin (result_json jsonb_path_ops);

-- ---------------------------------------------------------------------
-- Cell-level comparison details for selected table comparisons.
-- Useful for exact reports and auditability.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS table_comparison_cell_diff (
    id                      BIGSERIAL PRIMARY KEY,
    table_comparison_id      UUID NOT NULL REFERENCES table_comparison_result(id) ON DELETE CASCADE,

    base_row_id              UUID REFERENCES doc_table_row(id) ON DELETE SET NULL,
    target_row_id            UUID REFERENCES doc_table_row(id) ON DELETE SET NULL,
    base_column_id           UUID REFERENCES doc_table_column(id) ON DELETE SET NULL,
    target_column_id         UUID REFERENCES doc_table_column(id) ON DELETE SET NULL,

    row_key_base             TEXT,
    row_key_target           TEXT,
    column_name_base         TEXT,
    column_name_target       TEXT,

    before_value             TEXT,
    after_value              TEXT,
    change_type              TEXT NOT NULL,
    similarity               NUMERIC(4,3),
    confidence               NUMERIC(4,3),
    insight                  TEXT,
    metadata                 JSONB NOT NULL DEFAULT '{}'::jsonb,

    CHECK (change_type IN ('ADDED','DELETED','MODIFIED','UNCHANGED'))
);

CREATE INDEX IF NOT EXISTS idx_table_cell_diff_cmp ON table_comparison_cell_diff (table_comparison_id);
CREATE INDEX IF NOT EXISTS idx_table_cell_diff_change ON table_comparison_cell_diff (table_comparison_id, change_type);
CREATE INDEX IF NOT EXISTS idx_table_cell_diff_key_trgm ON table_comparison_cell_diff USING gin (row_key_base gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_table_cell_diff_after_key_trgm ON table_comparison_cell_diff USING gin (row_key_target gin_trgm_ops);

-- ---------------------------------------------------------------------
-- Page-level visual diff cache (used by side-by-side viewer).
-- regions are an array of {page, bbox, change_type} the UI can overlay.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS page_diff (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES comparison_run(id) ON DELETE CASCADE,
    side            CHAR(1) NOT NULL,               -- 'L' (base) or 'R' (target)
    page_number     INT NOT NULL,
    regions         JSONB NOT NULL,
    UNIQUE (run_id, side, page_number)
);

-- ---------------------------------------------------------------------
-- Saved NL queries (history + reusable) and their resolved interpretations.
-- response_view allows text/table/comparison/evidence rendering.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nl_query_log (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID REFERENCES comparison_run(id),
    user_id         TEXT,
    nl_text         TEXT NOT NULL,
    resolved_plan   JSONB NOT NULL,
    response_view   TEXT,                           -- summary | table | comparison | evidence | text
    answer_text     TEXT,
    result_columns  JSONB NOT NULL DEFAULT '[]'::jsonb,
    result_rows     JSONB NOT NULL DEFAULT '[]'::jsonb,
    citations       JSONB NOT NULL DEFAULT '[]'::jsonb,
    result_count    INT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nl_query_run ON nl_query_log (run_id);
CREATE INDEX IF NOT EXISTS idx_nl_query_text_trgm ON nl_query_log USING gin (nl_text gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_nl_query_plan_gin ON nl_query_log USING gin (resolved_plan jsonb_path_ops);

-- ---------------------------------------------------------------------
-- Convenience views
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_diff_summary AS
SELECT
    r.id              AS run_id,
    r.family_id,
    r.base_doc_id,
    r.target_doc_id,
    SUM((change_type = 'ADDED')::int)     AS n_added,
    SUM((change_type = 'DELETED')::int)   AS n_deleted,
    SUM((change_type = 'MODIFIED')::int)  AS n_modified,
    SUM((change_type = 'UNCHANGED')::int) AS n_unchanged
FROM comparison_run r
LEFT JOIN block_diff d ON d.run_id = r.id
GROUP BY r.id;

CREATE OR REPLACE VIEW v_document_tables AS
SELECT
    d.id AS document_id,
    d.label AS document_label,
    t.id AS table_id,
    t.table_index,
    t.title,
    t.context,
    t.page_start,
    t.page_end,
    t.column_count,
    t.row_count,
    t.header_source,
    t.extraction_strategy,
    t.extraction_confidence
FROM spec_document d
JOIN doc_table t ON t.document_id = d.id;

CREATE OR REPLACE VIEW v_table_cells AS
SELECT
    d.id AS document_id,
    d.label AS document_label,
    t.id AS table_id,
    t.title AS table_title,
    t.context AS table_context,
    t.page_start,
    r.id AS row_id,
    r.row_index,
    r.stable_key,
    r.row_label,
    c.id AS column_id,
    c.column_index,
    c.header_text,
    c.normalized_header,
    c.semantic_role,
    cell.raw_value,
    cell.normalized_value,
    cell.value_type
FROM spec_document d
JOIN doc_table t ON t.document_id = d.id
JOIN doc_table_row r ON r.table_id = t.id
JOIN doc_table_cell cell ON cell.row_id = r.id
JOIN doc_table_column c ON c.id = cell.column_id;
