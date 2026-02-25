"""Preloaded index of database schemas, tables, columns, samples, and business rule guides.

Built once at startup. Tools read from this index for instant responses.
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

DATABASE_PATH = Path(__file__).parent.parent / "hecks.duckdb"
GUIDES_DIR = Path(__file__).parent.parent / "evaluation" / "data" / "guides"
CACHE_PATH = Path(__file__).parent.parent / ".cache" / "agent_index.json"

SAMPLE_ROWS = 3


@dataclass
class ColumnInfo:
    name: str
    type: str
    nullable: bool


@dataclass
class TableInfo:
    columns: list[ColumnInfo] = field(default_factory=list)
    sample_rows: list[list[str]] = field(default_factory=list)


@dataclass
class GuideInfo:
    file: str
    content: str


@dataclass
class AgentIndex:
    schemas: dict[str, dict[str, TableInfo]] = field(default_factory=dict)
    guides: dict[str, GuideInfo] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    @staticmethod
    def build() -> "AgentIndex":
        """Build a fresh index by scanning the database and guide files."""
        index = AgentIndex()
        index._scan_database()
        index._scan_guides()
        index._save_cache()
        return index

    # ------------------------------------------------------------------
    # Database scanning
    # ------------------------------------------------------------------

    def _scan_database(self) -> None:
        conn = duckdb.connect(str(DATABASE_PATH), read_only=True)
        try:
            # Get all schemas
            schema_rows = conn.execute(
                """
                SELECT DISTINCT table_schema
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema
                """
            ).fetchall()

            for (schema_name,) in schema_rows:
                tables: dict[str, TableInfo] = {}

                # Tables in this schema
                table_rows = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = ? AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """,
                    [schema_name],
                ).fetchall()

                for (table_name,) in table_rows:
                    # Columns
                    col_rows = conn.execute(
                        """
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = ? AND table_name = ?
                        ORDER BY ordinal_position
                        """,
                        [schema_name, table_name],
                    ).fetchall()

                    columns = [
                        ColumnInfo(
                            name=cn,
                            type=dt,
                            nullable=nu == "YES",
                        )
                        for cn, dt, nu in col_rows
                    ]

                    # Sample rows
                    sample_rows: list[list[str]] = []
                    try:
                        rows = conn.execute(
                            f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT {SAMPLE_ROWS}'
                        ).fetchall()
                        for row in rows:
                            sample_rows.append([str(v) for v in row])
                    except Exception:
                        pass

                    tables[table_name] = TableInfo(columns=columns, sample_rows=sample_rows)

                self.schemas[schema_name] = tables
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Guide scanning
    # ------------------------------------------------------------------

    def _scan_guides(self) -> None:
        if not GUIDES_DIR.exists():
            return

        schema_names_lower = {s.lower(): s for s in self.schemas}

        for guide_path in sorted(GUIDES_DIR.glob("*.md")):
            content = guide_path.read_text()
            first_line = content.split("\n", 1)[0].strip().lstrip("# ").strip()

            schema_name = self._extract_schema_from_title(first_line, schema_names_lower)
            if schema_name is None:
                schema_name = self._fuzzy_match_filename(guide_path.stem, schema_names_lower)

            if schema_name is not None:
                self.guides[schema_name] = GuideInfo(file=guide_path.name, content=content)

    @staticmethod
    def _extract_schema_from_title(
        title: str, known_lower: dict[str, str]
    ) -> str | None:
        """Extract schema name from guide title.

        Handles patterns like:
          - "Title (SchemaName Database)"
          - "Title (SchemaName / OtherName)"
          - "Financial Database Business Rules" (no parens)
        """
        # Try parenthetical first: (SchemaName Database)
        m = re.search(r"\(([^)]+)\)", title)
        if m:
            inner = m.group(1)
            # Try each token / slash-separated name inside parens
            for candidate in re.split(r"[/,]", inner):
                candidate = candidate.strip()
                # Remove trailing "Database(s)" if present
                candidate = re.sub(r"\s+Databases?\s*$", "", candidate).strip()
                if candidate.lower() in known_lower:
                    return known_lower[candidate.lower()]

        # Fallback: look for "<word> Database" anywhere in the title
        m2 = re.search(r"(\w+)\s+Database", title)
        if m2:
            candidate = m2.group(1)
            if candidate.lower() in known_lower:
                return known_lower[candidate.lower()]

        return None

    @staticmethod
    def _fuzzy_match_filename(stem: str, known_lower: dict[str, str]) -> str | None:
        """Fuzzy match guide filename against known schema names."""
        tokens = re.split(r"[_\-\s]+", stem.lower())
        for token in tokens:
            if token in known_lower:
                return known_lower[token]
        return None

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def _save_cache(self) -> None:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "schemas": {
                schema: {
                    table: {
                        "columns": [
                            {"name": c.name, "type": c.type, "nullable": c.nullable}
                            for c in info.columns
                        ],
                        "sample_rows": info.sample_rows,
                    }
                    for table, info in tables.items()
                }
                for schema, tables in self.schemas.items()
            },
            "guides": {
                schema: {"file": g.file, "content": g.content}
                for schema, g in self.guides.items()
            },
        }
        CACHE_PATH.write_text(json.dumps(data, indent=2))

    # ------------------------------------------------------------------
    # Public query API
    # ------------------------------------------------------------------

    def get_schema_list(self) -> list[str]:
        return sorted(self.schemas.keys())

    def get_tables(self, schema: str) -> list[str]:
        tables = self.schemas.get(schema)
        if tables is None:
            return []
        return sorted(tables.keys())

    def get_table_info(self, schema: str, table: str) -> TableInfo | None:
        tables = self.schemas.get(schema)
        if tables is None:
            return None
        return tables.get(table)

    def get_table_description(self, schema: str, table: str, max_columns: int = 40) -> str:
        info = self.get_table_info(schema, table)
        if info is None:
            return f"Table '{schema}.{table}' not found."
        columns = info.columns
        truncated = len(columns) > max_columns
        display_cols = columns[:max_columns]

        lines: list[str] = [f"Table: {schema}.{table} ({len(columns)} columns)", "", "Columns:"]
        for c in display_cols:
            nullable = ", nullable" if c.nullable else ""
            lines.append(f"  {c.name} ({c.type}{nullable})")
        if truncated:
            lines.append(f"  ... and {len(columns) - max_columns} more columns")
            lines.append(
                "  (Use run_query with SELECT * ... LIMIT 3 to see all columns)"
            )

        if info.sample_rows and not truncated:
            col_names = [c.name for c in display_cols]
            lines.append("")
            lines.append(f"Sample rows ({len(info.sample_rows)}):")
            lines.append("  " + " | ".join(col_names))
            lines.append("  " + "-" * (sum(len(n) + 3 for n in col_names)))
            for row in info.sample_rows:
                lines.append("  " + " | ".join(row[:max_columns]))
        return "\n".join(lines)

    def get_business_rules(self, schema: str) -> str | None:
        guide = self.guides.get(schema)
        if guide is not None:
            return guide.content
        return None

    def get_all_guide_topics(self) -> list[str]:
        return sorted(self.guides.keys())
