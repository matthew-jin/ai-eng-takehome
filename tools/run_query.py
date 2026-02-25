"""Tool for executing SQL queries against the DuckDB database."""

from framework.agent import Tool
from framework.database import execute_query

MAX_ROWS = 10


def run_query(query: str) -> str:
    """Execute a SQL query and return formatted results."""
    result = execute_query(query)

    if not result.is_success:
        return f"Query error: {result.error_message}\n\nPlease fix the query and try again."

    df = result.dataframe
    if df is None:
        return "Query returned no dataframe."

    col_names = df.columns
    rows = df.rows()
    total_rows = len(rows)

    lines: list[str] = [
        f"Rows: {total_rows}, Columns: {len(col_names)}",
        "",
        " | ".join(col_names),
        "-" * (sum(len(c) + 3 for c in col_names)),
    ]

    for row in rows[:MAX_ROWS]:
        lines.append(" | ".join(str(v) for v in row))

    if total_rows > MAX_ROWS:
        lines.append(f"... ({total_rows - MAX_ROWS} more rows)")

    return "\n".join(lines)


RUN_QUERY = Tool(
    name="run_query",
    description=(
        "Execute a SQL query against the DuckDB database and return the results. "
        "Use schema.table syntax (e.g. SELECT * FROM financial.account). "
        "Use this to test and verify your queries before submitting."
    ),
    parameters={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The SQL query to execute.",
            },
        },
        "required": ["query"],
    },
    function=run_query,
)
