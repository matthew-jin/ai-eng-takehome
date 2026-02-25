"""Schema discovery tools backed by the preloaded AgentIndex.

All three tools return instantly from the in-memory index (no DB queries).
Includes per-agent dedup caches to avoid the model re-requesting the same info.
"""

from framework.agent import Tool
from framework.index import AgentIndex


def make_explore_tools(index: AgentIndex) -> list[Tool]:
    """Create the schema exploration tools bound to a preloaded index.

    Each set of tools has its own dedup cache, so each agent (thread) gets
    its own cache when create_tools() is called per-worker.
    """
    _described: set[tuple[str, str]] = set()
    _schemas_listed = False

    def list_schemas() -> str:
        nonlocal _schemas_listed
        if _schemas_listed:
            return (
                "You already listed schemas above. "
                "Do NOT call this again — proceed to the next step."
            )
        _schemas_listed = True
        guided = set(index.get_all_guide_topics())
        all_schemas = index.get_schema_list()
        guided_schemas = [s for s in all_schemas if s in guided]
        other_schemas = [s for s in all_schemas if s not in guided]

        lines: list[str] = [
            "Available schemas and their tables:",
            "",
            "Schemas with business rules guides (most likely relevant):",
        ]
        for s in guided_schemas:
            tables = index.get_tables(s)
            lines.append(f"  - {s}: {', '.join(tables)}")
        lines.append("")
        lines.append(f"Other schemas (no guide): {', '.join(other_schemas)}")
        return "\n".join(lines)

    def list_tables(schema_name: str) -> str:
        tables = index.get_tables(schema_name)
        if not tables:
            available = index.get_schema_list()
            return (
                f"Schema '{schema_name}' not found.\n"
                f"Available schemas: {', '.join(available)}"
            )
        return f"Tables in {schema_name}:\n" + "\n".join(f"  - {t}" for t in tables)

    def describe_table(schema_name: str, table_name: str) -> str:
        key = (schema_name, table_name)
        if key in _described:
            return (
                f"You already described {schema_name}.{table_name} above. "
                "Do NOT call describe_table on the same table again — "
                "use the information you already have and write your query."
            )
        _described.add(key)
        return index.get_table_description(schema_name, table_name)

    return [
        Tool(
            name="list_schemas",
            description="List all available database schemas.",
            parameters={"type": "object", "properties": {}, "required": []},
            function=list_schemas,
        ),
        Tool(
            name="list_tables",
            description="List all tables in a given database schema.",
            parameters={
                "type": "object",
                "properties": {
                    "schema_name": {
                        "type": "string",
                        "description": "Name of the database schema (e.g. 'financial', 'Credit').",
                    },
                },
                "required": ["schema_name"],
            },
            function=list_tables,
        ),
        Tool(
            name="describe_table",
            description=(
                "Describe a table's columns (name, type, nullable) and show sample rows. "
                "Use this to understand the data before writing queries."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "schema_name": {
                        "type": "string",
                        "description": "Name of the database schema.",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table.",
                    },
                },
                "required": ["schema_name", "table_name"],
            },
            function=describe_table,
        ),
    ]
