"""Tool for retrieving business rules / guide documentation for a schema."""

from framework.agent import Tool
from framework.index import AgentIndex


def make_business_rules_tool(index: AgentIndex) -> Tool:
    """Create the business rules tool bound to a preloaded index.

    Includes per-agent dedup cache to prevent re-fetching the same guide.
    """
    _fetched: set[str] = set()

    def get_business_rules(schema_name: str) -> str:
        if schema_name in _fetched:
            return (
                f"You already retrieved business rules for '{schema_name}' above. "
                "Do NOT call this again — use the rules you already have."
            )
        _fetched.add(schema_name)

        content = index.get_business_rules(schema_name)
        if content is not None:
            return content

        available = index.get_all_guide_topics()
        return (
            f"No business rules guide found for schema '{schema_name}'.\n"
            f"Available guide topics: {', '.join(available)}"
        )

    return Tool(
        name="get_business_rules",
        description=(
            "Retrieve the business rules and domain guide for a database schema. "
            "ALWAYS call this before writing queries — the guide contains critical "
            "filtering rules, field definitions, and domain conventions that the "
            "question may not mention."
        ),
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
        function=get_business_rules,
    )
