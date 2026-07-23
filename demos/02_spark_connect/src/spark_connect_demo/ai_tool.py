"""Embed Spark in an AI application via Spark Connect.

    python -m spark_connect_demo.ai_tool

The story: an AI assistant answers business questions by calling a tool that runs a Spark
query over a persistent Connect session. Because the tool queries the *metric view* from
Demo 1 (delivery_metrics), the AI gets the SAME governed, aggregation-safe number a dashboard
would — no chance of it averaging ratios or double-counting distinct users.

Here the natural-language -> query routing is a small deterministic rule map so the demo runs
offline. In a real app this `MetricTool.answer` is exactly what you register as an LLM tool
(Claude/Anthropic tool use, an MCP server, etc.) — the LLM picks the intent, Spark Connect
executes it, the governed number comes back.
"""

from __future__ import annotations

from .config import get_spark

METRIC_VIEW = "delivery_metrics"

# intent -> (SQL against the governed metric view, human phrasing)
INTENTS = {
    "overall_conversion": (
        f"SELECT ROUND(MEASURE(conversion_rate), 4) AS v FROM {METRIC_VIEW}",
        "Overall conversion rate",
    ),
    "active_users": (
        f"SELECT MEASURE(active_users) AS v FROM {METRIC_VIEW}",
        "Monthly active users",
    ),
    "best_region": (
        f"SELECT region AS v FROM {METRIC_VIEW} GROUP BY region "
        f"ORDER BY MEASURE(conversion_rate) DESC LIMIT 1",
        "Best-converting region",
    ),
}

# a tiny keyword router standing in for an LLM's tool-call selection
QUESTIONS = [
    ("What's our overall conversion rate?", "overall_conversion"),
    ("How many active users this month?", "active_users"),
    ("Which region converts best?", "best_region"),
]


class MetricTool:
    """A Spark-Connect-backed tool an AI assistant can call. Holds one remote session."""

    def __init__(self):
        self.spark = get_spark("ai-metric-tool")

    def answer(self, intent: str):
        sql, _ = INTENTS[intent]
        return self.spark.sql(sql).first()["v"]

    def close(self):
        self.spark.stop()


def main() -> int:
    tool = MetricTool()
    try:
        # guard: the governed metric view comes from Demo 1
        if not tool.spark.catalog.tableExists(METRIC_VIEW):
            print(f"'{METRIC_VIEW}' not found on the server.")
            print("Run Demo 1 first:  (in demos/01_metrics_views)  python -m metrics_views_demo.run_footgun")
            return 2

        print("AI assistant answering via a Spark Connect tool (governed metric from Demo 1):\n")
        for question, intent in QUESTIONS:
            label = INTENTS[intent][1]
            value = tool.answer(intent)
            print(f"  Q: {question}")
            print(f"  → [{label}] = {value}   (MEASURE(), aggregation-safe)\n")

        print("Same numbers a dashboard gets. Governance follows the metric into the AI layer.")
    finally:
        tool.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
