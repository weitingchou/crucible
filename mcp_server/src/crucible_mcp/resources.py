"""MCP resource definitions for Crucible."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from . import client


def register_resources(mcp: FastMCP) -> None:

    @mcp.resource("crucible://fixtures/registry")
    async def get_fixture_registry() -> str:
        """Metadata about uploaded datasets in S3/MinIO (fixture IDs and their files)."""
        fixture_data = await client.list_fixtures()
        fixture_ids = fixture_data.get("fixture_ids", [])

        if not fixture_ids:
            return "No fixtures registered."

        lines = ["# Crucible Fixture Registry\n"]
        for fid in fixture_ids:
            try:
                detail = await client.get_fixture_files(fid)
                files = detail.get("files", [])
                total_bytes = sum(f.get("size", 0) for f in files)
                total_mb = total_bytes / (1024 * 1024)
                lines.append(f"## {fid} ({total_mb:.1f} MB, {len(files)} file(s))")
                for f in files:
                    size_mb = f.get("size", 0) / (1024 * 1024)
                    lines.append(f"  - {f['name']} ({size_mb:.1f} MB, modified {f['last_modified']})")
            except Exception as exc:
                lines.append(f"## {fid} (error fetching details: {exc})")
            lines.append("")

        return "\n".join(lines)

    @mcp.resource("crucible://telemetry/recent-stats")
    async def get_recent_stats() -> str:
        """Summary of the last 5 test runs for baseline comparison context."""
        data = await client.get_recent_stats()
        runs = data.get("runs", [])

        if not runs:
            return "No test runs recorded yet."

        lines = ["# Recent Crucible Test Runs\n"]
        for r in runs:
            if r.get("completed_at") and r.get("submitted_at"):
                lines.append(
                    f"## {r['run_label']} ({r['run_id'][:8]}...)\n"
                    f"  Status:       {r['status']}\n"
                    f"  SUT:          {r['sut_type']}\n"
                    f"  Scaling:      {r['scaling_mode']}\n"
                    f"  Submitted:    {r['submitted_at']}\n"
                    f"  Completed:    {r.get('completed_at', 'N/A')}\n"
                )
            else:
                lines.append(
                    f"## {r['run_label']} ({r['run_id'][:8]}...)\n"
                    f"  Status:       {r['status']}\n"
                    f"  SUT:          {r['sut_type']}\n"
                    f"  Scaling:      {r['scaling_mode']}\n"
                    f"  Submitted:    {r['submitted_at']}\n"
                )

        return "\n".join(lines)

    @mcp.resource("crucible://logs/{run_id}")
    async def get_run_logs(run_id: str) -> str:
        """S3 artifact files produced by a test run (CSV metrics output from k6)."""
        data = await client.get_run_artifacts(run_id)
        artifacts = data.get("artifacts", [])

        if not artifacts:
            return f"No artifacts found for run {run_id}. The run may still be in progress or failed before producing output."

        lines = [f"# Artifacts for Run {run_id}\n"]
        for a in artifacts:
            size_kb = a["size"] / 1024
            lines.append(f"- {a['key']} ({size_kb:.1f} KB)")

        return "\n".join(lines)
