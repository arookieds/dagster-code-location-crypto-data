from datetime import datetime, timezone

from dagster import InputContext, OutputContext, RunRecord


def get_run_info(context: OutputContext | InputContext) -> dict[str, str]:
    """Extract run information from Dagster context.

    Retrieves run ID and timestamp from the Dagster run record. Used to generate
    unique filesystem keys and metadata for stored files.

    Args:
        context: Dagster output or input context containing run information

    Returns:
        Dictionary with keys:
            - run_id: The Dagster run ID or "No run id" if unavailable
            - timestamp: Unix timestamp in milliseconds as string
            - dt: Formatted datetime string (YY-MM-DD HH:MM:SS.ffffff)
    """
    run_id: str = "No run id"

    if isinstance(context, OutputContext):
        run_id = context.run_id
    elif isinstance(context, InputContext):
        # Check if upstream_output exists and has a run_id
        upstream = getattr(context, "upstream_output", None)
        if upstream is not None:
            run_id = getattr(upstream, "run_id", None) or "No run id"

    # Try to get run record for timestamp, with fallback to current time
    dt: datetime = datetime.now(timezone.utc)

    step_context = getattr(context, "step_context", None)
    if step_context is not None and run_id != "No run id":
        instance = getattr(step_context, "instance", None)
        if instance is not None:
            run: RunRecord | None = instance.get_run_record_by_id(run_id)
            if run is not None and run.create_timestamp is not None:
                dt = run.create_timestamp

    timestamp: str = str(dt.timestamp() * 1000)
    return {
        "run_id": run_id,
        "timestamp": timestamp,
        "dt": dt.strftime("%y-%m-%d %H:%M:%S.%f"),
    }
