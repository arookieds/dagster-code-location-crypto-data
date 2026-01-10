from __future__ import annotations

from datetime import UTC, datetime

from dagster import (
    DagsterInvariantViolationError,
    InputContext,
    OutputContext,
)


def get_run_info(context: OutputContext | InputContext) -> dict[str, str]:
    """Extract run information from Dagster context safely.

    This utility handles both OutputContext (during extraction) and
    InputContext (during transformation) and safely extracts run_id
    and timestamps without raising InvariantViolationErrors.

    Args:
        context: Dagster OutputContext or InputContext

    Returns:
        Dictionary containing run_id, timestamp, and formatted datetime
    """
    run_id: str = "No run id"

    # 1. Extract Run ID safely
    if isinstance(context, OutputContext):
        try:
            run_id = context.run_id
        except (DagsterInvariantViolationError, AttributeError):
            run_id = "No run id"
    elif isinstance(context, InputContext):
        upstream = getattr(context, "upstream_output", None)
        if upstream is not None:
            try:
                # Upstream is an OutputContext; accessing run_id may raise
                run_id = upstream.run_id
            except (DagsterInvariantViolationError, AttributeError):
                run_id = "No run id"

    # 2. Extract Timestamp safely
    # We try to get the actual step execution time, falling back to 'now'
    try:
        step_context = getattr(context, "step_context", None)
        if step_context:
            # Some contexts have step_context but accessing it raises
            # if not fully initialized in certain test/standalone scenarios
            dt_obj = datetime.fromtimestamp(step_context.run_id_timestamp, tz=UTC)
        else:
            dt_obj = datetime.now(UTC)
    except (DagsterInvariantViolationError, AttributeError):
        dt_obj = datetime.now(UTC)

    timestamp = str(int(dt_obj.timestamp() * 1000))
    dt_str = dt_obj.strftime("%y-%m-%d %H:%M:%S.%f")

    return {
        "run_id": run_id,
        "timestamp": timestamp,
        "dt": dt_str,
    }
