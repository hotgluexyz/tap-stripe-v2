from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.helpers._typing import to_json_compatible
import logging
PROGRESS_MARKERS = "progress_markers"
PROGRESS_MARKER_NOTE = "Note"
SIGNPOST_MARKER = "replication_key_signpost"
STARTING_MARKER = "starting_replication_value"

logger = logging.getLogger(__name__)

def increment_state(
    stream_or_partition_state: dict,
    latest_record: dict,
    replication_key: str,
    is_sorted: bool,
) -> None:
    """Update the state using data from the latest record.

    Raises InvalidStreamSortException if is_sorted=True and unsorted
    data is detected in the stream.
    """
    progress_dict = stream_or_partition_state
    if not is_sorted:
        if PROGRESS_MARKERS not in stream_or_partition_state:
            stream_or_partition_state[PROGRESS_MARKERS] = {
                PROGRESS_MARKER_NOTE: "Progress is not resumable if interrupted."
            }
        progress_dict = stream_or_partition_state[PROGRESS_MARKERS]
    old_rk_value = to_json_compatible(progress_dict.get("replication_key_value"))
    new_rk_value = to_json_compatible(latest_record[replication_key])
    logger.info(f"old_rk_value: {old_rk_value}, new_rk_value: {new_rk_value}")
    if old_rk_value is None or new_rk_value >= old_rk_value:
        progress_dict["replication_key"] = replication_key
        progress_dict["replication_key_value"] = new_rk_value
        return

    if is_sorted:
        raise InvalidStreamSortException(
            f"Unsorted data detected in stream. Latest value '{new_rk_value}' is "
            f"smaller than previous max '{old_rk_value}'."
        )