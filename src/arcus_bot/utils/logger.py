"""Application logging configuration shared by bot entrypoints."""

import logging


def configure_logging(level: str) -> None:
    """Configure timestamped process logs."""
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
