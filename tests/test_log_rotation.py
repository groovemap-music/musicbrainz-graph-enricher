"""Proves the service's file log sink rotates instead of growing without bound.

``main()`` calls ``setup_logging(SERVICE_NAME, log_file=Path(f"/logs/{SERVICE_NAME}.log"))``
(see ``brainzgraphinator.brainzgraphinator.main``). Existing tests mock ``setup_logging``
entirely, so none of them exercise what handler that call actually installs. This test calls
the real, unmocked ``setup_logging`` with the same positional/keyword shape the service uses
and asserts the resulting file handler is a size-capped ``RotatingFileHandler`` — the behavior
``groovemap-runtime`` gained in the pinned revision (``common.log_rotation.
build_rotating_file_handler``), replacing the previously unbounded ``logging.FileHandler``.
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING

from common import setup_logging

from brainzgraphinator.brainzgraphinator import SERVICE_NAME


if TYPE_CHECKING:
    from pathlib import Path


def test_service_log_file_handler_is_size_capped_and_rotating(tmp_path: Path) -> None:
    """The exact setup_logging call main() makes installs a bounded RotatingFileHandler."""
    log_file = tmp_path / f"{SERVICE_NAME}.log"

    try:
        setup_logging(SERVICE_NAME, log_file=log_file)

        root_handlers = logging.getLogger().handlers
        file_handlers = [handler for handler in root_handlers if isinstance(handler, RotatingFileHandler)]

        assert file_handlers, "expected a RotatingFileHandler among the root logger's handlers"
        handler = file_handlers[0]
        assert handler.baseFilename == str(log_file)
        # Bounded, not unbounded: both must be positive so the file cannot grow forever
        # and old rotations cannot accumulate forever.
        assert handler.maxBytes > 0
        assert handler.backupCount > 0
    finally:
        for handler in logging.getLogger().handlers:
            handler.close()
