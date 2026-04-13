"""Logging configuration and utilities."""
from __future__ import annotations

import logging
import logging.config
from typing import Any

import yaml


def setup_logging(logging_config_path: str) -> None:
    """
    Initialise logging from a YAML config file.

    Parameters
    ----------
    logging_config_path : str
        Path to the logging YAML configuration file.

    Raises
    ------
    ValueError
        If the logging config is invalid.
    """
    with open(logging_config_path, "r", encoding="utf-8") as f:
        cfg: dict[str, Any] = yaml.safe_load(f) or {}
    logging.config.dictConfig(cfg)


def get_logger(name: str) -> logging.Logger:
    """
    Return a module-level logger.

    Parameters
    ----------
    name : str
        Logger name, typically ``__name__``.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    return logging.getLogger(name)
