"""Configuration loading utilities."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml

from src.core.exceptions import ConfigLoadError


@dataclass(frozen=True)
class ConfigLoader:
    """
    Loads and validates application YAML configuration.

    Parameters
    ----------
    config_path : str
        Path to the YAML configuration file.
    """

    config_path: str

    def load(self) -> dict[str, Any]:
        """
        Parse and return the YAML configuration.

        Returns
        -------
        dict[str, Any]
            Parsed configuration dictionary.

        Raises
        ------
        ConfigLoadError
            If the file cannot be read or is not a valid YAML mapping.
        """
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                raise ConfigLoadError("Root of config must be a mapping/object.")
            return data
        except ConfigLoadError:
            raise
        except Exception as exc:
            raise ConfigLoadError(
                f"Failed to load config: {self.config_path}"
            ) from exc


def get_config_value(cfg: dict[str, Any], dotted_key: str, default: Any = None) -> Any:
    """
    Safely retrieve a value from a nested config dict using a dotted key path.

    Parameters
    ----------
    cfg : dict[str, Any]
        Loaded configuration dictionary.
    dotted_key : str
        Dotted key path, e.g. ``"sites.WB.z_adjustment"``.
    default : Any
        Value to return if the key path does not exist.

    Returns
    -------
    Any
        Value at the key path, or *default*.
    """
    cur: Any = cfg
    for part in dotted_key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur
