"""
--- Data Source Configuration ---
Loader for YAML config
"""

from pathlib import Path
from pydantic import BaseModel
import yaml

from rbc.config.schema import SCHEMA_REGISTRY

ROOT_DIR: Path = Path(__file__).resolve().parents[2]
CONFIGS_DIR: Path = Path(ROOT_DIR, "configs")


def load_config(source: str, configs_dir: Path | None = None) -> BaseModel:
    """
    Load YAML config for a specific source.

    Args:
        source (str): Name of the data source, i.e. "entsoe".
        configs_dir (Path | None, optional): Path to the config directory.
            Defaults to rbc.config.loader.CONFIGS_DIR.

    Returns:
        BaseModel: Config data as a Pydantic object.

    Raises:
        ValueError: If no config YAML file exists for the given source.
    """
    configs_dir = configs_dir or CONFIGS_DIR
    cfg_path = Path(configs_dir, source + ".yaml")
    if not cfg_path.is_file():
        raise ValueError(f"Config file for {source} not found: {cfg_path}")

    data = yaml.safe_load(cfg_path.read_text()) or {}
    data = {"source": source, **data}

    try:
        schema = SCHEMA_REGISTRY[source]
    except KeyError:
        raise ValueError(f"Unknown source: {source}")

    return schema.model_validate(data)
