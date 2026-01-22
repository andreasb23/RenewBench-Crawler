# tests/config/test_loader.py
from pathlib import Path
import pytest

import rbc.config.loader as loader
from rbc.config.schema import SCHEMA_REGISTRY


@pytest.mark.parametrize("source", list(SCHEMA_REGISTRY.keys()))
def test_load_config(tmp_configs_dir: Path, source: str):
    """
    Check that "load_config" loads a YAML config for a source correctly.

    Args:
        tmp_configs_dir (Path): Path to the temporary config directory.
        source (str): Name of the data source (input to "load_config").
    """
    cfg = loader.load_config(source=source, configs_dir=tmp_configs_dir)
    assert cfg.source == source


def test_load_config_missing_file(tmp_configs_dir: Path):
    """
    Ensure that a source without a matching YAML config are rejected.

    Args:
        tmp_configs_dir (Path): Path to the temporary config directory.
    """
    fake_cfg_path = Path(tmp_configs_dir, "fake.yaml")
    fake_cfg_path.unlink(missing_ok=True)

    with pytest.raises(ValueError, match="not found"):
        loader.load_config(source="fake", configs_dir=tmp_configs_dir)


def test_load_config_unknown_source(tmp_configs_dir: Path):
    """
    Ensure that existing YAML files for sources that are not in the model
    registry are rejected.

    Args:
        tmp_configs_dir (Path): Path to the temporary config directory.
    """
    unknown_cfg_path = Path(tmp_configs_dir, "unknown.yaml")
    unknown_cfg_path.write_text("")

    with pytest.raises(ValueError, match="Unknown source"):
        loader.load_config(source="unknown", configs_dir=tmp_configs_dir)
