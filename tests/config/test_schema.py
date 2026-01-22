# tests/config/test_schema.py
import pytest
from pydantic import ValidationError

from rbc.config.schema import SCHEMA_REGISTRY


@pytest.mark.parametrize("source", list(SCHEMA_REGISTRY.keys()))
def test_config_validates(source: str, source_configs: dict):
    """
    Check the cfg output is valid given a set of expected inputs.

    Args:
        source (str): Name of the data source.
        source_configs (dict): Dictionary of all source configurations.
    """
    assert source in source_configs, f"Missing test cfg for source '{source}'"
    schema = SCHEMA_REGISTRY[source]
    cfg_dict = source_configs[source]

    cfg_obj = schema.model_validate({"source": source, **cfg_dict})
    assert cfg_obj.source == source

    if cfg_dict.get("access"):
        if cfg_dict["access"].get("api_key"):
            assert cfg_obj.access.api_key == cfg_dict["access"]["api_key"]
        if cfg_dict["access"].get("username"):
            assert cfg_obj.access.username == cfg_dict["access"]["username"]
            assert cfg_obj.access.password == cfg_dict["access"]["password"]


@pytest.mark.parametrize("source", list(SCHEMA_REGISTRY.keys()))
@pytest.mark.parametrize("bad", ["", "   ", "YOUR-SECRET", "COMMIT"])
def test_config_with_access_rejects_placeholders(
    source: str, source_configs: dict, bad: str
):
    """
    Check that schemas with access requirements reject placeholder or empty
    values.

    Args:
        source (str): Name of the data source.
        source_configs (dict): Dictionary of all source configurations.
        bad (str): Possible bad access string (placeholder or empty).
    """
    schema = SCHEMA_REGISTRY[source]
    cfg_dict = source_configs[source]

    if not cfg_dict.get("access"):
        pytest.skip(f"Source '{source}' has no access block. Skipped.")

    bad_cfg = {
        "source": source,
        **cfg_dict,
        "access": {k: bad for k in cfg_dict["access"].keys()},
    }

    with pytest.raises(ValidationError):
        schema.model_validate(bad_cfg)
