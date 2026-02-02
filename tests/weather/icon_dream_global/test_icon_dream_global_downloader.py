"""Tests for ICON-DREAM Global NWP data downloader."""

import pickle
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from rbc.weather.icon_dream_global import IconDreamGlobalDownloader


# ----------------------------------
# Fixtures
# ----------------------------------
@pytest.fixture
def tmp_output_path(tmp_path: Path) -> Path:
    """Temporary output directory."""
    return tmp_path


@pytest.fixture
def basic_args(tmp_output_path: Path) -> dict:
    """Basic initialization arguments."""
    return {
        "output_path": tmp_output_path,
        "years": [2020],
        "months": ["01"],
        "variables": ["T"],
        "dry_run": False,
        "resume": False,
    }


@pytest.fixture
def downloader(basic_args: dict) -> IconDreamGlobalDownloader:
    """Initialize IconDreamGlobalDownloader with mocked requests."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        # Mock the directory listing for variable discovery
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a><a href="/hourly/U/">U</a>'
        mock_get.return_value = mock_response

        with patch("rbc.weather.icon_dream_global.downloader.requests.head"):
            dl = IconDreamGlobalDownloader(**basic_args)
    return dl


# ----------------------------------
# Tests - Initialization
# ----------------------------------
def test_downloader_initialization(basic_args: dict) -> None:
    """Test basic initialization."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a>'
        mock_get.return_value = mock_response

        downloader = IconDreamGlobalDownloader(**basic_args)

        assert downloader.years == basic_args["years"]
        assert downloader.months == basic_args["months"]
        assert downloader.variables == basic_args["variables"]
        assert downloader.dry_run == basic_args["dry_run"]
        assert downloader.resume == basic_args["resume"]
        assert downloader.output_path == basic_args["output_path"]
        assert downloader.checkpoint_path == Path(
            basic_args["output_path"], "status.pickle"
        )


def test_downloader_initialization_default_months(tmp_output_path: Path) -> None:
    """Test initialization with default months (all 12)."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a>'
        mock_get.return_value = mock_response

        downloader = IconDreamGlobalDownloader(
            output_path=tmp_output_path,
            years=[2020],
        )

        assert len(downloader.months) == 12
        assert downloader.months[0] == "01"
        assert downloader.months[-1] == "12"


def test_downloader_initialization_custom_variables(tmp_output_path: Path) -> None:
    """Test initialization with custom variables."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a><a href="/hourly/U/">U</a>'
        mock_get.return_value = mock_response

        downloader = IconDreamGlobalDownloader(
            output_path=tmp_output_path,
            years=[2020],
            months=["01"],
            variables=["T", "U"],
        )

        assert downloader.variables == ["T", "U"]


# ----------------------------------
# Tests - Checkpoint handling
# ----------------------------------
def test_checkpoint_initialization_shape(basic_args: dict) -> None:
    """Test checkpoint has correct shape."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a>'
        mock_get.return_value = mock_response

        downloader = IconDreamGlobalDownloader(**basic_args)

        # Shape should be (years, months, variables)
        expected_shape = (
            len(basic_args["years"]),
            len(basic_args["months"]),
            len(basic_args["variables"]),
        )
        assert downloader.checkpoint.shape == expected_shape


def test_checkpoint_resume(tmp_output_path: Path) -> None:
    """Test checkpoint resume functionality."""
    # Save a fake checkpoint file
    checkpoint = np.ones((1, 1, 1))
    checkpoint_path = Path(tmp_output_path, "status.pickle")
    with open(checkpoint_path, "wb") as f:
        pickle.dump(checkpoint, f)

    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a>'
        mock_get.return_value = mock_response

        downloader = IconDreamGlobalDownloader(
            output_path=tmp_output_path,
            years=[2020],
            months=["01"],
            variables=["T"],
            resume=True,
        )

        np.testing.assert_array_equal(downloader.checkpoint, checkpoint)


def test_checkpoint_no_resume_fresh_start(tmp_output_path: Path) -> None:
    """Test that checkpoint is reset when resume=False."""
    # Save a checkpoint file
    old_checkpoint = np.ones((2, 2, 2))
    checkpoint_path = Path(tmp_output_path, "status.pickle")
    with open(checkpoint_path, "wb") as f:
        pickle.dump(old_checkpoint, f)

    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a>'
        mock_get.return_value = mock_response

        downloader = IconDreamGlobalDownloader(
            output_path=tmp_output_path,
            years=[2020],
            months=["01"],
            variables=["T"],
            resume=False,  # Don't resume
        )

        # Should be fresh zeros, not the old checkpoint
        assert downloader.checkpoint.shape == (1, 1, 1)
        np.testing.assert_array_equal(downloader.checkpoint, np.zeros((1, 1, 1)))


# ----------------------------------
# Tests - Variable validation
# ----------------------------------
def test_validate_variables_valid(downloader: IconDreamGlobalDownloader) -> None:
    """Test validation of valid variables."""
    # Should not raise any exception
    downloader._validate_variables()


def test_validate_variables_invalid(tmp_output_path: Path) -> None:
    """Test validation with invalid variable."""
    with pytest.raises(ValueError, match="Invalid variables"):
        with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.text = '<a href="/hourly/T/">T</a>'
            mock_get.return_value = mock_response

            IconDreamGlobalDownloader(
                output_path=tmp_output_path,
                years=[2020],
                variables=["INVALID_VAR"],
            )


# ----------------------------------
# Tests - Variable discovery
# ----------------------------------
def test_discover_available_variables(downloader: IconDreamGlobalDownloader) -> None:
    """Test variable discovery from DWD."""
    assert "T" in downloader.available_variables
    assert "U" in downloader.available_variables


def test_discover_available_variables_fallback(tmp_output_path: Path) -> None:
    """Test variable discovery fallback when request fails."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_get.side_effect = Exception("Network error")

        downloader = IconDreamGlobalDownloader(
            output_path=tmp_output_path,
            years=[2020],
            variables=["T"],
        )

        # Should fall back to default variables
        assert len(downloader.available_variables) > 0
        assert "T" in downloader.available_variables


# ----------------------------------
# Tests - Download functionality
# ----------------------------------
def test_download_file_dry_run(downloader: IconDreamGlobalDownloader) -> None:
    """Test _download_file in dry-run mode."""
    downloader.dry_run = True

    with patch("builtins.print"):
        status = downloader._download_file(year=2020, month="01", variable="T")

    assert status == 1
    # No actual network request should be made


def test_download_file_already_exists(downloader: IconDreamGlobalDownloader) -> None:
    """Test _download_file when file already exists."""
    # Create a dummy file
    dummy_file = downloader.output_path / "ICON-DREAM-Global_202001_T_hourly.grb"
    dummy_file.write_text("dummy content")

    status = downloader._download_file(year=2020, month="01", variable="T")

    assert status == 1


def test_download_file_success(downloader: IconDreamGlobalDownloader) -> None:
    """Test _download_file with successful download."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        # Mock successful response
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "1000000"}
        mock_response.iter_content.return_value = [b"x" * 1000000]
        mock_get.return_value = mock_response

        status = downloader._download_file(year=2020, month="01", variable="T")

        assert status == 1
        # File should exist
        assert (
            downloader.output_path / "ICON-DREAM-Global_202001_T_hourly.grb"
        ).exists()


def test_download_file_network_error(downloader: IconDreamGlobalDownloader) -> None:
    """Test _download_file with network error."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_get.side_effect = Exception("Network error")

        status = downloader._download_file(year=2020, month="01", variable="T")

        assert status == 0


# ----------------------------------
# Tests - Download data workflow
# ----------------------------------
def test_download_data_single_file(downloader: IconDreamGlobalDownloader) -> None:
    """Test download_data with single file."""
    with patch.object(downloader, "_download_file") as mock_download:
        mock_download.return_value = 1

        downloader.download_data()

        # Should call _download_file for each combination
        assert mock_download.called


def test_download_data_respects_checkpoint(
    downloader: IconDreamGlobalDownloader,
) -> None:
    """Test that download_data respects checkpoint."""
    # Mark first file as already downloaded
    downloader.checkpoint[0, 0, 0] = 1

    with patch.object(downloader, "_download_file") as mock_download:
        downloader.download_data()

        # Should not call _download_file since checkpoint is already 1
        assert not mock_download.called


def test_download_data_multiple_variables(tmp_output_path: Path) -> None:
    """Test download_data with multiple variables."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a><a href="/hourly/U/">U</a>'
        mock_get.return_value = mock_response

        downloader = IconDreamGlobalDownloader(
            output_path=tmp_output_path,
            years=[2020],
            months=["01"],
            variables=["T", "U"],
        )

        with patch.object(downloader, "_download_file") as mock_download:
            mock_download.return_value = 1
            downloader.download_data()

            # Should be called twice (T and U)
            assert mock_download.call_count == 2


# ----------------------------------
# Tests - Checkpoint saving
# ----------------------------------
def test_checkpoint_saved_to_disk(downloader: IconDreamGlobalDownloader) -> None:
    """Test that checkpoint is saved to disk."""
    downloader._save_checkpoint()

    assert downloader.checkpoint_path.exists()

    # Load and verify
    with open(downloader.checkpoint_path, "rb") as f:
        loaded = pickle.load(f)
    np.testing.assert_array_equal(loaded, downloader.checkpoint)


# ----------------------------------
# Tests - Utility methods
# ----------------------------------
def test_print_available_variables() -> None:
    """Test print_available_variables method."""
    with patch("builtins.print"):
        IconDreamGlobalDownloader.print_available_variables()
        # Should not raise any exception


def test_get_default_variables(tmp_output_path: Path) -> None:
    """Test _get_default_variables returns default."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a>'
        mock_get.return_value = mock_response

        downloader = IconDreamGlobalDownloader(
            output_path=tmp_output_path,
            years=[2020],
        )

        # Should use default variables
        assert "T" in downloader.variables


# ----------------------------------
# Tests - Multi-year/month scenarios
# ----------------------------------
def test_multi_year_download(tmp_output_path: Path) -> None:
    """Test download with multiple years."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a>'
        mock_get.return_value = mock_response

        downloader = IconDreamGlobalDownloader(
            output_path=tmp_output_path,
            years=[2020, 2021],
            months=["01", "02"],
            variables=["T"],
        )

        # Checkpoint should have 2 years x 2 months x 1 variable
        assert downloader.checkpoint.shape == (2, 2, 1)


def test_multi_month_download(tmp_output_path: Path) -> None:
    """Test download with multiple months."""
    with patch("rbc.weather.icon_dream_global.downloader.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = '<a href="/hourly/T/">T</a>'
        mock_get.return_value = mock_response

        downloader = IconDreamGlobalDownloader(
            output_path=tmp_output_path,
            years=[2020],
            months=["01", "02", "03"],
            variables=["T"],
        )

        # Checkpoint should have 1 year x 3 months x 1 variable
        assert downloader.checkpoint.shape == (1, 3, 1)
