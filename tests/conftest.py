"""Test Configuration."""

from pathlib import Path

from singer_sdk.testing.templates import TargetFileTestTemplate

pytest_plugins = ()


class TargetClickhouseFileTestTemplate(TargetFileTestTemplate):
    """Base Target File Test Template.

    Use this when sourcing Target test input from a .singer file.
    """

    @property
    def singer_filepath(self):
        """Get path to singer JSONL formatted messages file.

        Files will be sourced from `./target_test_streams/<test name>.singer`.

        Returns
            The expected Path to this tests singer file.

        """
        current_file_path = Path(__file__).resolve()
        return current_file_path.parent / "target_test_streams" / f"{self.name}.singer"
