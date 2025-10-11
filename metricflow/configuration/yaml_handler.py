import os
import yaml

from typing import Dict, Optional

from metricflow.configuration.constants import ENV_MF_DICT, OPTIONAL_ENV_VARS


class YamlFileHandler:
    """Class to handle interactions with a non-nested yaml."""

    def __init__(self, yaml_file_path: str) -> None:  # noqa: D
        self.yaml_file_path = yaml_file_path

    def _load_yaml(self) -> Dict[str, str]:
        """Reads the provided yaml file and loads it into a dictionary."""
        content: Dict[str, str] = {}
        if os.path.exists(self.yaml_file_path):
            with open(self.yaml_file_path) as f:
                content = yaml.load(f, Loader=yaml.SafeLoader) or {}
        return content

    def get_value(self, key: str) -> Optional[str]:
        """Get value from environment variable.

        For required environment variables, raise error if not set.
        For optional environment variables (like email), return default value if not set.
        """
        if key in ENV_MF_DICT:
            env_key = ENV_MF_DICT[key]
            env_value = os.getenv(env_key)

            # If not set, check if it's optional with a default value
            if not env_value:
                if key in OPTIONAL_ENV_VARS:
                    return OPTIONAL_ENV_VARS[key]
                raise ValueError(f"Required environment variable {env_key} is not set. Please set it and try again.")

            return env_value

        # Unknown config key
        return None

    def set_value(self, key: str, value: str) -> None:
        """Sets a value to a given key in yaml file."""
        content = self._load_yaml()
        content[key] = value
        with open(self.yaml_file_path, "w") as f:
            yaml.dump(content, f)

    def remove_value(self, key: str) -> None:
        """Removes a key in yaml file."""
        content = self._load_yaml()
        if key not in content:
            return
        del content[key]
        with open(self.yaml_file_path, "w") as f:
            yaml.dump(content, f)

    @property
    def url(self) -> str:
        """Returns the file url of this handler."""
        return f"file:/{self.yaml_file_path}"
