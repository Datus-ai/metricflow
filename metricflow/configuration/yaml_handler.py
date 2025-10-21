import os
import yaml

from typing import Dict, Optional


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
        """Get value from yaml config file.

        Returns:
            The value associated with the key, or None if not found.
        """
        content = self._load_yaml()
        value = content.get(key)

        # Return as string if value exists
        if value is not None:
            return str(value)

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
