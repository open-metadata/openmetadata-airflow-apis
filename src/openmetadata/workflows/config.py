import json
import pathlib
import re
from abc import ABC, abstractmethod
from typing import IO, Any, List, Optional
import io

import yaml
from expandvars import expandvars


class ConfigurationMechanism(ABC):
    @abstractmethod
    def load_config(self, config_fp: IO) -> dict:
        pass


class YamlConfigurationMechanism(ConfigurationMechanism):
    """load configuration from yaml files"""

    def load_config(self, config_fp: IO) -> dict:
        config = yaml.safe_load(config_fp)
        return config


class JsonConfigurationMechanism(ConfigurationMechanism):
    """load configuration from yaml files"""

    def load_config(self, config_fp: IO) -> dict:
        config = json.load(config_fp)
        return config


class ConfigurationError(Exception):
    """A configuration error has happened"""


def load_config_file(config_file: pathlib.Path) -> dict:
    if not config_file.is_file():
        raise ConfigurationError(f"Cannot open config file {config_file}")

    config_mech: ConfigurationMechanism
    if config_file.suffix in [".yaml", ".yml"]:
        config_mech = YamlConfigurationMechanism()
    elif config_file.suffix == ".json":
        config_mech = JsonConfigurationMechanism()
    else:
        raise ConfigurationError(
            "Only .json and .yml are supported. Cannot process file type {}".format(
                config_file.suffix
            )
        )

    with config_file.open() as raw_config_file:
        raw_config = raw_config_file.read()

    expanded_config_file = expandvars(raw_config, nounset=True)
    config_fp = io.StringIO(expanded_config_file)
    config = config_mech.load_config(config_fp)

    return config
