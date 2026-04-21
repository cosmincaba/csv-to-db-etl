import yaml
from pathlib import Path
from typing import Dict, Any
from src.logger import setup_logger

logger = setup_logger(__name__)

class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load pipeline configuration from a YAML file

    Args:
        config_path: Path to the YAML config file
    
    Returns:
        Dictionary containing the configuration.
    
    Raises:
        FileNotFoundError: If config file is not found
        ValueError: If config file is invalid
    """
    path = Path(config_path)

    if not path.exists():
            error_msg = f"Config file not found: {config_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
    
    logger.info(f"Loading configuration from {config_path}")

    try:
        with open(path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        if config is None:
            error_msg = f"Config file is empty: {config_path}"
            logger.error(error_msg)
            raise ConfigError(error_msg)

            # Validate required fields
        validate_config(config)

        logger.info("Configuration loaded successfully")
        logger.debug(f"Config: {config}")

        return config
    
    except yaml.YAMLError as e:
        error_msg = f"Invalid YAML syntax in {config_path}: {e}"
        logger.error(error_msg)
        raise ConfigError(error_msg)
    except ConfigError:
        raise
    except Exception as e:
        error_msg = f"Failed to load config: {e}"
        logger.error(error_msg)
        raise ConfigError(error_msg)

def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate that the config has all required fields

    Args:
        config: Configuration dictionary
    
    Raises:
        ValueError: If confing is missing required fields
    """
    required_fields = ['table', 'input_path', 'validation']

    for field in required_fields:
        if field not in config:
            error_msg = f"Config missing required field: '{field}'"
            logger.error(error_msg)
            raise ConfigError(error_msg)

    # Validate validation section
    validation = config['validation']
    required_validation = ['required_columns', 'non_null_columns']

    for field in required_validation:
        if field not in validation:
            error_msg = f"Validation config missing required field: '{field}'"
            logger.error(error_msg)
            raise ConfigError(error_msg)
    
    logger.debug("Config validation passed")

if __name__ == "__main__":
    # Test config loader
    logger.info("Testing config loader...")

    # Test 1: Valid config
    try:
        config = load_config("configs/customers.yaml")
        print("\n Test 1 passed: Load valid config")
    except Exception as e:
        print(f"\n Test 1 failed: {e}")
    
    # Test 2: Missing config file
    try:
        config = load_config("configs/missing.yaml")
        print("\n Test 2 failed: Should have raised FileNotFoundError")
    except FileNotFoundError:
        print("\n Test 2 passed: Correctly raised FileNotFoundError")
    except Exception as e:
        print(f"\n Test 2 failed: Unexpected error - {e}")