import yaml
from pathlib import Path
from typing import Dict, Any
from src.logger import setup_logger

logger = setup_logger(__name__)

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
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    logger.info(f"Loading configuration from {config_path}")

    try:
        with open(path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

            # Validate required fields
            validate_config(config)

            logger.info("Configuration loaded successfully")
            logger.debug(f"Config: {config}")

            return config
    
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML syntax: {e}")
        raise ValueError(f"Invalid YAML syntax in {config_path}: {e}")
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise
    
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
            raise ValueError(f"Config missing required field: '{field}'")

    # Validate validation section
    validation = config['validation']
    required_validation = ['required_columns', 'non_null_columns']

    for field in required_validation:
        if field not in validation:
            raise ValueError(f"Validation config missing required field: '{field}'")
    
    logger.debug("Config validation passed")

if __name__ == "__main__":
    # Test config loader
    logger.info("Testing config loader...")

    try:
        config = load_config("configs/customers.yaml")
        print("\nLoaded config:")
        print(config)
    except FileNotFoundError:
        print("\nConfig file doesn't exist yet - this is expected!")
        print("We will create it in the next step.")