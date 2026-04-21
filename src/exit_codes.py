"""
Exit codes for the ETL pipeline.

"""

# Success
EXIT_SUCCESS = 0

# General errors
EXIT_GENERAL_ERROR = 1
EXIT_INVALID_ARGUMENTS = 2

# File errors
EXIT_FILE_NOT_FOUND = 3
EXIT_FILE_READ_ERROR = 4

# Data errors
EXIT_NO_VALID_ROWS = 5
EXIT_VALIDATION_ERROR = 6

# Database errors
EXIT_DB_CONNECTION_ERROR = 7
EXIT_DB_LOAD_ERROR = 8

# Configuration errors
EXIT_CONFIG_ERROR = 9

def get_exit_code_description(code: int) -> str:
    """
    Get human-readable description for an exit code.

    Args:
        code: Exit code number
    
    Returns:
        Description of the exit code
    """
    descriptions = {
        EXIT_SUCCESS: "Success - Pipeline completed successfully",
        EXIT_GENERAL_ERROR: "General error - Unhandled exception",
        EXIT_INVALID_ARGUMENTS: "Invalid arguments - Check command-line args",
        EXIT_FILE_NOT_FOUND: "File not found - Input file doesn't exist",
        EXIT_FILE_READ_ERROR: "File read error - Can't read input file",
        EXIT_NO_VALID_ROWS: "No valid rows - All data failed validation",
        EXIT_VALIDATION_ERROR: "Validation error - Data quality issues",
        EXIT_DB_CONNECTION_ERROR: "Database connection error - Can't connect to PostgreSQL",
        EXIT_DB_LOAD_ERROR: "Database load error - Failed to load data",
        EXIT_CONFIG_ERROR: "Configuration error - Invalid or missing config",
    }

    return descriptions.get(code, f"Unknown exit code: {code}")

if __name__ == "__main__":
    # Test exit codes
    print("ETL Pipeline Exit Codes:")
    print("=" * 60)

    for code in range(10):
        print(f"{code}: {get_exit_code_description(code)}")