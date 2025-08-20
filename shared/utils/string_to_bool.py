def string_to_bool(value: str) -> bool:
    """
    Convert a string to boolean.

    Args:
        value: String to convert.

    Returns:
        True if string represents a truthy value, False otherwise.
    """
    if not value:
        return False

    return value.strip().lower() in {'true', 't', '1', 'yes', 'y', 'on'}
