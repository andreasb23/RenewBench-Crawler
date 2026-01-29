"""Utility functions for weather data processing."""


def get_days_in_month(year: int, month: str) -> int:
    """Get the number of days in a specific month.

    Args:
        year (int): Year.
        month (str): Month (format: '01' to '12').

    Returns:
        int: Number of days in the month.
    """
    month_int = int(month)
    if month_int in [1, 3, 5, 7, 8, 10, 12]:
        return 31
    elif month_int in [4, 6, 9, 11]:
        return 30
    else:  # February
        # Check for leap year
        if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
            return 29
        return 28
