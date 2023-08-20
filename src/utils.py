import datetime as dt


def string_to_timestamp(date: str) -> dt.datetime:
    """_summary_

    Args:
        date (str): _description_

    Returns:
        dt.datetime: _description_
    """
    year, month, day = date.split("-")
    if month.startswith("0"):
        month = month[1]
    if day.startswith("0"):
        day = day[1]

    # prepare data as dt.datetime format
    year, month, day = int(year), int(month), int(day)
    return int(dt.datetime(year, month, day).timestamp())
