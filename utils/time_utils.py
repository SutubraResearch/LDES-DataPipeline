def create_hourly_to_season_day_map():
    """Creates a mapping from hour numbers (1-8760) to (season_name, time_of_day)."""
    from itertools import product
    months_days = [(month, day) for month in range(1, 13) for day in range(1, 32)
                   if not (month == 2 and day > 28)
                   if not (month in [4, 6, 9, 11] and day > 30)]
    hours = range(1, 25)

    # Create (MM-DD, HH) combinations
    season_day_combinations = list(product(months_days, hours))

    # Map hour numbers to (season_name, time_of_day)
    hourly_map = {}
    for hour_number, ((month, day), hour) in enumerate(season_day_combinations, start=1):
        season_name = f"{month:02}-{day:02}"
        time_of_day = f"{hour:02}"
        hourly_map[hour_number] = (season_name, time_of_day)

    return hourly_map
