from pathlib import Path
from uuid import uuid4
import polars as pl
import numpy as np

from helpers import scan_matches
from src.queries import operator_top_100, match_top_10

def get_today(): 
    return '20241027'

def generate_dummy_daily_results():
    r6_logs_path = Path('data/logs/r6-matches.log') 
    lazy_df = scan_matches(r6_logs_path)
    top_100_avg_kills_dummy = operator_top_100(lazy_df).collect()
    top_10_matchs_dummy = match_top_10(lazy_df).collect()

    # today = '20241027'
    previous_days = ['20241026', '20241025', '20241024', '20241023', '20241022', '20241021', '20241020', '20241019', '20241018', '20241017', '20241018', '20241017']

    op_100_folder = 'data/daily/operator_top_100/'
    match_10_folder = 'data/daily/match_top_10/'

    def _replace_match_id(df): 
        match_ids = df["match_id"].unique().to_list()
        mapping = { match_id: str(uuid4()) for match_id in match_ids }
        return (
            df.with_columns(match_id=pl.col("match_id").replace(mapping))
        ) 

    def _store_sub_result(path, df): 
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            df.write_csv(file=path, include_header=True)

    for day in previous_days: 
        op_path = Path(f'{op_100_folder}{day}.csv')
        match_path = Path(f'{match_10_folder}{day}.csv')

        _store_sub_result(op_path, _replace_match_id(top_100_avg_kills_dummy))
        _store_sub_result(match_path, _replace_match_id(top_10_matchs_dummy))



def generate_matchs(n_matchs:int) -> pl.DataFrame: 
    """
    Generate a DataFrame containing simulated match data.

    This function creates a DataFrame representing match records, including player IDs, match IDs,
    operator IDs, and the number of kills for each entry. The number of matches is specified by 
    the user, and the function randomly generates additional data based on defined distributions 
    and parameters.

    After generation, the matchs are shuffled. 

    Parameters:
    -----------
    n_matchs : int
        The number of matches to simulate. Each match will have a varying number of rows 
        (gameplay entries) based on a normal distribution, constrained within specified 
        boundaries.

    Returns:
    --------
    pl.DataFrame
        A polars DataFrame containing the following columns:
        - 'player_id': A unique identifier for each player involved in the matches.
        - 'match_id': A unique identifier for each match played.
        - 'operator_id': The operator chosen by the player during the match.
        - 'nb_kills': The number of kills achieved by the player in that match.

    Notes:
    ------
    - If n_matchs is less than or equal to zero, an empty DataFrame is returned with the defined 
      column names.
    - The number of players available for selection is calculated as a ratio of the number of matches,
      with a minimum of 10 players.
    - The function generates match records with an average number of rows per match set at 55.833,
      with a standard deviation of approximately 9.33. The number of rows per match is clipped 
      between a minimum of 25 and a maximum of 82 to ensure reasonable gameplay scenarios.
    - Player IDs are generated as UUIDs, while operator IDs are randomly selected from a predefined
      list of operator values.
    - **Warning**: Generating a very high number of matches (e.g., 1,000,000) may lead to high memory usage,
      potentially causing your system to freeze, especially on machines with limited RAM (e.g., 8 GB). 
      It is recommended to test with smaller values first and considere batch generation.

    Example:
    ---------
    >>> df = generate_matchs(1000)
    >>> print(df.head())
    """
    if n_matchs <= 0: 
        return pl.DataFrame({'player_id': [], 'match_id': [], 'operator_id': [], 'nb_kills': []}) 
    
    nb_players_per_match = 10
    nb_players_ratio = 0.1  # 100/1000
    nb_players = max(nb_players_per_match, round(nb_players_ratio * n_matchs))

    match_nb_of_rows_low_boundary = 25
    match_nb_of_rows_high_boundary = 82

    players = [ str(uuid4()) for _ in range(nb_players) ] 
    matchs = [ str(uuid4()) for _ in range(n_matchs) ] 

    operators = np.array([14, 24, 30, 46, 64, 72, 73, 84, 100, 107, 109, 112, 130, 132, 173, 193, 194, 211, 230, 233, 237, 241, 245, 253])

    avg_nb_row_per_match = 55.833
    std_nb_row_per_match = 9.332856648058687

    match_nb_of_rows_all = np.clip(
        np.random.normal(avg_nb_row_per_match, std_nb_row_per_match, size=n_matchs), 
        match_nb_of_rows_low_boundary, 
        match_nb_of_rows_high_boundary
    ).astype(int)

    total_rows = match_nb_of_rows_all.sum()
    
    match_ids = np.empty(total_rows, dtype='U36')  # For UUID match_ids
    player_ids = np.empty(total_rows, dtype='U36')  # For UUID player_ids
    operator_ids = np.empty(total_rows, dtype=np.int32)
    nb_kills = np.empty(total_rows, dtype=np.int32)

    current_idx = 0
    
    for i, match_id in enumerate(matchs): 
        match_nb_of_rows = match_nb_of_rows_all[i]

        # Calculate the start index using modulo to wrap around
        start_idx = (i * 10) % nb_players
        end_idx = start_idx + 10

        # If the end index exceeds nb_players, wrap around to the beginning
        if end_idx > nb_players:
            match_players = players[start_idx:] + players[:end_idx % nb_players]
        else:
            match_players = players[start_idx:end_idx]

        sequence_players = np.random.choice(match_players, size=match_nb_of_rows, replace=True) 
        
        sequence_operators = np.random.choice(operators, size=match_nb_of_rows, replace=True)
        sequence_nb_kills = np.random.randint(0, 5, size=match_nb_of_rows)

        match_ids[current_idx:current_idx + match_nb_of_rows] = match_id
        player_ids[current_idx:current_idx + match_nb_of_rows] = sequence_players
        operator_ids[current_idx:current_idx + match_nb_of_rows] = sequence_operators
        nb_kills[current_idx:current_idx + match_nb_of_rows] = sequence_nb_kills

        current_idx += match_nb_of_rows

    matchs_df = pl.DataFrame({
        'player_id': player_ids,
        'match_id': match_ids,
        'operator_id': operator_ids,
        'nb_kills': nb_kills
    }).sample(fraction=1, shuffle=True)

    return matchs_df
