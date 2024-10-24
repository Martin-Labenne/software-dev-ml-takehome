from uuid import uuid4
import numpy as np
import pandas as pd

from typing import Union, IO

# Define the types for path_or_buf
FilePath = Union[str, bytes]  # Defines FilePath as either a string or bytes
WriteBuffer = IO  # IO is a generic type for file-like objects

def load_matchs(
    path_or_buf: FilePath | WriteBuffer[bytes] | WriteBuffer[str],
    chunksize: None | int = None
) -> pd.DataFrame | pd.io.parsers.readers.TextFileReader:
    df = pd.read_csv(
        path_or_buf, 
        header=None, 
        chunksize=chunksize,
        names=['player_id', 'match_id', 'operator_id', 'nb_kills']
    )
    return df

    
def store_matchs(
    path_or_buf: FilePath | WriteBuffer[bytes] | WriteBuffer[str], 
    matchs_df: pd.DataFrame
) -> None | str : 
    matchs_df.to_csv(path_or_buf, index=False, header=False)


def generate_matchs(n_matchs:int) -> pd.DataFrame: 
    """
    Generate a DataFrame containing simulated match data for a gaming scenario.

    This function creates a DataFrame representing match records, including player IDs, match IDs,
    operator IDs, and the number of kills for each entry. The number of matches is specified by 
    the user, and the function randomly generates additional data based on defined distributions 
    and parameters.

    Parameters:
    -----------
    n_matchs : int
        The number of matches to simulate. Each match will have a varying number of rows 
        (gameplay entries) based on a normal distribution, constrained within specified 
        boundaries.

    Returns:
    --------
    pd.DataFrame
        A pandas DataFrame containing the following columns:
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
        return pd.DataFrame({}, columns=['player_id', 'match_id', 'operator_id', 'nb_kills']) 
    
    nb_players_per_match = 10
    nb_players_ratio = 0.1  # 100/1000
    nb_players = max(nb_players_per_match, round(nb_players_ratio * n_matchs))

    match_nb_of_rows_low_boundary = 25
    match_nb_of_rows_high_boundary = 82

    players = [ str(uuid4()) for _ in range(nb_players) ] 
    matchs = [ str(uuid4()) for _ in range(n_matchs) ] 

    operators = np.array([14, 24, 30, 46, 64, 72, 73, 84, 100, 107, 109, 112, 130, 132, 173, 193, 194, 211, 230, 233, 237, 241, 245, 253])

    avg_nb_row_per_match = 55.833
    std_nb_row_per_match = 9.328189052543907

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

        # matchs: 1000
        # players: 100
        # match = 10 => 100 -> 109 (out of bounce) => 0 -> 9 = (i%10)*10 -> (i%10) + 1 * 10
        match_players = players[(i%10)*10: ((i%10) + 1) * 10]

        sequence_players = np.random.choice(match_players, size=match_nb_of_rows, replace=True) 
        
        sequence_operators = np.random.choice(operators, size=match_nb_of_rows, replace=True)
        sequence_nb_kills = np.random.randint(0, 5, size=match_nb_of_rows)

        match_ids[current_idx:current_idx + match_nb_of_rows] = match_id
        player_ids[current_idx:current_idx + match_nb_of_rows] = sequence_players
        operator_ids[current_idx:current_idx + match_nb_of_rows] = sequence_operators
        nb_kills[current_idx:current_idx + match_nb_of_rows] = sequence_nb_kills

        current_idx += match_nb_of_rows

    matchs_df = pd.DataFrame({
        'player_id': player_ids,
        'match_id': match_ids,
        'operator_id': operator_ids,
        'nb_kills': nb_kills
    })

    return matchs_df
