import polars as pl
from tempfile import NamedTemporaryFile
from pathlib import Path

def store_tempfile(df:pl.DataFrame) -> str: 
    with NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
        temp_file_path = temp_file.name
        df.write_csv(file=temp_file_path, include_header=True)
    
    return temp_file_path

def get_last_seven_files(dir_path: Path): 
    files = list(dir_path.glob('*')) 
    files.sort()
    
    last_seven_files = files[-7:]
    
    return last_seven_files

def store_format_operator_top_100(path: Path, df: pl.DataFrame) -> None: 
    """
    Store formatted top 100 kills per operator in a file.

    Groups by operator and formats matches and kills as strings, saving
    each operator's data on a new line.

    Parameters:
    -----------
    path : Path
        File path to save the formatted data.
    df : pl.DataFrame
        DataFrame containing 'operator_id', 'match_id', and 'nb_kills'.

    """
    if not path.parent.exists(): 
        path.parent.mkdir(parents=True, exist_ok=True)
        
    pre_format = df.group_by('operator_id').agg([
        pl.col('match_id'), 
        pl.col('nb_kills') 
    ])

    with path.open(mode='w') as file: 
        for operator_id, match_id, nb_kills in pre_format[0].iter_rows():
            match_kills_pairs = list(zip(match_id, nb_kills))
            match_kills_strings = [ f'{match}:{kills}' for match, kills in match_kills_pairs ]

            file.write(f'{operator_id}|{','.join(match_kills_strings)}\n')

    with path.open(mode='a') as file: 
        for operator_id, match_id, nb_kills in pre_format[1:].iter_rows():
            match_kills_pairs = list(zip(match_id, nb_kills))
            match_kills_strings = [ f'{match}:{kills}' for match, kills in match_kills_pairs ]

            file.write(f'{operator_id}|{','.join(match_kills_strings)}\n')

def store_format_match_top_10(path: Path, df: pl.DataFrame) -> None:
    """
    Store formatted top 10 kills per match in a file.

    Writes each match and associated kills in a formated on separate lines.

    Parameters:
    -----------
    path : Path
        File path to save the formatted data.
    df : pl.DataFrame
        DataFrame containing 'match_id' and 'nb_kills'.
    """

    if not path.parent.exists(): 
        path.parent.mkdir(parents=True, exist_ok=True)

    with path.open(mode='w') as file:
        for match_id, nb_kills in df[0].iter_rows():
            match_kills_string = f'{match_id}:{nb_kills}\n'

        file.write(match_kills_string)

    with path.open(mode='a') as file: 
        for match_id, nb_kills in df[1:].iter_rows():
            match_kills_string = f'{match_id}:{nb_kills}\n'

            file.write(match_kills_string)
