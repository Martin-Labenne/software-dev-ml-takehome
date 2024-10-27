from pathlib import Path
from uuid import uuid4
import polars as pl 

from src.matches import scan_matches
from src.constants import R6_MATCHES_LOG_LOCATION, PREVIOUS_DAYS
from src.queries import operator_top_100, match_top_10

def store_daily_result(
    path: Path, 
    df: pl.DataFrame
) -> None : 
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(file=path, include_header=True)


def generate_dummy_daily_results():
    r6_logs_path = Path(R6_MATCHES_LOG_LOCATION) 
    lazy_df = scan_matches(r6_logs_path)
    top_100_avg_kills_dummy = operator_top_100(lazy_df).collect()
    top_10_matches_dummy = match_top_10(lazy_df).collect()

    op_100_folder = 'data/daily/operator_top_100/'
    match_10_folder = 'data/daily/match_top_10/'

    def _replace_match_id(df): 
        match_ids = df["match_id"].unique().to_list()
        mapping = { match_id: str(uuid4()) for match_id in match_ids }
        return (
            df.with_columns(match_id=pl.col("match_id").replace(mapping))
        ) 

    for day in PREVIOUS_DAYS: 
        op_path = Path(f'{op_100_folder}{day}.csv')
        match_path = Path(f'{match_10_folder}{day}.csv')

        store_daily_result(op_path, _replace_match_id(top_100_avg_kills_dummy))
        store_daily_result(match_path, _replace_match_id(top_10_matches_dummy))
