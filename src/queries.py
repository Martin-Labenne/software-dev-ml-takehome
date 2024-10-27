import polars as pl 

def top_100_avg_kills_per_operator_per_match(df: pl.LazyFrame) -> pl.LazyFrame: 
    return (
        df.group_by('match_id', 'operator_id')
        .agg(
            pl.col('nb_kills')
            .mean()
        )
        .sort(
            'nb_kills', 
            descending=True
        )
        .group_by('operator_id')
        .head(100)
    )

def partition_by_match_prefix(df: pl.LazyFrame) -> pl.LazyFrame: 
    return (
        df.with_columns(
            pl.col('match_id')
            .str.slice(0,3)
            .alias('match_prefix')
        )
        .group_by('match_prefix')
        .agg([
            pl.col(col_name) 
            for col_name in ['player_id', 'match_id', 'operator_id', 'nb_kills']
        ])
    )

def top_10_matches_kills_by_player(df: pl.LazyFrame) -> pl.LazyFrame : 
    return (
        df.group_by('match_id', 'player_id')
        .agg(
            pl.col('nb_kills')
            .sum()
        ).group_by('match_id')
        .agg(
            pl.col('nb_kills')
            .max()
        )
        .sort(
            'nb_kills', 
            descending=True
        ).head(10)
    )