import polars as pl 

def operator_top_100(df: pl.LazyFrame) -> pl.LazyFrame: 
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

def merge_results_operator_top_100(df_list: list[pl.LazyFrame]) -> pl.LazyFrame: 
    return (
            pl.concat(
                df_list,
                how='vertical'
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

def match_top_10(df: pl.LazyFrame) -> pl.LazyFrame : 
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

def merge_results_match_top_10(df_list: list[pl.LazyFrame]) -> pl.LazyFrame:
    return (
        pl.concat(
            df_list,
            how='vertical'
        )
        .sort(
            'nb_kills', 
            descending=True
        )
        .head(10)
    )