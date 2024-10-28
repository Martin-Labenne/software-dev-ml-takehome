import polars as pl
import argparse
from pathlib import Path

from src import daily_processor as processor
from src.misc import get_last_seven_files, store_format_operator_top_100, store_format_match_top_10
from src.constants import TODAY
from src.queries import merge_results_operator_top_100, merge_results_match_top_10
from src.matches import generate_matches, store_matches, generate_millions_matchs
from src.daily_results import generate_dummy_daily_results

DEFAULT_LOG_PATH = Path('data/logs/r6-matches.log')
DEFAULT_CHUNK_SIZE = 10**7
RESULT_DIR = Path('data/rolling_seven_days/')
DIR_DAILY_OPERATOR_TOP_100 = Path('data/daily/operator_top_100/')
DIR_DAILY_MATCH_TOP_10 = Path('data/daily/match_top_10/')

def process_daily_log(log_path: Path, chunk_size: int):
    partition_map = processor.partition_log_file(log_path, chunk_size)
    
    # Daily operator and match processing
    operator_top_100 = processor.compute_daily_operator_top_100(partition_map)
    processor.store_daily_operator_top_100(operator_top_100, TODAY)
    
    match_top_10 = processor.compute_daily_match_top_10(partition_map)
    processor.store_daily_match_top_10(match_top_10, TODAY)

def update_rolling_seven_days():

    last_seven_files_operator_top_100 = get_last_seven_files(DIR_DAILY_OPERATOR_TOP_100)
    last_seven_files_match_top_10 = get_last_seven_files(DIR_DAILY_MATCH_TOP_10)
    
    rolling_seven_days_operator_top_100 = merge_results_operator_top_100(
        [pl.scan_csv(file) for file in last_seven_files_operator_top_100]
    ).collect()
    
    rolling_seven_days_match_top_10 = merge_results_match_top_10(
        [pl.scan_csv(file) for file in last_seven_files_match_top_10]
    ).collect()
    
    store_format_operator_top_100(
        RESULT_DIR / f'operator_top100_{TODAY}.txt', rolling_seven_days_operator_top_100
    )
    store_format_match_top_10(
        RESULT_DIR / f'match_top10_{TODAY}.txt', rolling_seven_days_match_top_10
    )


def main(): 
    parser = argparse.ArgumentParser(description="Process daily log and update rolling seven-day stats or generate large match datasets.")
    parser.add_argument('--action', choices=['process', 'generate-matches', 'dummy'], required=True, help="Choose to process logs, generate matches, or create dummy daily results.")
    parser.add_argument('--log_path', type=Path, help="Path to the log file (requiered for 'process' action).")
    parser.add_argument('--chunk_size', type=int, default=DEFAULT_CHUNK_SIZE, help="Chunk size for log file processing. Optionnal for 'process' action")
    parser.add_argument('--n_matches', type=int, help="Number of matches to generate (required for 'generate' action).")
    parser.add_argument('--n_million', type=int, help="Number of millions of matches to generate optionnl for 'generate' action. If n-million is provided with n-matches, n-matches is ignored")
    parser.add_argument('--output_path', type=Path, help="Path to output generated matches file (required for 'generate' action).")
    parser.add_argument('--corruption_ratio', type=float, default=0, help="Corruption ratio for generated matches. WARNING This feature does not scale well, Default 0, no corruption")
    
    args = parser.parse_args()

    match args.action: 
        case 'process':
            if not args.log_path:
                parser.error("The 'process' action requires --log_path.")
            
            print("Processing daily log file...")
            print("This action can take up to several minutes for very large log files")
            
            process_daily_log(args.log_path, args.chunk_size)
            update_rolling_seven_days()

            print(f"Log processing and update completed. Find your results at {RESULT_DIR.resolve()}")
        
        case 'generate-matches':
            if ( not args.output_path ) : 
                parser.error("The 'generate' action requires --output_path.")
            if not( args.n_matches or args.n_million ) :
                parser.error("The 'generate' action requires --n_matches or --n_million")

            print("Starting match generation...")
            if args.n_million is not None and args.n_million > 0: 
                print("Generating Millions of matchs takes time and can take several minutes to complete.\n In the mean time, you can grab a coffee ...")
                generate_millions_matchs(args.n_million, args.output_path, args.corruption_ratio)
            else : 
                store_matches(args.output_path, generate_matches(args.n_matches, args.corruption_ratio))
            print("Match generation completed.")
            
        case 'dummy': 
            print("Generating dummy daily results...")
            generate_dummy_daily_results()
            print("Dummy daily results generated.")

if __name__ == '__main__': 
    main()