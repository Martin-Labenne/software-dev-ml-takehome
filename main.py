import polars as pl
import argparse
from pathlib import Path
import logging
import psutil

from src import daily_processor as processor
from src.misc import get_last_seven_files, store_format_operator_top_100, store_format_match_top_10
from src.constants import TODAY
from src.queries import merge_results_operator_top_100, merge_results_match_top_10
from src.matches import generate_matches, store_matches, generate_millions_matchs
from src.daily_results import generate_dummy_daily_results

DEFAULT_LOG_PATH = Path('data/logs/matches.log')
DEFAULT_CHUNK_SIZE = 10**7
RESULT_DIR = Path('data/rolling_seven_days/')
DIR_DAILY_OPERATOR_TOP_100 = Path('data/daily/operator_top_100/')
DIR_DAILY_MATCH_TOP_10 = Path('data/daily/match_top_10/')
DEFAULT_LOGGING_PATH = Path('main.log')

logging.basicConfig(
    filename=DEFAULT_LOGGING_PATH,
    level=logging.INFO,  # Log at INFO level and above
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def log_performance_metrics():
    cpu_times = psutil.cpu_times()
    cpu_usage = psutil.cpu_percent(interval=1)
    total_cpu_time = sum(cpu_times)  # Total CPU time in seconds

    # Get RAM usage
    ram_info = psutil.virtual_memory()
    ram_usage = ram_info.percent
    total_ram_mb = ram_info.total / (1024 * 1024)  # Convert to MB
    used_ram_mb = ram_info.used / (1024 * 1024)    # Convert to MB


    logging.info("CPU Usage: %s%%, CPU Time: User: %s s, System: %s s, Idle: %s s, Total CPU Time: %s s, RAM Usage: %s%%, Used RAM: %s MB, Total RAM: %s MB", 
                 cpu_usage, cpu_times.user, cpu_times.system, cpu_times.idle, total_cpu_time, ram_usage, used_ram_mb, total_ram_mb)

def log(function): 
    def sub(*args, **kwargs):
        log_performance_metrics()
        res = function(*args, **kwargs)
        log_performance_metrics()
        return res
    return sub

@log
def process_daily_log(log_path: Path, chunk_size: int):
    logging.info("Starting to process daily log file")
    partition_map = processor.partition_log_file(log_path, chunk_size)
    
    # Daily operator and match processing
    operator_top_100 = processor.compute_daily_operator_top_100(partition_map)
    processor.store_daily_operator_top_100(operator_top_100, TODAY)
    
    match_top_10 = processor.compute_daily_match_top_10(partition_map)
    processor.store_daily_match_top_10(match_top_10, TODAY)
    logging.info("Daily log processing completed.")

@log
def update_rolling_seven_days():
    logging.info("Updating rolling seven days statistics")
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
    logging.info("Rolling seven days statistics updated.")

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
            try: 
                print("This action can take up to several minutes for very large log files")
                process_daily_log(args.log_path, args.chunk_size)
                update_rolling_seven_days()

                print(f"Log processing and update completed. Find your results at {RESULT_DIR.resolve()}")
            except Exception as e:
                logging.error("Error processing daily log file: %s", e)
        
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