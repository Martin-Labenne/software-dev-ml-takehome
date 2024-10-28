# Software Engineer - ML: Take-home test

## Overview

This project provides a processing pipeline to analyze daily gameplay data for a team-based first-person shooter. The goal is to calculate the top-performing operators and matches based on kills and to maintain rolling statistics over a seven-day window. The solution processes large daily log files, handles corrupted rows, and outputs aggregated data files that are easy to interpret and maintain.

### Key Components

- **Daily Log Processing** : Reads and processes daily logs
  - **Partitioning**: Each daily log file is divided into 4096 temporary files, using the first three characters of the match_id as the basis for partitioning. This ensures that each partition contains fewer than 500,000 rows, which is approximately equivalent to 10,000 matches.
  - **Statistics Computation**: It calculates the top 100 operators based on average kills and identifies the top 10 matches according to the number of kills.
- **Rolling Seven-Day Aggregation**: Merges daily results over the last seven days to create aggregated statistics for consistent tracking.
- **Data Generation**:
  - Allows generating large datasets of match records, with options for adding controlled corruption to simulate real-world conditions.
  - Allows generating placeholder daily results for testing purposes.

### Output Files

- **Daily Top Operator Kills**: operator_top100_YYYYMMDD.txt with each row containing the operator’s ID and their top matches by average kills.
- **Daily Top Matches**: match_top10_YYYYMMDD.txt listing matches with the highest player kills.

### Why Polars ?

In this project, I used Polars instead of Pandas for for handling large data files efficiently. Polars is a robust choice as it optimizes both speed and memory use without sacrificing functionality ; and it is written in **Rust**.

**Performance**: Polars is optimized for speed. It’s built in Rust, which allows for highly efficient data processing, especially with large datasets. It often performs operations faster than Pandas due to its low-level optimizations and memory-efficient structures.

**Memory Efficiency**: Polars is designed to handle out-of-core processing, meaning it can work with data larger than the system’s available memory by using chunks and lazy evaluation. This is particularly valuable here, given the daily log files with potentially tens of millions of rows, as it helps keep memory usage low.

**Multi-threading**: Polars takes advantage of multi-threading, allowing it to perform operations in parallel across multiple CPU cores. Pandas, on the other hand, is primarily single-threaded. This multi-threading capability makes Polars a strong choice for data-intensive tasks, such as calculating rolling statistics and processing large log files.

**Lazy Evaluation**: Polars supports lazy evaluation, which delays computations until absolutely necessary. This can improve both performance and memory efficiency by reducing intermediate data storage and performing operations only once all transformations are defined.

**Compatibility**: While Polars offers a similar syntax to Pandas, it also provides additional features, which allow more flexible and complex data manipulation. This lets users familiar with Pandas quickly adapt while still benefiting from Polars' performance advantages.

## Solution Setup

### Environment setup & Package installation

```bash
python3 -m venv .
source bin/activate
pip install -r requierement.txt
```

### Generate past days Dummy results

```bash
python3 main.py --action dummy
```

Find the output in `daily/match_top_10` and `daily/operator_top_100` directories.

### Generate some Logs

In order to test the solution, you will need to process a log file.
You can use the provided log file `data/logs/matches.log`, it contains the data for 1000 matches for 55k rows.

Or you can generate logs with one of the two following commands.

To generate up to 100 thousands matchs (100k matches ~ 5,5M log rows ~ 441 Mo).

```bash
python3 main.py --action generate-matches --output_path data/logs/100k_generated_matches.log --n_matches 100000
```

To generate Millions of matchs (1M matches ~ 55M rows ~ 4.4 Go). It can take some time depending on your device.

```bash
python3 main.py --action generate-matches --output_path data/logs/1M_generated_matches.log --n_million 1
```

## Running the Solution

To process a daily log file and update the seven-day statistics:

```bash
python3 main.py --action process --log_path data/logs/matchesYYYYMMDD.log --chunk_size 10000000
```

- `--log_path`: Path to the daily log file (required action).  
- `--chunk_size`: (Optional) Sets the number of rows to process at a time. Default is 10 million.

Aggregated seven-day rolling statistics are stored in `data/rolling_seven_days/`

### Full list of options

```bash
python3 main.py --help
```

## Improvements

- The formatting of the top 10 matches output could be improved by including the player_id of each match's top performer.
- Currently, dates are not dynamically managed. For simplicity, I've set the date to `27-10-2024` and generated 10 previous dates from this point. To execute seven-day rolling updates on statistics, the system simply fetches the latest seven files from the `daily/match_top_10` and `daily/operator_top_100` directories.
- Function organization could be streamlined for better readability.
- Unit tests are not yet implemented but would enhance reliability.
- Performance tracker or logger would enhance monitoring of the system.

## My personal setup

- OS: Ubuntu 24.04
- CPU: Intel Core i5 8th Gen, 8 Cores
- RAM: 8Go
- GPU: None
