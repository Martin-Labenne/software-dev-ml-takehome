# Software Engineer - ML: Take-home test

## Overview

This project provides a processing pipeline to analyze daily gameplay data for a team-based first-person shooter. The goal is to calculate the top-performing operators and matches based on kills and to maintain rolling statistics over a seven-day window. The solution processes large daily log files, handles corrupted rows, and outputs aggregated data files that are easy to interpret and maintain.

### Key Components

- Daily Log Processing: Reads and processes daily logs, calculating the top 100 operators by average kills and optionally the top 10 matches based on kills.
- Rolling Seven-Day Aggregation: Merges daily results over the last seven days to create aggregated statistics for consistent tracking.
- Data Generation:
  - Allows generating large datasets of match records, with options for adding controlled corruption to simulate real-world conditions.
  - Allows generating placeholder daily results for testing purposes.

### Output Files

- Daily Top Operator Kills: operator_top100_YYYYMMDD.txt with each row containing the operator’s ID and their top matches by average kills.
- Daily Top Matches: match_top10_YYYYMMDD.txt listing matches with the highest player kills.

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

Aggregated seven-day rolling statistics are stored in data/rolling_seven_days/

### Full list of options

```bash
python3 main.py --help
```

## My personal setup

- OS: Ubuntu 24.04
- CPU: Intel Core i5 8th Gen, 8 Cores
- RAM: 8Go
- GPU: None
