# Market Inefficiencies Project

Notebook runner for a market microstructure / inefficiencies pipeline focused on **cross-venue price dislocations**.

## What it does
- Upload a project bundle (`.zip`) to Google Colab
- Install dependencies
- Run an end-to-end workflow:
  1) data collection  
  2) dislocation detection  
  3) backtest + summary outputs

## Tech stack
- Python (pandas, numpy, etc.)
- SQLite (local storage)
- Jupyter / Google Colab

## Project structure
- `notebooks/projcode.ipynb` — main notebook runner
- `data/` — local data (ignored)
- `results/` — outputs (ignored)
- `.gitignore` — excludes large data + artifacts

## How to run
1. Open `notebooks/projcode.ipynb` in Google Colab  
2. Run cells top-to-bottom  
3. Follow prompts to upload the project bundle (`.zip`) if required

## Inputs & outputs
**Inputs:** raw market data / extracts (not included in repo)  
**Outputs:** intermediate tables + backtest summaries saved to `results/` (ignored)

## Reproducibility
This repo is designed to keep the code public while excluding large datasets and generated outputs via `.gitignore`.

## Notes
Large datasets, outputs, and logs are excluded via `.gitignore`.

