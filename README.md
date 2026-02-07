# Liquidity Migration Analysis (IXS/ETH): UniV2 → Arrakis-managed UniV4

This repository analyzes a real liquidity migration from Uniswap V2 to an Arrakis-managed Uniswap V4 position on Ethereum mainnet, covering:

1. Execution quality (slippage) before vs after migration,
2. UniV4 liquidity distribution across tick ranges (and a theoretical full-range baseline),
3. Vault performance vs holding and a simplified full-range LP baseline,
4. A written analytical summary + a short client-facing explanation.

Required slippage definition (excluding fees):
slippage = |spot price − avg execution price| / spot price × 100 − fee × 100.

## Scope & on-chain targets

- Pre-migration pool (UniV2): 0xC09bf2B1Bc8725903C509e8CAeef9190857215A8 (IXS/ETH).
- Post-migration pool (UniV4): UniV4 pool from the challenge link.
- Arrakis vault: 0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600.

## Repository structure

- scripts/ reproducible on-chain data pulls + plotting (final pipeline)
- scripts/legacy/ old/unused scripts kept only for reference (NOT used by final pipeline)
- data/raw/ static inputs + small JSON artifacts committed to repo
- data/processed/ exported datasets used for charts + reporting (CSV)
- figures/ generated PNG charts used in the report
- report/analysis.md technical write-up (methods + findings + caveats)
- report/client_summary.md short client-facing narrative (plain English)

## Setup

### 1) Python environment

python -m venv .venv

# Windows PowerShell

.venv\Scripts\Activate.ps1
pip install -r requirements.txt

### 2) RPC

Create a .env file (do NOT commit it):
RPC_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY

Optional:
VAULT_BLOCK_STRIDE=600
VAULT_SPOT_TINY_C0=0.000001

## Data sources

All on-chain state, quotes, and prices are fetched directly from Ethereum mainnet via JSON-RPC (tested with Alchemy through `RPC_URL`).
No off-chain indexed datasets (e.g., The Graph) are required; all outputs are reproducible by rerunning the scripts.

## Run order (end-to-end - one command Steps 0-5 from powershell on Windows)

powershell -ExecutionPolicy Bypass -File .\run_all.ps1

## Run order (end-to-end)

### Step 0 — sanity checks

python scripts/00_verify_addresses.py

Outputs (JSON in data/raw/):

- data/raw/address_verification.json

### Step 1 — token metadata (raw input)

python scripts/01_univ2_pair_metadata.py

Outputs (JSON in data/raw/):

- data/raw/univ2_pair_metadata.json

### Step 2 — migration block detection (UniV2)

python scripts/02_find_migration_block_univ2.py
python scripts/03_confirm_migration_events_univ2.py
python scripts/04_write_migration_block_final.py

Outputs (JSON in data/raw/):

- data/raw/migration_block_final.json

### Step 3 — execution quality (slippage) + plots

Computes slippage for the required sizes ($1k, $5k, $10k, $50k) and both directions (ETH→IXS and IXS→ETH),
for UniV2 pre-migration and UniV4 post-migration, then generates plots and a summary table.

Run:
python scripts/05_univ2_slippage_pre_usd.py
python scripts/06_univ4_slippage_post_usd.py
python scripts/07_execution_quality_plots.py

Final outputs committed in this repo:

- data/processed/execution_quality_summary.csv
- figures/execution_quality_slippage_ETH_to_IXS.png
- figures/execution_quality_slippage_IXS_to_ETH.png

Note:

- The execution-quality figures include a clearly marked migration boundary (pre UniV2 vs post UniV4).
- This repo commits the final summary + figures; underlying pre/post time-series datasets are reproducible by rerunning Step 3 scripts.

### Step 4 — UniV4 liquidity distribution (active tick ranges)

Visualizes the current liquidity distribution of the UniV4 pool across tick ranges and compares it to a theoretical full-range baseline.

Run:
python scripts/08_univ4_liquidity_distribution.py

Outputs:

- data/processed/univ4_active_ranges.csv
- data/processed/univ4_range_coverage.csv
- data/raw/univ4_liquidity_distribution_snapshot.json
- figures/univ4_liquidity_distribution_coverage.png

Note:

- The distribution plot is a range overlap coverage proxy, not exact per-tick liquidity depth.

### Step 5 — vault performance (time-series + plots)

Visualizes:

- vault token amounts over time (IXS and ETH),
- vault composition over time (USD value of IXS vs USD value of ETH),
- vault performance vs holding initial amounts,
- theoretical full-range LP performance over the same period (simplified baseline), and discusses tradeoffs.

Run:
python scripts/09a_probe_vault_interface.py
python scripts/09_vault_timeseries.py
python scripts/10_vault_performance_plots.py

Outputs:

- data/processed/vault_timeseries.csv
- data/processed/vault_performance_summary.csv
- figures/vault_token_amounts_over_time.png
- figures/vault_value_composition_over_time.png
- figures/vault_performance_index.png

## Figures (committed)

- figures/execution_quality_slippage_ETH_to_IXS.png
- figures/execution_quality_slippage_IXS_to_ETH.png
- figures/univ4_liquidity_distribution_coverage.png
- figures/vault_token_amounts_over_time.png
- figures/vault_value_composition_over_time.png
- figures/vault_performance_index.png

## Deliverables (what to read first)

- report/client_summary.md (short plain-English outcome + recommendation)
- report/analysis.md (methods + quantitative results + caveats)
- data/processed/execution_quality_summary.csv
- figures/ (PNG charts referenced by the report)

## Notes / limitations (explicit)

- UniV4 liquidity “distribution” is a range overlap coverage proxy, not exact per-tick depth.
- Vault underlying amounts come from vault.totalUnderlying(); because the vault does not expose token getters for the two outputs, mapping to (IXS, ETH) is done via ratio-matching to spot IXS/ETH and recorded in vault_timeseries.csv.
- Full-range LP baseline is simplified (no fee accrual) and should be treated as a conservative comparator.

## Safety

- Never commit .env or RPC keys.
- .venv/ and caches should be ignored via .gitignore.
