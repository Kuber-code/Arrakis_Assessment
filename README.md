# Liquidity Migration Analysis (IXS/ETH): UniV2 → Arrakis-managed UniV4

This repository analyzes a real liquidity migration from Uniswap V2 to an Arrakis-managed Uniswap V4 position on Ethereum mainnet, focusing on:
1) Execution quality (slippage) before vs after migration,
2) UniV4 liquidity distribution across tick ranges (and a theoretical full-range baseline),
3) Vault performance vs holding and a full-range LP baseline,
4) A client-facing synthesis and recommendation.  

The required slippage definition (excluding fees) is:
`slippage = |spot price − avg execution price| / spot price × 100 − fee × 100`.  
(See challenge statement for full requirements.)  

## Scope & on-chain targets
- Pre-migration pool (UniV2): `0xC09bf2B1Bc8725903C509e8CAeef9190857215A8` (IXS/ETH).  
- Post-migration pool (UniV4): the UniV4 pool in the challenge link.  
- Arrakis vault: `0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600`.  

## Repository structure
- `scripts/` – reproducible on-chain data pulls + plotting
- `data/raw/` – static metadata inputs (e.g., token address/symbol JSON)
- `data/processed/` – CSV exports used for charts + analysis
- `figures/` – generated PNG charts
- `report/analysis.md` – technical write-up (methods + findings + caveats)
- `report/client_summary.md` – short client-facing narrative (plain English)

## Setup
### 1) Python env
```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
