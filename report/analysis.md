# Liquidity Migration Analysis (IXS/ETH): UniV2 → Arrakis-managed UniV4

## 1) Objective
This work evaluates a liquidity migration from Uniswap V2 (IXS/ETH) to an Arrakis-managed Uniswap V4 position on Ethereum mainnet across:
- Execution quality (historical slippage) for multiple trade sizes and both directions,
- UniV4 liquidity distribution across active tick ranges vs a theoretical full-range baseline,
- Vault performance vs holding and vs a simplified full-range LP baseline,
- A client-ready synthesis.

## 2) Execution quality (slippage) — results
Slippage is measured (excluding fees) as:
`slippage = |spot price − avg execution price| / spot price × 100 − fee × 100`.

Data coverage (from `execution_quality_summary.csv`):
- UniV2 pre period: n=800 observations per (direction, size)
- UniV4 post period: n=1386 observations per (direction, size)

### 2.1 Median slippage: UniV2 pre → UniV4 post
All values are in percent (%).

#### ETH → IXS
| USD notional | UniV2 pre median | UniV4 post median | Change (pp) | Relative change |
|---:|---:|---:|---:|---:|
| 1,000  | 0.1871 | 0.1529 | -0.0342 | -18.3% |
| 5,000  | 0.9286 | 0.7546 | -0.1740 | -18.7% |
| 10,000 | 1.8400 | 1.4656 | -0.3744 | -20.3% |
| 50,000 | 8.5677 | 5.5418 | -3.0258 | -35.3% |

#### IXS → ETH
| USD notional | UniV2 pre median | UniV4 post median | Change (pp) | Relative change |
|---:|---:|---:|---:|---:|
| 1,000  | 0.1871 | 0.1534 | -0.0337 | -18.0% |
| 5,000  | 0.9286 | 0.7675 | -0.1611 | -17.3% |
| 10,000 | 1.8400 | 1.5392 | -0.3008 | -16.4% |
| 50,000 | 8.5677 | 7.9024 | -0.6652 | -7.8% |

**Interpretation:** post-migration UniV4 improves median execution quality across all requested sizes and both directions, with the largest relative improvement at $50k for ETH→IXS (median slippage down ~35%).  

### 2.2 Tail risk (p90) — where UniV4 helps and where it worsens
#### ETH → IXS p90
- 1k: 0.2514 → 0.2036 (improves ~19.0%)
- 5k: 1.2444 → 0.9995 (improves ~19.7%)
- 10k: 2.4581 → 1.9459 (improves ~20.8%)
- 50k: 11.1871 → 7.5935 (improves ~32.1%)

#### IXS → ETH p90
- 1k: 0.2514 → 0.2226 (improves ~11.5%)
- 5k: 1.2444 → 1.1436 (improves ~8.1%)
- 10k: 2.4581 → 2.4198 (slight improvement ~1.6%)
- 50k: 11.1871 → 13.1258 (**worsens** ~17.3%)

**Interpretation:** UniV4 improves the tail for ETH→IXS across all sizes, but for IXS→ETH at $50k the p90 becomes worse even though the median improves. This is consistent with concentrated liquidity: it improves typical (in-range) execution but can worsen “bad regime” outcomes (out-of-range / thin tail coverage).

## 3) UniV4 liquidity distribution (tick ranges)
Current active ranges (from your on-chain readout):
- n_ranges = 2
- widest range: [88950, 122300] width 33,350 ticks
- narrowest range: [94800, 105600] width 10,800 ticks
- tickSpacing = 50
- spot tick estimate ≈ 105,051 (micro-quote marker)

### 3.1 What the “distribution” chart represents
The plot `figures/univ4_liquidity_distribution_coverage.png` visualizes **range overlap coverage** across tick bins:
- coverage=2 inside the overlap of both ranges (the “core” region),
- coverage=1 in the backstop-only region,
- coverage=0 outside all active ranges.

This is a proxy for depth near price (not exact liquidity per tick), but it explains execution-quality differences:
- Better near-spot depth when price is inside the core,
- Less tail depth than a full-range position.

### 3.2 Full-range baseline (theoretical)
A conceptual “full-range” position would provide nonzero coverage across all ticks; compared to the observed (2-range) design, it sacrifices near-spot concentration for more uniform tail availability.

## 4) Vault performance (Arrakis vault vs hold vs full-range)
From `vault_performance_summary.csv`:
- Time window: 2025-12-10 13:52:23 UTC → 2026-02-06 16:28:35 UTC
- Vault USD value: 582,680.49 → 435,326.16
- Vault index at t1: 0.7471 (i.e., -25.3% vs inception)
- Hold index at t1: 0.4295 (i.e., -57.1% vs inception)
- Full-range LP baseline index at t1: 0.4330 (i.e., -56.7% vs inception)

### 4.1 Relative performance
Over the measured period:
- Vault outperforms holding by +0.3176 index points (0.7471 vs 0.4295), ~+74% higher ending value vs the hold baseline on an indexed basis.
- Vault outperforms the simplified full-range LP baseline by +0.3141 (0.7471 vs 0.4330), ~+73% higher on an indexed basis.

### 4.2 Exposure / composition hints (why might this happen)
Underlying amounts changed substantially:
- ETH increased: 70.4166 → 86.4987 ETH
- IXS increased: 1,884,670 → 4,577,443 IXS

Even with higher token counts, total USD value decreased, which is consistent with a period where the assets (especially IXS) depreciated materially; the strategy’s rebalancing and fee capture can still improve outcomes relative to passive benchmarks.

## 5) Caveats (explicit)
- UniV4 “liquidity distribution” chart is **range coverage**, not exact depth per tick.
- Vault underlying mapping uses a ratio-consistency method and was stable across 695/695 samples (`u0_ixs_u1_eth`), but the vault does not expose token getters for the two outputs, so this is documented explicitly.
- Full-range LP baseline is simplified (no fee accrual) and should be treated as a conservative comparator.

Execution quality plots show slippage over time for $1k/$5k/$10k/$50k and highlight the migration boundary (UniV2 → UniV4)