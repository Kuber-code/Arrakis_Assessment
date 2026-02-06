# Client Summary — IXS/ETH migration (UniV2 → Arrakis-managed UniV4)

## What changed, in one line
Post-migration UniV4 improved typical execution quality across trade sizes and both directions, and the Arrakis vault materially outperformed “hold” and a simplified full-range LP baseline over the measured period, with the main tradeoff being worse tail outcomes in certain regimes (notably large IXS→ETH trades).  

## 1) Execution quality (slippage): what traders experience
We measured slippage (net of fees) for $1k, $5k, $10k, and $50k swaps in both directions, before and after migration.

### Median slippage (UniV2 pre → UniV4 post)
ETH → IXS:
- $1k: 0.187% → 0.153% (≈18% improvement)
- $5k: 0.929% → 0.755% (≈19% improvement)
- $10k: 1.840% → 1.466% (≈20% improvement)
- $50k: 8.568% → 5.542% (≈35% improvement)

IXS → ETH:
- $1k: 0.187% → 0.153% (≈18% improvement)
- $5k: 0.929% → 0.767% (≈17% improvement)
- $10k: 1.840% → 1.539% (≈16% improvement)
- $50k: 8.568% → 7.902% (≈8% improvement)

### Tail outcomes (p90): where risk remains
For ETH → IXS, p90 improves across all sizes (including $50k: 11.19% → 7.59%).  
For IXS → ETH, p90 improves at $1k–$10k, but **worsens** at $50k (11.19% → 13.13%), indicating that in some scenarios (thin tail / out-of-range conditions) large sells of IXS can face meaningfully worse execution even if the “typical” case improved.

## 2) Liquidity distribution: why UniV4 helped typical execution
The Arrakis strategy currently uses two active tick ranges (a wide backstop plus a narrower core).  
This concentrates effective liquidity near the current price when spot is inside the core range, which improves quotes and reduces price impact for typical trade sizes.  
The tradeoff is less uniform tail coverage than a full-range position, which can show up as worse p90 outcomes for large trades in certain directions.

## 3) Vault performance: how the LP position did
From 2025-12-10 to 2026-02-06:
- Vault value moved from ~$582.7k to ~$435.3k (index 0.747; -25%).
- A “hold initial amounts” benchmark ended at index 0.429 (-57%).
- A simplified full-range LP benchmark ended at index 0.433 (-57%).

This means the Arrakis-managed vault significantly reduced drawdown versus passive alternatives over the observed window.

## Recommendation: migrate remaining UniV2 funds?
Based on the data:
- If your goal is improved execution for typical trades and better capital efficiency near spot, the UniV4 + Arrakis setup is strongly supported by median slippage improvements and vault-relative performance.
- If you expect frequent large IXS→ETH trades (~$50k) or you prioritize worst-case execution guarantees, you should treat the tail behavior as a real tradeoff and consider sizing, monitoring, or ensuring adequate tail coverage.

A pragmatic path is to migrate the remainder in tranches, and monitor:
- whether spot spends extended time near/outside active ranges,
- whether large IXS→ETH trades are common in your flow,
- and how p90 behaves during market stress.

Execution quality plots show slippage over time for $1k/$5k/$10k/$50k and highlight the migration boundary (UniV2 → UniV4)