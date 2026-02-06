# Client Summary (IXS/ETH): UniV2 → Arrakis-managed UniV4

**Was the migration beneficial?** Yes—on typical execution quality and on LP outcomes versus passive benchmarks.

**Under what metrics?**
- Execution quality improved post-migration for both directions and all tested sizes: e.g., median slippage fell from 0.187%→0.153% for $1k trades in both directions, and for $50k ETH→IXS it fell from 8.57%→5.54%.
- Vault performance over 2025-12-10 → 2026-02-06: vault index ended at 0.747 vs 0.429 for “hold” and 0.433 for a simplified full-range baseline (t0 = 1.0), meaning materially smaller drawdown than passive alternatives.

**What tradeoffs remain?**
- Tail risk for large IXS sells: for $50k IXS→ETH the p90 slippage worsened (11.19%→13.13%) even though the median improved (8.57%→7.90%).
- This is consistent with concentrated/range-managed liquidity: better typical outcomes near spot, but weaker worst-case outcomes when liquidity is less favorable (e.g., near/outside active ranges).

**Recommendation**
Migrate the remaining UniV2 funds into the Arrakis vault, ideally in tranches; if your flow includes frequent large IXS→ETH swaps (~$50k), add monitoring/guardrails focused on tail slippage (p90) and range-edge conditions.
