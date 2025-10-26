# Summary: baseline vs optimized

| Query | Baseline ms (max) | Optimized ms (max) | Speedup (baseline/opt) | Indexes/Collections |
|---|---:|---:|---:|---|
| Q1-mechanics-gt8 | 55.4032 (min 3.1260, max 55.4032) | 13.7511 (min 3.4579, max 13.7511) | 4.03 | idx-q1-mech |
| Q2-most-themes | 663.8758 (min 15.3780, max 663.8758) | 21.3990 (min 3.9307, max 21.3990) | 31.02 | idx-q2-themes |
| Q3-designer-publisher | 201.9435 (min 5.5505, max 201.9435) | 137.1377 (min 6.1645, max 137.1377) | 1.47 | idx-q3-dp |
| Q4-year-averages | 166.8677 (min 2.2408, max 166.8677) | 13.1068 (min 2.1713, max 13.1068) | 12.73 | idx-q4-year |
| Q5-quality-popularity | 119.8030 (min 6.7310, max 119.8030) | 40.9198 (min 3.1824, max 40.9198) | 2.93 | idx-q5-bayes/idx-q5-popularity |

## Notes

- Median computed from stored runMetrics (each run is median of 5 executions after warmup).
- Speedup is baseline_median / optimized_median.


## Worst-case (max) times across all runs

- Baseline worst (max ms): 663.8758
- Optimized worst (max ms): 137.1377