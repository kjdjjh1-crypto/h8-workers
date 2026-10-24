# h8-workers

Batch workers that aggregate public Binance market data (data.binance.vision) into 1-minute order-flow tables.
Each folder under `shards/` is one job: a key list, a small ledger and the aggregation code. Results come back as workflow artifacts.
