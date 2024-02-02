# Clickhouse Segments Tutorial

This repository provides a tutorial on how to implement live user segmentation in ClickHouse.

It accompanies a [technical blog post](https://dev.to/dittofeed-max/how-we-stopped-our-clickhouse-db-from-exploding-2969), which can be read in parallel.

These test implementations are written in ascending order of complexity.

1. [Naive](./src/1-naive.test.ts)
2. [Idempotent](./src/2-idempotent.test.ts)
3. [Micro-Batch](./src/3-microBatch.test.ts)
4. [Event Time vs. Processing Time](./src/4-eventTime.test.ts)

If you found this interesting, we'd love it if you shot over to our main repo and gave us a star! 🌟

https://github.com/dittofeed/dittofeed
