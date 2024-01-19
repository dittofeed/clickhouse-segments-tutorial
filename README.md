# Clickhouse Segments Tutorial

This repository provides a tutorial on how to implement live user segmentation in ClickHouse.

It accompanies a [technical blog post](https://dev.to/dittofeed-max/how-we-stopped-our-clickhouse-db-from-exploding-5a9i-temp-slug-1784361?preview=de3f8aca974830e0e6615eb385467cadb8ad104ef531fa5c1251ec05d9699a828f82a9dd4d23b8be9970334f8d50f666e1dbbfeaec26e2c53367e4f6), which can be read in parallel.

These test implementations are written in ascending order of complexity.

1. [Naive](./src/1-naive.test.ts)
2. [Idempotent](./src/2-idempotent.test.ts)
3. [Micro-Batch](./src/3-microBatch.test.ts)
4. [Event Time vs. Processing Time](./src/4-eventTime.test.ts)
