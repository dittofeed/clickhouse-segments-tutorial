import { createClient } from "@clickhouse/client";

export default async function teardown() {
  const ch = createClient({
    host: "http://localhost:8123",
    clickhouse_settings: {
      wait_end_of_query: 1,
    },
  });

  await ch.command({
    query: "DROP DATABASE segmentation",
  });
}
