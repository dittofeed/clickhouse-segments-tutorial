import { createClient } from "@clickhouse/client";

const ch = createClient({
  host: "http://localhost:8123",
  database: "segmentation",
  clickhouse_settings: {
    wait_end_of_query: 1,
  },
});

const setup = [
  `
    CREATE TABLE user_events_naive (
        user_id String,
        event_name LowCardinality(String),
        timestamp DateTime
    )
    Engine = MergeTree()
    ORDER BY (user_id, event_name, timestamp);`,
  `
    CREATE TABLE segment_assignments_naive (
        user_id String,
        value Boolean,
        assigned_at DateTime DEFAULT now(),
        INDEX value_idx value TYPE minmax GRANULARITY 4
    )
    Engine = ReplacingMergeTree()
    ORDER BY (user_id);`,
] as const;

interface NaiveEvent {
  user_id: string;
  event_name: string;
  timestamp: string;
}

describe("using a naive setup", () => {
  beforeAll(async () => {
    await Promise.all(
      setup.map((sql) =>
        ch.command({
          query: sql,
        })
      )
    );
  });

  it("calculates segments of users which clicked a button at least 2 times", async () => {
    await ch.insert({
      table: "user_events_naive (user_id, event_name, timestamp)",
      values: [
        {
          user_id: "1",
          event_name: "BUTTON_CLICK",
          timestamp: "2023-01-01 00:00:00",
        },
        {
          user_id: "1",
          event_name: "BUTTON_CLICK",
          timestamp: "2023-01-01 00:05:00",
        },
        {
          user_id: "2",
          event_name: "BUTTON_CLICK",
          timestamp: "2023-01-01 00:00:00",
        },
      ] satisfies NaiveEvent[],
      format: "JSONEachRow",
    });

    await ch.command({
      query: `
        INSERT INTO segment_assignments_naive (user_id, value)
        SELECT user_id, count() >= 2
        FROM user_events_naive
        WHERE event_name = 'BUTTON_CLICK'
        GROUP BY user_id
      `,
    });

    const segmentsResponse = await ch.query({
      query: `
          SELECT
            user_id,
            argMax(value, assigned_at) AS latest_value
          FROM segment_assignments_naive
          WHERE value = True
          GROUP BY user_id;
      `,
    });
    const { data: usersInSegment } = (await segmentsResponse.json()) as {
      data: { user_id: string }[];
    };

    expect(usersInSegment.map((u) => u.user_id)).toEqual(["1"]);
  });
});
