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
    CREATE TABLE user_events_idempotent (
        user_id String,
        event_name LowCardinality(String),
        message_id String,
        timestamp DateTime
    )
    Engine = MergeTree()
    ORDER BY (user_id, event_name, timestamp, message_id);`,
  `
    CREATE TABLE segment_assignments_idempotent (
        user_id String,
        value Boolean,
        assigned_at DateTime DEFAULT now(),
        INDEX value_idx value TYPE minmax GRANULARITY 4
    )
    Engine = ReplacingMergeTree()
    ORDER BY (user_id);`,
] as const;

interface IdempotentEvent {
  user_id: string;
  event_name: string;
  timestamp: string;
  message_id: string;
}

describe("using an idempotent setup", () => {
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
      table:
        "user_events_idempotent (user_id, event_name, timestamp, message_id)",
      values: [
        {
          user_id: "1",
          event_name: "BUTTON_CLICK",
          timestamp: "2023-01-01 00:00:00",
          message_id: "de4b1e29-7cf8-4e3e-b92b-05c8d5fd1606",
        },
        {
          user_id: "1",
          event_name: "BUTTON_CLICK",
          timestamp: "2023-01-01 00:05:00",
          message_id: "ca4222e5-4497-42aa-9323-f9ec04a91c87",
        },
        {
          user_id: "2",
          event_name: "BUTTON_CLICK",
          timestamp: "2023-01-01 00:00:00",
          message_id: "c38f4196-b60b-4f7c-b8e5-b243755c0f77",
        },
        // duplicate event
        {
          user_id: "2",
          event_name: "BUTTON_CLICK",
          timestamp: "2023-01-01 00:00:00",
          message_id: "c38f4196-b60b-4f7c-b8e5-b243755c0f77",
        },
      ] satisfies IdempotentEvent[],
      format: "JSONEachRow",
    });

    await ch.command({
      query: `
        INSERT INTO segment_assignments_idempotent (user_id, value)
        SELECT user_id, uniq(message_id) >= 2
        FROM user_events_idempotent
        WHERE event_name = 'BUTTON_CLICK'
        GROUP BY user_id
      `,
    });

    const segmentsResponse = await ch.query({
      query: `
          SELECT
            user_id,
            argMax(value, assigned_at) AS latest_value
          FROM segment_assignments_idempotent
          GROUP BY user_id
          HAVING latest_value = True;
      `,
    });
    const { data: usersInSegment } = (await segmentsResponse.json()) as {
      data: { user_id: string }[];
    };

    expect(usersInSegment.map((u) => u.user_id)).toEqual(["1"]);
  });
});
