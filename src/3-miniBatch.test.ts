import { createClient } from "@clickhouse/client";

const ch = createClient({
  host: "http://localhost:8123",
  database: "segmentation",
  clickhouse_settings: {
    wait_end_of_query: 1,
    date_time_input_format: "best_effort",
  },
});

const setupTables = [
  `
    CREATE TABLE user_events_mini_batch (
        user_id String,
        event_name LowCardinality(String),
        message_id String,
        timestamp DateTime
    )
    Engine = MergeTree()
    ORDER BY (user_id, event_name, timestamp, message_id);`,
  `
    CREATE TABLE user_states_mini_batch (
        user_id String,
        event_count AggregateFunction(uniq, String),
        computed_at DateTime DEFAULT now(),
    )
    Engine = AggregatingMergeTree()
    ORDER BY (user_id);`,
  `
    CREATE TABLE updated_user_states_mini_batch (
        user_id String,
        computed_at DateTime DEFAULT now()
    )
    Engine = MergeTree()
    PARTITION BY toYYYYMMDD(computed_at)
    ORDER BY computed_at
    TTL toStartOfDay(computed_at) + interval 100 day;`,
  `
    CREATE TABLE segment_assignments_mini_batch (
        user_id String,
        value Boolean,
        assigned_at DateTime DEFAULT now(),
        INDEX value_idx value TYPE minmax GRANULARITY 4
    )
    Engine = ReplacingMergeTree()
    ORDER BY (user_id);`,
] as const;

const setupViews = [
  `
    CREATE MATERIALIZED VIEW updated_user_states_mini_batch_mv
    TO updated_user_states_mini_batch
    AS SELECT
      user_id,
      computed_at
    FROM user_states_mini_batch;`,
] as const;

interface MiniBatchEvent {
  user_id: string;
  event_name: string;
  timestamp: string;
  message_id: string;
}

describe("using an mini batch setup", () => {
  beforeAll(async () => {
    await Promise.all(
      setupTables.map((sql) =>
        ch.command({
          query: sql,
        })
      )
    );

    await Promise.all(
      setupViews.map((sql) =>
        ch.command({
          query: sql,
        })
      )
    );
  });

  it("calculates segments of users which clicked a button at least 2 times", async () => {
    const now = new Date();
    const oneMinuteAgo = new Date(now.getTime() - 60 * 1000);
    const twoMinutesAgo = new Date(now.getTime() - 2 * 60 * 1000);

    await ch.insert({
      table:
        "user_events_mini_batch (user_id, event_name, timestamp, message_id)",
      values: [
        {
          user_id: "1",
          event_name: "BUTTON_CLICK",
          timestamp: twoMinutesAgo.toISOString(),
          message_id: "de4b1e29-7cf8-4e3e-b92b-05c8d5fd1606",
        },
        {
          user_id: "1",
          event_name: "BUTTON_CLICK",
          timestamp: oneMinuteAgo.toISOString(),
          message_id: "ca4222e5-4497-42aa-9323-f9ec04a91c87",
        },
        {
          user_id: "2",
          event_name: "BUTTON_CLICK",
          timestamp: twoMinutesAgo.toISOString(),
          message_id: "c38f4196-b60b-4f7c-b8e5-b243755c0f77",
        },
      ] satisfies MiniBatchEvent[],
      format: "JSONEachRow",
    });

    await ch.command({
      query: `
        INSERT INTO user_states_mini_batch
        SELECT
          user_id,
          uniqState(message_id),
          parseDateTimeBestEffort({now:String})
        FROM user_events_mini_batch
        WHERE
          event_name = 'BUTTON_CLICK'
          AND timestamp >= parseDateTimeBestEffort({lower_bound:String})
        GROUP BY user_id;
      `,
      query_params: {
        lower_bound: twoMinutesAgo.toISOString(),
        now: now.toISOString(),
      },
    });

    await ch.command({
      query: `
        INSERT INTO segment_assignments_mini_batch
        SELECT
          user_id,
          uniqMerge(event_count) >= 2,
          parseDateTimeBestEffort({now:String})
        FROM user_states_mini_batch
        WHERE
          user_id IN (
            SELECT user_id
            FROM updated_user_states_mini_batch
            WHERE computed_at >= parseDateTimeBestEffort({now:String})
          )
        GROUP BY user_id;
      `,
      query_params: {
        now: now.toISOString(),
      },
    });

    const segmentsResponse = await ch.query({
      query: `
          SELECT
            user_id,
            argMax(value, assigned_at) AS latest_value
          FROM segment_assignments_mini_batch
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
